/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright Flow Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package atree

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// MapMetaDataSlab is internal node, implementing MapSlab.
type MapMetaDataSlab struct {
	header          MapSlabHeader
	childrenHeaders []MapSlabHeader

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *MapExtraData
}

var _ MapSlab = &MapMetaDataSlab{}

// Map operations (get, set, remove, and pop iterate)

func (m *MapMetaDataSlab) getChildSlabByDigest(storage SlabStorage, hkey Digest, key Value) (MapSlab, int, error) {

	ans := -1
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	if ans == -1 {
		return nil, 0, NewKeyNotFoundError(key)
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].slabID

	child, err := getMapSlab(storage, childID)
	if err != nil {
		return nil, 0, err
	}

	return child, childHeaderIndex, nil
}

func (m *MapMetaDataSlab) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	child, _, err := m.getChildSlabByDigest(storage, hkey, key)
	if err != nil {
		return nil, nil, err
	}

	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return child.Get(storage, digester, level, hkey, comparator, key)
}

func (m *MapMetaDataSlab) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	child, index, err := m.getChildSlabByDigest(storage, hkey, key)
	if err != nil {
		return nil, nil, nil, err
	}

	k, v, nextKey, err := child.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
	if err != nil {
		return nil, nil, nil, err
	}

	if nextKey != nil {
		// Next element is still in the same child slab.
		return k, v, nextKey, nil
	}

	// Next element is in the next child slab.

	nextIndex := index + 1

	switch {
	case nextIndex < len(m.childrenHeaders):
		// Next element is in the next child of this MapMetaDataSlab.
		nextChildID := m.childrenHeaders[nextIndex].slabID

		nextChild, err := getMapSlab(storage, nextChildID)
		if err != nil {
			return nil, nil, nil, err
		}

		nextKey, err = firstKeyInMapSlab(storage, nextChild)
		if err != nil {
			return nil, nil, nil, err
		}

		return k, v, nextKey, nil

	case nextIndex == len(m.childrenHeaders):
		// Next element is outside this MapMetaDataSlab, so nextKey is nil.
		return k, v, nil, nil

	default: // nextIndex > len(m.childrenHeaders)
		// This should never happen.
		return nil, nil, nil, NewUnreachableError()
	}
}

func (m *MapMetaDataSlab) Set(
	storage SlabStorage,
	b DigesterBuilder,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (MapKey, MapValue, error) {

	ans := 0
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].slabID

	child, err := getMapSlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, err
	}

	keyStorable, existingMapValueStorable, err := child.Set(storage, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, nil, err
	}

	m.childrenHeaders[childHeaderIndex] = child.Header()

	if childHeaderIndex == 0 {
		// Update firstKey.  May not be necessary.
		m.header.firstKey = m.childrenHeaders[childHeaderIndex].firstKey
	}

	if child.IsFull() {
		err := m.SplitChildSlab(storage, child, childHeaderIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.SplitChildSlab().
			return nil, nil, err
		}
		return keyStorable, existingMapValueStorable, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err := m.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, nil, err
		}
		return keyStorable, existingMapValueStorable, nil
	}

	err = storeSlab(storage, m)
	if err != nil {
		return nil, nil, err
	}
	return keyStorable, existingMapValueStorable, nil
}

func (m *MapMetaDataSlab) Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	ans := -1
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	if ans == -1 {
		return nil, nil, NewKeyNotFoundError(key)
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].slabID

	child, err := getMapSlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, err
	}

	k, v, err := child.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Remove().
		return nil, nil, err
	}

	m.childrenHeaders[childHeaderIndex] = child.Header()

	if childHeaderIndex == 0 {
		// Update firstKey.  May not be necessary.
		m.header.firstKey = m.childrenHeaders[childHeaderIndex].firstKey
	}

	if child.IsFull() {
		err := m.SplitChildSlab(storage, child, childHeaderIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.SplitChildSlab().
			return nil, nil, err
		}
		return k, v, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err := m.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, nil, err
		}
		return k, v, nil
	}

	err = storeSlab(storage, m)
	if err != nil {
		return nil, nil, err
	}
	return k, v, nil
}

func (m *MapMetaDataSlab) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {

	// Iterate child slabs backwards
	for i := len(m.childrenHeaders) - 1; i >= 0; i-- {

		childID := m.childrenHeaders[i].slabID

		child, err := getMapSlab(storage, childID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return err
		}

		err = child.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapSlab.PopIterate().
			return err
		}

		// Remove child slab
		err = storage.Remove(childID)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
		}
	}

	// All child slabs are removed.

	// Reset meta data slab
	m.childrenHeaders = nil
	m.header.firstKey = 0
	m.header.size = mapMetaDataSlabPrefixSize

	return nil
}

// Slab operations (split, merge, and lend/borrow)

func (m *MapMetaDataSlab) SplitChildSlab(storage SlabStorage, child MapSlab, childHeaderIndex int) error {
	leftSlab, rightSlab, err := child.Split(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Split().
		return err
	}

	left := leftSlab.(MapSlab)
	right := rightSlab.(MapSlab)

	leftIndex, rightIndex := childHeaderIndex, childHeaderIndex+1

	// Set left slab header
	m.childrenHeaders[leftIndex] = left.Header()

	// Insert right slab header
	m.childrenHeaders = slices.Insert(
		m.childrenHeaders,
		rightIndex,
		right.Header(),
	)

	// Increase header size
	m.header.size += mapSlabHeaderSize

	// Store modified slabs
	err = storeSlab(storage, left)
	if err != nil {
		return err
	}

	err = storeSlab(storage, right)
	if err != nil {
		return err
	}

	return storeSlab(storage, m)
}

// MergeOrRebalanceChildSlab merges or rebalances child slab.
// parent slab's data is adjusted.
// If merged, then parent slab's data is adjusted.
//
// +-----------------------+-----------------------+----------------------+-----------------------+
// |			   | no left sibling (sib) | left sib can't lend  | left sib can lend     |
// +=======================+=======================+======================+=======================+
// | no right sib          | panic                 | merge with left      | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can't lend  | merge with right      | merge with smaller   | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can lend    | rebalance with right  | rebalance with right | rebalance with bigger |
// +-----------------------+-----------------------+----------------------+-----------------------+
func (m *MapMetaDataSlab) MergeOrRebalanceChildSlab(
	storage SlabStorage,
	child MapSlab,
	childHeaderIndex int,
	underflowSize uint32,
) error {

	// Retrieve left sibling of the same parent.
	var leftSib MapSlab
	if childHeaderIndex > 0 {
		leftSibID := m.childrenHeaders[childHeaderIndex-1].slabID

		var err error
		leftSib, err = getMapSlab(storage, leftSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return err
		}
	}

	// Retrieve right siblings of the same parent.
	var rightSib MapSlab
	if childHeaderIndex < len(m.childrenHeaders)-1 {
		rightSibID := m.childrenHeaders[childHeaderIndex+1].slabID

		var err error
		rightSib, err = getMapSlab(storage, rightSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return err
		}
	}

	leftCanLend := leftSib != nil && leftSib.CanLendToRight(underflowSize)
	rightCanLend := rightSib != nil && rightSib.CanLendToLeft(underflowSize)

	canRebalance := leftCanLend || rightCanLend

	// Child can rebalance elements with at least one sibling.
	if canRebalance {

		var leftSlab, rightSlab MapSlab
		var leftSlabIndex, rightSlabIndex int
		var leftBorrowFromRight bool

		if !leftCanLend {
			// Rebalance with right sib

			leftSlab, rightSlab = child, rightSib
			leftSlabIndex, rightSlabIndex = childHeaderIndex, childHeaderIndex+1
			leftBorrowFromRight = true

		} else if !rightCanLend {
			// Rebalance with left sib

			leftSlab, rightSlab = leftSib, child
			leftSlabIndex, rightSlabIndex = childHeaderIndex-1, childHeaderIndex
			leftBorrowFromRight = false

		} else if leftSib.ByteSize() > rightSib.ByteSize() { // Rebalance with bigger sib
			// Rebalance with left sib

			leftSlab, rightSlab = leftSib, child
			leftSlabIndex, rightSlabIndex = childHeaderIndex-1, childHeaderIndex
			leftBorrowFromRight = false

		} else { // leftSib.ByteSize() <= rightSib.ByteSize
			// Rebalance with right sib

			leftSlab, rightSlab = child, rightSib
			leftSlabIndex, rightSlabIndex = childHeaderIndex, childHeaderIndex+1
			leftBorrowFromRight = true
		}

		return m.rebalanceChildren(
			storage,
			leftSlab,
			rightSlab,
			leftSlabIndex,
			rightSlabIndex,
			leftBorrowFromRight,
		)
	}

	// Child can't rebalance with any sibling.  It must merge with one sibling.

	var leftSlab, rightSlab MapSlab
	var leftSlabIndex, rightSlabIndex int

	if leftSib == nil {
		// Merge (left) child slab with rightSib

		leftSlab, rightSlab = child, rightSib
		leftSlabIndex, rightSlabIndex = childHeaderIndex, childHeaderIndex+1

	} else if rightSib == nil {
		// Merge leftSib with (right) child slab

		leftSlab, rightSlab = leftSib, child
		leftSlabIndex, rightSlabIndex = childHeaderIndex-1, childHeaderIndex

	} else if leftSib.ByteSize() < rightSib.ByteSize() { // Merge with smaller sib
		// Merge leftSib with (right) child slab

		leftSlab, rightSlab = leftSib, child
		leftSlabIndex, rightSlabIndex = childHeaderIndex-1, childHeaderIndex

	} else { // leftSib.ByteSize() >= rightSib.ByteSize
		// Merge (left) child slab with rightSib

		leftSlab, rightSlab = child, rightSib
		leftSlabIndex, rightSlabIndex = childHeaderIndex, childHeaderIndex+1
	}

	return m.mergeChildren(
		storage,
		leftSlab,
		rightSlab,
		leftSlabIndex,
		rightSlabIndex,
	)
}

func (m *MapMetaDataSlab) rebalanceChildren(
	storage SlabStorage,
	leftSlab MapSlab,
	rightSlab MapSlab,
	leftSlabIndex int,
	rightSlabIndex int,
	leftBorrowFromRight bool,
) error {
	var err error

	if leftBorrowFromRight {
		err = leftSlab.BorrowFromRight(rightSlab)
	} else {
		err = leftSlab.LendToRight(rightSlab)
	}
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.BorrowFromRight().
		return err
	}

	m.childrenHeaders[leftSlabIndex] = leftSlab.Header()
	m.childrenHeaders[rightSlabIndex] = rightSlab.Header()

	if leftSlabIndex == 0 {
		m.header.firstKey = leftSlab.Header().firstKey
	}

	// Store modified slabs
	err = storeSlab(storage, leftSlab)
	if err != nil {
		return err
	}

	err = storeSlab(storage, rightSlab)
	if err != nil {
		return err
	}

	return storeSlab(storage, m)
}

func (m *MapMetaDataSlab) mergeChildren(
	storage SlabStorage,
	leftSlab MapSlab,
	rightSlab MapSlab,
	leftSlabIndex int,
	rightSlabIndex int,
) error {
	err := leftSlab.Merge(rightSlab)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Merge().
		return err
	}

	m.updateChildrenHeadersAfterMerge(
		leftSlab.Header(),
		leftSlabIndex,
		rightSlabIndex)

	m.header.size -= mapSlabHeaderSize

	if leftSlabIndex == 0 {
		m.header.firstKey = leftSlab.Header().firstKey
	}

	// Store modified slabs in storage
	err = storeSlab(storage, leftSlab)
	if err != nil {
		return err
	}

	err = storeSlab(storage, m)
	if err != nil {
		return err
	}

	// Remove right (merged) slab from storage
	err = storage.Remove(rightSlab.SlabID())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", rightSlab.SlabID()))
	}

	return nil
}

func (m *MapMetaDataSlab) updateChildrenHeadersAfterMerge(
	mergedSlabHeader MapSlabHeader,
	leftSlabIndex int,
	rightSlabIndex int,
) {
	// Update left slab header
	m.childrenHeaders[leftSlabIndex] = mergedSlabHeader

	// Remove right slab header
	m.childrenHeaders = slices.Delete[[]MapSlabHeader](
		m.childrenHeaders,
		rightSlabIndex,
		rightSlabIndex+1,
	)
}

func (m *MapMetaDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*MapMetaDataSlab)

	m.childrenHeaders = append(m.childrenHeaders, rightSlab.childrenHeaders...)
	m.header.size += rightSlab.header.size - mapMetaDataSlabPrefixSize

	return nil
}

func (m *MapMetaDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if len(m.childrenHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		return nil, nil, NewSlabSplitErrorf("MapMetaDataSlab (%s) has less than 2 child headers", m.header.slabID)
	}

	leftChildrenCount := int(math.Ceil(float64(len(m.childrenHeaders)) / 2))
	leftSize := leftChildrenCount * mapSlabHeaderSize

	sID, err := storage.GenerateSlabID(m.SlabID().address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", m.SlabID().address))
	}

	// Construct right slab
	rightSlab := &MapMetaDataSlab{
		header: MapSlabHeader{
			slabID:   sID,
			size:     m.header.size - uint32(leftSize),
			firstKey: m.childrenHeaders[leftChildrenCount].firstKey,
		},
	}

	rightSlab.childrenHeaders = slices.Clone(m.childrenHeaders[leftChildrenCount:])

	// Modify left (original) slab
	m.childrenHeaders = m.childrenHeaders[:leftChildrenCount]
	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftSize)

	return m, rightSlab, nil
}

func (m *MapMetaDataSlab) LendToRight(slab Slab) error {
	rightSlab := slab.(*MapMetaDataSlab)

	childrenHeadersLen := len(m.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftChildrenHeadersLen := childrenHeadersLen / 2
	rightChildrenHeadersLen := childrenHeadersLen - leftChildrenHeadersLen

	// Update right slab childrenHeaders by prepending borrowed children headers
	rightSlab.childrenHeaders = slices.Insert(
		rightSlab.childrenHeaders,
		0,
		m.childrenHeaders[leftChildrenHeadersLen:]...)

	// Update right slab header
	rightSlab.header.size = mapMetaDataSlabPrefixSize + uint32(rightChildrenHeadersLen)*mapSlabHeaderSize
	rightSlab.header.firstKey = rightSlab.childrenHeaders[0].firstKey

	// Update left slab (original)
	m.childrenHeaders = m.childrenHeaders[:leftChildrenHeadersLen]

	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftChildrenHeadersLen)*mapSlabHeaderSize

	return nil
}

func (m *MapMetaDataSlab) BorrowFromRight(slab Slab) error {

	rightSlab := slab.(*MapMetaDataSlab)

	childrenHeadersLen := len(m.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftSlabHeaderLen := childrenHeadersLen / 2
	rightSlabHeaderLen := childrenHeadersLen - leftSlabHeaderLen

	// Update left slab (original)
	m.childrenHeaders = append(m.childrenHeaders, rightSlab.childrenHeaders[:leftSlabHeaderLen-len(m.childrenHeaders)]...)

	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*mapSlabHeaderSize

	// Update right slab
	rightSlab.childrenHeaders = rightSlab.childrenHeaders[len(rightSlab.childrenHeaders)-rightSlabHeaderLen:]

	rightSlab.header.size = mapMetaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*mapSlabHeaderSize
	rightSlab.header.firstKey = rightSlab.childrenHeaders[0].firstKey

	return nil
}

func (m MapMetaDataSlab) IsFull() bool {
	return m.header.size > uint32(maxThreshold)
}

func (m MapMetaDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > m.header.size {
		return uint32(minThreshold) - m.header.size, true
	}
	return 0, false
}

func (m *MapMetaDataSlab) CanLendToLeft(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / mapSlabHeaderSize))
	return m.header.size-mapSlabHeaderSize*n > uint32(minThreshold)
}

func (m *MapMetaDataSlab) CanLendToRight(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / mapSlabHeaderSize))
	return m.header.size-mapSlabHeaderSize*n > uint32(minThreshold)
}

// Inline operations

func (m *MapMetaDataSlab) Inlined() bool {
	return false
}

func (m *MapMetaDataSlab) Inlinable(_ uint64) bool {
	return false
}

func (m *MapMetaDataSlab) Inline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to inline MapMetaDataSlab %s: MapMetaDataSlab can't be inlined", m.header.slabID))
}

func (m *MapMetaDataSlab) Uninline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to uninline MapMetaDataSlab %s: MapMetaDataSlab is already unlined", m.header.slabID))
}

// Other operations

func (m *MapMetaDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	if m.extraData == nil {
		return nil, NewNotValueError(m.SlabID())
	}

	digestBuilder := NewDefaultDigesterBuilder()

	digestBuilder.SetSeed(m.extraData.Seed, typicalRandomConstant)

	return &OrderedMap{
		Storage:         storage,
		root:            m,
		digesterBuilder: digestBuilder,
	}, nil
}

func (m *MapMetaDataSlab) ChildStorables() []Storable {
	childIDs := make([]Storable, len(m.childrenHeaders))

	for i, h := range m.childrenHeaders {
		childIDs[i] = SlabIDStorable(h.slabID)
	}

	return childIDs
}

func (m MapMetaDataSlab) IsData() bool {
	return false
}

func (m *MapMetaDataSlab) SetSlabID(id SlabID) {
	m.header.slabID = id
}

func (m *MapMetaDataSlab) Header() MapSlabHeader {
	return m.header
}

func (m *MapMetaDataSlab) ByteSize() uint32 {
	return m.header.size
}

func (m *MapMetaDataSlab) SlabID() SlabID {
	return m.header.slabID
}

func (m *MapMetaDataSlab) ExtraData() *MapExtraData {
	return m.extraData
}

func (m *MapMetaDataSlab) RemoveExtraData() *MapExtraData {
	extraData := m.extraData
	m.extraData = nil
	return extraData
}

func (m *MapMetaDataSlab) SetExtraData(extraData *MapExtraData) {
	m.extraData = extraData
}

func (m *MapMetaDataSlab) String() string {
	elemsStr := make([]string, len(m.childrenHeaders))
	for i, h := range m.childrenHeaders {
		elemsStr[i] = fmt.Sprintf("{id:%s size:%d firstKey:%d}", h.slabID, h.size, h.firstKey)
	}

	return fmt.Sprintf("MapMetaDataSlab id:%s size:%d firstKey:%d children: [%s]",
		m.header.slabID,
		m.header.size,
		m.header.firstKey,
		strings.Join(elemsStr, " "),
	)
}
