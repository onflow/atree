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
	"slices"
	"strings"
)

// ArrayDataSlab is leaf node, implementing ArraySlab.
type ArrayDataSlab struct {
	next     SlabID
	header   ArraySlabHeader
	elements []Storable

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *ArrayExtraData

	// inlined indicates whether this slab is stored inlined in its parent slab.
	// This flag affects Encode(), ByteSize(), etc.
	inlined bool
}

var _ ArraySlab = &ArrayDataSlab{}
var _ Slab = &ArrayDataSlab{}
var _ Storable = &ArrayDataSlab{}
var _ ContainerStorable = &ArrayDataSlab{}

// Array operations (get, set, insert, remove, and pop iterate)

func (a *ArrayDataSlab) Get(_ SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}
	return a.elements[index], nil
}

func (a *ArrayDataSlab) Set(storage SlabStorage, address Address, index uint64, value Value) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	oldElem := a.elements[index]

	storable, err := value.Storable(storage, address, maxInlineArrayElementSize)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
	}

	a.elements[index] = storable

	// Recompute slab size by adding all element sizes instead of using the size diff of old and new element because
	// oldElem can be the same storable when the same value is reset and oldElem.ByteSize() can equal storable.ByteSize().
	// Given this, size diff of the old and new element can be 0 even when its actual size changed.
	size := a.getPrefixSize()
	for _, e := range a.elements {
		size += e.ByteSize()
	}

	a.header.size = size

	if !a.inlined {
		err := storeSlab(storage, a)
		if err != nil {
			return nil, err
		}
	}

	return oldElem, nil
}

func (a *ArrayDataSlab) Insert(storage SlabStorage, address Address, index uint64, value Value) error {
	if index > uint64(len(a.elements)) {
		return NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	storable, err := value.Storable(storage, address, maxInlineArrayElementSize)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
	}

	// Insert new value at index in a.elements
	a.elements = slices.Insert(a.elements, int(index), storable)

	a.header.count++
	a.header.size += storable.ByteSize()

	if !a.inlined {
		err = storeSlab(storage, a)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *ArrayDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	v := a.elements[index]

	// Delete element at index from a.elements
	a.elements = slices.Delete(
		a.elements,
		int(index),
		int(index+1),
	)

	a.header.count--
	a.header.size -= v.ByteSize()

	if !a.inlined {
		err := storeSlab(storage, a)
		if err != nil {
			return nil, err
		}
	}

	return v, nil
}

func (a *ArrayDataSlab) PopIterate(_ SlabStorage, fn ArrayPopIterationFunc) error {

	// Iterate and reset elements backwards
	for i := len(a.elements) - 1; i >= 0; i-- {
		fn(a.elements[i])
	}

	// Reset data slab
	a.elements = nil
	a.header.count = 0
	a.header.size = a.getPrefixSize()

	return nil
}

// Slab operations (split, merge, and lend/borrow)

func (a *ArrayDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if len(a.elements) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, NewSlabSplitErrorf("ArrayDataSlab (%s) has less than 2 elements", a.header.slabID)
	}

	// This computes the ceil of split to give the first slab with more elements.
	dataSize := a.header.size - arrayDataSlabPrefixSize
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, e := range a.elements {
		elemSize := e.ByteSize()
		if leftSize+elemSize >= midPoint {
			// i is mid point element.  Place i on the small side.
			if leftSize <= dataSize-leftSize-elemSize {
				leftSize += elemSize
				leftCount = i + 1
			} else {
				leftCount = i
			}
			break
		}
		// left slab size < midPoint
		leftSize += elemSize
	}

	// Split elements
	var rightElements []Storable
	a.elements, rightElements = split(a.elements, leftCount)

	// Create right slab

	sID, err := storage.GenerateSlabID(a.header.slabID.address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf(
				"failed to generate slab ID for address 0x%x",
				a.header.slabID.address,
			),
		)
	}

	rightSlab := &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: sID,
			size:   arrayDataSlabPrefixSize + dataSize - leftSize,
			count:  uint32(len(rightElements)),
		},
		next:     a.next,
		elements: rightElements,
	}

	// Modify left (original) slab
	a.header.size = arrayDataSlabPrefixSize + leftSize
	a.header.count = uint32(leftCount)
	a.next = rightSlab.header.slabID

	return a, rightSlab, nil
}

func (a *ArrayDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*ArrayDataSlab)
	a.elements = merge(a.elements, rightSlab.elements)
	a.header.size = a.header.size + rightSlab.header.size - arrayDataSlabPrefixSize
	a.header.count += rightSlab.header.count
	a.next = rightSlab.next
	return nil
}

// LendToRight rebalances slabs by moving elements from left slab to right slab
func (a *ArrayDataSlab) LendToRight(slab Slab) error {

	rightSlab := slab.(*ArrayDataSlab)

	count := a.header.count + rightSlab.header.count
	size := a.header.size + rightSlab.header.size

	oldLeftCount := a.header.count
	leftCount := a.header.count
	leftSize := a.header.size

	midPoint := (size + 1) >> 1

	// Left slab size is as close to midPoint as possible while right slab size >= minThreshold
	for i := len(a.elements) - 1; i >= 0; i-- {
		elemSize := a.elements[i].ByteSize()
		if leftSize-elemSize < midPoint && size-leftSize >= minThreshold {
			break
		}
		leftSize -= elemSize
		leftCount--
	}

	// Move elements
	moveCount := oldLeftCount - leftCount
	a.elements, rightSlab.elements = lendToRight(a.elements, rightSlab.elements, int(moveCount))

	// Update right slab
	rightSlab.header.size = size - leftSize
	rightSlab.header.count = count - leftCount

	// Update left slab
	a.header.size = leftSize
	a.header.count = leftCount

	return nil
}

// BorrowFromRight rebalances slabs by moving elements from right slab to left slab.
func (a *ArrayDataSlab) BorrowFromRight(slab Slab) error {
	rightSlab := slab.(*ArrayDataSlab)

	count := a.header.count + rightSlab.header.count
	size := a.header.size + rightSlab.header.size

	oldLeftCount := a.header.count
	leftCount := a.header.count
	leftSize := a.header.size

	midPoint := (size + 1) >> 1

	for _, e := range rightSlab.elements {
		elemSize := e.ByteSize()
		if leftSize+elemSize > midPoint {
			if size-leftSize-elemSize >= minThreshold {
				// Include this element in left slab
				leftSize += elemSize
				leftCount++
			}
			break
		}
		leftSize += elemSize
		leftCount++
	}

	// Move elements
	moveCount := leftCount - oldLeftCount
	a.elements, rightSlab.elements = borrowFromRight(a.elements, rightSlab.elements, int(moveCount))

	// Update left slab
	a.header.size = leftSize
	a.header.count = leftCount

	// Update right slab
	rightSlab.header.size = size - leftSize
	rightSlab.header.count = count - leftCount

	return nil
}

func (a *ArrayDataSlab) IsFull() bool {
	return a.header.size > maxThreshold
}

// IsUnderflow returns the number of bytes needed for the data slab
// to reach the min threshold.
// Returns true if the min threshold has not been reached yet.
func (a *ArrayDataSlab) IsUnderflow() (uint32, bool) {
	if minThreshold > a.header.size {
		return minThreshold - a.header.size, true
	}
	return 0, false
}

// CanLendToLeft returns true if elements on the left of the slab could be removed
// so that the slab still stores more than the min threshold.
func (a *ArrayDataSlab) CanLendToLeft(size uint32) bool {
	if len(a.elements) < 2 {
		return false
	}
	if a.header.size-size < minThreshold {
		return false
	}
	lendSize := uint32(0)
	for i := range a.elements {
		lendSize += a.elements[i].ByteSize()
		if a.header.size-lendSize < minThreshold {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

// CanLendToRight returns true if elements on the right of the slab could be removed
// so that the slab still stores more than the min threshold.
func (a *ArrayDataSlab) CanLendToRight(size uint32) bool {
	if len(a.elements) < 2 {
		return false
	}
	if a.header.size-size < minThreshold {
		return false
	}
	lendSize := uint32(0)
	for i := len(a.elements) - 1; i >= 0; i-- {
		lendSize += a.elements[i].ByteSize()
		if a.header.size-lendSize < minThreshold {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

// Inline operations

// Inlinable returns true if
// - array data slab is root slab
// - size of inlined array data slab <= maxInlineSize
func (a *ArrayDataSlab) Inlinable(maxInlineSize uint32) bool {
	if a.extraData == nil {
		// Non-root data slab is not inlinable.
		return false
	}

	// At this point, this data slab is either
	// - inlined data slab, or
	// - not inlined root data slab

	// Compute inlined size from cached slab size
	inlinedSize := a.header.size
	if !a.inlined {
		inlinedSize = inlinedSize -
			arrayRootDataSlabPrefixSize +
			inlinedArrayDataSlabPrefixSize
	}

	// Inlined byte size must be less than max inline size.
	return inlinedSize <= maxInlineSize
}

// Inline converts not-inlined ArrayDataSlab to inlined ArrayDataSlab and removes it from storage.
func (a *ArrayDataSlab) Inline(storage SlabStorage) error {
	if a.inlined {
		return NewFatalError(fmt.Errorf("failed to inline ArrayDataSlab %s: it is inlined already", a.header.slabID))
	}

	id := a.header.slabID

	// Remove slab from storage because it is going to be inlined.
	err := storage.Remove(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
	}

	// Update data slab size as inlined slab.
	a.header.size = a.header.size -
		arrayRootDataSlabPrefixSize +
		inlinedArrayDataSlabPrefixSize

	// Update data slab inlined status.
	a.inlined = true

	return nil
}

// Uninline converts an inlined ArrayDataSlab to uninlined ArrayDataSlab and stores it in storage.
func (a *ArrayDataSlab) Uninline(storage SlabStorage) error {
	if !a.inlined {
		return NewFatalError(fmt.Errorf("failed to un-inline ArrayDataSlab %s: it is not inlined", a.header.slabID))
	}

	// Update data slab size
	a.header.size = a.header.size -
		inlinedArrayDataSlabPrefixSize +
		arrayRootDataSlabPrefixSize

	// Update data slab inlined status
	a.inlined = false

	// Store slab in storage
	return storeSlab(storage, a)
}

func (a *ArrayDataSlab) Inlined() bool {
	return a.inlined
}

// Other operations

func (a *ArrayDataSlab) SetSlabID(id SlabID) {
	a.header.slabID = id
}

func (a *ArrayDataSlab) Header() ArraySlabHeader {
	return a.header
}

func (a *ArrayDataSlab) IsData() bool {
	return true
}

func (a *ArrayDataSlab) SlabID() SlabID {
	return a.header.slabID
}

func (a *ArrayDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *ArrayDataSlab) ExtraData() *ArrayExtraData {
	return a.extraData
}

func (a *ArrayDataSlab) RemoveExtraData() *ArrayExtraData {
	extraData := a.extraData
	a.extraData = nil
	return extraData
}

func (a *ArrayDataSlab) SetExtraData(extraData *ArrayExtraData) {
	a.extraData = extraData
}

func (a *ArrayDataSlab) String() string {
	elemsStr := make([]string, len(a.elements))
	for i, e := range a.elements {
		elemsStr[i] = fmt.Sprint(e)
	}

	return fmt.Sprintf("ArrayDataSlab id:%s size:%d count:%d elements: [%s]",
		a.header.slabID,
		a.header.size,
		a.header.count,
		strings.Join(elemsStr, " "),
	)
}

func (a *ArrayDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	if a.extraData == nil {
		return nil, NewNotValueError(a.SlabID())
	}
	return &Array{
		Storage: storage,
		root:    a,
	}, nil
}

func (a *ArrayDataSlab) HasPointer() bool {
	return slices.ContainsFunc(a.elements, hasPointer)
}

func (a *ArrayDataSlab) ChildStorables() []Storable {
	return slices.Clone(a.elements)
}

func (a *ArrayDataSlab) getPrefixSize() uint32 {
	if a.inlined {
		return inlinedArrayDataSlabPrefixSize
	}
	if a.extraData != nil {
		return arrayRootDataSlabPrefixSize
	}
	return arrayDataSlabPrefixSize
}
