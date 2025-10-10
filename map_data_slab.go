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

import "fmt"

// MapDataSlab is leaf node, implementing MapSlab.
// anySize is true for data slab that isn't restricted by size requirement.
type MapDataSlab struct {
	next   SlabID
	header MapSlabHeader

	elements

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *MapExtraData

	anySize        bool
	collisionGroup bool
	inlined        bool
}

var _ MapSlab = &MapDataSlab{}
var _ ContainerStorable = &MapDataSlab{}

// Map operations (has, get, set, remove, and pop iterate)

func (m *MapDataSlab) Set(
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

	keyStorable, existingMapValueStorable, err := m.elements.Set(storage, m.SlabID().address, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Set().
		return nil, nil, err
	}

	// Adjust header's first key
	m.header.firstKey = m.firstKey()

	// Adjust header's slab size
	m.header.size = m.getPrefixSize() + m.Size()

	// Store modified slab
	if !m.inlined {
		err := storeSlab(storage, m)
		if err != nil {
			return nil, nil, err
		}
	}

	return keyStorable, existingMapValueStorable, nil
}

func (m *MapDataSlab) Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	k, v, err := m.elements.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Remove().
		return nil, nil, err
	}

	// Adjust header's first key
	m.header.firstKey = m.firstKey()

	// Adjust header's slab size
	m.header.size = m.getPrefixSize() + m.Size()

	// Store modified slab
	if !m.inlined {
		err := storeSlab(storage, m)
		if err != nil {
			return nil, nil, err
		}
	}

	return k, v, nil
}

func (m *MapDataSlab) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {
	err := m.elements.PopIterate(storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.PopIterate().
		return err
	}

	// Reset data slab
	m.header.size = m.getPrefixSize() + hkeyElementsPrefixSize
	m.header.firstKey = 0
	return nil
}

// Slab operations (split, merge, and lend/borrow)

func (m *MapDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if m.Count() < 2 {
		// Can't split slab with less than two elements
		return nil, nil, NewSlabSplitErrorf("MapDataSlab (%s) has less than 2 elements", m.header.slabID)
	}

	leftElements, rightElements, err := m.elements.Split()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Split().
		return nil, nil, err
	}

	sID, err := storage.GenerateSlabID(m.SlabID().address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", m.SlabID().address))
	}

	// Create new right slab
	rightSlab := &MapDataSlab{
		header: MapSlabHeader{
			slabID:   sID,
			size:     mapDataSlabPrefixSize + rightElements.Size(),
			firstKey: rightElements.firstKey(),
		},
		next:     m.next,
		elements: rightElements,
		anySize:  m.anySize,
	}

	// Modify left (original) slab
	m.header.size = mapDataSlabPrefixSize + leftElements.Size()
	m.next = rightSlab.header.slabID
	m.elements = leftElements

	return m, rightSlab, nil
}

func (m *MapDataSlab) Merge(slab Slab) error {

	rightSlab := slab.(*MapDataSlab)

	err := m.elements.Merge(rightSlab.elements)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Merge().
		return err
	}

	m.header.size = mapDataSlabPrefixSize + m.Size()
	m.header.firstKey = m.firstKey()

	m.next = rightSlab.next

	return nil
}

func (m *MapDataSlab) LendToRight(slab Slab) error {
	rightSlab := slab.(*MapDataSlab)

	if m.anySize || rightSlab.anySize {
		return NewSlabRebalanceErrorf("any sized data slab doesn't need to rebalance")
	}

	rightElements := rightSlab.elements
	err := m.elements.LendToRight(rightElements)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.LendToRight().
		return err
	}

	// Update right slab
	rightSlab.elements = rightElements
	rightSlab.header.size = mapDataSlabPrefixSize + rightElements.Size()
	rightSlab.header.firstKey = rightElements.firstKey()

	// Update left slab
	m.header.size = mapDataSlabPrefixSize + m.Size()

	return nil
}

func (m *MapDataSlab) BorrowFromRight(slab Slab) error {

	rightSlab := slab.(*MapDataSlab)

	if m.anySize || rightSlab.anySize {
		return NewSlabRebalanceErrorf("any sized data slab doesn't need to rebalance")
	}

	rightElements := rightSlab.elements
	err := m.elements.BorrowFromRight(rightElements)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.BorrowFromRight().
		return err
	}

	// Update right slab
	rightSlab.elements = rightElements
	rightSlab.header.size = mapDataSlabPrefixSize + rightElements.Size()
	rightSlab.header.firstKey = rightElements.firstKey()

	// Update left slab
	m.header.size = mapDataSlabPrefixSize + m.Size()
	m.header.firstKey = m.firstKey()

	return nil
}

func (m *MapDataSlab) IsFull() bool {
	if m.anySize {
		return false
	}
	return m.header.size > uint32(maxThreshold)
}

// IsUnderflow returns the number of bytes needed for the data slab
// to reach the min threshold.
// Returns true if the min threshold has not been reached yet.
func (m *MapDataSlab) IsUnderflow() (uint32, bool) {
	if m.anySize {
		return 0, false
	}
	if uint32(minThreshold) > m.header.size {
		return uint32(minThreshold) - m.header.size, true
	}
	return 0, false
}

// CanLendToLeft returns true if elements on the left of the slab could be removed
// so that the slab still stores more than the min threshold.
func (m *MapDataSlab) CanLendToLeft(size uint32) bool {
	if m.anySize {
		return false
	}
	return m.elements.CanLendToLeft(size)
}

// CanLendToRight returns true if elements on the right of the slab could be removed
// so that the slab still stores more than the min threshold.
func (m *MapDataSlab) CanLendToRight(size uint32) bool {
	if m.anySize {
		return false
	}
	return m.elements.CanLendToRight(size)
}

// Inline operations

func (m *MapDataSlab) Inlined() bool {
	return m.inlined
}

// Inlinable returns true if
// - map data slab is root slab
// - size of inlined map data slab <= maxInlineSize
func (m *MapDataSlab) Inlinable(maxInlineSize uint64) bool {
	if m.extraData == nil {
		// Non-root data slab is not inlinable.
		return false
	}

	inlinedSize := inlinedMapDataSlabPrefixSize + m.Size()

	// Inlined byte size must be less than max inline size.
	return uint64(inlinedSize) <= maxInlineSize
}

// Inline converts not-inlined MapDataSlab to inlined MapDataSlab and removes it from storage.
func (m *MapDataSlab) Inline(storage SlabStorage) error {
	if m.inlined {
		return NewFatalError(fmt.Errorf("failed to inline MapDataSlab %s: it is inlined already", m.header.slabID))
	}

	id := m.header.slabID

	// Remove slab from storage because it is going to be inlined.
	err := storage.Remove(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
	}

	// Update data slab size from not inlined to inlined
	m.header.size = inlinedMapDataSlabPrefixSize + m.Size()

	// Update data slab inlined status.
	m.inlined = true

	return nil
}

// Uninline converts an inlined MapDataSlab to uninlined MapDataSlab and stores it in storage.
func (m *MapDataSlab) Uninline(storage SlabStorage) error {
	if !m.inlined {
		return NewFatalError(fmt.Errorf("failed to uninline MapDataSlab %s: it is not inlined", m.header.slabID))
	}

	// Update data slab size from inlined to not inlined.
	m.header.size = mapRootDataSlabPrefixSize + m.Size()

	// Update data slab inlined status.
	m.inlined = false

	// Store slab in storage
	return storeSlab(storage, m)
}

// Other operations

func (m *MapDataSlab) HasPointer() bool {
	return m.hasPointer()
}

func (m *MapDataSlab) getPrefixSize() uint32 {
	if m.inlined {
		return inlinedMapDataSlabPrefixSize
	}
	if m.extraData != nil {
		return mapRootDataSlabPrefixSize
	}
	return mapDataSlabPrefixSize
}

func (m *MapDataSlab) isCollisionGroup() bool {
	return m.collisionGroup
}

func (m *MapDataSlab) elementCount() uint32 {
	return m.Count()
}

func (m *MapDataSlab) ChildStorables() []Storable {
	return elementsStorables(m.elements, nil)
}

func (m *MapDataSlab) StoredValue(storage SlabStorage) (Value, error) {
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

func (m *MapDataSlab) SetSlabID(id SlabID) {
	m.header.slabID = id
}

func (m *MapDataSlab) Header() MapSlabHeader {
	return m.header
}

func (m *MapDataSlab) IsData() bool {
	return true
}

func (m *MapDataSlab) SlabID() SlabID {
	return m.header.slabID
}

func (m *MapDataSlab) ByteSize() uint32 {
	return m.header.size
}

func (m *MapDataSlab) ExtraData() *MapExtraData {
	return m.extraData
}

func (m *MapDataSlab) RemoveExtraData() *MapExtraData {
	extraData := m.extraData
	m.extraData = nil
	return extraData
}

func (m *MapDataSlab) SetExtraData(extraData *MapExtraData) {
	m.extraData = extraData
}

func (m *MapDataSlab) String() string {
	if m.extraData == nil {
		return fmt.Sprintf("MapDataSlab id:%s size:%d firstkey:%d elements: [%s]",
			m.header.slabID,
			m.header.size,
			m.header.firstKey,
			m.elements.String(),
		)
	}

	return fmt.Sprintf("MapDataSlab id:%s seed:%d size:%d firstkey:%d elements: [%s]",
		m.header.slabID,
		m.extraData.Seed,
		m.header.size,
		m.header.firstKey,
		m.elements.String(),
	)
}
