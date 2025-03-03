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
	"errors"
	"fmt"
	"slices"
	"strings"
)

// MaxCollisionLimitPerDigest is the noncryptographic hash collision limit
// (per digest per map) we enforce in the first level. In the same map
// for the same digest, having a non-intentional collision should be rare and
// several collisions should be extremely rare.  The default limit should
// be high enough to ignore accidental collisions while mitigating attacks.
var MaxCollisionLimitPerDigest = uint32(255)

// hkeyElements
type hkeyElements struct {
	hkeys []Digest  // sorted list of unique hashed keys
	elems []element // elements corresponding to hkeys
	size  uint32    // total byte sizes
	level uint
}

var _ elements = &hkeyElements{}

func newHkeyElements(level uint) *hkeyElements {
	return &hkeyElements{
		level: level,
		size:  hkeyElementsPrefixSize,
	}
}

func newHkeyElementsWithElement(level uint, hkey Digest, elem element) *hkeyElements {
	return &hkeyElements{
		hkeys: []Digest{hkey},
		elems: []element{elem},
		size:  hkeyElementsPrefixSize + digestSize + elem.Size(),
		level: level,
	}
}

// Map operations (has, get, set, remove, and pop iterate)

func (e *hkeyElements) getElement(
	digester Digester,
	level uint,
	hkey Digest,
	key Value,
) (element, int, error) {

	if level >= digester.Levels() {
		return nil, 0, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
	}

	// binary search by hkey

	// Find index that e.hkeys[h] == hkey
	equalIndex := -1
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// No matching hkey
	if equalIndex == -1 {
		return nil, 0, NewKeyNotFoundError(key)
	}

	return e.elems[equalIndex], equalIndex, nil
}

func (e *hkeyElements) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	elem, _, err := e.getElement(digester, level, hkey, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by hkeyElements.getElement().
		return nil, nil, err
	}

	// Don't need to wrap error as external error because err is already categorized by element.Get().
	return elem.Get(storage, digester, level, hkey, comparator, key)
}

func (e *hkeyElements) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	elem, index, err := e.getElement(digester, level, hkey, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by hkeyElements.getElement().
		return nil, nil, nil, err
	}

	k, v, nk, err := elem.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by hkeyElements.get().
		return nil, nil, nil, err
	}

	if nk != nil {
		// Found next key in element group.
		return k, v, nk, nil
	}

	nextIndex := index + 1

	switch {
	case nextIndex < len(e.elems):
		// Next element is still in the same hkeyElements group.
		nextElement := e.elems[nextIndex]

		nextKey, err := firstKeyInElement(storage, nextElement)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by firstKeyInElement().
			return nil, nil, nil, err
		}

		return k, v, nextKey, nil

	case nextIndex == len(e.elems):
		// Next element is outside this hkeyElements group, so nextKey is nil.
		return k, v, nil, nil

	default: // nextIndex > len(e.elems)
		// This should never happen.
		return nil, nil, nil, NewUnreachableError()
	}
}

func (e *hkeyElements) Set(
	storage SlabStorage,
	address Address,
	b DigesterBuilder,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (MapKey, MapValue, error) {

	// Check hkeys are not empty
	if level >= digester.Levels() {
		return nil, nil, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
	}

	if len(e.hkeys) == 0 {
		// first element

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, nil, err
		}

		e.hkeys = []Digest{hkey}

		e.elems = []element{newElem}

		e.size += digestSize + newElem.Size()

		return newElem.key, nil, nil
	}

	if hkey < e.hkeys[0] {
		// prepend key and value

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, nil, err
		}

		e.hkeys = slices.Insert(e.hkeys, 0, hkey)

		e.elems = slices.Insert[[]element, element](e.elems, 0, newElem)

		e.size += digestSize + newElem.Size()

		return newElem.key, nil, nil
	}

	if hkey > e.hkeys[len(e.hkeys)-1] {
		// append key and value

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, nil, err
		}

		e.hkeys = append(e.hkeys, hkey)

		e.elems = append(e.elems, newElem)

		e.size += digestSize + newElem.Size()

		return newElem.key, nil, nil
	}

	equalIndex := -1   // first index that m.hkeys[h] == hkey
	lessThanIndex := 0 // last index that m.hkeys[h] > hkey
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			lessThanIndex = h
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// hkey digest has collision.
	if equalIndex != -1 {
		// New element has the same digest as existing elem.
		// elem is existing element before new element is inserted.
		elem := e.elems[equalIndex]

		// Enforce MaxCollisionLimitPerDigest at the first level (noncryptographic hash).
		if e.level == 0 {

			// Before new element with colliding digest is inserted,
			// existing elem is a single element or a collision group.
			// elem.Count() returns 1 for single element,
			// and returns > 1 for collision group.
			elementCount, err := elem.Count(storage)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by element.Count().
				return nil, nil, err
			}
			if elementCount == 0 {
				return nil, nil, NewMapElementCountError("expect element count > 0, got element count == 0")
			}

			// collisionCount is elementCount-1 because:
			// - if elem is single element, collision count is 0 (no collsion yet)
			// - if elem is collision group, collision count is 1 less than number
			//   of elements in collision group.
			collisionCount := elementCount - 1

			// Check if existing collision count reached MaxCollisionLimitPerDigest
			if collisionCount >= MaxCollisionLimitPerDigest {
				// Enforce collision limit on inserts and ignore updates.
				_, _, err = elem.Get(storage, digester, level, hkey, comparator, key)
				if err != nil {
					var knfe *KeyNotFoundError
					if errors.As(err, &knfe) {
						// Don't allow any more collisions for a digest that
						// already reached MaxCollisionLimitPerDigest.
						return nil, nil, NewCollisionLimitError(MaxCollisionLimitPerDigest)
					}
				}
			}
		}

		elem, keyStorable, existingMapValueStorable, err := elem.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by element.Set().
			return nil, nil, err
		}

		e.elems[equalIndex] = elem

		// Recompute slab size by adding all element sizes instead of using the size diff of old and new element because
		// oldElem can be the same storable when the same value is reset and oldElem.ByteSize() can equal storable.ByteSize().
		// Given this, size diff of the old and new element can be 0 even when its actual size changed.
		size := uint32(hkeyElementsPrefixSize)
		for _, element := range e.elems {
			size += element.Size() + digestSize
		}
		e.size = size

		return keyStorable, existingMapValueStorable, nil
	}

	// No matching hkey

	newElem, err := newSingleElement(storage, address, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newSingleElement().
		return nil, nil, err
	}

	// insert into sorted hkeys
	e.hkeys = slices.Insert(e.hkeys, lessThanIndex, hkey)

	// insert into sorted elements
	e.elems = slices.Insert[[]element, element](e.elems, lessThanIndex, newElem)

	e.size += digestSize + newElem.Size()

	return newElem.key, nil, nil
}

func (e *hkeyElements) Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	// Check digest level
	if level >= digester.Levels() {
		return nil, nil, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
	}

	if len(e.hkeys) == 0 || hkey < e.hkeys[0] || hkey > e.hkeys[len(e.hkeys)-1] {
		return nil, nil, NewKeyNotFoundError(key)
	}

	// binary search by hkey

	// Find index that e.hkeys[h] == hkey
	equalIndex := -1
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// No matching hkey
	if equalIndex == -1 {
		return nil, nil, NewKeyNotFoundError(key)
	}

	elem := e.elems[equalIndex]

	oldElemSize := elem.Size()

	k, v, elem, err := elem.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by element.Remove().
		return nil, nil, err
	}

	if elem == nil {
		// Remove element at equalIndex
		e.elems = slices.Delete[[]element, element](e.elems, equalIndex, equalIndex+1)

		// Remove hkey at equalIndex
		e.hkeys = slices.Delete(e.hkeys, equalIndex, equalIndex+1)

		// Adjust size
		e.size -= digestSize + oldElemSize

		return k, v, nil
	}

	e.elems[equalIndex] = elem

	e.size += elem.Size() - oldElemSize

	return k, v, nil
}

func (e *hkeyElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, NewIndexOutOfBoundsError(uint64(i), 0, uint64(len(e.elems)))
	}
	return e.elems[i], nil
}

func (e *hkeyElements) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {

	// Iterate and reset elements backwards
	for i := len(e.elems) - 1; i >= 0; i-- {
		elem := e.elems[i]

		err := elem.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by element.PopIterate().
			return err
		}
	}

	// Reset data slab
	e.hkeys = nil
	e.elems = nil
	e.size = hkeyElementsPrefixSize

	return nil
}

// Slab operations (split, merge, and lend/borrow)

func (e *hkeyElements) Merge(elems elements) error {

	rElems, ok := elems.(*hkeyElements)
	if !ok {
		return NewSlabMergeError(fmt.Errorf("cannot merge elements of different types (%T, %T)", e, elems))
	}

	e.hkeys = append(e.hkeys, rElems.hkeys...)
	e.elems = append(e.elems, rElems.elems...)
	e.size += rElems.Size() - hkeyElementsPrefixSize

	// Set merged elements to nil to prevent memory leak.
	clear(rElems.elems)

	return nil
}

func (e *hkeyElements) Split() (elements, elements, error) {

	// This computes the ceil of split to give the first slab more elements.
	dataSize := e.Size() - hkeyElementsPrefixSize
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, elem := range e.elems {
		elemSize := elem.Size() + digestSize
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

	// Create right slab elements
	rightElements := &hkeyElements{level: e.level}

	rightElements.hkeys = slices.Clone(e.hkeys[leftCount:])

	rightElements.elems = slices.Clone(e.elems[leftCount:])

	rightElements.size = dataSize - leftSize + hkeyElementsPrefixSize

	e.hkeys = e.hkeys[:leftCount]
	// NOTE: prevent memory leak
	clear(e.elems[leftCount:])
	e.elems = e.elems[:leftCount]
	e.size = hkeyElementsPrefixSize + leftSize

	return e, rightElements, nil
}

// LendToRight rebalances elements by moving elements from left to right
func (e *hkeyElements) LendToRight(re elements) error {

	minSize := minThreshold - mapDataSlabPrefixSize - hkeyElementsPrefixSize

	rightElements := re.(*hkeyElements)

	if e.level != rightElements.level {
		return NewSlabRebalanceError(
			NewHashLevelErrorf("left slab digest level %d != right slab digest level %d", e.level, rightElements.level),
		)
	}

	size := e.Size() + rightElements.Size() - hkeyElementsPrefixSize*2

	leftCount := len(e.elems)
	leftSize := e.Size() - hkeyElementsPrefixSize

	midPoint := (size + 1) >> 1

	// Left elements size is as close to midPoint as possible while right elements size >= minThreshold
	for i := len(e.elems) - 1; i >= 0; i-- {
		elemSize := e.elems[i].Size() + digestSize
		if leftSize-elemSize < midPoint && size-leftSize >= uint32(minSize) {
			break
		}
		leftSize -= elemSize
		leftCount--
	}

	// Update the right elements
	rightElements.hkeys = slices.Insert(rightElements.hkeys, 0, e.hkeys[leftCount:]...)
	rightElements.elems = slices.Insert(rightElements.elems, 0, e.elems[leftCount:]...)
	rightElements.size = size - leftSize + hkeyElementsPrefixSize

	// Update left slab
	// NOTE: prevent memory leak
	clear(e.elems[leftCount:])
	e.hkeys = e.hkeys[:leftCount]
	e.elems = e.elems[:leftCount]
	e.size = hkeyElementsPrefixSize + leftSize

	return nil
}

// BorrowFromRight rebalances slabs by moving elements from right slab to left slab.
func (e *hkeyElements) BorrowFromRight(re elements) error {

	minSize := minThreshold - mapDataSlabPrefixSize - hkeyElementsPrefixSize

	rightElements := re.(*hkeyElements)

	if e.level != rightElements.level {
		return NewSlabRebalanceError(
			NewHashLevelErrorf("left slab digest level %d != right slab digest level %d", e.level, rightElements.level),
		)
	}

	size := e.Size() + rightElements.Size() - hkeyElementsPrefixSize*2

	leftCount := len(e.elems)
	leftSize := e.Size() - hkeyElementsPrefixSize

	midPoint := (size + 1) >> 1

	for _, elem := range rightElements.elems {
		elemSize := elem.Size() + digestSize
		if leftSize+elemSize > midPoint {
			if size-leftSize-elemSize >= uint32(minSize) {
				// Include this element in left elements
				leftSize += elemSize
				leftCount++
			}
			break
		}
		leftSize += elemSize
		leftCount++
	}

	rightStartIndex := leftCount - len(e.elems)

	// Update left elements
	e.hkeys = append(e.hkeys, rightElements.hkeys[:rightStartIndex]...)
	e.elems = append(e.elems, rightElements.elems[:rightStartIndex]...)
	e.size = leftSize + hkeyElementsPrefixSize

	// Update right slab
	// TODO: copy elements to front instead?
	// NOTE: prevent memory leak
	for i := range rightStartIndex {
		rightElements.elems[i] = nil
	}
	rightElements.hkeys = rightElements.hkeys[rightStartIndex:]
	rightElements.elems = rightElements.elems[rightStartIndex:]
	rightElements.size = size - leftSize + hkeyElementsPrefixSize

	return nil
}

func (e *hkeyElements) CanLendToLeft(size uint32) bool {
	if len(e.elems) == 0 {
		return false
	}

	if len(e.elems) < 2 {
		return false
	}

	minSize := minThreshold - mapDataSlabPrefixSize
	if e.Size()-size < uint32(minSize) {
		return false
	}

	lendSize := uint32(0)
	for i := range e.elems {
		lendSize += e.elems[i].Size() + digestSize
		if e.Size()-lendSize < uint32(minSize) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

func (e *hkeyElements) CanLendToRight(size uint32) bool {
	if len(e.elems) == 0 {
		return false
	}

	if len(e.elems) < 2 {
		return false
	}

	minSize := minThreshold - mapDataSlabPrefixSize
	if e.Size()-size < uint32(minSize) {
		return false
	}

	lendSize := uint32(0)
	for i := len(e.elems) - 1; i >= 0; i-- {
		lendSize += e.elems[i].Size() + digestSize
		if e.Size()-lendSize < uint32(minSize) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

// Other operations

func (e *hkeyElements) Size() uint32 {
	return e.size
}

func (e *hkeyElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *hkeyElements) firstKey() Digest {
	if len(e.hkeys) > 0 {
		return e.hkeys[0]
	}
	return 0
}

func (e *hkeyElements) hasPointer() bool {
	for _, elem := range e.elems {
		if elem.hasPointer() {
			return true
		}
	}
	return false
}

func (e *hkeyElements) String() string {
	var s []string

	for i := range e.elems {
		s = append(s, fmt.Sprintf("%d:%s", e.hkeys[i], e.elems[i].String()))
	}

	return strings.Join(s, " ")
}
