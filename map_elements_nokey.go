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
	"strings"
)

// singleElements

type singleElements struct {
	elems []*singleElement // list of key+value pairs
	size  uint32           // total key+value byte sizes
	level uint
}

var _ elements = &singleElements{}

func newSingleElementsWithElement(level uint, elem *singleElement) *singleElements {
	return &singleElements{
		level: level,
		size:  singleElementsPrefixSize + elem.size,
		elems: []*singleElement{elem},
	}
}

// Map operations (has, get, set, remove, and pop iterate)

func (e *singleElements) get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, int, error) {

	if level != digester.Levels() {
		return nil, nil, 0, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search by key
	for i, elem := range e.elems {
		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, nil, 0, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}
		if equal {
			return elem.key, elem.value, i, nil
		}
	}

	return nil, nil, 0, NewKeyNotFoundError(key)
}

func (e *singleElements) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	k, v, _, err := e.get(storage, digester, level, hkey, comparator, key)
	return k, v, err
}

func (e *singleElements) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	k, v, index, err := e.get(storage, digester, level, hkey, comparator, key)
	if err != nil {
		return nil, nil, nil, err
	}

	nextIndex := index + 1

	switch {
	case nextIndex < len(e.elems):
		// Next element is still in the same singleElements group.
		nextKey := e.elems[nextIndex].key
		return k, v, nextKey, nil

	case nextIndex == len(e.elems):
		// Next element is outside this singleElements group, so nextKey is nil.
		return k, v, nil, nil

	default: // nextIndex > len(e.elems)
		// This should never happen.
		return nil, nil, nil, NewUnreachableError()
	}
}

func (e *singleElements) Set(
	storage SlabStorage,
	address Address,
	_ DigesterBuilder,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	_ HashInputProvider,
	key Value,
	value Value,
) (MapKey, MapValue, error) {

	if level != digester.Levels() {
		return nil, nil, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search key and update value
	for i := 0; i < len(e.elems); i++ {
		elem := e.elems[i]

		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}

		if equal {
			existingKeyStorable := elem.key
			existingValueStorable := elem.value

			vs, err := value.Storable(storage, address, maxInlineMapValueSize(uint64(elem.key.ByteSize())))
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by Value interface.
				return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
			}

			elem.value = vs
			elem.size = singleElementPrefixSize + elem.key.ByteSize() + elem.value.ByteSize()

			// Recompute slab size by adding all element sizes instead of using the size diff of old and new element because
			// oldElem can be the same storable when the same value is reset and oldElem.ByteSize() can equal storable.ByteSize().
			// Given this, size diff of the old and new element can be 0 even when its actual size changed.
			size := uint32(singleElementsPrefixSize)
			for _, element := range e.elems {
				size += element.Size()
			}
			e.size = size

			return existingKeyStorable, existingValueStorable, nil
		}
	}

	// no matching key, append new element to the end.
	newElem, err := newSingleElement(storage, address, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newSingleElement().
		return nil, nil, err
	}
	e.elems = append(e.elems, newElem)
	e.size += newElem.size

	return newElem.key, nil, nil
}

func (e *singleElements) Remove(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	if level != digester.Levels() {
		return nil, nil, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search by key
	for i, elem := range e.elems {

		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}

		if equal {
			// Remove this element
			copy(e.elems[i:], e.elems[i+1:])
			// Zero out last element to prevent memory leak
			e.elems[len(e.elems)-1] = nil
			// Reslice elements
			e.elems = e.elems[:len(e.elems)-1]

			// Adjust size
			e.size -= elem.Size()

			return elem.key, elem.value, nil
		}
	}

	return nil, nil, NewKeyNotFoundError(key)
}

func (e *singleElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, NewIndexOutOfBoundsError(uint64(i), 0, uint64(len(e.elems)))
	}
	return e.elems[i], nil
}

func (e *singleElements) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {

	// Iterate and reset elements backwards
	for i := len(e.elems) - 1; i >= 0; i-- {
		elem := e.elems[i]

		err := elem.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by singleElement.PopIterate().
			return err
		}
	}

	// Reset data slab
	e.elems = nil
	e.size = singleElementsPrefixSize

	return nil
}

// Slab operations (split, merge, and lend/borrow)

func (e *singleElements) Merge(_ elements) error {
	return NewNotApplicableError("singleElements", "elements", "Merge")
}

func (e *singleElements) Split() (elements, elements, error) {
	return nil, nil, NewNotApplicableError("singleElements", "elements", "Split")
}

func (e *singleElements) LendToRight(_ elements) error {
	return NewNotApplicableError("singleElements", "elements", "LendToRight")
}

func (e *singleElements) BorrowFromRight(_ elements) error {
	return NewNotApplicableError("singleElements", "elements", "BorrowFromRight")
}

func (e *singleElements) CanLendToLeft(_ uint32) bool {
	return false
}

func (e *singleElements) CanLendToRight(_ uint32) bool {
	return false
}

// Other operations

func (e *singleElements) hasPointer() bool {
	for _, elem := range e.elems {
		if elem.hasPointer() {
			return true
		}
	}
	return false
}

func (e *singleElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *singleElements) firstKey() Digest {
	return 0
}

func (e *singleElements) Size() uint32 {
	return e.size
}

func (e *singleElements) String() string {
	var s []string

	for i := 0; i < len(e.elems); i++ {
		s = append(s, fmt.Sprintf(":%s", e.elems[i].String()))
	}

	return strings.Join(s, " ")
}
