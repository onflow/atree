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
)

// elements is a list of elements.
type elements interface {
	fmt.Stringer

	getElementAndNextKey(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, MapKey, error)

	Get(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

	Set(
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
	) (MapKey, MapValue, error)

	Remove(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

	Merge(elements) error
	Split() (elements, elements, error)

	LendToRight(elements) error
	BorrowFromRight(elements) error

	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	Element(int) (element, error)

	Encode(*Encoder) error

	hasPointer() bool

	firstKey() Digest

	Count() uint32

	Size() uint32

	PopIterate(SlabStorage, MapPopIterationFunc) error
}

func firstKeyInMapSlab(storage SlabStorage, slab MapSlab) (MapKey, error) {
	dataSlab, err := firstMapDataSlab(storage, slab)
	if err != nil {
		return nil, err
	}
	return firstKeyInElements(storage, dataSlab.elements)
}

func firstKeyInElements(storage SlabStorage, elems elements) (MapKey, error) {
	switch elements := elems.(type) {
	case *hkeyElements:
		if len(elements.elems) == 0 {
			return nil, nil
		}
		firstElem := elements.elems[0]
		return firstKeyInElement(storage, firstElem)

	case *singleElements:
		if len(elements.elems) == 0 {
			return nil, nil
		}
		firstElem := elements.elems[0]
		return firstElem.key, nil

	default:
		return nil, NewUnreachableError()
	}
}

func firstKeyInElement(storage SlabStorage, elem element) (MapKey, error) {
	switch elem := elem.(type) {
	case *singleElement:
		return elem.key, nil

	case elementGroup:
		group, err := elem.Elements(storage)
		if err != nil {
			return nil, err
		}
		return firstKeyInElements(storage, group)

	default:
		return nil, NewUnreachableError()
	}
}

func elementsStorables(elems elements, childStorables []Storable) []Storable {

	switch v := elems.(type) {

	case *hkeyElements:
		for i := range v.elems {
			childStorables = elementStorables(v.elems[i], childStorables)
		}

	case *singleElements:
		for i := range v.elems {
			childStorables = elementStorables(v.elems[i], childStorables)
		}

	}

	return childStorables
}

func elementStorables(e element, childStorables []Storable) []Storable {

	switch v := e.(type) {

	case *externalCollisionGroup:
		return append(childStorables, SlabIDStorable(v.slabID))

	case *inlineCollisionGroup:
		return elementsStorables(v.elements, childStorables)

	case *singleElement:
		return append(childStorables, v.key, v.value)
	}

	panic(NewUnreachableError())
}
