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
	"reflect"
)

// ByteStorable is a type constraint that permits any type that
// implements Storable interface and has byte underlying type.
type ByteStorable interface {
	~byte
	Storable
}

// ByteArrayToByteSlice converts an array to []byte if the array elements are of ByteStorable type.
// ByteArrayToByteSlice returns UnexpectedElementTypeError error if any array element is not of ByteStorable type.
func ByteArrayToByteSlice[T ByteStorable](array *Array) ([]byte, error) {
	if array.Count() == 0 {
		return nil, nil
	}

	// Get first array data slab for traversal.
	slab, err := firstArrayDataSlab(array.Storage, array.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstArrayDataSlab().
		return nil, err
	}

	data := make([]byte, 0, array.Count())

	// Traverse array data slabs to construct []byte.
	for {
		for _, e := range slab.elements {
			b, ok := e.(T)
			if !ok {
				return nil, NewUnexpectedElementTypeError(reflect.TypeFor[T](), reflect.TypeOf(e))
			}
			data = append(data, byte(b))
		}

		if slab.next == SlabIDUndefined {
			break
		}

		nextSlab, err := getArraySlab(array.Storage, slab.next)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return nil, err
		}

		slab = nextSlab.(*ArrayDataSlab)
	}

	return data, nil
}

// ByteStorableValue is a type constraint that permits any type that
// implements Storable and Value interfaces, and has byte underlying type.
type ByteStorableValue interface {
	~byte
	Storable
	Value
}

const (
	byteStorableCBORTagSize  = 2
	byteStorableCBORDataSize = 2
)

// ByteSliceToByteArray converts []byte to an array.
func ByteSliceToByteArray[T ByteStorableValue](
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	data []byte,
	estimatedByteStorableSize uint32,
) (*Array, error) {
	if len(data) == 0 {
		return NewArray(storage, address, typeInfo)
	}

	if estimatedByteStorableSize == 0 {
		estimatedByteStorableSize = byteStorableCBORTagSize + byteStorableCBORDataSize
	}

	estimatedEncodedDataSize := int(estimatedByteStorableSize) * len(data)

	// If data can fit into a single data slab, create a new array with root data slab directly.
	if estimatedEncodedDataSize+arrayRootDataSlabPrefixSize < int(targetThreshold) {

		elementSize := uint32(0)
		elements := make([]Storable, len(data))
		for i, b := range data {
			e := T(b)
			elements[i] = e
			elementSize += e.ByteSize()
		}

		if elementSize+arrayRootDataSlabPrefixSize < targetThreshold {
			return newArrayWithElements(storage, address, typeInfo, elements, elementSize)
		}
	}

	// Since data is too large to fit into a single data slab, call
	// NewArrayFromBatchData() to create an array with multiple slabs.

	index := 0
	return NewArrayFromBatchData(
		storage,
		address,
		typeInfo,
		func() (Value, error) {
			if index == len(data) {
				return nil, nil
			}
			v := data[index]
			index++
			return T(v), nil
		})
}

func newArrayWithElements(
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	elements []Storable,
	elementSize uint32,
) (*Array, error) {
	// Create an empty array.
	array, err := NewArray(storage, address, typeInfo)
	if err != nil {
		return nil, err
	}

	// Modify array root slab to include elements.
	root := array.root.(*ArrayDataSlab)
	root.elements = elements
	root.header.count = uint32(len(elements))
	root.header.size += elementSize

	// This isn't needed because slab is already stored in the storage by pointer in NewArray(),
	// but it is added to be consistent with how other slab mutation functions are implemented.
	err = storeSlab(storage, root)
	if err != nil {
		return nil, err
	}

	return array, nil
}
