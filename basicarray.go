/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2021 Dapper Labs, Inc.
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
	"encoding/binary"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

const (
	basicArrayDataSlabPrefixSize = 1 + 8
)

type BasicArrayDataSlab struct {
	header   ArraySlabHeader
	elements []Storable
}

func (a *BasicArrayDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	return &BasicArray{storage: storage, root: a}, nil
}

type BasicArray struct {
	storage SlabStorage
	root    *BasicArrayDataSlab
}

var _ Value = &BasicArray{}

func (a *BasicArray) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
	return a.root, nil
}

func newBasicArrayDataSlabFromData(
	id StorageID,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
) (
	*BasicArrayDataSlab,
	error,
) {
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for basic array slab")
	}

	// Check flag
	if getSlabArrayType(data[1]) != slabBasicArray {
		return nil, NewDecodingErrorf(
			"data has invalid flag 0x%x, want 0x%x",
			data[0],
			maskBasicArray,
		)
	}

	cborDec := decMode.NewByteStreamDecoder(data[2:])

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec, StorageIDUndefined)
		if err != nil {
			return nil, NewDecodingError(err)
		}
		elements[i] = storable
	}

	return &BasicArrayDataSlab{
		header:   ArraySlabHeader{id: id, size: uint32(len(data)), count: uint32(elemCount)},
		elements: elements,
	}, nil
}

func (a *BasicArrayDataSlab) Encode(enc *Encoder) error {

	flag := maskBasicArray | maskSlabRoot

	// Encode flag
	_, err := enc.Write([]byte{0x0, flag})
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode CBOR array size for 9 bytes
	enc.Scratch[0] = 0x80 | 27
	binary.BigEndian.PutUint64(enc.Scratch[1:], uint64(len(a.elements)))

	_, err = enc.Write(enc.Scratch[:9])
	if err != nil {
		return NewEncodingError(err)
	}

	for i := 0; i < len(a.elements); i++ {
		err := a.elements[i].Encode(enc)
		if err != nil {
			return NewEncodingError(err)
		}
	}
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (a *BasicArrayDataSlab) ChildStorables() []Storable {
	s := make([]Storable, len(a.elements))
	copy(s, a.elements)
	return s
}

func (a *BasicArrayDataSlab) Get(_ SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}
	v := a.elements[index]
	return v, nil
}

func (a *BasicArrayDataSlab) Set(storage SlabStorage, index uint64, v Storable) error {
	if index >= uint64(len(a.elements)) {
		return NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	oldElem := a.elements[index]

	a.elements[index] = v

	a.header.size = a.header.size -
		oldElem.ByteSize() +
		v.ByteSize()

	err := storage.Store(a.header.id, a)
	if err != nil {
		return err
	}

	return nil
}

func (a *BasicArrayDataSlab) Insert(storage SlabStorage, index uint64, v Storable) error {
	if index > uint64(len(a.elements)) {
		return NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	if index == uint64(len(a.elements)) {
		a.elements = append(a.elements, v)
	} else {
		a.elements = append(a.elements, nil)
		copy(a.elements[index+1:], a.elements[index:])
		a.elements[index] = v
	}

	a.header.count++
	a.header.size += v.ByteSize()

	err := storage.Store(a.header.id, a)
	if err != nil {
		return err
	}

	return nil
}

func (a *BasicArrayDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	v := a.elements[index]

	switch index {
	case 0:
		a.elements = a.elements[1:]
	case uint64(len(a.elements)) - 1:
		a.elements = a.elements[:len(a.elements)-1]
	default:
		copy(a.elements[index:], a.elements[index+1:])
		a.elements = a.elements[:len(a.elements)-1]
	}

	a.header.count--
	a.header.size -= v.ByteSize()

	err := storage.Store(a.header.id, a)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (a *BasicArrayDataSlab) Count() uint64 {
	return uint64(len(a.elements))
}

func (a *BasicArrayDataSlab) Header() ArraySlabHeader {
	return a.header
}

func (a *BasicArrayDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *BasicArrayDataSlab) ID() StorageID {
	return a.header.id
}

func (a *BasicArrayDataSlab) String() string {
	return fmt.Sprintf("%v", a.elements)
}

func (a *BasicArrayDataSlab) Split(_ SlabStorage) (Slab, Slab, error) {
	return nil, nil, NewNotApplicableError("BasicArrayDataSlab", "Slab", "Split")
}

func (a *BasicArrayDataSlab) Merge(_ Slab) error {
	return NewNotApplicableError("BasicArrayDataSlab", "Slab", "Merge")
}

func (a *BasicArrayDataSlab) LendToRight(_ Slab) error {
	return NewNotApplicableError("BasicArrayDataSlab", "Slab", "LendToRight")
}

func (a *BasicArrayDataSlab) BorrowFromRight(_ Slab) error {
	return NewNotApplicableError("BasicArrayDataSlab", "Slab", "BorrowFromRight")
}

func NewBasicArray(storage SlabStorage, address Address) (*BasicArray, error) {
	sID, err := storage.GenerateStorageID(address)
	if err != nil {
		return nil, err
	}

	root := &BasicArrayDataSlab{
		header: ArraySlabHeader{
			id:   sID,
			size: basicArrayDataSlabPrefixSize,
		},
	}

	return &BasicArray{
		storage: storage,
		root:    root,
	}, nil
}

func (a *BasicArray) StorageID() StorageID {
	return a.root.ID()
}

func (a *BasicArray) Address() Address {
	return a.StorageID().Address
}

func NewBasicArrayWithRootID(storage SlabStorage, id StorageID) (*BasicArray, error) {
	if id == StorageIDUndefined {
		return nil, NewStorageIDErrorf("cannot create BasicArray from undefined storage id")
	}
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "BasicArray slab not found")
	}
	dataSlab, ok := slab.(*BasicArrayDataSlab)
	if !ok {
		return nil, NewSlabDataErrorf("slab %s isn't BasicArraySlab", id)
	}
	return &BasicArray{storage: storage, root: dataSlab}, nil
}

func (a *BasicArray) Get(index uint64) (Value, error) {
	storable, err := a.root.Get(a.storage, index)
	if err != nil {
		return nil, err
	}
	return storable.StoredValue(a.storage)
}

func (a *BasicArray) Set(index uint64, v Value) error {
	storable, err := v.Storable(a.storage, a.Address(), MaxInlineArrayElementSize)
	if err != nil {
		return err
	}
	return a.root.Set(a.storage, index, storable)
}

func (a *BasicArray) Append(v Value) error {
	index := uint64(a.root.header.count)
	return a.Insert(index, v)
}

func (a *BasicArray) Insert(index uint64, v Value) error {
	storable, err := v.Storable(a.storage, a.Address(), MaxInlineArrayElementSize)
	if err != nil {
		return err
	}
	return a.root.Insert(a.storage, index, storable)
}

func (a *BasicArray) Remove(index uint64) (Value, error) {
	storable, err := a.root.Remove(a.storage, index)
	if err != nil {
		return nil, err
	}
	return storable.StoredValue(a.storage)
}

func (a *BasicArray) Count() uint64 {
	return a.root.Count()
}

func (a *BasicArray) String() string {
	return a.root.String()
}
