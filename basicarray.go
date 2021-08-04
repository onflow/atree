/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

const (
	flagBasicArray byte = 0x80

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

func (a *BasicArray) DeepCopy(storage SlabStorage) (Value, error) {
	result := NewBasicArray(storage)

	for i, element := range a.root.elements {
		value, err := element.StoredValue(storage)
		if err != nil {
			return nil, err
		}

		valueCopy, err := value.DeepCopy(storage)
		if err != nil {
			return nil, err
		}

		err = result.Insert(uint64(i), valueCopy)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (a *BasicArray) Storable(SlabStorage) Storable {
	return a.root
}

func NewBasicArrayDataSlab(storage SlabStorage) *BasicArrayDataSlab {
	return &BasicArrayDataSlab{
		header: ArraySlabHeader{
			id:   storage.GenerateStorageID(),
			size: basicArrayDataSlabPrefixSize,
		},
	}
}

func newBasicArrayDataSlabFromData(
	storage SlabStorage,
	id StorageID,
	data []byte,
	decodeStorable StorableDecoder,
) (
	*BasicArrayDataSlab,
	error,
) {
	if len(data) == 0 {
		return nil, errors.New("data is too short for basic array")
	}

	// Check flag
	if data[0]&flagBasicArray == 0 {
		return nil, fmt.Errorf("data has invalid flag 0x%x, want 0x%x", data[0], flagBasicArray)
	}

	cborDec := cbor.NewByteStreamDecoder(data[1:])

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, err
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec, storage)
		if err != nil {
			return nil, err
		}
		elements[i] = storable
	}

	return &BasicArrayDataSlab{
		header:   ArraySlabHeader{id: id, size: uint32(len(data)), count: uint32(elemCount)},
		elements: elements,
	}, nil
}

func (a *BasicArrayDataSlab) Encode(enc *Encoder) error {
	// Encode flag
	_, err := enc.Write([]byte{flagBasicArray})
	if err != nil {
		return err
	}

	// Encode CBOR array size for 9 bytes
	enc.Scratch[0] = 0x80 | 27
	binary.BigEndian.PutUint64(enc.Scratch[1:], uint64(len(a.elements)))

	_, err = enc.Write(enc.Scratch[:9])
	if err != nil {
		return err
	}

	for i := 0; i < len(a.elements); i++ {
		err := a.elements[i].Encode(enc)
		if err != nil {
			return err
		}
	}
	err = enc.CBOR.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (a *BasicArrayDataSlab) Get(_ SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, fmt.Errorf("out of bounds")
	}
	v := a.elements[index]
	return v, nil
}

func (a *BasicArrayDataSlab) Set(storage SlabStorage, index uint64, v Storable) error {
	if index >= uint64(len(a.elements)) {
		return fmt.Errorf("out of bounds")
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
		return fmt.Errorf("out of bounds")
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
		return nil, fmt.Errorf("out of bounds")
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
	return nil, nil, errors.New("not applicable")
}

func (a *BasicArrayDataSlab) Merge(_ Slab) error {
	return errors.New("not applicable")
}

func (a *BasicArrayDataSlab) LendToRight(_ Slab) error {
	return errors.New("not applicable")
}

func (a *BasicArrayDataSlab) BorrowFromRight(_ Slab) error {
	return errors.New("not applicable")
}

func NewBasicArray(storage SlabStorage) *BasicArray {
	return &BasicArray{
		storage: storage,
		root:    NewBasicArrayDataSlab(storage),
	}
}

func (a *BasicArray) StorageID() StorageID {
	return a.root.ID()
}

func NewBasicArrayWithRootID(storage SlabStorage, id StorageID) (*BasicArray, error) {
	if id == StorageIDUndefined {
		return nil, fmt.Errorf("invalid storage id")
	}
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("slab %d not found", id)
	}
	dataSlab, ok := slab.(*BasicArrayDataSlab)
	if !ok {
		return nil, fmt.Errorf("slab %d is not BasicArrayDataSlab", id)
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
	storable := v.Storable(a.storage)
	return a.root.Set(a.storage, index, storable)
}

func (a *BasicArray) Append(v Value) error {
	index := uint64(a.root.header.count)
	return a.Insert(index, v)
}

func (a *BasicArray) Insert(index uint64, v Value) error {
	storable := v.Storable(a.storage)
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
