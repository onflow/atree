/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	flagBasicArray byte = 0x80

	basicArrayDataSlabPrefixSize = 1 + 8
)

type BasicArrayDataSlab struct {
	header   SlabHeader
	elements []Storable
}

type BasicArray struct {
	storage SlabStorage
	root    *BasicArrayDataSlab
}

func NewBasicArrayDataSlab() *BasicArrayDataSlab {
	return &BasicArrayDataSlab{
		header: SlabHeader{
			id:   generateStorageID(),
			size: basicArrayDataSlabPrefixSize,
		},
	}
}

func newBasicArrayDataSlabFromData(id StorageID, data []byte) (*BasicArrayDataSlab, error) {
	if len(data) == 0 {
		return nil, errors.New("data is too short for basic array")
	}

	// Check flag
	if data[0]&flagBasicArray == 0 {
		return nil, fmt.Errorf("data has invalid flag 0x%x, want 0x%x", data[0], flagBasicArray)
	}

	cborDec := NewByteStreamDecoder(data[1:])

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, err
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec)
		if err != nil {
			return nil, err
		}
		elements[i] = storable
	}

	return &BasicArrayDataSlab{
		header:   SlabHeader{id: id, size: uint32(len(data)), count: uint32(elemCount)},
		elements: elements,
	}, nil
}

func (array *BasicArrayDataSlab) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := array.Encode(enc)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (array *BasicArrayDataSlab) Encode(enc *Encoder) error {
	// Encode flag
	enc.Write([]byte{flagBasicArray})

	// Encode CBOR array size for 9 bytes
	enc.scratch[0] = 0x80 | 27
	binary.BigEndian.PutUint64(enc.scratch[1:], uint64(len(array.elements)))

	enc.Write(enc.scratch[:9])

	for i := 0; i < len(array.elements); i++ {
		err := array.elements[i].Encode(enc)
		if err != nil {
			return err
		}
	}
	enc.cbor.Flush()

	return nil
}

func (array *BasicArrayDataSlab) Get(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(array.elements)) {
		return nil, fmt.Errorf("out of bounds")
	}
	v := array.elements[index]
	return v, nil
}

func (array *BasicArrayDataSlab) Set(storage SlabStorage, index uint64, v Storable) error {
	if index >= uint64(len(array.elements)) {
		return fmt.Errorf("out of bounds")
	}

	oldElem := array.elements[index]

	array.elements[index] = v

	array.header.size = array.header.size - oldElem.ByteSize() + v.ByteSize()

	storage.Store(array.header.id, array)

	return nil
}

func (array *BasicArrayDataSlab) Insert(storage SlabStorage, index uint64, v Storable) error {
	if index > uint64(len(array.elements)) {
		return fmt.Errorf("out of bounds")
	}

	if index == uint64(len(array.elements)) {
		array.elements = append(array.elements, v)
	} else {
		array.elements = append(array.elements, nil)
		copy(array.elements[index+1:], array.elements[index:])
		array.elements[index] = v
	}

	array.header.count++
	array.header.size += v.ByteSize()

	storage.Store(array.header.id, array)

	return nil
}

func (array *BasicArrayDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(array.elements)) {
		return nil, fmt.Errorf("out of bounds")
	}

	v := array.elements[index]

	switch index {
	case 0:
		array.elements = array.elements[1:]
	case uint64(len(array.elements)) - 1:
		array.elements = array.elements[:len(array.elements)-1]
	default:
		copy(array.elements[index:], array.elements[index+1:])
		array.elements = array.elements[:len(array.elements)-1]
	}

	array.header.count--
	array.header.size -= v.ByteSize()

	storage.Store(array.header.id, array)

	return v, nil
}

func (array *BasicArrayDataSlab) Count() uint64 {
	return uint64(len(array.elements))
}

func (array *BasicArrayDataSlab) Header() SlabHeader {
	return array.header
}

func (array *BasicArrayDataSlab) ByteSize() uint32 {
	return array.header.size
}

func (array *BasicArrayDataSlab) ID() StorageID {
	return array.header.id
}

func (array *BasicArrayDataSlab) Mutable() bool {
	return true
}

func (array *BasicArrayDataSlab) String() string {
	return fmt.Sprintf("%v", array.elements)
}

func (array *BasicArrayDataSlab) Split() (Slab, Slab, error) {
	return nil, nil, errors.New("not applicable")
}

func (array *BasicArrayDataSlab) Merge(Slab) error {
	return errors.New("not applicable")
}

func (array *BasicArrayDataSlab) LendToRight(Slab) error {
	return errors.New("not applicable")
}

func (array *BasicArrayDataSlab) BorrowFromRight(Slab) error {
	return errors.New("not applicable")
}

func NewBasicArray(storage SlabStorage) *BasicArray {
	return &BasicArray{
		storage: storage,
		root:    NewBasicArrayDataSlab(),
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

func (array *BasicArray) Get(index uint64) (Storable, error) {
	return array.root.Get(array.storage, index)
}

func (array *BasicArray) Set(index uint64, v Storable) error {
	return array.root.Set(array.storage, index, v)
}

func (array *BasicArray) Append(v Storable) error {
	index := uint64(array.root.header.count)
	return array.Insert(index, v)
}

func (array *BasicArray) Insert(index uint64, v Storable) error {
	return array.root.Insert(array.storage, index, v)
}

func (array *BasicArray) Remove(index uint64) (Storable, error) {
	return array.root.Remove(array.storage, index)
}

func (array *BasicArray) Count() uint64 {
	return array.root.Count()
}

func (array *BasicArray) String() string {
	return array.root.String()
}
