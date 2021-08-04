/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"bytes"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

type Storable interface {
	Encode(*Encoder) error

	ByteSize() uint32

	StoredValue(storage SlabStorage) (Value, error)
}

const (
	flagMetaDataSlab byte = 0b00000001
	flagDataSlab     byte = 0b00000010
	flagArray        byte = 0b00000100
	flagStorable     byte = 0b10000000
)

const CBORTagStorageID = 255

type StorageIDStorable StorageID

var _ Storable = StorageIDStorable{}

func (v StorageIDStorable) StoredValue(storage SlabStorage) (Value, error) {
	id := StorageID(v)
	if err := id.Valid(); err != nil {
		return nil, err
	}

	slab, found, err := storage.Retrieve(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("slab %d not found", id)
	}
	return slab.StoredValue(storage)
}

// Encode encodes StorageIDStorable as
// cbor.Tag{
//		Number:  cborTagStorageID,
//		Content: byte(v),
// }
func (v StorageIDStorable) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagStorageID,
	})
	if err != nil {
		return err
	}

	copy(enc.Scratch[:], v.address[:])
	copy(enc.Scratch[8:], v.index[:])

	return enc.CBOR.EncodeBytes(enc.Scratch[:storageIDSize])
}

func (v StorageIDStorable) ByteSize() uint32 {
	// tag number (2 bytes) + byte string header (1 byte) + storage id (16 bytes)
	return 2 + 1 + storageIDSize
}

func (v StorageIDStorable) String() string {
	return fmt.Sprintf("StorageIDStorable(%d)", v)
}

// NonStorable represents a value that cannot be stored
//
type NonStorable struct {
	Value Value
}

var _ Storable = NonStorable{}

func (n NonStorable) Encode(_ *Encoder) error {
	return fmt.Errorf("value is non-storable")
}

func (n NonStorable) ByteSize() uint32 {
	return 1
}

func (n NonStorable) StoredValue(_ SlabStorage) (Value, error) {
	return n.Value, nil
}

// Encode is a wrapper for Storable.Encode()
func Encode(storable Storable, storage SlabStorage) ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, storage)

	err := storable.Encode(enc)
	if err != nil {
		return nil, err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeStorageIDStorable(dec *cbor.StreamDecoder) (Storable, error) {
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, err
	}

	if len(b) != storageIDSize {
		return nil, fmt.Errorf("invalid storage id buffer length %d", len(b))
	}

	var address Address
	copy(address[:], b)

	var index StorageIndex
	copy(index[:], b[8:])

	id := NewStorageID(address, index)
	return StorageIDStorable(id), nil
}
