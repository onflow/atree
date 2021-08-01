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

	ByteSize(storage SlabStorage) uint32

	StoredValue(storage SlabStorage) (Value, error)
}

const (
	flagMetaDataSlab byte = 0b00000001
	flagDataSlab     byte = 0b00000010
	flagArray        byte = 0b00000100
	flagStorable     byte = 0b00001000
)

const CBORTagStorageID = 255

type StorageIDStorable StorageID

var _ Storable = StorageIDStorable(0)

func (v StorageIDStorable) StoredValue(storage SlabStorage) (Value, error) {
	id := StorageID(v)
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
	return slab.StoredValue(storage)
}

// Encode encodes StorageIDStorable as
// cbor.Tag{
//		Number:  cborTagStorageID,
//		Content: uint64(v),
// }
func (v StorageIDStorable) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagStorageID,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint64(uint64(v))
}

// TODO: cache size
func (v StorageIDStorable) ByteSize(_ SlabStorage) uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + GetUintCBORSize(uint64(v))
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

func (n NonStorable) ByteSize(_ SlabStorage) uint32 {
	return 1
}

func (n NonStorable) StoredValue(_ SlabStorage) (Value, error) {
	return n.Value, nil
}

// Encode is a wrapper for Storable.Encode()
func Encode(storable Storable, storage SlabStorage) ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf, storage)

	err := storable.Encode(enc)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeStorageIDStorable(dec *cbor.StreamDecoder) (Storable, error) {
	n, err := dec.DecodeUint64()
	if err != nil {
		return nil, err
	}
	return StorageIDStorable(n), nil
}