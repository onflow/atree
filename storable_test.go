/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"fmt"
	"math"

	"github.com/fxamacker/cbor/v2"
)

// This file contains value implementations for testing purposes

const (
	cborTagUInt8Value  = 161
	cborTagUInt16Value = 162
	cborTagUInt32Value = 163
	cborTagUInt64Value = 164
)

type Uint8Value uint8

var _ Value = Uint8Value(0)
var _ Storable = Uint8Value(0)

func (v Uint8Value) DeepCopy(_ SlabStorage, _ Address) (Value, error) {
	return v, nil
}

func (v Uint8Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint8Value) Storable(_ SlabStorage, _ Address) (Storable, error) {
	return v, nil
}

// Encode encodes UInt8Value as
// cbor.Tag{
//		Number:  cborTagUInt8Value,
//		Content: uint8(v),
// }
func (v Uint8Value) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt8Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint8(uint8(v))
}

// TODO: cache size
func (v Uint8Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint8Value) String() string {
	return fmt.Sprintf("%d", uint8(v))
}

type Uint16Value uint16

var _ Value = Uint16Value(0)
var _ Storable = Uint16Value(0)

func (v Uint16Value) DeepCopy(_ SlabStorage, _ Address) (Value, error) {
	return v, nil
}

func (v Uint16Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint16Value) Storable(_ SlabStorage, _ Address) (Storable, error) {
	return v, nil
}

func (v Uint16Value) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt16Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint16(uint16(v))
}

// TODO: cache size
func (v Uint16Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint16Value) String() string {
	return fmt.Sprintf("%d", uint16(v))
}

type Uint32Value uint32

var _ Value = Uint32Value(0)
var _ Storable = Uint32Value(0)

func (v Uint32Value) DeepCopy(_ SlabStorage, _ Address) (Value, error) {
	return v, nil
}

func (v Uint32Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint32Value) Storable(_ SlabStorage, _ Address) (Storable, error) {
	return v, nil
}

// Encode encodes UInt32Value as
// cbor.Tag{
//		Number:  cborTagUInt32Value,
//		Content: uint32(v),
// }
func (v Uint32Value) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt32Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint32(uint32(v))
}

// TODO: cache size
func (v Uint32Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint32Value) String() string {
	return fmt.Sprintf("%d", uint32(v))
}

type Uint64Value uint64

var _ Value = Uint64Value(0)
var _ Storable = Uint64Value(0)

func (v Uint64Value) DeepCopy(_ SlabStorage, _ Address) (Value, error) {
	return v, nil
}

func (v Uint64Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint64Value) Storable(_ SlabStorage, _ Address) (Storable, error) {
	return v, nil
}

// Encode encodes UInt64Value as
// cbor.Tag{
//		Number:  cborTagUInt64Value,
//		Content: uint64(v),
// }
func (v Uint64Value) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt64Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint64(uint64(v))
}

// TODO: cache size
func (v Uint64Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

type StringValue struct {
	str  string
	size uint32
}

var _ Value = &StringValue{}
var _ Storable = &StringValue{}

func NewStringValue(s string) *StringValue {
	size := GetUintCBORSize(uint64(len(s))) + uint32(len(s))
	return &StringValue{str: s, size: size}
}

func (v *StringValue) DeepCopy(_ SlabStorage, _ Address) (Value, error) {
	return v, nil
}

func (v *StringValue) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v *StringValue) Storable(storage SlabStorage, address Address) (Storable, error) {
	if v.ByteSize() > uint32(MaxInlineElementSize) {

		// Create StorableSlab
		id := storage.GenerateStorageID(address)
		slab := &StorableSlab{
			StorageID: id,
			Storable:  v,
		}

		// Store StorableSlab in storage
		err := storage.Store(id, slab)
		if err != nil {
			return nil, err
		}

		// Return storage id as storable
		return StorageIDStorable(id), nil
	}

	return v, nil
}

func (v *StringValue) Encode(enc *Encoder) error {
	return enc.CBOR.EncodeString(v.str)
}

func (v *StringValue) ByteSize() uint32 {
	return v.size
}

func (v *StringValue) String() string {
	return v.str
}

func decodeStorable(dec *cbor.StreamDecoder, _ StorageID) (Storable, error) {
	t, err := dec.NextType()
	if err != nil {
		return nil, err
	}

	switch t {
	case cbor.TextStringType:
		s, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		return NewStringValue(s), nil

	case cbor.TagType:
		tagNumber, err := dec.DecodeTagNumber()
		if err != nil {
			return nil, err
		}

		switch tagNumber {
		case CBORTagStorageID:
			return DecodeStorageIDStorable(dec)

		case cborTagUInt8Value:
			n, err := dec.DecodeUint64()
			if err != nil {
				return nil, err
			}
			if n > math.MaxUint8 {
				return nil, fmt.Errorf("invalid data, got %d, expected max %d", n, math.MaxUint8)
			}
			return Uint8Value(n), nil

		case cborTagUInt16Value:
			n, err := dec.DecodeUint64()
			if err != nil {
				return nil, err
			}
			if n > math.MaxUint16 {
				return nil, fmt.Errorf("invalid data, got %d, expected max %d", n, math.MaxUint16)
			}
			return Uint16Value(n), nil

		case cborTagUInt32Value:
			n, err := dec.DecodeUint64()
			if err != nil {
				return nil, err
			}
			if n > math.MaxUint32 {
				return nil, fmt.Errorf("invalid data, got %d, expected max %d", n, math.MaxUint32)
			}
			return Uint32Value(n), nil

		case cborTagUInt64Value:
			n, err := dec.DecodeUint64()
			if err != nil {
				return nil, err
			}
			return Uint64Value(n), nil

		default:
			return nil, fmt.Errorf("invalid tag number %d", tagNumber)
		}
	default:
		return nil, fmt.Errorf("invalid cbor type %s for storable", t)
	}
}
