/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"bytes"
	"fmt"
)

type Storable interface {
	Encode(*Encoder) error

	ByteSize() uint32
	ID() StorageID

	Mutable() bool

	String() string

	Value(storage SlabStorage) (Value, error)
}

const (
	flagMetaDataSlab byte = 0x01
	flagDataSlab     byte = 0x02
	flagArray        byte = 0x04
)

const (
	cborTagStorageID = 255

	cborTagUInt8Value  = 161
	cborTagUInt16Value = 162
	cborTagUInt32Value = 163
	cborTagUInt64Value = 164
)

type Uint8Value uint8

var _ Value = Uint8Value(0)
var _ Storable = Uint8Value(0)
var _ MapKey = Uint8Value(0)

func (v Uint8Value) DeepCopy(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint8Value) Value(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint8Value) Storable() Storable {
	return v
}

// Encode encodes UInt8Value as
// cbor.Tag{
//		Number:  cborTagUInt8Value,
//		Content: uint8(v),
// }
func (v Uint8Value) Encode(enc *Encoder) error {
	err := enc.cbor.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt8Value,
	})
	if err != nil {
		return err
	}
	return enc.cbor.EncodeUint8(uint8(v))
}

// TODO: cache encoded data and size
func (v Uint8Value) HashCode() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := v.Encode(enc)
	if err != nil {
		return nil, err
	}
	enc.cbor.Flush()
	return buf.Bytes(), nil
}

// TODO: cache size
func (v Uint8Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
}

func (v Uint8Value) Equal(other Storable) bool {
	otherUint8, ok := other.(Uint8Value)
	if !ok {
		return false
	}
	return uint8(otherUint8) == uint8(v)
}

func (v Uint8Value) Mutable() bool {
	return false
}

func (v Uint8Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint8Value) String() string {
	return fmt.Sprintf("%d", uint8(v))
}

type Uint16Value uint16

var _ Value = Uint16Value(0)
var _ Storable = Uint16Value(0)
var _ MapKey = Uint16Value(0)

func (v Uint16Value) DeepCopy(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint16Value) Value(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint16Value) Storable() Storable {
	return v
}

func (v Uint16Value) Encode(enc *Encoder) error {
	err := enc.cbor.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt16Value,
	})
	if err != nil {
		return err
	}
	return enc.cbor.EncodeUint16(uint16(v))
}

// TODO: cache encoded data and size
func (v Uint16Value) HashCode() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := v.Encode(enc)
	if err != nil {
		return nil, err
	}
	enc.cbor.Flush()
	return buf.Bytes(), nil
}

// TODO: cache size
func (v Uint16Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
}

func (v Uint16Value) Equal(other Storable) bool {
	otherUint16, ok := other.(Uint16Value)
	if !ok {
		return false
	}
	return uint16(otherUint16) == uint16(v)
}

func (v Uint16Value) Mutable() bool {
	return false
}

func (v Uint16Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint16Value) String() string {
	return fmt.Sprintf("%d", uint16(v))
}

type Uint32Value uint32

var _ Value = Uint32Value(0)
var _ Storable = Uint32Value(0)
var _ MapKey = Uint32Value(0)

func (v Uint32Value) DeepCopy(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint32Value) Value(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint32Value) Storable() Storable {
	return v
}

// Encode encodes UInt32Value as
// cbor.Tag{
//		Number:  cborTagUInt32Value,
//		Content: uint32(v),
// }
func (v Uint32Value) Encode(enc *Encoder) error {
	err := enc.cbor.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt32Value,
	})
	if err != nil {
		return err
	}
	return enc.cbor.EncodeUint32(uint32(v))
}

// TODO: cache encoded data and size
func (v Uint32Value) HashCode() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := v.Encode(enc)
	if err != nil {
		return nil, err
	}
	enc.cbor.Flush()
	return buf.Bytes(), nil
}

// TODO: cache size
func (v Uint32Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
}

func (v Uint32Value) Equal(other Storable) bool {
	otherUint32, ok := other.(Uint32Value)
	if !ok {
		return false
	}
	return uint32(otherUint32) == uint32(v)
}

func (v Uint32Value) Mutable() bool {
	return false
}

func (v Uint32Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint32Value) String() string {
	return fmt.Sprintf("%d", uint32(v))
}

type Uint64Value uint64

var _ Value = Uint64Value(0)
var _ Storable = Uint64Value(0)
var _ MapKey = Uint64Value(0)

func (v Uint64Value) DeepCopy(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint64Value) Value(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint64Value) Storable() Storable {
	return v
}

// Encode encodes UInt64Value as
// cbor.Tag{
//		Number:  cborTagUInt64Value,
//		Content: uint64(v),
// }
func (v Uint64Value) Encode(enc *Encoder) error {
	err := enc.cbor.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt64Value,
	})
	if err != nil {
		return err
	}
	return enc.cbor.EncodeUint64(uint64(v))
}

// TODO: cache encoded data and size
func (v Uint64Value) HashCode() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := v.Encode(enc)
	if err != nil {
		return nil, err
	}
	enc.cbor.Flush()
	return buf.Bytes(), nil
}

// TODO: cache size
func (v Uint64Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
}

func (v Uint64Value) Equal(other Storable) bool {
	otherUint64, ok := other.(Uint64Value)
	if !ok {
		return false
	}
	return uint64(otherUint64) == uint64(v)
}

func (v Uint64Value) Mutable() bool {
	return false
}

func (v Uint64Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

type StringValue struct {
	str  string
	size uint32
	id   StorageID // Don't create valid storage id until id is requested
}

var _ Value = &StringValue{}
var _ Storable = &StringValue{}
var _ MapKey = &StringValue{}

func NewStringValue(storage SlabStorage, s string) *StringValue {
	size := getUintCBORSize(uint64(len(s))) + uint32(len(s))
	return &StringValue{
		str:  s,
		size: size,
		id:   storage.GenerateStorageID(),
	}
}

func (v *StringValue) DeepCopy(storage SlabStorage) (Value, error) {
	return &StringValue{
		str:  v.str,
		size: v.size,
		id:   storage.GenerateStorageID(),
	}, nil
}

func (v *StringValue) Value(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v *StringValue) Storable() Storable {
	return v
}

func (v *StringValue) Encode(enc *Encoder) error {
	return enc.cbor.EncodeString(v.str)
}

// TODO: cache encoded data and size
func (v *StringValue) HashCode() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := v.Encode(enc)
	if err != nil {
		return nil, err
	}
	enc.cbor.Flush()
	return buf.Bytes(), nil
}

func (v *StringValue) ByteSize() uint32 {
	return v.size
}

func (v *StringValue) Mutable() bool {
	return false
}

func (v *StringValue) ID() StorageID {
	return v.id
}

func (v *StringValue) Equal(other Storable) bool {
	otherString, ok := other.(*StringValue)
	if !ok {
		return false
	}
	return otherString.str == v.str
}

func (v *StringValue) String() string {
	return v.str
}

type StorageIDStorable StorageID

var _ Storable = StorageIDStorable(0)

func (v StorageIDStorable) Value(storage SlabStorage) (Value, error) {
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
	return slab.Value(storage)
}

// Encode encodes StorageIDStorable as
// cbor.Tag{
//		Number:  cborTagStorageID,
//		Content: uint64(v),
// }
func (v StorageIDStorable) Encode(enc *Encoder) error {
	err := enc.cbor.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagStorageID,
	})
	if err != nil {
		return err
	}
	return enc.cbor.EncodeUint64(uint64(v))
}

// TODO: cache size
func (v StorageIDStorable) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
}

func (v StorageIDStorable) Mutable() bool {
	return false
}

func (v StorageIDStorable) ID() StorageID {
	return StorageID(v)
}

func (v StorageIDStorable) String() string {
	return fmt.Sprintf("id(%d)", v)
}

// Encode is a wrapper for Storable.Encode()
func Encode(storable Storable) ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := storable.Encode(enc)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
