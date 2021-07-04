/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"fmt"
)

type Storable interface {
	Encode(*Encoder) error

	ByteSize() uint32
	ID() StorageID

	Mutable() bool

	String() string
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

// TODO: cache size
func (v Uint8Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
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

// TODO: cache size
func (v Uint16Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
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

// TODO: cache size
func (v Uint32Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
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

// TODO: cache size
func (v Uint64Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
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

type StorageIDValue StorageID

// Encode encodes StorageIDValue as
// cbor.Tag{
//		Number:  cborTagStorageID,
//		Content: uint64(v),
// }
func (v StorageIDValue) Encode(enc *Encoder) error {
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
func (v StorageIDValue) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + getUintCBORSize(uint64(v))
}

func (v StorageIDValue) Mutable() bool {
	return false
}

func (v StorageIDValue) ID() StorageID {
	return StorageID(v)
}

func (v StorageIDValue) String() string {
	return fmt.Sprintf("id(%d)", v)
}
