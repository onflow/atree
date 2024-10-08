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

package main

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/onflow/atree"

	"github.com/fxamacker/cbor/v2"
)

// This file is mostly from github.com/onflow/atree/storable_test.go
// This file contains value implementations for testing purposes.

const (
	reservedMinTagNum                 = 161
	reservedMinTagNumForContainerType = 230
	reservedMaxTagNum                 = 239
)

const (
	// CBOR tag numbers used to encode elements.

	cborTagUInt8Value  = 161
	cborTagUInt16Value = 162
	cborTagUInt32Value = 163
	cborTagUInt64Value = 164

	// CBOR tag numbers in this block cannot exceed 230 (reservedMinTagNumForContainerType).
)

const (
	// CBOR tag numbers used to encode container types.
	// Replace _ when new tag number is needed (use lower tag numbers first).

	arrayTypeTagNum = reservedMinTagNumForContainerType + iota
	compositeTypeTagNum
	mapTypeTagNum
	_
	_
	_
	_
	_
	_
	_
)

func init() {
	// Check if the CBOR tag number range is reserved for internal use by atree.
	// Smoke tests must only use available (unreserved by atree) CBOR tag numbers
	// to encode elements in atree managed containers.

	// As of Aug 15, 2024:
	// - Atree reserves CBOR tag numbers [240, 255] for atree internal use.
	// - Smoke tests reserve CBOR tag numbers [161, 239] to encode elements.

	tagNumOK, err := atree.IsCBORTagNumberRangeAvailable(reservedMinTagNum, reservedMaxTagNum)
	if err != nil {
		panic(err)
	}

	if !tagNumOK {
		atreeMinTagNum, atreeMaxTagNum := atree.ReservedCBORTagNumberRange()
		panic(fmt.Errorf(
			"smoke test tag numbers [%d, %d] overlaps with atree internal tag numbers [%d, %d]",
			reservedMinTagNum,
			reservedMaxTagNum,
			atreeMinTagNum,
			atreeMaxTagNum))
	}
}

type Uint8Value uint8

var _ atree.Value = Uint8Value(0)
var _ atree.Storable = Uint8Value(0)

func (v Uint8Value) ChildStorables() []atree.Storable {
	return nil
}

func (v Uint8Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint8Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint64) (atree.Storable, error) {
	return v, nil
}

// Encode encodes UInt8Value as
//
//	cbor.Tag{
//			Number:  cborTagUInt8Value,
//			Content: uint8(v),
//	}
func (v Uint8Value) Encode(enc *atree.Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt8Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint8(uint8(v))
}

func (v Uint8Value) getHashInput(scratch []byte) ([]byte, error) {

	const cborTypePositiveInt = 0x00

	buf := scratch
	if len(scratch) < 4 {
		buf = make([]byte, 4)
	}

	buf[0], buf[1] = 0xd8, cborTagUInt8Value // Tag number

	if v <= 23 {
		buf[2] = cborTypePositiveInt | byte(v)
		return buf[:3], nil
	}

	buf[2] = cborTypePositiveInt | byte(24)
	buf[3] = byte(v)
	return buf[:4], nil
}

// TODO: cache size
func (v Uint8Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint8Value) String() string {
	return fmt.Sprintf("%d", uint8(v))
}

type Uint16Value uint16

var _ atree.Value = Uint16Value(0)
var _ atree.Storable = Uint16Value(0)

func (v Uint16Value) ChildStorables() []atree.Storable {
	return nil
}

func (v Uint16Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint16Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint64) (atree.Storable, error) {
	return v, nil
}

func (v Uint16Value) Encode(enc *atree.Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt16Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint16(uint16(v))
}

func (v Uint16Value) getHashInput(scratch []byte) ([]byte, error) {
	const cborTypePositiveInt = 0x00

	buf := scratch
	if len(buf) < 8 {
		buf = make([]byte, 8)
	}

	buf[0], buf[1] = 0xd8, cborTagUInt16Value // Tag number

	if v <= 23 {
		buf[2] = cborTypePositiveInt | byte(v)
		return buf[:3], nil
	}

	if v <= math.MaxUint8 {
		buf[2] = cborTypePositiveInt | byte(24)
		buf[3] = byte(v)
		return buf[:4], nil
	}

	buf[2] = cborTypePositiveInt | byte(25)
	binary.BigEndian.PutUint16(buf[3:], uint16(v))
	return buf[:5], nil
}

// TODO: cache size
func (v Uint16Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint16Value) String() string {
	return fmt.Sprintf("%d", uint16(v))
}

type Uint32Value uint32

var _ atree.Value = Uint32Value(0)
var _ atree.Storable = Uint32Value(0)

func (v Uint32Value) ChildStorables() []atree.Storable {
	return nil
}

func (v Uint32Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint32Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint64) (atree.Storable, error) {
	return v, nil
}

// Encode encodes UInt32Value as
//
//	cbor.Tag{
//			Number:  cborTagUInt32Value,
//			Content: uint32(v),
//	}
func (v Uint32Value) Encode(enc *atree.Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt32Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint32(uint32(v))
}

func (v Uint32Value) getHashInput(scratch []byte) ([]byte, error) {

	const cborTypePositiveInt = 0x00

	buf := scratch
	if len(buf) < 8 {
		buf = make([]byte, 8)
	}

	buf[0], buf[1] = 0xd8, cborTagUInt32Value // Tag number

	if v <= 23 {
		buf[2] = cborTypePositiveInt | byte(v)
		return buf[:3], nil
	}

	if v <= math.MaxUint8 {
		buf[2] = cborTypePositiveInt | byte(24)
		buf[3] = byte(v)
		return buf[:4], nil
	}

	if v <= math.MaxUint16 {
		buf[2] = cborTypePositiveInt | byte(25)
		binary.BigEndian.PutUint16(buf[3:], uint16(v))
		return buf[:5], nil
	}

	buf[2] = cborTypePositiveInt | byte(26)
	binary.BigEndian.PutUint32(buf[3:], uint32(v))
	return buf[:7], nil
}

// TODO: cache size
func (v Uint32Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint32Value) String() string {
	return fmt.Sprintf("%d", uint32(v))
}

type Uint64Value uint64

var _ atree.Value = Uint64Value(0)
var _ atree.Storable = Uint64Value(0)

func (v Uint64Value) ChildStorables() []atree.Storable {
	return nil
}

func (v Uint64Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint64Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint64) (atree.Storable, error) {
	return v, nil
}

// Encode encodes UInt64Value as
//
//	cbor.Tag{
//			Number:  cborTagUInt64Value,
//			Content: uint64(v),
//	}
func (v Uint64Value) Encode(enc *atree.Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt64Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint64(uint64(v))
}

func (v Uint64Value) getHashInput(scratch []byte) ([]byte, error) {
	const cborTypePositiveInt = 0x00

	buf := scratch
	if len(buf) < 16 {
		buf = make([]byte, 16)
	}

	buf[0], buf[1] = 0xd8, cborTagUInt64Value // Tag number

	if v <= 23 {
		buf[2] = cborTypePositiveInt | byte(v)
		return buf[:3], nil
	}

	if v <= math.MaxUint8 {
		buf[2] = cborTypePositiveInt | byte(24)
		buf[3] = byte(v)
		return buf[:4], nil
	}

	if v <= math.MaxUint16 {
		buf[2] = cborTypePositiveInt | byte(25)
		binary.BigEndian.PutUint16(buf[3:], uint16(v))
		return buf[:5], nil
	}

	if v <= math.MaxUint32 {
		buf[2] = cborTypePositiveInt | byte(26)
		binary.BigEndian.PutUint32(buf[3:], uint32(v))
		return buf[:7], nil
	}

	buf[2] = cborTypePositiveInt | byte(27)
	binary.BigEndian.PutUint64(buf[3:], uint64(v))
	return buf[:11], nil
}

// TODO: cache size
func (v Uint64Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

type StringValue struct {
	str  string
	size uint32
}

var _ atree.Value = &StringValue{}
var _ atree.Storable = &StringValue{}

func NewStringValue(s string) StringValue {
	size := atree.GetUintCBORSize(uint64(len(s))) + uint32(len(s))
	return StringValue{str: s, size: size}
}

func (v StringValue) ChildStorables() []atree.Storable {
	return nil
}

func (v StringValue) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v StringValue) Storable(storage atree.SlabStorage, address atree.Address, maxInlineSize uint64) (atree.Storable, error) {
	if uint64(v.ByteSize()) <= maxInlineSize {
		return v, nil
	}

	return atree.NewStorableSlab(storage, address, v)
}

func (v StringValue) Encode(enc *atree.Encoder) error {
	return enc.CBOR.EncodeString(v.str)
}

func (v StringValue) getHashInput(scratch []byte) ([]byte, error) {

	const cborTypeTextString = 0x60

	buf := scratch
	if uint32(len(buf)) < v.size {
		buf = make([]byte, v.size)
	} else {
		buf = buf[:v.size]
	}

	slen := len(v.str)

	if slen <= 23 {
		buf[0] = cborTypeTextString | byte(slen)
		copy(buf[1:], v.str)
		return buf, nil
	}

	if slen <= math.MaxUint8 {
		buf[0] = cborTypeTextString | byte(24)
		buf[1] = byte(slen)
		copy(buf[2:], v.str)
		return buf, nil
	}

	if slen <= math.MaxUint16 {
		buf[0] = cborTypeTextString | byte(25)
		binary.BigEndian.PutUint16(buf[1:], uint16(slen))
		copy(buf[3:], v.str)
		return buf, nil
	}

	if slen <= math.MaxUint32 {
		buf[0] = cborTypeTextString | byte(26)
		binary.BigEndian.PutUint32(buf[1:], uint32(slen))
		copy(buf[5:], v.str)
		return buf, nil
	}

	buf[0] = cborTypeTextString | byte(27)
	binary.BigEndian.PutUint64(buf[1:], uint64(slen))
	copy(buf[9:], v.str)
	return buf, nil
}

func (v StringValue) ByteSize() uint32 {
	return v.size
}

func (v StringValue) String() string {
	return v.str
}

func decodeStorable(dec *cbor.StreamDecoder, id atree.SlabID, inlinedExtraData []atree.ExtraData) (atree.Storable, error) {
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

		case atree.CBORTagInlinedArray:
			return atree.DecodeInlinedArrayStorable(dec, decodeStorable, id, inlinedExtraData)

		case atree.CBORTagInlinedMap:
			return atree.DecodeInlinedMapStorable(dec, decodeStorable, id, inlinedExtraData)

		case atree.CBORTagInlinedCompactMap:
			return atree.DecodeInlinedCompactMapStorable(dec, decodeStorable, id, inlinedExtraData)

		case atree.CBORTagSlabID:
			return atree.DecodeSlabIDStorable(dec)

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

func compare(storage atree.SlabStorage, value atree.Value, storable atree.Storable) (bool, error) {
	switch v := value.(type) {

	case Uint8Value:
		other, ok := storable.(Uint8Value)
		if !ok {
			return false, nil
		}
		return uint8(other) == uint8(v), nil

	case Uint16Value:
		other, ok := storable.(Uint16Value)
		if !ok {
			return false, nil
		}
		return uint16(other) == uint16(v), nil

	case Uint32Value:
		other, ok := storable.(Uint32Value)
		if !ok {
			return false, nil
		}
		return uint32(other) == uint32(v), nil

	case Uint64Value:
		other, ok := storable.(Uint64Value)
		if !ok {
			return false, nil
		}
		return uint64(other) == uint64(v), nil

	case StringValue:
		other, ok := storable.(StringValue)
		if ok {
			return other.str == v.str, nil
		}

		// Retrieve value from storage
		otherValue, err := storable.StoredValue(storage)
		if err != nil {
			return false, err
		}
		other, ok = otherValue.(StringValue)
		if ok {
			return other.str == v.str, nil
		}

		return false, nil
	}

	return false, fmt.Errorf("value %T not supported for comparison", value)
}

func hashInputProvider(value atree.Value, buffer []byte) ([]byte, error) {
	switch v := value.(type) {

	case Uint8Value:
		return v.getHashInput(buffer)

	case Uint16Value:
		return v.getHashInput(buffer)

	case Uint32Value:
		return v.getHashInput(buffer)

	case Uint64Value:
		return v.getHashInput(buffer)

	case StringValue:
		return v.getHashInput(buffer)
	}

	return nil, fmt.Errorf("value %T not supported for hash input", value)
}
