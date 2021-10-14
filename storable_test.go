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
	"math"
	"strconv"

	"github.com/fxamacker/cbor/v2"
)

// This file contains value implementations for testing purposes

const (
	cborTagUInt8Value  = 161
	cborTagUInt16Value = 162
	cborTagUInt32Value = 163
	cborTagUInt64Value = 164
	cborTagSomeValue   = 165
)

type HashableValue interface {
	Value
	HashInput(scratch []byte) ([]byte, error)
}

type Uint8Value uint8

var _ Value = Uint8Value(0)
var _ Storable = Uint8Value(0)
var _ HashableValue = Uint8Value(0)

func (v Uint8Value) ChildStorables() []Storable { return nil }

func (v Uint8Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint8Value) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
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

func (v Uint8Value) HashInput(scratch []byte) ([]byte, error) {

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
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint8Value) String() string {
	return fmt.Sprintf("%d", uint8(v))
}

func (v Uint8Value) KeyString() string {
	return strconv.FormatInt(int64(v), 10)
}

type Uint16Value uint16

var _ Value = Uint16Value(0)
var _ Storable = Uint16Value(0)
var _ HashableValue = Uint16Value(0)

func (v Uint16Value) ChildStorables() []Storable { return nil }

func (v Uint16Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint16Value) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
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

func (v Uint16Value) HashInput(scratch []byte) ([]byte, error) {
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
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint16Value) String() string {
	return fmt.Sprintf("%d", uint16(v))
}

func (v Uint16Value) KeyString() string {
	return strconv.FormatInt(int64(v), 10)
}

type Uint32Value uint32

var _ Value = Uint32Value(0)
var _ Storable = Uint32Value(0)
var _ HashableValue = Uint32Value(0)

func (v Uint32Value) ChildStorables() []Storable { return nil }

func (v Uint32Value) DeepCopy(_ SlabStorage, _ Address) (Value, error) {
	return v, nil
}

func (v Uint32Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint32Value) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
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

func (v Uint32Value) HashInput(scratch []byte) ([]byte, error) {

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
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint32Value) String() string {
	return fmt.Sprintf("%d", uint32(v))
}

func (v Uint32Value) KeyString() string {
	return strconv.FormatInt(int64(v), 10)
}

type Uint64Value uint64

var _ Value = Uint64Value(0)
var _ Storable = Uint64Value(0)
var _ HashableValue = Uint64Value(0)

func (v Uint64Value) ChildStorables() []Storable { return nil }

func (v Uint64Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint64Value) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
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

func (v Uint64Value) HashInput(scratch []byte) ([]byte, error) {
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
	return 2 + GetUintCBORSize(uint64(v))
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

func (v Uint64Value) KeyString() string {
	return strconv.FormatInt(int64(v), 10)
}

type StringValue struct {
	str  string
	size uint32
}

var _ Value = StringValue{}
var _ Storable = StringValue{}
var _ HashableValue = StringValue{}

func NewStringValue(s string) StringValue {
	size := GetUintCBORSize(uint64(len(s))) + uint32(len(s))
	return StringValue{str: s, size: size}
}

func (v StringValue) ChildStorables() []Storable { return nil }

func (v StringValue) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v StringValue) Storable(storage SlabStorage, address Address, maxInlineSize uint64) (Storable, error) {
	if uint64(v.ByteSize()) > maxInlineSize {

		// Create StorableSlab
		id, err := storage.GenerateStorageID(address)
		if err != nil {
			return nil, NewStorageError(err)
		}

		slab := &StorableSlab{
			StorageID: id,
			Storable:  v,
		}

		// Store StorableSlab in storage
		err = storage.Store(id, slab)
		if err != nil {
			return nil, err
		}

		// Return storage id as storable
		return StorageIDStorable(id), nil
	}

	return v, nil
}

func (v StringValue) Encode(enc *Encoder) error {
	return enc.CBOR.EncodeString(v.str)
}

func (v StringValue) HashInput(scratch []byte) ([]byte, error) {

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

func (v StringValue) KeyString() string {
	return v.str
}

func decodeStorable(dec *cbor.StreamDecoder, id StorageID) (Storable, error) {
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

		case cborTagSomeValue:
			storable, err := decodeStorable(dec, id)
			if err != nil {
				return nil, err
			}
			return SomeStorable{Storable: storable}, nil

		default:
			return nil, fmt.Errorf("invalid tag number %d", tagNumber)
		}
	default:
		return nil, fmt.Errorf("invalid cbor type %s for storable", t)
	}
}

func decodeTypeInfo(dec *cbor.StreamDecoder) (TypeInfo, error) {
	value, err := dec.DecodeUint64()
	if err != nil {
		return nil, err
	}

	return testTypeInfo{value: value}, nil
}

func compare(storage SlabStorage, value Value, storable Storable) (bool, error) {
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

	case SomeValue:
		other, ok := storable.(SomeStorable)
		if !ok {
			return false, nil
		}

		return compare(storage, v.Value, other.Storable)
	}

	return false, fmt.Errorf("value %T not supported for comparison", value)
}

func hashInputProvider(value Value, buffer []byte) ([]byte, error) {
	switch v := value.(type) {

	case Uint8Value:
		return v.HashInput(buffer)

	case Uint16Value:
		return v.HashInput(buffer)

	case Uint32Value:
		return v.HashInput(buffer)

	case Uint64Value:
		return v.HashInput(buffer)

	case StringValue:
		return v.HashInput(buffer)

	case SomeValue:
		return v.HashInput(buffer)
	}

	return nil, fmt.Errorf("value %T not supported for hash input", value)
}

type SomeValue struct {
	Value Value
}

var _ Value = SomeValue{}
var _ HashableValue = SomeValue{}

func (v SomeValue) Storable(storage SlabStorage, address Address, maxSize uint64) (Storable, error) {

	valueStorable, err := v.Value.Storable(
		storage,
		address,
		maxSize-2,
	)
	if err != nil {
		return nil, err
	}

	return SomeStorable{
		Storable: valueStorable,
	}, nil
}

func (v SomeValue) HashInput(scratch []byte) ([]byte, error) {

	wv, ok := v.Value.(HashableValue)
	if !ok {
		return nil, fmt.Errorf("failed to hash wrapped value: %s", v.Value)
	}

	b, err := wv.HashInput(scratch)
	if err != nil {
		return nil, err
	}

	hi := make([]byte, len(b)+2)
	hi[0] = 0xd8
	hi[1] = cborTagSomeValue
	copy(hi[2:], b)

	return hi, nil
}

func (v SomeValue) String() string {
	return fmt.Sprintf("%s", v.Value)
}

type SomeStorable struct {
	Storable Storable
}

var _ Storable = SomeStorable{}

func (v SomeStorable) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + v.Storable.ByteSize()
}

func (v SomeStorable) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagSomeValue,
	})
	if err != nil {
		return err
	}
	return v.Storable.Encode(enc)
}

func (v SomeStorable) ChildStorables() []Storable {
	return []Storable{v.Storable}
}

func (v SomeStorable) StoredValue(storage SlabStorage) (Value, error) {
	wv, err := v.Storable.StoredValue(storage)
	if err != nil {
		return nil, err
	}

	return SomeValue{wv}, nil
}

func (v SomeStorable) String() string {
	return fmt.Sprintf("%s", v.Storable)
}
