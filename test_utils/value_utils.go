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

package testutils

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/onflow/atree"
)

// This file contains value implementations for testing purposes

const (
	cborTagUInt8Value                = 161
	cborTagUInt16Value               = 162
	cborTagUInt32Value               = 163
	cborTagUInt64Value               = 164
	CBORTagSomeValue                 = 165
	cborTagHashableMap               = 166
	cborTagSomeValueWithNestedLevels = 167
)

type HashableValue interface {
	atree.Value
	HashInput(scratch []byte) ([]byte, error)
}

// Uint8Value

type Uint8Value uint8

var _ atree.Value = Uint8Value(0)
var _ atree.Storable = Uint8Value(0)
var _ HashableValue = Uint8Value(0)

func (v Uint8Value) ChildStorables() []atree.Storable { return nil }

func (v Uint8Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint8Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint32) (atree.Storable, error) {
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

func (v Uint8Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint8Value) String() string {
	return fmt.Sprintf("%d", uint8(v))
}

// Uint16Value

type Uint16Value uint16

var _ atree.Value = Uint16Value(0)
var _ atree.Storable = Uint16Value(0)
var _ HashableValue = Uint16Value(0)

func (v Uint16Value) ChildStorables() []atree.Storable { return nil }

func (v Uint16Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint16Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint32) (atree.Storable, error) {
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

func (v Uint16Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint16Value) String() string {
	return fmt.Sprintf("%d", uint16(v))
}

// Uint32Value

type Uint32Value uint32

var _ atree.Value = Uint32Value(0)
var _ atree.Storable = Uint32Value(0)
var _ HashableValue = Uint32Value(0)

func (v Uint32Value) ChildStorables() []atree.Storable { return nil }

func (v Uint32Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint32Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint32) (atree.Storable, error) {
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

func (v Uint32Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint32Value) String() string {
	return fmt.Sprintf("%d", uint32(v))
}

// Uint64Value

type Uint64Value uint64

var _ atree.Value = Uint64Value(0)
var _ atree.Storable = Uint64Value(0)
var _ HashableValue = Uint64Value(0)

func NewUint64ValueFromInteger(i int) Uint64Value {
	if i < 0 {
		panic(fmt.Sprintf("expect positive int for Uint64Value, got %d", i))
	}
	return Uint64Value(i)
}

func (v Uint64Value) ChildStorables() []atree.Storable { return nil }

func (v Uint64Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint64Value) Storable(_ atree.SlabStorage, _ atree.Address, _ uint32) (atree.Storable, error) {
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

func (v Uint64Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

// StringValue

type StringValue struct {
	str  string
	size uint32
}

var _ atree.Value = StringValue{}
var _ atree.Storable = StringValue{}
var _ HashableValue = StringValue{}
var _ atree.ComparableStorable = StringValue{}

func NewStringValue(s string) StringValue {
	size := atree.GetUintCBORSize(uint64(len(s))) + uint32(len(s))
	return StringValue{str: s, size: size}
}

func (v StringValue) ChildStorables() []atree.Storable { return nil }

func (v StringValue) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v StringValue) Equal(other atree.Storable) bool {
	if _, ok := other.(StringValue); !ok {
		return false
	}
	return v.str == other.(StringValue).str
}

func (v StringValue) Less(other atree.Storable) bool {
	if _, ok := other.(StringValue); !ok {
		return false
	}
	return v.str < other.(StringValue).str
}

func (v StringValue) ID() string {
	return v.str
}

func (v StringValue) Copy() atree.Storable {
	return v
}

func (v StringValue) Storable(storage atree.SlabStorage, address atree.Address, maxInlineSize uint32) (atree.Storable, error) {
	if v.ByteSize() > maxInlineSize {
		return atree.NewStorableSlab(storage, address, v)
	}

	return v, nil
}

func (v StringValue) Encode(enc *atree.Encoder) error {
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

// SomeValue

type SomeValue struct {
	Value atree.Value
}

var _ atree.Value = SomeValue{}
var _ HashableValue = SomeValue{}
var _ atree.WrapperValue = SomeValue{}

// NOTE: For testing purposes, SomeValue and SomeStorable are mostly copied
// from github.com/onflow/cadence (interpreter.SomeValue and interpreter.SomeStorable).
// Ideally, integration tests at github.com/onflow/cadence should test integration with atree
// for mutations of nested data types.

func NewSomeValue(v atree.Value) SomeValue {
	return SomeValue{v}
}

func (v SomeValue) Storable(
	storage atree.SlabStorage,
	address atree.Address,
	maxInlineSize uint32,
) (atree.Storable, error) {

	// SomeStorable returned from this function can be encoded in two ways:
	// - if non-SomeStorable is too large, non-SomeStorable is encoded in a separate slab
	//   while SomeStorable wrapper is encoded inline with reference to slab containing
	//   non-SomeStorable.
	// - otherwise, SomeStorable with non-SomeStorable is encoded inline.
	//
	// The above applies to both immutable non-SomeValue (such as StringValue),
	// and mutable non-SomeValue (such as ArrayValue).

	nonSomeValue, nestedLevels := v.nonSomeValue()

	someStorableEncodedPrefixSize := getSomeStorableEncodedPrefixSize(nestedLevels)

	// Reduce maxInlineSize for non-SomeValue to make sure
	// that SomeStorable wrapper is always encoded inline.
	maxInlineSize -= someStorableEncodedPrefixSize

	nonSomeValueStorable, err := nonSomeValue.Storable(
		storage,
		address,
		maxInlineSize,
	)
	if err != nil {
		return nil, err
	}

	valueStorable := nonSomeValueStorable
	for i := 1; i < int(nestedLevels); i++ {
		valueStorable = SomeStorable{
			Storable: valueStorable,
		}
	}

	// No need to call maybeLargeImmutableStorable() here for SomeStorable because:
	// - encoded SomeStorable size = someStorableEncodedPrefixSize + non-SomeValueStorable size
	// - non-SomeValueStorable size < maxInlineSize - someStorableEncodedPrefixSize
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
	hi[1] = CBORTagSomeValue
	copy(hi[2:], b)

	return hi, nil
}

func (v SomeValue) String() string {
	return fmt.Sprintf("SomeValue(%s)", v.Value)
}

func (v SomeValue) UnwrapAtreeValue() (atree.Value, uint32) {
	nonSomeValue, nestedLevels := v.nonSomeValue()

	someStorableEncodedPrefixSize := getSomeStorableEncodedPrefixSize(nestedLevels)

	wv, ok := nonSomeValue.(atree.WrapperValue)
	if !ok {
		return nonSomeValue, someStorableEncodedPrefixSize
	}

	unwrappedValue, wrapperSize := wv.UnwrapAtreeValue()

	return unwrappedValue, wrapperSize + someStorableEncodedPrefixSize
}

// nonSomeValue returns a non-SomeValue and nested levels of SomeValue reached
// by traversing nested SomeValue (SomeValue containing SomeValue, etc.)
// until it reaches a non-SomeValue.
// For example,
//   - `SomeValue{true}` has non-SomeValue `true`, and nested levels 1
//   - `SomeValue{SomeValue{1}}` has non-SomeValue `1` and nested levels 2
//   - `SomeValue{SomeValue{[SomeValue{SomeValue{SomeValue{1}}}]}} has
//     non-SomeValue `[SomeValue{SomeValue{SomeValue{1}}}]` and nested levels 2
func (v SomeValue) nonSomeValue() (atree.Value, uint64) {
	nestedLevels := uint64(1)
	for {
		switch value := v.Value.(type) {
		case SomeValue:
			nestedLevels++
			v = value

		default:
			return value, nestedLevels
		}
	}
}

// MutableValue

type MutableValue struct {
	storable *MutableStorable
}

var _ atree.Value = &MutableValue{}

func NewMutableValue(storableSize uint32) *MutableValue {
	return &MutableValue{
		storable: &MutableStorable{
			size: storableSize,
		},
	}
}

func (v *MutableValue) Storable(atree.SlabStorage, atree.Address, uint32) (atree.Storable, error) {
	return v.storable, nil
}

func (v *MutableValue) UpdateStorableSize(n uint32) {
	v.storable.size = n
}

// HashableMap

type HashableMap struct {
	m *atree.OrderedMap
}

var _ atree.Value = &HashableMap{}
var _ HashableValue = &HashableMap{}

func NewHashableMap(m *atree.OrderedMap) *HashableMap {
	return &HashableMap{m}
}

func (v *HashableMap) Storable(storage atree.SlabStorage, address atree.Address, maxInlineSize uint32) (atree.Storable, error) {
	return v.m.Storable(storage, address, maxInlineSize)
}

func (v *HashableMap) HashInput(scratch []byte) ([]byte, error) {
	const (
		cborTypeByteString = 0x40

		valueIDLength          = len(atree.ValueID{})
		cborTagNumSize         = 2
		cborByteStringHeadSize = 1
		cborByteStringSize     = valueIDLength
		hashInputSize          = cborTagNumSize + cborByteStringHeadSize + cborByteStringSize
	)

	var buf []byte
	if len(scratch) >= hashInputSize {
		buf = scratch[:hashInputSize]
	} else {
		buf = make([]byte, hashInputSize)
	}

	// CBOR tag number
	buf[0], buf[1] = 0xd8, cborTagHashableMap

	// CBOR byte string head
	buf[2] = cborTypeByteString | byte(valueIDLength)

	vid := v.m.ValueID()
	copy(buf[3:], vid[:])
	return buf, nil
}

// CompareValue is used to set elements in OrderedMap.
func CompareValue(storage atree.SlabStorage, value atree.Value, storable atree.Storable) (bool, error) {
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

		return CompareValue(storage, v.Value, other.Storable)

	case *HashableMap:
		other, err := storable.StoredValue(storage)
		if err != nil {
			return false, err
		}

		otherMap, ok := other.(*atree.OrderedMap)
		if !ok {
			return false, nil
		}

		return v.m.ValueID() == otherMap.ValueID(), nil
	}

	return false, fmt.Errorf("value %T not supported for comparison", value)
}

func GetHashInput(value atree.Value, buffer []byte) ([]byte, error) {
	if hashable, ok := value.(HashableValue); ok {
		return hashable.HashInput(buffer)
	}

	return nil, fmt.Errorf("value %T doesn't implement HashableValue interface", value)
}
