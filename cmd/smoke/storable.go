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

	cborTagUInt8Value                = 161
	cborTagUInt16Value               = 162
	cborTagUInt32Value               = 163
	cborTagUInt64Value               = 164
	cborTagSomeValue                 = 165
	cborTagHashableMap               = 166
	cborTagSomeValueWithNestedLevels = 167

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

type HashableValue interface {
	atree.Value
	HashInput(scratch []byte) ([]byte, error)
}

type Uint8Value uint8

var _ HashableValue = Uint8Value(0)
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
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint8Value) String() string {
	return fmt.Sprintf("%d", uint8(v))
}

type Uint16Value uint16

var _ HashableValue = Uint16Value(0)
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
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint16Value) String() string {
	return fmt.Sprintf("%d", uint16(v))
}

type Uint32Value uint32

var _ HashableValue = Uint32Value(0)
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
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint32Value) String() string {
	return fmt.Sprintf("%d", uint32(v))
}

type Uint64Value uint64

var _ HashableValue = Uint64Value(0)
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
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

type StringValue struct {
	str  string
	size uint32
}

var _ HashableValue = &StringValue{}
var _ atree.Value = &StringValue{}
var _ atree.Storable = &StringValue{}
var _ atree.ComparableStorable = &StringValue{}

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

func (v StringValue) Storable(storage atree.SlabStorage, address atree.Address, maxInlineSize uint64) (atree.Storable, error) {
	if uint64(v.ByteSize()) <= maxInlineSize {
		return v, nil
	}

	return atree.NewStorableSlab(storage, address, v)
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

type SomeValue struct {
	Value atree.Value
}

var _ HashableValue = SomeValue{}
var _ atree.Value = SomeValue{}
var _ atree.WrapperValue = SomeValue{}

// NOTE: For testing purposes, SomeValue and SomeStorable are mostly copied
// from github.com/onflow/cadence (interpreter.SomeValue and interpreter.SomeStorable).
func (v SomeValue) Storable(
	storage atree.SlabStorage,
	address atree.Address,
	maxInlineSize uint64,
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
	maxInlineSize -= uint64(someStorableEncodedPrefixSize)

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
	hi[1] = cborTagSomeValue
	copy(hi[2:], b)

	return hi, nil
}

func (v SomeValue) String() string {
	return fmt.Sprintf("SomeValue(%s)", v.Value)
}

func (v SomeValue) UnwrapAtreeValue() (atree.Value, uint64) {
	nonSomeValue, nestedLevels := v.nonSomeValue()

	someStorableEncodedPrefixSize := getSomeStorableEncodedPrefixSize(nestedLevels)

	wv, ok := nonSomeValue.(atree.WrapperValue)
	if !ok {
		return nonSomeValue, uint64(someStorableEncodedPrefixSize)
	}

	unwrappedValue, wrapperSize := wv.UnwrapAtreeValue()

	return unwrappedValue, wrapperSize + uint64(someStorableEncodedPrefixSize)
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

const (
	cborTagSize                                    = 2
	someStorableWithMultipleNestedlevelsArraySize  = 1
	someStorableWithMultipleNestedLevelsArrayCount = 2
)

func getSomeStorableEncodedPrefixSize(nestedLevels uint64) uint32 {
	if nestedLevels == 1 {
		return cborTagSize
	}
	return cborTagSize +
		someStorableWithMultipleNestedlevelsArraySize +
		atree.GetUintCBORSize(nestedLevels)
}

type SomeStorable struct {
	Storable atree.Storable
}

var _ atree.ContainerStorable = SomeStorable{}
var _ atree.WrapperStorable = SomeStorable{}

func (s SomeStorable) HasPointer() bool {
	if ms, ok := s.Storable.(atree.ContainerStorable); ok {
		return ms.HasPointer()
	}
	return false
}

func (s SomeStorable) ByteSize() uint32 {
	nonSomeStorable, nestedLevels := s.nonSomeStorable()
	return getSomeStorableEncodedPrefixSize(nestedLevels) + nonSomeStorable.ByteSize()
}

func (s SomeStorable) Encode(e *atree.Encoder) error {
	nonSomeStorable, nestedLevels := s.nonSomeStorable()
	if nestedLevels == 1 {
		return s.encode(e)
	}
	return s.encodeMultipleNestedLevels(e, nestedLevels, nonSomeStorable)
}

// encode encodes SomeStorable with nested levels = 1 as
//
//	cbor.Tag{
//			Number: CBORTagSomeValue,
//			Content: Value(v.Value),
//	}
func (s SomeStorable) encode(e *atree.Encoder) error {
	// NOTE: when updating, also update SomeStorable.ByteSize
	err := e.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagSomeValue,
	})
	if err != nil {
		return err
	}
	return s.Storable.Encode(e)
}

// encodeMultipleNestedLevels encodes SomeStorable with nested levels > 1 as
//
//	cbor.Tag{
//			Number: CBORTagSomeValueWithNestedLevels,
//			Content: CBORArray[nested_levels, innermsot_value],
//	}
func (s SomeStorable) encodeMultipleNestedLevels(
	e *atree.Encoder,
	levels uint64,
	nonSomeStorable atree.Storable,
) error {
	// NOTE: when updating, also update SomeStorable.ByteSize
	err := e.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagSomeValueWithNestedLevels,
		// array of 2 elements
		0x82,
	})
	if err != nil {
		return err
	}

	err = e.CBOR.EncodeUint64(levels)
	if err != nil {
		return err
	}

	return nonSomeStorable.Encode(e)
}

// nonSomeStorable returns a non-SomeStorable and nested levels of SomeStorable reached
// by traversing nested SomeStorable (SomeStorable containing SomeStorable, etc.)
// until it reaches a non-SomeStorable.
// For example,
//   - `SomeStorable{true}` has non-SomeStorable `true`, and nested levels 1
//   - `SomeStorable{SomeStorable{1}}` has non-SomeStorable `1` and nested levels 2
//   - `SomeStorable{SomeStorable{[SomeStorable{SomeStorable{SomeStorable{1}}}]}} has
//     non-SomeStorable `[SomeStorable{SomeStorable{SomeStorable{1}}}]` and nested levels 2
func (s SomeStorable) nonSomeStorable() (atree.Storable, uint64) {
	nestedLevels := uint64(1)
	for {
		switch storable := s.Storable.(type) {
		case SomeStorable:
			nestedLevels++
			s = storable

		default:
			return storable, nestedLevels
		}
	}
}

func (s SomeStorable) ChildStorables() []atree.Storable {
	return []atree.Storable{s.Storable}
}

func (s SomeStorable) StoredValue(storage atree.SlabStorage) (atree.Value, error) {
	wv, err := s.Storable.StoredValue(storage)
	if err != nil {
		return nil, err
	}

	return SomeValue{wv}, nil
}

func (s SomeStorable) String() string {
	return fmt.Sprintf("SomeStorable(%s)", s.Storable)
}

func (s SomeStorable) UnwrapAtreeStorable() atree.Storable {
	storable := s.Storable
	for {
		ws, ok := storable.(atree.WrapperStorable)
		if !ok {
			break
		}
		storable = ws.UnwrapAtreeStorable()
	}
	return storable
}

func (s SomeStorable) WrapAtreeStorable(storable atree.Storable) atree.Storable {
	_, nestedLevels := s.nonSomeStorable()

	newStorable := SomeStorable{Storable: storable}
	for i := 1; i < int(nestedLevels); i++ {
		newStorable = SomeStorable{Storable: newStorable}
	}
	return newStorable
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

		case cborTagSomeValue:
			storable, err := decodeStorable(dec, id, inlinedExtraData)
			if err != nil {
				return nil, err
			}
			return SomeStorable{Storable: storable}, nil

		case cborTagSomeValueWithNestedLevels:
			count, err := dec.DecodeArrayHead()
			if err != nil {
				return nil, fmt.Errorf(
					"invalid some value with nested levels encoding: %w",
					err,
				)
			}

			if count != someStorableWithMultipleNestedLevelsArrayCount {
				return nil, fmt.Errorf(
					"invalid array count for some value with nested levels encoding: got %d, expect %d",
					count, someStorableWithMultipleNestedLevelsArrayCount,
				)
			}

			nestedLevels, err := dec.DecodeUint64()
			if err != nil {
				return nil, fmt.Errorf(
					"invalid nested levels for some value with nested levels encoding: %w",
					err,
				)
			}

			if nestedLevels <= 1 {
				return nil, fmt.Errorf(
					"invalid nested levels for some value with nested levels encoding: got %d, expect > 1",
					nestedLevels,
				)
			}

			nonSomeStorable, err := decodeStorable(dec, id, inlinedExtraData)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid nonSomeStorable for some value with nested levels encoding: %w",
					err,
				)
			}

			storable := SomeStorable{
				Storable: nonSomeStorable,
			}
			for i := uint64(1); i < nestedLevels; i++ {
				storable = SomeStorable{
					Storable: storable,
				}
			}

			return storable, nil

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

	case SomeValue:
		other, ok := storable.(SomeStorable)
		if !ok {
			return false, nil
		}

		return compare(storage, v.Value, other.Storable)
	}

	return false, fmt.Errorf("value %T not supported for comparison", value)
}

func hashInputProvider(value atree.Value, buffer []byte) ([]byte, error) {
	if hashable, ok := value.(HashableValue); ok {
		return hashable.HashInput(buffer)
	}

	return nil, fmt.Errorf("value %T doesn't implement HashableValue interface", value)
}
