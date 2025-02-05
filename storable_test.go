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

package atree

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

// This file contains value implementations for testing purposes

const (
	cborTagUInt8Value                = 161
	cborTagUInt16Value               = 162
	cborTagUInt32Value               = 163
	cborTagUInt64Value               = 164
	cborTagSomeValue                 = 165
	cborTagHashableMap               = 166
	cborTagSomeValueWithNestedLevels = 167
)

func TestIsCBORTagNumberRangeAvailable(t *testing.T) {
	minTagNum, maxTagNum := ReservedCBORTagNumberRange()

	t.Run("error", func(t *testing.T) {
		_, err := IsCBORTagNumberRangeAvailable(maxTagNum, minTagNum)
		var userError *UserError
		require.ErrorAs(t, err, &userError)
	})

	t.Run("identical", func(t *testing.T) {
		available, err := IsCBORTagNumberRangeAvailable(minTagNum, maxTagNum)
		require.NoError(t, err)
		require.False(t, available)
	})

	t.Run("subrange", func(t *testing.T) {
		available, err := IsCBORTagNumberRangeAvailable(minTagNum, maxTagNum-1)
		require.NoError(t, err)
		require.False(t, available)

		available, err = IsCBORTagNumberRangeAvailable(minTagNum+1, maxTagNum)
		require.NoError(t, err)
		require.False(t, available)
	})

	t.Run("partial overlap", func(t *testing.T) {
		available, err := IsCBORTagNumberRangeAvailable(minTagNum-1, maxTagNum-1)
		require.NoError(t, err)
		require.False(t, available)

		available, err = IsCBORTagNumberRangeAvailable(minTagNum+1, maxTagNum+1)
		require.NoError(t, err)
		require.False(t, available)
	})

	t.Run("non-overlap", func(t *testing.T) {
		available, err := IsCBORTagNumberRangeAvailable(minTagNum-10, minTagNum-1)
		require.NoError(t, err)
		require.True(t, available)

		available, err = IsCBORTagNumberRangeAvailable(minTagNum-1, minTagNum-1)
		require.NoError(t, err)
		require.True(t, available)

		available, err = IsCBORTagNumberRangeAvailable(maxTagNum+1, maxTagNum+10)
		require.NoError(t, err)
		require.True(t, available)

		available, err = IsCBORTagNumberRangeAvailable(maxTagNum+10, maxTagNum+10)
		require.NoError(t, err)
		require.True(t, available)
	})
}

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
//
//	cbor.Tag{
//			Number:  cborTagUInt8Value,
//			Content: uint8(v),
//	}
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

type Uint32Value uint32

var _ Value = Uint32Value(0)
var _ Storable = Uint32Value(0)
var _ HashableValue = Uint32Value(0)

func (v Uint32Value) ChildStorables() []Storable { return nil }

func (v Uint32Value) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v Uint32Value) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
	return v, nil
}

// Encode encodes UInt32Value as
//
//	cbor.Tag{
//			Number:  cborTagUInt32Value,
//			Content: uint32(v),
//	}
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
//
//	cbor.Tag{
//			Number:  cborTagUInt64Value,
//			Content: uint64(v),
//	}
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

type StringValue struct {
	str  string
	size uint32
}

var _ Value = StringValue{}
var _ Storable = StringValue{}
var _ HashableValue = StringValue{}
var _ ComparableStorable = StringValue{}

func NewStringValue(s string) StringValue {
	size := GetUintCBORSize(uint64(len(s))) + uint32(len(s))
	return StringValue{str: s, size: size}
}

func (v StringValue) ChildStorables() []Storable { return nil }

func (v StringValue) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (v StringValue) Equal(other Storable) bool {
	if _, ok := other.(StringValue); !ok {
		return false
	}
	return v.str == other.(StringValue).str
}

func (v StringValue) Less(other Storable) bool {
	if _, ok := other.(StringValue); !ok {
		return false
	}
	return v.str < other.(StringValue).str
}

func (v StringValue) ID() string {
	return v.str
}

func (v StringValue) Copy() Storable {
	return v
}

func (v StringValue) Storable(storage SlabStorage, address Address, maxInlineSize uint64) (Storable, error) {
	if uint64(v.ByteSize()) > maxInlineSize {

		// Create StorableSlab
		id, err := storage.GenerateSlabID(address)
		if err != nil {
			return nil, err
		}

		slab := &StorableSlab{
			slabID:   id,
			storable: v,
		}

		// Store StorableSlab in storage
		err = storage.Store(id, slab)
		if err != nil {
			return nil, err
		}

		// Return slab id as storable
		return SlabIDStorable(id), nil
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

func decodeStorable(dec *cbor.StreamDecoder, id SlabID, inlinedExtraData []ExtraData) (Storable, error) {
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
		case CBORTagInlinedArray:
			return DecodeInlinedArrayStorable(dec, decodeStorable, id, inlinedExtraData)

		case CBORTagInlinedMap:
			return DecodeInlinedMapStorable(dec, decodeStorable, id, inlinedExtraData)

		case CBORTagInlinedCompactMap:
			return DecodeInlinedCompactMapStorable(dec, decodeStorable, id, inlinedExtraData)

		case CBORTagSlabID:
			return DecodeSlabIDStorable(dec)

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

func decodeTypeInfo(dec *cbor.StreamDecoder) (TypeInfo, error) {
	t, err := dec.NextType()
	if err != nil {
		return nil, err
	}

	switch t {
	case cbor.UintType:
		value, err := dec.DecodeUint64()
		if err != nil {
			return nil, err
		}

		return testTypeInfo{value: value}, nil

	case cbor.TagType:
		tagNum, err := dec.DecodeTagNumber()
		if err != nil {
			return nil, err
		}

		switch tagNum {
		case testCompositeTypeInfoTagNum:
			value, err := dec.DecodeUint64()
			if err != nil {
				return nil, err
			}

			return testCompositeTypeInfo{value: value}, nil

		default:
			return nil, fmt.Errorf("failed to decode type info")
		}

	default:
		return nil, fmt.Errorf("failed to decode type info")
	}

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

	case *HashableMap:
		other, err := storable.StoredValue(storage)
		if err != nil {
			return false, err
		}

		otherMap, ok := other.(*OrderedMap)
		if !ok {
			return false, nil
		}

		return v.m.ValueID() == otherMap.ValueID(), nil
	}

	return false, fmt.Errorf("value %T not supported for comparison", value)
}

func hashInputProvider(value Value, buffer []byte) ([]byte, error) {
	if hashable, ok := value.(HashableValue); ok {
		return hashable.HashInput(buffer)
	}

	return nil, fmt.Errorf("value %T doesn't implement HashableValue interface", value)
}

type SomeValue struct {
	Value Value
}

var _ Value = SomeValue{}
var _ HashableValue = SomeValue{}
var _ WrapperValue = SomeValue{}

// NOTE: For testing purposes, SomeValue and SomeStorable are mostly copied
// from github.com/onflow/cadence (interpreter.SomeValue and interpreter.SomeStorable).
// Ideally, integration tests at github.com/onflow/cadence should test integration with atree
// for mutations of nested data types.

func (v SomeValue) Storable(
	storage SlabStorage,
	address Address,
	maxInlineSize uint64,
) (Storable, error) {

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

func (v SomeValue) UnwrapAtreeValue() (Value, uint64) {
	nonSomeValue, nestedLevels := v.nonSomeValue()

	someStorableEncodedPrefixSize := getSomeStorableEncodedPrefixSize(nestedLevels)

	wv, ok := nonSomeValue.(WrapperValue)
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
func (v SomeValue) nonSomeValue() (Value, uint64) {
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
	return cborTagSize + someStorableWithMultipleNestedlevelsArraySize + getUintCBORSize(nestedLevels)
}

type SomeStorable struct {
	Storable Storable
}

var _ ContainerStorable = SomeStorable{}
var _ WrapperStorable = SomeStorable{}

func (s SomeStorable) HasPointer() bool {
	if ms, ok := s.Storable.(ContainerStorable); ok {
		return ms.HasPointer()
	}
	return false
}

func (s SomeStorable) ByteSize() uint32 {
	nonSomeStorable, nestedLevels := s.nonSomeStorable()
	return getSomeStorableEncodedPrefixSize(nestedLevels) + nonSomeStorable.ByteSize()
}

func (s SomeStorable) Encode(e *Encoder) error {
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
func (s SomeStorable) encode(e *Encoder) error {
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
	e *Encoder,
	levels uint64,
	nonSomeStorable Storable,
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
func (s SomeStorable) nonSomeStorable() (Storable, uint64) {
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

func (s SomeStorable) ChildStorables() []Storable {
	return []Storable{s.Storable}
}

func (s SomeStorable) StoredValue(storage SlabStorage) (Value, error) {
	wv, err := s.Storable.StoredValue(storage)
	if err != nil {
		return nil, err
	}

	return SomeValue{wv}, nil
}

func (s SomeStorable) String() string {
	return fmt.Sprintf("SomeStorable(%s)", s.Storable)
}

func (s SomeStorable) UnwrapAtreeStorable() Storable {
	storable := s.Storable
	for {
		ws, ok := storable.(WrapperStorable)
		if !ok {
			break
		}
		storable = ws.UnwrapAtreeStorable()
	}
	return storable
}

func (s SomeStorable) WrapAtreeStorable(storable Storable) Storable {
	_, nestedLevels := s.nonSomeStorable()

	newStorable := SomeStorable{Storable: storable}
	for i := 1; i < int(nestedLevels); i++ {
		newStorable = SomeStorable{Storable: newStorable}
	}
	return newStorable
}

type testMutableValue struct {
	storable *mutableStorable
}

var _ Value = &testMutableValue{}

func newTestMutableValue(storableSize uint32) *testMutableValue {
	return &testMutableValue{
		storable: &mutableStorable{
			size: storableSize,
		},
	}
}

func (v *testMutableValue) Storable(SlabStorage, Address, uint64) (Storable, error) {
	return v.storable, nil
}

func (v *testMutableValue) updateStorableSize(n uint32) {
	v.storable.size = n
}

type mutableStorable struct {
	size uint32
}

var _ Storable = &mutableStorable{}

func (s *mutableStorable) ByteSize() uint32 {
	return s.size
}

func (s *mutableStorable) StoredValue(SlabStorage) (Value, error) {
	return &testMutableValue{s}, nil
}

func (*mutableStorable) ChildStorables() []Storable {
	return nil
}

func (*mutableStorable) Encode(*Encoder) error {
	// no-op for testing
	return nil
}

type HashableMap struct {
	m *OrderedMap
}

var _ Value = &HashableMap{}
var _ HashableValue = &HashableMap{}

func NewHashableMap(m *OrderedMap) *HashableMap {
	return &HashableMap{m}
}

func (v *HashableMap) Storable(storage SlabStorage, address Address, maxInlineSize uint64) (Storable, error) {
	return v.m.Storable(storage, address, maxInlineSize)
}

func (v *HashableMap) HashInput(scratch []byte) ([]byte, error) {
	const (
		cborTypeByteString = 0x40

		valueIDLength          = len(ValueID{})
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

func getUintCBORSize(v uint64) uint32 {
	if v <= 23 {
		return 1
	}
	if v <= math.MaxUint8 {
		return 2
	}
	if v <= math.MaxUint16 {
		return 3
	}
	if v <= math.MaxUint32 {
		return 5
	}
	return 9
}
