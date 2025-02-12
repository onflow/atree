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

package test_utils

import (
	"fmt"
	"math"

	"github.com/fxamacker/cbor/v2"

	"github.com/onflow/atree"
)

// SomeStorable

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
//			Content: atree.Value(v.atree.Value),
//	}
func (s SomeStorable) encode(e *atree.Encoder) error {
	// NOTE: when updating, also update SomeStorable.ByteSize
	err := e.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagSomeValue,
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

// MutableStorable

type MutableStorable struct {
	size uint32
}

var _ atree.Storable = &MutableStorable{}

func (s *MutableStorable) ByteSize() uint32 {
	return s.size
}

func (s *MutableStorable) StoredValue(atree.SlabStorage) (atree.Value, error) {
	return &MutableValue{s}, nil
}

func (*MutableStorable) ChildStorables() []atree.Storable {
	return nil
}

func (*MutableStorable) Encode(*atree.Encoder) error {
	// no-op for testing
	return nil
}

func DecodeStorable(dec *cbor.StreamDecoder, id atree.SlabID, inlinedExtraData []atree.ExtraData) (atree.Storable, error) {
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
			return atree.DecodeInlinedArrayStorable(dec, DecodeStorable, id, inlinedExtraData)

		case atree.CBORTagInlinedMap:
			return atree.DecodeInlinedMapStorable(dec, DecodeStorable, id, inlinedExtraData)

		case atree.CBORTagInlinedCompactMap:
			return atree.DecodeInlinedCompactMapStorable(dec, DecodeStorable, id, inlinedExtraData)

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

		case CBORTagSomeValue:
			storable, err := DecodeStorable(dec, id, inlinedExtraData)
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

			nonSomeStorable, err := DecodeStorable(dec, id, inlinedExtraData)
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
