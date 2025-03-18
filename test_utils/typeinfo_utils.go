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

	cbor "github.com/fxamacker/cbor/v2/cborstream"

	"github.com/onflow/atree"
)

// SimpleTypeInfo

type SimpleTypeInfo struct {
	value uint64
}

var _ atree.TypeInfo = SimpleTypeInfo{}

func NewSimpleTypeInfo(value uint64) SimpleTypeInfo {
	return SimpleTypeInfo{value}
}

func (i SimpleTypeInfo) Value() uint64 {
	return i.value
}

func (i SimpleTypeInfo) Copy() atree.TypeInfo {
	return i
}

func (i SimpleTypeInfo) IsComposite() bool {
	return false
}

func (i SimpleTypeInfo) Identifier() string {
	return fmt.Sprintf("uint64(%d)", i)
}

func (i SimpleTypeInfo) Encode(enc *cbor.StreamEncoder) error {
	return enc.EncodeUint64(i.value)
}

func (i SimpleTypeInfo) Equal(other atree.TypeInfo) bool {
	otherTestTypeInfo, ok := other.(SimpleTypeInfo)
	return ok && i.value == otherTestTypeInfo.value
}

// CompositeTypeInfo

const CompositeTypeInfoTagNum = 246

type CompositeTypeInfo struct {
	value uint64
}

var _ atree.TypeInfo = CompositeTypeInfo{}

func NewCompositeTypeInfo(value uint64) CompositeTypeInfo {
	return CompositeTypeInfo{value}
}

func (i CompositeTypeInfo) Copy() atree.TypeInfo {
	return i
}

func (i CompositeTypeInfo) IsComposite() bool {
	return true
}

func (i CompositeTypeInfo) Identifier() string {
	return fmt.Sprintf("composite(%d)", i)
}

func (i CompositeTypeInfo) Encode(enc *cbor.StreamEncoder) error {
	err := enc.EncodeTagHead(CompositeTypeInfoTagNum)
	if err != nil {
		return err
	}
	return enc.EncodeUint64(i.value)
}

func (i CompositeTypeInfo) Equal(other atree.TypeInfo) bool {
	otherTestTypeInfo, ok := other.(CompositeTypeInfo)
	return ok && i.value == otherTestTypeInfo.value
}

func CompareTypeInfo(a, b atree.TypeInfo) bool {
	switch a := a.(type) {
	case SimpleTypeInfo:
		return a.Equal(b)

	case CompositeTypeInfo:
		return a.Equal(b)

	default:
		return false
	}
}

func DecodeTypeInfo(dec *cbor.StreamDecoder) (atree.TypeInfo, error) {
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

		return SimpleTypeInfo{value: value}, nil

	case cbor.TagType:
		tagNum, err := dec.DecodeTagNumber()
		if err != nil {
			return nil, err
		}

		switch tagNum {
		case CompositeTypeInfoTagNum:
			value, err := dec.DecodeUint64()
			if err != nil {
				return nil, err
			}

			return CompositeTypeInfo{value: value}, nil

		default:
			return nil, fmt.Errorf("failed to decode type info")
		}

	default:
		return nil, fmt.Errorf("failed to decode type info")
	}

}
