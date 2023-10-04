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

package main

import (
	"fmt"

	"github.com/onflow/atree"

	"github.com/fxamacker/cbor/v2"
)

const (
	maxArrayTypeValue = 10
	maxMapTypeValue   = 10

	arrayTypeTagNum = 246
	mapTypeTagNum   = 245
)

type arrayTypeInfo struct {
	value int
}

func newArrayTypeInfo() arrayTypeInfo {
	return arrayTypeInfo{value: r.Intn(maxArrayTypeValue)}
}

var _ atree.TypeInfo = arrayTypeInfo{}

func (i arrayTypeInfo) Copy() atree.TypeInfo {
	return i
}

func (i arrayTypeInfo) IsComposite() bool {
	return false
}

func (i arrayTypeInfo) ID() string {
	return fmt.Sprintf("array(%d)", i)
}

func (i arrayTypeInfo) Encode(e *cbor.StreamEncoder) error {
	err := e.EncodeTagHead(arrayTypeTagNum)
	if err != nil {
		return err
	}
	return e.EncodeInt64(int64(i.value))
}

func (i arrayTypeInfo) Equal(other atree.TypeInfo) bool {
	otherArrayTypeInfo, ok := other.(arrayTypeInfo)
	return ok && i.value == otherArrayTypeInfo.value
}

type mapTypeInfo struct {
	value int
}

var _ atree.TypeInfo = mapTypeInfo{}

func newMapTypeInfo() mapTypeInfo {
	return mapTypeInfo{value: r.Intn(maxMapTypeValue)}
}

func (i mapTypeInfo) Copy() atree.TypeInfo {
	return i
}

func (i mapTypeInfo) IsComposite() bool {
	return false
}

func (i mapTypeInfo) ID() string {
	return fmt.Sprintf("map(%d)", i)
}

func (i mapTypeInfo) Encode(e *cbor.StreamEncoder) error {
	err := e.EncodeTagHead(mapTypeTagNum)
	if err != nil {
		return err
	}
	return e.EncodeInt64(int64(i.value))
}

func (i mapTypeInfo) Equal(other atree.TypeInfo) bool {
	otherMapTypeInfo, ok := other.(mapTypeInfo)
	return ok && i.value == otherMapTypeInfo.value
}

func decodeTypeInfo(dec *cbor.StreamDecoder) (atree.TypeInfo, error) {
	num, err := dec.DecodeTagNumber()
	if err != nil {
		return nil, err
	}
	switch num {
	case arrayTypeTagNum:
		value, err := dec.DecodeInt64()
		if err != nil {
			return nil, err
		}

		return arrayTypeInfo{value: int(value)}, nil

	case mapTypeTagNum:
		value, err := dec.DecodeInt64()
		if err != nil {
			return nil, err
		}

		return mapTypeInfo{value: int(value)}, nil

	default:
		return nil, fmt.Errorf("failed to decode type info with tag number %d", num)
	}
}
