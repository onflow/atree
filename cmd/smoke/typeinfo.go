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
	"bytes"
	"fmt"
	"sync"

	"github.com/onflow/atree"

	"github.com/fxamacker/cbor/v2"
)

const (
	maxArrayTypeValue = 10
	maxMapTypeValue   = 10
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

var compositeFieldNames = []string{"a", "b", "c"}

type compositeTypeInfo struct {
	fieldStartIndex int // inclusive start index of fieldNames
	fieldEndIndex   int // exclusive end index of fieldNames
}

var _ atree.TypeInfo = mapTypeInfo{}

// newCompositeTypeInfo creates one of 10 compositeTypeInfo randomly.
// 10 possible composites:
// - ID: composite(0_0), field names: []
// - ID: composite(0_1), field names: ["a"]
// - ID: composite(0_2), field names: ["a", "b"]
// - ID: composite(0_3), field names: ["a", "b", "c"]
// - ID: composite(1_1), field names: []
// - ID: composite(1_2), field names: ["b"]
// - ID: composite(1_3), field names: ["b", "c"]
// - ID: composite(2_2), field names: []
// - ID: composite(2_3), field names: ["c"]
// - ID: composite(3_3), field names: []
func newCompositeTypeInfo() compositeTypeInfo {
	// startIndex is [0, 3]
	startIndex := r.Intn(len(compositeFieldNames) + 1)

	// count is [0, 3]
	count := r.Intn(len(compositeFieldNames) - startIndex + 1)

	endIndex := startIndex + count
	if endIndex > len(compositeFieldNames) {
		panic("not reachable")
	}

	return compositeTypeInfo{fieldStartIndex: startIndex, fieldEndIndex: endIndex}
}

func (i compositeTypeInfo) getFieldNames() []string {
	return compositeFieldNames[i.fieldStartIndex:i.fieldEndIndex]
}

func (i compositeTypeInfo) Copy() atree.TypeInfo {
	return i
}

func (i compositeTypeInfo) IsComposite() bool {
	return true
}

func (i compositeTypeInfo) Encode(e *cbor.StreamEncoder) error {
	err := e.EncodeTagHead(compositeTypeTagNum)
	if err != nil {
		return err
	}
	err = e.EncodeArrayHead(2)
	if err != nil {
		return err
	}
	err = e.EncodeInt64(int64(i.fieldStartIndex))
	if err != nil {
		return err
	}
	return e.EncodeInt64(int64(i.fieldEndIndex))
}

func (i compositeTypeInfo) Equal(other atree.TypeInfo) bool {
	otherCompositeTypeInfo, ok := other.(compositeTypeInfo)
	if !ok {
		return false
	}
	return i.fieldStartIndex == otherCompositeTypeInfo.fieldStartIndex &&
		i.fieldEndIndex == otherCompositeTypeInfo.fieldEndIndex
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

	case compositeTypeTagNum:
		count, err := dec.DecodeArrayHead()
		if err != nil {
			return nil, err
		}
		if count != 2 {
			return nil, fmt.Errorf(
				"failed to decode composite type info: expect 2 elemets, got %d elements",
				count,
			)
		}

		startIndex, err := dec.DecodeInt64()
		if err != nil {
			return nil, err
		}

		endIndex, err := dec.DecodeInt64()
		if err != nil {
			return nil, err
		}

		if endIndex < startIndex {
			return nil, fmt.Errorf(
				"failed to decode composite type info: endIndex %d < startIndex %d",
				endIndex,
				startIndex,
			)
		}

		if endIndex > int64(len(compositeFieldNames)) {
			return nil, fmt.Errorf(
				"failed to decode composite type info: endIndex %d > len(compositeFieldNames) %d",
				endIndex,
				len(compositeFieldNames))
		}

		return compositeTypeInfo{fieldStartIndex: int(startIndex), fieldEndIndex: int(endIndex)}, nil

	default:
		return nil, fmt.Errorf("failed to decode type info with tag number %d", num)
	}
}

func compareTypeInfo(a atree.TypeInfo, b atree.TypeInfo) bool {
	aID, _ := getEncodedTypeInfo(a)
	bID, _ := getEncodedTypeInfo(b)
	return aID == bID
}

func getEncodedTypeInfo(ti atree.TypeInfo) (string, error) {
	b := getTypeIDBuffer()
	defer putTypeIDBuffer(b)

	enc := cbor.NewStreamEncoder(b)
	err := ti.Encode(enc)
	if err != nil {
		return "", err
	}
	enc.Flush()

	return b.String(), nil
}

const defaultTypeIDBufferSize = 256

var typeIDBufferPool = sync.Pool{
	New: func() any {
		e := new(bytes.Buffer)
		e.Grow(defaultTypeIDBufferSize)
		return e
	},
}

func getTypeIDBuffer() *bytes.Buffer {
	return typeIDBufferPool.Get().(*bytes.Buffer)
}

func putTypeIDBuffer(e *bytes.Buffer) {
	e.Reset()
	typeIDBufferPool.Put(e)
}
