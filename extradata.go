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
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

type ExtraData interface {
	isExtraData() bool
	Type() TypeInfo
	Encode(enc *Encoder, encodeTypeInfo encodeTypeInfo) error
}

type InlinedExtraData struct {
	extraData         []extraDataAndEncodedTypeInfo // Used to encode deduplicated ExtraData in order
	compactMapTypeSet map[string]compactMapTypeInfo // Used to deduplicate compactMapExtraData by encoded TypeInfo + sorted field names
	arrayExtraDataSet map[string]int                // Used to deduplicate arrayExtraData by encoded TypeInfo
}

type compactMapTypeInfo struct {
	index int
	keys  []ComparableStorable
}

type extraDataAndEncodedTypeInfo struct {
	extraData       ExtraData
	encodedTypeInfo string // cached encoded type info
}

func newInlinedExtraDataFromData(
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	defaultDecodeTypeInfo TypeInfoDecoder,
) ([]ExtraData, []byte, error) {

	dec := decMode.NewByteStreamDecoder(data)

	count, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, nil, NewDecodingError(err)
	}

	if count != inlinedExtraDataArrayCount {
		return nil, nil, NewDecodingError(fmt.Errorf("failed to decode inlined extra data: expect %d elements, got %d elements", inlinedExtraDataArrayCount, count))
	}

	// element 0: array of duplicate type info
	typeInfoCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, nil, NewDecodingError(err)
	}

	inlinedTypeInfo := make([]TypeInfo, int(typeInfoCount))
	for i := range inlinedTypeInfo {
		inlinedTypeInfo[i], err = defaultDecodeTypeInfo(dec)
		if err != nil {
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode typeInfo")
		}
	}

	decodeTypeInfo := decodeTypeInfoRefIfNeeded(inlinedTypeInfo, defaultDecodeTypeInfo)

	// element 1: array of deduplicated extra data info
	extraDataCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, nil, NewDecodingError(err)
	}

	if extraDataCount == 0 {
		return nil, nil, NewDecodingError(fmt.Errorf("failed to decode inlined extra data: expect at least one inlined extra data"))
	}

	inlinedExtraData := make([]ExtraData, extraDataCount)
	for i := range inlinedExtraData {
		tagNum, err := dec.DecodeTagNumber()
		if err != nil {
			return nil, nil, NewDecodingError(err)
		}

		switch tagNum {
		case CBORTagInlinedArrayExtraData:
			inlinedExtraData[i], err = newArrayExtraData(dec, decodeTypeInfo)
			if err != nil {
				// err is already categorized by newArrayExtraData().
				return nil, nil, err
			}

		case CBORTagInlinedMapExtraData:
			inlinedExtraData[i], err = newMapExtraData(dec, decodeTypeInfo)
			if err != nil {
				// err is already categorized by newMapExtraData().
				return nil, nil, err
			}

		case CBORTagInlinedCompactMapExtraData:
			inlinedExtraData[i], err = newCompactMapExtraData(dec, decodeTypeInfo, decodeStorable)
			if err != nil {
				// err is already categorized by newCompactMapExtraData().
				return nil, nil, err
			}

		default:
			return nil, nil, NewDecodingError(fmt.Errorf("failed to decode inlined extra data: unsupported tag number %d", tagNum))
		}
	}

	return inlinedExtraData, data[dec.NumBytesDecoded():], nil
}

func newInlinedExtraData() *InlinedExtraData {
	// Maps used for deduplication are initialized lazily.
	return &InlinedExtraData{}
}

const inlinedExtraDataArrayCount = 2

var typeInfoRefTagHeadAndTagNumber = []byte{0xd8, CBORTagTypeInfoRef}

// Encode encodes inlined extra data as 2-element array:
//
//	+-----------------------+------------------------+
//	| [+ inlined type info] | [+ inlined extra data] |
//	+-----------------------+------------------------+
func (ied *InlinedExtraData) Encode(enc *Encoder) error {

	typeInfos, typeInfoIndexes := ied.findDuplicateTypeInfo()

	var err error

	err = enc.CBOR.EncodeArrayHead(inlinedExtraDataArrayCount)
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: array of duplicate type info
	err = enc.CBOR.EncodeArrayHead(uint64(len(typeInfos)))
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode type info
	for _, typeInfo := range typeInfos {
		// Encode cached type info as is.
		err = enc.CBOR.EncodeRawBytes([]byte(typeInfo))
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// element 1: deduplicated array of extra data
	err = enc.CBOR.EncodeArrayHead(uint64(len(ied.extraData)))
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode inlined extra data
	for _, extraDataInfo := range ied.extraData {
		var tagNum uint64

		switch extraDataInfo.extraData.(type) {
		case *ArrayExtraData:
			tagNum = CBORTagInlinedArrayExtraData

		case *MapExtraData:
			tagNum = CBORTagInlinedMapExtraData

		case *compactMapExtraData:
			tagNum = CBORTagInlinedCompactMapExtraData

		default:
			return NewEncodingError(fmt.Errorf("failed to encode unsupported extra data type %T", extraDataInfo.extraData))
		}

		err = enc.CBOR.EncodeTagHead(tagNum)
		if err != nil {
			return NewEncodingError(err)
		}

		err = extraDataInfo.extraData.Encode(enc, func(enc *Encoder, _ TypeInfo) error {
			encodedTypeInfo := extraDataInfo.encodedTypeInfo

			index, exist := typeInfoIndexes[encodedTypeInfo]
			if !exist {
				// typeInfo is not encoded separately, so encode typeInfo as is here.
				err = enc.CBOR.EncodeRawBytes([]byte(encodedTypeInfo))
				if err != nil {
					return NewEncodingError(err)
				}
				return nil
			}

			err = enc.CBOR.EncodeRawBytes(typeInfoRefTagHeadAndTagNumber)
			if err != nil {
				return NewEncodingError(err)
			}

			err = enc.CBOR.EncodeUint64(uint64(index))
			if err != nil {
				return NewEncodingError(err)
			}

			return nil
		})
		if err != nil {
			// err is already categorized by ExtraData.Encode().
			return err
		}
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (ied *InlinedExtraData) findDuplicateTypeInfo() ([]string, map[string]int) {
	if len(ied.extraData) < 2 {
		// No duplicate type info
		return nil, nil
	}

	// Make a copy of encoded type info to sort
	encodedTypeInfo := make([]string, len(ied.extraData))
	for i, info := range ied.extraData {
		encodedTypeInfo[i] = info.encodedTypeInfo
	}

	sort.Strings(encodedTypeInfo)

	// Find duplicate type info
	var duplicateTypeInfo []string
	var duplicateTypeInfoIndexes map[string]int

	for currentIndex := 1; currentIndex < len(encodedTypeInfo); {

		if encodedTypeInfo[currentIndex-1] != encodedTypeInfo[currentIndex] {
			currentIndex++
			continue
		}

		// Found duplicate type info at currentIndex
		duplicate := encodedTypeInfo[currentIndex]

		// Insert duplicate into duplicate type info list and map
		duplicateTypeInfo = append(duplicateTypeInfo, duplicate)

		if duplicateTypeInfoIndexes == nil {
			duplicateTypeInfoIndexes = make(map[string]int)
		}
		duplicateTypeInfoIndexes[duplicate] = len(duplicateTypeInfo) - 1

		// Skip same duplicate from sorted list
		currentIndex++
		for currentIndex < len(encodedTypeInfo) && encodedTypeInfo[currentIndex] == duplicate {
			currentIndex++
		}
	}

	return duplicateTypeInfo, duplicateTypeInfoIndexes
}

// addArrayExtraData returns index of deduplicated array extra data.
// Array extra data is deduplicated by array type info ID because array
// extra data only contains type info.
func (ied *InlinedExtraData) addArrayExtraData(data *ArrayExtraData) (int, error) {
	encodedTypeInfo, err := getEncodedTypeInfo(data.TypeInfo)
	if err != nil {
		// err is already categorized by getEncodedTypeInfo().
		return 0, err
	}

	if ied.arrayExtraDataSet == nil {
		ied.arrayExtraDataSet = make(map[string]int)
	}

	index, exist := ied.arrayExtraDataSet[encodedTypeInfo]
	if exist {
		return index, nil
	}

	index = len(ied.extraData)
	ied.extraData = append(ied.extraData, extraDataAndEncodedTypeInfo{data, encodedTypeInfo})
	ied.arrayExtraDataSet[encodedTypeInfo] = index

	return index, nil
}

// addMapExtraData returns index of map extra data.
// Map extra data is not deduplicated because it also contains count and seed.
func (ied *InlinedExtraData) addMapExtraData(data *MapExtraData) (int, error) {
	encodedTypeInfo, err := getEncodedTypeInfo(data.TypeInfo)
	if err != nil {
		// err is already categorized by getEncodedTypeInfo().
		return 0, err
	}

	index := len(ied.extraData)
	ied.extraData = append(ied.extraData, extraDataAndEncodedTypeInfo{data, encodedTypeInfo})
	return index, nil
}

// addCompactMapExtraData returns index of deduplicated compact map extra data.
// Compact map extra data is deduplicated by TypeInfo.ID() with sorted field names.
func (ied *InlinedExtraData) addCompactMapExtraData(
	data *MapExtraData,
	digests []Digest,
	keys []ComparableStorable,
) (int, []ComparableStorable, error) {

	encodedTypeInfo, err := getEncodedTypeInfo(data.TypeInfo)
	if err != nil {
		// err is already categorized by getEncodedTypeInfo().
		return 0, nil, err
	}

	if ied.compactMapTypeSet == nil {
		ied.compactMapTypeSet = make(map[string]compactMapTypeInfo)
	}

	compactMapTypeID := makeCompactMapTypeID(encodedTypeInfo, keys)
	info, exist := ied.compactMapTypeSet[compactMapTypeID]
	if exist {
		return info.index, info.keys, nil
	}

	compactMapData := &compactMapExtraData{
		mapExtraData: data,
		hkeys:        digests,
		keys:         keys,
	}

	index := len(ied.extraData)
	ied.extraData = append(ied.extraData, extraDataAndEncodedTypeInfo{compactMapData, encodedTypeInfo})

	ied.compactMapTypeSet[compactMapTypeID] = compactMapTypeInfo{
		keys:  keys,
		index: index,
	}

	return index, keys, nil
}

func (ied *InlinedExtraData) empty() bool {
	return len(ied.extraData) == 0
}

func getEncodedTypeInfo(ti TypeInfo) (string, error) {
	b := getTypeIDBuffer()
	defer putTypeIDBuffer(b)

	enc := cbor.NewStreamEncoder(b)
	err := ti.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfo.Encode().
		return "", wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode type info")
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
