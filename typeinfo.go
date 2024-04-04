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
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

type TypeInfo interface {
	Encode(*cbor.StreamEncoder) error
	IsComposite() bool
	Copy() TypeInfo
}

type TypeInfoDecoder func(
	decoder *cbor.StreamDecoder,
) (
	TypeInfo,
	error,
)

// encodeTypeInfo encodes TypeInfo either:
// - as is (for TypeInfo in root slab extra data section), or
// - as index of inlined TypeInfos (for TypeInfo in inlined slab extra data section)
type encodeTypeInfo func(*Encoder, TypeInfo) error

// defaultEncodeTypeInfo encodes TypeInfo as is.
func defaultEncodeTypeInfo(enc *Encoder, typeInfo TypeInfo) error {
	return typeInfo.Encode(enc.CBOR)
}

func decodeTypeInfoRefIfNeeded(inlinedTypeInfo []TypeInfo, defaultTypeInfoDecoder TypeInfoDecoder) TypeInfoDecoder {
	if len(inlinedTypeInfo) == 0 {
		return defaultTypeInfoDecoder
	}

	return func(decoder *cbor.StreamDecoder) (TypeInfo, error) {
		rawTypeInfo, err := decoder.DecodeRawBytes()
		if err != nil {
			return nil, NewDecodingError(fmt.Errorf("failed to decode raw type info: %w", err))
		}

		if len(rawTypeInfo) > len(typeInfoRefTagHeadAndTagNumber) &&
			bytes.Equal(
				rawTypeInfo[:len(typeInfoRefTagHeadAndTagNumber)],
				typeInfoRefTagHeadAndTagNumber) {

			// Type info is encoded as type info ref.

			var index uint64

			err = cbor.Unmarshal(rawTypeInfo[len(typeInfoRefTagHeadAndTagNumber):], &index)
			if err != nil {
				return nil, NewDecodingError(err)
			}

			if index >= uint64(len(inlinedTypeInfo)) {
				return nil, NewDecodingError(fmt.Errorf("failed to decode type info ref: expect index < %d, got %d", len(inlinedTypeInfo), index))
			}

			return inlinedTypeInfo[int(index)], nil
		}

		// Decode type info as is.

		dec := cbor.NewByteStreamDecoder(rawTypeInfo)

		return defaultTypeInfoDecoder(dec)
	}
}

type ExtraData interface {
	isExtraData() bool
	Type() TypeInfo
	Encode(enc *Encoder, encodeTypeInfo encodeTypeInfo) error
}

// compactMapExtraData is used for inlining compact values.
// compactMapExtraData includes hkeys and keys with map extra data
// because hkeys and keys are the same in order and content for
// all values with the same compact type and map seed.
type compactMapExtraData struct {
	mapExtraData *MapExtraData
	hkeys        []Digest             // hkeys is ordered by mapExtraData.Seed
	keys         []ComparableStorable // keys is ordered by mapExtraData.Seed
}

var _ ExtraData = &compactMapExtraData{}

const compactMapExtraDataLength = 3

func (c *compactMapExtraData) isExtraData() bool {
	return true
}

func (c *compactMapExtraData) Type() TypeInfo {
	return c.mapExtraData.TypeInfo
}

func (c *compactMapExtraData) Encode(enc *Encoder, encodeTypeInfo encodeTypeInfo) error {
	err := enc.CBOR.EncodeArrayHead(compactMapExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: map extra data
	err = c.mapExtraData.Encode(enc, encodeTypeInfo)
	if err != nil {
		return err
	}

	// element 1: digests
	totalDigestSize := len(c.hkeys) * digestSize

	var digests []byte
	if totalDigestSize <= len(enc.Scratch) {
		digests = enc.Scratch[:totalDigestSize]
	} else {
		digests = make([]byte, totalDigestSize)
	}

	for i := 0; i < len(c.hkeys); i++ {
		binary.BigEndian.PutUint64(digests[i*digestSize:], uint64(c.hkeys[i]))
	}

	err = enc.CBOR.EncodeBytes(digests)
	if err != nil {
		return NewEncodingError(err)
	}

	// element 2: field names
	err = enc.CBOR.EncodeArrayHead(uint64(len(c.keys)))
	if err != nil {
		return NewEncodingError(err)
	}

	for _, key := range c.keys {
		err = key.Encode(enc)
		if err != nil {
			return NewEncodingError(err)
		}
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func newCompactMapExtraData(
	dec *cbor.StreamDecoder,
	decodeTypeInfo TypeInfoDecoder,
	decodeStorable StorableDecoder,
) (*compactMapExtraData, error) {

	length, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if length != compactMapExtraDataLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"compact extra data has invalid length %d, want %d",
				length,
				arrayExtraDataLength,
			))
	}

	// element 0: map extra data
	mapExtraData, err := newMapExtraData(dec, decodeTypeInfo)
	if err != nil {
		return nil, err
	}

	// element 1: digests
	digestBytes, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if len(digestBytes)%digestSize != 0 {
		return nil, NewDecodingError(
			fmt.Errorf(
				"decoding digests failed: number of bytes %d is not multiple of %d",
				len(digestBytes),
				digestSize))
	}

	digestCount := len(digestBytes) / digestSize

	// element 2: keys
	keyCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if keyCount != uint64(digestCount) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"decoding compact map key failed: number of keys %d is different from number of digests %d",
				keyCount,
				digestCount))
	}

	hkeys := make([]Digest, digestCount)
	for i := 0; i < digestCount; i++ {
		hkeys[i] = Digest(binary.BigEndian.Uint64(digestBytes[i*digestSize:]))
	}

	keys := make([]ComparableStorable, keyCount)
	for i := uint64(0); i < keyCount; i++ {
		// Decode compact map key
		key, err := decodeStorable(dec, SlabIDUndefined, nil)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode key's storable")
		}
		compactMapKey, ok := key.(ComparableStorable)
		if !ok {
			return nil, NewDecodingError(fmt.Errorf("failed to decode key's storable: got %T, expect ComparableStorable", key))
		}
		keys[i] = compactMapKey
	}

	return &compactMapExtraData{mapExtraData: mapExtraData, hkeys: hkeys, keys: keys}, nil
}

type compactMapTypeInfo struct {
	index int
	keys  []ComparableStorable
}

type extraDataAndEncodedTypeInfo struct {
	extraData       ExtraData
	encodedTypeInfo string // cached encoded type info
}

type InlinedExtraData struct {
	extraData         []extraDataAndEncodedTypeInfo // Used to encode deduplicated ExtraData in order
	compactMapTypeSet map[string]compactMapTypeInfo // Used to deduplicate compactMapExtraData by encoded TypeInfo + sorted field names
	arrayExtraDataSet map[string]int                // Used to deduplicate arrayExtraData by encoded TypeInfo
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
	for i := uint64(0); i < typeInfoCount; i++ {
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
	for i := uint64(0); i < extraDataCount; i++ {
		tagNum, err := dec.DecodeTagNumber()
		if err != nil {
			return nil, nil, NewDecodingError(err)
		}

		switch tagNum {
		case CBORTagInlinedArrayExtraData:
			inlinedExtraData[i], err = newArrayExtraData(dec, decodeTypeInfo)
			if err != nil {
				return nil, nil, err
			}

		case CBORTagInlinedMapExtraData:
			inlinedExtraData[i], err = newMapExtraData(dec, decodeTypeInfo)
			if err != nil {
				return nil, nil, err
			}

		case CBORTagInlinedCompactMapExtraData:
			inlinedExtraData[i], err = newCompactMapExtraData(dec, decodeTypeInfo, decodeStorable)
			if err != nil {
				return nil, nil, err
			}

		default:
			return nil, nil, NewDecodingError(fmt.Errorf("failed to decode inlined extra data: unsupported tag number %d", tagNum))
		}
	}

	return inlinedExtraData, data[dec.NumBytesDecoded():], nil
}

// addArrayExtraData returns index of deduplicated array extra data.
// Array extra data is deduplicated by array type info ID because array
// extra data only contains type info.
func (ied *InlinedExtraData) addArrayExtraData(data *ArrayExtraData) (int, error) {
	encodedTypeInfo, err := getEncodedTypeInfo(data.TypeInfo)
	if err != nil {
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

// makeCompactMapTypeID returns id of concatenated t.ID() with sorted names with "," as separator.
func makeCompactMapTypeID(encodedTypeInfo string, names []ComparableStorable) string {
	const separator = ","

	if len(names) == 1 {
		return encodedTypeInfo + separator + names[0].ID()
	}

	sorter := newFieldNameSorter(names)

	sort.Sort(sorter)

	return encodedTypeInfo + separator + sorter.join(separator)
}

// fieldNameSorter sorts names by index (not in place sort).
type fieldNameSorter struct {
	names []ComparableStorable
	index []int
}

func newFieldNameSorter(names []ComparableStorable) *fieldNameSorter {
	index := make([]int, len(names))
	for i := 0; i < len(names); i++ {
		index[i] = i
	}
	return &fieldNameSorter{
		names: names,
		index: index,
	}
}

func (fn *fieldNameSorter) Len() int {
	return len(fn.names)
}

func (fn *fieldNameSorter) Less(i, j int) bool {
	i = fn.index[i]
	j = fn.index[j]
	return fn.names[i].Less(fn.names[j])
}

func (fn *fieldNameSorter) Swap(i, j int) {
	fn.index[i], fn.index[j] = fn.index[j], fn.index[i]
}

func (fn *fieldNameSorter) join(sep string) string {
	var sb strings.Builder
	for i, index := range fn.index {
		if i > 0 {
			sb.WriteString(sep)
		}
		sb.WriteString(fn.names[index].ID())
	}
	return sb.String()
}

func getEncodedTypeInfo(ti TypeInfo) (string, error) {
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
	New: func() interface{} {
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
