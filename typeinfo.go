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
	"sort"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

type TypeInfo interface {
	Encode(*cbor.StreamEncoder) error
	IsComposite() bool
	Identifier() string
	Copy() TypeInfo
}

type TypeInfoDecoder func(
	decoder *cbor.StreamDecoder,
) (
	TypeInfo,
	error,
)

type ExtraData interface {
	isExtraData() bool
	Encode(enc *Encoder) error
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

func (c *compactMapExtraData) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeArrayHead(compactMapExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: map extra data
	err = c.mapExtraData.Encode(enc)
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

type InlinedExtraData struct {
	extraData       []ExtraData
	compactMapTypes map[string]compactMapTypeInfo
	arrayTypes      map[string]int
}

func newInlinedExtraData() *InlinedExtraData {
	return &InlinedExtraData{}
}

// Encode encodes inlined extra data as CBOR array.
func (ied *InlinedExtraData) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeArrayHead(uint64(len(ied.extraData)))
	if err != nil {
		return NewEncodingError(err)
	}

	var tagNum uint64

	for _, extraData := range ied.extraData {
		switch extraData.(type) {
		case *ArrayExtraData:
			tagNum = CBORTagInlinedArrayExtraData

		case *MapExtraData:
			tagNum = CBORTagInlinedMapExtraData

		case *compactMapExtraData:
			tagNum = CBORTagInlinedCompactMapExtraData

		default:
			return NewEncodingError(fmt.Errorf("failed to encode unsupported extra data type %T", extraData))
		}

		err = enc.CBOR.EncodeTagHead(tagNum)
		if err != nil {
			return NewEncodingError(err)
		}

		err = extraData.Encode(enc)
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

func newInlinedExtraDataFromData(
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) ([]ExtraData, []byte, error) {

	dec := decMode.NewByteStreamDecoder(data)

	count, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, nil, NewDecodingError(err)
	}

	if count == 0 {
		return nil, nil, NewDecodingError(fmt.Errorf("failed to decode inlined extra data: expect at least one inlined extra data"))
	}

	inlinedExtraData := make([]ExtraData, count)
	for i := uint64(0); i < count; i++ {
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
func (ied *InlinedExtraData) addArrayExtraData(data *ArrayExtraData) int {
	if ied.arrayTypes == nil {
		ied.arrayTypes = make(map[string]int)
	}

	id := data.TypeInfo.Identifier()
	index, exist := ied.arrayTypes[id]
	if exist {
		return index
	}

	index = len(ied.extraData)
	ied.extraData = append(ied.extraData, data)
	ied.arrayTypes[id] = index
	return index
}

// addMapExtraData returns index of map extra data.
// Map extra data is not deduplicated because it also contains count and seed.
func (ied *InlinedExtraData) addMapExtraData(data *MapExtraData) int {
	index := len(ied.extraData)
	ied.extraData = append(ied.extraData, data)
	return index
}

// addCompactMapExtraData returns index of deduplicated compact map extra data.
// Compact map extra data is deduplicated by TypeInfo.ID() with sorted field names.
func (ied *InlinedExtraData) addCompactMapExtraData(
	data *MapExtraData,
	digests []Digest,
	keys []ComparableStorable,
) (int, []ComparableStorable) {

	if ied.compactMapTypes == nil {
		ied.compactMapTypes = make(map[string]compactMapTypeInfo)
	}

	id := makeCompactMapTypeID(data.TypeInfo, keys)
	info, exist := ied.compactMapTypes[id]
	if exist {
		return info.index, info.keys
	}

	compactMapData := &compactMapExtraData{
		mapExtraData: data,
		hkeys:        digests,
		keys:         keys,
	}

	index := len(ied.extraData)
	ied.extraData = append(ied.extraData, compactMapData)

	ied.compactMapTypes[id] = compactMapTypeInfo{
		keys:  keys,
		index: index,
	}

	return index, keys
}

func (ied *InlinedExtraData) empty() bool {
	return len(ied.extraData) == 0
}

// makeCompactMapTypeID returns id of concatenated t.ID() with sorted names with "," as separator.
func makeCompactMapTypeID(t TypeInfo, names []ComparableStorable) string {
	const separator = ","

	if len(names) == 1 {
		return t.Identifier() + separator + names[0].ID()
	}

	sorter := newFieldNameSorter(names)

	sort.Sort(sorter)

	return t.Identifier() + separator + sorter.join(separator)
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
