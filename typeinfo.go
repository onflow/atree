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

	"github.com/fxamacker/cbor/v2"
)

type TypeInfo interface {
	Encode(*cbor.StreamEncoder) error
	IsComposite() bool
	ID() string
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

// compositeExtraData is used for inlining composite values.
// compositeExtraData includes hkeys and keys with map extra data
// because hkeys and keys are the same in order and content for
// all values with the same composite type and map seed.
type compositeExtraData struct {
	mapExtraData *MapExtraData
	hkeys        []Digest   // hkeys is ordered by mapExtraData.Seed
	keys         []Storable // keys is ordered by mapExtraData.Seed
}

var _ ExtraData = &compositeExtraData{}

const compositeExtraDataLength = 3

func (c *compositeExtraData) isExtraData() bool {
	return true
}

func (c *compositeExtraData) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeArrayHead(compositeExtraDataLength)
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

func newCompositeExtraData(
	dec *cbor.StreamDecoder,
	decodeTypeInfo TypeInfoDecoder,
	decodeStorable StorableDecoder,
) (*compositeExtraData, error) {

	length, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if length != compositeExtraDataLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"composite extra data has invalid length %d, want %d",
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
				"decoding composite key failed: number of keys %d is different from number of digests %d",
				keyCount,
				digestCount))
	}

	hkeys := make([]Digest, digestCount)
	for i := 0; i < digestCount; i++ {
		hkeys[i] = Digest(binary.BigEndian.Uint64(digestBytes[i*digestSize:]))
	}

	keys := make([]Storable, keyCount)
	for i := uint64(0); i < keyCount; i++ {
		// Decode composite key
		key, err := decodeStorable(dec, SlabIDUndefined, nil)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode key's storable")
		}
		keys[i] = key
	}

	return &compositeExtraData{mapExtraData: mapExtraData, hkeys: hkeys, keys: keys}, nil
}

type compositeTypeInfo struct {
	index int
	keys  []ComparableStorable
}

type inlinedExtraData struct {
	extraData      []ExtraData
	compositeTypes map[string]compositeTypeInfo
	arrayTypes     map[string]int
}

func newInlinedExtraData() *inlinedExtraData {
	return &inlinedExtraData{
		compositeTypes: make(map[string]compositeTypeInfo),
		arrayTypes:     make(map[string]int),
	}
}

// Encode encodes inlined extra data as CBOR array.
func (ied *inlinedExtraData) Encode(enc *Encoder) error {
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

		case *compositeExtraData:
			tagNum = CBORTagInlinedCompositeExtraData

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

		case CBORTagInlinedCompositeExtraData:
			inlinedExtraData[i], err = newCompositeExtraData(dec, decodeTypeInfo, decodeStorable)
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
func (ied *inlinedExtraData) addArrayExtraData(data *ArrayExtraData) int {
	id := data.TypeInfo.ID()
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
func (ied *inlinedExtraData) addMapExtraData(data *MapExtraData) int {
	index := len(ied.extraData)
	ied.extraData = append(ied.extraData, data)
	return index
}

// addCompositeExtraData returns index of deduplicated composite extra data.
// Composite extra data is deduplicated by TypeInfo.ID() with sorted field names.
func (ied *inlinedExtraData) addCompositeExtraData(
	data *MapExtraData,
	digests []Digest,
	keys []ComparableStorable,
) (int, []ComparableStorable) {

	id := makeCompositeTypeID(data.TypeInfo, keys)
	info, exist := ied.compositeTypes[id]
	if exist {
		return info.index, info.keys
	}

	storableKeys := make([]Storable, len(keys))
	for i, k := range keys {
		storableKeys[i] = k
	}

	compositeData := &compositeExtraData{
		mapExtraData: data,
		hkeys:        digests,
		keys:         storableKeys,
	}

	index := len(ied.extraData)
	ied.extraData = append(ied.extraData, compositeData)

	ied.compositeTypes[id] = compositeTypeInfo{
		keys:  keys,
		index: index,
	}

	return index, keys
}

func (ied *inlinedExtraData) empty() bool {
	return len(ied.extraData) == 0
}

// makeCompositeTypeID returns id of concatenated t.ID() with sorted names with "," as separator.
func makeCompositeTypeID(t TypeInfo, names []ComparableStorable) string {
	const separator = ","

	if len(names) == 1 {
		return t.ID() + separator + names[0].ID()
	}

	sorter := newFieldNameSorter(names)

	sort.Sort(sorter)

	return t.ID() + separator + sorter.join(separator)
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
	var s string
	for _, i := range fn.index {
		s += sep + fn.names[i].ID()
	}
	return s
}
