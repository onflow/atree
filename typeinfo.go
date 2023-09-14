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

	"github.com/fxamacker/cbor/v2"
)

type TypeInfo interface {
	Encode(*cbor.StreamEncoder) error
	IsComposite() bool
	ID() string
	// TODO: maybe add a copy function because decoded TypeInfo can be shared by multiple slabs if not copied.
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
	hkeys        []Digest // hkeys is ordered by mapExtraData.Seed
	keys         []MapKey // keys is ordered by mapExtraData.Seed
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

	keys := make([]MapKey, keyCount)
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

type compositeTypeID struct {
	id         string
	fieldCount int
}

type compositeTypeInfo struct {
	index int
	keys  []MapKey
}

type inlinedExtraData struct {
	extraData      []ExtraData
	compositeTypes map[compositeTypeID]compositeTypeInfo
	arrayTypes     map[string]int
}

func newInlinedExtraData() *inlinedExtraData {
	return &inlinedExtraData{
		compositeTypes: make(map[compositeTypeID]compositeTypeInfo),
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
// Composite extra data is deduplicated by TypeInfo.ID() and number of fields,
// Composite fields can be removed but new fields can't be added, and existing field types can't be modified.
// Given this, composites with same type ID and same number of fields have the same fields.
// See https://developers.flow.com/cadence/language/contract-updatability#fields
func (ied *inlinedExtraData) addCompositeExtraData(data *MapExtraData, digests []Digest, keys []MapKey) int {
	id := compositeTypeID{data.TypeInfo.ID(), int(data.Count)}
	info, exist := ied.compositeTypes[id]
	if exist {
		return info.index
	}

	compositeData := &compositeExtraData{
		mapExtraData: data,
		hkeys:        digests,
		keys:         keys,
	}

	index := len(ied.extraData)
	ied.extraData = append(ied.extraData, compositeData)

	ied.compositeTypes[id] = compositeTypeInfo{
		keys:  keys,
		index: index,
	}

	return index
}

// getCompositeTypeInfo returns index of composite type and cached keys.
// NOTE: use this function instead of addCompositeExtraData to check if
// composite type is already added to save some allocation.
func (ied *inlinedExtraData) getCompositeTypeInfo(t TypeInfo, fieldCount int) (int, []MapKey, bool) {
	id := compositeTypeID{t.ID(), fieldCount}
	info, exist := ied.compositeTypes[id]
	if !exist {
		return 0, nil, false
	}
	return info.index, info.keys, true
}

func (ied *inlinedExtraData) empty() bool {
	return len(ied.extraData) == 0
}
