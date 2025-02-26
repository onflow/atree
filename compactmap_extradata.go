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
	"sort"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

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
		// err is already categorized by newMapExtraData().
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
	for i := range hkeys {
		hkeys[i] = Digest(binary.BigEndian.Uint64(digestBytes[i*digestSize:]))
	}

	keys := make([]ComparableStorable, keyCount)
	for i := range keys {
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

func (c *compactMapExtraData) Encode(enc *Encoder, encodeTypeInfo encodeTypeInfo) error {
	err := enc.CBOR.EncodeArrayHead(compactMapExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: map extra data
	err = c.mapExtraData.Encode(enc, encodeTypeInfo)
	if err != nil {
		// err is already categorized by MapExtraData.Encode().
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

	for i := range c.hkeys {
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
			// Wrap err as external error (if needed) because err is returned by ComparableStorable.Encode().
			return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode key's storable")
		}
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (c *compactMapExtraData) isExtraData() bool {
	return true
}

func (c *compactMapExtraData) Type() TypeInfo {
	return c.mapExtraData.TypeInfo
}

// makeCompactMapTypeID returns id of concatenated t.ID() with sorted names with "," as separator.
func makeCompactMapTypeID(encodedTypeInfo string, names []ComparableStorable) string {
	const separator = ","

	if len(names) == 0 {
		return encodedTypeInfo
	}

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
	for i := range index {
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
