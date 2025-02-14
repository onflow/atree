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

	"github.com/fxamacker/cbor/v2"
)

func newElementsFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder, slabID SlabID, inlinedExtraData []ExtraData) (elements, error) {

	arrayCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != 3 {
		return nil, NewDecodingError(fmt.Errorf("decoding elements failed: expect array of 3 elements, got %d elements", arrayCount))
	}

	level, err := cborDec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	digestBytes, err := cborDec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if len(digestBytes)%digestSize != 0 {
		return nil, NewDecodingError(fmt.Errorf("decoding digests failed: number of bytes is not multiple of %d", digestSize))
	}

	digestCount := len(digestBytes) / digestSize
	hkeys := make([]Digest, digestCount)
	for i := 0; i < digestCount; i++ {
		hkeys[i] = Digest(binary.BigEndian.Uint64(digestBytes[i*digestSize:]))
	}

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if digestCount != 0 && uint64(digestCount) != elemCount {
		return nil, NewDecodingError(fmt.Errorf("decoding elements failed: number of hkeys %d isn't the same as number of elements %d", digestCount, elemCount))
	}

	if digestCount == 0 && elemCount > 0 {
		// elements are singleElements

		// Decode elements
		size := uint32(singleElementsPrefixSize)
		elems := make([]*singleElement, elemCount)
		for i := 0; i < int(elemCount); i++ {
			elem, err := newSingleElementFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by newSingleElementFromData().
				return nil, err
			}

			elems[i] = elem
			size += elem.Size()
		}

		// Create singleElements
		elements := &singleElements{
			elems: elems,
			level: uint(level),
			size:  size,
		}

		return elements, nil
	}

	// elements are hkeyElements

	// Decode elements
	size := uint32(hkeyElementsPrefixSize)
	elems := make([]element, elemCount)
	for i := 0; i < int(elemCount); i++ {
		elem, err := newElementFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newElementFromData().
			return nil, err
		}

		elems[i] = elem
		size += digestSize + elem.Size()
	}

	// Create hkeyElements
	elements := &hkeyElements{
		hkeys: hkeys,
		elems: elems,
		level: uint(level),
		size:  size,
	}

	return elements, nil
}
