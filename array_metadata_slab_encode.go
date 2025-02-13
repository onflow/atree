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
)

// Encode encodes this array meta-data slab to the given encoder.
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+------------+--------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (12 bytes):
//
//	+------------------------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+--------------------------------+------------------------------+
//
// Content (n * 14 bytes):
//
//	[[slab index (8 bytes), count (4 bytes), size (2 bytes)], ...]
//
// See ArrayExtraData.Encode() for extra data section format.
func (a *ArrayMetaDataSlab) Encode(enc *Encoder) error {

	const version = 1

	h, err := newArraySlabHead(version, slabArrayMeta)
	if err != nil {
		return NewEncodingError(err)
	}

	if a.extraData != nil {
		h.setRoot()
	}

	// Write head (version + flag)
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data if present
	if a.extraData != nil {
		// Use defaultEncodeTypeInfo to encode root level TypeInfo as is.
		err = a.extraData.Encode(enc, defaultEncodeTypeInfo)
		if err != nil {
			// Don't need to wrap because err is already categorized by ArrayExtraData.Encode().
			return err
		}
	}

	// Encode shared address to scratch
	copy(enc.Scratch[:], a.header.slabID.address[:])

	// Encode child header count to scratch
	const childHeaderCountOffset = SlabAddressLength
	binary.BigEndian.PutUint16(
		enc.Scratch[childHeaderCountOffset:],
		uint16(len(a.childrenHeaders)),
	)

	// Write scratch content to encoder
	const totalSize = childHeaderCountOffset + 2
	_, err = enc.Write(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode children headers
	for _, h := range a.childrenHeaders {
		// Encode slab index to scratch
		copy(enc.Scratch[:], h.slabID.index[:])

		// Encode count
		const countOffset = SlabIndexLength
		binary.BigEndian.PutUint32(enc.Scratch[countOffset:], h.count)

		// Encode size
		const sizeOffset = countOffset + 4
		binary.BigEndian.PutUint16(enc.Scratch[sizeOffset:], uint16(h.size))

		const totalSize = sizeOffset + 2
		_, err = enc.Write(enc.Scratch[:totalSize])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	return nil
}
