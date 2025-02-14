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

import "encoding/binary"

// Encode encodes map meta-data slab to the given encoder.
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
// Content (n * 18 bytes):
//
//	[ +[slab index (8 bytes), first key (8 bytes), size (2 bytes)]]
//
// See MapExtraData.Encode() for extra data section format.
func (m *MapMetaDataSlab) Encode(enc *Encoder) error {

	const version = 1

	h, err := newMapSlabHead(version, slabMapMeta)
	if err != nil {
		return NewEncodingError(err)
	}

	if m.extraData != nil {
		h.setRoot()
	}

	// Write head (version and flag)
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data if present
	if m.extraData != nil {
		// Use defaultEncodeTypeInfo to encode root level TypeInfo as is.
		err = m.extraData.Encode(enc, defaultEncodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapExtraData.Encode().
			return err
		}
	}

	// Encode shared address to scratch
	copy(enc.Scratch[:], m.header.slabID.address[:])

	// Encode child header count to scratch
	const childHeaderCountOffset = SlabAddressLength
	binary.BigEndian.PutUint16(
		enc.Scratch[childHeaderCountOffset:],
		uint16(len(m.childrenHeaders)),
	)

	// Write scratch content to encoder
	const totalSize = childHeaderCountOffset + 2
	_, err = enc.Write(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode children headers
	for _, h := range m.childrenHeaders {
		// Encode slab index to scratch
		copy(enc.Scratch[:], h.slabID.index[:])

		const firstKeyOffset = SlabIndexLength
		binary.BigEndian.PutUint64(enc.Scratch[firstKeyOffset:], uint64(h.firstKey))

		const sizeOffset = firstKeyOffset + digestSize
		binary.BigEndian.PutUint16(enc.Scratch[sizeOffset:], uint16(h.size))

		const totalSize = sizeOffset + 2
		_, err = enc.Write(enc.Scratch[:totalSize])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	return nil
}
