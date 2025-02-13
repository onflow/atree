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
)

// encodeAsInlined encodes inlined array data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedArray,
// and tag contant as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
func (a *ArrayDataSlab) encodeAsInlined(enc *Encoder) error {
	if a.extraData == nil {
		return NewEncodingError(
			fmt.Errorf("failed to encode non-root array data slab as inlined"))
	}

	if !a.inlined {
		return NewEncodingError(
			fmt.Errorf("failed to encode standalone array data slab as inlined"))
	}

	extraDataIndex, err := enc.inlinedExtraData().addArrayExtraData(a.extraData)
	if err != nil {
		// err is already categorized by InlinedExtraData.addArrayExtraData().
		return err
	}

	if extraDataIndex > maxInlinedExtraDataIndex {
		return NewEncodingError(
			fmt.Errorf("failed to encode inlined array data slab: extra data index %d exceeds limit %d", extraDataIndex, maxInlinedExtraDataIndex))
	}

	// Encode tag number and array head of 3 elements
	err = enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagInlinedArray,
		// array head of 3 elements
		0x83,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: extra data index
	// NOTE: encoded extra data index is fixed sized CBOR uint
	err = enc.CBOR.EncodeRawBytes([]byte{
		0x18,
		byte(extraDataIndex),
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 1: slab index
	err = enc.CBOR.EncodeBytes(a.header.slabID.index[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// element 2: array elements
	err = a.encodeElements(enc)
	if err != nil {
		// err is already categorized by ArrayDataSlab.encodeElements().
		return err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// Encode encodes this array data slab to the given encoder.
//
// DataSlab Header:
//
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | extra data (if root) | inlined extra data (if present) | next slab ID (if non-empty) |
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded array of elements
//
// See ArrayExtraData.Encode() for extra data section format.
// See InlinedExtraData.Encode() for inlined extra data section format.
func (a *ArrayDataSlab) Encode(enc *Encoder) error {

	if a.inlined {
		return a.encodeAsInlined(enc)
	}

	// Encoding is done in two steps:
	//
	// 1. Encode array elements using a new buffer while collecting inlined extra data from inlined elements.
	// 2. Encode slab with deduplicated inlined extra data and copy encoded elements from previous buffer.

	// Get a buffer from a pool to encode elements.
	elementBuf := getBuffer()
	defer putBuffer(elementBuf)

	elementEnc := NewEncoder(elementBuf, enc.encMode)

	err := a.encodeElements(elementEnc)
	if err != nil {
		// err is already categorized by Array.encodeElements().
		return err
	}

	err = elementEnc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	const version = 1

	h, err := newArraySlabHead(version, slabArrayData)
	if err != nil {
		return NewEncodingError(err)
	}

	if a.HasPointer() {
		h.setHasPointers()
	}

	if a.next != SlabIDUndefined {
		h.setHasNextSlabID()
	}

	if a.extraData != nil {
		h.setRoot()
	}

	if elementEnc.hasInlinedExtraData() {
		h.setHasInlinedSlabs()
	}

	// Encode head (version + flag)
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data
	if a.extraData != nil {
		// Use defaultEncodeTypeInfo to encode root level TypeInfo as is.
		err = a.extraData.Encode(enc, defaultEncodeTypeInfo)
		if err != nil {
			// err is already categorized by ArrayExtraData.Encode().
			return err
		}
	}

	// Encode inlined extra data
	if elementEnc.hasInlinedExtraData() {
		err = elementEnc.inlinedExtraData().Encode(enc)
		if err != nil {
			// err is already categorized by inlinedExtraData.Encode().
			return err
		}
	}

	// Encode next slab ID
	if a.next != SlabIDUndefined {
		n, err := a.next.ToRawBytes(enc.Scratch[:])
		if err != nil {
			// Don't need to wrap because err is already categorized by SlabID.ToRawBytes().
			return err
		}

		_, err = enc.Write(enc.Scratch[:n])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode elements by copying raw bytes from previous buffer
	err = enc.CBOR.EncodeRawBytes(elementBuf.Bytes())
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (a *ArrayDataSlab) encodeElements(enc *Encoder) error {
	// Encode CBOR array size manually for fix-sized encoding

	enc.Scratch[0] = 0x80 | 25

	countOffset := 1
	const countSize = 2
	binary.BigEndian.PutUint16(
		enc.Scratch[countOffset:],
		uint16(len(a.elements)),
	)

	// Write scratch content to encoder
	totalSize := countOffset + countSize
	err := enc.CBOR.EncodeRawBytes(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode data slab content (array of elements)
	for _, e := range a.elements {
		err = e.Encode(enc)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode array element")
		}
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}
