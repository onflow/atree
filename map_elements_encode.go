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

// Encode encodes hkeyElements to the given encoder.
//
//	CBOR encoded array [
//	    0: level (uint)
//	    1: hkeys (byte string)
//	    2: elements (array)
//	]
func (e *hkeyElements) Encode(enc *Encoder) error {

	if e.level > maxDigestLevel {
		return NewFatalError(fmt.Errorf("hash level %d exceeds max digest level %d", e.level, maxDigestLevel))
	}

	// Encode CBOR array head of 3 elements (level, hkeys, elements)
	const cborArrayHeadOfThreeElements = 0x83
	enc.Scratch[0] = cborArrayHeadOfThreeElements

	// Encode hash level
	enc.Scratch[1] = byte(e.level)

	// Encode hkeys as byte string

	// Encode hkeys bytes header manually for fix-sized encoding
	// TODO: maybe make this header dynamic to reduce size
	// CBOR byte string head 0x59 indicates that the number of bytes in byte string are encoded in the next 2 bytes.
	const cborByteStringHead = 0x59
	enc.Scratch[2] = cborByteStringHead

	binary.BigEndian.PutUint16(enc.Scratch[3:], uint16(len(e.hkeys)*8))

	// Write scratch content to encoder
	const totalSize = 5
	err := enc.CBOR.EncodeRawBytes(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode hkeys
	for i := 0; i < len(e.hkeys); i++ {
		binary.BigEndian.PutUint64(enc.Scratch[:], uint64(e.hkeys[i]))
		err = enc.CBOR.EncodeRawBytes(enc.Scratch[:digestSize])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode elements

	// Encode elements array header manually for fix-sized encoding
	// TODO: maybe make this header dynamic to reduce size
	// CBOR array head 0x99 indicating that the number of array elements are encoded in the next 2 bytes.
	const cborArrayHead = 0x99
	enc.Scratch[0] = cborArrayHead
	binary.BigEndian.PutUint16(enc.Scratch[1:], uint16(len(e.elems)))
	err = enc.CBOR.EncodeRawBytes(enc.Scratch[:3])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode each element
	for _, e := range e.elems {
		err = e.Encode(enc)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by element.Encode().
			return err
		}
	}

	// TODO: is Flush necessary
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// Encode encodes singleElements to the given encoder.
//
//	CBOR encoded array [
//	    0: level (uint)
//	    1: hkeys (0 length byte string)
//	    2: elements (array)
//	]
func (e *singleElements) Encode(enc *Encoder) error {

	if e.level > maxDigestLevel {
		return NewFatalError(fmt.Errorf("digest level %d exceeds max digest level %d", e.level, maxDigestLevel))
	}

	// Encode CBOR array header for 3 elements (level, hkeys, elements)
	enc.Scratch[0] = 0x83

	// Encode hash level
	enc.Scratch[1] = byte(e.level)

	// Encode hkeys (empty byte string)
	enc.Scratch[2] = 0x40

	// Encode elements

	// Encode elements array header manually for fix-sized encoding
	// TODO: maybe make this header dynamic to reduce size
	enc.Scratch[3] = 0x99
	binary.BigEndian.PutUint16(enc.Scratch[4:], uint16(len(e.elems)))

	// Write scratch content to encoder
	const totalSize = 6
	err := enc.CBOR.EncodeRawBytes(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode each element
	for _, e := range e.elems {
		err = e.Encode(enc)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by singleElement.Encode().
			return err
		}
	}

	// TODO: is Flush necessar?
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}
