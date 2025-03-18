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
	"fmt"

	"github.com/SophisticaSean/cbor/v2"
)

type SlabIDStorable SlabID

var _ ContainerStorable = SlabIDStorable{}

func (v SlabIDStorable) HasPointer() bool {
	return true
}

func (v SlabIDStorable) ChildStorables() []Storable {
	return nil
}

func (v SlabIDStorable) StoredValue(storage SlabStorage) (Value, error) {
	id := SlabID(v)
	if err := id.Valid(); err != nil {
		// Don't need to wrap error as external error because err is already categorized by SlabID.Valid().
		return nil, err
	}

	slab, found, err := storage.Retrieve(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "slab not found for stored value")
	}
	value, err := slab.StoredValue(storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}
	return value, nil
}

// Encode encodes SlabIDStorable as
//
//	cbor.Tag{
//			Number:  cborTagSlabID,
//			Content: byte(v),
//	}
func (v SlabIDStorable) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagSlabID,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	copy(enc.Scratch[:], v.address[:])
	copy(enc.Scratch[8:], v.index[:])

	err = enc.CBOR.EncodeBytes(enc.Scratch[:SlabIDLength])
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (v SlabIDStorable) ByteSize() uint32 {
	// tag number (2 bytes) + byte string header (1 byte) + slab id (16 bytes)
	return 2 + 1 + SlabIDLength
}

func (v SlabIDStorable) String() string {
	return fmt.Sprintf("SlabIDStorable(%d)", v)
}

func DecodeSlabIDStorable(dec *cbor.StreamDecoder) (Storable, error) {
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	id, err := NewSlabIDFromRawBytes(b)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
		return nil, err
	}

	return SlabIDStorable(id), nil
}
