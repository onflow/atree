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
	"bytes"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

type Storable interface {
	Encode(*Encoder) error

	ByteSize() uint32

	StoredValue(storage SlabStorage) (Value, error)

	// ChildStorables only returns child storables in this storable
	// (not recursive).  This function shouldn't load extra slabs.
	ChildStorables() []Storable
}

const (
	CBORTagInlineCollisionGroup   = 253
	CBORTagExternalCollisionGroup = 254

	CBORTagStorageID = 255
)

type StorageIDStorable StorageID

var _ Storable = StorageIDStorable{}

func (v StorageIDStorable) ChildStorables() []Storable {
	return nil
}

func (v StorageIDStorable) StoredValue(storage SlabStorage) (Value, error) {
	id := StorageID(v)
	if err := id.Valid(); err != nil {
		// Don't need to wrap error as external error because err is already categorized by StorageID.Valid().
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

// Encode encodes StorageIDStorable as
//
//	cbor.Tag{
//			Number:  cborTagStorageID,
//			Content: byte(v),
//	}
func (v StorageIDStorable) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagStorageID,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	copy(enc.Scratch[:], v.Address[:])
	copy(enc.Scratch[8:], v.Index[:])

	err = enc.CBOR.EncodeBytes(enc.Scratch[:storageIDSize])
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (v StorageIDStorable) ByteSize() uint32 {
	// tag number (2 bytes) + byte string header (1 byte) + storage id (16 bytes)
	return 2 + 1 + storageIDSize
}

func (v StorageIDStorable) String() string {
	return fmt.Sprintf("StorageIDStorable(%d)", v)
}

// Encode is a wrapper for Storable.Encode()
func Encode(storable Storable, encMode cbor.EncMode) ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, encMode)

	err := storable.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode storable")
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return nil, NewEncodingError(err)
	}

	return buf.Bytes(), nil
}

func DecodeStorageIDStorable(dec *cbor.StreamDecoder) (Storable, error) {
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	id, err := NewStorageIDFromRawBytes(b)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by NewStorageIDFromRawBytes().
		return nil, err
	}

	return StorageIDStorable(id), nil
}

func getLoadedValue(storage SlabStorage, storable Storable) (Value, error) {
	switch storable := storable.(type) {
	case StorageIDStorable:
		slab := storage.RetrieveIfLoaded(StorageID(storable))
		if slab == nil {
			// Skip because it references unloaded slab.
			return nil, nil
		}

		v, err := slab.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil

	default:
		v, err := storable.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil
	}
}
