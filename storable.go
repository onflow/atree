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
		return nil, err
	}

	slab, found, err := storage.Retrieve(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "slab not found for stored value")
	}
	return slab.StoredValue(storage)
}

// Encode encodes StorageIDStorable as
// cbor.Tag{
//		Number:  cborTagStorageID,
//		Content: byte(v),
// }
func (v StorageIDStorable) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagStorageID,
	})
	if err != nil {
		return err
	}

	copy(enc.Scratch[:], v.Address[:])
	copy(enc.Scratch[8:], v.Index[:])

	return enc.CBOR.EncodeBytes(enc.Scratch[:storageIDSize])
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
		return nil, err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeStorageIDStorable(dec *cbor.StreamDecoder) (Storable, error) {
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, err
	}

	if len(b) != storageIDSize {
		return nil, NewStorageIDErrorf("incorrect storage id buffer length %d", len(b))
	}

	var address Address
	copy(address[:], b)

	var index StorageIndex
	copy(index[:], b[8:])

	id := NewStorageID(address, index)
	return StorageIDStorable(id), nil
}
