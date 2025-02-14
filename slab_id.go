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
	"encoding/binary"
	"fmt"
)

const (
	SlabAddressLength = 8
	SlabIndexLength   = 8
	SlabIDLength      = SlabAddressLength + SlabIndexLength
)

// WARNING: Any changes to SlabID or its components (Address and SlabIndex)
// require updates to ValueID definition and functions.
type (
	Address   [SlabAddressLength]byte
	SlabIndex [SlabIndexLength]byte

	// SlabID identifies slab in storage.
	// SlabID should only be used to retrieve,
	// store, and remove slab in storage.
	SlabID struct {
		address Address
		index   SlabIndex
	}
)

var (
	AddressUndefined   = Address{}
	SlabIndexUndefined = SlabIndex{}
	SlabIDUndefined    = SlabID{}
)

// SlabIndex

// Next returns new SlabIndex with index+1 value.
// The caller is responsible for preventing overflow
// by checking if the index value is valid before
// calling this function.
func (index SlabIndex) Next() SlabIndex {
	i := binary.BigEndian.Uint64(index[:])

	var next SlabIndex
	binary.BigEndian.PutUint64(next[:], i+1)

	return next
}

func SlabIndexToLedgerKey(ind SlabIndex) []byte {
	return []byte(LedgerBaseStorageSlabPrefix + string(ind[:]))
}

// SlabID

func NewSlabID(address Address, index SlabIndex) SlabID {
	return SlabID{address, index}
}

func NewSlabIDFromRawBytes(b []byte) (SlabID, error) {
	if len(b) < SlabIDLength {
		return SlabID{}, NewSlabIDErrorf("incorrect slab ID buffer length %d", len(b))
	}

	var address Address
	copy(address[:], b)

	var index SlabIndex
	copy(index[:], b[SlabAddressLength:])

	return SlabID{address, index}, nil
}

func (id SlabID) ToRawBytes(b []byte) (int, error) {
	if len(b) < SlabIDLength {
		return 0, NewSlabIDErrorf("incorrect slab ID buffer length %d", len(b))
	}
	copy(b, id.address[:])
	copy(b[SlabAddressLength:], id.index[:])
	return SlabIDLength, nil
}

func (id SlabID) String() string {
	return fmt.Sprintf(
		"0x%x.%d",
		binary.BigEndian.Uint64(id.address[:]),
		binary.BigEndian.Uint64(id.index[:]),
	)
}

func (id SlabID) AddressAsUint64() uint64 {
	return binary.BigEndian.Uint64(id.address[:])
}

// Address returns the address of SlabID.
func (id SlabID) Address() Address {
	return id.address
}

func (id SlabID) IndexAsUint64() uint64 {
	return binary.BigEndian.Uint64(id.index[:])
}

func (id SlabID) HasTempAddress() bool {
	return id.address == AddressUndefined
}

func (id SlabID) Index() SlabIndex {
	return id.index
}

func (id SlabID) Valid() error {
	if id == SlabIDUndefined {
		return NewSlabIDError("undefined slab ID")
	}
	if id.index == SlabIndexUndefined {
		return NewSlabIDError("undefined slab index")
	}
	return nil
}

func (id SlabID) Compare(other SlabID) int {
	result := bytes.Compare(id.address[:], other.address[:])
	if result == 0 {
		return bytes.Compare(id.index[:], other.index[:])
	}
	return result
}
