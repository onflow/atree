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
	ValueIDLength = SlabIDLength
)

// ValueID identifies an Array or OrderedMap. ValueID is consistent
// independent of inlining status, while ValueID and SlabID are used
// differently despite having the same size and content under the hood.
// By contrast, SlabID is affected by inlining because it identifies
// a slab in storage.  Given this, ValueID should be used for
// resource tracking, etc.
type ValueID [ValueIDLength]byte

var emptyValueID = ValueID{}

func (vid ValueID) equal(sid SlabID) bool {
	return bytes.Equal(vid[:len(sid.address)], sid.address[:]) &&
		bytes.Equal(vid[len(sid.address):], sid.index[:])
}

func (vid ValueID) String() string {
	return fmt.Sprintf(
		"0x%x.%d",
		binary.BigEndian.Uint64(vid[:SlabAddressLength]),
		binary.BigEndian.Uint64(vid[SlabAddressLength:]),
	)
}

func slabIDToValueID(sid SlabID) ValueID {
	var id ValueID
	n := copy(id[:], sid.address[:])
	copy(id[n:], sid.index[:])
	return id
}
