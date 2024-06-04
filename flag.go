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
)

type slabType int

const (
	slabTypeUndefined slabType = iota
	slabArray
	slabMap
	slabStorable
)

type slabArrayType int

const (
	slabArrayUndefined slabArrayType = iota
	slabArrayData
	slabArrayMeta
	slabLargeImmutableArray
	slabBasicArray
)

type slabMapType int

const (
	slabMapUndefined slabMapType = iota
	slabMapData
	slabMapMeta
	slabMapLargeEntry
	slabMapCollisionGroup
)

// Version and flag masks for the 1st byte of encoded slab.
// Flags in this group are only for v1 and above.
const (
	maskVersion         byte = 0b1111_0000
	maskHasNextSlabID   byte = 0b0000_0010 // This flag is only relevant for data slab.
	maskHasInlinedSlabs byte = 0b0000_0001
)

// Flag masks for the 2nd byte of encoded slab.
// Flags in this group are available for all versions.
const (
	// Slab flags: 3 high bits
	maskSlabRoot        byte = 0b100_00000
	maskSlabHasPointers byte = 0b010_00000
	maskSlabAnySize     byte = 0b001_00000

	// Array flags: 3 low bits (4th and 5th bits are 0)
	maskArrayData byte = 0b000_00000
	maskArrayMeta byte = 0b000_00001
	// maskLargeImmutableArray byte = 0b000_00010 // not used for now
	maskBasicArray byte = 0b000_00011 // used for benchmarking

	// Map flags: 3 low bits (4th bit is 0, 5th bit is 1)
	maskMapData byte = 0b000_01000
	maskMapMeta byte = 0b000_01001
	// maskLargeMapEntry  byte = 0b000_01010 // not used for now
	maskCollisionGroup byte = 0b000_01011

	// Storable flags: 3 low bits (4th bit is 1, 5th bit is 1)
	maskStorable byte = 0b000_11111
)

const (
	maxVersion = 0b0000_1111
)

type head [2]byte

// newArraySlabHead returns an array slab head of given version and slab type.
func newArraySlabHead(version byte, t slabArrayType) (*head, error) {
	if version > maxVersion {
		return nil, fmt.Errorf("encoding version must be less than %d, got %d", maxVersion+1, version)
	}

	var h head

	h[0] = version << 4

	switch t {
	case slabArrayData:
		h[1] = maskArrayData

	case slabArrayMeta:
		h[1] = maskArrayMeta

	case slabBasicArray:
		h[1] = maskBasicArray

	default:
		return nil, fmt.Errorf("unsupported array slab type %d", t)
	}

	return &h, nil
}

// newMapSlabHead returns a map slab head of given version and slab type.
func newMapSlabHead(version byte, t slabMapType) (*head, error) {
	if version > maxVersion {
		return nil, fmt.Errorf("encoding version must be less than %d, got %d", maxVersion+1, version)
	}

	var h head

	h[0] = version << 4

	switch t {
	case slabMapData:
		h[1] = maskMapData

	case slabMapMeta:
		h[1] = maskMapMeta

	case slabMapCollisionGroup:
		h[1] = maskCollisionGroup

	default:
		return nil, fmt.Errorf("unsupported map slab type %d", t)
	}

	return &h, nil
}

// newStorableSlabHead returns a storable slab head of given version.
func newStorableSlabHead(version byte) (*head, error) {
	if version > maxVersion {
		return nil, fmt.Errorf("encoding version must be less than %d, got %d", maxVersion+1, version)
	}

	var h head
	h[0] = version << 4
	h[1] = maskStorable
	return &h, nil
}

// newHeadFromData returns a head with given data.
func newHeadFromData(data []byte) (head, error) {
	if len(data) != 2 {
		return head{}, fmt.Errorf("head must be 2 bytes, got %d bytes", len(data))
	}

	return head{data[0], data[1]}, nil
}

func (h *head) version() byte {
	return (h[0] & maskVersion) >> 4
}

func (h *head) isRoot() bool {
	return h[1]&maskSlabRoot > 0
}

func (h *head) setRoot() {
	h[1] |= maskSlabRoot
}

func (h *head) hasPointers() bool {
	return h[1]&maskSlabHasPointers > 0
}

func (h *head) setHasPointers() {
	h[1] |= maskSlabHasPointers
}

func (h *head) hasSizeLimit() bool {
	return h[1]&maskSlabAnySize == 0
}

func (h *head) setNoSizeLimit() {
	h[1] |= maskSlabAnySize
}

func (h *head) hasInlinedSlabs() bool {
	return h[0]&maskHasInlinedSlabs > 0
}

func (h *head) setHasInlinedSlabs() {
	h[0] |= maskHasInlinedSlabs
}

func (h *head) hasNextSlabID() bool {
	if h.version() == 0 {
		return !h.isRoot()
	}
	return h[0]&maskHasNextSlabID > 0
}

func (h *head) setHasNextSlabID() {
	h[0] |= maskHasNextSlabID
}

func (h head) getSlabType() slabType {
	f := h[1]
	// Extract 4th and 5th bits for slab type.
	dataType := (f & byte(0b000_11000)) >> 3
	switch dataType {
	case 0:
		// 4th and 5th bits are 0.
		return slabArray
	case 1:
		// 4th bit is 0 and 5th bit is 1.
		return slabMap
	case 3:
		// 4th and 5th bit are 1.
		return slabStorable
	default:
		return slabTypeUndefined
	}
}

func (h head) getSlabArrayType() slabArrayType {
	if h.getSlabType() != slabArray {
		return slabArrayUndefined
	}

	f := h[1]

	// Extract 3 low bits for slab array type.
	dataType := (f & byte(0b000_00111))
	switch dataType {
	case 0:
		return slabArrayData
	case 1:
		return slabArrayMeta
	case 2:
		return slabLargeImmutableArray
	case 3:
		return slabBasicArray
	default:
		return slabArrayUndefined
	}
}

func (h head) getSlabMapType() slabMapType {
	if h.getSlabType() != slabMap {
		return slabMapUndefined
	}

	f := h[1]

	// Extract 3 low bits for slab map type.
	dataType := (f & byte(0b000_00111))
	switch dataType {
	case 0:
		return slabMapData
	case 1:
		return slabMapMeta
	case 2:
		return slabMapLargeEntry
	case 3:
		return slabMapCollisionGroup
	default:
		return slabMapUndefined
	}
}
