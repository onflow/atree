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

func newMapDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for map data slab")
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	mapType := h.getSlabMapType()

	if mapType != slabMapData && mapType != slabMapCollisionGroup {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want map data slab flag or map collision group flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newMapDataSlabFromDataV0(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	case 1:
		return newMapDataSlabFromDataV1(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for map data slab", h.version())
	}
}

// newMapDataSlabFromDataV0 decodes data in version 0:
//
// Root DataSlab Header:
//
//	+-------------------------------+------------+-------------------------------+
//	| slab version + flag (2 bytes) | extra data | slab version + flag (2 bytes) |
//	+-------------------------------+------------+-------------------------------+
//
// Non-root DataSlab Header (18 bytes):
//
//	+-------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | next sib slab ID (16 bytes) |
//	+-------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See hkeyElements.Encode() and singleElements.Encode() for elements section format.
func newMapDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapDataSlab,
	error,
) {
	var err error
	var extraData *MapExtraData

	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		data = data[versionAndFlagSize:]
	}

	var next SlabID

	if !h.isRoot() {
		// Check data length for next slab ID
		if len(data) < SlabIDLength {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		// Decode next slab ID
		var err error
		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		data = data[SlabIDLength:]
	}

	// Decode elements
	cborDec := decMode.NewByteStreamDecoder(data)
	elements, err := newElementsFromData(cborDec, decodeStorable, id, nil)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromDataV0().
		return nil, err
	}

	// Compute slab size for version 1.
	slabSize := versionAndFlagSize + elements.Size()
	if !h.isRoot() {
		slabSize += SlabIDLength
	}

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: elements.firstKey(),
	}

	return &MapDataSlab{
		next:           next,
		header:         header,
		elements:       elements,
		extraData:      extraData,
		anySize:        !h.hasSizeLimit(),
		collisionGroup: h.getSlabMapType() == slabMapCollisionGroup,
	}, nil
}

// newMapDataSlabFromDataV1 decodes data in version 1:
//
// DataSlab Header:
//
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | extra data (if root) | inlined extra data (if present) | next slab ID (if non-empty) |
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See InlinedExtraData.Encode() for inlined extra data section format.
// See hkeyElements.Encode() and singleElements.Encode() for elements section format.
func newMapDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapDataSlab,
	error,
) {
	var err error
	var extraData *MapExtraData
	var inlinedExtraData []ExtraData
	var next SlabID

	// Decode extra data
	if h.isRoot() {
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}
	}

	// Decode inlined extra data
	if h.hasInlinedSlabs() {
		inlinedExtraData, data, err = newInlinedExtraDataFromData(
			data,
			decMode,
			decodeStorable,
			decodeTypeInfo,
		)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newInlinedExtraDataFromData().
			return nil, err
		}
	}

	// Decode next slab ID for non-root slab
	if h.hasNextSlabID() {
		if len(data) < SlabIDLength {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		data = data[SlabIDLength:]
	}

	// Decode elements
	cborDec := decMode.NewByteStreamDecoder(data)
	elements, err := newElementsFromData(cborDec, decodeStorable, id, inlinedExtraData)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromDataV1().
		return nil, err
	}

	// Compute slab size.
	slabSize := versionAndFlagSize + elements.Size()
	if !h.isRoot() {
		slabSize += SlabIDLength
	}

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: elements.firstKey(),
	}

	return &MapDataSlab{
		next:           next,
		header:         header,
		elements:       elements,
		extraData:      extraData,
		anySize:        !h.hasSizeLimit(),
		collisionGroup: h.getSlabMapType() == slabMapCollisionGroup,
	}, nil
}

// DecodeInlinedCompactMapStorable decodes inlined compact map data. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedCompactMap, and tag contant
// as 3-element array:
//
// - index of inlined extra data
// - value ID index
// - CBOR array of elements
//
// NOTE: This function doesn't decode tag number because tag number is decoded
// in the caller and decoder only contains tag content.
func DecodeInlinedCompactMapStorable(
	dec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	parentSlabID SlabID,
	inlinedExtraData []ExtraData,
) (
	Storable,
	error,
) {
	const inlinedMapDataSlabArrayCount = 3

	arrayCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != inlinedMapDataSlabArrayCount {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data, expect array of %d elements, got %d elements",
				inlinedMapDataSlabArrayCount,
				arrayCount))
	}

	// element 0: extra data index
	extraDataIndex, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if extraDataIndex >= uint64(len(inlinedExtraData)) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: inlined extra data index %d exceeds number of inlined extra data %d",
				extraDataIndex,
				len(inlinedExtraData)))
	}

	extraData, ok := inlinedExtraData[extraDataIndex].(*compactMapExtraData)
	if !ok {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: expect *compactMapExtraData, got %T",
				inlinedExtraData[extraDataIndex]))
	}

	// element 1: slab index
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if len(b) != SlabIndexLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: expect %d bytes for slab index, got %d bytes",
				SlabIndexLength,
				len(b)))
	}

	var index SlabIndex
	copy(index[:], b)

	slabID := NewSlabID(parentSlabID.address, index)

	// Decode values
	elemCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if elemCount != uint64(len(extraData.keys)) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode compact map values: got %d, expect %d",
				elemCount,
				extraData.mapExtraData.Count))
	}

	// Make a copy of digests because extraData is shared by all inlined compact map data referring to the same type.
	hkeys := make([]Digest, len(extraData.hkeys))
	copy(hkeys, extraData.hkeys)

	// Decode values
	elementsSize := uint32(hkeyElementsPrefixSize)
	elems := make([]element, elemCount)
	for i := range elems {
		value, err := decodeStorable(dec, slabID, inlinedExtraData)
		if err != nil {
			return nil, err
		}

		// Make a copy of key in case it is shared.
		key := extraData.keys[i].Copy()

		elemSize := singleElementPrefixSize + key.ByteSize() + value.ByteSize()
		elem := &singleElement{key, value, elemSize}

		elems[i] = elem
		elementsSize += digestSize + elem.Size()
	}

	// Create hkeyElements
	elements := &hkeyElements{
		hkeys: hkeys,
		elems: elems,
		level: 0,
		size:  elementsSize,
	}

	header := MapSlabHeader{
		slabID:   slabID,
		size:     inlinedMapDataSlabPrefixSize + elements.Size(),
		firstKey: elements.firstKey(),
	}

	return &MapDataSlab{
		header:   header,
		elements: elements,
		extraData: &MapExtraData{
			// Make a copy of extraData.TypeInfo because
			// inlined extra data are shared by all inlined slabs.
			TypeInfo: extraData.mapExtraData.TypeInfo.Copy(),
			Count:    extraData.mapExtraData.Count,
			Seed:     extraData.mapExtraData.Seed,
		},
		anySize:        false,
		collisionGroup: false,
		inlined:        true,
	}, nil
}

// DecodeInlinedMapStorable decodes inlined map data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedMap, and tag contant
// as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
//
// NOTE: This function doesn't decode tag number because tag number is decoded
// in the caller and decoder only contains tag content.
func DecodeInlinedMapStorable(
	dec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	parentSlabID SlabID,
	inlinedExtraData []ExtraData,
) (
	Storable,
	error,
) {
	const inlinedMapDataSlabArrayCount = 3

	arrayCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != inlinedMapDataSlabArrayCount {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined map data slab, expect array of %d elements, got %d elements",
				inlinedMapDataSlabArrayCount,
				arrayCount))
	}

	// element 0: extra data index
	extraDataIndex, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if extraDataIndex >= uint64(len(inlinedExtraData)) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: inlined extra data index %d exceeds number of inlined extra data %d",
				extraDataIndex,
				len(inlinedExtraData)))
	}
	extraData, ok := inlinedExtraData[extraDataIndex].(*MapExtraData)
	if !ok {
		return nil, NewDecodingError(
			fmt.Errorf(
				"extra data (%T) is wrong type, expect *MapExtraData",
				inlinedExtraData[extraDataIndex]))
	}

	// element 1: slab index
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if len(b) != SlabIndexLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: expect %d bytes for slab index, got %d bytes",
				SlabIndexLength,
				len(b)))
	}

	var index SlabIndex
	copy(index[:], b)

	slabID := NewSlabID(parentSlabID.address, index)

	// Decode elements
	elements, err := newElementsFromData(dec, decodeStorable, slabID, inlinedExtraData)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromData().
		return nil, err
	}

	header := MapSlabHeader{
		slabID:   slabID,
		size:     inlinedMapDataSlabPrefixSize + elements.Size(),
		firstKey: elements.firstKey(),
	}

	// NOTE: extra data doesn't need to be copied because every inlined map has its own inlined extra data.

	return &MapDataSlab{
		header:   header,
		elements: elements,
		extraData: &MapExtraData{
			// Make a copy of extraData.TypeInfo because
			// inlined extra data are shared by all inlined slabs.
			TypeInfo: extraData.TypeInfo.Copy(),
			Count:    extraData.Count,
			Seed:     extraData.Seed,
		},
		anySize:        false,
		collisionGroup: false,
		inlined:        true,
	}, nil
}
