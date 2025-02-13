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

	"github.com/fxamacker/cbor/v2"
)

func newArrayDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for array data slab")
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if h.getSlabArrayType() != slabArrayData {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want array data slab flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newArrayDataSlabFromDataV0(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	case 1:
		return newArrayDataSlabFromDataV1(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for array data slab", h.version())
	}
}

// newArrayDataSlabFromDataV0 decodes data in version 0:
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
//	CBOR encoded array of elements
//
// See ArrayExtraData.Encode() for extra data section format.
func newArrayDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayDataSlab,
	error,
) {

	var err error
	var extraData *ArrayExtraData

	// Check flag for extra data
	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// err is categorized already by newArrayExtraDataFromData.
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		data = data[versionAndFlagSize:]
	}

	var next SlabID
	if !h.isRoot() {
		// Check data length for next slab ID
		if len(data) < SlabIDLength {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		// Decode next slab ID
		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// error returned from NewSlabIDFromRawBytes is categorized already.
			return nil, err
		}

		data = data[SlabIDLength:]
	}

	// Check data length for array element head
	if len(data) < arrayDataSlabElementHeadSize {
		return nil, NewDecodingErrorf("data is too short for array data slab")
	}

	// Decode content (CBOR array)
	cborDec := decMode.NewByteStreamDecoder(data)

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	// Compute slab size for version 1.
	slabSize := uint32(arrayDataSlabPrefixSize)
	if h.isRoot() {
		slabSize = arrayRootDataSlabPrefixSize
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec, id, nil)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode array element")
		}
		elements[i] = storable
		slabSize += storable.ByteSize()
	}

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
		count:  uint32(elemCount),
	}

	return &ArrayDataSlab{
		next:      next,
		header:    header,
		elements:  elements,
		extraData: extraData,
	}, nil
}

// newArrayDataSlabFromDataV1 decodes data in version 1:
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
func newArrayDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte, // data doesn't include head (first two bytes)
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayDataSlab,
	error,
) {
	var err error
	var extraData *ArrayExtraData
	var inlinedExtraData []ExtraData
	var next SlabID

	// Decode extra data
	if h.isRoot() {
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// err is categorized already by newArrayExtraDataFromData.
			return nil, err
		}
	}

	// Decode inlined slab extra data
	if h.hasInlinedSlabs() {
		inlinedExtraData, data, err = newInlinedExtraDataFromData(
			data,
			decMode,
			decodeStorable,
			decodeTypeInfo,
		)
		if err != nil {
			// err is categorized already by newInlinedExtraDataFromData.
			return nil, err
		}
	}

	// Decode next slab ID
	if h.hasNextSlabID() {
		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// error returned from NewSlabIDFromRawBytes is categorized already.
			return nil, err
		}

		data = data[SlabIDLength:]
	}

	// Check minimum data length after header
	if len(data) < arrayDataSlabElementHeadSize {
		return nil, NewDecodingErrorf("data is too short for array data slab")
	}

	// Decode content (CBOR array)
	cborDec := decMode.NewByteStreamDecoder(data)

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	slabSize := uint32(arrayDataSlabPrefixSize)
	if h.isRoot() {
		slabSize = arrayRootDataSlabPrefixSize
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec, id, inlinedExtraData)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode array element")
		}
		elements[i] = storable
		slabSize += storable.ByteSize()
	}

	// Check if data reached EOF
	if cborDec.NumBytesDecoded() < len(data) {
		return nil, NewDecodingErrorf("data has %d bytes of extraneous data for array data slab", len(data)-cborDec.NumBytesDecoded())
	}

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
		count:  uint32(elemCount),
	}

	return &ArrayDataSlab{
		next:      next,
		header:    header,
		elements:  elements,
		extraData: extraData,
		inlined:   false, // this function is only called when slab is not inlined.
	}, nil
}

// DecodeInlinedArrayStorable decodes inlined array data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedArray, and tag contant
// as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
//
// NOTE: This function doesn't decode tag number because tag number is decoded
// in the caller and decoder only contains tag content.
func DecodeInlinedArrayStorable(
	dec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	parentSlabID SlabID,
	inlinedExtraData []ExtraData,
) (
	Storable,
	error,
) {
	const inlinedArrayDataSlabArrayCount = 3

	arrayCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != inlinedArrayDataSlabArrayCount {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined array data slab: expect %d elements, got %d elements",
				inlinedArrayDataSlabArrayCount,
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
				"failed to decode inlined array data slab: inlined extra data index %d exceeds number of inlined extra data %d",
				extraDataIndex,
				len(inlinedExtraData)))
	}

	extraData, ok := inlinedExtraData[extraDataIndex].(*ArrayExtraData)
	if !ok {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined array data slab: expect *ArrayExtraData, got %T",
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
				"failed to decode inlined array data slab: expect %d bytes for slab index, got %d bytes",
				SlabIndexLength,
				len(b)))
	}

	var index SlabIndex
	copy(index[:], b)

	slabID := NewSlabID(parentSlabID.address, index)

	// Decode array elements (CBOR array)
	elemCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	size := uint32(inlinedArrayDataSlabPrefixSize)

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(dec, slabID, inlinedExtraData)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode array element")
		}
		elements[i] = storable

		size += storable.ByteSize()
	}

	header := ArraySlabHeader{
		slabID: slabID,
		size:   size,
		count:  uint32(elemCount),
	}

	return &ArrayDataSlab{
		header:   header,
		elements: elements,
		extraData: &ArrayExtraData{
			// Make a copy of extraData.TypeInfo because
			// inlined extra data are shared by all inlined slabs.
			TypeInfo: extraData.TypeInfo.Copy(),
		},
		inlined: true,
	}, nil
}
