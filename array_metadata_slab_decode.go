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

	"github.com/fxamacker/cbor/v2"
)

func newArrayMetaDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayMetaDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for array metadata slab")
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if h.getSlabArrayType() != slabArrayMeta {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want array metadata slab flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newArrayMetaDataSlabFromDataV0(id, h, data, decMode, decodeTypeInfo)

	case 1:
		return newArrayMetaDataSlabFromDataV1(id, h, data, decMode, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for array metadata slab", h.version())
	}
}

// newArrayMetaDataSlabFromDataV0 decodes data in version 0:
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | slab version + flag (2 byte) | child header count (2 bytes) |
//	+------------------------------+------------+------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (4 bytes):
//
//	+------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child header count (2 bytes) |
//	+------------------------------+------------------------------+
//
// Content (n * 24 bytes):
//
//	[[slab id (16 bytes), count (4 bytes), size (4 bytes)], ...]
//
// See ArrayExtraData.Encode() for extra data section format.
func newArrayMetaDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayMetaDataSlab,
	error,
) {
	// NOTE: the following encoded sizes are for version 0 only (changed in later version).
	const (
		// meta data children array head size: 2 bytes
		arrayMetaDataArrayHeadSizeV0 = 2

		// slab header size: slab id (16 bytes) + count (4 bytes) + size (4 bytes)
		arraySlabHeaderSizeV0 = SlabIDLength + 4 + 4
	)

	var err error
	var extraData *ArrayExtraData

	if h.isRoot() {
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap because err is already categorized by newArrayExtraDataFromData().
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		data = data[versionAndFlagSize:]
	}

	// Check data length (after decoding extra data if present)
	if len(data) < arrayMetaDataArrayHeadSizeV0 {
		return nil, NewDecodingErrorf("data is too short for array metadata slab")
	}

	// Decode number of child headers
	childHeaderCount := binary.BigEndian.Uint16(data)

	data = data[arrayMetaDataArrayHeadSizeV0:]

	expectedDataLength := arraySlabHeaderSizeV0 * int(childHeaderCount)
	if len(data) != expectedDataLength {
		return nil, NewDecodingErrorf(
			"data has unexpected length %d, want %d",
			len(data),
			expectedDataLength,
		)
	}

	// Decode child headers
	childrenHeaders := make([]ArraySlabHeader, childHeaderCount)
	childrenCountSum := make([]uint32, childHeaderCount)
	totalCount := uint32(0)
	offset := 0

	for i := 0; i < int(childHeaderCount); i++ {
		slabID, err := NewSlabIDFromRawBytes(data[offset:])
		if err != nil {
			// Don't need to wrap because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		countOffset := offset + SlabIDLength
		count := binary.BigEndian.Uint32(data[countOffset:])

		sizeOffset := countOffset + 4
		size := binary.BigEndian.Uint32(data[sizeOffset:])

		totalCount += count

		childrenHeaders[i] = ArraySlabHeader{
			slabID: slabID,
			count:  count,
			size:   size,
		}
		childrenCountSum[i] = totalCount

		offset += arraySlabHeaderSizeV0
	}

	// Compute slab size in version 1.
	slabSize := arrayMetaDataSlabPrefixSize + arraySlabHeaderSize*uint32(childHeaderCount)

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
		count:  totalCount,
	}

	return &ArrayMetaDataSlab{
		header:           header,
		childrenHeaders:  childrenHeaders,
		childrenCountSum: childrenCountSum,
		extraData:        extraData,
	}, nil
}

// newArrayMetaDataSlabFromDataV1 decodes data in version 1:
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+------------+--------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (12 bytes):
//
//	+------------------------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+--------------------------------+------------------------------+
//
// Content (n * 14 bytes):
//
//	[[slab index (8 bytes), count (4 bytes), size (2 bytes)], ...]
//
// See ArrayExtraData.Encode() for extra data section format.
func newArrayMetaDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayMetaDataSlab,
	error,
) {
	var err error
	var extraData *ArrayExtraData

	if h.isRoot() {
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap because err is already categorized by newArrayExtraDataFromData().
			return nil, err
		}
	}

	// Check minimum data length after version, flag, and extra data are processed
	minLength := arrayMetaDataSlabPrefixSize - versionAndFlagSize
	if len(data) < minLength {
		return nil, NewDecodingErrorf("data is too short for array metadata slab")
	}

	offset := 0

	// Decode shared address of headers
	var address Address
	copy(address[:], data[offset:])
	offset += SlabAddressLength

	// Decode number of child headers
	const arrayHeaderSize = 2
	childHeaderCount := binary.BigEndian.Uint16(data[offset:])
	offset += arrayHeaderSize

	expectedDataLength := arraySlabHeaderSize * int(childHeaderCount)
	if len(data[offset:]) != expectedDataLength {
		return nil, NewDecodingErrorf(
			"data has unexpected length %d, want %d",
			len(data),
			expectedDataLength,
		)
	}

	// Decode child headers
	childrenHeaders := make([]ArraySlabHeader, childHeaderCount)
	childrenCountSum := make([]uint32, childHeaderCount)
	totalCount := uint32(0)

	for i := 0; i < int(childHeaderCount); i++ {
		// Decode slab index
		var index SlabIndex
		copy(index[:], data[offset:])

		slabID := SlabID{address, index}
		offset += SlabIndexLength

		// Decode count
		count := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		// Decode size
		size := binary.BigEndian.Uint16(data[offset:])
		offset += 2

		childrenHeaders[i] = ArraySlabHeader{
			slabID: slabID,
			count:  count,
			size:   uint32(size),
		}

		totalCount += count
		childrenCountSum[i] = totalCount
	}

	slabSize := arrayMetaDataSlabPrefixSize + arraySlabHeaderSize*uint32(childHeaderCount)

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
		count:  totalCount,
	}

	return &ArrayMetaDataSlab{
		header:           header,
		childrenHeaders:  childrenHeaders,
		childrenCountSum: childrenCountSum,
		extraData:        extraData,
	}, nil
}
