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

	cbor "github.com/fxamacker/cbor/v2/cborstream"
)

func newMapMetaDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapMetaDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for map metadata slab")
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if h.getSlabMapType() != slabMapMeta {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want map metadata slab flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newMapMetaDataSlabFromDataV0(id, h, data, decMode, decodeTypeInfo)

	case 1:
		return newMapMetaDataSlabFromDataV1(id, h, data, decMode, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for map metadata slab", h.version())
	}
}

// newMapMetaDataSlabFromDataV0 decodes data in version 0:
//
// Root MetaDataSlab Header:
//
//	+-------------------------------+------------+-------------------------------+------------------------------+
//	| slab version + flag (2 bytes) | extra data | slab version + flag (2 bytes) | child header count (2 bytes) |
//	+-------------------------------+------------+-------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (4 bytes):
//
//	+-------------------------------+------------------------------+
//	| slab version + flag (2 bytes) | child header count (2 bytes) |
//	+-------------------------------+------------------------------+
//
// Content (n * 28 bytes):
//
//	[ +[slab ID (16 bytes), first key (8 bytes), size (4 bytes)]]
//
// See MapExtraData.Encode() for extra data section format.
func newMapMetaDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (*MapMetaDataSlab, error) {
	const (
		mapMetaDataArrayHeadSizeV0 = 2
		mapSlabHeaderSizeV0        = SlabIDLength + 4 + digestSize
	)

	var err error
	var extraData *MapExtraData

	// Check flag for extra data
	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		data = data[versionAndFlagSize:]
	}

	// Check data length (after decoding extra data if present)
	if len(data) < mapMetaDataArrayHeadSizeV0 {
		return nil, NewDecodingErrorf("data is too short for map metadata slab")
	}

	// Decode number of child headers
	childHeaderCount := binary.BigEndian.Uint16(data)
	data = data[mapMetaDataArrayHeadSizeV0:]

	expectedDataLength := mapSlabHeaderSizeV0 * int(childHeaderCount)
	if len(data) != expectedDataLength {
		return nil, NewDecodingErrorf(
			"data has unexpected length %d, want %d",
			len(data),
			expectedDataLength,
		)
	}

	// Decode child headers
	childrenHeaders := make([]MapSlabHeader, childHeaderCount)
	offset := 0

	for i := range childrenHeaders {
		slabID, err := NewSlabIDFromRawBytes(data[offset:])
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		firstKeyOffset := offset + SlabIDLength
		firstKey := binary.BigEndian.Uint64(data[firstKeyOffset:])

		sizeOffset := firstKeyOffset + digestSize
		size := binary.BigEndian.Uint32(data[sizeOffset:])

		childrenHeaders[i] = MapSlabHeader{
			slabID:   slabID,
			size:     size,
			firstKey: Digest(firstKey),
		}

		offset += mapSlabHeaderSizeV0
	}

	var firstKey Digest
	if len(childrenHeaders) > 0 {
		firstKey = childrenHeaders[0].firstKey
	}

	// Compute slab size in version 1.
	slabSize := mapMetaDataSlabPrefixSize + mapSlabHeaderSize*uint32(childHeaderCount)

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: firstKey,
	}

	return &MapMetaDataSlab{
		header:          header,
		childrenHeaders: childrenHeaders,
		extraData:       extraData,
	}, nil
}

// newMapMetaDataSlabFromDataV1 decodes data in version 1:
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+------------+--------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (4 bytes):
//
//	+------------------------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+--------------------------------+------------------------------+
//
// Content (n * 18 bytes):
//
//	[ +[slab index (8 bytes), first key (8 bytes), size (2 bytes)]]
//
// See MapExtraData.Encode() for extra data section format.
func newMapMetaDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (*MapMetaDataSlab, error) {

	var err error
	var extraData *MapExtraData

	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}
	}

	// Check minimum data length after version, flag, and extra data are processed
	minLength := mapMetaDataSlabPrefixSize - versionAndFlagSize
	if len(data) < minLength {
		return nil, NewDecodingErrorf("data is too short for map metadata slab")
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

	expectedDataLength := mapSlabHeaderSize * int(childHeaderCount)
	if len(data[offset:]) != expectedDataLength {
		return nil, NewDecodingErrorf(
			"data has unexpected length %d, want %d",
			len(data),
			expectedDataLength,
		)
	}

	// Decode child headers
	childrenHeaders := make([]MapSlabHeader, childHeaderCount)

	for i := range childrenHeaders {
		// Decode slab index
		var index SlabIndex
		copy(index[:], data[offset:])
		offset += SlabIndexLength

		// Decode first key
		firstKey := binary.BigEndian.Uint64(data[offset:])
		offset += digestSize

		// Decode size
		size := binary.BigEndian.Uint16(data[offset:])
		offset += 2

		childrenHeaders[i] = MapSlabHeader{
			slabID:   SlabID{address, index},
			size:     uint32(size),
			firstKey: Digest(firstKey),
		}
	}

	var firstKey Digest
	if len(childrenHeaders) > 0 {
		firstKey = childrenHeaders[0].firstKey
	}

	slabSize := mapMetaDataSlabPrefixSize + mapSlabHeaderSize*uint32(childHeaderCount)

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: firstKey,
	}

	return &MapMetaDataSlab{
		header:          header,
		childrenHeaders: childrenHeaders,
		extraData:       extraData,
	}, nil
}
