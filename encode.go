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
	"io"
	"math"

	"github.com/fxamacker/cbor/v2"
)

// Encoder writes atree slabs to io.Writer.
type Encoder struct {
	io.Writer
	CBOR    *cbor.StreamEncoder
	Scratch [64]byte
}

func NewEncoder(w io.Writer, encMode cbor.EncMode) *Encoder {
	streamEncoder := encMode.NewStreamEncoder(w)
	return &Encoder{
		Writer: w,
		CBOR:   streamEncoder,
	}
}

type StorableDecoder func(
	decoder *cbor.StreamDecoder,
	storableSlabStorageID StorageID,
) (
	Storable,
	error,
)

func DecodeSlab(
	id StorageID,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	Slab,
	error,
) {
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short")
	}

	flag := data[1]

	dataType := getSlabType(flag)
	switch dataType {

	case slabArray:

		switch arrayDataType := getSlabArrayType(flag); arrayDataType {
		case slabArrayData:
			return newArrayDataSlabFromData(id, data, decMode, decodeStorable, decodeTypeInfo)
		case slabArrayMeta:
			return newArrayMetaDataSlabFromData(id, data, decMode, decodeTypeInfo)
		case slabBasicArray:
			return newBasicArrayDataSlabFromData(id, data, decMode, decodeStorable)
		default:
			return nil, NewDecodingErrorf("data has invalid flag 0x%x", flag)
		}

	case slabMap:

		switch mapDataType := getSlabMapType(flag); mapDataType {
		case slabMapData:
			return newMapDataSlabFromData(id, data, decMode, decodeStorable, decodeTypeInfo)
		case slabMapMeta:
			return newMapMetaDataSlabFromData(id, data, decMode, decodeTypeInfo)
		case slabMapCollisionGroup:
			return newMapDataSlabFromData(id, data, decMode, decodeStorable, decodeTypeInfo)
		default:
			return nil, NewDecodingErrorf("data has invalid flag 0x%x", flag)
		}

	case slabStorable:
		cborDec := decMode.NewByteStreamDecoder(data[versionAndFlagSize:])
		storable, err := decodeStorable(cborDec, id)
		if err != nil {
			return nil, NewDecodingError(err)
		}
		return StorableSlab{
			StorageID: id,
			Storable:  storable,
		}, nil

	default:
		return nil, NewDecodingErrorf("data has invalid flag 0x%x", flag)
	}
}

// TODO: make it inline
func GetUintCBORSize(n uint64) uint32 {
	if n <= 23 {
		return 1
	}
	if n <= math.MaxUint8 {
		return 2
	}
	if n <= math.MaxUint16 {
		return 3
	}
	if n <= math.MaxUint32 {
		return 5
	}
	return 9
}
