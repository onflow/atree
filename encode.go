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
	encMode cbor.EncMode
}

func NewEncoder(w io.Writer, encMode cbor.EncMode) *Encoder {
	streamEncoder := encMode.NewStreamEncoder(w)
	return &Encoder{
		Writer:  w,
		CBOR:    streamEncoder,
		encMode: encMode,
	}
}

// EncodeStorableAsElement encodes storable as Array or OrderedMap element.
// Storable is encode as an inlined ArrayDataSlab or MapDataSlab if it is ArrayDataSlab or MapDataSlab.
func EncodeStorableAsElement(enc *Encoder, storable Storable, inlinedTypeInfo *InlinedExtraData) error {

	switch storable := storable.(type) {

	case ContainerStorable:
		err := storable.EncodeAsElement(enc, inlinedTypeInfo)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ContainerStorable interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode container storable as element")
		}

	default:
		err := storable.Encode(enc)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode storable as element")
		}
	}

	return nil
}

type StorableDecoder func(
	decoder *cbor.StreamDecoder,
	storableSlabID SlabID,
	inlinedExtraData []ExtraData,
) (
	Storable,
	error,
)

func DecodeSlab(
	id SlabID,
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

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	switch h.getSlabType() {

	case slabArray:

		arrayDataType := h.getSlabArrayType()

		switch arrayDataType {
		case slabArrayData:
			return newArrayDataSlabFromData(id, data, decMode, decodeStorable, decodeTypeInfo)
		case slabArrayMeta:
			return newArrayMetaDataSlabFromData(id, data, decMode, decodeTypeInfo)
		case slabBasicArray:
			return newBasicArrayDataSlabFromData(id, data, decMode, decodeStorable)
		default:
			return nil, NewDecodingErrorf("data has invalid head 0x%x", h[:])
		}

	case slabMap:

		mapDataType := h.getSlabMapType()

		switch mapDataType {
		case slabMapData:
			return newMapDataSlabFromData(id, data, decMode, decodeStorable, decodeTypeInfo)
		case slabMapMeta:
			return newMapMetaDataSlabFromData(id, data, decMode, decodeTypeInfo)
		case slabMapCollisionGroup:
			return newMapDataSlabFromData(id, data, decMode, decodeStorable, decodeTypeInfo)
		default:
			return nil, NewDecodingErrorf("data has invalid head 0x%x", h[:])
		}

	case slabStorable:
		cborDec := decMode.NewByteStreamDecoder(data[versionAndFlagSize:])
		storable, err := decodeStorable(cborDec, id, nil)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode slab storable")
		}
		return &StorableSlab{
			slabID:   id,
			storable: storable,
		}, nil

	default:
		return nil, NewDecodingErrorf("data has invalid head 0x%x", h[:])
	}
}

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
