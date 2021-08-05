/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"errors"
	"fmt"
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
		Writer:  w,
		CBOR:    streamEncoder,
	}
}

type StorableDecoder func(
	decoder *cbor.StreamDecoder,
	storableSlabStorageID StorageID,
) (
	Storable,
	error,
)

func decodeSlab(id StorageID, data []byte, decMode cbor.DecMode, decodeStorable StorableDecoder) (Slab, error) {
	if len(data) < 2 {
		return nil, errors.New("data is too short")
	}
	flag := data[1]
	if flag&flagArray != 0 {

		if flag&flagMetaDataSlab != 0 {
			return newArrayMetaDataSlabFromData(id, data, decMode)
		}
		return newArrayDataSlabFromData(id, data, decMode, decodeStorable)

	} else if flag&flagBasicArray != 0 {
		return newBasicArrayDataSlabFromData(id, data, decMode, decodeStorable)
	} else if flag&flagStorable != 0 {
		const versionAndFlagSize = 2
		cborDec := decMode.NewByteStreamDecoder(data[versionAndFlagSize:])
		storable, err := decodeStorable(cborDec, id)
		if err != nil {
			return nil, err
		}
		return StorableSlab{
			StorageID: id,
			Storable:  storable,
		}, nil
	}
	return nil, fmt.Errorf("data has invalid flag %x", flag)
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
