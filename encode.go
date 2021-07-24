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
	cbor    *cbor.StreamEncoder
	scratch [32]byte
}

func newEncoder(w io.Writer) *Encoder {
	return &Encoder{
		Writer: w,
		cbor:   cbor.NewStreamEncoder(w),
	}
}

func decodeSlab(id StorageID, data []byte) (Slab, error) {
	if len(data) < 2 {
		return nil, errors.New("data is too short")
	}
	flag := data[1]
	if flag&flagArray != 0 {

		if flag&flagMetaDataSlab != 0 {
			return newArrayMetaDataSlabFromData(id, data)
		}
		return newArrayDataSlabFromData(id, data)

	} else if flag&flagBasicArray != 0 {
		return newBasicArrayDataSlabFromData(id, data)
	} else if flag&flagStorable != 0 {
		const versionAndFlagSize = 2
		cborDec := NewByteStreamDecoder(data[versionAndFlagSize:])
		storable, err := decodeStorable(cborDec)
		if err != nil {
			return nil, err
		}
		return StorableSlab{Storable: storable}, nil
	}
	return nil, fmt.Errorf("data has invalid flag %x", flag)
}

func decodeStorable(dec *cbor.StreamDecoder) (Storable, error) {
	tagNumber, err := dec.DecodeTagNumber()
	if err != nil {
		return nil, err
	}

	switch tagNumber {
	case cborTagStorageID:
		n, err := dec.DecodeUint64()
		if err != nil {
			return nil, err
		}
		return StorageIDStorable(n), nil

	case cborTagUInt8Value:
		n, err := dec.DecodeUint64()
		if err != nil {
			return nil, err
		}
		if n > math.MaxUint8 {
			return nil, fmt.Errorf("invalid data, got %d, expected max %d", n, math.MaxUint8)
		}
		return Uint8Value(n), nil

	case cborTagUInt16Value:
		n, err := dec.DecodeUint64()
		if err != nil {
			return nil, err
		}
		if n > math.MaxUint16 {
			return nil, fmt.Errorf("invalid data, got %d, expected max %d", n, math.MaxUint16)
		}
		return Uint16Value(n), nil

	case cborTagUInt32Value:
		n, err := dec.DecodeUint64()
		if err != nil {
			return nil, err
		}
		if n > math.MaxUint32 {
			return nil, fmt.Errorf("invalid data, got %d, expected max %d", n, math.MaxUint32)
		}
		return Uint32Value(n), nil

	case cborTagUInt64Value:
		n, err := dec.DecodeUint64()
		if err != nil {
			return nil, err
		}
		return Uint64Value(n), nil

	default:
		return nil, fmt.Errorf("invalid tag number %d", tagNumber)
	}
}

// TODO: make it inline
func getUintCBORSize(n uint64) uint32 {
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
