/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/fxamacker/cbor/v2"
)

// Encoder writes atree internal and leaf node values to io.Writer.
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

type Decoder struct {
	io.Reader
	scratch [32]byte
}

func newDecoder(data []byte) *Decoder {
	return &Decoder{
		Reader: bytes.NewBuffer(data),
	}
}

func (dec *Decoder) newCBORDecoder(size uint32) (*cbor.StreamDecoder, error) {
	b := make([]byte, size)
	n, err := dec.Read(b)
	if err != nil {
		return nil, err
	}
	if uint32(n) != size {
		return nil, errors.New("data is too short")
	}
	return cbor.NewByteStreamDecoder(b), nil
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
		return StorageIDValue(n), nil

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
