/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"flag"
	"fmt"

	"github.com/fxamacker/atree"
	"github.com/fxamacker/cbor/v2"
)

const cborTagUInt64Value = 164

type Uint64Value uint64

var _ atree.Value = Uint64Value(0)
var _ atree.Storable = Uint64Value(0)

func (v Uint64Value) DeepCopy(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint64Value) StoredValue(_ atree.SlabStorage) (atree.Value, error) {
	return v, nil
}

func (v Uint64Value) Storable(atree.SlabStorage) atree.Storable {
	return v
}

// Encode encodes UInt64Value as
// cbor.Tag{
//		Number:  cborTagUInt64Value,
//		Content: uint64(v),
// }
func (v Uint64Value) Encode(enc *atree.Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, cborTagUInt64Value,
	})
	if err != nil {
		return err
	}
	return enc.CBOR.EncodeUint64(uint64(v))
}

// TODO: cache size
func (v Uint64Value) ByteSize() uint32 {
	// tag number (2 bytes) + encoded content
	return 2 + atree.GetUintCBORSize(uint64(v))
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

func decodeStorable(dec *cbor.StreamDecoder, _ atree.StorageID) (atree.Storable, error) {
	tagNumber, err := dec.DecodeTagNumber()
	if err != nil {
		return nil, err
	}

	switch tagNumber {
	case atree.CBORTagStorageID:
		return atree.DecodeStorageIDStorable(dec)

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

// TODO: implement different slab size for metadata slab and data slab.
func main() {
	var slabSize uint64
	var numElements uint64
	var verbose bool

	flag.Uint64Var(&slabSize, "size", 1024, "slab size in bytes")
	flag.Uint64Var(&numElements, "count", 500, "number of elements in array")
	flag.BoolVar(&verbose, "verbose", false, "verbose output")

	flag.Parse()

	targetThreshold, minThreshold, maxThreshold := atree.SetThreshold(slabSize)

	fmt.Printf(
		"Inserting %d elements (uint64) into array with slab size %d, min size %d, and max size %d ...\n",
		numElements,
		targetThreshold,
		minThreshold,
		maxThreshold,
	)

	encMode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		fmt.Println(err)
		return
	}

	storage := atree.NewBasicSlabStorage(encMode)
	storage.DecodeStorable = decodeStorable

	array, err := atree.NewArray(storage)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := uint64(0); i < numElements; i++ {
		err := array.Append(Uint64Value(i))
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	stats, err := array.Stats()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("%+v\n", stats)

	if verbose {
		fmt.Printf("\n\n=========== array layout ===========\n")
		array.Print()
	}
}
