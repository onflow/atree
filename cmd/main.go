/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"flag"
	"fmt"

	"github.com/fxamacker/atree"
)


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

	storage := atree.NewBasicSlabStorage()

	array, err := atree.NewArray(storage)
	if err != nil {
		fmt.Println(err)
		return
	}

	for i := uint64(0); i < numElements; i++ {
		err := array.Append(atree.Uint64Value(i))
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
