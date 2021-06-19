package main

import (
	"flag"
	"fmt"
)

var (
	// Default slab size
	targetThreshold = uint64(1024) // 1kb

	// minThreshold = targetThreshold / 4
	minThreshold = targetThreshold / 2
	maxThreshold = uint64(float64(targetThreshold) * 1.5)
)

func setThreshold(threshold uint64) {
	targetThreshold = threshold
	// minThreshold = targetThreshold / 4
	minThreshold = targetThreshold / 2
	maxThreshold = uint64(float64(targetThreshold) * 1.5)
}

type Uint8Value uint8

func (v Uint8Value) ByteSize() uint32 {
	return 1
}

type Uint32Value uint32

func (v Uint32Value) ByteSize() uint32 {
	return 4
}

type Uint64Value uint64

func (v Uint64Value) ByteSize() uint32 {
	return 8
}

// TODO: implement different slab size for internal node and leaf node.
func main() {
	var slabSize uint64
	var numElements uint64
	var verbose bool

	flag.Uint64Var(&slabSize, "size", 1024, "slab size in bytes")
	flag.Uint64Var(&numElements, "count", 500, "number of elements in array")
	flag.BoolVar(&verbose, "verbose", false, "verbose output")

	flag.Parse()

	setThreshold(slabSize)

	fmt.Printf("Inserting %d elements (uint64) into array with slab size %d, min size %d, and max size %d ...\n", numElements, targetThreshold, minThreshold, maxThreshold)

	storage := NewBasicStorage()

	array := NewArray(storage)

	for i := uint64(0); i < numElements; i++ {
		array.Append(Uint64Value(i))
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
