/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

type slabType int

const (
	slabTypeUndefined slabType = iota
	slabArray
	slabMap
	slabStorable
)

type slabArrayType int

const (
	slabArrayUndefined slabArrayType = iota
	slabArrayData
	slabArrayMeta
	slabLargeImmutableArray
	slabBasicArray
)

const (
	// Slab flags: 3 high bits
	maskSlabRoot        byte = 0b100_00000
	maskSlabHasPointers byte = 0b010_00000
	maskSlabAnySize     byte = 0b001_00000

	// Array flags: 3 low bits (4th and 5th bits are 0)
	maskArrayData byte = 0b000_00000
	maskArrayMeta byte = 0b000_00001
	//maskLargeImmutableArray byte = 0b000_00010 // not used for now
	maskBasicArray byte = 0b000_00011 // used for benchmarking

	// Map flags: 3 low bits (4th bit is 0, 5th bit is 1)
	//maskMapData        byte = 0b000_01000
	//maskMapMeta        byte = 0b000_01001
	//maskLargeMapEntry  byte = 0b000_01010 // not used for now
	//maskCollisiongroup byte = 0b000_01011

	// Storable flags: 3 low bits (4th bit is 1, 5th bit is 1)
	maskStorable byte = 0b000_11111
)

func setRoot(f byte) byte {
	return f | maskSlabRoot
}

func setHasPointers(f byte) byte {
	return f | maskSlabHasPointers
}

func setNoSizeLimit(f byte) byte {
	return f | maskSlabAnySize
}

func isRoot(f byte) bool {
	return f&maskSlabRoot > 0
}

func hasPointers(f byte) bool {
	return f&maskSlabHasPointers > 0
}

func hasSizeLimit(f byte) bool {
	return f&maskSlabAnySize == 0
}

func getSlabType(f byte) slabType {
	dataType := (f & byte(0b000_11000)) >> 3
	switch dataType {
	case 0:
		// 4th and 5th bits are 0.
		return slabArray
	case 1:
		// 4th bit is 0 and 5th bit is 1.
		return slabMap
	case 3:
		// 4th and 5th bit are 1.
		return slabStorable
	default:
		return slabTypeUndefined
	}
}

func getSlabArrayType(f byte) slabArrayType {
	if getSlabType(f) != slabArray {
		return slabArrayUndefined
	}
	dataType := (f & byte(0b000_00111))
	switch dataType {
	case 0:
		return slabArrayData
	case 1:
		return slabArrayMeta
	case 2:
		return slabLargeImmutableArray
	case 3:
		return slabBasicArray
	default:
		return slabArrayUndefined
	}
}
