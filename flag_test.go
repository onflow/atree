/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlagIsRoot(t *testing.T) {
	for i := 0; i <= 255; i++ {
		if i >= 0x80 {
			require.True(t, isRoot(byte(i)))
		} else {
			require.False(t, isRoot(byte(i)))
		}
	}
}

func TestFlagSetRoot(t *testing.T) {
	for i := 0; i <= 255; i++ {
		require.True(t, isRoot(setRoot(byte(i))))
	}
}

func TestFlagHasPointers(t *testing.T) {
	for i := 0; i <= 255; i++ {
		if byte(i)&maskSlabHasPointers != 0 {
			require.True(t, hasPointers(byte(i)))
		} else {
			require.False(t, hasPointers(byte(i)))
		}
	}
}

func TestFlagSetHasPointers(t *testing.T) {
	for i := 0; i <= 255; i++ {
		require.True(t, hasPointers(setHasPointers(byte(i))))
	}
}

func TestFlagHasSizeLimit(t *testing.T) {
	for i := 0; i <= 255; i++ {
		if byte(i)&maskSlabAnySize == 0 {
			require.True(t, hasSizeLimit(byte(i)))
		} else {
			require.False(t, hasSizeLimit(byte(i)))
		}
	}
}

func TestFlagSetNoSizeLimit(t *testing.T) {
	for i := 0; i <= 255; i++ {
		f := setNoSizeLimit(byte(i))
		require.False(t, hasSizeLimit(f))
	}
}

func TestFlagGetSlabType(t *testing.T) {
	for i := 0; i <= 255; i++ {
		arrayFlag := byte(i) & 0b111_00111
		mapFlag := arrayFlag | 0b000_01000
		storableFlag := mapFlag | 0b000_11111

		require.Equal(t, slabArray, getSlabType(arrayFlag))
		require.Equal(t, slabMap, getSlabType(mapFlag))
		require.Equal(t, slabStorable, getSlabType(storableFlag))
	}
}

func TestFlagGetSlabArrayType(t *testing.T) {
	for i := 0; i <= 255; i++ {
		arrayDataFlag := byte(i) & 0b111_00000
		arrayMetaFlag := arrayDataFlag | 0b000_00001
		arrayLargeImmutableArrayFlag := arrayDataFlag | 0b000_00010
		basicArrayFlag := arrayDataFlag | 0b000_00011

		require.Equal(t, slabArrayData, getSlabArrayType(arrayDataFlag))
		require.Equal(t, slabArrayMeta, getSlabArrayType(arrayMetaFlag))
		require.Equal(t, slabLargeImmutableArray, getSlabArrayType(arrayLargeImmutableArrayFlag))
		require.Equal(t, slabBasicArray, getSlabArrayType(basicArrayFlag))
	}
}
