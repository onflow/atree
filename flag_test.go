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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlagIsRoot(t *testing.T) {
	testCases := []struct {
		name string
		h    head
	}{
		{"v0", head([2]byte{})},
		{"v1", head([2]byte{1 << 4, 0x0})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i <= 255; i++ {
				tc.h[1] = byte(i)
				if i >= 1<<7 {
					require.True(t, tc.h.isRoot())
				} else {
					require.False(t, tc.h.isRoot())
				}
			}
		})
	}
}

func TestFlagSetRootV1(t *testing.T) {
	var h head
	h[0] = 1 << 4 // version 1

	for i := 0; i <= 255; i++ {
		h[1] = byte(i)
		h.setRoot()
		require.True(t, h.isRoot())
	}
}

func TestFlagHasPointers(t *testing.T) {
	testCases := []struct {
		name string
		h    head
	}{
		{"v0", head([2]byte{})},
		{"v1", head([2]byte{1 << 4, 0x0})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i <= 255; i++ {
				tc.h[1] = byte(i)

				if byte(i)&maskSlabHasPointers != 0 {
					require.True(t, tc.h.hasPointers())
				} else {
					require.False(t, tc.h.hasPointers())
				}
			}
		})
	}
}

func TestFlagSetHasPointersV1(t *testing.T) {
	var h head
	h[0] = 1 << 4 // version 1

	for i := 0; i <= 255; i++ {
		h[1] = byte(i)
		h.setHasPointers()

		require.True(t, h.hasPointers())
	}
}

func TestFlagHasSizeLimit(t *testing.T) {
	testCases := []struct {
		name string
		h    head
	}{
		{"v0", head([2]byte{})},
		{"v1", head([2]byte{1 << 4, 0x0})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i <= 255; i++ {
				tc.h[1] = byte(i)

				if byte(i)&maskSlabAnySize == 0 {
					require.True(t, tc.h.hasSizeLimit())
				} else {
					require.False(t, tc.h.hasSizeLimit())
				}
			}
		})
	}
}

func TestFlagSetNoSizeLimitV1(t *testing.T) {
	var h head
	h[0] = 1 << 4 // version 1

	for i := 0; i <= 255; i++ {
		h[1] = byte(i)

		h.setNoSizeLimit()
		require.False(t, h.hasSizeLimit())
	}
}

func TestFlagHasNextSlabID(t *testing.T) {
	var h head
	h[0] = 1 << 4 // v1

	t.Run("has", func(t *testing.T) {
		// Flags in the first byte
		for i := 0; i < 32; i++ {
			h[0] |= byte(i)
			h[0] |= maskHasNextSlabID

			// Flags in the second byte
			for j := 0; j <= 255; j++ {
				h[1] = byte(j)
				require.True(t, h.hasNextSlabID())
			}
		}
	})

	t.Run("doesn't have", func(t *testing.T) {
		// Flags in the first byte
		for i := 0; i < 32; i++ {
			h[0] |= byte(i)
			h[0] &= ^maskHasNextSlabID

			// Flags in the second byte
			for j := 0; j <= 255; j++ {
				h[1] = byte(j)
				require.False(t, h.hasNextSlabID())
			}
		}
	})
}

func TestFlagSetHasNextSlabIDV1(t *testing.T) {
	var h head
	h[0] = 1 << 4 // version 1

	// Flags in the first byte
	for i := 0; i < 32; i++ {
		h[0] |= byte(i)

		// Flags in the second byte
		for i := 0; i <= 255; i++ {
			h[1] = byte(i)

			h.setHasNextSlabID()
			require.True(t, h.hasNextSlabID())
		}
	}
}

func TestFlagHasInlinedSlabs(t *testing.T) {
	var h head
	h[0] = 1 << 4 // v1

	t.Run("has", func(t *testing.T) {
		// Flags in the first byte
		for i := 0; i < 32; i++ {
			h[0] |= byte(i)
			h[0] |= maskHasInlinedSlabs

			// Flags in the second byte
			for j := 0; j <= 255; j++ {
				h[1] = byte(j)
				require.True(t, h.hasInlinedSlabs())
			}
		}
	})

	t.Run("doesn't have", func(t *testing.T) {
		// Flags in the first byte
		for i := 0; i < 32; i++ {
			h[0] |= byte(i)
			h[0] &= ^maskHasInlinedSlabs

			// Flags in the second byte
			for j := 0; j <= 255; j++ {
				h[1] = byte(j)
				require.False(t, h.hasInlinedSlabs())
			}
		}
	})
}

func TestFlagSetHasInlinedSlabsV1(t *testing.T) {
	var h head
	h[0] = 1 << 4 // version 1

	// Flags in the first byte
	for i := 0; i < 32; i++ {
		h[0] |= byte(i)

		// Flags in the second byte
		for i := 0; i <= 255; i++ {
			h[1] = byte(i)

			h.setHasInlinedSlabs()
			require.True(t, h.hasInlinedSlabs())
		}
	}
}

func TestFlagGetSlabType(t *testing.T) {
	testCases := []struct {
		name string
		h    head
	}{
		{"v0", head([2]byte{})},
		{"v1", head([2]byte{1 << 4, 0x0})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i <= 255; i++ {
				arrayFlag := byte(i) & 0b111_00111
				tc.h[1] = arrayFlag
				require.Equal(t, slabArray, tc.h.getSlabType())

				mapFlag := arrayFlag | 0b000_01000
				tc.h[1] = mapFlag
				require.Equal(t, slabMap, tc.h.getSlabType())

				storableFlag := arrayFlag | 0b000_11111
				tc.h[1] = storableFlag
				require.Equal(t, slabStorable, tc.h.getSlabType())
			}
		})
	}
}

func TestFlagGetSlabArrayType(t *testing.T) {
	testCases := []struct {
		name string
		h    head
	}{
		{"v0", head([2]byte{})},
		{"v1", head([2]byte{1 << 4, 0x0})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i <= 255; i++ {
				arrayDataFlag := byte(i) & 0b111_00000
				tc.h[1] = arrayDataFlag
				require.Equal(t, slabArrayData, tc.h.getSlabArrayType())

				arrayMetaFlag := arrayDataFlag | 0b000_00001
				tc.h[1] = arrayMetaFlag
				require.Equal(t, slabArrayMeta, tc.h.getSlabArrayType())

				arrayLargeImmutableArrayFlag := arrayDataFlag | 0b000_00010
				tc.h[1] = arrayLargeImmutableArrayFlag
				require.Equal(t, slabLargeImmutableArray, tc.h.getSlabArrayType())

				basicArrayFlag := arrayDataFlag | 0b000_00011
				tc.h[1] = basicArrayFlag
				require.Equal(t, slabBasicArray, tc.h.getSlabArrayType())
			}
		})
	}
}

func TestFlagGetSlabMapType(t *testing.T) {
	testCases := []struct {
		name string
		h    head
	}{
		{"v0", head([2]byte{})},
		{"v1", head([2]byte{1 << 4, 0x0})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i <= 255; i++ {
				b := byte(i)
				b |= 0b000_01000 // turn on map flag
				b &= 0b111_01111 // turn off storable flag

				mapDataFlag := b & 0b111_11000
				tc.h[1] = mapDataFlag
				require.Equal(t, slabMapData, tc.h.getSlabMapType())

				mapMetaFlag := mapDataFlag | 0b000_00001
				tc.h[1] = mapMetaFlag
				require.Equal(t, slabMapMeta, tc.h.getSlabMapType())

				mapLargeImmutableArrayFlag := mapDataFlag | 0b000_00010
				tc.h[1] = mapLargeImmutableArrayFlag
				require.Equal(t, slabMapLargeEntry, tc.h.getSlabMapType())

				collisionGroupFlag := mapDataFlag | 0b000_00011
				tc.h[1] = collisionGroupFlag
				require.Equal(t, slabMapCollisionGroup, tc.h.getSlabMapType())
			}
		})
	}
}

func TestVersion(t *testing.T) {
	t.Run("v0", func(t *testing.T) {
		const expectedVersion = byte(0)

		var h head
		// Flags in the second byte
		for i := 0; i <= 255; i++ {
			h[1] = byte(i)
			require.Equal(t, expectedVersion, h.version())
		}
	})

	t.Run("v1", func(t *testing.T) {
		const expectedVersion = byte(1)

		var h head
		h[0] = 0x10

		// Flags in the first byte
		for i := 0; i < 32; i++ {
			h[0] |= byte(i)

			// Flags in the second byte
			for j := 0; j <= 255; j++ {
				h[1] = byte(j)
				require.Equal(t, expectedVersion, h.version())
			}
		}
	})
}
