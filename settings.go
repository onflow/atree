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

import "fmt"

// Slab invariants:
// - each element can't take up more than half of slab size (including encoding overhead and digest)
// - data slab must have at least 2 elements when slab size > maxThreshold

const (
	defaultSlabSize       = uint64(1024)
	minSlabSize           = uint64(256)
	minElementCountInSlab = 2
)

var (
	targetThreshold            uint64
	minThreshold               uint64
	maxThreshold               uint64
	MaxInlineArrayElementSize  uint64
	maxInlineMapElementSize    uint64
	MaxInlineMapKeyOrValueSize uint64
)

func init() {
	SetThreshold(defaultSlabSize)
}

func SetThreshold(threshold uint64) (uint64, uint64, uint64, uint64) {
	if threshold < minSlabSize {
		panic(fmt.Sprintf("Slab size %d is smaller than minSlabSize %d", threshold, minSlabSize))
	}

	targetThreshold = threshold
	minThreshold = uint64(targetThreshold / 2)
	maxThreshold = uint64(float64(targetThreshold) * 1.5)

	// Total slab size available for array elements, excluding slab encoding overhead
	availableArrayElementsSize := targetThreshold - arrayDataSlabPrefixSize
	MaxInlineArrayElementSize = uint64(availableArrayElementsSize / minElementCountInSlab)

	// Total slab size available for map elements, excluding slab encoding overhead
	availableMapElementsSize := targetThreshold - mapDataSlabPrefixSize - hkeyElementsPrefixSize

	// Total encoding overhead for one map element (key+value)
	mapElementOverheadSize := uint64(digestSize)

	// Max inline size for a map's element
	maxInlineMapElementSize = uint64(availableMapElementsSize/minElementCountInSlab) - mapElementOverheadSize

	// Max inline size for a map's key or value, excluding element encoding overhead
	MaxInlineMapKeyOrValueSize = uint64((maxInlineMapElementSize - singleElementPrefixSize) / 2)

	return minThreshold, maxThreshold, MaxInlineArrayElementSize, MaxInlineMapKeyOrValueSize
}
