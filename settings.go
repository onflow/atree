/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

var (
	// Default slab size
	targetThreshold = uint64(1024) // 1kb

	// minThreshold = targetThreshold / 4
	minThreshold = targetThreshold / 2
	maxThreshold = uint64(float64(targetThreshold) * 1.5)

	MaxInlineElementSize = targetThreshold / 2
)

func SetThreshold(threshold uint64) (uint64, uint64, uint64) {
	targetThreshold = threshold
	// minThreshold = targetThreshold / 4
	minThreshold = targetThreshold / 2
	maxThreshold = uint64(float64(targetThreshold) * 1.5)
	MaxInlineElementSize = targetThreshold / 2

	return minThreshold, maxThreshold, MaxInlineElementSize
}
