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

var (
	// Default slab size
	targetThreshold = uint64(1024) // 1kb

	minThreshold = targetThreshold / 2
	maxThreshold = uint64(float64(targetThreshold) * 1.5)

	MaxInlineElementSize = targetThreshold / 2

	maxInlineMapElementSize = MaxInlineElementSize / 2
)

func SetThreshold(threshold uint64) (uint64, uint64, uint64) {
	targetThreshold = threshold
	// minThreshold = targetThreshold / 4
	minThreshold = targetThreshold / 2
	maxThreshold = uint64(float64(targetThreshold) * 1.5)
	MaxInlineElementSize = targetThreshold / 2
	maxInlineMapElementSize = MaxInlineElementSize / 2

	return minThreshold, maxThreshold, MaxInlineElementSize
}
