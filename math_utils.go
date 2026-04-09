/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright Flow Foundation
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

import "math"

// safeAdd2Uint32 return sum of a and b.  It returns true if sum doesn't overflow uint32.
func safeAdd2Uint32(a, b uint32) (uint32, bool) {
	sum := uint64(a) + uint64(b)
	if sum > math.MaxUint32 {
		return 0, false
	}
	return uint32(sum), true
}

// safeAdd3Uint32 return sum of a, b and c.  It returns true if sum doesn't overflow uint32.
func safeAdd3Uint32(a, b, c uint32) (uint32, bool) {
	sum := uint64(a) + uint64(b) + uint64(c)
	if sum > math.MaxUint32 {
		return 0, false
	}
	return uint32(sum), true
}
