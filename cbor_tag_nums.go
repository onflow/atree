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

import "fmt"

const (
	// WARNING: tag numbers defined in here in github.com/onflow/atree
	// MUST not overlap with tag numbers used by Cadence internal value encoding.
	// As of Aug. 14, 2024, Cadence uses tag numbers from 128 to 230.
	// See runtime/interpreter/encode.go at github.com/onflow/cadence.

	// Atree reserves CBOR tag numbers [240, 255] for internal use.
	// Applications must use non-overlapping CBOR tag numbers to encode
	// elements managed by atree containers.
	minInternalCBORTagNumber = 240
	maxInternalCBORTagNumber = 255

	// Reserved CBOR tag numbers for atree internal use.

	// Replace _ when new tag number is needed (use higher tag numbers first).
	// Atree will use higher tag numbers first because Cadence will use lower tag numbers first.
	// This approach allows more flexibility in case we need to revisit ranges used by Atree and Cadence.

	_ = 240
	_ = 241
	_ = 242
	_ = 243
	_ = 244
	_ = 245

	CBORTagTypeInfoRef = 246

	CBORTagInlinedArrayExtraData      = 247
	CBORTagInlinedMapExtraData        = 248
	CBORTagInlinedCompactMapExtraData = 249

	CBORTagInlinedArray      = 250
	CBORTagInlinedMap        = 251
	CBORTagInlinedCompactMap = 252

	CBORTagInlineCollisionGroup   = 253
	CBORTagExternalCollisionGroup = 254

	CBORTagSlabID = 255
)

// IsCBORTagNumberRangeAvailable returns true if the specified range is not reserved for internal use by atree.
// Applications must only use available (unreserved) CBOR tag numbers to encode elements in atree managed containers.
func IsCBORTagNumberRangeAvailable(minTagNum, maxTagNum uint64) (bool, error) {
	if minTagNum > maxTagNum {
		return false, NewUserError(fmt.Errorf("min CBOR tag number %d must be <= max CBOR tag number %d", minTagNum, maxTagNum))
	}

	return maxTagNum < minInternalCBORTagNumber || minTagNum > maxInternalCBORTagNumber, nil
}

// ReservedCBORTagNumberRange returns minTagNum and maxTagNum of the range of CBOR tag numbers
// reserved for internal use by atree.
func ReservedCBORTagNumberRange() (minTagNum, maxTagNum uint64) {
	return minInternalCBORTagNumber, maxInternalCBORTagNumber
}
