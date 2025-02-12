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

package main

import (
	"fmt"

	"github.com/onflow/atree"
)

const (
	reservedMinTagNum                 = 161
	reservedMinTagNumForContainerType = 230
	reservedMaxTagNum                 = 239
)

const (
	// CBOR tag numbers used to encode container types.
	// Replace _ when new tag number is needed (use lower tag numbers first).

	arrayTypeTagNum = reservedMinTagNumForContainerType + iota
	compositeTypeTagNum
	mapTypeTagNum
	_
	_
	_
	_
	_
	_
	_
)

func init() {
	// Check if the CBOR tag number range is reserved for internal use by atree.
	// Smoke tests must only use available (unreserved by atree) CBOR tag numbers
	// to encode elements in atree managed containers.

	// As of Aug 15, 2024:
	// - Atree reserves CBOR tag numbers [240, 255] for atree internal use.
	// - Smoke tests reserve CBOR tag numbers [161, 239] to encode elements.

	tagNumOK, err := atree.IsCBORTagNumberRangeAvailable(reservedMinTagNum, reservedMaxTagNum)
	if err != nil {
		panic(err)
	}

	if !tagNumOK {
		atreeMinTagNum, atreeMaxTagNum := atree.ReservedCBORTagNumberRange()
		panic(fmt.Errorf(
			"smoke test tag numbers [%d, %d] overlaps with atree internal tag numbers [%d, %d]",
			reservedMinTagNum,
			reservedMaxTagNum,
			atreeMinTagNum,
			atreeMaxTagNum))
	}
}
