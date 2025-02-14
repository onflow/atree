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

// NOTE: we use encoding size (in bytes) instead of Go type size for slab operations,
// such as merge and split, so size constants here are related to encoding size.
const (

	// version and flag size: version (1 byte) + flag (1 byte)
	versionAndFlagSize = 2

	// slab header size: slab index (8 bytes) + count (4 bytes) + size (2 bytes)
	// Support up to 4,294,967,295 elements in each array.
	// Support up to 65,535 bytes for slab size limit (default limit is 1536 max bytes).
	arraySlabHeaderSize = SlabIndexLength + 4 + 2

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + address (8 bytes) + child header count (2 bytes)
	// Support up to 65,535 children per metadata slab.
	arrayMetaDataSlabPrefixSize = versionAndFlagSize + SlabAddressLength + 2

	// Encoded element head in array data slab (fixed-size for easy computation).
	arrayDataSlabElementHeadSize = 3

	// non-root data slab prefix size: version (1 byte) + flag (1 byte) + next id (16 bytes) + element array head (3 bytes)
	// Support up to 65,535 elements in the array per data slab.
	arrayDataSlabPrefixSize = versionAndFlagSize + SlabIDLength + arrayDataSlabElementHeadSize

	// root data slab prefix size: version (1 byte) + flag (1 byte) + element array head (3 bytes)
	// Support up to 65,535 elements in the array per data slab.
	arrayRootDataSlabPrefixSize = versionAndFlagSize + arrayDataSlabElementHeadSize

	// inlined tag number size: CBOR tag number CBORTagInlinedArray or CBORTagInlinedMap
	inlinedTagNumSize = 2

	// inlined CBOR array head size: CBOR array head of 3 elements (extra data index, value id, elements)
	inlinedCBORArrayHeadSize = 1

	// inlined extra data index size: CBOR positive number encoded in 2 bytes [0, 255] (fixed-size for easy computation)
	inlinedExtraDataIndexSize = 2

	// inlined CBOR byte string head size for value ID: CBOR byte string head for byte string of 8 bytes
	inlinedCBORValueIDHeadSize = 1

	// inlined value id size: encoded in 8 bytes
	inlinedValueIDSize = 8

	// inlined array data slab prefix size:
	//   tag number (2 bytes) +
	//   3-element array head (1 byte) +
	//   extra data index (2 bytes) [0, 255] +
	//   value ID index head (1 byte) +
	//   value ID index (8 bytes) +
	//   element array head (3 bytes)
	inlinedArrayDataSlabPrefixSize = inlinedTagNumSize +
		inlinedCBORArrayHeadSize +
		inlinedExtraDataIndexSize +
		inlinedCBORValueIDHeadSize +
		inlinedValueIDSize +
		arrayDataSlabElementHeadSize

	maxInlinedExtraDataIndex = 255
)
