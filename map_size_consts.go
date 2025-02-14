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
	digestSize = 8

	// Encoded size of single element prefix size: CBOR array header (1 byte)
	singleElementPrefixSize = 1

	// Encoded size of inline collision group prefix size: CBOR tag number (2 bytes)
	inlineCollisionGroupPrefixSize = 2

	// Encoded size of external collision group prefix size: CBOR tag number (2 bytes)
	externalCollisionGroupPrefixSize = 2

	// Encoded size of digests: CBOR byte string head (3 bytes)
	digestPrefixSize = 3

	// Encoded size of number of elements: CBOR array head (3 bytes).
	elementPrefixSize = 3

	// hkey elements prefix size:
	// CBOR array header (1 byte) + level (1 byte) + hkeys byte string header (3 bytes) + elements array header (3 bytes)
	// Support up to 8,191 elements in the map per data slab.
	hkeyElementsPrefixSize = 1 + 1 + digestPrefixSize + elementPrefixSize

	// single elements prefix size:
	// CBOR array header (1 byte) + encoded level (1 byte) + hkeys byte string header (1 bytes) + elements array header (3 bytes)
	// Support up to 65,535 elements in the map per data slab.
	singleElementsPrefixSize = 1 + 1 + 1 + elementPrefixSize

	// slab header size: slab index (8 bytes) + size (2 bytes) + first digest (8 bytes)
	// Support up to 65,535 bytes for slab size limit (default limit is 1536 max bytes).
	mapSlabHeaderSize = SlabIndexLength + 2 + digestSize

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + address (8 bytes) + child header count (2 bytes)
	// Support up to 65,535 children per metadata slab.
	mapMetaDataSlabPrefixSize = versionAndFlagSize + SlabAddressLength + 2

	// version (1 byte) + flag (1 byte) + next id (16 bytes)
	mapDataSlabPrefixSize = versionAndFlagSize + SlabIDLength

	// version (1 byte) + flag (1 byte)
	mapRootDataSlabPrefixSize = versionAndFlagSize

	// maxDigestLevel is max levels of 64-bit digests allowed
	maxDigestLevel = 8

	// inlined map data slab prefix size:
	//   tag number (2 bytes) +
	//   3-element array head (1 byte) +
	//   extra data ref index (2 bytes) [0, 255] +
	//   value index head (1 byte) +
	//   value index (8 bytes)
	inlinedMapDataSlabPrefixSize = inlinedTagNumSize +
		inlinedCBORArrayHeadSize +
		inlinedExtraDataIndexSize +
		inlinedCBORValueIDHeadSize +
		inlinedValueIDSize
)
