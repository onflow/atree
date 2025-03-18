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

import (
	"bytes"
	"fmt"
	"reflect"

	cbor "github.com/fxamacker/cbor/v2/cborstream"
)

// VerifyMapSerialization traverses ordered map tree and verifies serialization
// by encoding, decoding, and re-encoding slabs.
// It compares in-memory objects of original slab with decoded slab.
// It also compares encoded data of original slab with encoded data of decoded slab.
func VerifyMapSerialization(
	m *OrderedMap,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {
	// Skip verification of inlined map serialization.
	if m.Inlined() {
		return nil
	}

	v := &serializationVerifier{
		storage:        m.Storage,
		cborDecMode:    cborDecMode,
		cborEncMode:    cborEncMode,
		decodeStorable: decodeStorable,
		decodeTypeInfo: decodeTypeInfo,
		compare:        compare,
	}
	return v.verifyMapSlab(m.root)
}

func (v *serializationVerifier) verifyMapSlab(slab MapSlab) error {

	id := slab.SlabID()

	// Encode slab
	data, err := EncodeSlab(slab, v.cborEncMode)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Encode().
		return err
	}

	// Decode encoded slab
	decodedSlab, err := DecodeSlab(id, data, v.cborDecMode, v.decodeStorable, v.decodeTypeInfo)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by DecodeSlab().
		return err
	}

	// Re-encode decoded slab
	dataFromDecodedSlab, err := EncodeSlab(decodedSlab, v.cborEncMode)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Encode().
		return err
	}

	// Verify encoding is deterministic (encoded data of original slab is same as encoded data of decoded slab)
	if !bytes.Equal(data, dataFromDecodedSlab) {
		return NewFatalError(fmt.Errorf("encoded data of original slab %s is different from encoded data of decoded slab, got %v, want %v",
			id, dataFromDecodedSlab, data))
	}

	// Extra check: encoded data size == header.size
	// This check is skipped for slabs with inlined compact map because
	// encoded size and slab size differ for inlined composites.
	// For inlined composites, digests and field keys are encoded in
	// compact map extra data section for reuse, and only compact map field
	// values are encoded in non-extra data section.
	// This reduces encoding size because compact map values of the same
	// compact map type can reuse encoded type info, seed, digests, and field names.
	// TODO: maybe add size check for slabs with inlined compact map by decoding entire slab.
	inlinedComposite, err := hasInlinedComposite(data)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by hasInlinedComposite().
		return err
	}
	if !inlinedComposite {
		encodedSlabSize, err := computeSize(data)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by computeSize().
			return err
		}

		if slab.Header().size != uint32(encodedSlabSize) {
			return NewFatalError(
				fmt.Errorf("slab %d encoded size %d != header.size %d",
					id, encodedSlabSize, slab.Header().size))
		}
	}

	switch slab := slab.(type) {
	case *MapDataSlab:
		decodedDataSlab, ok := decodedSlab.(*MapDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("decoded slab %d is not MapDataSlab", id))
		}

		// Compare slabs
		err = v.mapDataSlabEqual(slab, decodedDataSlab)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by mapDataSlabEqual().
			return fmt.Errorf("data slab %d round-trip serialization failed: %w", id, err)
		}

		return nil

	case *MapMetaDataSlab:
		decodedMetaSlab, ok := decodedSlab.(*MapMetaDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("decoded slab %d is not MapMetaDataSlab", id))
		}

		// Compare slabs
		err = v.mapMetaDataSlabEqual(slab, decodedMetaSlab)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by mapMetaDataSlabEqual().
			return fmt.Errorf("metadata slab %d round-trip serialization failed: %w", id, err)
		}

		for _, h := range slab.childrenHeaders {
			slab, err := getMapSlab(v.storage, h.slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getMapSlab().
				return err
			}

			// Verify child slabs
			err = v.verifyMapSlab(slab)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by verifyMapSlab().
				return err
			}
		}

		return nil

	default:
		return NewFatalError(fmt.Errorf("MapSlab is either *MapDataSlab or *MapMetaDataSlab, got %T", slab))
	}
}

func (v *serializationVerifier) mapDataSlabEqual(expected, actual *MapDataSlab) error {

	_, _, _, actualDecodedFromCompactMap := expected.canBeEncodedAsCompactMap()

	// Compare extra data
	err := mapExtraDataEqual(expected.extraData, actual.extraData, actualDecodedFromCompactMap)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by mapExtraDataEqual().
		return err
	}

	// Compare inlined
	if expected.inlined != actual.inlined {
		return NewFatalError(fmt.Errorf("inlined %t is wrong, want %t", actual.inlined, expected.inlined))
	}

	// Compare next
	if expected.next != actual.next {
		return NewFatalError(fmt.Errorf("next %d is wrong, want %d", actual.next, expected.next))
	}

	// Compare anySize flag
	if expected.anySize != actual.anySize {
		return NewFatalError(fmt.Errorf("anySize %t is wrong, want %t", actual.anySize, expected.anySize))
	}

	// Compare collisionGroup flag
	if expected.collisionGroup != actual.collisionGroup {
		return NewFatalError(fmt.Errorf("collisionGroup %t is wrong, want %t", actual.collisionGroup, expected.collisionGroup))
	}

	// Compare header
	if actualDecodedFromCompactMap {
		if expected.header.slabID != actual.header.slabID {
			return NewFatalError(fmt.Errorf("header.slabID %s is wrong, want %s", actual.header.slabID, expected.header.slabID))
		}
		if expected.header.size != actual.header.size {
			return NewFatalError(fmt.Errorf("header.size %d is wrong, want %d", actual.header.size, expected.header.size))
		}
	} else if !reflect.DeepEqual(expected.header, actual.header) {
		return NewFatalError(fmt.Errorf("header %+v is wrong, want %+v", actual.header, expected.header))
	}

	// Compare elements
	err = v.mapElementsEqual(expected.elements, actual.elements, actualDecodedFromCompactMap)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by mapElementsEqual().
		return err
	}

	return nil
}

func (v *serializationVerifier) mapElementsEqual(expected, actual elements, actualDecodedFromCompactMap bool) error {
	switch expectedElems := expected.(type) {

	case *hkeyElements:
		actualElems, ok := actual.(*hkeyElements)
		if !ok {
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
		}
		return v.mapHkeyElementsEqual(expectedElems, actualElems, actualDecodedFromCompactMap)

	case *singleElements:
		actualElems, ok := actual.(*singleElements)
		if !ok {
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
		}
		return v.mapSingleElementsEqual(expectedElems, actualElems)

	}

	return nil
}

func (v *serializationVerifier) mapHkeyElementsEqual(expected, actual *hkeyElements, actualDecodedFromCompactMap bool) error {

	if expected.level != actual.level {
		return NewFatalError(fmt.Errorf("hkeyElements level %d is wrong, want %d", actual.level, expected.level))
	}

	if expected.size != actual.size {
		return NewFatalError(fmt.Errorf("hkeyElements size %d is wrong, want %d", actual.size, expected.size))
	}

	if len(expected.hkeys) != len(actual.hkeys) {
		return NewFatalError(fmt.Errorf("hkeyElements hkeys len %d is wrong, want %d", len(actual.hkeys), len(expected.hkeys)))
	}

	if !actualDecodedFromCompactMap {
		if len(expected.hkeys) > 0 && !reflect.DeepEqual(expected.hkeys, actual.hkeys) {
			return NewFatalError(fmt.Errorf("hkeyElements hkeys %v is wrong, want %v", actual.hkeys, expected.hkeys))
		}
	}

	if len(expected.elems) != len(actual.elems) {
		return NewFatalError(fmt.Errorf("hkeyElements elems len %d is wrong, want %d", len(actual.elems), len(expected.elems)))
	}

	if actualDecodedFromCompactMap {
		for _, expectedEle := range expected.elems {
			found := false
			for _, actualEle := range actual.elems {
				err := v.mapElementEqual(expectedEle, actualEle, actualDecodedFromCompactMap)
				if err == nil {
					found = true
					break
				}
			}
			if !found {
				return NewFatalError(fmt.Errorf("hkeyElements elem %v is not found", expectedEle))
			}
		}
	} else {
		for i := range expected.elems {
			expectedEle := expected.elems[i]
			actualEle := actual.elems[i]

			err := v.mapElementEqual(expectedEle, actualEle, actualDecodedFromCompactMap)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by mapElementEqual().
				return err
			}
		}
	}

	return nil
}

func (v *serializationVerifier) mapSingleElementsEqual(expected, actual *singleElements) error {

	if expected.level != actual.level {
		return NewFatalError(fmt.Errorf("singleElements level %d is wrong, want %d", actual.level, expected.level))
	}

	if expected.size != actual.size {
		return NewFatalError(fmt.Errorf("singleElements size %d is wrong, want %d", actual.size, expected.size))
	}

	if len(expected.elems) != len(actual.elems) {
		return NewFatalError(fmt.Errorf("singleElements elems len %d is wrong, want %d", len(actual.elems), len(expected.elems)))
	}

	for i := range expected.elems {
		expectedElem := expected.elems[i]
		actualElem := actual.elems[i]

		err := v.mapSingleElementEqual(expectedElem, actualElem)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by mapSingleElementEqual().
			return err
		}
	}

	return nil
}

func (v *serializationVerifier) mapElementEqual(expected, actual element, actualDecodedFromCompactMap bool) error {
	switch expectedElem := expected.(type) {

	case *singleElement:
		actualElem, ok := actual.(*singleElement)
		if !ok {
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
		}
		return v.mapSingleElementEqual(expectedElem, actualElem)

	case *inlineCollisionGroup:
		actualElem, ok := actual.(*inlineCollisionGroup)
		if !ok {
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
		}
		return v.mapElementsEqual(expectedElem.elements, actualElem.elements, actualDecodedFromCompactMap)

	case *externalCollisionGroup:
		actualElem, ok := actual.(*externalCollisionGroup)
		if !ok {
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
		}
		return v.mapExternalCollisionElementsEqual(expectedElem, actualElem)
	}

	return nil
}

func (v *serializationVerifier) mapExternalCollisionElementsEqual(expected, actual *externalCollisionGroup) error {

	if expected.size != actual.size {
		return NewFatalError(fmt.Errorf("externalCollisionGroup size %d is wrong, want %d", actual.size, expected.size))
	}

	if expected.slabID != actual.slabID {
		return NewFatalError(fmt.Errorf("externalCollisionGroup id %d is wrong, want %d", actual.slabID, expected.slabID))
	}

	slab, err := getMapSlab(v.storage, expected.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return err
	}

	// Compare external collision slab
	err = v.verifyMapSlab(slab)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifyMapSlab().
		return err
	}

	return nil
}

func (v *serializationVerifier) mapSingleElementEqual(expected, actual *singleElement) error {

	if expected.size != actual.size {
		return NewFatalError(fmt.Errorf("singleElement size %d is wrong, want %d", actual.size, expected.size))
	}

	if !v.compare(expected.key, actual.key) {
		return NewFatalError(fmt.Errorf("singleElement key %v is wrong, want %v", actual.key, expected.key))
	}

	// Compare key stored in a separate slab
	if idStorable, ok := expected.key.(SlabIDStorable); ok {

		value, err := idStorable.StoredValue(v.storage)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.StoredValue().
			return err
		}

		err = v.verifyValue(value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifyValue().
			return err
		}
	}

	err := v.compareStorable(expected.value, actual.value)
	if err != nil {
		return NewFatalError(fmt.Errorf("failed to compare singleElement value with key %s: %s", expected.key, err))
	}

	return nil
}

func (v *serializationVerifier) mapMetaDataSlabEqual(expected, actual *MapMetaDataSlab) error {

	// Compare extra data
	err := mapExtraDataEqual(expected.extraData, actual.extraData, false)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by mapExtraDataEqual().
		return err
	}

	// Compare header
	if !reflect.DeepEqual(expected.header, actual.header) {
		return NewFatalError(fmt.Errorf("header %+v is wrong, want %+v", actual.header, expected.header))
	}

	// Compare childrenHeaders
	if !reflect.DeepEqual(expected.childrenHeaders, actual.childrenHeaders) {
		return NewFatalError(fmt.Errorf("childrenHeaders %+v is wrong, want %+v", actual.childrenHeaders, expected.childrenHeaders))
	}

	return nil
}

func mapExtraDataEqual(expected, actual *MapExtraData, actualDecodedFromCompactMap bool) error {

	if (expected == nil) && (actual == nil) {
		return nil
	}

	if (expected == nil) != (actual == nil) {
		return NewFatalError(fmt.Errorf("has extra data is %t, want %t", actual == nil, expected == nil))
	}

	if !reflect.DeepEqual(expected.TypeInfo, actual.TypeInfo) {
		return NewFatalError(fmt.Errorf("map extra data type %+v is wrong, want %+v", actual.TypeInfo, expected.TypeInfo))
	}

	if expected.Count != actual.Count {
		return NewFatalError(fmt.Errorf("map extra data count %d is wrong, want %d", actual.Count, expected.Count))
	}

	if !actualDecodedFromCompactMap {
		if expected.Seed != actual.Seed {
			return NewFatalError(fmt.Errorf("map extra data seed %d is wrong, want %d", actual.Seed, expected.Seed))
		}
	}

	return nil
}
