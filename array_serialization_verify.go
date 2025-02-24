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

	"github.com/fxamacker/cbor/v2"
)

// VerifyArraySerialization traverses array tree and verifies serialization
// by encoding, decoding, and re-encoding slabs.
// It compares in-memory objects of original slab with decoded slab.
// It also compares encoded data of original slab with encoded data of decoded slab.
func VerifyArraySerialization(
	a *Array,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {
	// Skip verification of inlined array serialization.
	if a.Inlined() {
		return nil
	}

	v := &serializationVerifier{
		storage:        a.Storage,
		cborDecMode:    cborDecMode,
		cborEncMode:    cborEncMode,
		decodeStorable: decodeStorable,
		decodeTypeInfo: decodeTypeInfo,
		compare:        compare,
	}
	return v.verifyArraySlab(a.root)
}

type serializationVerifier struct {
	storage        SlabStorage
	cborDecMode    cbor.DecMode
	cborEncMode    cbor.EncMode
	decodeStorable StorableDecoder
	decodeTypeInfo TypeInfoDecoder
	compare        StorableComparator
}

// verifySlab verifies serialization of not inlined ArraySlab.
func (v *serializationVerifier) verifyArraySlab(slab ArraySlab) error {

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
			return NewFatalError(fmt.Errorf("slab %s encoded size %d != header.size %d",
				id, encodedSlabSize, slab.Header().size))
		}
	}

	switch slab := slab.(type) {
	case *ArrayDataSlab:
		decodedDataSlab, ok := decodedSlab.(*ArrayDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("decoded slab %d is not ArrayDataSlab", id))
		}

		// Compare slabs
		err = v.arrayDataSlabEqual(slab, decodedDataSlab)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by arrayDataSlabEqual().
			return fmt.Errorf("data slab %d round-trip serialization failed: %w", id, err)
		}

		return nil

	case *ArrayMetaDataSlab:
		decodedMetaSlab, ok := decodedSlab.(*ArrayMetaDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("decoded slab %d is not ArrayMetaDataSlab", id))
		}

		// Compare slabs
		err = v.arrayMetaDataSlabEqual(slab, decodedMetaSlab)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by arrayMetaDataSlabEqual().
			return fmt.Errorf("metadata slab %d round-trip serialization failed: %w", id, err)
		}

		for _, h := range slab.childrenHeaders {
			childSlab, err := getArraySlab(v.storage, h.slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getArraySlab().
				return err
			}

			// Verify child slabs
			err = v.verifyArraySlab(childSlab)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by verifyArraySlab().
				return err
			}
		}

		return nil

	default:
		return NewFatalError(fmt.Errorf("ArraySlab is either *ArrayDataSlab or *ArrayMetaDataSlab, got %T", slab))
	}
}

func (v *serializationVerifier) arrayDataSlabEqual(expected, actual *ArrayDataSlab) error {

	// Compare extra data
	err := arrayExtraDataEqual(expected.extraData, actual.extraData)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by arrayExtraDataEqual().
		return err
	}

	// Compare inlined status
	if expected.inlined != actual.inlined {
		return NewFatalError(fmt.Errorf("inlined %t is wrong, want %t", actual.inlined, expected.inlined))
	}

	// Compare next
	if expected.next != actual.next {
		return NewFatalError(fmt.Errorf("next %d is wrong, want %d", actual.next, expected.next))
	}

	// Compare header
	if !reflect.DeepEqual(expected.header, actual.header) {
		return NewFatalError(fmt.Errorf("header %+v is wrong, want %+v", actual.header, expected.header))
	}

	// Compare elements length
	if len(expected.elements) != len(actual.elements) {
		return NewFatalError(fmt.Errorf("elements len %d is wrong, want %d", len(actual.elements), len(expected.elements)))
	}

	// Compare element
	for i := range expected.elements {
		ee := expected.elements[i]
		ae := actual.elements[i]

		err := v.compareStorable(ee, ae)
		if err != nil {
			return NewFatalError(fmt.Errorf("failed to compare element %d: %s", i, err))
		}
	}

	return nil
}

func (v *serializationVerifier) compareStorable(expected, actual Storable) error {

	switch expected := expected.(type) {

	case SlabIDStorable: // Compare not-inlined element
		if !v.compare(expected, actual) {
			return NewFatalError(fmt.Errorf("failed to compare SlabIDStorable: %+v is wrong, want %+v", actual, expected))
		}

		actualValue, err := actual.StoredValue(v.storage)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.StoredValue().
			return err
		}

		return v.verifyValue(actualValue)

	case *ArrayDataSlab: // Compare inlined array
		actual, ok := actual.(*ArrayDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("expect storable as inlined *ArrayDataSlab, actual %T", actual))
		}

		return v.arrayDataSlabEqual(expected, actual)

	case *MapDataSlab: // Compare inlined map
		actual, ok := actual.(*MapDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("expect storable as inlined *MapDataSlab, actual %T", actual))
		}

		return v.mapDataSlabEqual(expected, actual)

	case WrapperStorable: // Compare wrapper storable
		actual, ok := actual.(WrapperStorable)
		if !ok {
			return NewFatalError(fmt.Errorf("expect storable as WrapperStorable, actual %T", actual))
		}

		unwrappedExpected := expected.UnwrapAtreeStorable()
		unwrappedActual := actual.UnwrapAtreeStorable()

		return v.compareStorable(unwrappedExpected, unwrappedActual)

	default:
		if !v.compare(expected, actual) {
			return NewFatalError(fmt.Errorf("%+v is wrong, want %+v", actual, expected))
		}
	}

	return nil
}

func (v *serializationVerifier) arrayMetaDataSlabEqual(expected, actual *ArrayMetaDataSlab) error {

	// Compare extra data
	err := arrayExtraDataEqual(expected.extraData, actual.extraData)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by arrayExtraDataEqual().
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

	// Compare childrenCountSum
	if !reflect.DeepEqual(expected.childrenCountSum, actual.childrenCountSum) {
		return NewFatalError(fmt.Errorf("childrenCountSum %+v is wrong, want %+v", actual.childrenCountSum, expected.childrenCountSum))
	}

	return nil
}

func (v *serializationVerifier) verifyValue(value Value) error {

	switch value := value.(type) {
	case *Array:
		return v.verifyArraySlab(value.root)

	case *OrderedMap:
		return v.verifyMapSlab(value.root)
	}
	return nil
}

func arrayExtraDataEqual(expected, actual *ArrayExtraData) error {

	if (expected == nil) && (actual == nil) {
		return nil
	}

	if (expected == nil) != (actual == nil) {
		return NewFatalError(fmt.Errorf("has extra data is %t, want %t", actual == nil, expected == nil))
	}

	if !reflect.DeepEqual(*expected, *actual) {
		return NewFatalError(fmt.Errorf("extra data %+v is wrong, want %+v", *actual, *expected))
	}

	return nil
}

func computeSize(data []byte) (int, error) {
	if len(data) < versionAndFlagSize {
		return 0, NewDecodingError(fmt.Errorf("data is too short"))
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return 0, NewDecodingError(err)
	}

	slabExtraDataSize, inlinedSlabExtrDataSize, err := getExtraDataSizes(h, data[versionAndFlagSize:])
	if err != nil {
		return 0, err
	}

	isDataSlab := h.getSlabArrayType() == slabArrayData ||
		h.getSlabMapType() == slabMapData ||
		h.getSlabMapType() == slabMapCollisionGroup

	// computed size (slab header size):
	// - excludes slab extra data size
	// - excludes inlined slab extra data size
	// - adds next slab ID for non-root data slab if not encoded
	size := len(data)
	size -= slabExtraDataSize
	size -= inlinedSlabExtrDataSize

	if !h.isRoot() && isDataSlab && !h.hasNextSlabID() {
		size += SlabIDLength
	}

	return size, nil
}

func hasInlinedComposite(data []byte) (bool, error) {
	if len(data) < versionAndFlagSize {
		return false, NewDecodingError(fmt.Errorf("data is too short"))
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return false, NewDecodingError(err)
	}

	if !h.hasInlinedSlabs() {
		return false, nil
	}

	data = data[versionAndFlagSize:]

	// Skip slab extra data if needed.
	if h.isRoot() {
		dec := cbor.NewStreamDecoder(bytes.NewBuffer(data))
		b, err := dec.DecodeRawBytes()
		if err != nil {
			return false, NewDecodingError(err)
		}

		data = data[len(b):]
	}

	// Parse inlined extra data to find compact map extra data.
	dec := cbor.NewStreamDecoder(bytes.NewBuffer(data))

	count, err := dec.DecodeArrayHead()
	if err != nil {
		return false, NewDecodingError(err)
	}
	if count != inlinedExtraDataArrayCount {
		return false, NewDecodingError(fmt.Errorf("failed to decode inlined extra data, expect %d elements, got %d elements", inlinedExtraDataArrayCount, count))
	}

	// Skip element 0 (inlined type info)
	err = dec.Skip()
	if err != nil {
		return false, NewDecodingError(err)
	}

	// Decoding element 1 (inlined extra data)
	extraDataCount, err := dec.DecodeArrayHead()
	if err != nil {
		return false, NewDecodingError(err)
	}
	for range extraDataCount {
		tagNum, err := dec.DecodeTagNumber()
		if err != nil {
			return false, NewDecodingError(err)
		}
		if tagNum == CBORTagInlinedCompactMapExtraData {
			return true, nil
		}
		err = dec.Skip()
		if err != nil {
			return false, NewDecodingError(err)
		}
	}

	return false, nil
}

func getExtraDataSizes(h head, data []byte) (int, int, error) {

	var slabExtraDataSize, inlinedSlabExtraDataSize int

	if h.isRoot() {
		dec := cbor.NewStreamDecoder(bytes.NewBuffer(data))
		b, err := dec.DecodeRawBytes()
		if err != nil {
			return 0, 0, NewDecodingError(err)
		}
		slabExtraDataSize = len(b)

		data = data[slabExtraDataSize:]
	}

	if h.hasInlinedSlabs() {
		dec := cbor.NewStreamDecoder(bytes.NewBuffer(data))
		b, err := dec.DecodeRawBytes()
		if err != nil {
			return 0, 0, NewDecodingError(err)
		}
		inlinedSlabExtraDataSize = len(b)
	}

	return slabExtraDataSize, inlinedSlabExtraDataSize, nil
}

// getSlabIDFromStorable appends slab IDs from storable to ids.
// This function traverses child storables.  If child storable
// is inlined map or array, inlined map or array is also traversed.
func getSlabIDFromStorable(storable Storable, ids []SlabID) []SlabID {
	childStorables := storable.ChildStorables()

	for _, e := range childStorables {
		switch e := e.(type) {
		case SlabIDStorable:
			ids = append(ids, SlabID(e))

		case *ArrayDataSlab:
			ids = getSlabIDFromStorable(e, ids)

		case *MapDataSlab:
			ids = getSlabIDFromStorable(e, ids)
		}
	}

	return ids
}
