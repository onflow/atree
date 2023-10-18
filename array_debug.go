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

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

type ArrayStats struct {
	Levels            uint64
	ElementCount      uint64
	MetaDataSlabCount uint64
	DataSlabCount     uint64
	StorableSlabCount uint64
}

func (s *ArrayStats) SlabCount() uint64 {
	return s.DataSlabCount + s.MetaDataSlabCount + s.StorableSlabCount
}

// GetArrayStats returns stats about array slabs.
func GetArrayStats(a *Array) (ArrayStats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	dataSlabCount := uint64(0)
	storableSlabCount := uint64(0)

	nextLevelIDs := []SlabID{a.SlabID()}

	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getArraySlab(a.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getArraySlab().
				return ArrayStats{}, err
			}

			switch slab.(type) {
			case *ArrayDataSlab:
				dataSlabCount++

				ids := getSlabIDFromStorable(slab, nil)
				storableSlabCount += uint64(len(ids))

			case *ArrayMetaDataSlab:
				metaDataSlabCount++

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(SlabIDStorable)
					if !ok {
						return ArrayStats{}, NewFatalError(fmt.Errorf("metadata slab's child storables are not of type SlabIDStorable"))
					}
					nextLevelIDs = append(nextLevelIDs, SlabID(id))
				}
			}
		}

		level++

	}

	return ArrayStats{
		Levels:            level,
		ElementCount:      a.Count(),
		MetaDataSlabCount: metaDataSlabCount,
		DataSlabCount:     dataSlabCount,
		StorableSlabCount: storableSlabCount,
	}, nil
}

// PrintArray prints array slab data to stdout.
func PrintArray(a *Array) {
	dumps, err := DumpArraySlabs(a)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(strings.Join(dumps, "\n"))
}

func DumpArraySlabs(a *Array) ([]string, error) {
	var dumps []string

	nextLevelIDs := []SlabID{a.SlabID()}

	var overflowIDs []SlabID

	level := 0
	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getArraySlab(a.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getArraySlab().
				return nil, err
			}

			switch slab := slab.(type) {
			case *ArrayDataSlab:
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, slab))

				overflowIDs = getSlabIDFromStorable(slab, overflowIDs)

			case *ArrayMetaDataSlab:
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, slab))

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(SlabIDStorable)
					if !ok {
						return nil, NewFatalError(errors.New("metadata slab's child storables are not of type SlabIDStorable"))
					}
					nextLevelIDs = append(nextLevelIDs, SlabID(id))
				}
			}
		}

		level++
	}

	for _, id := range overflowIDs {
		slab, found, err := a.Storage.Retrieve(id)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(id, "slab not found during array slab dump")
		}
		dumps = append(dumps, slab.String())
	}

	return dumps, nil
}

type TypeInfoComparator func(TypeInfo, TypeInfo) bool

func VerifyArray(a *Array, address Address, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider, inlineEnabled bool) error {
	return verifyArray(a, address, typeInfo, tic, hip, inlineEnabled, map[SlabID]struct{}{})
}

func verifyArray(a *Array, address Address, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider, inlineEnabled bool, slabIDs map[SlabID]struct{}) error {
	// Verify array address (independent of array inlined status)
	if address != a.Address() {
		return NewFatalError(fmt.Errorf("array address %v, got %v", address, a.Address()))
	}

	// Verify array value ID (independent of array inlined status)
	err := verifyArrayValueID(a)
	if err != nil {
		return err
	}

	// Verify array slab ID (dependent of array inlined status)
	err = verifyArraySlabID(a)
	if err != nil {
		return err
	}

	// Verify array extra data
	extraData := a.root.ExtraData()
	if extraData == nil {
		return NewFatalError(fmt.Errorf("root slab %d doesn't have extra data", a.root.SlabID()))
	}

	// Verify that extra data has correct type information
	if typeInfo != nil && !tic(extraData.TypeInfo, typeInfo) {
		return NewFatalError(fmt.Errorf(
			"root slab %d type information %v is wrong, want %v",
			a.root.SlabID(),
			extraData.TypeInfo,
			typeInfo,
		))
	}

	v := &arrayVerifier{
		storage:       a.Storage,
		address:       address,
		tic:           tic,
		hip:           hip,
		inlineEnabled: inlineEnabled,
	}

	// Verify array slabs
	computedCount, dataSlabIDs, nextDataSlabIDs, err := v.verifySlab(a.root, 0, nil, []SlabID{}, []SlabID{}, slabIDs)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifySlab().
		return err
	}

	// Verify array count
	if computedCount != uint32(a.Count()) {
		return NewFatalError(fmt.Errorf("root slab %d count %d is wrong, want %d", a.root.SlabID(), a.Count(), computedCount))
	}

	// Verify next data slab ids
	if !reflect.DeepEqual(dataSlabIDs[1:], nextDataSlabIDs) {
		return NewFatalError(fmt.Errorf("chained next data slab ids %v are wrong, want %v",
			nextDataSlabIDs, dataSlabIDs[1:]))
	}

	return nil
}

type arrayVerifier struct {
	storage       SlabStorage
	address       Address
	tic           TypeInfoComparator
	hip           HashInputProvider
	inlineEnabled bool
}

// verifySlab verifies ArraySlab in memory which can be inlined or not inlined.
func (v *arrayVerifier) verifySlab(
	slab ArraySlab,
	level int,
	headerFromParentSlab *ArraySlabHeader,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	slabIDs map[SlabID]struct{},
) (
	elementCount uint32,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	err error,
) {
	id := slab.Header().slabID

	// Verify SlabID is unique
	if _, exist := slabIDs[id]; exist {
		return 0, nil, nil, NewFatalError(fmt.Errorf("found duplicate slab ID %s", id))
	}

	slabIDs[id] = struct{}{}

	// Verify slab address (independent of array inlined status)
	if v.address != id.address {
		return 0, nil, nil, NewFatalError(fmt.Errorf("array slab address %v, got %v", v.address, id.address))
	}

	// Verify that inlined slab is not in storage
	if slab.Inlined() {
		_, exist, err := v.storage.Retrieve(id)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storage interface.
			return 0, nil, nil, wrapErrorAsExternalErrorIfNeeded(err)
		}
		if exist {
			return 0, nil, nil, NewFatalError(fmt.Errorf("inlined slab %s is in storage", id))
		}
	}

	if level > 0 {
		// Verify that non-root slab doesn't have extra data
		if slab.ExtraData() != nil {
			return 0, nil, nil, NewFatalError(fmt.Errorf("non-root slab %s has extra data", id))
		}

		// Verify that non-root slab doesn't underflow
		if underflowSize, underflow := slab.IsUnderflow(); underflow {
			return 0, nil, nil, NewFatalError(fmt.Errorf("slab %s underflows by %d bytes", id, underflowSize))
		}

	}

	// Verify that slab doesn't overflow
	if slab.IsFull() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("slab %s overflows", id))
	}

	// Verify that header is in sync with header from parent slab
	if headerFromParentSlab != nil {
		if !reflect.DeepEqual(*headerFromParentSlab, slab.Header()) {
			return 0, nil, nil, NewFatalError(fmt.Errorf("slab %s header %+v is different from header %+v from parent slab",
				id, slab.Header(), headerFromParentSlab))
		}
	}

	switch slab := slab.(type) {
	case *ArrayDataSlab:
		return v.verifyDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, slabIDs)

	case *ArrayMetaDataSlab:
		return v.verifyMetaDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, slabIDs)

	default:
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArraySlab is either *ArrayDataSlab or *ArrayMetaDataSlab, got %T", slab))
	}
}

func (v *arrayVerifier) verifyDataSlab(
	dataSlab *ArrayDataSlab,
	level int,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	slabIDs map[SlabID]struct{},
) (
	elementCount uint32,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	err error,
) {
	id := dataSlab.header.slabID

	if !dataSlab.IsData() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArrayDataSlab %s is not data", id))
	}

	// Verify that element count is the same as header.count
	if uint32(len(dataSlab.elements)) != dataSlab.header.count {
		return 0, nil, nil, NewFatalError(fmt.Errorf("data slab %s header count %d is wrong, want %d",
			id, dataSlab.header.count, len(dataSlab.elements)))
	}

	// Verify that only root data slab can be inlined
	if level > 0 && dataSlab.Inlined() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("non-root slab %s is inlined", id))
	}

	// Verify that aggregated element size + slab prefix is the same as header.size
	computedSize := uint32(arrayDataSlabPrefixSize)
	if level == 0 {
		computedSize = uint32(arrayRootDataSlabPrefixSize)
		if dataSlab.Inlined() {
			computedSize = uint32(inlinedArrayDataSlabPrefixSize)
		}
	}

	for _, e := range dataSlab.elements {
		computedSize += e.ByteSize()
	}

	if computedSize != dataSlab.header.size {
		return 0, nil, nil, NewFatalError(fmt.Errorf("data slab %s header size %d is wrong, want %d",
			id, dataSlab.header.size, computedSize))
	}

	dataSlabIDs = append(dataSlabIDs, id)

	if dataSlab.next != SlabIDUndefined {
		nextDataSlabIDs = append(nextDataSlabIDs, dataSlab.next)
	}

	for _, e := range dataSlab.elements {

		value, err := e.StoredValue(v.storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return 0, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err,
				fmt.Sprintf(
					"data slab %s element %s can't be converted to value",
					id, e,
				))
		}

		// Verify element size <= inline size
		if e.ByteSize() > uint32(maxInlineArrayElementSize) {
			return 0, nil, nil, NewFatalError(fmt.Errorf("data slab %s element %s size %d is too large, want < %d",
				id, e, e.ByteSize(), maxInlineArrayElementSize))
		}

		switch e := e.(type) {
		case SlabIDStorable:
			// Verify not-inlined element > inline size, or can't be inlined
			if v.inlineEnabled {
				err = verifyNotInlinedValueStatusAndSize(value, uint32(maxInlineArrayElementSize))
				if err != nil {
					return 0, nil, nil, err
				}
			}

		case *ArrayDataSlab:
			// Verify inlined element's inlined status
			if !e.Inlined() {
				return 0, nil, nil, NewFatalError(fmt.Errorf("inlined array inlined status is false"))
			}

		case *MapDataSlab:
			// Verify inlined element's inlined status
			if !e.Inlined() {
				return 0, nil, nil, NewFatalError(fmt.Errorf("inlined map inlined status is false"))
			}
		}

		// Verify element
		err = verifyValue(value, v.address, nil, v.tic, v.hip, v.inlineEnabled, slabIDs)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifyValue().
			return 0, nil, nil, fmt.Errorf(
				"data slab %s element %q isn't valid: %w",
				id, e, err,
			)
		}
	}

	return dataSlab.header.count, dataSlabIDs, nextDataSlabIDs, nil
}

func (v *arrayVerifier) verifyMetaDataSlab(
	metaSlab *ArrayMetaDataSlab,
	level int,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	slabIDs map[SlabID]struct{},
) (
	elementCount uint32,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	err error,
) {
	id := metaSlab.header.slabID

	if metaSlab.IsData() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArrayMetaDataSlab %s is data", id))
	}

	if metaSlab.Inlined() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArrayMetaDataSlab %s shouldn't be inlined", id))
	}

	if level == 0 {
		// Verify that root slab has more than one child slabs
		if len(metaSlab.childrenHeaders) < 2 {
			return 0, nil, nil, NewFatalError(fmt.Errorf("root metadata slab %d has %d children, want at least 2 children ",
				id, len(metaSlab.childrenHeaders)))
		}
	}

	// Verify childrenCountSum
	if len(metaSlab.childrenCountSum) != len(metaSlab.childrenHeaders) {
		return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d has %d childrenCountSum, want %d",
			id, len(metaSlab.childrenCountSum), len(metaSlab.childrenHeaders)))
	}

	computedCount := uint32(0)
	// NOTE: We don't use range loop here because &h is passed as argument to another function.
	// If we use range, then h would be a temporary object and we'd be passing address of
	// temporary object to function, which can lead to bugs depending on usage. It's not a bug
	// with the current usage but it's less fragile to future changes by not using range here.
	for i := 0; i < len(metaSlab.childrenHeaders); i++ {
		h := metaSlab.childrenHeaders[i]

		childSlab, err := getArraySlab(v.storage, h.slabID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return 0, nil, nil, err
		}

		// Verify child slabs
		var count uint32
		count, dataSlabIDs, nextDataSlabIDs, err =
			v.verifySlab(childSlab, level+1, &h, dataSlabIDs, nextDataSlabIDs, slabIDs)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifySlab().
			return 0, nil, nil, err
		}

		computedCount += count

		// Verify childrenCountSum
		if metaSlab.childrenCountSum[i] != computedCount {
			return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d childrenCountSum[%d] is %d, want %d",
				id, i, metaSlab.childrenCountSum[i], computedCount))
		}
	}

	// Verify that aggregated element count is the same as header.count
	if computedCount != metaSlab.header.count {
		return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d header count %d is wrong, want %d",
			id, metaSlab.header.count, computedCount))
	}

	// Verify that aggregated header size + slab prefix is the same as header.size
	computedSize := uint32(len(metaSlab.childrenHeaders)*arraySlabHeaderSize) + arrayMetaDataSlabPrefixSize
	if computedSize != metaSlab.header.size {
		return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d header size %d is wrong, want %d",
			id, metaSlab.header.size, computedSize))
	}

	return metaSlab.header.count, dataSlabIDs, nextDataSlabIDs, nil
}

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
	for i := 0; i < len(expected.elements); i++ {
		ee := expected.elements[i]
		ae := actual.elements[i]

		switch ee := ee.(type) {

		case SlabIDStorable: // Compare not-inlined element
			if !v.compare(ee, ae) {
				return NewFatalError(fmt.Errorf("element %d %+v is wrong, want %+v", i, ae, ee))
			}

			ev, err := ee.StoredValue(v.storage)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.StoredValue().
				return err
			}

			return v.verifyValue(ev)

		case *ArrayDataSlab: // Compare inlined array
			ae, ok := ae.(*ArrayDataSlab)
			if !ok {
				return NewFatalError(fmt.Errorf("expect element as inlined *ArrayDataSlab, actual %T", ae))
			}

			return v.arrayDataSlabEqual(ee, ae)

		case *MapDataSlab: // Compare inlined map
			ae, ok := ae.(*MapDataSlab)
			if !ok {
				return NewFatalError(fmt.Errorf("expect element as inlined *MapDataSlab, actual %T", ae))
			}

			return v.mapDataSlabEqual(ee, ae)

		default:
			if !v.compare(ee, ae) {
				return NewFatalError(fmt.Errorf("element %d %+v is wrong, want %+v", i, ae, ee))
			}
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

func (v *serializationVerifier) verifyValue(value Value) error {

	switch value := value.(type) {
	case *Array:
		return v.verifyArraySlab(value.root)

	case *OrderedMap:
		return v.verifyMapSlab(value.root)
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
		size += slabIDSize
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

	for i := uint64(0); i < count; i++ {
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

// verifyArrayValueID verifies array ValueID is always the same as
// root slab's SlabID indepedent of array's inlined status.
func verifyArrayValueID(a *Array) error {
	rootSlabID := a.root.Header().slabID

	vid := a.ValueID()

	if !bytes.Equal(vid[:slabAddressSize], rootSlabID.address[:]) {
		return NewFatalError(
			fmt.Errorf(
				"expect first %d bytes of array value ID as %v, got %v",
				slabAddressSize,
				rootSlabID.address[:],
				vid[:slabAddressSize]))
	}

	if !bytes.Equal(vid[slabAddressSize:], rootSlabID.index[:]) {
		return NewFatalError(
			fmt.Errorf(
				"expect second %d bytes of array value ID as %v, got %v",
				slabIndexSize,
				rootSlabID.index[:],
				vid[slabAddressSize:]))
	}

	return nil
}

// verifyArraySlabID verifies array SlabID is either empty for inlined array, or
// same as root slab's SlabID for not-inlined array.
func verifyArraySlabID(a *Array) error {
	sid := a.SlabID()

	if a.Inlined() {
		if sid != SlabIDUndefined {
			return NewFatalError(
				fmt.Errorf(
					"expect empty slab ID for inlined array, got %v",
					sid))
		}
		return nil
	}

	rootSlabID := a.root.Header().slabID

	if sid == SlabIDUndefined {
		return NewFatalError(
			fmt.Errorf(
				"expect non-empty slab ID for not-inlined array, got %v",
				sid))
	}

	if sid != rootSlabID {
		return NewFatalError(
			fmt.Errorf(
				"expect array slab ID same as root slab's slab ID %s, got %s",
				rootSlabID,
				sid))
	}

	return nil
}

func verifyNotInlinedValueStatusAndSize(v Value, maxInlineSize uint32) error {

	switch v := v.(type) {
	case *Array:
		// Verify not-inlined array's inlined status
		if v.root.Inlined() {
			return NewFatalError(
				fmt.Errorf(
					"not-inlined array %s has inlined status",
					v.root.Header().slabID))
		}

		// Verify not-inlined array size.
		if v.root.IsData() {
			inlinableSize := v.root.ByteSize() - arrayRootDataSlabPrefixSize + inlinedArrayDataSlabPrefixSize
			if inlinableSize <= maxInlineSize {
				return NewFatalError(
					fmt.Errorf("not-inlined array root slab %s can be inlined, inlinable size %d <= max inline size %d",
						v.root.Header().slabID,
						inlinableSize,
						maxInlineSize))
			}
		}

	case *OrderedMap:
		// Verify not-inlined map's inlined status
		if v.Inlined() {
			return NewFatalError(
				fmt.Errorf(
					"not-inlined map %s has inlined status",
					v.root.Header().slabID))
		}

		// Verify not-inlined map size.
		if v.root.IsData() {
			inlinableSize := v.root.ByteSize() - mapRootDataSlabPrefixSize + inlinedMapDataSlabPrefixSize
			if inlinableSize <= maxInlineSize {
				return NewFatalError(
					fmt.Errorf("not-inlined map root slab %s can be inlined, inlinable size %d <= max inline size %d",
						v.root.Header().slabID,
						inlinableSize,
						maxInlineSize))
			}
		}
	}

	return nil
}
