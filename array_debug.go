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

	nextLevelIDs := []StorageID{a.StorageID()}

	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []StorageID(nil)

		for _, id := range ids {

			slab, err := getArraySlab(a.Storage, id)
			if err != nil {
				return ArrayStats{}, err
			}

			if slab.IsData() {
				dataSlabCount++

				childStorables := slab.ChildStorables()
				for _, s := range childStorables {
					if _, ok := s.(StorageIDStorable); ok {
						storableSlabCount++
					}
				}
			} else {
				metaDataSlabCount++

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(StorageIDStorable)
					if !ok {
						return ArrayStats{}, fmt.Errorf("metadata slab's child storables are not of type StorageIDStorable")
					}
					nextLevelIDs = append(nextLevelIDs, StorageID(id))
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

	nextLevelIDs := []StorageID{a.StorageID()}

	var overflowIDs []StorageID

	level := 0
	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []StorageID(nil)

		for _, id := range ids {

			slab, err := getArraySlab(a.Storage, id)
			if err != nil {
				return nil, err
			}

			if slab.IsData() {
				dataSlab := slab.(*ArrayDataSlab)
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, dataSlab))

				childStorables := dataSlab.ChildStorables()
				for _, e := range childStorables {
					if id, ok := e.(StorageIDStorable); ok {
						overflowIDs = append(overflowIDs, StorageID(id))
					}
				}

			} else {
				meta := slab.(*ArrayMetaDataSlab)
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, meta))

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(StorageIDStorable)
					if !ok {
						return nil, errors.New("metadata slab's child storables are not of type StorageIDStorable")
					}
					nextLevelIDs = append(nextLevelIDs, StorageID(id))
				}
			}
		}

		level++
	}

	for _, id := range overflowIDs {
		slab, found, err := a.Storage.Retrieve(id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(id, "slab not found during array slab dump")
		}
		dumps = append(dumps, fmt.Sprintf("overflow: %s", slab))
	}

	return dumps, nil
}

type TypeInfoComparator func(TypeInfo, TypeInfo) bool

func ValidArray(a *Array, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider) error {

	extraData := a.root.ExtraData()
	if extraData == nil {
		return fmt.Errorf("root slab %d doesn't have extra data", a.root.ID())
	}

	// Verify that extra data has correct type information
	if typeInfo != nil && !tic(extraData.TypeInfo, typeInfo) {
		return fmt.Errorf(
			"root slab %d type information %v is wrong, want %v",
			a.root.ID(),
			extraData.TypeInfo,
			typeInfo,
		)
	}

	computedCount, dataSlabIDs, nextDataSlabIDs, err :=
		validArraySlab(tic, hip, a.Storage, a.root.Header().id, 0, nil, []StorageID{}, []StorageID{})
	if err != nil {
		return err
	}

	// Verify array count
	if computedCount != uint32(a.Count()) {
		return fmt.Errorf("root slab %d count %d is wrong, want %d", a.root.ID(), a.Count(), computedCount)
	}

	// Verify next data slab ids
	if !reflect.DeepEqual(dataSlabIDs[1:], nextDataSlabIDs) {
		return fmt.Errorf("chained next data slab ids %v are wrong, want %v",
			nextDataSlabIDs, dataSlabIDs[1:])
	}

	return nil
}

func validArraySlab(
	tic TypeInfoComparator,
	hip HashInputProvider,
	storage SlabStorage,
	id StorageID,
	level int,
	headerFromParentSlab *ArraySlabHeader,
	dataSlabIDs []StorageID,
	nextDataSlabIDs []StorageID,
) (
	elementCount uint32,
	_dataSlabIDs []StorageID,
	_nextDataSlabIDs []StorageID,
	err error,
) {

	slab, err := getArraySlab(storage, id)
	if err != nil {
		return 0, nil, nil, err
	}

	if level > 0 {
		// Verify that non-root slab doesn't have extra data
		if slab.ExtraData() != nil {
			return 0, nil, nil, fmt.Errorf("non-root slab %d has extra data", id)
		}

		// Verify that non-root slab doesn't underflow
		if underflowSize, underflow := slab.IsUnderflow(); underflow {
			return 0, nil, nil, fmt.Errorf("slab %d underflows by %d bytes", id, underflowSize)
		}

	}

	// Verify that slab doesn't overflow
	if slab.IsFull() {
		return 0, nil, nil, fmt.Errorf("slab %d overflows", id)
	}

	// Verify that header is in sync with header from parent slab
	if headerFromParentSlab != nil {
		if !reflect.DeepEqual(*headerFromParentSlab, slab.Header()) {
			return 0, nil, nil, fmt.Errorf("slab %d header %+v is different from header %+v from parent slab",
				id, slab.Header(), headerFromParentSlab)
		}
	}

	if slab.IsData() {
		dataSlab, ok := slab.(*ArrayDataSlab)
		if !ok {
			return 0, nil, nil, fmt.Errorf("slab %d is not ArrayDataSlab", id)
		}

		// Verify that element count is the same as header.count
		if uint32(len(dataSlab.elements)) != dataSlab.header.count {
			return 0, nil, nil, fmt.Errorf("data slab %d header count %d is wrong, want %d",
				id, dataSlab.header.count, len(dataSlab.elements))
		}

		// Verify that aggregated element size + slab prefix is the same as header.size
		computedSize := uint32(arrayDataSlabPrefixSize)
		if level == 0 {
			computedSize = uint32(arrayRootDataSlabPrefixSize)
		}
		for _, e := range dataSlab.elements {

			// Verify element size is <= inline size
			if e.ByteSize() > uint32(MaxInlineArrayElementSize) {
				return 0, nil, nil, fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
					id, e, e.ByteSize(), MaxInlineArrayElementSize)
			}

			computedSize += e.ByteSize()
		}

		if computedSize != dataSlab.header.size {
			return 0, nil, nil, fmt.Errorf("data slab %d header size %d is wrong, want %d",
				id, dataSlab.header.size, computedSize)
		}

		dataSlabIDs = append(dataSlabIDs, id)

		if dataSlab.next != StorageIDUndefined {
			nextDataSlabIDs = append(nextDataSlabIDs, dataSlab.next)
		}

		// Verify element
		for _, e := range dataSlab.elements {
			v, err := e.StoredValue(storage)
			if err != nil {
				return 0, nil, nil, fmt.Errorf(
					"data slab %d element %s can't be converted to value: %w",
					id, e, err,
				)
			}
			err = ValidValue(v, nil, tic, hip)
			if err != nil {
				return 0, nil, nil, fmt.Errorf(
					"data slab %d element %s isn't valid: %w",
					id, e, err,
				)
			}
		}

		return dataSlab.header.count, dataSlabIDs, nextDataSlabIDs, nil
	}

	meta, ok := slab.(*ArrayMetaDataSlab)
	if !ok {
		return 0, nil, nil, fmt.Errorf("slab %d is not ArrayMetaDataSlab", id)
	}

	if level == 0 {
		// Verify that root slab has more than one child slabs
		if len(meta.childrenHeaders) < 2 {
			return 0, nil, nil, fmt.Errorf("root metadata slab %d has %d children, want at least 2 children ",
				id, len(meta.childrenHeaders))
		}
	}

	// Verify childrenCountSum
	if len(meta.childrenCountSum) != len(meta.childrenHeaders) {
		return 0, nil, nil, fmt.Errorf("metadata slab %d has %d childrenCountSum, want %d",
			id, len(meta.childrenCountSum), len(meta.childrenHeaders))
	}

	computedCount := uint32(0)
	for i, h := range meta.childrenHeaders {
		// Verify child slabs
		var count uint32
		count, dataSlabIDs, nextDataSlabIDs, err =
			validArraySlab(tic, hip, storage, h.id, level+1, &h, dataSlabIDs, nextDataSlabIDs)
		if err != nil {
			return 0, nil, nil, err
		}

		computedCount += count

		// Verify childrenCountSum
		if meta.childrenCountSum[i] != computedCount {
			return 0, nil, nil, fmt.Errorf("metadata slab %d childrenCountSum[%d] is %d, want %d",
				id, i, meta.childrenCountSum[i], computedCount)
		}
	}

	// Verify that aggregated element count is the same as header.count
	if computedCount != meta.header.count {
		return 0, nil, nil, fmt.Errorf("metadata slab %d header count %d is wrong, want %d",
			id, meta.header.count, computedCount)
	}

	// Verify that aggregated header size + slab prefix is the same as header.size
	computedSize := uint32(len(meta.childrenHeaders)*arraySlabHeaderSize) + arrayMetaDataSlabPrefixSize
	if computedSize != meta.header.size {
		return 0, nil, nil, fmt.Errorf("metadata slab %d header size %d is wrong, want %d",
			id, meta.header.size, computedSize)
	}

	return meta.header.count, dataSlabIDs, nextDataSlabIDs, nil
}

// ValidArraySerialization traverses array tree and verifies serialization
// by encoding, decoding, and re-encoding slabs.
// It compares in-memory objects of original slab with decoded slab.
// It also compares encoded data of original slab with encoded data of decoded slab.
func ValidArraySerialization(
	a *Array,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {
	return validArraySlabSerialization(
		a.Storage,
		a.root.ID(),
		cborDecMode,
		cborEncMode,
		decodeStorable,
		decodeTypeInfo,
		compare,
	)
}

func validArraySlabSerialization(
	storage SlabStorage,
	id StorageID,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	slab, err := getArraySlab(storage, id)
	if err != nil {
		return err
	}

	// Encode slab
	data, err := Encode(slab, cborEncMode)
	if err != nil {
		return err
	}

	// Decode encoded slab
	decodedSlab, err := DecodeSlab(id, data, cborDecMode, decodeStorable, decodeTypeInfo)
	if err != nil {
		return err
	}

	// Re-encode decoded slab
	dataFromDecodedSlab, err := Encode(decodedSlab, cborEncMode)
	if err != nil {
		return err
	}

	// Extra check: encoded data size == header.size
	encodedExtraDataSize, err := getEncodedArrayExtraDataSize(slab.ExtraData(), cborEncMode)
	if err != nil {
		return err
	}

	// Need to exclude extra data size from encoded data size.
	encodedSlabSize := uint32(len(data) - encodedExtraDataSize)
	if slab.Header().size != encodedSlabSize {
		return fmt.Errorf("slab %d encoded size %d != header.size %d (encoded extra data size %d)",
			id, encodedSlabSize, slab.Header().size, encodedExtraDataSize)
	}

	// Compare encoded data of original slab with encoded data of decoded slab
	if !bytes.Equal(data, dataFromDecodedSlab) {
		return fmt.Errorf("slab %d encoded data is different from decoded slab's encoded data, got %v, want %v",
			id, dataFromDecodedSlab, data)
	}

	if slab.IsData() {
		dataSlab, ok := slab.(*ArrayDataSlab)
		if !ok {
			return fmt.Errorf("slab %d is not ArrayDataSlab", id)
		}

		decodedDataSlab, ok := decodedSlab.(*ArrayDataSlab)
		if !ok {
			return fmt.Errorf("decoded slab %d is not ArrayDataSlab", id)
		}

		// Compare slabs
		err = arrayDataSlabEqual(
			dataSlab,
			decodedDataSlab,
			storage,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)
		if err != nil {
			return fmt.Errorf("data slab %d round-trip serialization failed: %w", id, err)
		}

		return nil
	}

	metaSlab, ok := slab.(*ArrayMetaDataSlab)
	if !ok {
		return fmt.Errorf("slab %d is not ArrayMetaDataSlab", id)
	}

	decodedMetaSlab, ok := decodedSlab.(*ArrayMetaDataSlab)
	if !ok {
		return fmt.Errorf("decoded slab %d is not ArrayMetaDataSlab", id)
	}

	// Compare slabs
	err = arrayMetaDataSlabEqual(metaSlab, decodedMetaSlab)
	if err != nil {
		return fmt.Errorf("metadata slab %d round-trip serialization failed: %w", id, err)
	}

	for _, h := range metaSlab.childrenHeaders {
		// Verify child slabs
		err = validArraySlabSerialization(
			storage,
			h.id,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func arrayDataSlabEqual(
	expected *ArrayDataSlab,
	actual *ArrayDataSlab,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	// Compare extra data
	err := arrayExtraDataEqual(expected.extraData, actual.extraData)
	if err != nil {
		return err
	}

	// Compare next
	if expected.next != actual.next {
		return fmt.Errorf("next %d is wrong, want %d", actual.next, expected.next)
	}

	// Compare header
	if !reflect.DeepEqual(expected.header, actual.header) {
		return fmt.Errorf("header %+v is wrong, want %+v", actual.header, expected.header)
	}

	// Compare elements length
	if len(expected.elements) != len(actual.elements) {
		return fmt.Errorf("elements len %d is wrong, want %d", len(actual.elements), len(expected.elements))
	}

	// Compare element
	for i := 0; i < len(expected.elements); i++ {
		ee := expected.elements[i]
		ae := actual.elements[i]
		if !compare(ee, ae) {
			return fmt.Errorf("element %d %+v is wrong, want %+v", i, ae, ee)
		}

		// Compare nested element
		if idStorable, ok := ee.(StorageIDStorable); ok {

			ev, err := idStorable.StoredValue(storage)
			if err != nil {
				return err
			}

			return ValidValueSerialization(
				ev,
				cborDecMode,
				cborEncMode,
				decodeStorable,
				decodeTypeInfo,
				compare,
			)
		}
	}

	return nil
}

func arrayMetaDataSlabEqual(expected, actual *ArrayMetaDataSlab) error {

	// Compare extra data
	err := arrayExtraDataEqual(expected.extraData, actual.extraData)
	if err != nil {
		return err
	}

	// Compare header
	if !reflect.DeepEqual(expected.header, actual.header) {
		return fmt.Errorf("header %+v is wrong, want %+v", actual.header, expected.header)
	}

	// Compare childrenHeaders
	if !reflect.DeepEqual(expected.childrenHeaders, actual.childrenHeaders) {
		return fmt.Errorf("childrenHeaders %+v is wrong, want %+v", actual.childrenHeaders, expected.childrenHeaders)
	}

	// Compare childrenCountSum
	if !reflect.DeepEqual(expected.childrenCountSum, actual.childrenCountSum) {
		return fmt.Errorf("childrenCountSum %+v is wrong, want %+v", actual.childrenCountSum, expected.childrenCountSum)
	}

	return nil
}

func arrayExtraDataEqual(expected, actual *ArrayExtraData) error {

	if (expected == nil) && (actual == nil) {
		return nil
	}

	if (expected == nil) != (actual == nil) {
		return fmt.Errorf("has extra data is %t, want %t", actual == nil, expected == nil)
	}

	if !reflect.DeepEqual(*expected, *actual) {
		return fmt.Errorf("extra data %+v is wrong, want %+v", *actual, *expected)
	}

	return nil
}

func getEncodedArrayExtraDataSize(extraData *ArrayExtraData, cborEncMode cbor.EncMode) (int, error) {
	if extraData == nil {
		return 0, nil
	}

	var buf bytes.Buffer
	enc := NewEncoder(&buf, cborEncMode)

	// Normally the flag shouldn't be 0. But in this case we just need the encoded data size
	// so the content of the flag doesn't matter.
	err := extraData.Encode(enc, byte(0))
	if err != nil {
		return 0, err
	}

	return len(buf.Bytes()), nil
}

func ValidValueSerialization(
	value Value,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	switch v := value.(type) {
	case *Array:
		return ValidArraySerialization(
			v,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)
	case *OrderedMap:
		return ValidMapSerialization(
			v,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)
	}
	return nil
}
