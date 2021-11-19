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
	"sort"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

type MapStats struct {
	Levels                 uint64
	ElementCount           uint64
	MetaDataSlabCount      uint64
	DataSlabCount          uint64
	CollisionDataSlabCount uint64
	StorableSlabCount      uint64
}

func (s *MapStats) SlabCount() uint64 {
	return s.DataSlabCount + s.MetaDataSlabCount + s.CollisionDataSlabCount + s.StorableSlabCount
}

// GetMapStats returns stats about the map slabs.
func GetMapStats(m *OrderedMap) (MapStats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	dataSlabCount := uint64(0)
	collisionDataSlabCount := uint64(0)
	storableDataSlabCount := uint64(0)

	nextLevelIDs := []StorageID{m.StorageID()}

	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []StorageID(nil)

		for _, id := range ids {

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				return MapStats{}, err
			}

			if slab.IsData() {
				dataSlabCount++

				leaf := slab.(*MapDataSlab)
				elementGroups := []elements{leaf.elements}

				for len(elementGroups) > 0 {

					var nestedElementGroups []elements

					for i := 0; i < len(elementGroups); i++ {

						elems := elementGroups[i]

						for j := 0; j < int(elems.Count()); j++ {
							elem, err := elems.Element(j)
							if err != nil {
								return MapStats{}, err
							}

							if group, ok := elem.(elementGroup); ok {
								if !group.Inline() {
									collisionDataSlabCount++
								}

								nested, err := group.Elements(m.Storage)
								if err != nil {
									return MapStats{}, err
								}
								nestedElementGroups = append(nestedElementGroups, nested)

							} else {
								e := elem.(*singleElement)
								if _, ok := e.key.(StorageIDStorable); ok {
									storableDataSlabCount++
								}
								if _, ok := e.value.(StorageIDStorable); ok {
									storableDataSlabCount++
								}
							}
						}
					}
					elementGroups = nestedElementGroups
				}
			} else {
				metaDataSlabCount++

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(StorageIDStorable)
					if !ok {
						return MapStats{}, fmt.Errorf("metadata slab's child storables are not of type StorageIDStorable")
					}
					nextLevelIDs = append(nextLevelIDs, StorageID(id))
				}
			}
		}

		level++
	}

	return MapStats{
		Levels:                 level,
		ElementCount:           m.Count(),
		MetaDataSlabCount:      metaDataSlabCount,
		DataSlabCount:          dataSlabCount,
		CollisionDataSlabCount: collisionDataSlabCount,
		StorableSlabCount:      storableDataSlabCount,
	}, nil
}

func PrintMap(m *OrderedMap) {
	dumps, err := DumpMapSlabs(m)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(strings.Join(dumps, "\n"))
}

func DumpMapSlabs(m *OrderedMap) ([]string, error) {
	var dumps []string

	nextLevelIDs := []StorageID{m.StorageID()}

	var overflowIDs []StorageID
	var collisionSlabIDs []StorageID

	level := 0
	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []StorageID(nil)

		for _, id := range ids {

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				return nil, err
			}

			if slab.IsData() {
				dataSlab := slab.(*MapDataSlab)
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, dataSlab))

				for i := 0; i < int(dataSlab.elements.Count()); i++ {
					elem, err := dataSlab.elements.Element(i)
					if err != nil {
						return nil, err
					}
					if group, ok := elem.(elementGroup); ok {
						if !group.Inline() {
							extSlab := group.(*externalCollisionGroup)
							collisionSlabIDs = append(collisionSlabIDs, extSlab.id)
						}
					}
				}

				childStorables := dataSlab.ChildStorables()
				for _, e := range childStorables {
					if id, ok := e.(StorageIDStorable); ok {
						overflowIDs = append(overflowIDs, StorageID(id))
					}
				}

			} else {
				meta := slab.(*MapMetaDataSlab)
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

	for _, id := range collisionSlabIDs {
		slab, err := getMapSlab(m.Storage, id)
		if err != nil {
			return nil, err
		}
		dumps = append(dumps, fmt.Sprintf("collision: %s", slab.String()))
	}

	// overflowIDs include collisionSlabIDs
	for _, id := range overflowIDs {
		found := false
		for _, cid := range collisionSlabIDs {
			if id == cid {
				found = true
				break
			}
		}
		if found {
			continue
		}
		slab, found, err := m.Storage.Retrieve(id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(id, "slab not found during map slab dump")
		}
		dumps = append(dumps, fmt.Sprintf("overflow: %s", slab))
	}

	return dumps, nil
}

func ValidMap(m *OrderedMap, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider) error {

	extraData := m.root.ExtraData()
	if extraData == nil {
		return fmt.Errorf("root slab %d doesn't have extra data", m.root.ID())
	}

	// Verify that extra data has correct type information
	if typeInfo != nil && !tic(extraData.TypeInfo, typeInfo) {
		return fmt.Errorf(
			"root slab %d type information %v, want %v",
			m.root.ID(),
			extraData.TypeInfo,
			typeInfo,
		)
	}

	// Verify that extra data has seed
	if extraData.Seed == 0 {
		return fmt.Errorf("root slab %d seed is uninitialized", m.root.ID())
	}

	computedCount, dataSlabIDs, nextDataSlabIDs, firstKeys, err := validMapSlab(
		m.Storage, m.digesterBuilder, tic, hip, m.root.ID(), 0, nil, []StorageID{}, []StorageID{}, []Digest{})
	if err != nil {
		return err
	}

	// Verify that extra data has correct count
	if computedCount != extraData.Count {
		return fmt.Errorf("root slab %d count %d is wrong, want %d",
			m.root.ID(), extraData.Count, computedCount)
	}

	// Verify next data slab ids
	if !reflect.DeepEqual(dataSlabIDs[1:], nextDataSlabIDs) {
		return fmt.Errorf("chained next data slab ids %v are wrong, want %v",
			nextDataSlabIDs, dataSlabIDs[1:])
	}

	// Verify data slabs' first keys are sorted
	if !sort.SliceIsSorted(firstKeys, func(i, j int) bool {
		return firstKeys[i] < firstKeys[j]
	}) {
		return fmt.Errorf("chained first keys %v are not sorted", firstKeys)
	}

	// Verify data slabs' first keys are unique
	if len(firstKeys) > 1 {
		prev := firstKeys[0]
		for _, d := range firstKeys[1:] {
			if prev == d {
				return fmt.Errorf("chained first keys %v are not unique", firstKeys)
			}
			prev = d
		}
	}

	return nil
}

func validMapSlab(
	storage SlabStorage,
	digesterBuilder DigesterBuilder,
	tic TypeInfoComparator,
	hip HashInputProvider,
	id StorageID,
	level int,
	headerFromParentSlab *MapSlabHeader,
	dataSlabIDs []StorageID,
	nextDataSlabIDs []StorageID,
	firstKeys []Digest,
) (
	elementCount uint64,
	_dataSlabIDs []StorageID,
	_nextDataSlabIDs []StorageID,
	_firstKeys []Digest,
	err error,
) {

	slab, err := getMapSlab(storage, id)
	if err != nil {
		return 0, nil, nil, nil, err
	}

	if level > 0 {
		// Verify that non-root slab doesn't have extra data.
		if slab.ExtraData() != nil {
			return 0, nil, nil, nil, fmt.Errorf("non-root slab %d has extra data", id)
		}

		// Verify that non-root slab doesn't underflow
		if underflowSize, underflow := slab.IsUnderflow(); underflow {
			return 0, nil, nil, nil, fmt.Errorf("slab %d underflows by %d bytes", id, underflowSize)
		}

	}

	// Verify that slab doesn't overflow
	if slab.IsFull() {
		return 0, nil, nil, nil, fmt.Errorf("slab %d overflows", id)
	}

	// Verify that header is in sync with header from parent slab
	if headerFromParentSlab != nil {
		if !reflect.DeepEqual(*headerFromParentSlab, slab.Header()) {
			return 0, nil, nil, nil, fmt.Errorf("slab %d header %+v is different from header %+v from parent slab",
				id, slab.Header(), headerFromParentSlab)
		}
	}

	if slab.IsData() {

		dataSlab, ok := slab.(*MapDataSlab)
		if !ok {
			return 0, nil, nil, nil, fmt.Errorf("slab %d is not MapDataSlab", id)
		}

		// Verify data slab's elements
		elementCount, elementSize, err := validMapElements(storage, digesterBuilder, tic, hip, id, dataSlab.elements, 0, nil)
		if err != nil {
			return 0, nil, nil, nil, err
		}

		// Verify slab's first key
		if dataSlab.elements.firstKey() != dataSlab.header.firstKey {
			return 0, nil, nil, nil, fmt.Errorf("data slab %d header first key %d is wrong, want %d",
				id, dataSlab.header.firstKey, dataSlab.elements.firstKey())
		}

		// Verify that aggregated element size + slab prefix is the same as header.size
		computedSize := uint32(mapDataSlabPrefixSize)
		if level == 0 {
			computedSize = uint32(mapRootDataSlabPrefixSize)
		}
		computedSize += elementSize

		if computedSize != dataSlab.header.size {
			return 0, nil, nil, nil, fmt.Errorf("data slab %d header size %d is wrong, want %d",
				id, dataSlab.header.size, computedSize)
		}

		// Verify any size flag
		if dataSlab.anySize {
			return 0, nil, nil, nil, fmt.Errorf("data slab %d anySize %t is wrong, want false",
				id, dataSlab.anySize)
		}

		// Verify collision group flag
		if dataSlab.collisionGroup {
			return 0, nil, nil, nil, fmt.Errorf("data slab %d collisionGroup %t is wrong, want false",
				id, dataSlab.collisionGroup)
		}

		dataSlabIDs = append(dataSlabIDs, id)

		if dataSlab.next != StorageIDUndefined {
			nextDataSlabIDs = append(nextDataSlabIDs, dataSlab.next)
		}

		firstKeys = append(firstKeys, dataSlab.header.firstKey)

		return elementCount, dataSlabIDs, nextDataSlabIDs, firstKeys, nil
	}

	meta, ok := slab.(*MapMetaDataSlab)
	if !ok {
		return 0, nil, nil, nil, fmt.Errorf("slab %d is not MapMetaDataSlab", id)
	}

	if level == 0 {
		// Verify that root slab has more than one child slabs
		if len(meta.childrenHeaders) < 2 {
			return 0, nil, nil, nil, fmt.Errorf("root metadata slab %d has %d children, want at least 2 children ",
				id, len(meta.childrenHeaders))
		}
	}

	elementCount = 0
	for _, h := range meta.childrenHeaders {
		// Verify child slabs
		count := uint64(0)
		count, dataSlabIDs, nextDataSlabIDs, firstKeys, err =
			validMapSlab(storage, digesterBuilder, tic, hip, h.id, level+1, &h, dataSlabIDs, nextDataSlabIDs, firstKeys)
		if err != nil {
			return 0, nil, nil, nil, err
		}

		elementCount += count
	}

	// Verify slab header first key
	if meta.childrenHeaders[0].firstKey != meta.header.firstKey {
		return 0, nil, nil, nil, fmt.Errorf("metadata slab %d header first key %d is wrong, want %d",
			id, meta.header.firstKey, meta.childrenHeaders[0].firstKey)
	}

	// Verify that child slab's first keys are sorted.
	sortedHKey := sort.SliceIsSorted(meta.childrenHeaders, func(i, j int) bool {
		return meta.childrenHeaders[i].firstKey < meta.childrenHeaders[j].firstKey
	})
	if !sortedHKey {
		return 0, nil, nil, nil, fmt.Errorf("metadata slab %d child slab's first key isn't sorted %+v", id, meta.childrenHeaders)
	}

	// Verify that child slab's first keys are unique.
	if len(meta.childrenHeaders) > 1 {
		prev := meta.childrenHeaders[0].firstKey
		for _, h := range meta.childrenHeaders[1:] {
			if prev == h.firstKey {
				return 0, nil, nil, nil, fmt.Errorf("metadata slab %d child header first key isn't unique %v",
					id, meta.childrenHeaders)
			}
			prev = h.firstKey
		}
	}

	// Verify slab header's size
	computedSize := uint32(len(meta.childrenHeaders)*mapSlabHeaderSize) + mapMetaDataSlabPrefixSize
	if computedSize != meta.header.size {
		return 0, nil, nil, nil, fmt.Errorf("metadata slab %d header size %d is wrong, want %d",
			id, meta.header.size, computedSize)
	}

	return elementCount, dataSlabIDs, nextDataSlabIDs, firstKeys, nil
}

func validMapElements(
	storage SlabStorage,
	db DigesterBuilder,
	tic TypeInfoComparator,
	hip HashInputProvider,
	id StorageID,
	elements elements,
	digestLevel int,
	hkeyPrefixes []Digest,
) (
	elementCount uint64,
	elementSize uint32,
	err error,
) {

	switch elems := elements.(type) {
	case *hkeyElements:
		return validMapHkeyElements(storage, db, tic, hip, id, elems, digestLevel, hkeyPrefixes)
	case *singleElements:
		return validMapSingleElements(storage, db, tic, hip, id, elems, digestLevel, hkeyPrefixes)
	default:
		return 0, 0, fmt.Errorf("slab %d has unknown elements type %T at digest level %d", id, elements, digestLevel)
	}
}

func validMapHkeyElements(
	storage SlabStorage,
	db DigesterBuilder,
	tic TypeInfoComparator,
	hip HashInputProvider,
	id StorageID,
	elements *hkeyElements,
	digestLevel int,
	hkeyPrefixes []Digest,
) (
	elementCount uint64,
	elementSize uint32,
	err error,
) {

	// Verify element's level
	if digestLevel != elements.level {
		return 0, 0, fmt.Errorf("data slab %d elements digest level %d is wrong, want %d",
			id, elements.level, digestLevel)
	}

	// Verify number of hkeys is the same as number of elements
	if len(elements.hkeys) != len(elements.elems) {
		return 0, 0, fmt.Errorf("data slab %d hkeys count %d is wrong, want %d",
			id, len(elements.hkeys), len(elements.elems))
	}

	// Verify hkeys are sorted
	if !sort.SliceIsSorted(elements.hkeys, func(i, j int) bool {
		return elements.hkeys[i] < elements.hkeys[j]
	}) {
		return 0, 0, fmt.Errorf("data slab %d hkeys is not sorted %v", id, elements.hkeys)
	}

	// Verify hkeys are unique
	if len(elements.hkeys) > 1 {
		prev := elements.hkeys[0]
		for _, d := range elements.hkeys[1:] {
			if prev == d {
				return 0, 0, fmt.Errorf("data slab %d hkeys is not unique %v", id, elements.hkeys)
			}
			prev = d
		}
	}

	elementSize = uint32(hkeyElementsPrefixSize)

	for i := 0; i < len(elements.elems); i++ {
		e := elements.elems[i]

		elementSize += digestSize

		// Verify element size is <= inline size
		if digestLevel == 0 {
			if e.Size() > uint32(maxInlineMapElementSize) {
				return 0, 0, fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
					id, e, e.Size(), maxInlineMapElementSize)
			}
		}

		if group, ok := e.(elementGroup); ok {

			ge, err := group.Elements(storage)
			if err != nil {
				return 0, 0, err
			}

			hkeys := make([]Digest, len(hkeyPrefixes)+1)
			copy(hkeys, hkeyPrefixes)
			hkeys[len(hkeys)-1] = elements.hkeys[i]

			count, size, err := validMapElements(storage, db, tic, hip, id, ge, digestLevel+1, hkeys)
			if err != nil {
				return 0, 0, err
			}

			if _, ok := e.(*inlineCollisionGroup); ok {
				size += inlineCollisionGroupPrefixSize
			} else {
				size = externalCollisionGroupPrefixSize + 2 + 1 + 16
			}

			// Verify element group size
			if size != e.Size() {
				return 0, 0, fmt.Errorf("data slab %d element %s size %d is wrong, want %d", id, e, e.Size(), size)
			}

			elementSize += e.Size()

			elementCount += uint64(count)

		} else {

			se, ok := e.(*singleElement)
			if !ok {
				return 0, 0, fmt.Errorf("data slab %d element type %T is wrong, want *singleElement", id, e)
			}

			hkeys := make([]Digest, len(hkeyPrefixes)+1)
			copy(hkeys, hkeyPrefixes)
			hkeys[len(hkeys)-1] = elements.hkeys[i]

			// Verify element
			computedSize, maxDigestLevel, err := validSingleElement(storage, db, tic, hip, se, hkeys)
			if err != nil {
				return 0, 0, fmt.Errorf("data slab %d: %w", id, err)
			}

			// Verify digest level
			if digestLevel >= maxDigestLevel {
				return 0, 0, fmt.Errorf("data slab %d, hkey elements %s: digest level %d is wrong, want < %d",
					id, elements, digestLevel, maxDigestLevel)
			}

			elementSize += computedSize

			elementCount++
		}
	}

	// Verify elements size
	if elementSize != elements.Size() {
		return 0, 0, fmt.Errorf("data slab %d elements size %d is wrong, want %d", id, elements.Size(), elementSize)
	}

	return elementCount, elementSize, nil
}

func validMapSingleElements(
	storage SlabStorage,
	db DigesterBuilder,
	tic TypeInfoComparator,
	hip HashInputProvider,
	id StorageID,
	elements *singleElements,
	digestLevel int,
	hkeyPrefixes []Digest,
) (
	elementCount uint64,
	elementSize uint32,
	err error,
) {

	// Verify elements' level
	if digestLevel != elements.level {
		return 0, 0, fmt.Errorf("data slab %d elements level %d is wrong, want %d",
			id, elements.level, digestLevel)
	}

	elementSize = singleElementsPrefixSize

	for _, e := range elements.elems {

		// Verify element
		computedSize, maxDigestLevel, err := validSingleElement(storage, db, tic, hip, e, hkeyPrefixes)
		if err != nil {
			return 0, 0, fmt.Errorf("data slab %d: %w", id, err)
		}

		// Verify element size is <= inline size
		if e.Size() > uint32(maxInlineMapElementSize) {
			return 0, 0, fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
				id, e, e.Size(), maxInlineMapElementSize)
		}

		// Verify digest level
		if digestLevel != maxDigestLevel {
			return 0, 0, fmt.Errorf("data slab %d single elements %s digest level %d is wrong, want %d",
				id, elements, digestLevel, maxDigestLevel)
		}

		elementSize += computedSize
	}

	// Verify elements size
	if elementSize != elements.Size() {
		return 0, 0, fmt.Errorf("slab %d elements size %d is wrong, want %d", id, elements.Size(), elementSize)
	}

	return uint64(len(elements.elems)), elementSize, nil
}

func validSingleElement(
	storage SlabStorage,
	db DigesterBuilder,
	tic TypeInfoComparator,
	hip HashInputProvider,
	e *singleElement,
	digests []Digest,
) (
	size uint32,
	digestMaxLevel int,
	err error,
) {

	// Verify key pointer
	if _, keyPointer := e.key.(StorageIDStorable); e.keyPointer != keyPointer {
		return 0, 0, fmt.Errorf("element %s keyPointer %t is wrong, want %t", e, e.keyPointer, keyPointer)
	}

	// Verify key
	kv, err := e.key.StoredValue(storage)
	if err != nil {
		return 0, 0, fmt.Errorf("element %s key can't be converted to value: %w", e, err)
	}

	err = ValidValue(kv, nil, tic, hip)
	if err != nil {
		return 0, 0, fmt.Errorf("element %s key isn't valid: %w", e, err)
	}

	// Verify value pointer
	if _, valuePointer := e.value.(StorageIDStorable); e.valuePointer != valuePointer {
		return 0, 0, fmt.Errorf("element %s valuePointer %t is wrong, want %t", e, e.valuePointer, valuePointer)
	}

	// Verify value
	vv, err := e.value.StoredValue(storage)
	if err != nil {
		return 0, 0, fmt.Errorf("element %s value can't be converted to value: %w", e, err)
	}

	err = ValidValue(vv, nil, tic, hip)
	if err != nil {
		return 0, 0, fmt.Errorf("element %s value isn't valid: %w", e, err)
	}

	// Verify size
	computedSize := singleElementPrefixSize + e.key.ByteSize() + e.value.ByteSize()
	if computedSize != e.Size() {
		return 0, 0, fmt.Errorf("element %s size %d is wrong, want %d", e, e.Size(), computedSize)
	}

	// Verify digest
	digest, err := db.Digest(hip, kv)
	if err != nil {
		return 0, 0, err
	}

	computedDigests, err := digest.DigestPrefix(digest.Levels())
	if err != nil {
		return 0, 0, err
	}

	if !reflect.DeepEqual(digests, computedDigests[:len(digests)]) {
		return 0, 0, fmt.Errorf("element %s digest %v is wrong, want %v", e, digests, computedDigests)
	}

	return computedSize, digest.Levels(), nil
}

func ValidValue(value Value, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider) error {
	switch v := value.(type) {
	case *Array:
		return ValidArray(v, typeInfo, tic, hip)
	case *OrderedMap:
		return ValidMap(v, typeInfo, tic, hip)
	}
	return nil
}

// ValidMapSerialization traverses ordered map tree and verifies serialization
// by encoding, decoding, and re-encoding slabs.
// It compares in-memory objects of original slab with decoded slab.
// It also compares encoded data of original slab with encoded data of decoded slab.
func ValidMapSerialization(
	m *OrderedMap,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {
	return validMapSlabSerialization(
		m.Storage,
		m.root.ID(),
		cborDecMode,
		cborEncMode,
		decodeStorable,
		decodeTypeInfo,
		compare,
	)
}

func validMapSlabSerialization(
	storage SlabStorage,
	id StorageID,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	slab, err := getMapSlab(storage, id)
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
	encodedExtraDataSize, err := getEncodedMapExtraDataSize(slab.ExtraData(), cborEncMode)
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
		dataSlab, ok := slab.(*MapDataSlab)
		if !ok {
			return fmt.Errorf("slab %d is not MapDataSlab", id)
		}

		decodedDataSlab, ok := decodedSlab.(*MapDataSlab)
		if !ok {
			return fmt.Errorf("decoded slab %d is not MapDataSlab", id)
		}

		// Compare slabs
		err = mapDataSlabEqual(
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

	metaSlab, ok := slab.(*MapMetaDataSlab)
	if !ok {
		return fmt.Errorf("slab %d is not MapMetaDataSlab", id)
	}

	decodedMetaSlab, ok := decodedSlab.(*MapMetaDataSlab)
	if !ok {
		return fmt.Errorf("decoded slab %d is not MapMetaDataSlab", id)
	}

	// Compare slabs
	err = mapMetaDataSlabEqual(metaSlab, decodedMetaSlab)
	if err != nil {
		return fmt.Errorf("metadata slab %d round-trip serialization failed: %w", id, err)
	}

	for _, h := range metaSlab.childrenHeaders {
		// Verify child slabs
		err = validMapSlabSerialization(
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

func mapDataSlabEqual(
	expected *MapDataSlab,
	actual *MapDataSlab,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	// Compare extra data
	err := mapExtraDataEqual(expected.extraData, actual.extraData)
	if err != nil {
		return err
	}

	// Compare next
	if expected.next != actual.next {
		return fmt.Errorf("next %d is wrong, want %d", actual.next, expected.next)
	}

	// Compare anySize flag
	if expected.anySize != actual.anySize {
		return fmt.Errorf("anySize %t is wrong, want %t", actual.anySize, expected.anySize)
	}

	// Compare collisionGroup flag
	if expected.collisionGroup != actual.collisionGroup {
		return fmt.Errorf("collisionGroup %t is wrong, want %t", actual.collisionGroup, expected.collisionGroup)
	}

	// Compare header
	if !reflect.DeepEqual(expected.header, actual.header) {
		return fmt.Errorf("header %+v is wrong, want %+v", actual.header, expected.header)
	}

	// Compare elements
	err = mapElementsEqual(
		expected.elements,
		actual.elements,
		storage,
		cborDecMode,
		cborEncMode,
		decodeStorable,
		decodeTypeInfo,
		compare,
	)
	if err != nil {
		return err
	}

	return nil
}

func mapElementsEqual(
	expected elements,
	actual elements,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {
	switch expectedElems := expected.(type) {

	case *hkeyElements:
		actualElems, ok := actual.(*hkeyElements)
		if !ok {
			return fmt.Errorf("elements type %T is wrong, want %T", actual, expected)
		}
		return mapHkeyElementsEqual(
			expectedElems,
			actualElems,
			storage,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)

	case *singleElements:
		actualElems, ok := actual.(*singleElements)
		if !ok {
			return fmt.Errorf("elements type %T is wrong, want %T", actual, expected)
		}
		return mapSingleElementsEqual(
			expectedElems,
			actualElems,
			storage,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)

	}

	return nil
}

func mapHkeyElementsEqual(
	expected *hkeyElements,
	actual *hkeyElements,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	if expected.level != actual.level {
		return fmt.Errorf("hkeyElements level %d is wrong, want %d", actual.level, expected.level)
	}

	if expected.size != actual.size {
		return fmt.Errorf("hkeyElements size %d is wrong, want %d", actual.size, expected.size)
	}

	if len(expected.hkeys) == 0 {
		if len(actual.hkeys) != 0 {
			return fmt.Errorf("hkeyElements hkeys %v is wrong, want %v", actual.hkeys, expected.hkeys)
		}
	} else {
		if !reflect.DeepEqual(expected.hkeys, actual.hkeys) {
			return fmt.Errorf("hkeyElements hkeys %v is wrong, want %v", actual.hkeys, expected.hkeys)
		}
	}

	if len(expected.elems) != len(actual.elems) {
		return fmt.Errorf("hkeyElements elems len %d is wrong, want %d", len(actual.elems), len(expected.elems))
	}

	for i := 0; i < len(expected.elems); i++ {
		expectedEle := expected.elems[i]
		actualEle := actual.elems[i]

		err := mapElementEqual(
			expectedEle,
			actualEle,
			storage,
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

func mapSingleElementsEqual(
	expected *singleElements,
	actual *singleElements,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	if expected.level != actual.level {
		return fmt.Errorf("singleElements level %d is wrong, want %d", actual.level, expected.level)
	}

	if expected.size != actual.size {
		return fmt.Errorf("singleElements size %d is wrong, want %d", actual.size, expected.size)
	}

	if len(expected.elems) != len(actual.elems) {
		return fmt.Errorf("singleElements elems len %d is wrong, want %d", len(actual.elems), len(expected.elems))
	}

	for i := 0; i < len(expected.elems); i++ {
		expectedElem := expected.elems[i]
		actualElem := actual.elems[i]

		err := mapSingleElementEqual(
			expectedElem,
			actualElem,
			storage,
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

func mapElementEqual(
	expected element,
	actual element,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {
	switch expectedElem := expected.(type) {

	case *singleElement:
		actualElem, ok := actual.(*singleElement)
		if !ok {
			return fmt.Errorf("elements type %T is wrong, want %T", actual, expected)
		}
		return mapSingleElementEqual(
			expectedElem,
			actualElem,
			storage,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)

	case *inlineCollisionGroup:
		actualElem, ok := actual.(*inlineCollisionGroup)
		if !ok {
			return fmt.Errorf("elements type %T is wrong, want %T", actual, expected)
		}
		return mapElementsEqual(
			expectedElem.elements,
			actualElem.elements,
			storage,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)

	case *externalCollisionGroup:
		actualElem, ok := actual.(*externalCollisionGroup)
		if !ok {
			return fmt.Errorf("elements type %T is wrong, want %T", actual, expected)
		}
		return mapExternalCollisionElementsEqual(
			expectedElem,
			actualElem,
			storage,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)

	}

	return nil
}

func mapExternalCollisionElementsEqual(
	expected *externalCollisionGroup,
	actual *externalCollisionGroup,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	if expected.size != actual.size {
		return fmt.Errorf("externalCollisionGroup size %d is wrong, want %d", actual.size, expected.size)
	}

	if expected.id != actual.id {
		return fmt.Errorf("externalCollisionGroup id %d is wrong, want %d", actual.id, expected.id)
	}

	// Compare external collision slab
	err := validMapSlabSerialization(
		storage,
		expected.id,
		cborDecMode,
		cborEncMode,
		decodeStorable,
		decodeTypeInfo,
		compare,
	)
	if err != nil {
		return err
	}

	return nil
}

func mapSingleElementEqual(
	expected *singleElement,
	actual *singleElement,
	storage SlabStorage,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	if expected.size != actual.size {
		return fmt.Errorf("singleElement size %d is wrong, want %d", actual.size, expected.size)
	}

	if expected.keyPointer != actual.keyPointer {
		return fmt.Errorf("singleElement keyPointer %t is wrong, want %t", actual.keyPointer, expected.keyPointer)
	}

	if expected.valuePointer != actual.valuePointer {
		return fmt.Errorf("singleElement valuePointer %t is wrong, want %t", actual.valuePointer, expected.valuePointer)
	}

	if !compare(expected.key, actual.key) {
		return fmt.Errorf("singleElement key %v is wrong, want %v", actual.key, expected.key)
	}

	// Compare key stored in a separate slab
	if idStorable, ok := expected.key.(StorageIDStorable); ok {

		v, err := idStorable.StoredValue(storage)
		if err != nil {
			return err
		}

		err = ValidValueSerialization(
			v,
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

	if !compare(expected.value, actual.value) {
		return fmt.Errorf("singleElement value %v is wrong, want %v", actual.value, expected.value)
	}

	// Compare value stored in a separate slab
	if idStorable, ok := expected.value.(StorageIDStorable); ok {

		v, err := idStorable.StoredValue(storage)
		if err != nil {
			return err
		}

		err = ValidValueSerialization(
			v,
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

func mapMetaDataSlabEqual(expected, actual *MapMetaDataSlab) error {

	// Compare extra data
	err := mapExtraDataEqual(expected.extraData, actual.extraData)
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

	return nil
}

func mapExtraDataEqual(expected, actual *MapExtraData) error {

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

func getEncodedMapExtraDataSize(extraData *MapExtraData, cborEncMode cbor.EncMode) (int, error) {
	if extraData == nil {
		return 0, nil
	}

	var buf bytes.Buffer
	enc := NewEncoder(&buf, cborEncMode)

	err := extraData.Encode(enc, byte(0), byte(0))
	if err != nil {
		return 0, err
	}

	return len(buf.Bytes()), nil
}
