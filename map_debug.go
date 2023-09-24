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

	nextLevelIDs := []SlabID{m.SlabID()}

	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getMapSlab().
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
								// Don't need to wrap error as external error because err is already categorized by elements.Element().
								return MapStats{}, err
							}

							if group, ok := elem.(elementGroup); ok {
								if !group.Inline() {
									collisionDataSlabCount++
								}

								nested, err := group.Elements(m.Storage)
								if err != nil {
									// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
									return MapStats{}, err
								}
								nestedElementGroups = append(nestedElementGroups, nested)

							} else {
								e := elem.(*singleElement)
								if _, ok := e.key.(SlabIDStorable); ok {
									storableDataSlabCount++
								}
								if _, ok := e.value.(SlabIDStorable); ok {
									storableDataSlabCount++
								}
								// This handles use case of inlined array or map value containing SlabID
								ids := getSlabIDFromStorable(e.value, nil)
								storableDataSlabCount += uint64(len(ids))
							}
						}
					}
					elementGroups = nestedElementGroups
				}
			} else {
				metaDataSlabCount++

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(SlabIDStorable)
					if !ok {
						return MapStats{}, NewFatalError(fmt.Errorf("metadata slab's child storables are not of type SlabIDStorable"))
					}
					nextLevelIDs = append(nextLevelIDs, SlabID(id))
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

	nextLevelIDs := []SlabID{m.SlabID()}

	var overflowIDs []SlabID
	var collisionSlabIDs []SlabID

	level := 0
	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getMapSlab().
				return nil, err
			}

			if slab.IsData() {
				dataSlab := slab.(*MapDataSlab)
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, dataSlab))

				for i := 0; i < int(dataSlab.elements.Count()); i++ {
					elem, err := dataSlab.elements.Element(i)
					if err != nil {
						// Don't need to wrap error as external error because err is already categorized by elements.Element().
						return nil, err
					}
					if group, ok := elem.(elementGroup); ok {
						if !group.Inline() {
							extSlab := group.(*externalCollisionGroup)
							collisionSlabIDs = append(collisionSlabIDs, extSlab.slabID)
						}
					}
				}

				overflowIDs = getSlabIDFromStorable(dataSlab, overflowIDs)

			} else {
				meta := slab.(*MapMetaDataSlab)
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, meta))

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

	for _, id := range collisionSlabIDs {
		slab, err := getMapSlab(m.Storage, id)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
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
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(id, "slab not found during map slab dump")
		}
		dumps = append(dumps, slab.String())
	}

	return dumps, nil
}

func VerifyMap(m *OrderedMap, address Address, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider, inlineEnabled bool) error {

	// Verify map address
	if address != m.Address() {
		return NewFatalError(fmt.Errorf("map address %v, got %v", address, m.Address()))
	}

	// Verify map value ID
	err := verifyMapValueID(m)
	if err != nil {
		return err
	}

	// Verify map slab ID
	err = verifyMapSlabID(m)
	if err != nil {
		return err
	}

	// Verify map extra data
	extraData := m.root.ExtraData()
	if extraData == nil {
		return NewFatalError(fmt.Errorf("root slab %d doesn't have extra data", m.root.SlabID()))
	}

	// Verify that extra data has correct type information
	if typeInfo != nil && !tic(extraData.TypeInfo, typeInfo) {
		return NewFatalError(
			fmt.Errorf(
				"root slab %d type information %v, want %v",
				m.root.SlabID(),
				extraData.TypeInfo,
				typeInfo,
			))
	}

	// Verify that extra data has seed
	if extraData.Seed == 0 {
		return NewFatalError(fmt.Errorf("root slab %d seed is uninitialized", m.root.SlabID()))
	}

	v := &mapVerifier{
		storage:         m.Storage,
		address:         address,
		digesterBuilder: m.digesterBuilder,
		tic:             tic,
		hip:             hip,
		inlineEnabled:   inlineEnabled,
	}

	computedCount, dataSlabIDs, nextDataSlabIDs, firstKeys, err := v.verifySlab(
		m.root, 0, nil, []SlabID{}, []SlabID{}, []Digest{})
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifySlab().
		return err
	}

	// Verify that extra data has correct count
	if computedCount != extraData.Count {
		return NewFatalError(
			fmt.Errorf(
				"root slab %d count %d is wrong, want %d",
				m.root.SlabID(),
				extraData.Count,
				computedCount,
			))
	}

	// Verify next data slab ids
	if !reflect.DeepEqual(dataSlabIDs[1:], nextDataSlabIDs) {
		return NewFatalError(fmt.Errorf("chained next data slab ids %v are wrong, want %v",
			nextDataSlabIDs, dataSlabIDs[1:]))
	}

	// Verify data slabs' first keys are sorted
	if !sort.SliceIsSorted(firstKeys, func(i, j int) bool {
		return firstKeys[i] < firstKeys[j]
	}) {
		return NewFatalError(fmt.Errorf("chained first keys %v are not sorted", firstKeys))
	}

	// Verify data slabs' first keys are unique
	if len(firstKeys) > 1 {
		prev := firstKeys[0]
		for _, d := range firstKeys[1:] {
			if prev == d {
				return NewFatalError(fmt.Errorf("chained first keys %v are not unique", firstKeys))
			}
			prev = d
		}
	}

	return nil
}

type mapVerifier struct {
	storage         SlabStorage
	address         Address
	digesterBuilder DigesterBuilder
	tic             TypeInfoComparator
	hip             HashInputProvider
	inlineEnabled   bool
}

func (v *mapVerifier) verifySlab(
	slab MapSlab,
	level int,
	headerFromParentSlab *MapSlabHeader,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	firstKeys []Digest,
) (
	elementCount uint64,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	_firstKeys []Digest,
	err error,
) {

	id := slab.Header().slabID

	// Verify slab address (independent of map inlined status)
	if v.address != id.address {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("map slab address %v, got %v", v.address, id.address))
	}

	// Verify that inlined slab is not in storage
	if slab.Inlined() {
		_, exist, err := v.storage.Retrieve(id)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storage interface.
			return 0, nil, nil, nil, wrapErrorAsExternalErrorIfNeeded(err)
		}
		if exist {
			return 0, nil, nil, nil, NewFatalError(fmt.Errorf("inlined slab %s is in storage", id))
		}
	}

	if level > 0 {
		// Verify that non-root slab doesn't have extra data.
		if slab.ExtraData() != nil {
			return 0, nil, nil, nil, NewFatalError(fmt.Errorf("non-root slab %d has extra data", id))
		}

		// Verify that non-root slab doesn't underflow
		if underflowSize, underflow := slab.IsUnderflow(); underflow {
			return 0, nil, nil, nil, NewFatalError(fmt.Errorf("slab %d underflows by %d bytes", id, underflowSize))
		}

	}

	// Verify that slab doesn't overflow
	if slab.IsFull() {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("slab %d overflows", id))
	}

	// Verify that header is in sync with header from parent slab
	if headerFromParentSlab != nil {
		if !reflect.DeepEqual(*headerFromParentSlab, slab.Header()) {
			return 0, nil, nil, nil, NewFatalError(
				fmt.Errorf("slab %d header %+v is different from header %+v from parent slab",
					id, slab.Header(), headerFromParentSlab))
		}
	}

	switch slab := slab.(type) {
	case *MapDataSlab:
		return v.verifyDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, firstKeys)

	case *MapMetaDataSlab:
		return v.verifyMetaDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, firstKeys)

	default:
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("MapSlab is either *MapDataSlab or *MapMetaDataSlab, got %T", slab))
	}
}

func (v *mapVerifier) verifyDataSlab(
	dataSlab *MapDataSlab,
	level int,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	firstKeys []Digest,
) (
	elementCount uint64,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	_firstKeys []Digest,
	err error,
) {
	id := dataSlab.header.slabID

	if !dataSlab.IsData() {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("MapDataSlab %s is not data", id))
	}

	// Verify data slab's elements
	elementCount, elementSize, err := v.verifyElements(id, dataSlab.elements, 0, nil)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifyElements().
		return 0, nil, nil, nil, err
	}

	// Verify slab's first key
	if dataSlab.elements.firstKey() != dataSlab.header.firstKey {
		return 0, nil, nil, nil, NewFatalError(
			fmt.Errorf("data slab %d header first key %d is wrong, want %d",
				id, dataSlab.header.firstKey, dataSlab.elements.firstKey()))
	}

	// Verify that only root slab can be inlined
	if level > 0 && dataSlab.Inlined() {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("non-root slab %s is inlined", id))
	}

	// Verify that aggregated element size + slab prefix is the same as header.size
	computedSize := uint32(mapDataSlabPrefixSize)
	if level == 0 {
		computedSize = uint32(mapRootDataSlabPrefixSize)
		if dataSlab.Inlined() {
			computedSize = uint32(inlinedMapDataSlabPrefixSize)
		}
	}
	computedSize += elementSize

	if computedSize != dataSlab.header.size {
		return 0, nil, nil, nil, NewFatalError(
			fmt.Errorf("data slab %d header size %d is wrong, want %d",
				id, dataSlab.header.size, computedSize))
	}

	// Verify any size flag
	if dataSlab.anySize {
		return 0, nil, nil, nil, NewFatalError(
			fmt.Errorf("data slab %d anySize %t is wrong, want false",
				id, dataSlab.anySize))
	}

	// Verify collision group flag
	if dataSlab.collisionGroup {
		return 0, nil, nil, nil, NewFatalError(
			fmt.Errorf("data slab %d collisionGroup %t is wrong, want false",
				id, dataSlab.collisionGroup))
	}

	dataSlabIDs = append(dataSlabIDs, id)

	if dataSlab.next != SlabIDUndefined {
		nextDataSlabIDs = append(nextDataSlabIDs, dataSlab.next)
	}

	firstKeys = append(firstKeys, dataSlab.header.firstKey)

	return elementCount, dataSlabIDs, nextDataSlabIDs, firstKeys, nil
}

func (v *mapVerifier) verifyMetaDataSlab(
	metaSlab *MapMetaDataSlab,
	level int,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	firstKeys []Digest,
) (
	elementCount uint64,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	_firstKeys []Digest,
	err error,
) {
	id := metaSlab.header.slabID

	if metaSlab.IsData() {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("MapMetaDataSlab %s is data", id))
	}

	if metaSlab.Inlined() {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("MapMetaDataSlab %s can't be inlined", id))
	}

	if level == 0 {
		// Verify that root slab has more than one child slabs
		if len(metaSlab.childrenHeaders) < 2 {
			return 0, nil, nil, nil, NewFatalError(
				fmt.Errorf("root metadata slab %d has %d children, want at least 2 children ",
					id, len(metaSlab.childrenHeaders)))
		}
	}

	elementCount = 0
	for i := 0; i < len(metaSlab.childrenHeaders); i++ {
		h := metaSlab.childrenHeaders[i]

		childSlab, err := getMapSlab(v.storage, h.slabID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return 0, nil, nil, nil, err
		}

		// Verify child slabs
		count := uint64(0)
		count, dataSlabIDs, nextDataSlabIDs, firstKeys, err =
			v.verifySlab(childSlab, level+1, &h, dataSlabIDs, nextDataSlabIDs, firstKeys)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifySlab().
			return 0, nil, nil, nil, err
		}

		elementCount += count
	}

	// Verify slab header first key
	if metaSlab.childrenHeaders[0].firstKey != metaSlab.header.firstKey {
		return 0, nil, nil, nil, NewFatalError(
			fmt.Errorf("metadata slab %d header first key %d is wrong, want %d",
				id, metaSlab.header.firstKey, metaSlab.childrenHeaders[0].firstKey))
	}

	// Verify that child slab's first keys are sorted.
	sortedHKey := sort.SliceIsSorted(metaSlab.childrenHeaders, func(i, j int) bool {
		return metaSlab.childrenHeaders[i].firstKey < metaSlab.childrenHeaders[j].firstKey
	})
	if !sortedHKey {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d child slab's first key isn't sorted %+v", id, metaSlab.childrenHeaders))
	}

	// Verify that child slab's first keys are unique.
	if len(metaSlab.childrenHeaders) > 1 {
		prev := metaSlab.childrenHeaders[0].firstKey
		for _, h := range metaSlab.childrenHeaders[1:] {
			if prev == h.firstKey {
				return 0, nil, nil, nil, NewFatalError(
					fmt.Errorf("metadata slab %d child header first key isn't unique %v",
						id, metaSlab.childrenHeaders))
			}
			prev = h.firstKey
		}
	}

	// Verify slab header's size
	computedSize := uint32(len(metaSlab.childrenHeaders)*mapSlabHeaderSize) + mapMetaDataSlabPrefixSize
	if computedSize != metaSlab.header.size {
		return 0, nil, nil, nil, NewFatalError(
			fmt.Errorf("metadata slab %d header size %d is wrong, want %d",
				id, metaSlab.header.size, computedSize))
	}

	return elementCount, dataSlabIDs, nextDataSlabIDs, firstKeys, nil
}

func (v *mapVerifier) verifyElements(
	id SlabID,
	elements elements,
	digestLevel uint,
	hkeyPrefixes []Digest,
) (
	elementCount uint64,
	elementSize uint32,
	err error,
) {

	switch elems := elements.(type) {
	case *hkeyElements:
		return v.verifyHkeyElements(id, elems, digestLevel, hkeyPrefixes)
	case *singleElements:
		return v.verifySingleElements(id, elems, digestLevel, hkeyPrefixes)
	default:
		return 0, 0, NewFatalError(fmt.Errorf("slab %d has unknown elements type %T at digest level %d", id, elements, digestLevel))
	}
}

func (v *mapVerifier) verifyHkeyElements(
	id SlabID,
	elements *hkeyElements,
	digestLevel uint,
	hkeyPrefixes []Digest,
) (
	elementCount uint64,
	elementSize uint32,
	err error,
) {

	// Verify element's level
	if digestLevel != elements.level {
		return 0, 0, NewFatalError(
			fmt.Errorf("data slab %d elements digest level %d is wrong, want %d",
				id, elements.level, digestLevel))
	}

	// Verify number of hkeys is the same as number of elements
	if len(elements.hkeys) != len(elements.elems) {
		return 0, 0, NewFatalError(
			fmt.Errorf("data slab %d hkeys count %d is wrong, want %d",
				id, len(elements.hkeys), len(elements.elems)))
	}

	// Verify hkeys are sorted
	if !sort.SliceIsSorted(elements.hkeys, func(i, j int) bool {
		return elements.hkeys[i] < elements.hkeys[j]
	}) {
		return 0, 0, NewFatalError(fmt.Errorf("data slab %d hkeys is not sorted %v", id, elements.hkeys))
	}

	// Verify hkeys are unique
	if len(elements.hkeys) > 1 {
		prev := elements.hkeys[0]
		for _, d := range elements.hkeys[1:] {
			if prev == d {
				return 0, 0, NewFatalError(fmt.Errorf("data slab %d hkeys is not unique %v", id, elements.hkeys))
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
				return 0, 0, NewFatalError(
					fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
						id, e, e.Size(), maxInlineMapElementSize))
			}
		}

		if group, ok := e.(elementGroup); ok {

			ge, err := group.Elements(v.storage)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
				return 0, 0, err
			}

			hkeys := make([]Digest, len(hkeyPrefixes)+1)
			copy(hkeys, hkeyPrefixes)
			hkeys[len(hkeys)-1] = elements.hkeys[i]

			count, size, err := v.verifyElements(id, ge, digestLevel+1, hkeys)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by verifyElements().
				return 0, 0, err
			}

			if _, ok := e.(*inlineCollisionGroup); ok {
				size += inlineCollisionGroupPrefixSize
			} else {
				size = externalCollisionGroupPrefixSize + 2 + 1 + 16
			}

			// Verify element group size
			if size != e.Size() {
				return 0, 0, NewFatalError(fmt.Errorf("data slab %d element %s size %d is wrong, want %d", id, e, e.Size(), size))
			}

			elementSize += e.Size()

			elementCount += count

		} else {

			se, ok := e.(*singleElement)
			if !ok {
				return 0, 0, NewFatalError(fmt.Errorf("data slab %d element type %T is wrong, want *singleElement", id, e))
			}

			hkeys := make([]Digest, len(hkeyPrefixes)+1)
			copy(hkeys, hkeyPrefixes)
			hkeys[len(hkeys)-1] = elements.hkeys[i]

			// Verify element
			computedSize, maxDigestLevel, err := v.verifySingleElement(se, hkeys)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by verifySingleElement().
				return 0, 0, fmt.Errorf("data slab %d: %w", id, err)
			}

			// Verify digest level
			if digestLevel >= maxDigestLevel {
				return 0, 0, NewFatalError(
					fmt.Errorf("data slab %d, hkey elements %s: digest level %d is wrong, want < %d",
						id, elements, digestLevel, maxDigestLevel))
			}

			elementSize += computedSize

			elementCount++
		}
	}

	// Verify elements size
	if elementSize != elements.Size() {
		return 0, 0, NewFatalError(fmt.Errorf("data slab %d elements size %d is wrong, want %d", id, elements.Size(), elementSize))
	}

	return elementCount, elementSize, nil
}

func (v *mapVerifier) verifySingleElements(
	id SlabID,
	elements *singleElements,
	digestLevel uint,
	hkeyPrefixes []Digest,
) (
	elementCount uint64,
	elementSize uint32,
	err error,
) {

	// Verify elements' level
	if digestLevel != elements.level {
		return 0, 0, NewFatalError(
			fmt.Errorf("data slab %d elements level %d is wrong, want %d",
				id, elements.level, digestLevel))
	}

	elementSize = singleElementsPrefixSize

	for _, e := range elements.elems {

		// Verify element
		computedSize, maxDigestLevel, err := v.verifySingleElement(e, hkeyPrefixes)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifySingleElement().
			return 0, 0, fmt.Errorf("data slab %d: %w", id, err)
		}

		// Verify element size is <= inline size
		if e.Size() > uint32(maxInlineMapElementSize) {
			return 0, 0, NewFatalError(
				fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
					id, e, e.Size(), maxInlineMapElementSize))
		}

		// Verify digest level
		if digestLevel != maxDigestLevel {
			return 0, 0, NewFatalError(
				fmt.Errorf("data slab %d single elements %s digest level %d is wrong, want %d",
					id, elements, digestLevel, maxDigestLevel))
		}

		elementSize += computedSize
	}

	// Verify elements size
	if elementSize != elements.Size() {
		return 0, 0, NewFatalError(fmt.Errorf("slab %d elements size %d is wrong, want %d", id, elements.Size(), elementSize))
	}

	return uint64(len(elements.elems)), elementSize, nil
}

func (v *mapVerifier) verifySingleElement(
	e *singleElement,
	digests []Digest,
) (
	size uint32,
	digestMaxLevel uint,
	err error,
) {
	// Verify key storable's size is less than size limit
	if e.key.ByteSize() > uint32(maxInlineMapKeySize) {
		return 0, 0, NewFatalError(
			fmt.Errorf(
				"map element key %s size %d exceeds size limit %d",
				e.key, e.key.ByteSize(), maxInlineMapKeySize,
			))
	}

	// Verify value storable's size is less than size limit
	valueSizeLimit := maxInlineMapValueSize(uint64(e.key.ByteSize()))
	if e.value.ByteSize() > uint32(valueSizeLimit) {
		return 0, 0, NewFatalError(
			fmt.Errorf(
				"map element value %s size %d exceeds size limit %d",
				e.value, e.value.ByteSize(), valueSizeLimit,
			))
	}

	// Verify key
	kv, err := e.key.StoredValue(v.storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Stroable interface.
		return 0, 0, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("element %s key can't be converted to value", e))
	}

	err = verifyValue(kv, v.address, nil, v.tic, v.hip, v.inlineEnabled)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifyValue().
		return 0, 0, fmt.Errorf("element %s key isn't valid: %w", e, err)
	}

	// Verify value
	vv, err := e.value.StoredValue(v.storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Stroable interface.
		return 0, 0, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("element %s value can't be converted to value", e))
	}

	err = verifyValue(vv, v.address, nil, v.tic, v.hip, v.inlineEnabled)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifyValue().
		return 0, 0, fmt.Errorf("element %s value isn't valid: %w", e, err)
	}

	// Verify not-inlined array/map > inline size, or can't be inlined
	if v.inlineEnabled {
		if _, ok := e.value.(SlabIDStorable); ok {
			err = verifyNotInlinedValueStatusAndSize(vv, uint32(valueSizeLimit))
			if err != nil {
				return 0, 0, err
			}
		}
	}

	// Verify size
	computedSize := singleElementPrefixSize + e.key.ByteSize() + e.value.ByteSize()
	if computedSize != e.Size() {
		return 0, 0, NewFatalError(fmt.Errorf("element %s size %d is wrong, want %d", e, e.Size(), computedSize))
	}

	// Verify digest
	digest, err := v.digesterBuilder.Digest(v.hip, kv)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return 0, 0, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create digester")
	}

	computedDigests, err := digest.DigestPrefix(digest.Levels())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digester interface.
		return 0, 0, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate digest prefix up to level %d", digest.Levels()))
	}

	if !reflect.DeepEqual(digests, computedDigests[:len(digests)]) {
		return 0, 0, NewFatalError(fmt.Errorf("element %s digest %v is wrong, want %v", e, digests, computedDigests))
	}

	return computedSize, digest.Levels(), nil
}

func verifyValue(value Value, address Address, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider, inlineEnabled bool) error {
	switch v := value.(type) {
	case *Array:
		return VerifyArray(v, address, typeInfo, tic, hip, inlineEnabled)
	case *OrderedMap:
		return VerifyMap(v, address, typeInfo, tic, hip, inlineEnabled)
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
		m.root.SlabID(),
		cborDecMode,
		cborEncMode,
		decodeStorable,
		decodeTypeInfo,
		compare,
	)
}

func validMapSlabSerialization(
	storage SlabStorage,
	id SlabID,
	cborDecMode cbor.DecMode,
	cborEncMode cbor.EncMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	compare StorableComparator,
) error {

	slab, err := getMapSlab(storage, id)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return err
	}

	// Encode slab
	data, err := Encode(slab, cborEncMode)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Encode().
		return err
	}

	// Decode encoded slab
	decodedSlab, err := DecodeSlab(id, data, cborDecMode, decodeStorable, decodeTypeInfo)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by DecodeSlab().
		return err
	}

	// Re-encode decoded slab
	dataFromDecodedSlab, err := Encode(decodedSlab, cborEncMode)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Encode().
		return err
	}

	// Extra check: encoded data size == header.size
	// This check is skipped for slabs with inlined composite because
	// encoded size and slab size differ for inlined composites.
	// For inlined composites, digests and field keys are encoded in
	// composite extra data section for reuse, and only composite field
	// values are encoded in non-extra data section.
	// This reduces encoding size because composite values of the same
	// composite type can reuse encoded type info, seed, digests, and field names.
	// TODO: maybe add size check for slabs with inlined composite by decoding entire slab.
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

	// Compare encoded data of original slab with encoded data of decoded slab
	if !bytes.Equal(data, dataFromDecodedSlab) {
		return NewFatalError(
			fmt.Errorf("slab %d encoded data is different from decoded slab's encoded data, got %v, want %v",
				id, dataFromDecodedSlab, data))
	}

	if slab.IsData() {
		dataSlab, ok := slab.(*MapDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("slab %d is not MapDataSlab", id))
		}

		decodedDataSlab, ok := decodedSlab.(*MapDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("decoded slab %d is not MapDataSlab", id))
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
			// Don't need to wrap error as external error because err is already categorized by mapDataSlabEqual().
			return fmt.Errorf("data slab %d round-trip serialization failed: %w", id, err)
		}

		return nil
	}

	metaSlab, ok := slab.(*MapMetaDataSlab)
	if !ok {
		return NewFatalError(fmt.Errorf("slab %d is not MapMetaDataSlab", id))
	}

	decodedMetaSlab, ok := decodedSlab.(*MapMetaDataSlab)
	if !ok {
		return NewFatalError(fmt.Errorf("decoded slab %d is not MapMetaDataSlab", id))
	}

	// Compare slabs
	err = mapMetaDataSlabEqual(metaSlab, decodedMetaSlab)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by mapMetaDataSlabEqual().
		return fmt.Errorf("metadata slab %d round-trip serialization failed: %w", id, err)
	}

	for _, h := range metaSlab.childrenHeaders {
		// Verify child slabs
		err = validMapSlabSerialization(
			storage,
			h.slabID,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			compare,
		)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by validMapSlabSerialization().
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
	if !reflect.DeepEqual(expected.header, actual.header) {
		return NewFatalError(fmt.Errorf("header %+v is wrong, want %+v", actual.header, expected.header))
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
		// Don't need to wrap error as external error because err is already categorized by mapElementsEqual().
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
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
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
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
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
		return NewFatalError(fmt.Errorf("hkeyElements level %d is wrong, want %d", actual.level, expected.level))
	}

	if expected.size != actual.size {
		return NewFatalError(fmt.Errorf("hkeyElements size %d is wrong, want %d", actual.size, expected.size))
	}

	if len(expected.hkeys) == 0 {
		if len(actual.hkeys) != 0 {
			return NewFatalError(fmt.Errorf("hkeyElements hkeys %v is wrong, want %v", actual.hkeys, expected.hkeys))
		}
	} else {
		if !reflect.DeepEqual(expected.hkeys, actual.hkeys) {
			return NewFatalError(fmt.Errorf("hkeyElements hkeys %v is wrong, want %v", actual.hkeys, expected.hkeys))
		}
	}

	if len(expected.elems) != len(actual.elems) {
		return NewFatalError(fmt.Errorf("hkeyElements elems len %d is wrong, want %d", len(actual.elems), len(expected.elems)))
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
			// Don't need to wrap error as external error because err is already categorized by mapElementEqual().
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
		return NewFatalError(fmt.Errorf("singleElements level %d is wrong, want %d", actual.level, expected.level))
	}

	if expected.size != actual.size {
		return NewFatalError(fmt.Errorf("singleElements size %d is wrong, want %d", actual.size, expected.size))
	}

	if len(expected.elems) != len(actual.elems) {
		return NewFatalError(fmt.Errorf("singleElements elems len %d is wrong, want %d", len(actual.elems), len(expected.elems)))
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
			// Don't need to wrap error as external error because err is already categorized by mapSingleElementEqual().
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
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
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
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
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
			return NewFatalError(fmt.Errorf("elements type %T is wrong, want %T", actual, expected))
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
		return NewFatalError(fmt.Errorf("externalCollisionGroup size %d is wrong, want %d", actual.size, expected.size))
	}

	if expected.slabID != actual.slabID {
		return NewFatalError(fmt.Errorf("externalCollisionGroup id %d is wrong, want %d", actual.slabID, expected.slabID))
	}

	// Compare external collision slab
	err := validMapSlabSerialization(
		storage,
		expected.slabID,
		cborDecMode,
		cborEncMode,
		decodeStorable,
		decodeTypeInfo,
		compare,
	)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by validMapSlabSerialization().
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
		return NewFatalError(fmt.Errorf("singleElement size %d is wrong, want %d", actual.size, expected.size))
	}

	if !compare(expected.key, actual.key) {
		return NewFatalError(fmt.Errorf("singleElement key %v is wrong, want %v", actual.key, expected.key))
	}

	// Compare key stored in a separate slab
	if idStorable, ok := expected.key.(SlabIDStorable); ok {

		v, err := idStorable.StoredValue(storage)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.StoredValue().
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
			// Don't need to wrap error as external error because err is already categorized by ValidValueSerialization().
			return err
		}
	}

	// Compare nested element
	switch ee := expected.value.(type) {
	case SlabIDStorable:
		if !compare(expected.value, actual.value) {
			return NewFatalError(fmt.Errorf("singleElement value %v is wrong, want %v", actual.value, expected.value))
		}

		v, err := ee.StoredValue(storage)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.StoredValue().
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
			// Don't need to wrap error as external error because err is already categorized by ValidValueSerialization().
			return err
		}

	case *ArrayDataSlab:
		ae, ok := actual.value.(*ArrayDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("expect element as *ArrayDataSlab, actual %T", ae))
		}

		return arrayDataSlabEqual(ee, ae, storage, cborDecMode, cborEncMode, decodeStorable, decodeTypeInfo, compare)

	case *MapDataSlab:
		ae, ok := actual.value.(*MapDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("expect element as *MapDataSlab, actual %T", ae))
		}

		return mapDataSlabEqual(ee, ae, storage, cborDecMode, cborEncMode, decodeStorable, decodeTypeInfo, compare)

	default:
		if !compare(expected.value, actual.value) {
			return NewFatalError(fmt.Errorf("singleElement value %v is wrong, want %v", actual.value, expected.value))
		}
	}

	return nil
}

func mapMetaDataSlabEqual(expected, actual *MapMetaDataSlab) error {

	// Compare extra data
	err := mapExtraDataEqual(expected.extraData, actual.extraData)
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

func mapExtraDataEqual(expected, actual *MapExtraData) error {

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

// verifyMapValueID verifies map ValueID is always the same as
// root slab's SlabID indepedent of map's inlined status.
func verifyMapValueID(m *OrderedMap) error {
	rootSlabID := m.root.Header().slabID

	vid := m.ValueID()

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

// verifyMapSlabID verifies map SlabID is either empty for inlined map, or
// same as root slab's SlabID for not-inlined map.
func verifyMapSlabID(m *OrderedMap) error {
	sid := m.SlabID()

	if m.Inlined() {
		if sid != SlabIDUndefined {
			return NewFatalError(
				fmt.Errorf(
					"expect empty slab ID for inlined array, got %v",
					sid))
		}
		return nil
	}

	rootSlabID := m.root.Header().slabID

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
