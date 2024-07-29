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

			switch slab := slab.(type) {
			case *MapDataSlab:
				dataSlabCount++

				elementGroups := []elements{slab.elements}

				for len(elementGroups) > 0 {

					var nestedElementGroups []elements

					for _, group := range elementGroups {
						for i := 0; i < int(group.Count()); i++ {
							elem, err := group.Element(i)
							if err != nil {
								// Don't need to wrap error as external error because err is already categorized by elements.Element().
								return MapStats{}, err
							}

							switch e := elem.(type) {
							case elementGroup:
								nestedGroup := e

								if !nestedGroup.Inline() {
									collisionDataSlabCount++
								}

								nested, err := nestedGroup.Elements(m.Storage)
								if err != nil {
									// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
									return MapStats{}, err
								}

								nestedElementGroups = append(nestedElementGroups, nested)

							case *singleElement:
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

			case *MapMetaDataSlab:
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

			switch slab := slab.(type) {
			case *MapDataSlab:
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, slab))

				for i := 0; i < int(slab.elements.Count()); i++ {
					elem, err := slab.elements.Element(i)
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

				overflowIDs = getSlabIDFromStorable(slab, overflowIDs)

			case *MapMetaDataSlab:
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
	return verifyMap(m, address, typeInfo, tic, hip, inlineEnabled, map[SlabID]struct{}{})
}

func verifyMap(m *OrderedMap, address Address, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider, inlineEnabled bool, slabIDs map[SlabID]struct{}) error {

	// Verify map address (independent of array inlined status)
	if address != m.Address() {
		return NewFatalError(fmt.Errorf("map address %v, got %v", address, m.Address()))
	}

	// Verify map value ID (independent of array inlined status)
	err := verifyMapValueID(m)
	if err != nil {
		return err
	}

	// Verify map slab ID (dependent of array inlined status)
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
		m.root, 0, nil, []SlabID{}, []SlabID{}, []Digest{}, slabIDs)
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
	slabIDs map[SlabID]struct{},
) (
	elementCount uint64,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	_firstKeys []Digest,
	err error,
) {

	id := slab.Header().slabID

	// Verify SlabID is unique
	if _, exist := slabIDs[id]; exist {
		return 0, nil, nil, nil, NewFatalError(fmt.Errorf("found duplicate slab ID %s", id))
	}

	slabIDs[id] = struct{}{}

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
		return v.verifyDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, firstKeys, slabIDs)

	case *MapMetaDataSlab:
		return v.verifyMetaDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, firstKeys, slabIDs)

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
	slabIDs map[SlabID]struct{},
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
	elementCount, elementSize, err := v.verifyElements(id, dataSlab.elements, 0, nil, slabIDs)
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
	if dataSlab.Inlined() {
		if level > 0 {
			return 0, nil, nil, nil, NewFatalError(fmt.Errorf("non-root slab %s is inlined", id))
		}
		if dataSlab.extraData == nil {
			return 0, nil, nil, nil, NewFatalError(fmt.Errorf("inlined slab %s doesn't have extra data", id))
		}
		if dataSlab.next != SlabIDUndefined {
			return 0, nil, nil, nil, NewFatalError(fmt.Errorf("inlined slab %s has next slab ID", id))
		}
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
	slabIDs map[SlabID]struct{},
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
			v.verifySlab(childSlab, level+1, &h, dataSlabIDs, nextDataSlabIDs, firstKeys, slabIDs)
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
	slabIDs map[SlabID]struct{},
) (
	elementCount uint64,
	elementSize uint32,
	err error,
) {

	switch elems := elements.(type) {
	case *hkeyElements:
		return v.verifyHkeyElements(id, elems, digestLevel, hkeyPrefixes, slabIDs)
	case *singleElements:
		return v.verifySingleElements(id, elems, digestLevel, hkeyPrefixes, slabIDs)
	default:
		return 0, 0, NewFatalError(fmt.Errorf("slab %d has unknown elements type %T at digest level %d", id, elements, digestLevel))
	}
}

func (v *mapVerifier) verifyHkeyElements(
	id SlabID,
	elements *hkeyElements,
	digestLevel uint,
	hkeyPrefixes []Digest,
	slabIDs map[SlabID]struct{},
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

		hkeys := make([]Digest, len(hkeyPrefixes)+1)
		copy(hkeys, hkeyPrefixes)
		hkeys[len(hkeys)-1] = elements.hkeys[i]

		elementSize += digestSize

		// Verify element size is <= inline size
		if digestLevel == 0 {
			if e.Size() > uint32(maxInlineMapElementSize) {
				return 0, 0, NewFatalError(
					fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
						id, e, e.Size(), maxInlineMapElementSize))
			}
		}

		switch e := e.(type) {
		case elementGroup:
			group, err := e.Elements(v.storage)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
				return 0, 0, err
			}

			count, size, err := v.verifyElements(id, group, digestLevel+1, hkeys, slabIDs)
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

		case *singleElement:
			// Verify element
			computedSize, maxDigestLevel, err := v.verifySingleElement(e, hkeys, slabIDs)
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

		default:
			return 0, 0, NewFatalError(fmt.Errorf("data slab %d element type %T is wrong, want either elementGroup or *singleElement", id, e))
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
	slabIDs map[SlabID]struct{},
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
		computedSize, maxDigestLevel, err := v.verifySingleElement(e, hkeyPrefixes, slabIDs)
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
	slabIDs map[SlabID]struct{},
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

	err = verifyValue(kv, v.address, nil, v.tic, v.hip, v.inlineEnabled, slabIDs)
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

	switch e := e.value.(type) {
	case SlabIDStorable:
		// Verify not-inlined value > inline size, or can't be inlined
		if v.inlineEnabled {
			err = verifyNotInlinedValueStatusAndSize(vv, uint32(valueSizeLimit))
			if err != nil {
				return 0, 0, err
			}
		}

	case *ArrayDataSlab:
		// Verify inlined element's inlined status
		if !e.Inlined() {
			return 0, 0, NewFatalError(fmt.Errorf("inlined array inlined status is false"))
		}

	case *MapDataSlab:
		// Verify inlined element's inlined status
		if !e.Inlined() {
			return 0, 0, NewFatalError(fmt.Errorf("inlined map inlined status is false"))
		}
	}

	err = verifyValue(vv, v.address, nil, v.tic, v.hip, v.inlineEnabled, slabIDs)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifyValue().
		return 0, 0, fmt.Errorf("element %s value isn't valid: %w", e, err)
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

func verifyValue(value Value, address Address, typeInfo TypeInfo, tic TypeInfoComparator, hip HashInputProvider, inlineEnabled bool, slabIDs map[SlabID]struct{}) error {
	switch v := value.(type) {
	case *Array:
		return verifyArray(v, address, typeInfo, tic, hip, inlineEnabled, slabIDs)
	case *OrderedMap:
		return verifyMap(v, address, typeInfo, tic, hip, inlineEnabled, slabIDs)
	}
	return nil
}

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
		for i := 0; i < len(expected.elems); i++ {
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

	for i := 0; i < len(expected.elems); i++ {
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

	// Compare nested element
	switch ee := expected.value.(type) {
	case SlabIDStorable: // Compare not-inlined element
		if !v.compare(expected.value, actual.value) {
			return NewFatalError(fmt.Errorf("singleElement value %v is wrong, want %v", actual.value, expected.value))
		}

		value, err := ee.StoredValue(v.storage)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.StoredValue().
			return err
		}

		err = v.verifyValue(value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifyVaue().
			return err
		}

	case *ArrayDataSlab: // Compare inlined array element
		ae, ok := actual.value.(*ArrayDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("expect element as *ArrayDataSlab, actual %T", ae))
		}

		return v.arrayDataSlabEqual(ee, ae)

	case *MapDataSlab: // Compare inlined map element
		ae, ok := actual.value.(*MapDataSlab)
		if !ok {
			return NewFatalError(fmt.Errorf("expect element as *MapDataSlab, actual %T", ae))
		}

		return v.mapDataSlabEqual(ee, ae)

	default:
		if !v.compare(expected.value, actual.value) {
			return NewFatalError(fmt.Errorf("singleElement value %v is wrong, want %v", actual.value, expected.value))
		}
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
