/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"container/list"
	"fmt"
	"reflect"
	"sort"
)

type MapStats struct {
	Levels                 uint64
	ElementCount           uint64
	MetaDataSlabCount      uint64
	DataSlabCount          uint64
	CollisionDataSlabCount uint64
	StorableSlabCount      uint64
}

// GetMapStats returns stats about the map slabs.
func GetMapStats(m *OrderedMap) (MapStats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	metaDataSlabSize := uint64(0)
	dataSlabCount := uint64(0)
	dataSlabSize := uint64(0)
	collisionDataSlabCount := uint64(0)
	storableDataSlabCount := uint64(0)

	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(m.root.Header().id)

	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				return MapStats{}, err
			}

			if slab.IsData() {
				leaf := slab.(*MapDataSlab)
				dataSlabCount++
				dataSlabSize += uint64(leaf.header.size)

				for i := 0; i < int(leaf.elements.Count()); i++ {
					elem, err := leaf.elements.Element(i)
					if err != nil {
						return MapStats{}, err
					}
					if group, ok := elem.(elementGroup); ok {
						if !group.Inline() {
							collisionDataSlabCount++
						}
					} else {
						e := elem.(*singleElement)
						if _, ok := e.key.(*StorageIDStorable); ok {
							storableDataSlabCount++
						}
						if _, ok := e.value.(*StorageIDStorable); ok {
							storableDataSlabCount++
						}
					}
				}
			} else {
				meta := slab.(*MapMetaDataSlab)
				metaDataSlabCount++
				metaDataSlabSize += uint64(meta.header.size)

				for _, h := range meta.childrenHeaders {
					nextLevelIDs.PushBack(h.id)
				}
			}
		}

		level++
	}

	return MapStats{
		Levels:                 level,
		MetaDataSlabCount:      metaDataSlabCount,
		DataSlabCount:          dataSlabCount,
		CollisionDataSlabCount: collisionDataSlabCount,
		StorableSlabCount:      storableDataSlabCount,
	}, nil
}

func PrintMap(m *OrderedMap) {
	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(m.root.Header().id)

	collisionSlabIDs := list.New()

	level := 0
	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				fmt.Println(err)
				return
			}

			if slab.IsData() {
				dataSlab := slab.(*MapDataSlab)
				fmt.Printf("level %d, leaf (%+v): %s\n", level+1, dataSlab.header, dataSlab.String())

				for i := 0; i < int(dataSlab.elements.Count()); i++ {
					elem, err := dataSlab.elements.Element(i)
					if err != nil {
						fmt.Println(err)
						return
					}
					if group, ok := elem.(elementGroup); ok {
						if !group.Inline() {
							extSlab := group.(*externalCollisionGroup)
							collisionSlabIDs.PushBack(extSlab.id)
						}
					}
				}

			} else {
				meta := slab.(*MapMetaDataSlab)
				fmt.Printf("level %d, meta (%+v) headers: [", level+1, meta.header)
				for _, h := range meta.childrenHeaders {
					fmt.Printf("%+v ", h)
					nextLevelIDs.PushBack(h.id)
				}
				fmt.Println("]")
			}
		}

		level++
	}

	if collisionSlabIDs.Len() > 0 {
		for e := collisionSlabIDs.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("collision slab: (id %d) %s\n", id, slab.String())
		}
	}
}

func ValidMap(m *OrderedMap, typeInfo TypeInfo, hip HashInputProvider) error {

	extraData := m.root.ExtraData()
	if extraData == nil {
		return fmt.Errorf("root slab %d doesn't have extra data", m.root.ID())
	}

	// Verify that extra data has correct type information
	if typeInfo != nil && !extraData.TypeInfo.Equal(typeInfo) {
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
		m.Storage, m.digesterBuilder, hip, m.root.ID(), 0, nil, []StorageID{}, []StorageID{}, []Digest{})
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
		elementCount, elementSize, err := validMapElements(storage, digesterBuilder, hip, id, dataSlab.elements, 0, nil)
		if err != nil {
			return 0, nil, nil, nil, err
		}

		// Verify slab's first key
		if dataSlab.elements.firstKey() != dataSlab.header.firstKey {
			return 0, nil, nil, nil, fmt.Errorf("data slab %d header first key %d is wrong, want %d",
				id, dataSlab.header.firstKey, dataSlab.elements.firstKey())
		}

		// Verify that aggregated element size + slab prefix is the same as header.size
		computedSize := uint32(mapDataSlabPrefixSize) + elementSize
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
		count, dataSlabIDs, nextDataSlabIDs, firstKeys, err = validMapSlab(storage, digesterBuilder, hip, h.id, level+1, &h, dataSlabIDs, nextDataSlabIDs, firstKeys)
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
		return validMapHkeyElements(storage, db, hip, id, elems, digestLevel, hkeyPrefixes)
	case *singleElements:
		return validMapSingleElements(storage, db, hip, id, elems, digestLevel, hkeyPrefixes)
	default:
		return 0, 0, fmt.Errorf("slab %d has unknown elements type %T at digest level %d", id, elements, digestLevel)
	}
}

func validMapHkeyElements(
	storage SlabStorage,
	db DigesterBuilder,
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
			if e.Size() > uint32(MaxInlineElementSize) {
				return 0, 0, fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
					id, e, e.Size(), MaxInlineElementSize)
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

			count, size, err := validMapElements(storage, db, hip, id, ge, digestLevel+1, hkeys)
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
			computedSize, maxDigestLevel, err := validSingleElement(storage, db, hip, se, hkeys)
			if err != nil {
				return 0, 0, fmt.Errorf("data slab %d %s", id, err)
			}

			// Verify digest level
			if digestLevel >= maxDigestLevel {
				return 0, 0, fmt.Errorf("data slab %d hkey elements %s digest level %d is wrong, want < %d",
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
		computedSize, maxDigestLevel, err := validSingleElement(storage, db, hip, e, hkeyPrefixes)
		if err != nil {
			return 0, 0, fmt.Errorf("data slab %d %s", id, err)
		}

		// Verify element size is <= inline size
		if e.Size() > uint32(MaxInlineElementSize) {
			return 0, 0, fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
				id, e, e.Size(), MaxInlineElementSize)
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
		return 0, 0, fmt.Errorf("element %s key can't be converted to value, %s", e, err)
	}

	err = ValidValue(kv, nil, hip)
	if err != nil {
		return 0, 0, fmt.Errorf("element %s key isn't valid, %s", e, err)
	}

	// Verify value pointer
	if _, valuePointer := e.value.(StorageIDStorable); e.valuePointer != valuePointer {
		return 0, 0, fmt.Errorf("element %s valuePointer %t is wrong, want %t", e, e.valuePointer, valuePointer)
	}

	// Verify value
	vv, err := e.value.StoredValue(storage)
	if err != nil {
		return 0, 0, fmt.Errorf("element %s value can't be converted to value, %s", e, err)
	}

	err = ValidValue(vv, nil, hip)
	if err != nil {
		return 0, 0, fmt.Errorf("element %s value isn't valid, %s", e, err)
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

func ValidValue(value Value, typeInfo TypeInfo, hip HashInputProvider) error {
	switch v := value.(type) {
	case *Array:
		return ValidArray(v, typeInfo, hip)
	case *OrderedMap:
		return ValidMap(v, typeInfo, hip)
	}
	return nil
}
