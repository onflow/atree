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
}

// Stats returns stats about the map slabs.
func (m *OrderedMap) Stats() (MapStats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	metaDataSlabSize := uint64(0)
	dataSlabCount := uint64(0)
	dataSlabSize := uint64(0)
	collisionDataSlabCount := uint64(0)

	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(m.root.Header().id)

	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getMapSlab(m.storage, id)
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
	}, nil
}

func (m *OrderedMap) Print() {
	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(m.root.Header().id)

	collisionSlabIDs := list.New()

	level := 0
	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getMapSlab(m.storage, id)
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

			slab, err := getMapSlab(m.storage, id)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("collision slab: (id %d) %s\n", id, slab.String())
		}
	}
}

func (m *OrderedMap) valid() (bool, error) {
	return m._valid(m.root.Header().id, 0)
}

func (m *OrderedMap) _validElements(id StorageID, db DigesterBuilder, elements elements, level int, hkeyPrefixes []Digest) (uint32, error) {

	if elems, ok := elements.(*hkeyElements); ok {
		return m._validHkeyElements(id, db, elems, level, hkeyPrefixes)
	}

	if elems, ok := elements.(*singleElements); ok {
		return m._validSingleElements(id, db, elems, level, hkeyPrefixes)
	}

	return 0, fmt.Errorf("slab %d, element level %d, elements is wrong type %T, expect *hkeyElements, or *singleElements",
		id, level, elements)
}

func (m *OrderedMap) _validHkeyElements(id StorageID, db DigesterBuilder, elements *hkeyElements, level int, hkeyPrefixes []Digest) (uint32, error) {

	if level != elements.level {
		return 0, fmt.Errorf("slab %d, level %d, expect %d",
			id, elements.level, level)
	}

	if len(elements.hkeys) != len(elements.elems) {
		return 0, fmt.Errorf("slab %d, element level %d, hkeys is wrong length %d, expect %d",
			id, level, len(elements.hkeys), len(elements.elems))
	}

	if !sort.SliceIsSorted(elements.hkeys, func(i, j int) bool {
		return elements.hkeys[i] < elements.hkeys[j]
	}) {
		return 0, fmt.Errorf("slab %d, element level %d, hkeys is not sorted %v",
			id, level, elements.hkeys)
	}

	size := uint32(0)
	for i := 0; i < len(elements.elems); i++ {
		e := elements.elems[i]

		if group, ok := e.(elementGroup); ok {
			ge, err := group.Elements(m.storage)
			if err != nil {
				return 0, err
			}

			var prefixes []Digest
			prefixes = append(prefixes, hkeyPrefixes...)
			prefixes = append(prefixes, elements.hkeys[i])

			_, err = m._validElements(id, db, ge, level+1, prefixes)
			if err != nil {
				return 0, err
			}

			size += group.Size()

		} else {

			se, ok := e.(*singleElement)
			if !ok {
				return 0, fmt.Errorf("got element %T, expect *singleElement", e)
			}

			ks, err := se.key.StoredValue(m.storage)
			if err != nil {
				return 0, err
			}

			ck, ok := ks.(ComparableValue)
			if !ok {
				return 0, fmt.Errorf("key %s doesn't implement ComparableValue", ks)
			}

			// Verify single element size
			computedSize := se.key.ByteSize() + se.value.ByteSize()
			if computedSize != e.Size() {
				return 0, fmt.Errorf("slab %d, element level %d, element %s, size %d, computed size %d",
					id, level, elements.String(), e.Size(), computedSize)
			}

			// Verify single element hashed value
			d, err := db.Digest(ck)
			if err != nil {
				return 0, err
			}
			computedHkey, err := d.DigestPrefix(d.Levels())
			if err != nil {
				return 0, err
			}

			var hkeys []Digest
			hkeys = append(hkeys, hkeyPrefixes...)
			hkeys = append(hkeys, elements.hkeys[i])

			if !reflect.DeepEqual(hkeys, computedHkey[:len(hkeys)]) {
				return 0, fmt.Errorf("slab %d, element level %d, element %s, hkey %v, computed hkeys %d",
					id, level, elements.String(), hkeys, computedHkey)
			}

			size += computedSize
		}
	}

	if size != elements.size {
		return 0, fmt.Errorf("slab %d, element level %d, elements size %d, computed size %d",
			id, level, elements.size, size)
	}

	return size, nil
}

func (m *OrderedMap) _validSingleElements(id StorageID, db DigesterBuilder, elements *singleElements, level int, hkeyPrefixes []Digest) (uint32, error) {

	if level != elements.level {
		return 0, fmt.Errorf("slab %d, level %d, expect %d",
			id, elements.level, level)
	}

	size := uint32(0)

	for _, e := range elements.elems {

		ks, err := e.key.StoredValue(m.storage)
		if err != nil {
			return 0, err
		}

		ck, ok := ks.(ComparableValue)
		if !ok {
			return 0, fmt.Errorf("key %s doesn't implement ComparableValue", ks)
		}

		// Verify single element size
		computedSize := e.key.ByteSize() + e.value.ByteSize()
		if computedSize != e.Size() {
			return 0, fmt.Errorf("slab %d, element level %d, element %s, size %d, computed size %d",
				id, level, elements.String(), e.Size(), computedSize)
		}

		// Verify single element hashed value
		digest, err := db.Digest(ck)
		if err != nil {
			return 0, err
		}
		computedHkey, err := digest.DigestPrefix(digest.Levels())
		if err != nil {
			return 0, err
		}

		if !reflect.DeepEqual(hkeyPrefixes, computedHkey) {
			return 0, fmt.Errorf("slab %d, element level %d, element %s, hkey %v, computed hkeys %d",
				id, level, elements.String(), hkeyPrefixes, computedHkey)
		}

		size += computedSize
	}

	if size != elements.size {
		return 0, fmt.Errorf("slab %d, element level %d, elements size %d, computed size %d",
			id, level, elements.size, size)
	}

	return size, nil
}

func (m *OrderedMap) _valid(id StorageID, level int) (bool, error) {
	slab, err := getMapSlab(m.storage, id)
	if err != nil {
		return false, err
	}

	if slab.IsData() {
		dataSlab, ok := slab.(*MapDataSlab)
		if !ok {
			return false, fmt.Errorf("slab %d is not MapDataSlab", id)
		}

		elementSize, err := m._validElements(id, m.digesterBuilder, dataSlab.elements, 0, nil)
		if err != nil {
			return false, err
		}

		_, underflow := dataSlab.IsUnderflow()
		validFill := (level == 0) || (!dataSlab.IsFull() && !underflow)

		validFirstKey := dataSlab.elements.firstKey() == dataSlab.header.firstKey

		computedSize := uint32(mapDataSlabPrefixSize) + elementSize
		validSize := computedSize == dataSlab.header.size

		return validFill && validFirstKey && validSize, nil
	}

	meta, ok := slab.(*MapMetaDataSlab)
	if !ok {
		return false, fmt.Errorf("slab %d is not MapMetaDataSlab", id)
	}

	for _, h := range meta.childrenHeaders {
		verified, err := m._valid(h.id, level+1)
		if !verified || err != nil {
			return false, err
		}
	}

	_, underflow := meta.IsUnderflow()
	validFill := (level == 0) || (!meta.IsFull() && !underflow)

	validFirstKey := meta.childrenHeaders[0].firstKey == meta.header.firstKey

	sortedHKey := sort.SliceIsSorted(meta.childrenHeaders, func(i, j int) bool {
		return meta.childrenHeaders[i].firstKey < meta.childrenHeaders[j].firstKey
	})

	computedSize := uint32(len(meta.childrenHeaders)*mapSlabHeaderSize) + mapMetaDataSlabPrefixSize
	validSize := computedSize == meta.header.size

	return validFill && validFirstKey && sortedHKey && validSize, nil
}