/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"container/list"
	"fmt"
	"sort"
	"strings"
)

type mapStats struct {
	Levels                 uint64
	ElementCount           uint64
	MetaDataSlabCount      uint64
	DataSlabCount          uint64
	CollisionDataSlabCount uint64
}

// Stats returns stats about the map slabs.
func (m *OrderedMap) Stats() (mapStats, error) {
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
				return mapStats{}, err
			}

			if slab.IsData() {
				leaf := slab.(*MapDataSlab)
				dataSlabCount++
				dataSlabSize += uint64(leaf.header.size)

				for _, elem := range leaf.elements.elems {
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

	return mapStats{
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
				fmt.Printf("level %d, leaf (%+v): (prefix %v)", level+1, dataSlab.header, dataSlab.hkeyPrefixes)

				var kvStr []string
				if len(dataSlab.elems) <= 6 {
					for i := 0; i < len(dataSlab.elements.elems); i++ {
						elem := dataSlab.elements.elems[i]
						if len(dataSlab.elements.hkeys) == 0 {
							kvStr = append(kvStr, elem.String())
						} else {
							kvStr = append(kvStr,
								fmt.Sprintf("%d:%s", dataSlab.elements.hkeys[i], elem.String()))
						}
					}
				} else {
					for i := 0; i < 3; i++ {
						elem := dataSlab.elements.elems[i]
						if len(dataSlab.elements.hkeys) == 0 {
							kvStr = append(kvStr, elem.String())
						} else {
							kvStr = append(kvStr,
								fmt.Sprintf("%d:%s", dataSlab.elements.hkeys[i], elem.String()))
						}
					}
					kvStr = append(kvStr, "...")
					for i := len(dataSlab.hkeys) - 3; i < len(dataSlab.hkeys); i++ {
						elem := dataSlab.elements.elems[i]
						if len(dataSlab.elements.hkeys) == 0 {
							kvStr = append(kvStr, elem.String())
						} else {
							kvStr = append(kvStr,
								fmt.Sprintf("%d:%s", dataSlab.elements.hkeys[i], elem.String()))
						}
					}
				}

				fmt.Printf("{%s}\n", strings.Join(kvStr, " "))

				for _, e := range dataSlab.elements.elems {
					if group, ok := e.(elementGroup); ok {
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

func (m *OrderedMap) _validElements(id StorageID, h Hasher, elements elements, level int) (uint32, error) {
	// For digest size 16:
	//   level 0: hkey prefix len == 0, hkey length == element length
	//   level 1: hkey prefix len == 1, hkey length == element length
	//   level 2: hkey prefix len == 2, hkey length == 0

	hkeyLen := h.DigestSize() / 8

	if level != len(elements.hkeyPrefixes) {
		return 0, fmt.Errorf("slab %d, element level %d, hkeyPrefixes is wrong length %d, expect %d",
			id, level, len(elements.hkeyPrefixes), level)
	}

	if level < hkeyLen && len(elements.hkeys) != len(elements.elems) {
		return 0, fmt.Errorf("slab %d, element level %d, hkeys is wrong length %d, expect %d",
			id, level, len(elements.hkeys), len(elements.elems))
	}

	if level == hkeyLen && len(elements.hkeys) != 0 {
		return 0, fmt.Errorf("slab %d, element level %d, hkeys is wrong length %d, expect %d",
			id, level, len(elements.hkeys), 0)
	}

	if len(elements.hkeys) > 0 {
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
	}

	size := uint32(0)
	for i := 0; i < len(elements.elems); i++ {
		e := elements.elems[i]

		if group, ok := e.(elementGroup); ok {
			ge, err := group.Elements(m.storage)
			if err != nil {
				return 0, err
			}

			_, err = m._validElements(id, h, ge, level+1)
			if err != nil {
				return 0, err
			}

			size += group.Size()

		} else {

			k, err := e.MapKey()
			if err != nil {
				return 0, err
			}

			v, err := e.MapValue()
			if err != nil {
				return 0, err
			}

			// Verify single element size
			computedSize := k.ByteSize() + v.ByteSize()
			if computedSize != e.Size() {
				return 0, fmt.Errorf("slab %d, element level %d, element %s, size %d, computed size %d",
					id, level, elements.String(), e.Size(), computedSize)
			}

			// Verify single element hashed value
			computedHkey, err := h.Hash(k)
			if err != nil {
				return 0, err
			}

			// Combine element's hkeyPrefixes with hkey
			hkeys := make([]uint64, len(elements.hkeyPrefixes))
			copy(hkeys, elements.hkeyPrefixes)

			if len(elements.hkeys) > 0 {
				hkeys = append(hkeys, elements.hkeys[i])
			}

			if len(computedHkey) < len(hkeys) {
				return 0, fmt.Errorf("slab %d, element level %d, element %s, hkey %v, computed hkeys %d",
					id, level, elements.String(), hkeys, computedHkey)
			}

			for j := 0; j < len(hkeys); j++ {
				if hkeys[j] != computedHkey[j] {
					return 0, fmt.Errorf("slab %d, element level %d, element %s, hkey %v, computed hkeys %d",
						id, level, elements.String(), hkeys, computedHkey)
				}
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

		elementSize, err := m._validElements(id, m.hasher, dataSlab.elements, 0)
		if err != nil {
			return false, err
		}

		computedSize := uint32(mapDataSlabPrefixSize) + elementSize

		_, underflow := dataSlab.IsUnderflow()
		validFill := (level == 0) || (!dataSlab.IsFull() && !underflow)

		validFirstKey := dataSlab.hkeys[0] == dataSlab.header.firstKey

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

	computedSize := uint32(len(meta.childrenHeaders)*mapSlabHeaderSize) + mapMetaDataSlabPrefixSize
	validSize := computedSize == meta.header.size

	validFirstKey := meta.childrenHeaders[0].firstKey == meta.header.firstKey

	sortedHKey := sort.SliceIsSorted(meta.childrenHeaders, func(i, j int) bool {
		return meta.childrenHeaders[i].firstKey < meta.childrenHeaders[j].firstKey
	})

	return validFill && validFirstKey && sortedHKey && validSize, nil
}
