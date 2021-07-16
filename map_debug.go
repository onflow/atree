/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"container/list"
	"fmt"
	"sort"
	"strings"
)

// Stats returns stats about the map slabs.
func (m *OrderedMap) Stats() (Stats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	metaDataSlabSize := uint64(0)
	dataSlabCount := uint64(0)
	dataSlabSize := uint64(0)

	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(m.root.Header().id)

	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getMapSlab(m.storage, id)
			if err != nil {
				return Stats{}, err
			}

			if slab.IsData() {
				leaf := slab.(*MapDataSlab)
				dataSlabCount++
				dataSlabSize += uint64(leaf.header.size)
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

	return Stats{
		Levels:            level,
		MetaDataSlabCount: metaDataSlabCount,
		DataSlabCount:     dataSlabCount,
	}, nil
}

func (m *OrderedMap) Print() {
	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(m.root.Header().id)

	overflowIDs := list.New()

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
				fmt.Printf("level %d, leaf (%+v): ", level+1, dataSlab.header)

				var kvStr []string
				if len(dataSlab.hkeys) <= 6 {
					for i, hkey := range dataSlab.hkeys {
						key := dataSlab.keys[i]
						value := dataSlab.values[i]
						kvStr = append(kvStr, fmt.Sprintf("%d:%v:%v", hkey, key, value))
					}
				} else {
					for i := 0; i < 3; i++ {
						hkey := dataSlab.hkeys[i]
						key := dataSlab.keys[i]
						value := dataSlab.values[i]
						kvStr = append(kvStr, fmt.Sprintf("%d:%v:%v", hkey, key, value))
					}
					kvStr = append(kvStr, "...")
					for i := len(dataSlab.hkeys) - 3; i < len(dataSlab.hkeys); i++ {
						hkey := dataSlab.hkeys[i]
						key := dataSlab.keys[i]
						value := dataSlab.values[i]
						kvStr = append(kvStr, fmt.Sprintf("%d:%v:%v", hkey, key, value))
					}
				}

				fmt.Printf("{%s}\n", strings.Join(kvStr, " "))

				for _, v := range dataSlab.values {
					if id, ok := v.(StorageIDValue); ok {
						overflowIDs.PushBack(StorageID(id))
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

	if overflowIDs.Len() > 0 {
		for e := overflowIDs.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			// TODO: expand this to include other types
			slab, err := getMapSlab(m.storage, id)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("overflow: (id %d) %s\n", id, slab.String())
		}
	}
}

func (m *OrderedMap) valid() (bool, error) {
	return m._valid(m.root.Header().id, 0)
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

		computedSize := uint32(mapDataSlabPrefixSize)
		for i, k := range dataSlab.keys {
			computedSize += k.ByteSize() + dataSlab.values[i].ByteSize()
		}

		_, underflow := dataSlab.IsUnderflow()
		validFill := (level == 0) || (!dataSlab.IsFull() && !underflow)

		validFirstKey := dataSlab.hkeys[0] == dataSlab.header.firstKey

		sortedHKey := sort.SliceIsSorted(dataSlab.hkeys, func(i, j int) bool { return dataSlab.hkeys[i] < dataSlab.hkeys[j] })

		validSize := computedSize == dataSlab.header.size

		if !validFill {
			fmt.Printf("data slab %d !validFill\n", id)
		}
		if !validFirstKey {
			fmt.Printf("data slab %d !validFirstKey\n", id)
		}
		if !sortedHKey {
			fmt.Printf("data slab %d !sortedHKey\n", id)
		}
		if !validSize {
			fmt.Printf("data slab %d !validSize, got %d, computed %d\n", id, dataSlab.header.size, computedSize)
		}

		return validFill && validFirstKey && sortedHKey && validSize, nil
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

	if !validFill {
		fmt.Printf("meta slab %d !validFill\n", id)
	}
	if !validFirstKey {
		fmt.Printf("meta slab %d !validFirstKey\n", id)
	}
	if !sortedHKey {
		fmt.Printf("meta slab %d !sortedHKey\n", id)
	}
	if !validSize {
		fmt.Printf("meta slab %d !validSize\n", id)
	}
	return validFill && validFirstKey && sortedHKey && validSize, nil
}
