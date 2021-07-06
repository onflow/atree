/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"container/list"
	"fmt"
	"strings"
)

type Stats struct {
	Levels            uint64
	ElementCount      uint64
	MetaDataSlabCount uint64
	DataSlabCount     uint64
}

// Stats returns stats about the array slabs.
func (array *Array) Stats() (Stats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	metaDataSlabSize := uint64(0)
	dataSlabCount := uint64(0)
	dataSlabSize := uint64(0)

	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(array.root.Header().id)

	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getArraySlab(array.storage, id)
			if err != nil {
				return Stats{}, err
			}

			if slab.IsData() {
				leaf := slab.(*ArrayDataSlab)
				dataSlabCount++
				dataSlabSize += uint64(leaf.header.size)
			} else {
				meta := slab.(*ArrayMetaDataSlab)
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
		ElementCount:      array.Count(),
		MetaDataSlabCount: metaDataSlabCount,
		DataSlabCount:     dataSlabCount,
	}, nil
}

func (array *Array) Print() {
	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(array.root.Header().id)

	overflowIDs := list.New()

	level := 0
	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getArraySlab(array.storage, id)
			if err != nil {
				fmt.Println(err)
				return
			}

			if slab.IsData() {
				dataSlab := slab.(*ArrayDataSlab)
				fmt.Printf("level %d, leaf (%+v): ", level+1, dataSlab.header)

				var elements []Storable
				if len(dataSlab.elements) <= 6 {
					elements = dataSlab.elements
				} else {
					elements = append(elements, dataSlab.elements[:3]...)
					elements = append(elements, dataSlab.elements[len(dataSlab.elements)-3:]...)
				}

				var elemsStr []string
				for _, e := range elements {
					if id, ok := e.(StorageIDValue); ok {
						overflowIDs.PushBack(StorageID(id))
					}
					elemsStr = append(elemsStr, e.String())
				}

				if len(dataSlab.elements) > 6 {
					elemsStr = append(elemsStr, "")
					copy(elemsStr[4:], elemsStr[3:])
					elemsStr[3] = "..."
				}
				fmt.Printf("[%s]\n", strings.Join(elemsStr, " "))
			} else {
				meta := slab.(*ArrayMetaDataSlab)
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
			slab, err := getArraySlab(array.storage, id)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("overflow: (id %d) %s\n", id, slab.String())
		}
	}
}

func (array *Array) valid() (bool, error) {
	verified, _, err := array._valid(array.root.Header().id, 0)
	return verified, err
}

func (array *Array) _valid(id StorageID, level int) (bool, uint32, error) {

	slab, err := getArraySlab(array.storage, id)
	if err != nil {
		return false, 0, err
	}
	if slab.IsData() {
		dataSlab, ok := slab.(*ArrayDataSlab)
		if !ok {
			return false, 0, fmt.Errorf("slab %d is not ArrayDataSlab", id)
		}

		count := uint32(len(dataSlab.elements))

		computedSize := uint32(0)
		for _, e := range dataSlab.elements {
			computedSize += e.ByteSize()
		}

		_, underflow := dataSlab.IsUnderflow()
		validFill := (level == 0) || (!dataSlab.IsFull() && !underflow)

		validCount := count == dataSlab.header.count

		validSize := (dataSlabPrefixSize + computedSize) == dataSlab.header.size

		return validFill && validCount && validSize, count, nil
	}

	meta, ok := slab.(*ArrayMetaDataSlab)
	if !ok {
		return false, 0, fmt.Errorf("slab %d is not ArrayMetaDataSlab", id)
	}
	sum := uint32(0)
	for _, h := range meta.childrenHeaders {
		verified, count, err := array._valid(h.id, level+1)
		if !verified || err != nil {
			return false, 0, err
		}
		sum += count
	}

	_, underflow := meta.IsUnderflow()
	validFill := (level == 0) || (!meta.IsFull() && !underflow)

	validCount := sum == meta.header.count

	computedSize := uint32(len(meta.childrenHeaders)*slabHeaderSize) + metaDataSlabPrefixSize
	validSize := computedSize == meta.header.size

	return validFill && validCount && validSize, sum, nil
}
