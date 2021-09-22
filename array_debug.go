/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

type Stats struct {
	Levels            uint64
	ElementCount      uint64
	MetaDataSlabCount uint64
	DataSlabCount     uint64
}

// ArrayStats returns stats about array slabs.
func ArrayStats(a *Array) (Stats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	metaDataSlabSize := uint64(0)
	dataSlabCount := uint64(0)
	dataSlabSize := uint64(0)

	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(a.root.Header().id)

	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getArraySlab(a.Storage, id)
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
		ElementCount:      a.Count(),
		MetaDataSlabCount: metaDataSlabCount,
		DataSlabCount:     dataSlabCount,
	}, nil
}

// PrintArray prints array slab data to stdout.
func PrintArray(a *Array) {
	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(a.root.Header().id)

	overflowIDs := list.New()

	level := 0
	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, err := getArraySlab(a.Storage, id)
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
					if id, ok := e.(StorageIDStorable); ok {
						overflowIDs.PushBack(StorageID(id))
					}
					elemsStr = append(elemsStr, fmt.Sprint(e))
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
			slab, err := getArraySlab(a.Storage, id)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("overflow: (id %d) %s\n", id, slab.String())
		}
	}
}

func validArray(a *Array, typeInfo cbor.RawMessage) error {

	// Verify that root has correct type information
	extraData := a.root.ExtraData()
	if extraData == nil {
		return errors.New("root slab doesn't have extra data")
	}
	if !bytes.Equal(extraData.TypeInfo, typeInfo) {
		return fmt.Errorf(
			"type information is %v, want %v",
			extraData.TypeInfo,
			typeInfo,
		)
	}

	_, err := validArraySlab(a.Storage, a.root.Header().id, 0)
	return err
}

func validArraySlab(storage SlabStorage, id StorageID, level int) (elementCount uint32, err error) {

	slab, err := getArraySlab(storage, id)
	if err != nil {
		return 0, err
	}

	if level > 0 {
		// Verify that non-root slab doesn't have extra data.
		if slab.ExtraData() != nil {
			return 0, fmt.Errorf("non-root slab %d has extra data", id)
		}

		// Verify that non-root slab doesn't underflow
		if underflowSize, underflow := slab.IsUnderflow(); underflow {
			return 0, fmt.Errorf("slab %d underflows by %d bytes", id, underflowSize)
		}

	}

	// Verify that slab doesn't overflow
	if slab.IsFull() {
		return 0, fmt.Errorf("slab %d is full", id)
	}

	if slab.IsData() {
		dataSlab, ok := slab.(*ArrayDataSlab)
		if !ok {
			return 0, fmt.Errorf("slab %d is not ArrayDataSlab", id)
		}

		// Verify that element count is the same as header.count
		if uint32(len(dataSlab.elements)) != dataSlab.header.count {
			return 0, fmt.Errorf("data slab %d doesn't have valid count, want %d, got %d", id, len(dataSlab.elements), dataSlab.header.count)
		}

		// Verify that aggregated element size + slab prefix is the same as header.size
		computedSize := uint32(arrayDataSlabPrefixSize)
		for _, e := range dataSlab.elements {
			computedSize += e.ByteSize()
		}

		if computedSize != dataSlab.header.size {
			return 0, fmt.Errorf("data slab %d doesn't have valid size, want %d, got %d", id, computedSize, dataSlab.header.size)
		}

		return dataSlab.header.count, nil
	}

	meta, ok := slab.(*ArrayMetaDataSlab)
	if !ok {
		return 0, fmt.Errorf("slab %d is not ArrayMetaDataSlab", id)
	}

	sum := uint32(0)
	for _, h := range meta.childrenHeaders {
		// Verify child slabs
		count, err := validArraySlab(storage, h.id, level+1)
		if err != nil {
			return 0, err
		}

		sum += count
	}

	// Verify that aggregated element count is the same as header.count in meta slab
	if sum != meta.header.count {
		return 0, fmt.Errorf("metadata slab %d doesn't have valid count, want %d, got %d", id, sum, meta.header.count)
	}

	// Verify that aggregated header size + slab prefix is the same as header.size
	computedSize := uint32(len(meta.childrenHeaders)*arraySlabHeaderSize) + arrayMetaDataSlabPrefixSize
	if computedSize != meta.header.size {
		return 0, fmt.Errorf("metadata slab %d size is invalid, want %d, got %d", id, computedSize, meta.header.size)
	}

	return sum, nil
}
