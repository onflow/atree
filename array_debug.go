/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"container/list"
	"fmt"
	"reflect"
	"strings"
)

type ArrayStats struct {
	Levels            uint64
	ElementCount      uint64
	MetaDataSlabCount uint64
	DataSlabCount     uint64
}

// GetArrayStats returns stats about array slabs.
func GetArrayStats(a *Array) (ArrayStats, error) {
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
				return ArrayStats{}, err
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

	return ArrayStats{
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
				fmt.Printf("level %d, leaf (%+v next:%d): ", level+1, dataSlab.header, dataSlab.next)

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

func ValidArray(a *Array, typeInfo TypeInfo, hip HashInputProvider) error {

	extraData := a.root.ExtraData()
	if extraData == nil {
		return fmt.Errorf("root slab %d doesn't have extra data", a.root.ID())
	}

	// Verify that extra data has correct type information
	if typeInfo != nil && !extraData.TypeInfo.Equal(typeInfo) {
		return fmt.Errorf(
			"root slab %d type information %v is wrong, want %v",
			a.root.ID(),
			extraData.TypeInfo,
			typeInfo,
		)
	}

	computedCount, dataSlabIDs, nextDataSlabIDs, err :=
		validArraySlab(hip, a.Storage, a.root.Header().id, 0, nil, []StorageID{}, []StorageID{})
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
		for _, e := range dataSlab.elements {

			// Verify element size is <= inline size
			if e.ByteSize() > uint32(MaxInlineElementSize) {
				return 0, nil, nil, fmt.Errorf("data slab %d element %s size %d is too large, want < %d",
					id, e, e.ByteSize(), MaxInlineElementSize)
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
				return 0, nil, nil, fmt.Errorf("data slab %d element %s can't be converted to value, %s",
					id, e, err)
			}
			err = ValidValue(v, nil, hip)
			if err != nil {
				return 0, nil, nil, fmt.Errorf("data slab %d element %s isn't valid, %s",
					id, e, err)
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
		count, dataSlabIDs, nextDataSlabIDs, err = validArraySlab(hip, storage, h.id, level+1, &h, dataSlabIDs, nextDataSlabIDs)
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
