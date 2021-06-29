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
	Levels                uint64
	ElementCount          uint64
	InternalNodeCount     uint64
	LeafNodeCount         uint64
	InternalNodeOccupancy float64
	LeafNodeOccupancy     float64 // sum(leaf node size)/(num of leaf node * threshold size)
}

// Stats returns stats about the array slabs.
func (array *Array) Stats() (Stats, error) {
	if array.root == nil {
		return Stats{}, nil
	}

	level := uint64(0)
	internalNodeCount := uint64(0)
	internalNodeSize := uint64(0)
	leafNodeCount := uint64(0)
	leafNodeSize := uint64(0)

	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(array.root.Header().id)

	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			node, err := getArrayNodeFromStorageID(array.storage, id)
			if err != nil {
				return Stats{}, err
			}

			if node.IsLeaf() {
				// leaf node
				leaf := node.(*ArrayDataSlab)
				leafNodeCount++
				leafNodeSize += uint64(leaf.header.size)
			} else {
				// internal node
				node := node.(*ArrayMetaDataSlab)
				internalNodeCount++
				internalNodeSize += uint64(node.header.size)

				for _, h := range node.orderedHeaders {
					nextLevelIDs.PushBack(h.id)
				}
			}
		}

		level++
	}

	leafNodeOccupancy := float64(leafNodeSize) / float64(targetThreshold*leafNodeCount)
	internalNodeNodeOccupancy := float64(internalNodeSize) / float64(targetThreshold*internalNodeCount)

	return Stats{
		Levels:                level,
		ElementCount:          array.Count(),
		InternalNodeCount:     internalNodeCount,
		LeafNodeCount:         leafNodeCount,
		InternalNodeOccupancy: internalNodeNodeOccupancy,
		LeafNodeOccupancy:     leafNodeOccupancy,
	}, nil
}

func (array *Array) Print() {
	if array.root == nil {
		fmt.Printf("empty tree\n")
		return
	}

	nextLevelIDs := list.New()
	nextLevelIDs.PushBack(array.root.Header().id)

	overflowIDs := list.New()

	level := 0
	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			node, err := getArrayNodeFromStorageID(array.storage, id)
			if err != nil {
				fmt.Println(err)
				return
			}

			if node.IsLeaf() {
				// leaf node
				leaf := node.(*ArrayDataSlab)
				fmt.Printf("level %d, leaf (%+v): ", level+1, *(leaf.header))

				var elements []Storable
				if len(leaf.elements) <= 6 {
					elements = leaf.elements
				} else {
					elements = append(elements, leaf.elements[:3]...)
					elements = append(elements, leaf.elements[len(leaf.elements)-3:]...)
				}

				var elemsStr []string
				for _, e := range elements {
					if id, ok := e.(StorageIDValue); ok {
						overflowIDs.PushBack(StorageID(id))
					}
					elemsStr = append(elemsStr, e.String())
				}

				if len(leaf.elements) > 6 {
					elemsStr = append(elemsStr, "")
					copy(elemsStr[4:], elemsStr[3:])
					elemsStr[3] = "..."
				}
				fmt.Printf("[%s]\n", strings.Join(elemsStr, " "))
			} else {
				// internal node
				node := node.(*ArrayMetaDataSlab)
				fmt.Printf("level %d, meta (%+v) headers: [", level+1, *(node.header))
				for _, h := range node.orderedHeaders {
					fmt.Printf("%+v ", *h)
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
			node, err := getArrayNodeFromStorageID(array.storage, id)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Printf("overflow node: (id %d) %s\n", id, node.String())
		}
	}
}

func (array *Array) valid() (bool, error) {
	if array.root == nil {
		return true, nil
	}
	verified, _, err := array._valid(array.root.Header().id, 0)
	return verified, err
}

func (array *Array) _valid(id StorageID, level int) (bool, uint32, error) {

	node, err := getArrayNodeFromStorageID(array.storage, id)
	if err != nil {
		return false, 0, err
	}
	if node.IsLeaf() {
		node, ok := node.(*ArrayDataSlab)
		if !ok {
			return false, 0, fmt.Errorf("slab %d is not ArrayDataSlab", id)
		}

		count := uint32(len(node.elements))

		computedSize := uint32(0)
		for _, e := range node.elements {
			computedSize += e.ByteSize()
		}

		_, underflow := node.IsUnderflow()
		validTreeNodeSize := (level == 0) || (!node.IsFull() && !underflow)

		validCount := count == node.header.count

		validSize := (dataSlabPrefixSize + computedSize) == node.header.size

		return validTreeNodeSize && validCount && validSize, count, nil
	}

	metaNode, ok := node.(*ArrayMetaDataSlab)
	if !ok {
		return false, 0, fmt.Errorf("slab %d is not ArrayMetaDataSlab", id)
	}
	sum := uint32(0)
	for _, h := range metaNode.orderedHeaders {
		verified, count, err := array._valid(h.id, level+1)
		if !verified || err != nil {
			return false, 0, err
		}
		sum += count
	}

	_, underflow := node.IsUnderflow()
	validTreeNodeSize := (level == 0) || (!node.IsFull() && !underflow)

	validCount := sum == metaNode.header.count

	computedSize := uint32(len(metaNode.orderedHeaders)*headerSize) + metaDataSlabPrefixSize
	validSize := computedSize == metaNode.header.size

	return validTreeNodeSize && validCount && validSize, sum, nil
}
