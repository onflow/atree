package main

import (
	"container/list"
	"fmt"
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

			slab, found, err := array.storage.Retrieve(id)
			if err != nil {
				return Stats{}, err
			}
			if !found {
				return Stats{}, fmt.Errorf("slab %d not found", id)
			}

			if slab.IsLeaf() {
				// leaf node
				leaf := slab.(*ArrayDataSlab)
				leafNodeCount++
				leafNodeSize += uint64(leaf.header.size)
			} else {
				// internal node
				node := slab.(*ArrayMetaDataSlab)
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

	level := 0
	for nextLevelIDs.Len() > 0 {

		ids := nextLevelIDs

		nextLevelIDs = list.New()

		for e := ids.Front(); e != nil; e = e.Next() {
			id := e.Value.(StorageID)

			slab, found, err := array.storage.Retrieve(id)
			if err != nil {
				fmt.Println(err)
				return
			}
			if !found {
				fmt.Printf("slab %d not found", id)
				return
			}

			if slab.IsLeaf() {
				// leaf node
				leaf := slab.(*ArrayDataSlab)
				fmt.Printf("level %d, leaf (%+v): ", level+1, *(leaf.header))
				if len(leaf.elements) <= 6 {
					fmt.Printf("%+v\n", leaf.elements)
				} else {
					es := leaf.elements
					lastIdx := len(leaf.elements) - 1
					fmt.Printf("[%d %d %d ... %d %d %d]\n", es[0], es[1], es[2], es[lastIdx-2], es[lastIdx-1], es[lastIdx])
				}
			} else {
				// internal node
				node := slab.(*ArrayMetaDataSlab)
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
}

func (array *Array) valid() (bool, error) {
	if array.root == nil {
		return true, nil
	}
	verified, _, err := array._valid(array.root.Header().id)
	return verified, err
}

func (array *Array) _valid(id StorageID) (bool, uint32, error) {
	slab, found, err := array.storage.Retrieve(id)
	if err != nil {
		return false, 0, err
	}
	if !found {
		return false, 0, fmt.Errorf("slab %d not found", id)
	}
	if slab.IsLeaf() {
		node, ok := slab.(*ArrayDataSlab)
		if !ok {
			return false, 0, fmt.Errorf("slab %d is not ArrayDataSlab", id)
		}
		count := uint32(len(node.elements))
		t := count == node.header.count && count*8 == node.header.size
		return t, count, nil
	}

	node, ok := slab.(*ArrayMetaDataSlab)
	if !ok {
		return false, 0, fmt.Errorf("slab %d is not ArrayMetaDataSlab", id)
	}
	sum := uint32(0)
	for _, h := range node.orderedHeaders {
		verified, count, err := array._valid(h.id)
		if !verified || err != nil {
			return false, 0, err
		}
		sum += count
	}
	t := sum == node.header.count && uint32(len(node.orderedHeaders)*headerSize) == node.header.size
	return t, sum, nil
}
