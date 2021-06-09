package main

import (
	"container/list"
	"fmt"
	"math"
)

const headerSize = 16

var (
	// Default slab size
	targetThreshold = uint64(1024) // 1kb

	minThreshold = targetThreshold / 4
	maxThreshold = uint64(float64(targetThreshold) * 1.5)
)

func setThreshold(threshold uint64) {
	targetThreshold = threshold
	minThreshold = targetThreshold / 4
	maxThreshold = uint64(float64(targetThreshold) * 1.5)
}

type Segmentable interface {
	Split() (Segmentable, Segmentable, error)
	Merge(Segmentable) error
	// ByteSize() uint32
}

type Slab interface {
	IsLeaf() bool
	IsFull() bool

	Segmentable
}

type SlabHeader struct {
	id    StorageID // id is used to retrieve slab from storage
	size  uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count uint32    // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

// ArraySlab is leaf node
type ArraySlab struct {
	storage  Storage
	header   *SlabHeader
	elements []uint64
}

// ArrayMetaSlab is internal node
type ArrayMetaSlab struct {
	storage        Storage
	header         *SlabHeader
	orderedHeaders []*SlabHeader
}

// Array is tree
type Array struct {
	storage Storage
	root    *ArrayMetaSlab
}

func newArraySlab(storage Storage) *ArraySlab {
	slab := &ArraySlab{
		storage: storage,
		header: &SlabHeader{
			id: generateStorageID(),
		},
	}

	storage.Store(slab.header.id, slab)

	return slab
}

func (a *ArraySlab) get(index uint64) (uint64, error) {
	if index >= uint64(len(a.elements)) {
		return 0, fmt.Errorf("out of bounds")
	}
	return a.elements[index], nil
}

func (a *ArraySlab) append(v uint64) error {
	a.elements = append(a.elements, v)
	a.header.count++
	a.header.size += 8 // size of uint64
	return nil
}

func (a *ArraySlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

func (a *ArraySlab) IsLeaf() bool {
	return true
}

func (a *ArraySlab) Split() (Segmentable, Segmentable, error) {
	if len(a.elements) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split array slab with less than 2 elements")
	}

	// this compute the ceil of split keep the first part with more members (optimized for append operations)
	size := a.header.size
	d := float64(size) / float64(2)
	breakPoint := int(math.Ceil(d))

	newSlabStartIndex := 0
	slab1Size := 0
	for i := range a.elements {
		slab1Size += 8 // size of uint64
		if slab1Size >= breakPoint {
			newSlabStartIndex = i + 1
			break
		}
	}

	newSlab := newArraySlab(a.storage)
	newSlab.elements = a.elements[newSlabStartIndex:]
	newSlab.header.size = a.header.size - uint32(slab1Size)
	newSlab.header.count = a.header.count - uint32(newSlabStartIndex)

	a.elements = a.elements[:newSlabStartIndex]
	a.header.size = uint32(slab1Size)
	a.header.count = uint32(newSlabStartIndex)

	return a, newSlab, nil
}

func (a *ArraySlab) Merge(seg Segmentable) error {
	slab2 := seg.(*ArraySlab)
	a.elements = append(a.elements, slab2.elements...)
	a.header.size += slab2.header.size
	a.header.count += slab2.header.count
	return nil
}

func newArrayMetaSlab(storage Storage) *ArrayMetaSlab {
	slab := &ArrayMetaSlab{
		storage: storage,
		header: &SlabHeader{
			id: generateStorageID(),
		},
	}

	storage.Store(slab.header.id, slab)

	return slab
}

func (a *ArrayMetaSlab) get(index uint64) (uint64, error) {

	if index >= uint64(a.header.count) {
		return 0, fmt.Errorf("index %d out of bounds for %d", index, a.header.count)
	}

	var id StorageID
	found := false

	startIndex := uint64(0)
	for _, h := range a.orderedHeaders {
		if index >= startIndex && index < startIndex+uint64(h.count) {
			found = true
			id = h.id
			break
		}
		startIndex += uint64(h.count)
	}

	if !found {
		return 0, fmt.Errorf("out of bounds")
	}

	slab, found, err := a.storage.Retrieve(id)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("slab %d not found", id)
	}

	adjustedIndex := index - startIndex

	if slab.IsLeaf() {
		leaf := slab.(*ArraySlab)
		return leaf.get(adjustedIndex)
	}

	// slab is an internal node
	meta := slab.(*ArrayMetaSlab)
	return meta.get(adjustedIndex)
}

func (a *ArrayMetaSlab) append(v uint64) error {

	if len(a.orderedHeaders) == 0 {

		slab := newArraySlab(a.storage)
		a.orderedHeaders = append(a.orderedHeaders, slab.header)
		a.header.count = 1
		a.header.size = headerSize

		return slab.append(v)
	}

	headerIdx := len(a.orderedHeaders) - 1
	header := a.orderedHeaders[headerIdx]

	slab, found, err := a.storage.Retrieve(header.id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("slab %d not found", header.id)
	}

	if slab.IsLeaf() {
		leaf := slab.(*ArraySlab)
		err := leaf.append(v)
		if err != nil {
			return err
		}
		a.header.count++

		if leaf.IsFull() {
			leftSeg, rightSeg, err := leaf.Split()
			if err != nil {
				return err
			}

			leftLeaf := leftSeg.(*ArraySlab)
			rightLeaf := rightSeg.(*ArraySlab)

			a.orderedHeaders = a.orderedHeaders[:headerIdx]
			a.orderedHeaders = append(a.orderedHeaders, leftLeaf.header, rightLeaf.header)
			a.header.size += headerSize
		}

		return nil
	}

	meta := slab.(*ArrayMetaSlab)
	err = meta.append(v)
	if err != nil {
		return err
	}
	a.header.count++

	if meta.IsFull() {
		leftSeg, rightSeg, err := meta.Split()
		if err != nil {
			return err
		}

		leftMeta := leftSeg.(*ArrayMetaSlab)
		rightMeta := rightSeg.(*ArrayMetaSlab)

		a.orderedHeaders = a.orderedHeaders[:headerIdx]
		a.orderedHeaders = append(a.orderedHeaders, leftMeta.header, rightMeta.header)
		a.header.size += headerSize
	}

	return nil
}

func (a ArrayMetaSlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

func (a ArrayMetaSlab) IsLeaf() bool {
	return false
}

func (a *ArrayMetaSlab) Merge(seg Segmentable) error {
	meta2 := seg.(*ArrayMetaSlab)
	a.orderedHeaders = append(a.orderedHeaders, meta2.orderedHeaders...)
	a.header.size += meta2.header.size
	a.header.count += meta2.header.count
	return nil
}

func (a *ArrayMetaSlab) Split() (Segmentable, Segmentable, error) {

	if len(a.orderedHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		return a, nil, fmt.Errorf("can't split meta slab with less than 2 headers")
	}

	// this compute the ceil of split keep the first part with more members (optimized for append operations)
	size := a.header.size
	d := float64(size) / float64(2)
	breakPoint := int(math.Ceil(d))

	slab1Size := uint32(0)
	slab1Count := uint32(0)
	newSlabStartIndex := 0
	for i, h := range a.orderedHeaders {
		slab1Size += headerSize
		slab1Count += h.count
		if slab1Size >= uint32(breakPoint) {
			newSlabStartIndex = i + 1
			break
		}
	}

	right := newArrayMetaSlab(a.storage)
	right.orderedHeaders = a.orderedHeaders[newSlabStartIndex:]
	right.header.count = a.header.count - slab1Count
	right.header.size = a.header.size - slab1Size

	a.orderedHeaders = a.orderedHeaders[:newSlabStartIndex]
	a.header.count = slab1Count
	a.header.size = slab1Size

	return a, right, nil
}

func NewArray(storage Storage) *Array {
	return &Array{storage: storage}
}

func (array *Array) Get(i uint64) (uint64, error) {
	if array.root == nil {
		return 0, fmt.Errorf("out of bounds")
	}
	return array.root.get(i)
}

func (array *Array) Append(v uint64) error {
	if array.root == nil {
		array.root = newArrayMetaSlab(array.storage)
	}

	err := array.root.append(v)
	if err != nil {
		return err
	}

	if array.root.IsFull() {
		left, right, err := array.root.Split()
		if err != nil {
			return err
		}

		leftSlab := left.(*ArrayMetaSlab)
		rightSlab := right.(*ArrayMetaSlab)

		newRoot := newArrayMetaSlab(array.storage)
		newRoot.orderedHeaders = []*SlabHeader{leftSlab.header, rightSlab.header}
		newRoot.header.count = leftSlab.header.count + rightSlab.header.count
		newRoot.header.size = headerSize * 2

		array.root = newRoot
	}

	return nil
}

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
	nextLevelIDs.PushBack(array.root.header.id)

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
				leaf := slab.(*ArraySlab)
				leafNodeCount++
				leafNodeSize += uint64(leaf.header.size)
			} else {
				// internal node
				node := slab.(*ArrayMetaSlab)
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
		ElementCount:          uint64(array.root.header.count),
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
	nextLevelIDs.PushBack(array.root.header.id)

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
				leaf := slab.(*ArraySlab)
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
				node := slab.(*ArrayMetaSlab)
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
