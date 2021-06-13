package main

import (
	"fmt"
	"math"
)

const headerSize = 16

type Slab interface {
	IsLeaf() bool // Currently, only used for debugging and stats
	IsFull() bool

	Header() *SlabHeader

	Split() (Slab, Slab, error)
	Merge(Slab) error
}

type SlabHeader struct {
	id    StorageID // id is used to retrieve slab from storage
	size  uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count uint32    // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

// ArrayDataSlab is leaf node, implementing ArrayNode
type ArrayDataSlab struct {
	prev     StorageID
	next     StorageID
	header   *SlabHeader
	elements []uint64
	storage  Storage
}

// ArrayMetaDataSlab is internal node, implementing ArrayNode
type ArrayMetaDataSlab struct {
	storage        Storage
	header         *SlabHeader
	orderedHeaders []*SlabHeader
}

type ArrayNode interface {
	Get(index uint64) (uint64, error)
	Insert(index uint64, v uint64) error

	Slab
}

// Array is tree
type Array struct {
	storage           Storage
	root              *ArrayMetaDataSlab
	dataSlabStorageID StorageID
}

func newArrayDataSlab(storage Storage) *ArrayDataSlab {
	slab := &ArrayDataSlab{
		storage: storage,
		header: &SlabHeader{
			id: generateStorageID(),
		},
	}

	storage.Store(slab.header.id, slab)

	return slab
}

func (a *ArrayDataSlab) Get(index uint64) (uint64, error) {
	if index >= uint64(len(a.elements)) {
		return 0, fmt.Errorf("out of bounds")
	}
	return a.elements[index], nil
}

func (a *ArrayDataSlab) Insert(index uint64, v uint64) error {
	if index == uint64(len(a.elements)) {
		a.elements = append(a.elements, v)
	} else {
		a.elements = append(a.elements, 0)
		copy(a.elements[index+1:], a.elements[index:])
		a.elements[index] = v
	}

	a.header.count++
	a.header.size += 8 // size of uint64
	return nil
}

func (a *ArrayDataSlab) Header() *SlabHeader {
	return a.header
}

func (a *ArrayDataSlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

func (a *ArrayDataSlab) IsLeaf() bool {
	return true
}

func (a *ArrayDataSlab) Split() (Slab, Slab, error) {
	if len(a.elements) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split array slab with less than 2 elements")
	}

	// This computes the ceil of split keep the first part with more members (optimized for append operations)
	size := a.header.size
	d := float64(size) / float64(2)
	breakPoint := uint32(math.Ceil(d))

	rightSlabStartIndex := 0
	leftSlabSize := uint32(0)
	for i := range a.elements {
		leftSlabSize += 8 // size of uint64
		if leftSlabSize >= breakPoint {
			rightSlabStartIndex = i + 1
			break
		}
	}

	rightSlab := newArrayDataSlab(a.storage)
	rightSlab.elements = make([]uint64, len(a.elements)-rightSlabStartIndex)
	copy(rightSlab.elements, a.elements[rightSlabStartIndex:])
	rightSlab.header.size = a.header.size - leftSlabSize
	rightSlab.header.count = uint32(len(rightSlab.elements))
	rightSlab.prev = a.header.id
	rightSlab.next = a.next

	a.elements = a.elements[:rightSlabStartIndex]
	a.header.size = leftSlabSize
	a.header.count = uint32(len(a.elements))
	a.next = rightSlab.header.id

	return a, rightSlab, nil
}

func (a *ArrayDataSlab) Merge(slab Slab) error {
	slab2 := slab.(*ArrayDataSlab)
	a.elements = append(a.elements, slab2.elements...)
	a.header.size += slab2.header.size
	a.header.count += slab2.header.count
	return nil
}

func newArrayMetaDataSlab(storage Storage) *ArrayMetaDataSlab {
	slab := &ArrayMetaDataSlab{
		storage: storage,
		header: &SlabHeader{
			id: generateStorageID(),
		},
	}

	storage.Store(slab.header.id, slab)

	return slab
}

func (a *ArrayMetaDataSlab) Get(index uint64) (uint64, error) {

	if index >= uint64(a.header.count) {
		return 0, fmt.Errorf("index %d out of bounds for slab %d", index, a.header.id)
	}

	var id StorageID

	startIndex := uint64(0)
	for _, h := range a.orderedHeaders {
		if index >= startIndex && index < startIndex+uint64(h.count) {
			id = h.id
			break
		}
		startIndex += uint64(h.count)
	}

	if id == StorageIDUndefined {
		return 0, fmt.Errorf("index %d out of bounds for slab %d", index, a.header.id)
	}

	slab, found, err := a.storage.Retrieve(id)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("slab %d not found", id)
	}

	adjustedIndex := index - startIndex

	node, ok := slab.(ArrayNode)
	if !ok {
		return 0, fmt.Errorf("slab %d is not ArrayNode", id)
	}
	return node.Get(adjustedIndex)
}

// Insert inserts v into correct ArrayDataSlab.
// index must be >=0 and <= a.header.count.
// If index == a.header.count, Insert appends v to the end of underlying data slab.
func (a *ArrayMetaDataSlab) Insert(index uint64, v uint64) error {
	if index > uint64(a.header.count) {
		return fmt.Errorf("insert at index %d out of bounds", index)
	}

	if len(a.orderedHeaders) == 0 {
		slab := newArrayDataSlab(a.storage)
		a.orderedHeaders = append(a.orderedHeaders, slab.header)
		a.header.count = 1
		a.header.size = headerSize

		return slab.Insert(0, v)
	}

	var id StorageID
	var adjustedIndex uint64
	i := 0

	if index == uint64(a.header.count) {
		i = len(a.orderedHeaders) - 1
		header := a.orderedHeaders[i]
		id = header.id
		adjustedIndex = uint64(header.count)
	} else {
		var h *SlabHeader
		startIndex := uint64(0)
		for i, h = range a.orderedHeaders {
			if index >= startIndex && index < startIndex+uint64(h.count) {
				id = h.id
				adjustedIndex = index - startIndex
				break
			}
			startIndex += uint64(h.count)
		}
	}

	slab, found, err := a.storage.Retrieve(id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("insert(%d, %d): slab %d not found", index, v, id)
	}

	node, ok := slab.(ArrayNode)
	if !ok {
		return fmt.Errorf("slab %d is not ArrayNode", id)
	}

	err = node.Insert(adjustedIndex, v)
	if err != nil {
		return err
	}

	a.header.count++

	if node.IsFull() {
		left, right, err := node.Split()
		if err != nil {
			return err
		}

		a.orderedHeaders = append(a.orderedHeaders, nil)
		if i < len(a.orderedHeaders)-2 {
			copy(a.orderedHeaders[i+2:], a.orderedHeaders[i+1:])
		}
		a.orderedHeaders[i] = left.Header()
		a.orderedHeaders[i+1] = right.Header()

		a.header.size += headerSize
	}

	return nil
}

func (a *ArrayMetaDataSlab) Header() *SlabHeader {
	return a.header
}

func (a ArrayMetaDataSlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

func (a ArrayMetaDataSlab) IsLeaf() bool {
	return false
}

func (a *ArrayMetaDataSlab) Merge(slab Slab) error {
	meta2 := slab.(*ArrayMetaDataSlab)
	a.orderedHeaders = append(a.orderedHeaders, meta2.orderedHeaders...)
	a.header.size += meta2.header.size
	a.header.count += meta2.header.count
	return nil
}

func (a *ArrayMetaDataSlab) Split() (Slab, Slab, error) {

	if len(a.orderedHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		return nil, nil, fmt.Errorf("can't split meta slab with less than 2 headers")
	}

	rightSlabStartIndex := int(math.Ceil(float64(len(a.orderedHeaders)) / float64(2)))
	leftSlabSize := rightSlabStartIndex * headerSize

	leftSlabCount := uint32(0)
	for i := 0; i < rightSlabStartIndex; i++ {
		leftSlabCount += a.orderedHeaders[i].count
	}

	right := newArrayMetaDataSlab(a.storage)
	right.orderedHeaders = make([]*SlabHeader, len(a.orderedHeaders)-rightSlabStartIndex)
	copy(right.orderedHeaders, a.orderedHeaders[rightSlabStartIndex:])
	right.header.count = a.header.count - leftSlabCount
	right.header.size = a.header.size - uint32(leftSlabSize)

	a.orderedHeaders = a.orderedHeaders[:rightSlabStartIndex]
	a.header.count = leftSlabCount
	a.header.size = uint32(leftSlabSize)

	return a, right, nil
}

func NewArray(storage Storage) *Array {
	return &Array{storage: storage}
}

func (array *Array) Get(i uint64) (uint64, error) {
	if array.root == nil {
		return 0, fmt.Errorf("out of bounds")
	}
	return array.root.Get(i)
}

func (array *Array) Append(v uint64) error {
	if array.root == nil {
		return array.Insert(0, v)
	}
	return array.Insert(uint64(array.root.header.count), v)
}

func (array *Array) Insert(index uint64, v uint64) error {
	if array.root == nil {
		if index != 0 {
			return fmt.Errorf("out of bounds")
		}

		array.root = newArrayMetaDataSlab(array.storage)
		err := array.root.Insert(0, v)
		if err != nil {
			return err
		}
		array.dataSlabStorageID = array.root.orderedHeaders[0].id
		return nil
	}

	err := array.root.Insert(index, v)
	if err != nil {
		return err
	}

	if array.root.IsFull() {
		// Shallow copy root node with a new StorageID
		copiedRoot := newArrayMetaDataSlab(array.storage)
		copiedRoot.header.size = array.root.header.size
		copiedRoot.header.count = array.root.header.count
		copiedRoot.orderedHeaders = array.root.orderedHeaders

		// Split copied root node
		left, right, err := copiedRoot.Split()
		if err != nil {
			return err
		}

		// Reset root with new nodes (StorageID is unchanged).
		array.root.orderedHeaders = []*SlabHeader{left.Header(), right.Header()}
		array.root.header.count = left.Header().count + right.Header().count
		array.root.header.size = headerSize * 2
	}

	return nil
}

func (array *Array) Iterate(fn func(uint64)) error {
	id := array.dataSlabStorageID

	for id != StorageIDUndefined {

		slab, found, err := array.storage.Retrieve(id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("slab %d not found", id)
		}

		dataSlab := slab.(*ArrayDataSlab)

		for i := 0; i < len(dataSlab.elements); i++ {
			fn(dataSlab.elements[i])
		}

		id = dataSlab.next
	}

	return nil
}

func (array *Array) StorageID() StorageID {
	if array.root == nil {
		return StorageIDUndefined
	}
	return array.root.header.id
}
