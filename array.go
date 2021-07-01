/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	// slab header size: storage id (8 bytes) + count (4 bytes) + size (4 bytes)
	headerSize = 8 + 4 + 4

	// meta data slab prefix size: flag (1 byte) + child header count (4 bytes)
	metaDataSlabPrefixSize = 1 + 4

	// flag (1 byte) + prev id (8 bytes) + next id (8 bytes) + CBOR array size (3 bytes)
	// (3 bytes of array size support up to 65535 array elements)
	dataSlabPrefixSize = 1 + 8 + 8 + 3
)

type Slab interface {
	Storable

	// Bytes is a wrapper for Storable.Encode()
	Bytes() ([]byte, error)

	Header() *SlabHeader

	Split() (Slab, Slab, error)
	Merge(Slab) error
	// LendToRight rebalances nodes by moving elements from left to right
	LendToRight(Slab) error
	// BorrowFromRight rebalances nodes by moving elements from right to left
	BorrowFromRight(Slab) error
}

type SlabHeader struct {
	id    StorageID // id is used to retrieve slab from storage
	size  uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count uint32    // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

// ArrayDataSlab is leaf node, implementing Slab, ArrayNode
// TODO: clarify if design doc wants leaf to hold user data in fixed size array + overflow leaf
// (cache friendly) or in a variable size slice (not cache friendly).
type ArrayDataSlab struct {
	prev     StorageID
	next     StorageID
	header   *SlabHeader
	elements []Storable
}

// ArrayMetaDataSlab is internal node, implementing Slab, ArrayNode
type ArrayMetaDataSlab struct {
	header         *SlabHeader
	orderedHeaders []*SlabHeader
}

type ArrayNode interface {
	Get(storage SlabStorage, index uint64) (Storable, error)
	Set(storage SlabStorage, index uint64, v Storable) error
	Insert(storage SlabStorage, index uint64, v Storable) error
	Remove(storage SlabStorage, index uint64) (Storable, error)

	ShallowCloneWithNewID() ArrayNode

	IsLeaf() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	Slab
}

// Array is tree
type Array struct {
	storage           SlabStorage
	root              ArrayNode
	dataSlabStorageID StorageID
}

type IndexOutOfRangeError struct {
	// TODO: add more info
}

func (e IndexOutOfRangeError) Error() string {
	// TODO: add more info
	return "index out of range"
}

func (a *Array) StorageID() StorageID {
	return a.dataSlabStorageID
}

func newArrayDataSlab() *ArrayDataSlab {
	return &ArrayDataSlab{
		header: &SlabHeader{
			id:   generateStorageID(),
			size: dataSlabPrefixSize,
		},
	}
}

func newArrayDataSlabFromData(slabID StorageID, data []byte) (*ArrayDataSlab, error) {

	if len(data) < dataSlabPrefixSize {
		return nil, errors.New("data is too short for array data slab")
	}

	// Check flag
	if data[0]&flagArray == 0 || data[0]&flagLeafNode == 0 {
		return nil, fmt.Errorf("data has invalid flag 0x%x, want 0x%x", data[0], flagArray&flagLeafNode)
	}

	// Decode prev storage id
	prev := binary.BigEndian.Uint64(data[1:9])

	// Decode next storage id
	next := binary.BigEndian.Uint64(data[9:17])

	cborDec := NewByteStreamDecoder(data[17:])

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, err
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec)
		if err != nil {
			return nil, err
		}
		elements[i] = storable
	}

	return &ArrayDataSlab{
		prev:     StorageID(prev),
		next:     StorageID(next),
		header:   &SlabHeader{id: slabID, size: uint32(len(data)), count: uint32(elemCount)},
		elements: elements,
	}, nil
}

// TODO: make this function inline
// TODO: reuse bytes.Buffer with a sync.Pool
func (a *ArrayDataSlab) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := a.Encode(enc)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Leaf node header (17 bytes):
//   | slab flag (1 bytes) | prev sib storage ID (8 bytes) | next sib storage ID (8 bytes) |
// Leaf node content (for now):
//   CBOR encoded array of elements
func (a *ArrayDataSlab) Encode(enc *Encoder) error {

	// Encode flag
	enc.scratch[0] = flagLeafNode | flagArray

	// Encode prev storage id
	binary.BigEndian.PutUint64(enc.scratch[1:], uint64(a.prev))

	// Encode next storage id
	binary.BigEndian.PutUint64(enc.scratch[9:], uint64(a.next))

	// Encode CBOR array size (encode cbor array size manually for fixed sized serialization)
	enc.scratch[17] = 0x80 | 25
	binary.BigEndian.PutUint16(enc.scratch[18:], uint16(len(a.elements)))

	enc.Write(enc.scratch[:20])

	// Encode data slab content (array of elements)
	for _, e := range a.elements {
		err := e.Encode(enc)
		if err != nil {
			return err
		}
	}
	enc.cbor.Flush()

	return nil
}

func (a *ArrayDataSlab) ShallowCloneWithNewID() ArrayNode {
	copied := newArrayDataSlab()
	copied.header.size = a.header.size
	copied.header.count = a.header.count
	copied.elements = a.elements
	copied.prev = a.prev
	copied.next = a.next

	return copied
}

func (a *ArrayDataSlab) Get(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, IndexOutOfRangeError{}
	}
	return a.elements[index], nil
}

func (a *ArrayDataSlab) Set(storage SlabStorage, index uint64, v Storable) error {
	if index >= uint64(len(a.elements)) {
		return IndexOutOfRangeError{}
	}
	oldSize := a.elements[index].ByteSize()
	a.elements[index] = v
	a.header.size = a.header.size - oldSize + v.ByteSize()

	storage.Store(a.header.id, a)

	return nil
}

func (a *ArrayDataSlab) Insert(storage SlabStorage, index uint64, v Storable) error {
	if index > uint64(len(a.elements)) {
		return IndexOutOfRangeError{}
	}
	if index == uint64(len(a.elements)) {
		a.elements = append(a.elements, v)
	} else {
		a.elements = append(a.elements, nil)
		copy(a.elements[index+1:], a.elements[index:])
		a.elements[index] = v
	}

	a.header.count++
	a.header.size += v.ByteSize()

	storage.Store(a.header.id, a)

	return nil
}

func (a *ArrayDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, IndexOutOfRangeError{}
	}
	v := a.elements[index]

	switch index {
	case 0:
		a.elements = a.elements[1:]
	case uint64(len(a.elements)) - 1:
		a.elements = a.elements[:len(a.elements)-1]
	default:
		copy(a.elements[index:], a.elements[index+1:])
		a.elements = a.elements[:len(a.elements)-1]
	}

	a.header.count--
	a.header.size -= v.ByteSize()

	storage.Store(a.header.id, a)

	return v, nil
}

func (a *ArrayDataSlab) Split() (Slab, Slab, error) {
	if len(a.elements) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split array slab with less than 2 elements")
	}

	// This computes the ceil of split to keep the first part with more members (optimized for append operations)
	dataSize := a.header.size - dataSlabPrefixSize
	d := float64(dataSize) / float64(2)
	midPoint := uint32(math.Ceil(d))

	rightSlabStartIndex := 0
	leftSlabSize := uint32(0)
	for i, e := range a.elements {
		elemSize := e.ByteSize()
		if leftSlabSize+elemSize >= midPoint {
			// i is mid point element.  Place i on the small side.
			if leftSlabSize <= dataSize-leftSlabSize-elemSize {
				leftSlabSize += elemSize
				rightSlabStartIndex = i + 1
			} else {
				rightSlabStartIndex = i
			}
			break
		}
		// left slab size < midPoint
		leftSlabSize += elemSize
	}

	rightSlab := newArrayDataSlab()
	rightSlab.elements = make([]Storable, len(a.elements)-rightSlabStartIndex)
	copy(rightSlab.elements, a.elements[rightSlabStartIndex:])
	rightSlab.prev = a.header.id
	rightSlab.next = a.next

	rightSlab.header.size = dataSlabPrefixSize + dataSize - leftSlabSize
	rightSlab.header.count = uint32(len(rightSlab.elements))

	a.elements = a.elements[:rightSlabStartIndex]
	a.header.size = dataSlabPrefixSize + leftSlabSize
	a.header.count = uint32(len(a.elements))
	a.next = rightSlab.header.id

	return a, rightSlab, nil
}

func (a *ArrayDataSlab) Merge(slab Slab) error {
	slab2 := slab.(*ArrayDataSlab)
	a.elements = append(a.elements, slab2.elements...)
	a.header.size = a.header.size + slab2.header.size - dataSlabPrefixSize
	a.header.count += slab2.header.count
	a.next = slab2.next
	return nil
}

// LendToRight rebalances nodes by moving elements from left to right
func (a *ArrayDataSlab) LendToRight(slab Slab) error {

	b := slab.(*ArrayDataSlab)

	count := a.header.count + b.header.count
	size := a.header.size + b.header.size

	midPoint := uint32(math.Ceil(float64(size) / 2))

	leftSlabCount := a.header.count
	leftSlabDataSize := a.header.size

	// Left node size is as close to midPoint as possible while right node size >= minThreshold
	for i := len(a.elements) - 1; i >= 0; i-- {
		if leftSlabDataSize-a.elements[i].ByteSize() < midPoint && size-leftSlabDataSize >= uint32(minThreshold) {
			break
		}
		leftSlabDataSize -= a.elements[i].ByteSize()
		leftSlabCount--
	}

	if leftSlabCount == a.header.count {
		panic(fmt.Sprintf("computed left slab %v remains unchanged after rebalancing", *a.header))
	}

	// Update right slab elements
	elements := make([]Storable, count-leftSlabCount)
	n := copy(elements, a.elements[leftSlabCount:])
	copy(elements[n:], b.elements)

	b.elements = elements

	// Update left slab elements
	a.elements = a.elements[:leftSlabCount]

	// Update left slab header
	a.header.size = leftSlabDataSize
	a.header.count = leftSlabCount

	// Update right slab header
	b.header.size = size - leftSlabDataSize
	b.header.count = count - leftSlabCount

	return nil
}

// BorrowFromRight rebalances nodes by moving elements from right to left
func (a *ArrayDataSlab) BorrowFromRight(slab Slab) error {
	b := slab.(*ArrayDataSlab)

	count := a.header.count + b.header.count
	size := a.header.size + b.header.size

	midPoint := uint32(math.Ceil(float64(size) / 2))

	leftSlabCount := uint32(len(a.elements))
	leftSlabDataSize := a.header.size

	for _, e := range b.elements {
		if leftSlabDataSize+e.ByteSize() > midPoint {
			if size-leftSlabDataSize-e.ByteSize() >= uint32(minThreshold) {
				// Include this element in left node
				leftSlabDataSize += e.ByteSize()
				leftSlabCount++
			}
			break
		}
		leftSlabDataSize += e.ByteSize()
		leftSlabCount++
	}

	if leftSlabCount == a.header.count {
		panic(fmt.Sprintf("computed left slab %v remains unchanged after rebalancing", *a.header))
	}

	rightStartIndex := leftSlabCount - a.header.count

	// Update left slab elements
	a.elements = append(a.elements, b.elements[:rightStartIndex]...)

	// Update right slab elements
	b.elements = b.elements[rightStartIndex:]

	// Update left slab header
	a.header.size = leftSlabDataSize
	a.header.count = leftSlabCount

	// Update right slab header
	b.header.size = size - leftSlabDataSize
	b.header.count = count - leftSlabCount

	return nil
}

func (a *ArrayDataSlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

func (a *ArrayDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > a.header.size {
		return uint32(minThreshold) - a.header.size, true
	}
	return 0, false
}

func (a *ArrayDataSlab) CanLendToLeft(size uint32) bool {
	if len(a.elements) == 0 {
		panic(fmt.Sprintf("empty data slab %d", a.header.id))
	}
	if len(a.elements) < 2 {
		return false
	}
	if a.header.size-size < uint32(minThreshold) {
		return false
	}
	lendSize := uint32(0)
	for i := 0; i < len(a.elements); i++ {
		lendSize += a.elements[i].ByteSize()
		if a.header.size-lendSize < uint32(minThreshold) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

func (a *ArrayDataSlab) CanLendToRight(size uint32) bool {
	if len(a.elements) == 0 {
		panic(fmt.Sprintf("empty data slab %d", a.header.id))
	}
	if len(a.elements) < 2 {
		return false
	}
	if a.header.size-size < uint32(minThreshold) {
		return false
	}
	lendSize := uint32(0)
	for i := len(a.elements) - 1; i >= 0; i-- {
		lendSize += a.elements[i].ByteSize()
		if a.header.size-lendSize < uint32(minThreshold) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

func (a *ArrayDataSlab) Header() *SlabHeader {
	return a.header
}

func (a *ArrayDataSlab) IsLeaf() bool {
	return true
}

func (a *ArrayDataSlab) Mutable() bool {
	return true
}

func (a *ArrayDataSlab) ID() StorageID {
	return a.header.id
}

func (a *ArrayDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *ArrayDataSlab) String() string {
	var elements []Storable
	if len(a.elements) <= 6 {
		elements = a.elements
	} else {
		elements = append(elements, a.elements[:3]...)
		elements = append(elements, a.elements[len(a.elements)-3:]...)
	}

	var elemsStr []string
	for _, e := range elements {
		elemsStr = append(elemsStr, e.String())
	}

	if len(a.elements) > 6 {
		elemsStr = append(elemsStr, "")
		copy(elemsStr[4:], elemsStr[3:])
		elemsStr[3] = "..."
	}
	return fmt.Sprintf("[%s]", strings.Join(elemsStr, " "))
}

func newArrayMetaDataSlab() *ArrayMetaDataSlab {
	return &ArrayMetaDataSlab{
		header: &SlabHeader{
			id:   generateStorageID(),
			size: metaDataSlabPrefixSize,
		},
	}
}

func newArrayMetaDataSlabFromData(slabID StorageID, data []byte) (*ArrayMetaDataSlab, error) {
	if len(data) < metaDataSlabPrefixSize {
		return nil, errors.New("data is too short for array metadata slab")
	}

	// Check flag
	if data[0]&flagArray == 0 || data[0]&flagInternalNode == 0 {
		return nil, fmt.Errorf("data has invalid flag 0x%x, want 0x%x", data[0], flagArray&flagInternalNode)
	}

	// Decode number of child headers
	headerCount := binary.BigEndian.Uint32(data[1:5])

	if len(data) != metaDataSlabPrefixSize+16*int(headerCount) {
		return nil, fmt.Errorf("data has unexpected length %d, want %d", len(data), metaDataSlabPrefixSize+16*headerCount)
	}

	off := 5

	// Decode child headers
	headers := make([]*SlabHeader, headerCount)
	totalCount := uint32(0)
	for i := 0; i < int(headerCount); i++ {
		id := binary.BigEndian.Uint64(data[off : off+8])
		count := binary.BigEndian.Uint32(data[off+8 : off+12])
		size := binary.BigEndian.Uint32(data[off+12 : off+16])

		headers[i] = &SlabHeader{id: StorageID(id), count: count, size: size}

		totalCount += count

		off += 16
	}

	return &ArrayMetaDataSlab{
		header:         &SlabHeader{id: slabID, size: uint32(len(data)), count: totalCount},
		orderedHeaders: headers,
	}, nil
}

// TODO: make this function inline
func (a *ArrayMetaDataSlab) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	err := a.Encode(enc)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Internal node header (5 bytes):
//      | slab flag (1 bytes) | child header count (4 bytes) |
// Internal node content (n * 16 bytes):
// 	[[count, size, storage id], ...]
func (a *ArrayMetaDataSlab) Encode(enc *Encoder) error {

	// Encode flag
	enc.scratch[0] = flagArray | flagInternalNode

	// Encode child header count
	binary.BigEndian.PutUint32(enc.scratch[1:], uint32(len(a.orderedHeaders)))

	enc.Write(enc.scratch[:5])

	// Encode child headers
	for _, h := range a.orderedHeaders {
		binary.BigEndian.PutUint64(enc.scratch[:], uint64(h.id))
		binary.BigEndian.PutUint32(enc.scratch[8:], h.count)
		binary.BigEndian.PutUint32(enc.scratch[12:], h.size)
		enc.Write(enc.scratch[:16])
	}

	return nil
}

func (a *ArrayMetaDataSlab) ShallowCloneWithNewID() ArrayNode {
	copied := newArrayMetaDataSlab()
	copied.header.size = a.header.size
	copied.header.count = a.header.count
	copied.orderedHeaders = a.orderedHeaders

	return copied
}

func (a *ArrayMetaDataSlab) Get(storage SlabStorage, index uint64) (Storable, error) {

	if index >= uint64(a.header.count) {
		return nil, IndexOutOfRangeError{}
	}

	// Find child slab containing the element at given index.
	// index is decremented by each child slab's element count.
	// When decremented index is less than the element count,
	// index is already adjusted to that slab's index range.
	var id StorageID
	for _, h := range a.orderedHeaders {
		if index < uint64(h.count) {
			id = h.id
			break
		}
		index -= uint64(h.count)
	}

	node, err := getArrayNodeFromStorageID(storage, id)
	if err != nil {
		return nil, err
	}

	return node.Get(storage, index)
}

func (a *ArrayMetaDataSlab) Set(storage SlabStorage, index uint64, v Storable) error {

	if index >= uint64(a.header.count) {
		return IndexOutOfRangeError{}
	}

	// Find child slab containing the element at given index.
	// index is decremented by each child slab's element count.
	// When decremented index is less than the element count,
	// index is already adjusted to that slab's index range.
	var id StorageID
	var headerIndex int
	for i, h := range a.orderedHeaders {
		if index < uint64(h.count) {
			id = h.id
			headerIndex = i
			break
		}
		index -= uint64(h.count)
	}

	node, err := getArrayNodeFromStorageID(storage, id)
	if err != nil {
		return err
	}

	err = node.Set(storage, index, v)
	if err != nil {
		return err
	}

	a.orderedHeaders[headerIndex] = node.Header()

	if node.IsFull() {
		return a.SplitChildNode(storage, node, headerIndex)
	}

	if underflowSize, underflow := node.IsUnderflow(); underflow {
		return a.MergeOrRebalanceChildNode(storage, node, headerIndex, underflowSize)
	}

	storage.Store(a.header.id, a)

	return nil
}

// Insert inserts v into correct ArrayDataSlab.
// index must be >=0 and <= a.header.count.
// If index == a.header.count, Insert appends v to the end of underlying data slab.
func (a *ArrayMetaDataSlab) Insert(storage SlabStorage, index uint64, v Storable) error {
	if index > uint64(a.header.count) {
		return IndexOutOfRangeError{}
	}

	if len(a.orderedHeaders) == 0 {
		panic("Inserting to empty MetaDataSlab")
	}

	var id StorageID
	var headerIndex int
	if index == uint64(a.header.count) {
		headerIndex = len(a.orderedHeaders) - 1
		h := a.orderedHeaders[headerIndex]
		id = h.id
		index = uint64(h.count)
	} else {
		// Find child slab containing the element at given index.
		// index is decremented by each child slab's element count.
		// When decremented index is less than the element count,
		// index is already adjusted to that slab's index range.
		for i, h := range a.orderedHeaders {
			if index < uint64(h.count) {
				id = h.id
				headerIndex = i
				break
			}
			index -= uint64(h.count)
		}
	}

	node, err := getArrayNodeFromStorageID(storage, id)
	if err != nil {
		return err
	}

	err = node.Insert(storage, index, v)
	if err != nil {
		return err
	}

	a.orderedHeaders[headerIndex] = node.Header()
	a.header.count++

	if node.IsFull() {
		return a.SplitChildNode(storage, node, headerIndex)
	}

	storage.Store(a.header.id, a)

	return nil
}

func (a *ArrayMetaDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {

	if index >= uint64(a.header.count) {
		return nil, IndexOutOfRangeError{}
	}

	// Find child slab containing the element at given index.
	// index is decremented by each child slab's element count.
	// When decremented index is less than the element count,
	// index is already adjusted to that slab's index range.
	var id StorageID
	var headerIndex int
	for i, h := range a.orderedHeaders {
		if index < uint64(h.count) {
			id = h.id
			headerIndex = i
			break
		}
		index -= uint64(h.count)
	}

	node, err := getArrayNodeFromStorageID(storage, id)
	if err != nil {
		return nil, err
	}

	v, err := node.Remove(storage, index)
	if err != nil {
		return nil, err
	}

	a.orderedHeaders[headerIndex] = node.Header()
	a.header.count--

	if underflowSize, isUnderflow := node.IsUnderflow(); isUnderflow {
		err = a.MergeOrRebalanceChildNode(storage, node, headerIndex, underflowSize)
		if err != nil {
			return nil, err
		}
	}

	storage.Store(a.header.id, a)

	return v, nil
}

func (a *ArrayMetaDataSlab) SplitChildNode(storage SlabStorage, node ArrayNode, nodeHeaderIndex int) error {
	left, right, err := node.Split()
	if err != nil {
		return err
	}

	a.orderedHeaders = append(a.orderedHeaders, nil)
	if nodeHeaderIndex < len(a.orderedHeaders)-2 {
		copy(a.orderedHeaders[nodeHeaderIndex+2:], a.orderedHeaders[nodeHeaderIndex+1:])
	}
	a.orderedHeaders[nodeHeaderIndex] = left.Header()
	a.orderedHeaders[nodeHeaderIndex+1] = right.Header()

	a.header.size += headerSize

	storage.Store(left.Header().id, left)
	storage.Store(right.Header().id, right)
	storage.Store(a.header.id, a)

	return nil
}

// MergeOrRebalanceChildNode merges or rebalances child node.  If merged, then
// parent node's data is adjusted.
// +-----------------------+-----------------------+----------------------+-----------------------+
// |			   | no left sibling (sib) | left sib can't lend  | left sib can lend     |
// +=======================+=======================+======================+=======================+
// | no right sib          | panic                 | merge with left      | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can't lend  | merge with right      | merge with smaller   | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can lend    | rebalance with right  | rebalance with right | rebalance with bigger |
// +-----------------------+-----------------------+----------------------+-----------------------+
func (a *ArrayMetaDataSlab) MergeOrRebalanceChildNode(storage SlabStorage, node ArrayNode, nodeHeaderIndex int, underflowSize uint32) error {

	// left and right siblings of the same parent.
	var leftSib, rightSib ArrayNode
	if nodeHeaderIndex > 0 {
		leftSibID := a.orderedHeaders[nodeHeaderIndex-1].id

		var err error
		leftSib, err = getArrayNodeFromStorageID(storage, leftSibID)
		if err != nil {
			return err
		}
		a.orderedHeaders[nodeHeaderIndex-1] = leftSib.Header()
	}
	if nodeHeaderIndex < len(a.orderedHeaders)-1 {
		rightSibID := a.orderedHeaders[nodeHeaderIndex+1].id

		var err error
		rightSib, err = getArrayNodeFromStorageID(storage, rightSibID)
		if err != nil {
			return err
		}
		a.orderedHeaders[nodeHeaderIndex+1] = rightSib.Header()
	}

	leftCanLend := leftSib != nil && leftSib.CanLendToRight(underflowSize)
	rightCanLend := rightSib != nil && rightSib.CanLendToLeft(underflowSize)

	// Node can rebalance elements with at least one sibling.
	// Parent node doesn't need to be modified for rebalancing.
	if leftCanLend || rightCanLend {

		// Rebalance with right sib
		if !leftCanLend {
			err := node.BorrowFromRight(rightSib)
			if err != nil {
				return err
			}
			storage.Store(node.ID(), node)
			storage.Store(rightSib.ID(), rightSib)
			storage.Store(a.header.id, a)
			return nil
		}

		// Rebalance with left sib
		if !rightCanLend {
			err := leftSib.LendToRight(node)
			if err != nil {
				return err
			}
			storage.Store(leftSib.ID(), leftSib)
			storage.Store(node.ID(), node)
			storage.Store(a.header.id, a)
			return nil
		}

		// Rebalance with bigger sib
		if leftSib.Header().size > rightSib.Header().size {
			err := leftSib.LendToRight(node)
			if err != nil {
				return err
			}
			storage.Store(leftSib.ID(), leftSib)
			storage.Store(node.ID(), node)
			storage.Store(a.header.id, a)
			return nil
		}

		err := node.BorrowFromRight(rightSib)
		if err != nil {
			return err
		}
		storage.Store(node.ID(), node)
		storage.Store(rightSib.ID(), rightSib)
		storage.Store(a.header.id, a)
		return nil
	}

	// Node can't rebalance with any sibling.  It must merge with one sibling.
	// Parent node needs to be modified for merging.

	if leftSib == nil {

		// Merge with right
		err := node.Merge(rightSib)
		if err != nil {
			return err
		}

		// Update MetaDataSlab
		copy(a.orderedHeaders[nodeHeaderIndex+1:], a.orderedHeaders[nodeHeaderIndex+2:])
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

		a.header.size -= headerSize

		storage.Store(node.ID(), node)
		storage.Store(a.header.id, a)

		// Remove right sib from SlabStorage
		storage.Remove(rightSib.Header().id)

		return nil
	}

	if rightSib == nil {

		// Merge with left
		err := leftSib.Merge(node)
		if err != nil {
			return err
		}

		// Update MetaDataSlab
		copy(a.orderedHeaders[nodeHeaderIndex:], a.orderedHeaders[nodeHeaderIndex+1:])
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

		a.header.size -= headerSize

		storage.Store(leftSib.ID(), leftSib)
		storage.Store(a.header.id, a)

		// Remove node from SlabStorage
		storage.Remove(node.Header().id)

		return nil
	}

	// Merge with smaller sib
	if leftSib.Header().size < rightSib.Header().size {
		err := leftSib.Merge(node)
		if err != nil {
			return err
		}

		// Update MetaDataSlab
		copy(a.orderedHeaders[nodeHeaderIndex:], a.orderedHeaders[nodeHeaderIndex+1:])
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

		a.header.size -= headerSize

		storage.Store(leftSib.ID(), leftSib)
		storage.Store(a.header.id, a)

		// Remove node from SlabStorage
		storage.Remove(node.Header().id)

		return nil
	}

	err := node.Merge(rightSib)
	if err != nil {
		return err
	}

	// Update MetaDataSlab
	copy(a.orderedHeaders[nodeHeaderIndex+1:], a.orderedHeaders[nodeHeaderIndex+2:])
	a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]

	a.header.size -= headerSize

	storage.Store(node.ID(), node)
	storage.Store(a.header.id, a)

	// Remove rightSib from SlabStorage
	storage.Remove(rightSib.Header().id)

	return nil
}

func (a *ArrayMetaDataSlab) Merge(slab Slab) error {
	meta2 := slab.(*ArrayMetaDataSlab)
	a.orderedHeaders = append(a.orderedHeaders, meta2.orderedHeaders...)
	a.header.size += meta2.header.size - metaDataSlabPrefixSize
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

	right := newArrayMetaDataSlab()
	right.orderedHeaders = make([]*SlabHeader, len(a.orderedHeaders)-rightSlabStartIndex)
	copy(right.orderedHeaders, a.orderedHeaders[rightSlabStartIndex:])
	right.header.count = a.header.count - leftSlabCount
	right.header.size = a.header.size - uint32(leftSlabSize)

	a.orderedHeaders = a.orderedHeaders[:rightSlabStartIndex]
	a.header.count = leftSlabCount
	a.header.size = metaDataSlabPrefixSize + uint32(leftSlabSize)

	return a, right, nil
}

func (a *ArrayMetaDataSlab) LendToRight(slab Slab) error {
	b := slab.(*ArrayMetaDataSlab)

	headerLen := len(a.orderedHeaders) + len(b.orderedHeaders)
	leftSlabHeaderLen := headerLen / 2
	rightSlabHeaderLen := headerLen - leftSlabHeaderLen

	// Prepend some headers to right (b) from left (a)

	// Update slab b elements
	rElements := make([]*SlabHeader, rightSlabHeaderLen)
	n := copy(rElements, a.orderedHeaders[leftSlabHeaderLen:])
	copy(rElements[n:], b.orderedHeaders)
	b.orderedHeaders = rElements

	// Update slab a elements
	a.orderedHeaders = a.orderedHeaders[:leftSlabHeaderLen]

	// Update slab a header
	a.header.count = 0
	for i := 0; i < len(a.orderedHeaders); i++ {
		a.header.count += a.orderedHeaders[i].count
	}
	a.header.size = metaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*headerSize

	// Update slab b header
	b.header.count = 0
	for i := 0; i < len(b.orderedHeaders); i++ {
		b.header.count += b.orderedHeaders[i].count
	}
	b.header.size = metaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*headerSize

	return nil
}

func (a *ArrayMetaDataSlab) BorrowFromRight(slab Slab) error {
	b := slab.(*ArrayMetaDataSlab)

	headerLen := len(a.orderedHeaders) + len(b.orderedHeaders)
	leftSlabHeaderLen := headerLen / 2
	rightSlabHeaderLen := headerLen - leftSlabHeaderLen

	// Append some headers to left (a) from right (b)

	// Update slab a elements
	a.orderedHeaders = append(a.orderedHeaders, b.orderedHeaders[:leftSlabHeaderLen-len(a.orderedHeaders)]...)

	// Update slab b elements
	b.orderedHeaders = b.orderedHeaders[len(b.orderedHeaders)-rightSlabHeaderLen:]

	// Update slab a header
	a.header.count = 0
	for i := 0; i < len(a.orderedHeaders); i++ {
		a.header.count += a.orderedHeaders[i].count
	}
	a.header.size = metaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*headerSize

	// Update slab b header
	b.header.count = 0
	for i := 0; i < len(b.orderedHeaders); i++ {
		b.header.count += b.orderedHeaders[i].count
	}
	b.header.size = metaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*headerSize

	return nil
}

func (a ArrayMetaDataSlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

func (a ArrayMetaDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > a.header.size {
		return uint32(minThreshold) - a.header.size, true
	}
	return 0, false
}

func (a *ArrayMetaDataSlab) CanLendToLeft(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / headerSize))
	return a.header.size-headerSize*n > uint32(minThreshold)
}

func (a *ArrayMetaDataSlab) CanLendToRight(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / headerSize))
	return a.header.size-headerSize*n > uint32(minThreshold)
}

func (a ArrayMetaDataSlab) IsLeaf() bool {
	return false
}

func (a *ArrayMetaDataSlab) Header() *SlabHeader {
	return a.header
}

func (a *ArrayMetaDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *ArrayMetaDataSlab) Mutable() bool {
	return true
}

func (a *ArrayMetaDataSlab) ID() StorageID {
	return a.header.id
}

func (a *ArrayMetaDataSlab) String() string {
	var elemsStr []string
	for _, h := range a.orderedHeaders {
		elemsStr = append(elemsStr, fmt.Sprintf("%+v", *h))
	}
	return strings.Join(elemsStr, " ")
}

func NewArray(storage SlabStorage) *Array {
	root := newArrayDataSlab()

	storage.Store(root.header.id, root)

	return &Array{
		storage:           storage,
		root:              root,
		dataSlabStorageID: root.header.id,
	}
}

func NewArrayWithRootID(storage SlabStorage, rootID StorageID) (*Array, error) {
	root, err := getArrayNodeFromStorageID(storage, rootID)
	if err != nil {
		return nil, err
	}
	return &Array{storage: storage, root: root}, nil
}

func (array *Array) Get(i uint64) (Storable, error) {
	v, err := array.root.Get(array.storage, i)
	if err != nil {
		return nil, err
	}
	// TODO: optimize this
	if idValue, ok := v.(StorageIDValue); ok {
		id := StorageID(idValue)
		if id == StorageIDUndefined {
			return nil, fmt.Errorf("invalid storage id")
		}
		slab, found, err := array.storage.Retrieve(id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("slab %d not found", id)
		}
		return slab, nil
	}
	return v, nil
}

func (array *Array) Set(index uint64, v Storable) error {
	if v.Mutable() || v.ByteSize() > uint32(maxInlineElementSize) {
		v = StorageIDValue(v.ID())
	}
	return array.root.Set(array.storage, index, v)
}

func (array *Array) Append(v Storable) error {
	return array.Insert(array.Count(), v)
}

func (array *Array) Insert(index uint64, v Storable) error {
	if v.Mutable() || v.ByteSize() > uint32(maxInlineElementSize) {
		v = StorageIDValue(v.ID())
	}

	err := array.root.Insert(array.storage, index, v)
	if err != nil {
		return err
	}

	if array.root.IsFull() {

		copiedRoot := array.root.ShallowCloneWithNewID()

		// Split copied root node
		left, right, err := copiedRoot.Split()
		if err != nil {
			return err
		}

		array.storage.Store(left.Header().id, left)
		array.storage.Store(right.Header().id, right)

		if array.root.IsLeaf() {

			// Create new ArrayMetaDataSlab with the same storage ID as root
			array.root = &ArrayMetaDataSlab{
				header: &SlabHeader{
					id: array.root.Header().id,
				},
			}

			array.dataSlabStorageID = left.Header().id
		}

		root := array.root.(*ArrayMetaDataSlab)
		root.orderedHeaders = []*SlabHeader{left.Header(), right.Header()}
		root.header.count = left.Header().count + right.Header().count
		root.header.size = metaDataSlabPrefixSize + headerSize*uint32(len(root.orderedHeaders))

		array.storage.Store(array.root.Header().id, array.root)
	}

	return nil
}

func (array *Array) Remove(index uint64) (Storable, error) {
	v, err := array.root.Remove(array.storage, index)
	if err != nil {
		return nil, err
	}

	if !array.root.IsLeaf() {
		// Set root to its child node if there is only one child node left.
		root := array.root.(*ArrayMetaDataSlab)
		if len(root.orderedHeaders) == 1 {

			childID := root.orderedHeaders[0].id

			node, err := getArrayNodeFromStorageID(array.storage, childID)
			if err != nil {
				return nil, err
			}

			oldRootID := root.header.id

			node.Header().id = oldRootID

			array.storage.Store(oldRootID, node)

			array.storage.Remove(childID)

			array.root = node

			if _, ok := array.root.(*ArrayDataSlab); ok {
				array.dataSlabStorageID = oldRootID
			}

			//array.storage.Store(array.root.ID(), array.root)
		}
	}

	return v, nil
}

func (array *Array) Iterate(fn func(Storable)) error {
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

func (array *Array) Count() uint64 {
	return uint64(array.root.Header().count)
}

func (array *Array) String() string {
	if array.root.IsLeaf() {
		return array.root.String()
	}
	meta := array.root.(*ArrayMetaDataSlab)
	return array.string(meta)
}

func (array *Array) string(meta *ArrayMetaDataSlab) string {
	var elemsStr []string

	for _, h := range meta.orderedHeaders {
		node, err := getArrayNodeFromStorageID(array.storage, h.id)
		if err != nil {
			return err.Error()
		}
		if node.IsLeaf() {
			leaf := node.(*ArrayDataSlab)
			elemsStr = append(elemsStr, leaf.String())
		} else {
			meta := node.(*ArrayMetaDataSlab)
			elemsStr = append(elemsStr, array.string(meta))
		}
	}
	return strings.Join(elemsStr, " ")
}

func getArrayNodeFromStorageID(storage SlabStorage, id StorageID) (ArrayNode, error) {
	if id == StorageIDUndefined {
		return nil, fmt.Errorf("invalid storage id")
	}
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("slab %d not found", id)
	}
	node, ok := slab.(ArrayNode)
	if !ok {
		return nil, fmt.Errorf("slab %d is not ArrayNode", id)
	}
	return node, nil
}
