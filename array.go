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
	slabHeaderSize = 8 + 4 + 4

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + child header count (2 bytes)
	metaDataSlabPrefixSize = 1 + 1 + 2

	// version (1 byte) + flag (1 byte) + prev id (8 bytes) + next id (8 bytes) + CBOR array size (3 bytes)
	// (3 bytes of array size support up to 65535 array elements)
	dataSlabPrefixSize = 2 + 8 + 8 + 3
)

type Slab interface {
	Storable

	// Bytes is a wrapper for Storable.Encode()
	Bytes() ([]byte, error)

	Header() SlabHeader

	Split() (Slab, Slab, error)
	Merge(Slab) error
	// LendToRight rebalances slabs by moving elements from left to right
	LendToRight(Slab) error
	// BorrowFromRight rebalances slabs by moving elements from right to left
	BorrowFromRight(Slab) error
}

type SlabHeader struct {
	id    StorageID // id is used to retrieve slab from storage
	size  uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count uint32    // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

// ArrayDataSlab is leaf node, implementing ArraySlab.
type ArrayDataSlab struct {
	prev     StorageID
	next     StorageID
	header   SlabHeader
	elements []Storable
}

// ArrayMetaDataSlab is internal node, implementing ArraySlab.
type ArrayMetaDataSlab struct {
	header          SlabHeader
	childrenHeaders []SlabHeader
}

type ArraySlab interface {
	Get(storage SlabStorage, index uint64) (Storable, error)
	Set(storage SlabStorage, index uint64, v Storable) error
	Insert(storage SlabStorage, index uint64, v Storable) error
	Remove(storage SlabStorage, index uint64) (Storable, error)

	ShallowCloneWithNewID() ArraySlab

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetID(StorageID)

	Slab
}

// Array is tree
type Array struct {
	storage SlabStorage
	root    ArraySlab
}

type IndexOutOfRangeError struct {
	// TODO: add more info
}

func (e IndexOutOfRangeError) Error() string {
	// TODO: add more info
	return "index out of range"
}

type ArraySlabNotFoundError struct {
	id  StorageID
	err error
}

func (e ArraySlabNotFoundError) Error() string {
	return fmt.Sprintf("failed to retrieve ArraySlab %d: %v", e.id, e.err)
}

func newArrayDataSlab() *ArrayDataSlab {
	return &ArrayDataSlab{
		header: SlabHeader{
			id:   generateStorageID(),
			size: dataSlabPrefixSize,
		},
	}
}

func newArrayDataSlabFromData(id StorageID, data []byte) (*ArrayDataSlab, error) {

	if len(data) < dataSlabPrefixSize {
		return nil, errors.New("data is too short for array data slab")
	}

	// Check flag
	if data[1]&flagArray == 0 || data[1]&flagDataSlab == 0 {
		return nil, fmt.Errorf("data has invalid flag 0x%x, want 0x%x", data[0], flagArray&flagDataSlab)
	}

	// Decode prev storage id
	prev := binary.BigEndian.Uint64(data[2:10])

	// Decode next storage id
	next := binary.BigEndian.Uint64(data[10:18])

	cborDec := NewByteStreamDecoder(data[18:])

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
		header:   SlabHeader{id: id, size: uint32(len(data)), count: uint32(elemCount)},
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

// header (18 bytes):
//   | slab version + flag (2 bytes) | prev sib storage ID (8 bytes) | next sib storage ID (8 bytes) |
// content (for now):
//   CBOR encoded array of elements
func (a *ArrayDataSlab) Encode(enc *Encoder) error {

	// Encode version
	enc.scratch[0] = 0

	// Encode flag
	enc.scratch[1] = flagDataSlab | flagArray

	// Encode prev storage id
	binary.BigEndian.PutUint64(enc.scratch[2:], uint64(a.prev))

	// Encode next storage id
	binary.BigEndian.PutUint64(enc.scratch[10:], uint64(a.next))

	// Encode CBOR array size manually for fix-sized encoding
	enc.scratch[18] = 0x80 | 25
	binary.BigEndian.PutUint16(enc.scratch[19:], uint16(len(a.elements)))

	enc.Write(enc.scratch[:21])

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

func (a *ArrayDataSlab) ShallowCloneWithNewID() ArraySlab {
	return &ArrayDataSlab{
		header: SlabHeader{
			id:    generateStorageID(),
			size:  a.header.size,
			count: a.header.count,
		},
		elements: a.elements,
		prev:     a.prev,
		next:     a.next,
	}
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
		return nil, nil, fmt.Errorf("can't split slab with less than 2 elements")
	}

	// This computes the ceil of split to give the first slab with more elements.
	dataSize := a.header.size - dataSlabPrefixSize
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, e := range a.elements {
		elemSize := e.ByteSize()
		if leftSize+elemSize >= midPoint {
			// i is mid point element.  Place i on the small side.
			if leftSize <= dataSize-leftSize-elemSize {
				leftSize += elemSize
				leftCount = i + 1
			} else {
				leftCount = i
			}
			break
		}
		// left slab size < midPoint
		leftSize += elemSize
	}

	// Construct right slab
	rightSlab := &ArrayDataSlab{
		header: SlabHeader{
			id:    generateStorageID(),
			size:  dataSlabPrefixSize + dataSize - leftSize,
			count: uint32(len(a.elements) - leftCount),
		},
		prev: a.header.id,
		next: a.next,
	}

	rightSlab.elements = make([]Storable, len(a.elements)-leftCount)
	copy(rightSlab.elements, a.elements[leftCount:])

	// Modify left (original) slab
	a.elements = a.elements[:leftCount]
	a.header.size = dataSlabPrefixSize + leftSize
	a.header.count = uint32(leftCount)
	a.next = rightSlab.header.id

	return a, rightSlab, nil
}

func (a *ArrayDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*ArrayDataSlab)
	a.elements = append(a.elements, rightSlab.elements...)
	a.header.size = a.header.size + rightSlab.header.size - dataSlabPrefixSize
	a.header.count += rightSlab.header.count
	a.next = rightSlab.next
	return nil
}

// LendToRight rebalances slabs by moving elements from left slab to right slab
func (a *ArrayDataSlab) LendToRight(slab Slab) error {

	rightSlab := slab.(*ArrayDataSlab)

	count := a.header.count + rightSlab.header.count
	size := a.header.size + rightSlab.header.size

	leftCount := a.header.count
	leftSize := a.header.size

	midPoint := (size + 1) >> 1

	// Left slab size is as close to midPoint as possible while right slab size >= minThreshold
	for i := len(a.elements) - 1; i >= 0; i-- {
		elemSize := a.elements[i].ByteSize()
		if leftSize-elemSize < midPoint && size-leftSize >= uint32(minThreshold) {
			break
		}
		leftSize -= elemSize
		leftCount--
	}

	// Update right slab
	elements := make([]Storable, count-leftCount)
	n := copy(elements, a.elements[leftCount:])
	copy(elements[n:], rightSlab.elements)

	rightSlab.elements = elements
	rightSlab.header.size = size - leftSize
	rightSlab.header.count = count - leftCount

	// Update left slab
	a.elements = a.elements[:leftCount]
	a.header.size = leftSize
	a.header.count = leftCount

	return nil
}

// BorrowFromRight rebalances slabs by moving elements from right slab to left slab.
func (a *ArrayDataSlab) BorrowFromRight(slab Slab) error {
	rightSlab := slab.(*ArrayDataSlab)

	count := a.header.count + rightSlab.header.count
	size := a.header.size + rightSlab.header.size

	leftCount := a.header.count
	leftSize := a.header.size

	midPoint := (size + 1) >> 1

	for _, e := range rightSlab.elements {
		elemSize := e.ByteSize()
		if leftSize+elemSize > midPoint {
			if size-leftSize-elemSize >= uint32(minThreshold) {
				// Include this element in left slab
				leftSize += elemSize
				leftCount++
			}
			break
		}
		leftSize += elemSize
		leftCount++
	}

	rightStartIndex := leftCount - a.header.count

	// Update left slab
	a.elements = append(a.elements, rightSlab.elements[:rightStartIndex]...)
	a.header.size = leftSize
	a.header.count = leftCount

	// Update right slab
	rightSlab.elements = rightSlab.elements[rightStartIndex:]
	rightSlab.header.size = size - leftSize
	rightSlab.header.count = count - leftCount

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

func (a *ArrayDataSlab) SetID(id StorageID) {
	a.header.id = id
}

func (a *ArrayDataSlab) Header() SlabHeader {
	return a.header
}

func (a *ArrayDataSlab) IsData() bool {
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

func newArrayMetaDataSlabFromData(id StorageID, data []byte) (*ArrayMetaDataSlab, error) {
	if len(data) < metaDataSlabPrefixSize {
		return nil, errors.New("data is too short for array metadata slab")
	}

	// Check flag
	if data[1]&flagArray == 0 || data[1]&flagMetaDataSlab == 0 {
		return nil, fmt.Errorf("data has invalid flag 0x%x, want 0x%x", data[0], flagArray&flagMetaDataSlab)
	}

	// Decode number of child headers
	headerCount := binary.BigEndian.Uint16(data[2:4])

	if len(data) != metaDataSlabPrefixSize+16*int(headerCount) {
		return nil, fmt.Errorf("data has unexpected length %d, want %d", len(data), metaDataSlabPrefixSize+16*headerCount)
	}

	off := 4

	// Decode child headers
	headers := make([]SlabHeader, headerCount)
	totalCount := uint32(0)
	for i := 0; i < int(headerCount); i++ {
		id := binary.BigEndian.Uint64(data[off : off+8])
		count := binary.BigEndian.Uint32(data[off+8 : off+12])
		size := binary.BigEndian.Uint32(data[off+12 : off+16])

		headers[i] = SlabHeader{id: StorageID(id), count: count, size: size}

		totalCount += count

		off += 16
	}

	return &ArrayMetaDataSlab{
		header:          SlabHeader{id: id, size: uint32(len(data)), count: totalCount},
		childrenHeaders: headers,
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

// header (4 bytes):
//      | slab version (1 bytes) | slab flag (1 bytes) | child header count (2 bytes) |
// content (n * 16 bytes):
// 	[[count, size, storage id], ...]
func (a *ArrayMetaDataSlab) Encode(enc *Encoder) error {

	// Encode version
	enc.scratch[0] = 0

	// Encode flag
	enc.scratch[1] = flagArray | flagMetaDataSlab

	// Encode child header count
	binary.BigEndian.PutUint16(enc.scratch[2:], uint16(len(a.childrenHeaders)))

	enc.Write(enc.scratch[:4])

	// Encode children headers
	for _, h := range a.childrenHeaders {
		binary.BigEndian.PutUint64(enc.scratch[:], uint64(h.id))
		binary.BigEndian.PutUint32(enc.scratch[8:], h.count)
		binary.BigEndian.PutUint32(enc.scratch[12:], h.size)
		enc.Write(enc.scratch[:16])
	}

	return nil
}

func (a *ArrayMetaDataSlab) ShallowCloneWithNewID() ArraySlab {
	return &ArrayMetaDataSlab{
		header: SlabHeader{
			id:    generateStorageID(),
			size:  a.header.size,
			count: a.header.count,
		},
		childrenHeaders: a.childrenHeaders,
	}
}

func (a *ArrayMetaDataSlab) Get(storage SlabStorage, index uint64) (Storable, error) {

	if index >= uint64(a.header.count) {
		return nil, IndexOutOfRangeError{}
	}

	// Find child slab containing the element at given index.
	// index is decremented by each child slab's element count.
	// When decremented index is less than the element count,
	// index is already adjusted to that slab's index range.
	var childID StorageID
	for _, h := range a.childrenHeaders {
		if index < uint64(h.count) {
			childID = h.id
			break
		}
		index -= uint64(h.count)
	}

	childSlab, _, err := storage.Retrieve(childID)

	if child, ok := childSlab.(ArraySlab); ok {
		return child.Get(storage, index)
	}

	return nil, ArraySlabNotFoundError{childID, err}
}

func (a *ArrayMetaDataSlab) Set(storage SlabStorage, index uint64, v Storable) error {

	if index >= uint64(a.header.count) {
		return IndexOutOfRangeError{}
	}

	// Find child slab containing the element at given index.
	// index is decremented by each child slab's element count.
	// When decremented index is less than the element count,
	// index is already adjusted to that slab's index range.
	var childID StorageID
	var childHeaderIndex int
	for i, h := range a.childrenHeaders {
		if index < uint64(h.count) {
			childID = h.id
			childHeaderIndex = i
			break
		}
		index -= uint64(h.count)
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		return err
	}

	err = child.Set(storage, index, v)
	if err != nil {
		return err
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	if child.IsFull() {
		return a.SplitChildSlab(storage, child, childHeaderIndex)
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		return a.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
	}

	storage.Store(a.header.id, a)

	return nil
}

// Insert inserts v into the correct child slab.
// index must be >=0 and <= a.header.count.
// If index == a.header.count, Insert appends v to the end of underlying slab.
func (a *ArrayMetaDataSlab) Insert(storage SlabStorage, index uint64, v Storable) error {
	if index > uint64(a.header.count) {
		return IndexOutOfRangeError{}
	}

	if len(a.childrenHeaders) == 0 {
		panic("Inserting to empty MetaDataSlab")
	}

	var childID StorageID
	var childHeaderIndex int
	if index == uint64(a.header.count) {
		childHeaderIndex = len(a.childrenHeaders) - 1
		h := a.childrenHeaders[childHeaderIndex]
		childID = h.id
		index = uint64(h.count)
	} else {
		// Find child slab containing the element at given index.
		// index is decremented by each child slab's element count.
		// When decremented index is less than the element count,
		// index is already adjusted to that slab's index range.
		for i, h := range a.childrenHeaders {
			if index < uint64(h.count) {
				childID = h.id
				childHeaderIndex = i
				break
			}
			index -= uint64(h.count)
		}
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		return err
	}

	err = child.Insert(storage, index, v)
	if err != nil {
		return err
	}

	a.header.count++

	a.childrenHeaders[childHeaderIndex] = child.Header()

	if child.IsFull() {
		return a.SplitChildSlab(storage, child, childHeaderIndex)
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
	var childID StorageID
	var childHeaderIndex int
	for i, h := range a.childrenHeaders {
		if index < uint64(h.count) {
			childID = h.id
			childHeaderIndex = i
			break
		}
		index -= uint64(h.count)
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		return nil, err
	}

	v, err := child.Remove(storage, index)
	if err != nil {
		return nil, err
	}

	a.header.count--

	a.childrenHeaders[childHeaderIndex] = child.Header()

	if underflowSize, isUnderflow := child.IsUnderflow(); isUnderflow {
		err = a.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			return nil, err
		}
	}

	storage.Store(a.header.id, a)

	return v, nil
}

func (a *ArrayMetaDataSlab) SplitChildSlab(storage SlabStorage, child ArraySlab, childHeaderIndex int) error {
	left, right, err := child.Split()
	if err != nil {
		return err
	}

	// Add new child slab (right) to childrenHeaders
	a.childrenHeaders = append(a.childrenHeaders, SlabHeader{})
	if childHeaderIndex < len(a.childrenHeaders)-2 {
		copy(a.childrenHeaders[childHeaderIndex+2:], a.childrenHeaders[childHeaderIndex+1:])
	}
	a.childrenHeaders[childHeaderIndex] = left.Header()
	a.childrenHeaders[childHeaderIndex+1] = right.Header()

	// Increase header size
	a.header.size += slabHeaderSize

	// Store modified slabs
	storage.Store(left.Header().id, left)
	storage.Store(right.Header().id, right)
	storage.Store(a.header.id, a)

	return nil
}

// MergeOrRebalanceChildSlab merges or rebalances child slab.  If merged, then
// parent slab's data is adjusted.
// +-----------------------+-----------------------+----------------------+-----------------------+
// |			   | no left sibling (sib) | left sib can't lend  | left sib can lend     |
// +=======================+=======================+======================+=======================+
// | no right sib          | panic                 | merge with left      | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can't lend  | merge with right      | merge with smaller   | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can lend    | rebalance with right  | rebalance with right | rebalance with bigger |
// +-----------------------+-----------------------+----------------------+-----------------------+
func (a *ArrayMetaDataSlab) MergeOrRebalanceChildSlab(storage SlabStorage, child ArraySlab, childHeaderIndex int, underflowSize uint32) error {

	// Retrieve left and right siblings of the same parent.
	var leftSib, rightSib ArraySlab
	if childHeaderIndex > 0 {
		leftSibID := a.childrenHeaders[childHeaderIndex-1].id

		var err error
		leftSib, err = getArraySlab(storage, leftSibID)
		if err != nil {
			return err
		}
	}
	if childHeaderIndex < len(a.childrenHeaders)-1 {
		rightSibID := a.childrenHeaders[childHeaderIndex+1].id

		var err error
		rightSib, err = getArraySlab(storage, rightSibID)
		if err != nil {
			return err
		}
	}

	leftCanLend := leftSib != nil && leftSib.CanLendToRight(underflowSize)
	rightCanLend := rightSib != nil && rightSib.CanLendToLeft(underflowSize)

	// Child can rebalance elements with at least one sibling.
	if leftCanLend || rightCanLend {

		// Rebalance with right sib
		if !leftCanLend {
			err := child.BorrowFromRight(rightSib)
			if err != nil {
				return err
			}

			a.childrenHeaders[childHeaderIndex] = child.Header()
			a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			storage.Store(child.ID(), child)
			storage.Store(rightSib.ID(), rightSib)
			storage.Store(a.header.id, a)
			return nil
		}

		// Rebalance with left sib
		if !rightCanLend {
			err := leftSib.LendToRight(child)
			if err != nil {
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			storage.Store(leftSib.ID(), leftSib)
			storage.Store(child.ID(), child)
			storage.Store(a.header.id, a)
			return nil
		}

		// Rebalance with bigger sib
		if leftSib.Header().size > rightSib.Header().size {
			err := leftSib.LendToRight(child)
			if err != nil {
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			storage.Store(leftSib.ID(), leftSib)
			storage.Store(child.ID(), child)
			storage.Store(a.header.id, a)
			return nil
		}

		err := child.BorrowFromRight(rightSib)
		if err != nil {
			return err
		}

		a.childrenHeaders[childHeaderIndex] = child.Header()
		a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

		storage.Store(child.ID(), child)
		storage.Store(rightSib.ID(), rightSib)
		storage.Store(a.header.id, a)
		return nil
	}

	// Child can't rebalance with any sibling.  It must merge with one sibling.

	if leftSib == nil {

		// Merge with right
		err := child.Merge(rightSib)
		if err != nil {
			return err
		}

		a.childrenHeaders[childHeaderIndex] = child.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(a.childrenHeaders[childHeaderIndex+1:], a.childrenHeaders[childHeaderIndex+2:])
		a.childrenHeaders = a.childrenHeaders[:len(a.childrenHeaders)-1]

		a.header.size -= slabHeaderSize

		// Store modified slabs in storage
		storage.Store(child.ID(), child)
		storage.Store(a.header.id, a)

		// Remove right sib from storage
		storage.Remove(rightSib.Header().id)

		return nil
	}

	if rightSib == nil {

		// Merge with left
		err := leftSib.Merge(child)
		if err != nil {
			return err
		}

		a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(a.childrenHeaders[childHeaderIndex:], a.childrenHeaders[childHeaderIndex+1:])
		a.childrenHeaders = a.childrenHeaders[:len(a.childrenHeaders)-1]

		a.header.size -= slabHeaderSize

		// Store modified slabs in storage
		storage.Store(leftSib.ID(), leftSib)
		storage.Store(a.header.id, a)

		// Remove child from storage
		storage.Remove(child.Header().id)

		return nil
	}

	// Merge with smaller sib
	if leftSib.Header().size < rightSib.Header().size {
		err := leftSib.Merge(child)
		if err != nil {
			return err
		}

		a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(a.childrenHeaders[childHeaderIndex:], a.childrenHeaders[childHeaderIndex+1:])
		a.childrenHeaders = a.childrenHeaders[:len(a.childrenHeaders)-1]

		a.header.size -= slabHeaderSize

		// Store modified slabs in storage
		storage.Store(leftSib.ID(), leftSib)
		storage.Store(a.header.id, a)

		// Remove child from storage
		storage.Remove(child.Header().id)

		return nil
	}

	err := child.Merge(rightSib)
	if err != nil {
		return err
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Update MetaDataSlab's childrenHeaders
	copy(a.childrenHeaders[childHeaderIndex+1:], a.childrenHeaders[childHeaderIndex+2:])
	a.childrenHeaders = a.childrenHeaders[:len(a.childrenHeaders)-1]

	a.header.size -= slabHeaderSize

	// Store modified slabs in storage
	storage.Store(child.ID(), child)
	storage.Store(a.header.id, a)

	// Remove rightSib from storage
	storage.Remove(rightSib.Header().id)

	return nil
}

func (a *ArrayMetaDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*ArrayMetaDataSlab)
	a.childrenHeaders = append(a.childrenHeaders, rightSlab.childrenHeaders...)
	a.header.size += rightSlab.header.size - metaDataSlabPrefixSize
	a.header.count += rightSlab.header.count
	return nil
}

func (a *ArrayMetaDataSlab) Split() (Slab, Slab, error) {

	if len(a.childrenHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		return nil, nil, fmt.Errorf("can't split meta slab with less than 2 headers")
	}

	leftChildrenCount := int(math.Ceil(float64(len(a.childrenHeaders)) / 2))
	leftSize := leftChildrenCount * slabHeaderSize

	leftCount := uint32(0)
	for i := 0; i < leftChildrenCount; i++ {
		leftCount += a.childrenHeaders[i].count
	}

	// Construct right slab
	rightSlab := &ArrayMetaDataSlab{
		header: SlabHeader{
			id:    generateStorageID(),
			size:  a.header.size - uint32(leftSize),
			count: a.header.count - leftCount,
		},
	}

	rightSlab.childrenHeaders = make([]SlabHeader, len(a.childrenHeaders)-leftChildrenCount)
	copy(rightSlab.childrenHeaders, a.childrenHeaders[leftChildrenCount:])

	// Modify left (original)slab
	a.childrenHeaders = a.childrenHeaders[:leftChildrenCount]
	a.header.count = leftCount
	a.header.size = metaDataSlabPrefixSize + uint32(leftSize)

	return a, rightSlab, nil
}

func (a *ArrayMetaDataSlab) LendToRight(slab Slab) error {
	rightSlab := slab.(*ArrayMetaDataSlab)

	childrenHeadersLen := len(a.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftChildrenHeadersLen := childrenHeadersLen / 2

	// Update right slab childrenHeaders by prepending borrowed children headers
	rightChildrenHeaders := make([]SlabHeader, childrenHeadersLen-leftChildrenHeadersLen)
	n := copy(rightChildrenHeaders, a.childrenHeaders[leftChildrenHeadersLen:])
	copy(rightChildrenHeaders[n:], rightSlab.childrenHeaders)
	rightSlab.childrenHeaders = rightChildrenHeaders

	// Update right slab header
	rightSlab.header.count = 0
	for i := 0; i < len(rightSlab.childrenHeaders); i++ {
		rightSlab.header.count += rightSlab.childrenHeaders[i].count
	}
	rightSlab.header.size = metaDataSlabPrefixSize + uint32(len(rightSlab.childrenHeaders))*slabHeaderSize

	// Update left slab (original)
	a.childrenHeaders = a.childrenHeaders[:leftChildrenHeadersLen]

	a.header.count = 0
	for i := 0; i < len(a.childrenHeaders); i++ {
		a.header.count += a.childrenHeaders[i].count
	}
	a.header.size = metaDataSlabPrefixSize + uint32(leftChildrenHeadersLen)*slabHeaderSize

	return nil
}

func (a *ArrayMetaDataSlab) BorrowFromRight(slab Slab) error {
	rightSlab := slab.(*ArrayMetaDataSlab)

	childrenHeadersLen := len(a.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftSlabHeaderLen := childrenHeadersLen / 2
	rightSlabHeaderLen := childrenHeadersLen - leftSlabHeaderLen

	// Update left slab (original)
	a.childrenHeaders = append(a.childrenHeaders, rightSlab.childrenHeaders[:leftSlabHeaderLen-len(a.childrenHeaders)]...)

	a.header.count = 0
	for i := 0; i < len(a.childrenHeaders); i++ {
		a.header.count += a.childrenHeaders[i].count
	}
	a.header.size = metaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*slabHeaderSize

	// Update right slab
	rightSlab.childrenHeaders = rightSlab.childrenHeaders[len(rightSlab.childrenHeaders)-rightSlabHeaderLen:]

	rightSlab.header.count = 0
	for i := 0; i < len(rightSlab.childrenHeaders); i++ {
		rightSlab.header.count += rightSlab.childrenHeaders[i].count
	}
	rightSlab.header.size = metaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*slabHeaderSize

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
	n := uint32(math.Ceil(float64(size) / slabHeaderSize))
	return a.header.size-slabHeaderSize*n > uint32(minThreshold)
}

func (a *ArrayMetaDataSlab) CanLendToRight(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / slabHeaderSize))
	return a.header.size-slabHeaderSize*n > uint32(minThreshold)
}

func (a ArrayMetaDataSlab) IsData() bool {
	return false
}

func (a *ArrayMetaDataSlab) SetID(id StorageID) {
	a.header.id = id
}

func (a *ArrayMetaDataSlab) Header() SlabHeader {
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
	for _, h := range a.childrenHeaders {
		elemsStr = append(elemsStr, fmt.Sprintf("%+v", h))
	}
	return strings.Join(elemsStr, " ")
}

func NewArray(storage SlabStorage) *Array {
	root := newArrayDataSlab()

	storage.Store(root.header.id, root)

	return &Array{
		storage: storage,
		root:    root,
	}
}

func NewArrayWithRootID(storage SlabStorage, rootID StorageID) (*Array, error) {
	root, err := getArraySlab(storage, rootID)
	if err != nil {
		return nil, err
	}
	return &Array{storage: storage, root: root}, nil
}

func (a *Array) Get(i uint64) (Storable, error) {
	v, err := a.root.Get(a.storage, i)
	if err != nil {
		return nil, err
	}
	// TODO: optimize this
	if idValue, ok := v.(StorageIDValue); ok {
		id := StorageID(idValue)
		if id == StorageIDUndefined {
			return nil, fmt.Errorf("invalid storage id")
		}
		slab, found, err := a.storage.Retrieve(id)
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

func (a *Array) Set(index uint64, v Storable) error {
	if v.Mutable() || v.ByteSize() > uint32(maxInlineElementSize) {
		v = StorageIDValue(v.ID())
	}
	return a.root.Set(a.storage, index, v)
}

func (a *Array) Append(v Storable) error {
	return a.Insert(a.Count(), v)
}

func (a *Array) Insert(index uint64, v Storable) error {
	if v.Mutable() || v.ByteSize() > uint32(maxInlineElementSize) {
		v = StorageIDValue(v.ID())
	}

	err := a.root.Insert(a.storage, index, v)
	if err != nil {
		return err
	}

	if a.root.IsFull() {

		copiedRoot := a.root.ShallowCloneWithNewID()

		// Split copied root
		left, right, err := copiedRoot.Split()
		if err != nil {
			return err
		}

		if a.root.IsData() {
			// Create new ArrayMetaDataSlab with the same storage ID as root
			rootID := a.root.ID()
			a.root = &ArrayMetaDataSlab{
				header: SlabHeader{
					id: rootID,
				},
			}
		}

		root := a.root.(*ArrayMetaDataSlab)
		root.childrenHeaders = []SlabHeader{left.Header(), right.Header()}
		root.header.count = left.Header().count + right.Header().count
		root.header.size = metaDataSlabPrefixSize + slabHeaderSize*uint32(len(root.childrenHeaders))

		a.storage.Store(left.Header().id, left)
		a.storage.Store(right.Header().id, right)
		a.storage.Store(a.root.Header().id, a.root)
	}

	return nil
}

func (a *Array) Remove(index uint64) (Storable, error) {
	v, err := a.root.Remove(a.storage, index)
	if err != nil {
		return nil, err
	}

	if !a.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := a.root.(*ArrayMetaDataSlab)
		if len(root.childrenHeaders) == 1 {

			rootID := root.header.id

			childID := root.childrenHeaders[0].id

			child, err := getArraySlab(a.storage, childID)
			if err != nil {
				return nil, err
			}

			a.root = child

			a.root.SetID(rootID)

			a.storage.Store(rootID, a.root)
			a.storage.Remove(childID)
		}
	}

	return v, nil
}

func (a *Array) Iterate(fn func(Storable)) error {
	slab, err := firstDataSlab(a.storage, a.root)
	if err != nil {
		return nil
	}

	id := slab.ID()

	for id != StorageIDUndefined {
		slab, found, err := a.storage.Retrieve(id)
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

func (a *Array) Count() uint64 {
	return uint64(a.root.Header().count)
}

func (a *Array) StorageID() StorageID {
	return a.root.Header().id
}

func (a *Array) String() string {
	if a.root.IsData() {
		return a.root.String()
	}
	meta := a.root.(*ArrayMetaDataSlab)
	return a.string(meta)
}

func (a *Array) string(meta *ArrayMetaDataSlab) string {
	var elemsStr []string

	for _, h := range meta.childrenHeaders {
		child, err := getArraySlab(a.storage, h.id)
		if err != nil {
			return err.Error()
		}
		if child.IsData() {
			data := child.(*ArrayDataSlab)
			elemsStr = append(elemsStr, data.String())
		} else {
			meta := child.(*ArrayMetaDataSlab)
			elemsStr = append(elemsStr, a.string(meta))
		}
	}
	return strings.Join(elemsStr, " ")
}

func getArraySlab(storage SlabStorage, id StorageID) (ArraySlab, error) {
	slab, _, err := storage.Retrieve(id)

	if arraySlab, ok := slab.(ArraySlab); ok {
		return arraySlab, nil
	}

	return nil, ArraySlabNotFoundError{id, err}
}

func firstDataSlab(storage SlabStorage, slab ArraySlab) (ArraySlab, error) {
	if slab.IsData() {
		return slab, nil
	}
	meta := slab.(*ArrayMetaDataSlab)
	firstChildID := meta.childrenHeaders[0].id
	firstChild, err := getArraySlab(storage, firstChildID)
	if err != nil {
		return nil, err
	}
	return firstDataSlab(storage, firstChild)
}
