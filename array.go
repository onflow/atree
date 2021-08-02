/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	storageIDSize = 8

	// slab header size: storage id (8 bytes) + count (4 bytes) + size (4 bytes)
	arraySlabHeaderSize = storageIDSize + 4 + 4

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + child header count (2 bytes)
	arrayMetaDataSlabPrefixSize = 1 + 1 + 2

	// version (1 byte) + flag (1 byte) + prev id (8 bytes) + next id (8 bytes) + CBOR array size (3 bytes)
	// (3 bytes of array size support up to 65535 array elements)
	arrayDataSlabPrefixSize = 2 + storageIDSize + storageIDSize + 3

	// 32 is faster than 24 and 40.
	linearScanThreshold = 32
)

type ArraySlabHeader struct {
	id    StorageID // id is used to retrieve slab from storage
	size  uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count uint32    // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

// ArrayDataSlab is leaf node, implementing ArraySlab.
type ArrayDataSlab struct {
	prev     StorageID
	next     StorageID
	header   ArraySlabHeader
	elements []Storable
}

func (a *ArrayDataSlab) Value(storage SlabStorage) (Value, error) {
	return &Array{storage: storage, root: a}, nil
}

var _ ArraySlab = &ArrayDataSlab{}

// ArrayMetaDataSlab is internal node, implementing ArraySlab.
type ArrayMetaDataSlab struct {
	header          ArraySlabHeader
	childrenHeaders []ArraySlabHeader
	// Cumulative counts in the children.
	// For example, if the counts in childrenHeaders are [10, 15, 12],
	// childrenCountSum is [10, 25, 37]
	childrenCountSum []uint32
}

var _ ArraySlab = &ArrayMetaDataSlab{}

func (a *ArrayMetaDataSlab) Value(storage SlabStorage) (Value, error) {
	return &Array{storage: storage, root: a}, nil
}

type ArraySlab interface {
	Slab

	Get(storage SlabStorage, index uint64) (Storable, error)
	Set(storage SlabStorage, index uint64, v Storable) error
	Insert(storage SlabStorage, index uint64, v Storable) error
	Remove(storage SlabStorage, index uint64) (Storable, error)

	ShallowCloneWithNewID(SlabStorage) ArraySlab

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetID(StorageID)

	Header() ArraySlabHeader
}

// Array is tree
type Array struct {
	storage SlabStorage
	root    ArraySlab
}

var _ Value = &Array{}

func (a *Array) Value(_ SlabStorage) (Value, error) {
	return a, nil
}

func (a *Array) Storable() Storable {
	return a.root
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

func newArrayDataSlab(storage SlabStorage) *ArrayDataSlab {
	return &ArrayDataSlab{
		header: ArraySlabHeader{
			id:   storage.GenerateStorageID(),
			size: arrayDataSlabPrefixSize,
		},
	}
}

func newArrayDataSlabFromData(id StorageID, data []byte) (*ArrayDataSlab, error) {
	// Check data length
	if len(data) < arrayDataSlabPrefixSize {
		return nil, errors.New("data is too short for array data slab")
	}

	// Check flag
	flag := data[1]
	if flag&flagArray == 0 || flag&flagDataSlab == 0 {
		return nil, fmt.Errorf(
			"data has invalid flag 0x%x, want 0x%x",
			flag,
			flagArray&flagDataSlab,
		)
	}

	const versionAndFlagSize = 2

	// Decode prev storage ID
	const prevStorageIDOffset = versionAndFlagSize
	prev := binary.BigEndian.Uint64(data[prevStorageIDOffset:])

	// Decode next storage ID
	const nextStorageIDOffset = prevStorageIDOffset + storageIDSize
	next := binary.BigEndian.Uint64(data[nextStorageIDOffset:])

	// Decode content (CBOR array)
	const contentOffset = nextStorageIDOffset + storageIDSize
	cborDec := NewByteStreamDecoder(data[contentOffset:])

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

	header := ArraySlabHeader{
		id:    id,
		size:  uint32(len(data)),
		count: uint32(elemCount),
	}

	return &ArrayDataSlab{
		prev:     StorageID(prev),
		next:     StorageID(next),
		header:   header,
		elements: elements,
	}, nil
}

// Encode encodes this array data slab to the given encoder.
//
// Header (18 bytes):
//
//   +-------------------------------+-------------------------------+-------------------------------+
//   | slab version + flag (2 bytes) | prev sib storage ID (8 bytes) | next sib storage ID (8 bytes) |
//   +-------------------------------+-------------------------------+-------------------------------+
//
// Content (for now):
//
//   CBOR encoded array of elements
//
func (a *ArrayDataSlab) Encode(enc *Encoder) error {

	// Encode version
	enc.scratch[0] = 0

	// Encode flag
	enc.scratch[1] = flagDataSlab | flagArray

	const versionAndFlagSize = 2

	// Encode prev storage ID to scratch
	const prevStorageIDOffset = versionAndFlagSize
	binary.BigEndian.PutUint64(
		enc.scratch[prevStorageIDOffset:],
		uint64(a.prev),
	)

	// Encode next storage ID to scratch
	const nextStorageIDOffset = prevStorageIDOffset + storageIDSize
	binary.BigEndian.PutUint64(
		enc.scratch[nextStorageIDOffset:],
		uint64(a.next),
	)

	// Encode CBOR array size manually for fix-sized encoding
	const contentOffset = nextStorageIDOffset + storageIDSize

	enc.scratch[contentOffset] = 0x80 | 25

	const countOffset = contentOffset + 1
	const countSize = 2
	binary.BigEndian.PutUint16(
		enc.scratch[countOffset:],
		uint16(len(a.elements)),
	)

	// Write scratch content to encoder
	const totalSize = countOffset + countSize
	_, err := enc.Write(enc.scratch[:totalSize])
	if err != nil {
		return err
	}

	// Encode data slab content (array of elements)
	for _, e := range a.elements {
		err = e.Encode(enc)
		if err != nil {
			return err
		}
	}

	return enc.cbor.Flush()
}

func (a *ArrayDataSlab) ShallowCloneWithNewID(storage SlabStorage) ArraySlab {
	return &ArrayDataSlab{
		header: ArraySlabHeader{
			id:    storage.GenerateStorageID(),
			size:  a.header.size,
			count: a.header.count,
		},
		elements: a.elements,
		prev:     a.prev,
		next:     a.next,
	}
}

func (a *ArrayDataSlab) Get(_ SlabStorage, index uint64) (Storable, error) {
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

	return storage.Store(a.header.id, a)
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

	return storage.Store(a.header.id, a)
}

func (a *ArrayDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, IndexOutOfRangeError{}
	}

	v := a.elements[index]

	lastIndex := len(a.elements) - 1

	if index != uint64(lastIndex) {
		copy(a.elements[index:], a.elements[index+1:])
	}

	// NOTE: prevent memory leak
	a.elements[lastIndex] = nil

	a.elements = a.elements[:lastIndex]

	a.header.count--
	a.header.size -= v.ByteSize()

	err := storage.Store(a.header.id, a)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (a *ArrayDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if len(a.elements) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split slab with less than 2 elements")
	}

	// This computes the ceil of split to give the first slab with more elements.
	dataSize := a.header.size - arrayDataSlabPrefixSize
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
	rightSlabCount := len(a.elements) - leftCount
	rightSlab := &ArrayDataSlab{
		header: ArraySlabHeader{
			id:    storage.GenerateStorageID(),
			size:  arrayDataSlabPrefixSize + dataSize - leftSize,
			count: uint32(rightSlabCount),
		},
		prev: a.header.id,
		next: a.next,
	}

	rightSlab.elements = make([]Storable, rightSlabCount)
	copy(rightSlab.elements, a.elements[leftCount:])

	// Modify left (original) slab
	// NOTE: prevent memory leak
	for i := leftCount; i < len(a.elements); i++ {
		a.elements[i] = nil
	}
	a.elements = a.elements[:leftCount]
	a.header.size = arrayDataSlabPrefixSize + leftSize
	a.header.count = uint32(leftCount)
	a.next = rightSlab.header.id

	return a, rightSlab, nil
}

func (a *ArrayDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*ArrayDataSlab)
	a.elements = append(a.elements, rightSlab.elements...)
	a.header.size = a.header.size + rightSlab.header.size - arrayDataSlabPrefixSize
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

	// Update the right slab
	//
	// It is easier and less error-prone to realloc elements for the right slab.

	elements := make([]Storable, count-leftCount)
	n := copy(elements, a.elements[leftCount:])
	copy(elements[n:], rightSlab.elements)

	rightSlab.elements = elements
	rightSlab.header.size = size - leftSize
	rightSlab.header.count = count - leftCount

	// Update left slab
	// NOTE: prevent memory leak
	for i := leftCount; i < uint32(len(a.elements)); i++ {
		a.elements[i] = nil
	}
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
	// TODO: copy elements to front instead?
	// NOTE: prevent memory leak
	for i := uint32(0); i < rightStartIndex; i++ {
		rightSlab.elements[i] = nil
	}
	rightSlab.elements = rightSlab.elements[rightStartIndex:]
	rightSlab.header.size = size - leftSize
	rightSlab.header.count = count - leftCount

	return nil
}

func (a *ArrayDataSlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

// IsUnderflow returns the number of bytes needed for the data slab
// to reach the min threshold.
// Returns true if the min threshold has not been reached yet.
//
func (a *ArrayDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > a.header.size {
		return uint32(minThreshold) - a.header.size, true
	}
	return 0, false
}

// CanLendToLeft returns true if elements on the left of the slab could be removed
// so that the slab still stores more than the min threshold.
//
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

// CanLendToRight returns true if elements on the right of the slab could be removed
// so that the slab still stores more than the min threshold.
//
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

func (a *ArrayDataSlab) Header() ArraySlabHeader {
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
	if len(data) < arrayMetaDataSlabPrefixSize {
		return nil, errors.New("data is too short for array metadata slab")
	}

	// Check flag
	flag := data[1]
	if flag&flagArray == 0 || flag&flagMetaDataSlab == 0 {
		return nil, fmt.Errorf(
			"data has invalid flag 0x%x, want 0x%x",
			flag,
			flagArray&flagMetaDataSlab,
		)
	}

	const versionAndFlagSize = 2

	// Decode number of child headers
	const childHeaderCountOffset = versionAndFlagSize
	childHeaderCount := binary.BigEndian.Uint16(data[childHeaderCountOffset:])

	const childHeaderSize = 16

	expectedDataLength := arrayMetaDataSlabPrefixSize + childHeaderSize*int(childHeaderCount)
	if len(data) != expectedDataLength {
		return nil, fmt.Errorf(
			"data has unexpected length %d, want %d",
			len(data),
			expectedDataLength,
		)
	}

	// Decode child headers
	childrenHeaders := make([]ArraySlabHeader, childHeaderCount)
	childrenCountSum := make([]uint32, childHeaderCount)
	totalCount := uint32(0)
	offset := childHeaderCountOffset + 2

	for i := 0; i < int(childHeaderCount); i++ {
		storageID := binary.BigEndian.Uint64(data[offset:])

		countOffset := offset + storageIDSize
		count := binary.BigEndian.Uint32(data[countOffset:])

		sizeOffset := countOffset + 4
		size := binary.BigEndian.Uint32(data[sizeOffset:])

		totalCount += count

		childrenHeaders[i] = ArraySlabHeader{
			id:    StorageID(storageID),
			count: count,
			size:  size,
		}
		childrenCountSum[i] = totalCount

		offset += childHeaderSize
	}

	header := ArraySlabHeader{
		id:    id,
		size:  uint32(len(data)),
		count: totalCount,
	}

	return &ArrayMetaDataSlab{
		header:           header,
		childrenHeaders:  childrenHeaders,
		childrenCountSum: childrenCountSum,
	}, nil
}

// Encode encodes this array meta-data slab to the given encoder.
//
// Header (4 bytes):
//
//     +-----------------------+--------------------+------------------------------+
//     | slab version (1 byte) | slab flag (1 byte) | child header count (2 bytes) |
//     +-----------------------+--------------------+------------------------------+
//
// Content (n * 16 bytes):
//
// 	[[count, size, storage id], ...]
//
func (a *ArrayMetaDataSlab) Encode(enc *Encoder) error {

	// Encode version
	enc.scratch[0] = 0

	// Encode flag
	enc.scratch[1] = flagArray | flagMetaDataSlab

	const versionAndFlagSize = 2

	// Encode child header count to scratch
	const childHeaderCountOffset = versionAndFlagSize
	binary.BigEndian.PutUint16(
		enc.scratch[childHeaderCountOffset:],
		uint16(len(a.childrenHeaders)),
	)

	// Write scratch content to encoder
	const totalSize = childHeaderCountOffset + 2
	_, err := enc.Write(enc.scratch[:totalSize])
	if err != nil {
		return err
	}

	// Encode children headers
	for _, h := range a.childrenHeaders {
		binary.BigEndian.PutUint64(enc.scratch[:], uint64(h.id))

		const countOffset = storageIDSize
		binary.BigEndian.PutUint32(enc.scratch[countOffset:], h.count)

		const sizeOffset = countOffset + 4
		binary.BigEndian.PutUint32(enc.scratch[sizeOffset:], h.size)

		const totalSize = sizeOffset + 4
		_, err = enc.Write(enc.scratch[:totalSize])
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *ArrayMetaDataSlab) ShallowCloneWithNewID(storage SlabStorage) ArraySlab {
	return &ArrayMetaDataSlab{
		header: ArraySlabHeader{
			id:    storage.GenerateStorageID(),
			size:  a.header.size,
			count: a.header.count,
		},
		childrenHeaders:  a.childrenHeaders,
		childrenCountSum: a.childrenCountSum,
	}
}

// TODO: improve naming
func (a *ArrayMetaDataSlab) childSlabIndexInfo(
	index uint64,
) (
	childHeaderIndex int,
	adjustedIndex uint64,
	childID StorageID,
	err error,
) {
	if index >= uint64(a.header.count) {
		return 0, 0, 0, IndexOutOfRangeError{}
	}

	// Either perform a linear scan (for small number of children),
	// or a binary search

	count := len(a.childrenCountSum)

	if count < linearScanThreshold {
		for i, countSum := range a.childrenCountSum {
			if index < uint64(countSum) {
				childHeaderIndex = i
				break
			}
		}
	} else {
		low, high := 0, count
		for low < high {
			// The following line is borrowed from Go runtime .
			mid := int(uint(low+high) >> 1) // avoid overflow when computing mid
			midCountSum := uint64(a.childrenCountSum[mid])

			if midCountSum < index {
				low = mid + 1
			} else if midCountSum > index {
				high = mid
			} else {
				low = mid + 1
				break

			}
		}
		childHeaderIndex = low
	}

	childHeader := a.childrenHeaders[childHeaderIndex]
	adjustedIndex = index + uint64(childHeader.count) - uint64(a.childrenCountSum[childHeaderIndex])
	childID = childHeader.id

	return childHeaderIndex, adjustedIndex, childID, nil
}

func (a *ArrayMetaDataSlab) Get(storage SlabStorage, index uint64) (Storable, error) {

	_, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		return nil, err
	}

	return child.Get(storage, adjustedIndex)
}

func (a *ArrayMetaDataSlab) Set(storage SlabStorage, index uint64, v Storable) error {

	childHeaderIndex, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		return err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		return err
	}

	err = child.Set(storage, adjustedIndex, v)
	if err != nil {
		return err
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Update may increase or decrease the size,
	// check if full and for underflow

	if child.IsFull() {
		return a.SplitChildSlab(storage, child, childHeaderIndex)
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		return a.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
	}

	return storage.Store(a.header.id, a)
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
	var adjustedIndex uint64
	if index == uint64(a.header.count) {
		childHeaderIndex = len(a.childrenHeaders) - 1
		h := a.childrenHeaders[childHeaderIndex]
		childID = h.id
		adjustedIndex = uint64(h.count)
	} else {
		var err error
		childHeaderIndex, adjustedIndex, childID, err = a.childSlabIndexInfo(index)
		if err != nil {
			return err
		}
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		return err
	}

	err = child.Insert(storage, adjustedIndex, v)
	if err != nil {
		return err
	}

	a.header.count++

	// Increment childrenCountSum from childHeaderIndex
	for i := childHeaderIndex; i < len(a.childrenCountSum); i++ {
		a.childrenCountSum[i]++
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Insertion increases the size,
	// check if full

	if child.IsFull() {
		return a.SplitChildSlab(storage, child, childHeaderIndex)
	}

	// Insertion always increases the size,
	// so there is no need to check underflow

	return storage.Store(a.header.id, a)
}

func (a *ArrayMetaDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {

	if index >= uint64(a.header.count) {
		return nil, IndexOutOfRangeError{}
	}

	childHeaderIndex, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		return nil, err
	}

	v, err := child.Remove(storage, adjustedIndex)
	if err != nil {
		return nil, err
	}

	a.header.count--

	// Decrement childrenCountSum from childHeaderIndex
	for i := childHeaderIndex; i < len(a.childrenCountSum); i++ {
		a.childrenCountSum[i]--
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Removal decreases the size,
	// check for underflow

	if underflowSize, isUnderflow := child.IsUnderflow(); isUnderflow {
		err = a.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			return nil, err
		}
	}

	// Removal always decreases the size,
	// so there is no need to check isFull

	err = storage.Store(a.header.id, a)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (a *ArrayMetaDataSlab) SplitChildSlab(storage SlabStorage, child ArraySlab, childHeaderIndex int) error {
	leftSlab, rightSlab, err := child.Split(storage)
	if err != nil {
		return err
	}

	left := leftSlab.(ArraySlab)
	right := rightSlab.(ArraySlab)

	// Add new child slab (right) to childrenHeaders
	a.childrenHeaders = append(a.childrenHeaders, ArraySlabHeader{})
	if childHeaderIndex < len(a.childrenHeaders)-2 {
		copy(a.childrenHeaders[childHeaderIndex+2:], a.childrenHeaders[childHeaderIndex+1:])
	}
	a.childrenHeaders[childHeaderIndex] = left.Header()
	a.childrenHeaders[childHeaderIndex+1] = right.Header()

	// Adjust childrenCountSum
	a.childrenCountSum = append(a.childrenCountSum, uint32(0))
	copy(a.childrenCountSum[childHeaderIndex+1:], a.childrenCountSum[childHeaderIndex:])
	a.childrenCountSum[childHeaderIndex] -= right.Header().count

	// Increase header size
	a.header.size += arraySlabHeaderSize

	// Store modified slabs
	err = storage.Store(left.ID(), left)
	if err != nil {
		return err
	}
	err = storage.Store(right.ID(), right)
	if err != nil {
		return err
	}
	return storage.Store(a.header.id, a)
}

// MergeOrRebalanceChildSlab merges or rebalances child slab.
// If merged, then parent slab's data is adjusted.
//
// +-----------------------+-----------------------+----------------------+-----------------------+
// |                       | no left sibling (sib) | left sib can't lend  | left sib can lend     |
// +=======================+=======================+======================+=======================+
// | no right sib          | panic                 | merge with left      | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can't lend  | merge with right      | merge with smaller   | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can lend    | rebalance with right  | rebalance with right | rebalance with bigger |
// +-----------------------+-----------------------+----------------------+-----------------------+
//
func (a *ArrayMetaDataSlab) MergeOrRebalanceChildSlab(
	storage SlabStorage,
	child ArraySlab,
	childHeaderIndex int,
	underflowSize uint32,
) error {

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
			baseCountSum := a.childrenCountSum[childHeaderIndex] - child.Header().count

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				return err
			}

			a.childrenHeaders[childHeaderIndex] = child.Header()
			a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex] = baseCountSum + child.Header().count

			// Store modified slabs
			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}
			err = storage.Store(rightSib.ID(), rightSib)
			if err != nil {
				return err
			}
			return storage.Store(a.header.id, a)
		}

		// Rebalance with left sib
		if !rightCanLend {
			baseCountSum := a.childrenCountSum[childHeaderIndex-1] - leftSib.Header().count

			err := leftSib.LendToRight(child)
			if err != nil {
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex-1] = baseCountSum + leftSib.Header().count

			// Store modified slabs
			err = storage.Store(leftSib.ID(), leftSib)
			if err != nil {
				return err
			}
			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}
			return storage.Store(a.header.id, a)
		}

		// Rebalance with bigger sib
		if leftSib.ByteSize() > rightSib.ByteSize() {
			baseCountSum := a.childrenCountSum[childHeaderIndex-1] - leftSib.Header().count

			err := leftSib.LendToRight(child)
			if err != nil {
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex-1] = baseCountSum + leftSib.Header().count

			// Store modified slabs
			err = storage.Store(leftSib.ID(), leftSib)
			if err != nil {
				return err
			}
			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}
			return storage.Store(a.header.id, a)
		} else {
			// leftSib.ByteSize() <= rightSib.ByteSize

			baseCountSum := a.childrenCountSum[childHeaderIndex] - child.Header().count

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				return err
			}

			a.childrenHeaders[childHeaderIndex] = child.Header()
			a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex] = baseCountSum + child.Header().count

			// Store modified slabs
			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}
			err = storage.Store(rightSib.ID(), rightSib)
			if err != nil {
				return err
			}
			err = storage.Store(a.header.id, a)
			if err != nil {
				return err
			}
			return nil
		}
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

		// Adjust childrenCountSum
		copy(a.childrenCountSum[childHeaderIndex:], a.childrenCountSum[childHeaderIndex+1:])
		a.childrenCountSum = a.childrenCountSum[:len(a.childrenCountSum)-1]

		a.header.size -= arraySlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(child.ID(), child)
		if err != nil {
			return err
		}
		err = storage.Store(a.header.id, a)
		if err != nil {
			return err
		}

		// Remove right sib from storage
		storage.Remove(rightSib.ID())

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

		// Adjust childrenCountSum
		copy(a.childrenCountSum[childHeaderIndex-1:], a.childrenCountSum[childHeaderIndex:])
		a.childrenCountSum = a.childrenCountSum[:len(a.childrenCountSum)-1]

		a.header.size -= arraySlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(leftSib.ID(), leftSib)
		if err != nil {
			return err
		}
		err = storage.Store(a.header.id, a)
		if err != nil {
			return err
		}

		// Remove child from storage
		storage.Remove(child.ID())

		return nil
	}

	// Merge with smaller sib
	if leftSib.ByteSize() < rightSib.ByteSize() {
		err := leftSib.Merge(child)
		if err != nil {
			return err
		}

		a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(a.childrenHeaders[childHeaderIndex:], a.childrenHeaders[childHeaderIndex+1:])
		a.childrenHeaders = a.childrenHeaders[:len(a.childrenHeaders)-1]

		// Adjust childrenCountSum
		copy(a.childrenCountSum[childHeaderIndex-1:], a.childrenCountSum[childHeaderIndex:])
		a.childrenCountSum = a.childrenCountSum[:len(a.childrenCountSum)-1]

		a.header.size -= arraySlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(leftSib.ID(), leftSib)
		if err != nil {
			return err
		}
		err = storage.Store(a.header.id, a)
		if err != nil {
			return err
		}

		// Remove child from storage
		storage.Remove(child.ID())

		return nil
	} else {
		// leftSib.ByteSize() > rightSib.ByteSize

		err := child.Merge(rightSib)
		if err != nil {
			return err
		}

		a.childrenHeaders[childHeaderIndex] = child.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(a.childrenHeaders[childHeaderIndex+1:], a.childrenHeaders[childHeaderIndex+2:])
		a.childrenHeaders = a.childrenHeaders[:len(a.childrenHeaders)-1]

		// Adjust childrenCountSum
		copy(a.childrenCountSum[childHeaderIndex:], a.childrenCountSum[childHeaderIndex+1:])
		a.childrenCountSum = a.childrenCountSum[:len(a.childrenCountSum)-1]

		a.header.size -= arraySlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(child.ID(), child)
		if err != nil {
			return err
		}
		err = storage.Store(a.header.id, a)
		if err != nil {
			return err
		}

		// Remove rightSib from storage
		storage.Remove(rightSib.ID())

		return nil
	}
}

func (a *ArrayMetaDataSlab) Merge(slab Slab) error {

	// The assumption len > 0 holds in all cases except for the root slab

	baseCountSum := a.childrenCountSum[len(a.childrenCountSum)-1]
	leftSlabChildrenCount := len(a.childrenHeaders)

	rightSlab := slab.(*ArrayMetaDataSlab)
	a.childrenHeaders = append(a.childrenHeaders, rightSlab.childrenHeaders...)
	a.header.size += rightSlab.header.size - arrayMetaDataSlabPrefixSize
	a.header.count += rightSlab.header.count

	// Adjust childrenCountSum
	for i := leftSlabChildrenCount; i < len(a.childrenHeaders); i++ {
		baseCountSum += a.childrenHeaders[i].count
		a.childrenCountSum = append(a.childrenCountSum, baseCountSum)
	}

	return nil
}

func (a *ArrayMetaDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {

	if len(a.childrenHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		panic("can't split meta slab with less than 2 headers")
	}

	leftChildrenCount := int(math.Ceil(float64(len(a.childrenHeaders)) / 2))
	leftSize := leftChildrenCount * arraySlabHeaderSize

	leftCount := uint32(0)
	for i := 0; i < leftChildrenCount; i++ {
		leftCount += a.childrenHeaders[i].count
	}

	// Construct right slab
	rightSlab := &ArrayMetaDataSlab{
		header: ArraySlabHeader{
			id:    storage.GenerateStorageID(),
			size:  a.header.size - uint32(leftSize),
			count: a.header.count - leftCount,
		},
	}

	rightSlab.childrenHeaders = make([]ArraySlabHeader, len(a.childrenHeaders)-leftChildrenCount)
	copy(rightSlab.childrenHeaders, a.childrenHeaders[leftChildrenCount:])

	rightSlab.childrenCountSum = make([]uint32, len(rightSlab.childrenHeaders))
	countSum := uint32(0)
	for i := 0; i < len(rightSlab.childrenCountSum); i++ {
		countSum += rightSlab.childrenHeaders[i].count
		rightSlab.childrenCountSum[i] = countSum
	}

	// Modify left (original)slab
	a.childrenHeaders = a.childrenHeaders[:leftChildrenCount]
	a.childrenCountSum = a.childrenCountSum[:leftChildrenCount]
	a.header.count = leftCount
	a.header.size = arrayMetaDataSlabPrefixSize + uint32(leftSize)

	return a, rightSlab, nil
}

func (a *ArrayMetaDataSlab) LendToRight(slab Slab) error {
	rightSlab := slab.(*ArrayMetaDataSlab)

	childrenHeadersLen := len(a.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftChildrenHeadersLen := childrenHeadersLen / 2
	rightChildrenHeadersLen := childrenHeadersLen - leftChildrenHeadersLen

	// Update right slab childrenHeaders by prepending borrowed children headers
	rightChildrenHeaders := make([]ArraySlabHeader, rightChildrenHeadersLen)
	n := copy(rightChildrenHeaders, a.childrenHeaders[leftChildrenHeadersLen:])
	copy(rightChildrenHeaders[n:], rightSlab.childrenHeaders)
	rightSlab.childrenHeaders = rightChildrenHeaders

	// Rebuild right slab childrenCountSum
	rightSlab.childrenCountSum = make([]uint32, len(rightSlab.childrenHeaders))
	countSum := uint32(0)
	for i := 0; i < len(rightSlab.childrenCountSum); i++ {
		countSum += rightSlab.childrenHeaders[i].count
		rightSlab.childrenCountSum[i] = countSum
	}

	// Update right slab header
	rightSlab.header.count = 0
	for i := 0; i < len(rightSlab.childrenHeaders); i++ {
		rightSlab.header.count += rightSlab.childrenHeaders[i].count
	}
	rightSlab.header.size = arrayMetaDataSlabPrefixSize + uint32(len(rightSlab.childrenHeaders))*arraySlabHeaderSize

	// Update left slab (original)
	a.childrenHeaders = a.childrenHeaders[:leftChildrenHeadersLen]
	a.childrenCountSum = a.childrenCountSum[:leftChildrenHeadersLen]

	a.header.count = 0
	for i := 0; i < len(a.childrenHeaders); i++ {
		a.header.count += a.childrenHeaders[i].count
	}
	a.header.size = arrayMetaDataSlabPrefixSize + uint32(leftChildrenHeadersLen)*arraySlabHeaderSize

	return nil
}

func (a *ArrayMetaDataSlab) BorrowFromRight(slab Slab) error {
	originalLeftSlabCountSum := a.header.count
	originalLeftSlabHeaderLen := len(a.childrenHeaders)

	rightSlab := slab.(*ArrayMetaDataSlab)

	childrenHeadersLen := len(a.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftSlabHeaderLen := childrenHeadersLen / 2
	rightSlabHeaderLen := childrenHeadersLen - leftSlabHeaderLen

	// Update left slab (original)
	a.childrenHeaders = append(a.childrenHeaders, rightSlab.childrenHeaders[:leftSlabHeaderLen-len(a.childrenHeaders)]...)

	countSum := originalLeftSlabCountSum
	for i := originalLeftSlabHeaderLen; i < len(a.childrenHeaders); i++ {
		countSum += a.childrenHeaders[i].count
		a.childrenCountSum = append(a.childrenCountSum, countSum)
	}
	a.header.count = countSum
	a.header.size = arrayMetaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*arraySlabHeaderSize

	// Update right slab
	rightSlab.childrenHeaders = rightSlab.childrenHeaders[len(rightSlab.childrenHeaders)-rightSlabHeaderLen:]
	rightSlab.childrenCountSum = rightSlab.childrenCountSum[:len(rightSlab.childrenHeaders)]

	countSum = uint32(0)
	for i := 0; i < len(rightSlab.childrenHeaders); i++ {
		countSum += rightSlab.childrenHeaders[i].count
		rightSlab.childrenCountSum[i] = countSum
	}
	rightSlab.header.count = countSum
	rightSlab.header.size = arrayMetaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*arraySlabHeaderSize

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
	n := uint32(math.Ceil(float64(size) / arraySlabHeaderSize))
	return a.header.size-arraySlabHeaderSize*n > uint32(minThreshold)
}

func (a *ArrayMetaDataSlab) CanLendToRight(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / arraySlabHeaderSize))
	return a.header.size-arraySlabHeaderSize*n > uint32(minThreshold)
}

func (a ArrayMetaDataSlab) IsData() bool {
	return false
}

func (a *ArrayMetaDataSlab) SetID(id StorageID) {
	a.header.id = id
}

func (a *ArrayMetaDataSlab) Header() ArraySlabHeader {
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

func NewArray(storage SlabStorage) (*Array, error) {
	root := newArrayDataSlab(storage)

	err := storage.Store(root.header.id, root)
	if err != nil {
		return nil, err
	}

	return &Array{
		storage: storage,
		root:    root,
	}, nil
}

func NewArrayWithRootID(storage SlabStorage, rootID StorageID) (*Array, error) {
	root, err := getArraySlab(storage, rootID)
	if err != nil {
		return nil, err
	}
	return &Array{storage: storage, root: root}, nil
}

func (a *Array) Get(i uint64) (Value, error) {
	storable, err := a.root.Get(a.storage, i)
	if err != nil {
		return nil, err
	}

	return storable.Value(a.storage)
}

func (a *Array) Set(index uint64, value Value) error {
	storable := value.Storable()
	if storable.Mutable() || storable.ByteSize() > uint32(maxInlineElementSize) {
		storable = StorageIDStorable(storable.ID())
	}
	return a.root.Set(a.storage, index, storable)
}

func (a *Array) Append(value Value) error {
	return a.Insert(a.Count(), value)
}

func (a *Array) Insert(index uint64, value Value) error {
	storable := value.Storable()
	if storable.Mutable() || storable.ByteSize() > uint32(maxInlineElementSize) {
		storable = StorageIDStorable(storable.ID())
	}

	err := a.root.Insert(a.storage, index, storable)
	if err != nil {
		return err
	}

	if a.root.IsFull() {

		copiedRoot := a.root.ShallowCloneWithNewID(a.storage)

		// Split copied root
		leftSlab, rightSlab, err := copiedRoot.Split(a.storage)
		if err != nil {
			return err
		}

		left := leftSlab.(ArraySlab)
		right := rightSlab.(ArraySlab)

		if a.root.IsData() {
			// Create new ArrayMetaDataSlab with the same storage ID as root
			rootID := a.root.ID()
			a.root = &ArrayMetaDataSlab{
				header: ArraySlabHeader{
					id: rootID,
				},
			}
		}

		root := a.root.(*ArrayMetaDataSlab)
		root.childrenHeaders = []ArraySlabHeader{left.Header(), right.Header()}
		root.childrenCountSum = []uint32{left.Header().count, left.Header().count + right.Header().count}
		root.header.count = left.Header().count + right.Header().count
		root.header.size = arrayMetaDataSlabPrefixSize + arraySlabHeaderSize*uint32(len(root.childrenHeaders))

		err = a.storage.Store(left.ID(), left)
		if err != nil {
			return err
		}
		err = a.storage.Store(right.ID(), right)
		if err != nil {
			return err
		}
		err = a.storage.Store(a.root.ID(), a.root)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Array) Remove(index uint64) (Value, error) {
	storable, err := a.root.Remove(a.storage, index)
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

			err = a.storage.Store(rootID, a.root)
			if err != nil {
				return nil, err
			}
			a.storage.Remove(childID)
		}
	}

	return storable.Value(a.storage)
}

type ArrayIterator struct {
	storage  SlabStorage
	id       StorageID
	dataSlab *ArrayDataSlab
	index    int
}

func (i *ArrayIterator) Next() (Value, error) {
	if i.dataSlab == nil {
		if i.id == StorageIDUndefined {
			return nil, nil
		}

		slab, found, err := i.storage.Retrieve(i.id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("slab %d not found", i.id)
		}

		i.dataSlab = slab.(*ArrayDataSlab)
		i.index = 0
	}

	var element Value
	var err error
	if i.index < len(i.dataSlab.elements) {
		element, err = i.dataSlab.elements[i.index].Value(i.storage)
		if err != nil {
			return nil, err
		}

		i.index++
	}

	if i.index >= len(i.dataSlab.elements) {
		i.id = i.dataSlab.next
		i.dataSlab = nil
	}

	return element, nil
}

func (a *Array) Iterator() (*ArrayIterator, error) {
	slab, err := firstArrayDataSlab(a.storage, a.root)
	if err != nil {
		return nil, err
	}

	return &ArrayIterator{
		storage: a.storage,
		id:      slab.ID(),
	}, nil
}

type ArrayIterationFunc func(element Value) (resume bool, err error)

func (a *Array) Iterate(fn ArrayIterationFunc) error {

	iterator, err := a.Iterator()
	if err != nil {
		return err
	}

	for {
		value, err := iterator.Next()
		if err != nil {
			return err
		}
		if value == nil {
			return nil
		}
		resume, err := fn(value)
		if err != nil {
			return err
		}
		if !resume {
			return nil
		}
	}
}

func (a *Array) DeepCopy(storage SlabStorage) (Value, error) {
	result, err := NewArray(storage)
	if err != nil {
		return nil, err
	}

	var index uint64
	err = a.Iterate(func(element Value) (resume bool, err error) {

		elementCopy, err := element.DeepCopy(storage)
		if err != nil {
			return false, err
		}

		err = result.Insert(index, elementCopy)
		if err != nil {
			return false, err
		}

		index++

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (a *Array) Count() uint64 {
	return uint64(a.root.Header().count)
}

func (a *Array) StorageID() StorageID {
	return a.root.ID()
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

func firstArrayDataSlab(storage SlabStorage, slab ArraySlab) (ArraySlab, error) {
	if slab.IsData() {
		return slab, nil
	}
	meta := slab.(*ArrayMetaDataSlab)
	firstChildID := meta.childrenHeaders[0].id
	firstChild, err := getArraySlab(storage, firstChildID)
	if err != nil {
		return nil, err
	}
	return firstArrayDataSlab(storage, firstChild)
}
