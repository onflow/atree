package main

import (
	"fmt"
	"math"
	"strings"
)

const headerSize = 16

type Slab interface {
	Storable

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
	storage  SlabStorage
	elements []Storable
}

// ArrayMetaDataSlab is internal node, implementing Slab, ArrayNode
type ArrayMetaDataSlab struct {
	storage        SlabStorage
	header         *SlabHeader
	orderedHeaders []*SlabHeader
}

type ArrayNode interface {
	Get(index uint64) (Storable, error)
	Set(index uint64, v Storable) error
	Insert(index uint64, v Storable) error
	Remove(index uint64) (Storable, error)

	ShallowCloneWithNewID(storage SlabStorage) ArrayNode

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

func newArrayDataSlab(storage SlabStorage) *ArrayDataSlab {
	slab := &ArrayDataSlab{
		storage: storage,
		header: &SlabHeader{
			id: generateStorageID(),
		},
	}

	storage.Store(slab.header.id, slab)

	return slab
}

func (a *ArrayDataSlab) ShallowCloneWithNewID(storage SlabStorage) ArrayNode {
	copied := newArrayDataSlab(storage)
	copied.header.size = a.header.size
	copied.header.count = a.header.count
	copied.elements = a.elements
	copied.prev = a.prev
	copied.next = a.next
	return copied
}

func (a *ArrayDataSlab) Get(index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, fmt.Errorf("out of bounds")
	}
	return a.elements[index], nil
}

func (a *ArrayDataSlab) Set(index uint64, v Storable) error {
	if index >= uint64(len(a.elements)) {
		return fmt.Errorf("out of bounds")
	}
	oldSize := a.elements[index].ByteSize()
	a.elements[index] = v
	a.header.size = a.header.size - oldSize + v.ByteSize()
	return nil
}

func (a *ArrayDataSlab) Insert(index uint64, v Storable) error {
	if index == uint64(len(a.elements)) {
		a.elements = append(a.elements, v)
	} else {
		a.elements = append(a.elements, nil)
		copy(a.elements[index+1:], a.elements[index:])
		a.elements[index] = v
	}

	a.header.count++
	a.header.size += v.ByteSize()
	return nil
}

func (a *ArrayDataSlab) Remove(index uint64) (Storable, error) {
	v := a.elements[index]

	switch index {
	case 0:
		a.elements = a.elements[1:]
	case uint64(len(a.elements)):
		a.elements = a.elements[:len(a.elements)-1]
	default:
		copy(a.elements[index:], a.elements[index+1:])
		a.elements = a.elements[:len(a.elements)-1]
	}

	a.header.count--
	a.header.size -= v.ByteSize()
	return v, nil
}

func (a *ArrayDataSlab) Split() (Slab, Slab, error) {
	if len(a.elements) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split array slab with less than 2 elements")
	}

	// This computes the ceil of split keep the first part with more members (optimized for append operations)
	size := a.header.size
	d := float64(size) / float64(2)
	midPoint := uint32(math.Ceil(d))

	rightSlabStartIndex := 0
	leftSlabSize := uint32(0)
	for i, e := range a.elements {
		leftSlabSize += e.ByteSize()
		if leftSlabSize >= midPoint {
			rightSlabStartIndex = i + 1
			break
		}
	}

	rightSlab := newArrayDataSlab(a.storage)
	rightSlab.elements = make([]Storable, len(a.elements)-rightSlabStartIndex)
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

// LendToRight rebalances nodes by moving elements from left to right
func (a *ArrayDataSlab) LendToRight(slab Slab) error {

	b := slab.(*ArrayDataSlab)

	count := a.header.count + b.header.count
	size := a.header.size + b.header.size

	midPoint := uint32(math.Ceil(float64(size) / 2))

	leftSlabCount := a.header.count
	leftSlabSize := a.header.size

	// Left node size is as close to midPoint as possible while right node size >= minThreshold
	for i := len(a.elements) - 1; i >= 0; i-- {
		if leftSlabSize-a.elements[i].ByteSize() < midPoint && size-leftSlabSize >= uint32(minThreshold) {
			break
		}
		leftSlabSize -= a.elements[i].ByteSize()
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
	a.header.size = leftSlabSize
	a.header.count = leftSlabCount

	// Update right slab header
	b.header.size = size - leftSlabSize
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
	leftSlabSize := a.header.size

	for _, e := range b.elements {
		if leftSlabSize+e.ByteSize() > midPoint {
			if size-leftSlabSize-e.ByteSize() >= uint32(minThreshold) {
				// Include this element in left node
				leftSlabSize += e.ByteSize()
				leftSlabCount++
			}
			break
		}
		leftSlabSize += e.ByteSize()
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
	a.header.size = leftSlabSize
	a.header.count = leftSlabCount

	// Update right slab header
	b.header.size = size - leftSlabSize
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

func newArrayMetaDataSlab(storage SlabStorage) *ArrayMetaDataSlab {
	slab := &ArrayMetaDataSlab{
		storage: storage,
		header: &SlabHeader{
			id: generateStorageID(),
		},
	}

	storage.Store(slab.header.id, slab)

	return slab
}

func (a *ArrayMetaDataSlab) ShallowCloneWithNewID(storage SlabStorage) ArrayNode {
	copied := newArrayMetaDataSlab(storage)
	copied.header.size = a.header.size
	copied.header.count = a.header.count
	copied.orderedHeaders = a.orderedHeaders
	return copied
}

func (a *ArrayMetaDataSlab) Get(index uint64) (Storable, error) {

	if index >= uint64(a.header.count) {
		return nil, fmt.Errorf("index %d out of bounds for slab %d", index, a.header.id)
	}

	var id StorageID
	for _, h := range a.orderedHeaders {
		if index < uint64(h.count) {
			id = h.id
			break
		}
		index -= uint64(h.count)
	}

	node, err := getArrayNodeFromStorageID(a.storage, id)
	if err != nil {
		return nil, err
	}

	return node.Get(index)
}

func (a *ArrayMetaDataSlab) Set(index uint64, v Storable) error {

	if index >= uint64(a.header.count) {
		return fmt.Errorf("index %d out of bounds for slab %d", index, a.header.id)
	}

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

	node, err := getArrayNodeFromStorageID(a.storage, id)
	if err != nil {
		return err
	}

	err = node.Set(index, v)
	if err != nil {
		return err
	}

	if node.IsFull() {
		return a.SplitChildNode(node, headerIndex)
	}

	if underflowSize, underflow := node.IsUnderflow(); underflow {
		return a.MergeOrRebalanceChildNode(node, headerIndex, underflowSize)
	}

	return nil
}

// Insert inserts v into correct ArrayDataSlab.
// index must be >=0 and <= a.header.count.
// If index == a.header.count, Insert appends v to the end of underlying data slab.
func (a *ArrayMetaDataSlab) Insert(index uint64, v Storable) error {
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
	var headerIndex int
	if index == uint64(a.header.count) {
		headerIndex = len(a.orderedHeaders) - 1
		h := a.orderedHeaders[headerIndex]
		id = h.id
		index = uint64(h.count)
	} else {
		for i, h := range a.orderedHeaders {
			if index < uint64(h.count) {
				id = h.id
				headerIndex = i
				break
			}
			index -= uint64(h.count)
		}
	}

	node, err := getArrayNodeFromStorageID(a.storage, id)
	if err != nil {
		return err
	}

	err = node.Insert(index, v)
	if err != nil {
		return err
	}

	a.header.count++

	if node.IsFull() {
		return a.SplitChildNode(node, headerIndex)
	}

	return nil
}

func (a *ArrayMetaDataSlab) Remove(index uint64) (Storable, error) {

	if index > uint64(a.header.count) {
		return nil, fmt.Errorf("remove at index %d out of bounds", index)
	}

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

	node, err := getArrayNodeFromStorageID(a.storage, id)
	if err != nil {
		return nil, err
	}

	v, err := node.Remove(index)
	if err != nil {
		return nil, err
	}

	a.header.count--

	if underflowSize, isUnderflow := node.IsUnderflow(); isUnderflow {
		err = a.MergeOrRebalanceChildNode(node, headerIndex, underflowSize)
		if err != nil {
			return nil, err
		}
	}

	return v, nil
}

func (a *ArrayMetaDataSlab) SplitChildNode(node ArrayNode, nodeHeaderIndex int) error {
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
func (a *ArrayMetaDataSlab) MergeOrRebalanceChildNode(node ArrayNode, nodeHeaderIndex int, underflowSize uint32) error {

	// left and right siblings of the same parent.
	var leftSib, rightSib ArrayNode
	if nodeHeaderIndex > 0 {
		leftSibID := a.orderedHeaders[nodeHeaderIndex-1].id

		var err error
		leftSib, err = getArrayNodeFromStorageID(a.storage, leftSibID)
		if err != nil {
			return err
		}
	}
	if nodeHeaderIndex < len(a.orderedHeaders)-1 {
		rightSibID := a.orderedHeaders[nodeHeaderIndex+1].id

		var err error
		rightSib, err = getArrayNodeFromStorageID(a.storage, rightSibID)
		if err != nil {
			return err
		}
	}

	leftCanLend := leftSib != nil && leftSib.CanLendToRight(underflowSize)
	rightCanLend := rightSib != nil && rightSib.CanLendToLeft(underflowSize)

	// Node can rebalance elements with at least one sibling.
	// Parent node doesn't need to be modified for rebalancing.
	if leftCanLend || rightCanLend {

		// Rebalance with right sib
		if !leftCanLend {
			return node.BorrowFromRight(rightSib)
		}

		// Rebalance with left sib
		if !rightCanLend {
			return leftSib.LendToRight(node)
		}

		// Rebalance with bigger sib
		if leftSib.Header().size > rightSib.Header().size {
			return leftSib.LendToRight(node)
		}

		return node.BorrowFromRight(rightSib)
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
	} else {
		err := node.Merge(rightSib)
		if err != nil {
			return err
		}

		// Update MetaDataSlab
		copy(a.orderedHeaders[nodeHeaderIndex+1:], a.orderedHeaders[nodeHeaderIndex+2:])
		a.orderedHeaders = a.orderedHeaders[:len(a.orderedHeaders)-1]
	}

	a.header.size -= headerSize

	return nil
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
	a.header.size = uint32(leftSlabHeaderLen) * headerSize

	// Update slab b header
	b.header.count = 0
	for i := 0; i < len(b.orderedHeaders); i++ {
		b.header.count += b.orderedHeaders[i].count
	}
	b.header.size = uint32(rightSlabHeaderLen) * headerSize

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
	a.header.size = uint32(leftSlabHeaderLen) * headerSize

	// Update slab b header
	b.header.count = 0
	for i := 0; i < len(b.orderedHeaders); i++ {
		b.header.count += b.orderedHeaders[i].count
	}
	b.header.size = uint32(rightSlabHeaderLen) * headerSize

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

func (a *ArrayMetaDataSlab) ID() StorageID {
	return a.header.id
}

func (a *ArrayMetaDataSlab) String() string {
	var elemsStr []string
	for _, h := range a.orderedHeaders {
		node, err := getArrayNodeFromStorageID(a.storage, h.id)
		if err != nil {
			return err.Error()
		}
		elemsStr = append(elemsStr, node.String())
	}
	return strings.Join(elemsStr, " ")
}

func NewArray(storage SlabStorage) *Array {
	return &Array{storage: storage}
}

func (array *Array) Get(i uint64) (Storable, error) {
	if array.root == nil {
		return nil, fmt.Errorf("out of bounds")
	}
	v, err := array.root.Get(i)
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
	if array.root == nil {
		return fmt.Errorf("out of bounds")
	}
	if v.ByteSize() > uint32(maxInlineElementSize) {
		v = StorageIDValue(v.ID())
	}
	return array.root.Set(index, v)
}

func (array *Array) Append(v Storable) error {
	return array.Insert(array.Count(), v)
}

func (array *Array) Insert(index uint64, v Storable) error {
	if v.ByteSize() > uint32(maxInlineElementSize) {
		v = StorageIDValue(v.ID())
	}
	if array.root == nil {
		if index != 0 {
			return fmt.Errorf("out of bounds")
		}
		array.root = newArrayDataSlab(array.storage)
		err := array.root.Insert(0, v)
		if err != nil {
			return err
		}
		array.dataSlabStorageID = array.root.Header().id
		return nil
	}

	err := array.root.Insert(index, v)
	if err != nil {
		return err
	}

	if array.root.IsFull() {

		copiedRoot := array.root.ShallowCloneWithNewID(array.storage)

		// Split copied root node
		left, right, err := copiedRoot.Split()
		if err != nil {
			return err
		}

		if array.root.IsLeaf() {

			// Create new ArrayMetaDataSlab with the same storage ID as root
			array.root = &ArrayMetaDataSlab{
				storage: array.storage,
				header: &SlabHeader{
					id: array.root.Header().id,
				},
			}

			array.storage.Store(array.root.Header().id, array.root)

			array.dataSlabStorageID = left.Header().id
		}

		root := array.root.(*ArrayMetaDataSlab)
		root.orderedHeaders = []*SlabHeader{left.Header(), right.Header()}
		root.header.count = left.Header().count + right.Header().count
		root.header.size = headerSize * 2
	}

	return nil
}

func (array *Array) Remove(index uint64) (Storable, error) {
	if array.root == nil {
		return nil, fmt.Errorf("out of bounds")
	}

	v, err := array.root.Remove(index)
	if err != nil {
		return nil, err
	}

	if array.root.IsLeaf() {
		// Set root to nil if tree is empty.
		root := array.root.(*ArrayDataSlab)
		if len(root.elements) == 0 {
			array.root = nil
			array.dataSlabStorageID = StorageIDUndefined
		}
		return v, nil
	}

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

		array.root = node

		if _, ok := array.root.(*ArrayDataSlab); ok {
			array.dataSlabStorageID = oldRootID
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
	if array.root == nil {
		return 0
	}
	return uint64(array.root.Header().count)
}

func (array *Array) String() string {
	if array.root == nil {
		return "[]"
	}
	if array.root.IsLeaf() {
		return array.root.String()
	}
	return fmt.Sprintf("[%s]", array.root.String())
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
