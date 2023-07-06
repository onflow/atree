/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2021 Dapper Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package atree

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

const (
	slabIDSize = 16

	// version and flag size: version (1 byte) + flag (1 byte)
	versionAndFlagSize = 2

	// slab header size: slab id (16 bytes) + count (4 bytes) + size (4 bytes)
	arraySlabHeaderSize = slabIDSize + 4 + 4

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + child header count (2 bytes)
	arrayMetaDataSlabPrefixSize = versionAndFlagSize + 2

	// version (1 byte) + flag (1 byte) + next id (16 bytes) + CBOR array size (3 bytes)
	// up to 65535 array elements are supported
	arrayDataSlabPrefixSize = versionAndFlagSize + slabIDSize + 3

	// version (1 byte) + flag (1 byte) + CBOR array size (3 bytes)
	// up to 65535 array elements are supported
	arrayRootDataSlabPrefixSize = versionAndFlagSize + 3

	// 32 is faster than 24 and 40.
	linearScanThreshold = 32
)

type ArraySlabHeader struct {
	slabID SlabID // id is used to retrieve slab from storage
	size   uint32 // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count  uint32 // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

type ArrayExtraData struct {
	TypeInfo TypeInfo // array type
}

// ArrayDataSlab is leaf node, implementing ArraySlab.
type ArrayDataSlab struct {
	next     SlabID
	header   ArraySlabHeader
	elements []Storable

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *ArrayExtraData
}

func (a *ArrayDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	if a.extraData == nil {
		return nil, NewNotValueError(a.SlabID())
	}
	return &Array{
		Storage: storage,
		root:    a,
	}, nil
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

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *ArrayExtraData
}

var _ ArraySlab = &ArrayMetaDataSlab{}

func (a *ArrayMetaDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	if a.extraData == nil {
		return nil, NewNotValueError(a.SlabID())
	}
	return &Array{
		Storage: storage,
		root:    a,
	}, nil
}

type ArraySlab interface {
	Slab

	Get(storage SlabStorage, index uint64) (Storable, error)
	Set(storage SlabStorage, address Address, index uint64, value Value) (Storable, error)
	Insert(storage SlabStorage, address Address, index uint64, value Value) error
	Remove(storage SlabStorage, index uint64) (Storable, error)

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetSlabID(SlabID)

	Header() ArraySlabHeader

	ExtraData() *ArrayExtraData
	RemoveExtraData() *ArrayExtraData
	SetExtraData(*ArrayExtraData)

	PopIterate(SlabStorage, ArrayPopIterationFunc) error
}

// Array is tree
type Array struct {
	Storage SlabStorage
	root    ArraySlab
}

var _ Value = &Array{}

func (a *Array) Address() Address {
	return a.root.SlabID().address
}

func (a *Array) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
	return SlabIDStorable(a.SlabID()), nil
}

const arrayExtraDataLength = 1

func newArrayExtraDataFromData(
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayExtraData,
	[]byte,
	error,
) {
	// Check data length
	if len(data) < versionAndFlagSize {
		return nil, data, NewDecodingErrorf("data is too short for array extra data")
	}

	// Check flag
	flag := data[1]
	if !isRoot(flag) {
		return nil, data, NewDecodingErrorf("array extra data has invalid flag 0x%x, want root flag", flag)
	}

	// Decode extra data

	dec := decMode.NewByteStreamDecoder(data[versionAndFlagSize:])

	length, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, data, NewDecodingError(err)
	}

	if length != arrayExtraDataLength {
		return nil, data, NewDecodingError(
			fmt.Errorf(
				"data has invalid length %d, want %d",
				length,
				arrayExtraDataLength,
			))
	}

	typeInfo, err := decodeTypeInfo(dec)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfoDecoder callback.
		return nil, data, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode type info")
	}

	// Reslice for remaining data
	n := dec.NumBytesDecoded()
	data = data[versionAndFlagSize+n:]

	return &ArrayExtraData{
		TypeInfo: typeInfo,
	}, data, nil
}

// Encode encodes extra data to the given encoder.
//
// Header (2 bytes):
//
//	+-----------------------------+--------------------------+
//	| extra data version (1 byte) | extra data flag (1 byte) |
//	+-----------------------------+--------------------------+
//
// Content (for now):
//
//	CBOR encoded array of extra data: cborArray{type info}
//
// Extra data flag is the same as the slab flag it prepends.
func (a *ArrayExtraData) Encode(enc *Encoder, flag byte) error {
	// Encode version
	enc.Scratch[0] = 0

	// Encode flag
	enc.Scratch[1] = flag

	// Write scratch content to encoder
	_, err := enc.Write(enc.Scratch[:versionAndFlagSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data
	err = enc.CBOR.EncodeArrayHead(arrayExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	err = a.TypeInfo.Encode(enc.CBOR)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfo interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode type info")
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func newArrayDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for array data slab")
	}

	isRootSlab := isRoot(data[1])

	var extraData *ArrayExtraData

	// Check flag for extra data
	if isRootSlab {
		// Decode extra data
		var err error
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// err is categorized already by newArrayExtraDataFromData.
			return nil, err
		}
	}

	minDataLength := arrayDataSlabPrefixSize
	if isRootSlab {
		minDataLength = arrayRootDataSlabPrefixSize
	}

	// Check data length (after decoding extra data if present)
	if len(data) < minDataLength {
		return nil, NewDecodingErrorf("data is too short for array data slab")
	}

	// Check flag
	flag := data[1]

	if getSlabArrayType(flag) != slabArrayData {
		return nil, NewDecodingErrorf(
			"data has invalid flag 0x%x, want 0x%x",
			flag,
			maskArrayData,
		)
	}

	var next SlabID

	var contentOffset int

	if !isRootSlab {

		// Decode next slab ID
		const nextSlabIDOffset = versionAndFlagSize
		var err error
		next, err = NewSlabIDFromRawBytes(data[nextSlabIDOffset:])
		if err != nil {
			// error returned from NewSlabIDFromRawBytes is categorized already.
			return nil, err
		}

		contentOffset = nextSlabIDOffset + slabIDSize

	} else {
		contentOffset = versionAndFlagSize
	}

	// Decode content (CBOR array)
	cborDec := decMode.NewByteStreamDecoder(data[contentOffset:])

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec, SlabIDUndefined)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode array element")
		}
		elements[i] = storable
	}

	header := ArraySlabHeader{
		slabID: id,
		size:   uint32(len(data)),
		count:  uint32(elemCount),
	}

	return &ArrayDataSlab{
		next:      next,
		header:    header,
		elements:  elements,
		extraData: extraData,
	}, nil
}

// Encode encodes this array data slab to the given encoder.
//
// Header (18 bytes):
//
//	+-------------------------------+--------------------------------+
//	| slab version + flag (2 bytes) | next sib slab ID (16 bytes) |
//	+-------------------------------+--------------------------------+
//
// Content (for now):
//
//	CBOR encoded array of elements
//
// If this is root slab, extra data section is prepended to slab's encoded content.
// See ArrayExtraData.Encode() for extra data section format.
func (a *ArrayDataSlab) Encode(enc *Encoder) error {

	flag := maskArrayData

	if a.hasPointer() {
		flag = setHasPointers(flag)
	}

	// Encode extra data if present
	if a.extraData != nil {
		flag = setRoot(flag)

		err := a.extraData.Encode(enc, flag)
		if err != nil {
			// err is already categorized by ArrayExtraData.Encode().
			return err
		}
	}

	// Encode version
	enc.Scratch[0] = 0

	// Encode flag
	enc.Scratch[1] = flag

	var contentOffset int

	// Encode next slab ID for non-root data slabs
	if a.extraData == nil {

		// Encode next slab ID to scratch
		const nextSlabIDOffset = versionAndFlagSize
		_, err := a.next.ToRawBytes(enc.Scratch[nextSlabIDOffset:])
		if err != nil {
			// Don't need to wrap because err is already categorized by SlabID.ToRawBytes().
			return err
		}

		contentOffset = nextSlabIDOffset + slabIDSize
	} else {
		contentOffset = versionAndFlagSize
	}

	// Encode CBOR array size manually for fix-sized encoding

	enc.Scratch[contentOffset] = 0x80 | 25

	countOffset := contentOffset + 1
	const countSize = 2
	binary.BigEndian.PutUint16(
		enc.Scratch[countOffset:],
		uint16(len(a.elements)),
	)

	// Write scratch content to encoder
	totalSize := countOffset + countSize
	_, err := enc.Write(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode data slab content (array of elements)
	for _, e := range a.elements {
		err = e.Encode(enc)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode array element")
		}
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (a *ArrayDataSlab) hasPointer() bool {
	for _, e := range a.elements {
		if _, ok := e.(SlabIDStorable); ok {
			return true
		}
	}
	return false
}

func (a *ArrayDataSlab) ChildStorables() []Storable {
	s := make([]Storable, len(a.elements))
	copy(s, a.elements)
	return s
}

func (a *ArrayDataSlab) Get(_ SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}
	return a.elements[index], nil
}

func (a *ArrayDataSlab) Set(storage SlabStorage, address Address, index uint64, value Value) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	oldElem := a.elements[index]
	oldSize := oldElem.ByteSize()

	storable, err := value.Storable(storage, address, maxInlineArrayElementSize)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
	}

	a.elements[index] = storable
	a.header.size = a.header.size - oldSize + storable.ByteSize()

	err = storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}

	return oldElem, nil
}

func (a *ArrayDataSlab) Insert(storage SlabStorage, address Address, index uint64, value Value) error {
	if index > uint64(len(a.elements)) {
		return NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
	}

	storable, err := value.Storable(storage, address, maxInlineArrayElementSize)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
	}

	if index == uint64(len(a.elements)) {
		a.elements = append(a.elements, storable)
	} else {
		a.elements = append(a.elements, nil)
		copy(a.elements[index+1:], a.elements[index:])
		a.elements[index] = storable
	}

	a.header.count++
	a.header.size += storable.ByteSize()

	err = storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}

	return nil
}

func (a *ArrayDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {
	if index >= uint64(len(a.elements)) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(len(a.elements)))
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

	err := storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}

	return v, nil
}

func (a *ArrayDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if len(a.elements) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, NewSlabSplitErrorf("ArrayDataSlab (%s) has less than 2 elements", a.header.slabID)
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
	sID, err := storage.GenerateSlabID(a.header.slabID.address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf(
				"failed to generate slab ID for address 0x%x",
				a.header.slabID.address,
			),
		)
	}
	rightSlabCount := len(a.elements) - leftCount
	rightSlab := &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: sID,
			size:   arrayDataSlabPrefixSize + dataSize - leftSize,
			count:  uint32(rightSlabCount),
		},
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
	a.next = rightSlab.header.slabID

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
func (a *ArrayDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > a.header.size {
		return uint32(minThreshold) - a.header.size, true
	}
	return 0, false
}

// CanLendToLeft returns true if elements on the left of the slab could be removed
// so that the slab still stores more than the min threshold.
func (a *ArrayDataSlab) CanLendToLeft(size uint32) bool {
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
func (a *ArrayDataSlab) CanLendToRight(size uint32) bool {
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

func (a *ArrayDataSlab) SetSlabID(id SlabID) {
	a.header.slabID = id
}

func (a *ArrayDataSlab) Header() ArraySlabHeader {
	return a.header
}

func (a *ArrayDataSlab) IsData() bool {
	return true
}

func (a *ArrayDataSlab) SlabID() SlabID {
	return a.header.slabID
}

func (a *ArrayDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *ArrayDataSlab) ExtraData() *ArrayExtraData {
	return a.extraData
}

func (a *ArrayDataSlab) RemoveExtraData() *ArrayExtraData {
	extraData := a.extraData
	a.extraData = nil
	return extraData
}

func (a *ArrayDataSlab) SetExtraData(extraData *ArrayExtraData) {
	a.extraData = extraData
}

func (a *ArrayDataSlab) PopIterate(_ SlabStorage, fn ArrayPopIterationFunc) error {

	// Iterate and reset elements backwards
	for i := len(a.elements) - 1; i >= 0; i-- {
		fn(a.elements[i])
	}

	// Reset data slab
	a.elements = nil
	a.header.count = 0
	a.header.size = arrayDataSlabPrefixSize

	return nil
}

func (a *ArrayDataSlab) String() string {
	elemsStr := make([]string, len(a.elements))
	for i, e := range a.elements {
		elemsStr[i] = fmt.Sprint(e)
	}

	return fmt.Sprintf("ArrayDataSlab id:%s size:%d count:%d elements: [%s]",
		a.header.slabID,
		a.header.size,
		a.header.count,
		strings.Join(elemsStr, " "),
	)
}

func newArrayMetaDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayMetaDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for array metadata slab")
	}

	var extraData *ArrayExtraData

	// Check flag for extra data
	if isRoot(data[1]) {
		// Decode extra data
		var err error
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap because err is already categorized by newArrayExtraDataFromData().
			return nil, err
		}
	}

	// Check data length (after decoding extra data if present)
	if len(data) < arrayMetaDataSlabPrefixSize {
		return nil, NewDecodingErrorf("data is too short for array metadata slab")
	}

	// Check flag
	flag := data[1]
	if getSlabArrayType(flag) != slabArrayMeta {
		return nil, NewDecodingErrorf(
			"data has invalid flag 0x%x, want 0x%x",
			flag,
			maskArrayMeta,
		)
	}

	// Decode number of child headers
	const childHeaderCountOffset = versionAndFlagSize
	childHeaderCount := binary.BigEndian.Uint16(data[childHeaderCountOffset:])

	expectedDataLength := arrayMetaDataSlabPrefixSize + arraySlabHeaderSize*int(childHeaderCount)
	if len(data) != expectedDataLength {
		return nil, NewDecodingErrorf(
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
		slabID, err := NewSlabIDFromRawBytes(data[offset:])
		if err != nil {
			// Don't need to wrap because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		countOffset := offset + slabIDSize
		count := binary.BigEndian.Uint32(data[countOffset:])

		sizeOffset := countOffset + 4
		size := binary.BigEndian.Uint32(data[sizeOffset:])

		totalCount += count

		childrenHeaders[i] = ArraySlabHeader{
			slabID: slabID,
			count:  count,
			size:   size,
		}
		childrenCountSum[i] = totalCount

		offset += arraySlabHeaderSize
	}

	header := ArraySlabHeader{
		slabID: id,
		size:   uint32(len(data)),
		count:  totalCount,
	}

	return &ArrayMetaDataSlab{
		header:           header,
		childrenHeaders:  childrenHeaders,
		childrenCountSum: childrenCountSum,
		extraData:        extraData,
	}, nil
}

// Encode encodes this array meta-data slab to the given encoder.
//
// Header (4 bytes):
//
//	+-----------------------+--------------------+------------------------------+
//	| slab version (1 byte) | slab flag (1 byte) | child header count (2 bytes) |
//	+-----------------------+--------------------+------------------------------+
//
// Content (n * 16 bytes):
//
//	[[count, size, slab ID], ...]
//
// If this is root slab, extra data section is prepended to slab's encoded content.
// See ArrayExtraData.Encode() for extra data section format.
func (a *ArrayMetaDataSlab) Encode(enc *Encoder) error {

	flag := maskArrayMeta

	// Encode extra data if present
	if a.extraData != nil {
		flag = setRoot(flag)

		err := a.extraData.Encode(enc, flag)
		if err != nil {
			// Don't need to wrap because err is already categorized by ArrayExtraData.Encode().
			return err
		}
	}

	// Encode version
	enc.Scratch[0] = 0

	// Encode flag
	enc.Scratch[1] = flag

	// Encode child header count to scratch
	const childHeaderCountOffset = versionAndFlagSize
	binary.BigEndian.PutUint16(
		enc.Scratch[childHeaderCountOffset:],
		uint16(len(a.childrenHeaders)),
	)

	// Write scratch content to encoder
	const totalSize = childHeaderCountOffset + 2
	_, err := enc.Write(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode children headers
	for _, h := range a.childrenHeaders {
		_, err := h.slabID.ToRawBytes(enc.Scratch[:])
		if err != nil {
			// Don't need to wrap because err is already categorized by SlabID.ToRawBytes().
			return err
		}

		const countOffset = slabIDSize
		binary.BigEndian.PutUint32(enc.Scratch[countOffset:], h.count)

		const sizeOffset = countOffset + 4
		binary.BigEndian.PutUint32(enc.Scratch[sizeOffset:], h.size)

		const totalSize = sizeOffset + 4
		_, err = enc.Write(enc.Scratch[:totalSize])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	return nil
}

func (a *ArrayMetaDataSlab) ChildStorables() []Storable {

	childIDs := make([]Storable, len(a.childrenHeaders))

	for i, h := range a.childrenHeaders {
		childIDs[i] = SlabIDStorable(h.slabID)
	}

	return childIDs
}

// TODO: improve naming
func (a *ArrayMetaDataSlab) childSlabIndexInfo(
	index uint64,
) (
	childHeaderIndex int,
	adjustedIndex uint64,
	childID SlabID,
	err error,
) {
	if index >= uint64(a.header.count) {
		return 0, 0, SlabID{}, NewIndexOutOfBoundsError(index, 0, uint64(a.header.count))
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
	childID = childHeader.slabID

	return childHeaderIndex, adjustedIndex, childID, nil
}

func (a *ArrayMetaDataSlab) Get(storage SlabStorage, index uint64) (Storable, error) {

	_, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}

	// Don't need to wrap error as external error because err is already categorized by ArraySlab.Get().
	return child.Get(storage, adjustedIndex)
}

func (a *ArrayMetaDataSlab) Set(storage SlabStorage, address Address, index uint64, value Value) (Storable, error) {

	childHeaderIndex, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}

	existingElem, err := child.Set(storage, address, adjustedIndex, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Set().
		return nil, err
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Update may increase or decrease the size,
	// check if full and for underflow

	if child.IsFull() {
		err = a.SplitChildSlab(storage, child, childHeaderIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.SplitChildSlab().
			return nil, err
		}
		return existingElem, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err = a.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, err
		}
		return existingElem, nil
	}

	err = storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}
	return existingElem, nil
}

// Insert inserts v into the correct child slab.
// index must be >=0 and <= a.header.count.
// If index == a.header.count, Insert appends v to the end of underlying slab.
func (a *ArrayMetaDataSlab) Insert(storage SlabStorage, address Address, index uint64, value Value) error {
	if index > uint64(a.header.count) {
		return NewIndexOutOfBoundsError(index, 0, uint64(a.header.count))
	}

	var childID SlabID
	var childHeaderIndex int
	var adjustedIndex uint64
	if index == uint64(a.header.count) {
		childHeaderIndex = len(a.childrenHeaders) - 1
		h := a.childrenHeaders[childHeaderIndex]
		childID = h.slabID
		adjustedIndex = uint64(h.count)
	} else {
		var err error
		childHeaderIndex, adjustedIndex, childID, err = a.childSlabIndexInfo(index)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
			return err
		}
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return err
	}

	err = child.Insert(storage, address, adjustedIndex, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Insert().
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
		// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.SplitChildSlab().
		return a.SplitChildSlab(storage, child, childHeaderIndex)
	}

	// Insertion always increases the size,
	// so there is no need to check underflow

	err = storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}

	return nil
}

func (a *ArrayMetaDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {

	if index >= uint64(a.header.count) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(a.header.count))
	}

	childHeaderIndex, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}

	v, err := child.Remove(storage, adjustedIndex)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Remove().
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
			// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, err
		}
	}

	// Removal always decreases the size,
	// so there is no need to check isFull

	err = storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}

	return v, nil
}

func (a *ArrayMetaDataSlab) SplitChildSlab(storage SlabStorage, child ArraySlab, childHeaderIndex int) error {
	leftSlab, rightSlab, err := child.Split(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Split().
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
	err = storage.Store(left.SlabID(), left)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", left.SlabID()))
	}

	err = storage.Store(right.SlabID(), right)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", right.SlabID()))
	}

	err = storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}

	return nil
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
func (a *ArrayMetaDataSlab) MergeOrRebalanceChildSlab(
	storage SlabStorage,
	child ArraySlab,
	childHeaderIndex int,
	underflowSize uint32,
) error {

	// Retrieve left and right siblings of the same parent.
	var leftSib, rightSib ArraySlab
	if childHeaderIndex > 0 {
		leftSibID := a.childrenHeaders[childHeaderIndex-1].slabID

		var err error
		leftSib, err = getArraySlab(storage, leftSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return err
		}
	}
	if childHeaderIndex < len(a.childrenHeaders)-1 {
		rightSibID := a.childrenHeaders[childHeaderIndex+1].slabID

		var err error
		rightSib, err = getArraySlab(storage, rightSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
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
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.BorrowFromRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex] = child.Header()
			a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex] = baseCountSum + child.Header().count

			// Store modified slabs
			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}
			err = storage.Store(rightSib.SlabID(), rightSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", rightSib.SlabID()))
			}
			err = storage.Store(a.header.slabID, a)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
			}
			return nil
		}

		// Rebalance with left sib
		if !rightCanLend {
			baseCountSum := a.childrenCountSum[childHeaderIndex-1] - leftSib.Header().count

			err := leftSib.LendToRight(child)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.LendToRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex-1] = baseCountSum + leftSib.Header().count

			// Store modified slabs
			err = storage.Store(leftSib.SlabID(), leftSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
			}
			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}
			err = storage.Store(a.header.slabID, a)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
			}
			return nil
		}

		// Rebalance with bigger sib
		if leftSib.ByteSize() > rightSib.ByteSize() {
			baseCountSum := a.childrenCountSum[childHeaderIndex-1] - leftSib.Header().count

			err := leftSib.LendToRight(child)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.LendToRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex-1] = baseCountSum + leftSib.Header().count

			// Store modified slabs
			err = storage.Store(leftSib.SlabID(), leftSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
			}
			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}
			err = storage.Store(a.header.slabID, a)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
			}
			return nil
		} else {
			// leftSib.ByteSize() <= rightSib.ByteSize

			baseCountSum := a.childrenCountSum[childHeaderIndex] - child.Header().count

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.BorrowFromRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex] = child.Header()
			a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex] = baseCountSum + child.Header().count

			// Store modified slabs
			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}
			err = storage.Store(rightSib.SlabID(), rightSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", rightSib.SlabID()))
			}
			err = storage.Store(a.header.slabID, a)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
			}
			return nil
		}
	}

	// Child can't rebalance with any sibling.  It must merge with one sibling.

	if leftSib == nil {

		// Merge with right
		err := child.Merge(rightSib)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.Merge().
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
		err = storage.Store(child.SlabID(), child)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
		}

		err = storage.Store(a.header.slabID, a)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
		}

		// Remove right sib from storage
		err = storage.Remove(rightSib.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", rightSib.SlabID()))
		}

		return nil
	}

	if rightSib == nil {

		// Merge with left
		err := leftSib.Merge(child)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.Merge().
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
		err = storage.Store(leftSib.SlabID(), leftSib)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
		}

		err = storage.Store(a.header.slabID, a)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
		}

		// Remove child from storage
		err = storage.Remove(child.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", child.SlabID()))
		}

		return nil
	}

	// Merge with smaller sib
	if leftSib.ByteSize() < rightSib.ByteSize() {
		err := leftSib.Merge(child)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.Merge().
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
		err = storage.Store(leftSib.SlabID(), leftSib)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
		}
		err = storage.Store(a.header.slabID, a)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
		}

		// Remove child from storage
		err = storage.Remove(child.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", child.SlabID()))
		}
		return nil

	} else {
		// leftSib.ByteSize > rightSib.ByteSize

		err := child.Merge(rightSib)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.Merge().
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
		err = storage.Store(child.SlabID(), child)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
		}
		err = storage.Store(a.header.slabID, a)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
		}

		// Remove rightSib from storage
		err = storage.Remove(rightSib.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", rightSib.SlabID()))
		}

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
		return nil, nil, NewSlabSplitErrorf("ArrayMetaDataSlab (%s) has less than 2 child headers", a.header.slabID)
	}

	leftChildrenCount := int(math.Ceil(float64(len(a.childrenHeaders)) / 2))
	leftSize := leftChildrenCount * arraySlabHeaderSize

	leftCount := uint32(0)
	for i := 0; i < leftChildrenCount; i++ {
		leftCount += a.childrenHeaders[i].count
	}

	// Construct right slab
	sID, err := storage.GenerateSlabID(a.header.slabID.address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", a.header.slabID.address))
	}

	rightSlab := &ArrayMetaDataSlab{
		header: ArraySlabHeader{
			slabID: sID,
			size:   a.header.size - uint32(leftSize),
			count:  a.header.count - leftCount,
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

func (a *ArrayMetaDataSlab) SetSlabID(id SlabID) {
	a.header.slabID = id
}

func (a *ArrayMetaDataSlab) Header() ArraySlabHeader {
	return a.header
}

func (a *ArrayMetaDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *ArrayMetaDataSlab) SlabID() SlabID {
	return a.header.slabID
}

func (a *ArrayMetaDataSlab) ExtraData() *ArrayExtraData {
	return a.extraData
}

func (a *ArrayMetaDataSlab) RemoveExtraData() *ArrayExtraData {
	extraData := a.extraData
	a.extraData = nil
	return extraData
}

func (a *ArrayMetaDataSlab) SetExtraData(extraData *ArrayExtraData) {
	a.extraData = extraData
}

func (a *ArrayMetaDataSlab) PopIterate(storage SlabStorage, fn ArrayPopIterationFunc) error {

	// Iterate child slabs backwards
	for i := len(a.childrenHeaders) - 1; i >= 0; i-- {

		childID := a.childrenHeaders[i].slabID

		child, err := getArraySlab(storage, childID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return err
		}

		err = child.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.PopIterate().
			return err
		}

		// Remove child slab
		err = storage.Remove(childID)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
		}
	}

	// All child slabs are removed.

	// Reset meta data slab
	a.childrenCountSum = nil
	a.childrenHeaders = nil
	a.header.count = 0
	a.header.size = arrayMetaDataSlabPrefixSize

	return nil
}

func (a *ArrayMetaDataSlab) String() string {
	elemsStr := make([]string, len(a.childrenHeaders))
	for i, h := range a.childrenHeaders {
		elemsStr[i] = fmt.Sprintf("{id:%s size:%d count:%d}", h.slabID, h.size, h.count)
	}

	return fmt.Sprintf("ArrayMetaDataSlab id:%s size:%d count:%d children: [%s]",
		a.header.slabID,
		a.header.size,
		a.header.count,
		strings.Join(elemsStr, " "),
	)
}

func NewArray(storage SlabStorage, address Address, typeInfo TypeInfo) (*Array, error) {

	extraData := &ArrayExtraData{TypeInfo: typeInfo}

	sID, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	root := &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: sID,
			size:   arrayRootDataSlabPrefixSize,
		},
		extraData: extraData,
	}

	err = storage.Store(root.header.slabID, root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", root.header.slabID))
	}

	return &Array{
		Storage: storage,
		root:    root,
	}, nil
}

func NewArrayWithRootID(storage SlabStorage, rootID SlabID) (*Array, error) {
	if rootID == SlabIDUndefined {
		return nil, NewSlabIDErrorf("cannot create Array from undefined slab ID")
	}

	root, err := getArraySlab(storage, rootID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}

	extraData := root.ExtraData()
	if extraData == nil {
		return nil, NewNotValueError(rootID)
	}

	return &Array{
		Storage: storage,
		root:    root,
	}, nil
}

func (a *Array) Get(i uint64) (Value, error) {
	storable, err := a.root.Get(a.Storage, i)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Get().
		return nil, err
	}
	v, err := storable.StoredValue(a.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}
	return v, nil
}

func (a *Array) Set(index uint64, value Value) (Storable, error) {
	existingStorable, err := a.root.Set(a.Storage, a.Address(), index, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Set().
		return nil, err
	}

	if a.root.IsFull() {
		err = a.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by Array.splitRoot().
			return nil, err
		}
		return existingStorable, nil
	}

	if !a.root.IsData() {
		root := a.root.(*ArrayMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err = a.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by Array.promoteChildAsNewRoot().
				return nil, err
			}
		}
	}

	return existingStorable, nil
}

func (a *Array) Append(value Value) error {
	// Don't need to wrap error as external error because err is already categorized by Array.Insert().
	return a.Insert(a.Count(), value)
}

func (a *Array) Insert(index uint64, value Value) error {
	err := a.root.Insert(a.Storage, a.Address(), index, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Insert().
		return err
	}

	if a.root.IsFull() {
		// Don't need to wrap error as external error because err is already categorized by Array.splitRoot().
		return a.splitRoot()
	}

	return nil
}

func (a *Array) Remove(index uint64) (Storable, error) {
	storable, err := a.root.Remove(a.Storage, index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Remove().
		return nil, err
	}

	if !a.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := a.root.(*ArrayMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err = a.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by Array.promoteChildAsNewRoot().
				return nil, err
			}
		}
	}

	return storable, nil
}

func (a *Array) splitRoot() error {

	if a.root.IsData() {
		// Adjust root data slab size before splitting
		dataSlab := a.root.(*ArrayDataSlab)
		dataSlab.header.size = dataSlab.header.size - arrayRootDataSlabPrefixSize + arrayDataSlabPrefixSize
	}

	// Get old root's extra data and reset it to nil in old root
	extraData := a.root.RemoveExtraData()

	// Save root node id
	rootID := a.root.SlabID()

	// Assign a new slab ID to old root before splitting it.
	sID, err := a.Storage.GenerateSlabID(a.Address())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", a.Address()))
	}

	oldRoot := a.root
	oldRoot.SetSlabID(sID)

	// Split old root
	leftSlab, rightSlab, err := oldRoot.Split(a.Storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Split().
		return err
	}

	left := leftSlab.(ArraySlab)
	right := rightSlab.(ArraySlab)

	// Create new ArrayMetaDataSlab with the old root's slab ID
	newRoot := &ArrayMetaDataSlab{
		header: ArraySlabHeader{
			slabID: rootID,
			count:  left.Header().count + right.Header().count,
			size:   arrayMetaDataSlabPrefixSize + arraySlabHeaderSize*2,
		},
		childrenHeaders:  []ArraySlabHeader{left.Header(), right.Header()},
		childrenCountSum: []uint32{left.Header().count, left.Header().count + right.Header().count},
		extraData:        extraData,
	}

	a.root = newRoot

	err = a.Storage.Store(left.SlabID(), left)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", left.SlabID()))
	}
	err = a.Storage.Store(right.SlabID(), right)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", right.SlabID()))
	}
	err = a.Storage.Store(a.root.SlabID(), a.root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.root.SlabID()))
	}

	return nil
}

func (a *Array) promoteChildAsNewRoot(childID SlabID) error {

	child, err := getArraySlab(a.Storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return err
	}

	if child.IsData() {
		// Adjust data slab size before promoting non-root data slab to root
		dataSlab := child.(*ArrayDataSlab)
		dataSlab.header.size = dataSlab.header.size - arrayDataSlabPrefixSize + arrayRootDataSlabPrefixSize
	}

	extraData := a.root.RemoveExtraData()

	rootID := a.root.SlabID()

	a.root = child

	a.root.SetSlabID(rootID)

	a.root.SetExtraData(extraData)

	err = a.Storage.Store(rootID, a.root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", rootID))
	}
	err = a.Storage.Remove(childID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
	}

	return nil
}

var emptyArrayIterator = &ArrayIterator{}

type ArrayIterator struct {
	storage        SlabStorage
	id             SlabID
	dataSlab       *ArrayDataSlab
	index          int
	remainingCount int
}

func (i *ArrayIterator) Next() (Value, error) {
	if i.remainingCount == 0 {
		return nil, nil
	}

	if i.dataSlab == nil {
		if i.id == SlabIDUndefined {
			return nil, nil
		}

		slab, found, err := i.storage.Retrieve(i.id)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", i.id))
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(i.id, "slab not found during array iteration")
		}

		i.dataSlab = slab.(*ArrayDataSlab)
		i.index = 0
	}

	var element Value
	var err error
	if i.index < len(i.dataSlab.elements) {
		element, err = i.dataSlab.elements[i.index].StoredValue(i.storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		i.index++
	}

	if i.index >= len(i.dataSlab.elements) {
		i.id = i.dataSlab.next
		i.dataSlab = nil
	}

	i.remainingCount--

	return element, nil
}

func (a *Array) Iterator() (*ArrayIterator, error) {
	slab, err := firstArrayDataSlab(a.Storage, a.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstArrayDataSlab().
		return nil, err
	}

	return &ArrayIterator{
		storage:        a.Storage,
		id:             slab.SlabID(),
		dataSlab:       slab,
		remainingCount: int(a.Count()),
	}, nil
}

func (a *Array) RangeIterator(startIndex uint64, endIndex uint64) (*ArrayIterator, error) {
	count := a.Count()

	if startIndex > count || endIndex > count {
		return nil, NewSliceOutOfBoundsError(startIndex, endIndex, 0, count)
	}

	if startIndex > endIndex {
		return nil, NewInvalidSliceIndexError(startIndex, endIndex)
	}

	numberOfElements := endIndex - startIndex

	if numberOfElements == 0 {
		return emptyArrayIterator, nil
	}

	var dataSlab *ArrayDataSlab
	index := startIndex

	if a.root.IsData() {
		dataSlab = a.root.(*ArrayDataSlab)
	} else if startIndex == 0 {
		var err error
		dataSlab, err = firstArrayDataSlab(a.Storage, a.root)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by firstArrayDataSlab().
			return nil, err
		}
	} else {
		var err error
		// getArrayDataSlabWithIndex returns data slab containing element at startIndex,
		// getArrayDataSlabWithIndex also returns adjusted index for this element at returned data slab.
		// Adjusted index must be used as index when creating ArrayIterator.
		dataSlab, index, err = getArrayDataSlabWithIndex(a.Storage, a.root, startIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArrayDataSlabWithIndex().
			return nil, err
		}
	}

	return &ArrayIterator{
		storage:        a.Storage,
		id:             dataSlab.SlabID(),
		dataSlab:       dataSlab,
		index:          int(index),
		remainingCount: int(numberOfElements),
	}, nil
}

type ArrayIterationFunc func(element Value) (resume bool, err error)

func (a *Array) Iterate(fn ArrayIterationFunc) error {

	iterator, err := a.Iterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Array.Iterator().
		return err
	}

	for {
		value, err := iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayIterator.Next().
			return err
		}
		if value == nil {
			return nil
		}
		resume, err := fn(value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ArrayIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}

func (a *Array) IterateRange(startIndex uint64, endIndex uint64, fn ArrayIterationFunc) error {

	iterator, err := a.RangeIterator(startIndex, endIndex)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Array.RangeIterator().
		return err
	}

	for {
		value, err := iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayIterator.Next().
			return err
		}
		if value == nil {
			return nil
		}
		resume, err := fn(value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ArrayIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}
func (a *Array) Count() uint64 {
	return uint64(a.root.Header().count)
}

func (a *Array) SlabID() SlabID {
	return a.root.SlabID()
}

func (a *Array) ValueID() ValueID {
	sid := a.SlabID()

	var id ValueID
	copy(id[:], sid.address[:])
	copy(id[8:], sid.index[:])

	return id
}

func (a *Array) Type() TypeInfo {
	if extraData := a.root.ExtraData(); extraData != nil {
		return extraData.TypeInfo
	}
	return nil
}

func (a *Array) String() string {
	iterator, err := a.Iterator()
	if err != nil {
		return err.Error()
	}

	var elemsStr []string
	for {
		v, err := iterator.Next()
		if err != nil {
			return err.Error()
		}
		if v == nil {
			break
		}
		elemsStr = append(elemsStr, fmt.Sprintf("%s", v))
	}

	return fmt.Sprintf("[%s]", strings.Join(elemsStr, " "))
}

func getArraySlab(storage SlabStorage, id SlabID) (ArraySlab, error) {
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		// err can be an external error because storage is an interface.
		return nil, wrapErrorAsExternalErrorIfNeeded(err)
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "array slab not found")
	}
	arraySlab, ok := slab.(ArraySlab)
	if !ok {
		return nil, NewSlabDataErrorf("slab %s isn't ArraySlab", id)
	}
	return arraySlab, nil
}

func firstArrayDataSlab(storage SlabStorage, slab ArraySlab) (*ArrayDataSlab, error) {
	if slab.IsData() {
		return slab.(*ArrayDataSlab), nil
	}
	meta := slab.(*ArrayMetaDataSlab)
	firstChildID := meta.childrenHeaders[0].slabID
	firstChild, err := getArraySlab(storage, firstChildID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}
	// Don't need to wrap error as external error because err is already categorized by firstArrayDataSlab().
	return firstArrayDataSlab(storage, firstChild)
}

// getArrayDataSlabWithIndex returns data slab containing element at specified index
func getArrayDataSlabWithIndex(storage SlabStorage, slab ArraySlab, index uint64) (*ArrayDataSlab, uint64, error) {
	if slab.IsData() {
		dataSlab := slab.(*ArrayDataSlab)
		if index >= uint64(len(dataSlab.elements)) {
			return nil, 0, NewIndexOutOfBoundsError(index, 0, uint64(len(dataSlab.elements)))
		}
		return dataSlab, index, nil
	}

	metaSlab := slab.(*ArrayMetaDataSlab)
	_, adjustedIndex, childID, err := metaSlab.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, 0, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, 0, err
	}

	// Don't need to wrap error as external error because err is already categorized by getArrayDataSlabWithIndex().
	return getArrayDataSlabWithIndex(storage, child, adjustedIndex)
}

type ArrayPopIterationFunc func(Storable)

// PopIterate iterates and removes elements backward.
// Each element is passed to ArrayPopIterationFunc callback before removal.
func (a *Array) PopIterate(fn ArrayPopIterationFunc) error {

	err := a.root.PopIterate(a.Storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.PopIterate().
		return err
	}

	rootID := a.root.SlabID()

	extraData := a.root.ExtraData()

	// Set root to empty data slab
	a.root = &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: rootID,
			size:   arrayRootDataSlabPrefixSize,
		},
		extraData: extraData,
	}

	// Save root slab
	err = a.Storage.Store(a.root.SlabID(), a.root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.root.SlabID()))
	}

	return nil
}

type ArrayElementProvider func() (Value, error)

func NewArrayFromBatchData(storage SlabStorage, address Address, typeInfo TypeInfo, fn ArrayElementProvider) (*Array, error) {

	var slabs []ArraySlab

	id, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	dataSlab := &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: id,
			size:   arrayDataSlabPrefixSize,
		},
	}

	// Batch append data by creating a list of ArrayDataSlab
	for {
		value, err := fn()
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ArrayElementProvider callback.
			return nil, wrapErrorAsExternalErrorIfNeeded(err)
		}
		if value == nil {
			break
		}

		// Finalize current data slab without appending new element
		if dataSlab.header.size >= uint32(targetThreshold) {

			// Generate storge id for next data slab
			nextID, err := storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(
					err,
					fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			// Save next slab's slab ID in data slab
			dataSlab.next = nextID

			// Append data slab to dataSlabs
			slabs = append(slabs, dataSlab)

			// Create next data slab
			dataSlab = &ArrayDataSlab{
				header: ArraySlabHeader{
					slabID: nextID,
					size:   arrayDataSlabPrefixSize,
				},
			}

		}

		storable, err := value.Storable(storage, address, maxInlineArrayElementSize)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Value interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
		}

		// Append new element
		dataSlab.elements = append(dataSlab.elements, storable)
		dataSlab.header.count++
		dataSlab.header.size += storable.ByteSize()
	}

	// Append last data slab to slabs
	slabs = append(slabs, dataSlab)

	for len(slabs) > 1 {

		lastSlab := slabs[len(slabs)-1]

		// Rebalance last slab if needed
		if underflowSize, underflow := lastSlab.IsUnderflow(); underflow {

			leftSib := slabs[len(slabs)-2]

			if leftSib.CanLendToRight(underflowSize) {

				// Rebalance with left
				err := leftSib.LendToRight(lastSlab)
				if err != nil {
					// Don't need to wrap error as external error because err is already categorized by ArraySlab.LeftToRight().
					return nil, err
				}

			} else {

				// Merge with left
				err := leftSib.Merge(lastSlab)
				if err != nil {
					// Don't need to wrap error as external error because err is already categorized by ArraySlab.Merge().
					return nil, err
				}

				// Remove last slab from slabs
				slabs[len(slabs)-1] = nil
				slabs = slabs[:len(slabs)-1]
			}
		}

		// All slabs are within target size range.

		if len(slabs) == 1 {
			// This happens when there were exactly two slabs and
			// last slab has merged with the first slab.
			break
		}

		// Store all slabs
		for _, slab := range slabs {
			err = storage.Store(slab.SlabID(), slab)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(
					err,
					fmt.Sprintf("failed to store slab %s", slab.SlabID()))
			}
		}

		// Get next level meta slabs
		slabs, err = nextLevelArraySlabs(storage, address, slabs)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by nextLevelArraySlabs().
			return nil, err
		}

	}

	// found root slab
	root := slabs[0]

	// root is data slab, adjust its size
	if dataSlab, ok := root.(*ArrayDataSlab); ok {
		dataSlab.header.size = dataSlab.header.size - arrayDataSlabPrefixSize + arrayRootDataSlabPrefixSize
	}

	extraData := &ArrayExtraData{TypeInfo: typeInfo}

	// Set extra data in root
	root.SetExtraData(extraData)

	// Store root
	err = storage.Store(root.SlabID(), root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", root.SlabID()))
	}

	return &Array{
		Storage: storage,
		root:    root,
	}, nil
}

// nextLevelArraySlabs returns next level meta data slabs from slabs.
// slabs must have at least 2 elements.  It is reused and returned as next level slabs.
// Caller is responsible for rebalance last slab and storing returned slabs in storage.
func nextLevelArraySlabs(storage SlabStorage, address Address, slabs []ArraySlab) ([]ArraySlab, error) {

	maxNumberOfHeadersInMetaSlab := (maxThreshold - arrayMetaDataSlabPrefixSize) / arraySlabHeaderSize

	nextLevelSlabsIndex := 0

	// Generate storge id
	id, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	metaSlab := &ArrayMetaDataSlab{
		header: ArraySlabHeader{
			slabID: id,
			size:   arrayMetaDataSlabPrefixSize,
		},
	}

	for _, slab := range slabs {

		if len(metaSlab.childrenHeaders) == int(maxNumberOfHeadersInMetaSlab) {

			slabs[nextLevelSlabsIndex] = metaSlab
			nextLevelSlabsIndex++

			// Generate storge id for next meta data slab
			id, err = storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(
					err,
					fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			metaSlab = &ArrayMetaDataSlab{
				header: ArraySlabHeader{
					slabID: id,
					size:   arrayMetaDataSlabPrefixSize,
				},
			}
		}

		metaSlab.header.size += arraySlabHeaderSize
		metaSlab.header.count += slab.Header().count

		metaSlab.childrenHeaders = append(metaSlab.childrenHeaders, slab.Header())
		metaSlab.childrenCountSum = append(metaSlab.childrenCountSum, metaSlab.header.count)
	}

	// Append last meta slab to slabs
	slabs[nextLevelSlabsIndex] = metaSlab
	nextLevelSlabsIndex++

	return slabs[:nextLevelSlabsIndex], nil
}
