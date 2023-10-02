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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

// NOTE: we use encoding size (in bytes) instead of Go type size for slab operations,
// such as merge and split, so size constants here are related to encoding size.
const (
	slabAddressSize = 8
	slabIndexSize   = 8
	slabIDSize      = slabAddressSize + slabIndexSize

	// version and flag size: version (1 byte) + flag (1 byte)
	versionAndFlagSize = 2

	// slab header size: slab index (8 bytes) + count (4 bytes) + size (2 bytes)
	// Support up to 4,294,967,295 elements in each array.
	// Support up to 65,535 bytes for slab size limit (default limit is 1536 max bytes).
	arraySlabHeaderSize = slabIndexSize + 4 + 2

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + address (8 bytes) + child header count (2 bytes)
	// Support up to 65,535 children per metadata slab.
	arrayMetaDataSlabPrefixSize = versionAndFlagSize + slabAddressSize + 2

	// Encoded element head in array data slab (fixed-size for easy computation).
	arrayDataSlabElementHeadSize = 3

	// non-root data slab prefix size: version (1 byte) + flag (1 byte) + next id (16 bytes) + element array head (3 bytes)
	// Support up to 65,535 elements in the array per data slab.
	arrayDataSlabPrefixSize = versionAndFlagSize + slabIDSize + arrayDataSlabElementHeadSize

	// root data slab prefix size: version (1 byte) + flag (1 byte) + element array head (3 bytes)
	// Support up to 65,535 elements in the array per data slab.
	arrayRootDataSlabPrefixSize = versionAndFlagSize + arrayDataSlabElementHeadSize

	// 32 is faster than 24 and 40.
	linearScanThreshold = 32

	// inlined tag number size: CBOR tag number CBORTagInlinedArray or CBORTagInlinedMap
	inlinedTagNumSize = 2

	// inlined CBOR array head size: CBOR array head of 3 elements (extra data index, value id, elements)
	inlinedCBORArrayHeadSize = 1

	// inlined extra data index size: CBOR positive number encoded in 2 bytes [0, 255] (fixed-size for easy computation)
	inlinedExtraDataIndexSize = 2

	// inlined CBOR byte string head size for value ID: CBOR byte string head for byte string of 8 bytes
	inlinedCBORValueIDHeadSize = 1

	// inlined value id size: encoded in 8 bytes
	inlinedValueIDSize = 8

	// inlined array data slab prefix size:
	//   tag number (2 bytes) +
	//   3-element array head (1 byte) +
	//   extra data index (2 bytes) [0, 255] +
	//   value ID index head (1 byte) +
	//   value ID index (8 bytes) +
	//   element array head (3 bytes)
	inlinedArrayDataSlabPrefixSize = inlinedTagNumSize +
		inlinedCBORArrayHeadSize +
		inlinedExtraDataIndexSize +
		inlinedCBORValueIDHeadSize +
		inlinedValueIDSize +
		arrayDataSlabElementHeadSize
)

type ArraySlabHeader struct {
	slabID SlabID // id is used to retrieve slab from storage
	size   uint32 // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count  uint32 // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

type ArrayExtraData struct {
	TypeInfo TypeInfo // array type
}

var _ ExtraData = &ArrayExtraData{}

// ArrayDataSlab is leaf node, implementing ArraySlab.
type ArrayDataSlab struct {
	next     SlabID
	header   ArraySlabHeader
	elements []Storable

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *ArrayExtraData

	// inlined indicates whether this slab is stored inlined in its parent slab.
	// This flag affects Encode(), ByteSize(), etc.
	inlined bool
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

	Inlined() bool
	Inlinable(maxInlineSize uint64) bool
	Inline(SlabStorage) error
	Uninline(SlabStorage) error
}

// Array is a heterogeneous variable-size array, storing any type of values
// into a smaller ordered list of values and provides efficient functionality
// to lookup, insert and remove elements anywhere in the array.
//
// Array elements can be stored in one or more relatively fixed-sized segments.
//
// Array can be inlined into its parent container when the entire content fits in
// parent container's element size limit.  Specifically, array with one segment
// which fits in size limit can be inlined, while arrays with multiple segments
// can't be inlined.
type Array struct {
	Storage SlabStorage
	root    ArraySlab

	// parentUpdater is a callback that notifies parent container when this array is modified.
	// If this callback is nil, this array has no parent.  Otherwise, this array has parent
	// and this callback must be used when this array is changed by Append, Insert, Set, Remove, etc.
	//
	// parentUpdater acts like "parent pointer".  It is not stored physically and is only in memory.
	// It is setup when child array is returned from parent's Get.  It is also setup when
	// new child is added to parent through Set or Insert.
	parentUpdater parentUpdater

	// mutableElementIndex tracks index of mutable element, such as Array and OrderedMap.
	// This is needed by mutable element to properly update itself through parentUpdater.
	// WARNING: since mutableElementIndex is created lazily, we need to create mutableElementIndex
	// if it is nil before adding/updating elements.  Range, delete, and read are no-ops on nil Go map.
	// TODO: maybe optimize by replacing map to get faster updates.
	mutableElementIndex map[ValueID]uint64
}

var _ Value = &Array{}
var _ mutableValueNotifier = &Array{}

func (a *Array) Address() Address {
	return a.root.SlabID().address
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
	dec := decMode.NewByteStreamDecoder(data)

	extraData, err := newArrayExtraData(dec, decodeTypeInfo)
	if err != nil {
		return nil, data, err
	}

	return extraData, data[dec.NumBytesDecoded():], nil
}

// newArrayExtraData decodes CBOR array to extra data:
//
//	cborArray{type info}
func newArrayExtraData(dec *cbor.StreamDecoder, decodeTypeInfo TypeInfoDecoder) (*ArrayExtraData, error) {
	length, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if length != arrayExtraDataLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"array extra data has invalid length %d, want %d",
				length,
				arrayExtraDataLength,
			))
	}

	typeInfo, err := decodeTypeInfo(dec)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfoDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode type info")
	}

	return &ArrayExtraData{TypeInfo: typeInfo}, nil
}

func (a *ArrayExtraData) isExtraData() bool {
	return true
}

// Encode encodes extra data as CBOR array:
//
//	[type info]
func (a *ArrayExtraData) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeArrayHead(arrayExtraDataLength)
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

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if h.getSlabArrayType() != slabArrayData {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want array data slab flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newArrayDataSlabFromDataV0(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	case 1:
		return newArrayDataSlabFromDataV1(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for array data slab", h.version())
	}
}

// newArrayDataSlabFromDataV0 decodes data in version 0:
//
// Root DataSlab Header:
//
//	+-------------------------------+------------+-------------------------------+
//	| slab version + flag (2 bytes) | extra data | slab version + flag (2 bytes) |
//	+-------------------------------+------------+-------------------------------+
//
// Non-root DataSlab Header (18 bytes):
//
//	+-------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | next sib slab ID (16 bytes) |
//	+-------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded array of elements
//
// See ArrayExtraData.Encode() for extra data section format.
func newArrayDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayDataSlab,
	error,
) {

	var err error
	var extraData *ArrayExtraData

	// Check flag for extra data
	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// err is categorized already by newArrayExtraDataFromData.
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		data = data[versionAndFlagSize:]
	}

	var next SlabID
	if !h.isRoot() {
		// Check data length for next slab ID
		if len(data) < slabIDSize {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		// Decode next slab ID
		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// error returned from NewSlabIDFromRawBytes is categorized already.
			return nil, err
		}

		data = data[slabIDSize:]
	}

	// Check data length for array element head
	if len(data) < arrayDataSlabElementHeadSize {
		return nil, NewDecodingErrorf("data is too short for array data slab")
	}

	// Decode content (CBOR array)
	cborDec := decMode.NewByteStreamDecoder(data)

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	// Compute slab size for version 1.
	slabSize := uint32(arrayDataSlabPrefixSize)
	if h.isRoot() {
		slabSize = arrayRootDataSlabPrefixSize
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec, id, nil)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode array element")
		}
		elements[i] = storable
		slabSize += storable.ByteSize()
	}

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
		count:  uint32(elemCount),
	}

	return &ArrayDataSlab{
		next:      next,
		header:    header,
		elements:  elements,
		extraData: extraData,
	}, nil
}

// newArrayDataSlabFromDataV1 decodes data in version 1:
//
// DataSlab Header:
//
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | extra data (if root) | inlined extra data (if present) | next slab ID (if non-empty) |
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded array of elements
//
// See ArrayExtraData.Encode() for extra data section format.
// See InlinedExtraData.Encode() for inlined extra data section format.
func newArrayDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte, // data doesn't include head (first two bytes)
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayDataSlab,
	error,
) {
	var err error
	var extraData *ArrayExtraData
	var inlinedExtraData []ExtraData
	var next SlabID

	// Decode extra data
	if h.isRoot() {
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// err is categorized already by newArrayExtraDataFromData.
			return nil, err
		}
	}

	// Decode inlined slab extra data
	if h.hasInlinedSlabs() {
		inlinedExtraData, data, err = newInlinedExtraDataFromData(
			data,
			decMode,
			decodeStorable,
			decodeTypeInfo,
		)
		if err != nil {
			// err is categorized already by newInlinedExtraDataFromData.
			return nil, err
		}
	}

	// Decode next slab ID
	if h.hasNextSlabID() {
		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// error returned from NewSlabIDFromRawBytes is categorized already.
			return nil, err
		}

		data = data[slabIDSize:]
	}

	// Check minimum data length after header
	if len(data) < arrayDataSlabElementHeadSize {
		return nil, NewDecodingErrorf("data is too short for array data slab")
	}

	// Decode content (CBOR array)
	cborDec := decMode.NewByteStreamDecoder(data)

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	slabSize := uint32(arrayDataSlabPrefixSize)
	if h.isRoot() {
		slabSize = arrayRootDataSlabPrefixSize
	}

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(cborDec, id, inlinedExtraData)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode array element")
		}
		elements[i] = storable
		slabSize += storable.ByteSize()
	}

	// Check if data reached EOF
	if cborDec.NumBytesDecoded() < len(data) {
		return nil, NewDecodingErrorf("data has %d bytes of extraneous data for array data slab", len(data)-cborDec.NumBytesDecoded())
	}

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
		count:  uint32(elemCount),
	}

	return &ArrayDataSlab{
		next:      next,
		header:    header,
		elements:  elements,
		extraData: extraData,
		inlined:   false, // this function is only called when slab is not inlined.
	}, nil
}

// DecodeInlinedArrayStorable decodes inlined array data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedArray, and tag contant
// as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
//
// NOTE: This function doesn't decode tag number because tag number is decoded
// in the caller and decoder only contains tag content.
func DecodeInlinedArrayStorable(
	dec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	parentSlabID SlabID,
	inlinedExtraData []ExtraData,
) (
	Storable,
	error,
) {
	const inlinedArrayDataSlabArrayCount = 3

	arrayCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != inlinedArrayDataSlabArrayCount {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined array data slab: expect %d elements, got %d elements",
				inlinedArrayDataSlabArrayCount,
				arrayCount))
	}

	// element 0: extra data index
	extraDataIndex, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if extraDataIndex >= uint64(len(inlinedExtraData)) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined array data slab: inlined extra data index %d exceeds number of inlined extra data %d",
				extraDataIndex,
				len(inlinedExtraData)))
	}

	extraData, ok := inlinedExtraData[extraDataIndex].(*ArrayExtraData)
	if !ok {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined array data slab: expect *ArrayExtraData, got %T",
				inlinedExtraData[extraDataIndex]))
	}

	// element 1: slab index
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if len(b) != slabIndexSize {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined array data slab: expect %d bytes for slab index, got %d bytes",
				slabIndexSize,
				len(b)))
	}

	var index [8]byte
	copy(index[:], b)

	slabID := NewSlabID(parentSlabID.address, index)

	// Decode array elements (CBOR array)
	elemCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	size := uint32(inlinedArrayDataSlabPrefixSize)

	elements := make([]Storable, elemCount)
	for i := 0; i < int(elemCount); i++ {
		storable, err := decodeStorable(dec, slabID, inlinedExtraData)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode array element")
		}
		elements[i] = storable

		size += storable.ByteSize()
	}

	header := ArraySlabHeader{
		slabID: slabID,
		size:   size,
		count:  uint32(elemCount),
	}

	return &ArrayDataSlab{
		header:   header,
		elements: elements,
		extraData: &ArrayExtraData{
			// Make a copy of extraData.TypeInfo because
			// inlined extra data are shared by all inlined slabs.
			TypeInfo: extraData.TypeInfo.Copy(),
		},
		inlined: true,
	}, nil
}

// encodeAsInlined encodes inlined array data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedArray,
// and tag contant as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
func (a *ArrayDataSlab) encodeAsInlined(enc *Encoder, inlinedTypeInfo *inlinedExtraData) error {
	if a.extraData == nil {
		return NewEncodingError(
			fmt.Errorf("failed to encode non-root array data slab as inlined"))
	}

	if !a.inlined {
		return NewEncodingError(
			fmt.Errorf("failed to encode standalone array data slab as inlined"))
	}

	extraDataIndex := inlinedTypeInfo.addArrayExtraData(a.extraData)

	if extraDataIndex > 255 {
		return NewEncodingError(
			fmt.Errorf("failed to encode inlined array data slab: extra data index %d exceeds limit 255", extraDataIndex))
	}

	var err error

	// Encode tag number and array head of 3 elements
	err = enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagInlinedArray,
		// array head of 3 elements
		0x83,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: extra data index
	// NOTE: encoded extra data index is fixed sized CBOR uint
	err = enc.CBOR.EncodeRawBytes([]byte{
		0x18,
		byte(extraDataIndex),
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 1: slab index
	err = enc.CBOR.EncodeBytes(a.header.slabID.index[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// element 2: array elements
	err = a.encodeElements(enc, inlinedTypeInfo)
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// Encode encodes this array data slab to the given encoder.
//
// DataSlab Header:
//
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | extra data (if root) | inlined extra data (if present) | next slab ID (if non-empty) |
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded array of elements
//
// See ArrayExtraData.Encode() for extra data section format.
// See InlinedExtraData.Encode() for inlined extra data section format.
func (a *ArrayDataSlab) Encode(enc *Encoder) error {

	if a.inlined {
		return NewEncodingError(
			fmt.Errorf("failed to encode inlined array data slab as standalone slab"))
	}

	// Encoding is done in two steps:
	//
	// 1. Encode array elements using a new buffer while collecting inlined extra data from inlined elements.
	// 2. Encode slab with deduplicated inlined extra data and copy encoded elements from previous buffer.

	inlinedTypes := newInlinedExtraData()

	// TODO: maybe use a buffer pool
	var elementBuf bytes.Buffer
	elementEnc := NewEncoder(&elementBuf, enc.encMode)

	err := a.encodeElements(elementEnc, inlinedTypes)
	if err != nil {
		// err is already categorized by Array.encodeElements().
		return err
	}

	err = elementEnc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	const version = 1

	h, err := newArraySlabHead(version, slabArrayData)
	if err != nil {
		return NewEncodingError(err)
	}

	if a.hasPointer() {
		h.setHasPointers()
	}

	if a.next != SlabIDUndefined {
		h.setHasNextSlabID()
	}

	if a.extraData != nil {
		h.setRoot()
	}

	if !inlinedTypes.empty() {
		h.setHasInlinedSlabs()
	}

	// Encode head (version + flag)
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data
	if a.extraData != nil {
		err = a.extraData.Encode(enc)
		if err != nil {
			// err is already categorized by ArrayExtraData.Encode().
			return err
		}
	}

	// Encode inlined extra data
	if !inlinedTypes.empty() {
		err = inlinedTypes.Encode(enc)
		if err != nil {
			// err is already categorized by inlinedExtraData.Encode().
			return err
		}
	}

	// Encode next slab ID
	if a.next != SlabIDUndefined {
		n, err := a.next.ToRawBytes(enc.Scratch[:])
		if err != nil {
			// Don't need to wrap because err is already categorized by SlabID.ToRawBytes().
			return err
		}

		_, err = enc.Write(enc.Scratch[:n])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode elements by copying raw bytes from previous buffer
	err = enc.CBOR.EncodeRawBytes(elementBuf.Bytes())
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (a *ArrayDataSlab) encodeElements(enc *Encoder, inlinedTypeInfo *inlinedExtraData) error {
	// Encode CBOR array size manually for fix-sized encoding

	enc.Scratch[0] = 0x80 | 25

	countOffset := 1
	const countSize = 2
	binary.BigEndian.PutUint16(
		enc.Scratch[countOffset:],
		uint16(len(a.elements)),
	)

	// Write scratch content to encoder
	totalSize := countOffset + countSize
	err := enc.CBOR.EncodeRawBytes(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode data slab content (array of elements)
	for _, e := range a.elements {
		err = encodeStorableAsElement(enc, e, inlinedTypeInfo)
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

func (a *ArrayDataSlab) Inlined() bool {
	return a.inlined
}

// Inlinable returns true if
// - array data slab is root slab
// - size of inlined array data slab <= maxInlineSize
func (a *ArrayDataSlab) Inlinable(maxInlineSize uint64) bool {
	if a.extraData == nil {
		// Non-root data slab is not inlinable.
		return false
	}

	// At this point, this data slab is either
	// - inlined data slab, or
	// - not inlined root data slab

	// Compute inlined size from cached slab size
	inlinedSize := a.header.size
	if !a.inlined {
		inlinedSize = inlinedSize -
			arrayRootDataSlabPrefixSize +
			inlinedArrayDataSlabPrefixSize
	}

	// Inlined byte size must be less than max inline size.
	return uint64(inlinedSize) <= maxInlineSize
}

// Inline converts not-inlined ArrayDataSlab to inlined ArrayDataSlab and removes it from storage.
func (a *ArrayDataSlab) Inline(storage SlabStorage) error {
	if a.inlined {
		return NewFatalError(fmt.Errorf("failed to inline ArrayDataSlab %s: it is inlined already", a.header.slabID))
	}

	id := a.header.slabID

	// Remove slab from storage because it is going to be inlined.
	err := storage.Remove(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
	}

	// Update data slab size as inlined slab.
	a.header.size = a.header.size -
		arrayRootDataSlabPrefixSize +
		inlinedArrayDataSlabPrefixSize

	// Update data slab inlined status.
	a.inlined = true

	return nil
}

// Uninline converts an inlined ArrayDataSlab to uninlined ArrayDataSlab and stores it in storage.
func (a *ArrayDataSlab) Uninline(storage SlabStorage) error {
	if !a.inlined {
		return NewFatalError(fmt.Errorf("failed to un-inline ArrayDataSlab %s: it is not inlined", a.header.slabID))
	}

	// Update data slab size
	a.header.size = a.header.size -
		inlinedArrayDataSlabPrefixSize +
		arrayRootDataSlabPrefixSize

	// Update data slab inlined status
	a.inlined = false

	// Store slab in storage
	err := storage.Store(a.header.slabID, a)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
	}

	return nil
}

func (a *ArrayDataSlab) hasPointer() bool {
	for _, e := range a.elements {
		if hasPointer(e) {
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

func (a *ArrayDataSlab) getPrefixSize() uint32 {
	if a.inlined {
		return inlinedArrayDataSlabPrefixSize
	}
	if a.extraData != nil {
		return arrayRootDataSlabPrefixSize
	}
	return arrayDataSlabPrefixSize
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

	storable, err := value.Storable(storage, address, maxInlineArrayElementSize)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
	}

	a.elements[index] = storable

	// Recompute slab size by adding all element sizes instead of using the size diff of old and new element because
	// oldElem can be the same storable when the same value is reset and oldElem.ByteSize() can equal storable.ByteSize().
	// Given this, size diff of the old and new element can be 0 even when its actual size changed.
	size := a.getPrefixSize()
	for _, e := range a.elements {
		size += e.ByteSize()
	}

	a.header.size = size

	if !a.inlined {
		err := storage.Store(a.header.slabID, a)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
		}
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

	if !a.inlined {
		err := storage.Store(a.header.slabID, a)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
		}
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

	if !a.inlined {
		err := storage.Store(a.header.slabID, a)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.header.slabID))
		}
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
	a.header.size = a.getPrefixSize()

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

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if h.getSlabArrayType() != slabArrayMeta {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want array metadata slab flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newArrayMetaDataSlabFromDataV0(id, h, data, decMode, decodeTypeInfo)

	case 1:
		return newArrayMetaDataSlabFromDataV1(id, h, data, decMode, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for array metadata slab", h.version())
	}
}

// newArrayMetaDataSlabFromDataV0 decodes data in version 0:
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | slab version + flag (2 byte) | child header count (2 bytes) |
//	+------------------------------+------------+------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (4 bytes):
//
//	+------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child header count (2 bytes) |
//	+------------------------------+------------------------------+
//
// Content (n * 24 bytes):
//
//	[[slab id (16 bytes), count (4 bytes), size (4 bytes)], ...]
//
// See ArrayExtraData.Encode() for extra data section format.
func newArrayMetaDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayMetaDataSlab,
	error,
) {
	// NOTE: the following encoded sizes are for version 0 only (changed in later version).
	const (
		// meta data children array head size: 2 bytes
		arrayMetaDataArrayHeadSizeV0 = 2

		// slab header size: slab id (16 bytes) + count (4 bytes) + size (4 bytes)
		arraySlabHeaderSizeV0 = slabIDSize + 4 + 4
	)

	var err error
	var extraData *ArrayExtraData

	if h.isRoot() {
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap because err is already categorized by newArrayExtraDataFromData().
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		data = data[versionAndFlagSize:]
	}

	// Check data length (after decoding extra data if present)
	if len(data) < arrayMetaDataArrayHeadSizeV0 {
		return nil, NewDecodingErrorf("data is too short for array metadata slab")
	}

	// Decode number of child headers
	childHeaderCount := binary.BigEndian.Uint16(data)

	data = data[arrayMetaDataArrayHeadSizeV0:]

	expectedDataLength := arraySlabHeaderSizeV0 * int(childHeaderCount)
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
	offset := 0

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

		offset += arraySlabHeaderSizeV0
	}

	// Compute slab size in version 1.
	slabSize := arrayMetaDataSlabPrefixSize + arraySlabHeaderSize*uint32(childHeaderCount)

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
		count:  totalCount,
	}

	return &ArrayMetaDataSlab{
		header:           header,
		childrenHeaders:  childrenHeaders,
		childrenCountSum: childrenCountSum,
		extraData:        extraData,
	}, nil
}

// newArrayMetaDataSlabFromDataV1 decodes data in version 1:
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+------------+--------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (12 bytes):
//
//	+------------------------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+--------------------------------+------------------------------+
//
// Content (n * 14 bytes):
//
//	[[slab index (8 bytes), count (4 bytes), size (2 bytes)], ...]
//
// See ArrayExtraData.Encode() for extra data section format.
func newArrayMetaDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayMetaDataSlab,
	error,
) {
	var err error
	var extraData *ArrayExtraData

	if h.isRoot() {
		extraData, data, err = newArrayExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap because err is already categorized by newArrayExtraDataFromData().
			return nil, err
		}
	}

	// Check minimum data length after version, flag, and extra data are processed
	minLength := arrayMetaDataSlabPrefixSize - versionAndFlagSize
	if len(data) < minLength {
		return nil, NewDecodingErrorf("data is too short for array metadata slab")
	}

	offset := 0

	// Decode shared address of headers
	var address Address
	copy(address[:], data[offset:])
	offset += slabAddressSize

	// Decode number of child headers
	const arrayHeaderSize = 2
	childHeaderCount := binary.BigEndian.Uint16(data[offset:])
	offset += arrayHeaderSize

	expectedDataLength := arraySlabHeaderSize * int(childHeaderCount)
	if len(data[offset:]) != expectedDataLength {
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

	for i := 0; i < int(childHeaderCount); i++ {
		// Decode slab index
		var index SlabIndex
		copy(index[:], data[offset:])

		slabID := SlabID{address, index}
		offset += slabIndexSize

		// Decode count
		count := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		// Decode size
		size := binary.BigEndian.Uint16(data[offset:])
		offset += 2

		childrenHeaders[i] = ArraySlabHeader{
			slabID: slabID,
			count:  count,
			size:   uint32(size),
		}

		totalCount += count
		childrenCountSum[i] = totalCount
	}

	slabSize := arrayMetaDataSlabPrefixSize + arraySlabHeaderSize*uint32(childHeaderCount)

	header := ArraySlabHeader{
		slabID: id,
		size:   slabSize,
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
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+------------+--------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (12 bytes):
//
//	+------------------------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+--------------------------------+------------------------------+
//
// Content (n * 14 bytes):
//
//	[[slab index (8 bytes), count (4 bytes), size (2 bytes)], ...]
//
// See ArrayExtraData.Encode() for extra data section format.
func (a *ArrayMetaDataSlab) Encode(enc *Encoder) error {

	const version = 1

	h, err := newArraySlabHead(version, slabArrayMeta)
	if err != nil {
		return NewEncodingError(err)
	}

	if a.extraData != nil {
		h.setRoot()
	}

	// Write head (version + flag)
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data if present
	if a.extraData != nil {
		err = a.extraData.Encode(enc)
		if err != nil {
			// Don't need to wrap because err is already categorized by ArrayExtraData.Encode().
			return err
		}
	}

	// Encode shared address to scratch
	copy(enc.Scratch[:], a.header.slabID.address[:])

	// Encode child header count to scratch
	const childHeaderCountOffset = slabAddressSize
	binary.BigEndian.PutUint16(
		enc.Scratch[childHeaderCountOffset:],
		uint16(len(a.childrenHeaders)),
	)

	// Write scratch content to encoder
	const totalSize = childHeaderCountOffset + 2
	_, err = enc.Write(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode children headers
	for _, h := range a.childrenHeaders {
		// Encode slab index to scratch
		copy(enc.Scratch[:], h.slabID.index[:])

		// Encode count
		const countOffset = slabIndexSize
		binary.BigEndian.PutUint32(enc.Scratch[countOffset:], h.count)

		// Encode size
		const sizeOffset = countOffset + 4
		binary.BigEndian.PutUint16(enc.Scratch[sizeOffset:], uint16(h.size))

		const totalSize = sizeOffset + 2
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

func (a *ArrayMetaDataSlab) Inlined() bool {
	return false
}

func (a *ArrayMetaDataSlab) Inlinable(_ uint64) bool {
	return false
}

func (a *ArrayMetaDataSlab) Inline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to inline ArrayMetaDataSlab %s: ArrayMetaDataSlab can't be inlined", a.header.slabID))
}

func (a *ArrayMetaDataSlab) Uninline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to uninline ArrayMetaDataSlab %s: ArrayMetaDataSlab is already unlined", a.header.slabID))
}

func (a *ArrayMetaDataSlab) IsData() bool {
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

// TODO: maybe optimize this
func (a *Array) incrementIndexFrom(index uint64) error {
	// Although range loop over Go map is not deterministic, it is OK
	// to use here because this operation is free of side-effect and
	// leads to the same results independent of map order.
	for id, i := range a.mutableElementIndex {
		if i >= index {
			if a.mutableElementIndex[id]+1 >= a.Count() {
				return NewFatalError(fmt.Errorf("failed to increment index of ValueID %s in array %s: new index exceeds array count", id, a.ValueID()))
			}
			a.mutableElementIndex[id]++
		}
	}
	return nil
}

// TODO: maybe optimize this
func (a *Array) decrementIndexFrom(index uint64) error {
	// Although range loop over Go map is not deterministic, it is OK
	// to use here because this operation is free of side-effect and
	// leads to the same results independent of map order.
	for id, i := range a.mutableElementIndex {
		if i > index {
			if a.mutableElementIndex[id] <= 0 {
				return NewFatalError(fmt.Errorf("failed to decrement index of ValueID %s in array %s: new index < 0", id, a.ValueID()))
			}
			a.mutableElementIndex[id]--
		}
	}
	return nil
}

func (a *Array) getIndexByValueID(id ValueID) (uint64, bool) {
	index, exist := a.mutableElementIndex[id]
	return index, exist
}

func (a *Array) setParentUpdater(f parentUpdater) {
	a.parentUpdater = f
}

// setCallbackWithChild sets up callback function with child value (child)
// so parent array (a) can be notified when child value is modified.
func (a *Array) setCallbackWithChild(i uint64, child Value, maxInlineSize uint64) {
	c, ok := child.(mutableValueNotifier)
	if !ok {
		return
	}

	vid := c.ValueID()

	// mutableElementIndex is lazily initialized.
	if a.mutableElementIndex == nil {
		a.mutableElementIndex = make(map[ValueID]uint64)
	}

	// Index i will be updated with array operations, which affects element index.
	a.mutableElementIndex[vid] = i

	c.setParentUpdater(func() (found bool, err error) {

		// Avoid unnecessary write operation on parent container.
		// Child value was stored as SlabIDStorable (not inlined) in parent container,
		// and continues to be stored as SlabIDStorable (still not inlinable),
		// so no update to parent container is needed.
		if !c.Inlined() && !c.Inlinable(maxInlineSize) {
			return true, nil
		}

		// Get latest adjusted index by child value ID.
		adjustedIndex, exist := a.getIndexByValueID(vid)
		if !exist {
			return false, nil
		}

		storable, err := a.root.Get(a.Storage, adjustedIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.Get().
			return false, err
		}

		// Verify retrieved element is either SlabIDStorable or Slab, with identical value ID.
		switch x := storable.(type) {
		case SlabIDStorable:
			sid := SlabID(x)
			if !vid.equal(sid) {
				return false, nil
			}

		case Slab:
			sid := x.SlabID()
			if !vid.equal(sid) {
				return false, nil
			}

		default:
			return false, nil
		}

		// Set child value with parent array using updated index.
		// Set() calls c.Storable() which returns inlined or not-inlined child storable.
		existingValueStorable, err := a.set(adjustedIndex, c)
		if err != nil {
			return false, err
		}

		// Verify overwritten storable has identical value ID.

		switch x := existingValueStorable.(type) {
		case SlabIDStorable:
			sid := SlabID(x)
			if !vid.equal(sid) {
				return false, NewFatalError(
					fmt.Errorf(
						"failed to reset child value in parent updater callback: overwritten SlabIDStorable %s != value ID %s",
						sid,
						vid))
			}

		case Slab:
			sid := x.SlabID()
			if !vid.equal(sid) {
				return false, NewFatalError(
					fmt.Errorf(
						"failed to reset child value in parent updater callback: overwritten Slab ID %s != value ID %s",
						sid,
						vid))
			}

		case nil:
			return false, NewFatalError(
				fmt.Errorf(
					"failed to reset child value in parent updater callback: overwritten value is nil"))

		default:
			return false, NewFatalError(
				fmt.Errorf(
					"failed to reset child value in parent updater callback: overwritten value is wrong type %T",
					existingValueStorable))
		}

		return true, nil
	})
}

// notifyParentIfNeeded calls parent updater if this array (a) is a child element in another container.
func (a *Array) notifyParentIfNeeded() error {
	if a.parentUpdater == nil {
		return nil
	}

	// If parentUpdater() doesn't find child array (a), then no-op on parent container
	// and unset parentUpdater callback in child array.  This can happen when child
	// array is an outdated reference (removed or overwritten in parent container).
	found, err := a.parentUpdater()
	if err != nil {
		return err
	}
	if !found {
		a.parentUpdater = nil
	}
	return nil
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

	// As a parent, this array (a) sets up notification callback with child
	// value (v) so this array can be notified when child value is modified.
	a.setCallbackWithChild(i, v, maxInlineArrayElementSize)

	return v, nil
}

func (a *Array) Set(index uint64, value Value) (Storable, error) {
	existingStorable, err := a.set(index, value)
	if err != nil {
		return nil, err
	}

	var existingValueID ValueID

	// If overwritten storable is an inlined slab, uninline the slab and store it in storage.
	// This is to prevent potential data loss because the overwritten inlined slab was not in
	// storage and any future changes to it would have been lost.
	switch s := existingStorable.(type) {
	case ArraySlab:
		err = s.Uninline(a.Storage)
		if err != nil {
			return nil, err
		}
		existingStorable = SlabIDStorable(s.SlabID())
		existingValueID = slabIDToValueID(s.SlabID())

	case MapSlab:
		err = s.Uninline(a.Storage)
		if err != nil {
			return nil, err
		}
		existingStorable = SlabIDStorable(s.SlabID())
		existingValueID = slabIDToValueID(s.SlabID())

	case SlabIDStorable:
		existingValueID = slabIDToValueID(SlabID(s))
	}

	// Remove overwritten array/map's ValueID from mutableElementIndex if:
	// - new value isn't array/map, or
	// - new value is array/map with different value ID
	if existingValueID != emptyValueID {
		newValue, ok := value.(mutableValueNotifier)
		if !ok || existingValueID != newValue.ValueID() {
			delete(a.mutableElementIndex, existingValueID)
		}
	}

	return existingStorable, nil
}

func (a *Array) set(index uint64, value Value) (Storable, error) {
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

	// This array (a) is a parent to the new child (value), and this array
	// can also be a child in another container.
	//
	// As a parent, this array needs to setup notification callback with
	// the new child value, so it can be notified when child is modified.
	//
	// If this array is a child, it needs to notify its parent because its
	// content (maybe also its size) is changed by this "Set" operation.

	// If this array is a child, it notifies parent by invoking callback because
	// this array is changed by setting new child.
	err = a.notifyParentIfNeeded()
	if err != nil {
		return nil, err
	}

	// As a parent, this array sets up notification callback with child value
	// so this array can be notified when child value is modified.
	//
	// Setting up notification with new child value can happen at any time
	// (either before or after this array notifies its parent) because
	// setting up notification doesn't trigger any read/write ops on parent or child.
	a.setCallbackWithChild(index, value, maxInlineArrayElementSize)

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
		err = a.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by Array.splitRoot().
			return err
		}
	}

	err = a.incrementIndexFrom(index)
	if err != nil {
		return err
	}

	// This array (a) is a parent to the new child (value), and this array
	// can also be a child in another container.
	//
	// As a parent, this array needs to setup notification callback with
	// the new child value, so it can be notified when child is modified.
	//
	// If this array is a child, it needs to notify its parent because its
	// content (also its size) is changed by this "Insert" operation.

	// If this array is a child, it notifies parent by invoking callback because
	// this array is changed by inserting new child.
	err = a.notifyParentIfNeeded()
	if err != nil {
		return err
	}

	// As a parent, this array sets up notification callback with child value
	// so this array can be notified when child value is modified.
	//
	// Setting up notification with new child value can happen at any time
	// (either before or after this array notifies its parent) because
	// setting up notification doesn't trigger any read/write ops on parent or child.
	a.setCallbackWithChild(index, value, maxInlineArrayElementSize)

	return nil
}

func (a *Array) Remove(index uint64) (Storable, error) {
	storable, err := a.remove(index)
	if err != nil {
		return nil, err
	}

	// If overwritten storable is an inlined slab, uninline the slab and store it in storage.
	// This is to prevent potential data loss because the overwritten inlined slab was not in
	// storage and any future changes to it would have been lost.
	switch s := storable.(type) {
	case ArraySlab:
		err = s.Uninline(a.Storage)
		if err != nil {
			return nil, err
		}
		storable = SlabIDStorable(s.SlabID())

		// Delete removed element ValueID from mutableElementIndex
		removedValueID := slabIDToValueID(s.SlabID())
		delete(a.mutableElementIndex, removedValueID)

	case MapSlab:
		err = s.Uninline(a.Storage)
		if err != nil {
			return nil, err
		}
		storable = SlabIDStorable(s.SlabID())

		// Delete removed element ValueID from mutableElementIndex
		removedValueID := slabIDToValueID(s.SlabID())
		delete(a.mutableElementIndex, removedValueID)

	case SlabIDStorable:
		// Delete removed element ValueID from mutableElementIndex
		removedValueID := slabIDToValueID(SlabID(s))
		delete(a.mutableElementIndex, removedValueID)
	}

	return storable, nil
}

func (a *Array) remove(index uint64) (Storable, error) {
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

	err = a.decrementIndexFrom(index)
	if err != nil {
		return nil, err
	}

	// If this array is a child, it notifies parent by invoking callback because
	// this array is changed by removing element.
	err = a.notifyParentIfNeeded()
	if err != nil {
		return nil, err
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

func (a *Array) Inlined() bool {
	return a.root.Inlined()
}

func (a *Array) Inlinable(maxInlineSize uint64) bool {
	return a.root.Inlinable(maxInlineSize)
}

// Storable returns array a as either:
// - SlabIDStorable, or
// - inlined data slab storable
func (a *Array) Storable(_ SlabStorage, _ Address, maxInlineSize uint64) (Storable, error) {

	inlined := a.root.Inlined()
	inlinable := a.root.Inlinable(maxInlineSize)

	switch {
	case inlinable && inlined:
		// Root slab is inlinable and was inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return a.root, nil

	case !inlinable && !inlined:
		// Root slab is not inlinable and was not inlined.
		// Return root slab ID as storable, no size adjustment and change to storage.
		return SlabIDStorable(a.SlabID()), nil

	case inlinable && !inlined:
		// Root slab is inlinable and was NOT inlined.

		// Inline root data slab.
		err := a.root.Inline(a.Storage)
		if err != nil {
			return nil, err
		}

		return a.root, nil

	case !inlinable && inlined:

		// Root slab is NOT inlinable and was previously inlined.

		// Uninline root slab.
		err := a.root.Uninline(a.Storage)
		if err != nil {
			return nil, err
		}

		return SlabIDStorable(a.SlabID()), nil

	default:
		panic("not reachable")
	}
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
	if a.root.Inlined() {
		return SlabIDUndefined
	}
	return a.root.SlabID()
}

func (a *Array) ValueID() ValueID {
	return slabIDToValueID(a.root.SlabID())
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

	inlined := a.root.Inlined()

	size := uint32(arrayRootDataSlabPrefixSize)
	if inlined {
		size = inlinedArrayDataSlabPrefixSize
	}

	// Set root to empty data slab
	a.root = &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: rootID,
			size:   size,
		},
		extraData: extraData,
		inlined:   inlined,
	}

	// Save root slab
	if !a.Inlined() {
		err = a.Storage.Store(a.root.SlabID(), a.root)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", a.root.SlabID()))
		}
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

type arrayLoadedElementIterator struct {
	storage SlabStorage
	slab    *ArrayDataSlab
	index   int
}

func (i *arrayLoadedElementIterator) next() (Value, error) {
	// Iterate loaded elements in data slab.
	for i.index < len(i.slab.elements) {
		element := i.slab.elements[i.index]
		i.index++

		v, err := getLoadedValue(i.storage, element)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getLoadedValue.
			return nil, err
		}
		if v == nil {
			// Skip this element because it references unloaded slab.
			// Try next element.
			continue
		}

		return v, nil
	}

	// Reach end of elements
	return nil, nil
}

type arrayLoadedSlabIterator struct {
	storage SlabStorage
	slab    *ArrayMetaDataSlab
	index   int
}

func (i *arrayLoadedSlabIterator) next() Slab {
	// Iterate loaded slabs in meta data slab.
	for i.index < len(i.slab.childrenHeaders) {
		header := i.slab.childrenHeaders[i.index]
		i.index++

		childSlab := i.storage.RetrieveIfLoaded(header.slabID)
		if childSlab == nil {
			// Skip this child because it references unloaded slab.
			// Try next child.
			continue
		}

		return childSlab
	}

	// Reach end of children.
	return nil
}

// ArrayLoadedValueIterator is used to iterate over loaded array elements.
type ArrayLoadedValueIterator struct {
	storage      SlabStorage
	parents      []*arrayLoadedSlabIterator // LIFO stack for parents of dataIterator
	dataIterator *arrayLoadedElementIterator
}

func (i *ArrayLoadedValueIterator) nextDataIterator() (*arrayLoadedElementIterator, error) {

	// Iterate parents (LIFO) to find next loaded array data slab.
	for len(i.parents) > 0 {
		lastParent := i.parents[len(i.parents)-1]

		nextChildSlab := lastParent.next()

		switch slab := nextChildSlab.(type) {
		case *ArrayDataSlab:
			// Create data iterator
			return &arrayLoadedElementIterator{
				storage: i.storage,
				slab:    slab,
			}, nil

		case *ArrayMetaDataSlab:
			// Push new parent to parents queue
			newParent := &arrayLoadedSlabIterator{
				storage: i.storage,
				slab:    slab,
			}
			i.parents = append(i.parents, newParent)

		case nil:
			// Reach end of last parent.
			// Reset last parent to nil and pop last parent from parents stack.
			lastParentIndex := len(i.parents) - 1
			i.parents[lastParentIndex] = nil
			i.parents = i.parents[:lastParentIndex]

		default:
			return nil, NewSlabDataErrorf("slab %s isn't ArraySlab", nextChildSlab.SlabID())
		}
	}

	// Reach end of parents stack.
	return nil, nil
}

// Next iterates and returns next loaded element.
// It returns nil Value at end of loaded elements.
func (i *ArrayLoadedValueIterator) Next() (Value, error) {
	if i.dataIterator != nil {
		element, err := i.dataIterator.next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by arrayLoadedElementIterator.next().
			return nil, err
		}
		if element != nil {
			return element, nil
		}

		// Reach end of element in current data slab.
		i.dataIterator = nil
	}

	// Get next data iterator.
	var err error
	i.dataIterator, err = i.nextDataIterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by arrayLoadedValueIterator.nextDataIterator().
		return nil, err
	}
	if i.dataIterator != nil {
		return i.Next()
	}

	// Reach end of loaded value iterator
	return nil, nil
}

// LoadedValueIterator returns iterator to iterate loaded array elements.
func (a *Array) LoadedValueIterator() (*ArrayLoadedValueIterator, error) {
	switch slab := a.root.(type) {

	case *ArrayDataSlab:
		// Create a data iterator from root slab.
		dataIterator := &arrayLoadedElementIterator{
			storage: a.Storage,
			slab:    slab,
		}

		// Create iterator with data iterator (no parents).
		iterator := &ArrayLoadedValueIterator{
			storage:      a.Storage,
			dataIterator: dataIterator,
		}

		return iterator, nil

	case *ArrayMetaDataSlab:
		// Create a slab iterator from root slab.
		slabIterator := &arrayLoadedSlabIterator{
			storage: a.Storage,
			slab:    slab,
		}

		// Create iterator with parent (data iterater is uninitialized).
		iterator := &ArrayLoadedValueIterator{
			storage: a.Storage,
			parents: []*arrayLoadedSlabIterator{slabIterator},
		}

		return iterator, nil

	default:
		return nil, NewSlabDataErrorf("slab %s isn't ArraySlab", slab.SlabID())
	}
}

// IterateLoadedValues iterates loaded array values.
func (a *Array) IterateLoadedValues(fn ArrayIterationFunc) error {
	iterator, err := a.LoadedValueIterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Array.LoadedValueIterator().
		return err
	}

	for {
		value, err := iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayLoadedValueIterator.Next().
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
