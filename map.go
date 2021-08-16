/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	digestKeySize = 8

	// slab header size: storage id (16 bytes) + size (4 bytes) + hashed first key (8 bytes)
	mapSlabHeaderSize = storageIDSize + 4 + digestKeySize

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + child header count (2 bytes)
	mapMetaDataSlabPrefixSize = 1 + 1 + 2

	// version (1 byte) + flag (1 byte) + prev id (16 bytes) + next id (16 bytes) + CBOR array size (3 bytes) for keys and values
	// (3 bytes of array size support up to 65535 array elements)
	mapDataSlabPrefixSize = 2 + storageIDSize + storageIDSize + 3
)

type MapKey Storable

type MapValue Storable

// element is one indivisible unit that must stay together (e.g. collision group)
type element interface {
	fmt.Stringer

	Get(storage SlabStorage, digester Digester, level int, hkey Digest, key ComparableValue) (MapValue, error)

	// Set returns updated element, which may be a different type of element because of hash collision.
	// Caller needs to save returned element.
	Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level int, hkey Digest, key ComparableValue, value MapValue) (elem element, isNewElem bool, err error)

	Size() uint32
}

// elementGroup is a group of elements that must stay together during splitting or rebalancing.
type elementGroup interface {
	element

	Inline() bool

	// Elements returns underlying elements.
	Elements(storage SlabStorage) (elements, error)
}

// elements is a list of elements.
type elements interface {
	fmt.Stringer

	Get(storage SlabStorage, digester Digester, level int, hkey Digest, key ComparableValue) (MapValue, error)
	Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level int, hkey Digest, key ComparableValue, value MapValue) (isNewElem bool, err error)

	Merge(elements) error
	Split() (elements, elements, error)

	LendToRight(elements) error
	BorrowFromRight(elements) error

	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	Element(int) (element, error)

	firstKey() Digest

	Count() uint32

	Size() uint32
}

type singleElement struct {
	key   MapKey
	value MapValue
	size  uint32
}

var _ element = &singleElement{}

type inlineCollisionGroup struct {
	elements
}

var _ element = &inlineCollisionGroup{}
var _ elementGroup = &inlineCollisionGroup{}

type externalCollisionGroup struct {
	id   StorageID
	size uint32
}

var _ element = &externalCollisionGroup{}
var _ elementGroup = &externalCollisionGroup{}

type hkeyElements struct {
	hkeys []Digest  // sorted list of unique hashed keys
	elems []element // elements corresponding to hkeys
	size  uint32    // total byte sizes
	level int
}

var _ elements = &hkeyElements{}

type singleElements struct {
	elems []*singleElement // list of key+value pairs
	size  uint32           // total key+value byte sizes
	level int
}

var _ elements = &singleElements{}

type MapSlabHeader struct {
	id       StorageID // id is used to retrieve slab from storage
	size     uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	firstKey Digest    // firstKey (first hashed key) is used to lookup value
}

type MapExtraData struct {
	_        struct{} `cbor:",toarray"`
	TypeInfo string
	Count    uint64
}

// MapDataSlab is leaf node, implementing MapSlab.
// anySize is true for data slab that isn't restricted by size requirement.
type MapDataSlab struct {
	prev   StorageID
	next   StorageID
	header MapSlabHeader

	elements

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *MapExtraData

	anySize bool
}

var _ MapSlab = &MapDataSlab{}

// MapMetaDataSlab is internal node, implementing MapSlab.
type MapMetaDataSlab struct {
	header          MapSlabHeader
	childrenHeaders []MapSlabHeader

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *MapExtraData
}

var _ MapSlab = &MapMetaDataSlab{}

type MapSlab interface {
	Slab
	fmt.Stringer

	Get(storage SlabStorage, digester Digester, level int, hkey Digest, key ComparableValue) (MapValue, error)
	Set(storage SlabStorage, b DigesterBuilder, digester Digester, level int, hkey Digest, key ComparableValue, value MapValue) (isNewElem bool, err error)
	Remove(storage SlabStorage, level int, hkey Digest, key ComparableValue) (MapValue, error)

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetID(StorageID)

	Header() MapSlabHeader

	ExtraData() *MapExtraData
	RemoveExtraData() *MapExtraData
	SetExtraData(*MapExtraData)
}

type OrderedMap struct {
	storage         SlabStorage
	root            MapSlab
	digesterBuilder DigesterBuilder
}

var _ Value = &OrderedMap{}

type MapSlabNotFoundError struct {
	id  StorageID
	err error
}

func (e MapSlabNotFoundError) Error() string {
	return fmt.Sprintf("failed to retrieve MapSlab %d: %v", e.id, e.err)
}

func newSingleElement(key MapKey, value MapValue) *singleElement {
	return &singleElement{
		key:   key,
		value: value,
		size:  key.ByteSize() + value.ByteSize(),
	}
}

func (e *singleElement) Get(storage SlabStorage, _ Digester, _ int, _ Digest, key ComparableValue) (MapValue, error) {
	kv, err := e.key.StoredValue(storage)
	if err != nil {
		return nil, err
	}

	if key.Equal(kv) {
		return e.value, nil
	}
	return nil, fmt.Errorf("key %s not found", key)
}

// Set updates value if key matches, otherwise returns inlineCollisionGroup with existing and new elements.
// NOTE: Existing key needs to be rehashed because we store minimum digest for non-collision element.
//       Rehashing only happens when we create new inlineCollisionGroup.
//       Adding new element to existing inlineCollisionGroup doesn't require rehashing.
func (e *singleElement) Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level int, hkey Digest, key ComparableValue, value MapValue) (element, bool, error) {

	v, err := e.key.StoredValue(storage)
	if err != nil {
		return nil, false, err
	}

	kv, ok := v.(ComparableValue)
	if !ok {
		return nil, false, errors.New("existing key doesn't implement ComparableValue interface")
	}

	if key.Equal(kv) {
		e.value = value
		e.size = e.key.ByteSize() + e.value.ByteSize()
		return e, false, nil
	}

	// Hash collision detected

	// Generate digest for existing key (see function comment)
	existingKeyDigest, err := b.Digest(kv)
	if err != nil {
		return nil, false, err
	}

	// Create collision group with existing and new elements
	var elements elements
	if level+1 == digester.Levels() {
		elements = &singleElements{level: level + 1}
	} else {
		elements = &hkeyElements{level: level + 1}
	}

	var newElem element

	newElem = &inlineCollisionGroup{elements: elements}

	newElem, _, err = newElem.Set(storage, address, b, existingKeyDigest, level, hkey, kv, e.value)
	if err != nil {
		return nil, false, err
	}

	newElem, _, err = newElem.Set(storage, address, b, digester, level, hkey, key, value)
	if err != nil {
		return nil, false, err
	}

	return newElem, true, nil
}

func (e *singleElement) Size() uint32 {
	return e.size
}

func (e *singleElement) String() string {
	return fmt.Sprintf("%s:%s", e.key, e.value)
}

func (e *inlineCollisionGroup) Get(storage SlabStorage, digester Digester, level int, _ Digest, key ComparableValue) (MapValue, error) {

	// Adjust level and hkey for collision group
	level += 1
	if level > digester.Levels() {
		panic(fmt.Sprintf("inlineCollisionGroup.Get() level %d, expect <= %d", level, digester.Levels()))
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey
	return e.elements.Get(storage, digester, level, hkey, key)
}

func (e *inlineCollisionGroup) Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level int, _ Digest, key ComparableValue, value MapValue) (element, bool, error) {

	// Adjust level and hkey for collision group
	level += 1
	if level > digester.Levels() {
		panic(fmt.Sprintf("inlineCollisionGroup.Set() level %d, expect <= %d", level, digester.Levels()))
	}
	hkey, _ := digester.Digest(level)

	isNewElem, err := e.elements.Set(storage, address, b, digester, level, hkey, key, value)
	if err != nil {
		return nil, false, err
	}

	if level == 1 {
		// Export oversized inline collision group to separete slab (external collision group)
		// for first level collision.
		if e.elements.Size() > uint32(MaxInlineElementSize) {

			id := storage.GenerateStorageID(address)

			// Create MapDataSlab
			slab := &MapDataSlab{
				header: MapSlabHeader{
					id:       id,
					size:     mapDataSlabPrefixSize + e.elements.Size(),
					firstKey: e.elements.firstKey(),
				},
				elements: e.elements, // elems shouldn't be copied
				anySize:  true,
			}

			err := storage.Store(id, slab)
			if err != nil {
				return nil, false, err
			}

			// Create and return externalCollisionGroup (wrapper of newly created MapDataSlab)
			return &externalCollisionGroup{
				id:   id,
				size: StorageIDStorable(id).ByteSize(),
			}, isNewElem, nil
		}
	}

	return e, isNewElem, nil
}

func (e *inlineCollisionGroup) Size() uint32 {
	return e.elements.Size()
}

func (e *inlineCollisionGroup) Inline() bool {
	return true
}

func (e *inlineCollisionGroup) Elements(_ SlabStorage) (elements, error) {
	return e.elements, nil
}

func (e *inlineCollisionGroup) String() string {
	return "inline [" + e.elements.String() + "]"
}

func (e *externalCollisionGroup) Get(storage SlabStorage, digester Digester, level int, _ Digest, key ComparableValue) (MapValue, error) {
	slab, err := getMapSlab(storage, e.id)
	if err != nil {
		return nil, err
	}

	// Adjust level and hkey for collision group
	level += 1
	if level > digester.Levels() {
		panic(fmt.Sprintf("externalCollisionGroup.Get() level %d, expect <= %d", level, digester.Levels()))
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey
	return slab.Get(storage, digester, level, hkey, key)
}

func (e *externalCollisionGroup) Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level int, _ Digest, key ComparableValue, value MapValue) (element, bool, error) {
	slab, err := getMapSlab(storage, e.id)
	if err != nil {
		return nil, false, err
	}

	// Adjust level and hkey for collision group
	level += 1
	if level > digester.Levels() {
		panic(fmt.Sprintf("externalCollisionGroup.Set() level %d, expect <= %d", level, digester.Levels()))
	}
	hkey, _ := digester.Digest(level)

	isNewElem, err := slab.Set(storage, b, digester, level, hkey, key, value)
	if err != nil {
		return nil, false, err
	}
	return e, isNewElem, nil
}

func (e *externalCollisionGroup) Size() uint32 {
	return e.size
}

func (e *externalCollisionGroup) Inline() bool {
	return false
}

func (e *externalCollisionGroup) Elements(storage SlabStorage) (elements, error) {
	slab, err := getMapSlab(storage, e.id)
	if err != nil {
		return nil, err
	}
	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return nil, fmt.Errorf("expect MapDataSlab, got %T", slab)
	}
	return dataSlab.elements, nil
}

func (e *externalCollisionGroup) String() string {
	return fmt.Sprintf("external group(%d)", e.id)
}

func (e *hkeyElements) Get(storage SlabStorage, digester Digester, level int, hkey Digest, key ComparableValue) (MapValue, error) {

	if level >= digester.Levels() {
		panic(fmt.Sprintf("hkeyElements.Get() level %d, expect < %d", level, digester.Levels()))
	}

	// binary search by hkey

	// Find index that e.hkeys[h] == hkey
	equalIndex := -1
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// No matching hkey
	if equalIndex == -1 {
		return nil, fmt.Errorf("key %s not found", key)
	}

	elem := e.elems[equalIndex]

	return elem.Get(storage, digester, level, hkey, key)
}

func (e *hkeyElements) Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level int, hkey Digest, key ComparableValue, value MapValue) (bool, error) {

	// Check hkeys are not empty
	if level >= digester.Levels() {
		panic(fmt.Sprintf("hkeyElements.Set() level %d, expect < %d", level, digester.Levels()))
	}

	ks, err := key.Storable(storage, address)
	if err != nil {
		return false, err
	}

	newElem := newSingleElement(ks, value)

	if len(e.hkeys) == 0 {
		// only element

		e.hkeys = []Digest{hkey}

		e.elems = []element{newElem}

		e.size += newElem.size

		return true, nil
	}

	if hkey < e.hkeys[0] {
		// prepend key and value

		e.hkeys = append(e.hkeys, Digest(0))
		copy(e.hkeys[1:], e.hkeys)
		e.hkeys[0] = hkey

		e.elems = append(e.elems, nil)
		copy(e.elems[1:], e.elems)
		e.elems[0] = newElem

		e.size += newElem.size

		return true, nil
	}

	if hkey > e.hkeys[len(e.hkeys)-1] {
		// append key and value

		e.hkeys = append(e.hkeys, hkey)

		e.elems = append(e.elems, newElem)

		e.size += newElem.size

		return true, nil
	}

	equalIndex := -1   // first index that m.hkeys[h] == hkey
	lessThanIndex := 0 // last index that m.hkeys[h] > hkey
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			lessThanIndex = h
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// Has matching hkey
	if equalIndex != -1 {

		elem := e.elems[equalIndex]

		oldElemSize := elem.Size()

		elem, isNewElem, err := elem.Set(storage, address, b, digester, level, hkey, key, value)
		if err != nil {
			return false, err
		}

		e.elems[equalIndex] = elem

		e.size += elem.Size() - oldElemSize

		return isNewElem, nil
	}

	// No matching hkey

	// insert into sorted hkeys
	e.hkeys = append(e.hkeys, Digest(0))
	copy(e.hkeys[lessThanIndex+1:], e.hkeys[lessThanIndex:])
	e.hkeys[lessThanIndex] = hkey

	// insert into sorted elements
	e.elems = append(e.elems, nil)
	copy(e.elems[lessThanIndex+1:], e.elems[lessThanIndex:])
	e.elems[lessThanIndex] = newElem

	e.size += newElem.Size()

	return true, nil
}

func (e *hkeyElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, errors.New("index out of bounds")
	}
	return e.elems[i], nil
}

func (e *hkeyElements) Merge(elems elements) error {

	rElems, ok := elems.(*hkeyElements)
	if !ok {
		panic(fmt.Sprintf("hkeyElements.Merge() elems type %T, want *hkeyElements", elems))
	}

	e.hkeys = append(e.hkeys, rElems.hkeys...)
	e.elems = append(e.elems, rElems.elems...)
	e.size += rElems.size

	// Set merged elements to nil to prevent memory leak
	for i := 0; i < len(rElems.elems); i++ {
		rElems.elems[i] = nil
	}

	return nil
}

func (e *hkeyElements) Split() (elements, elements, error) {
	if len(e.elems) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split elements with less than 2 elements")
	}

	// This computes the ceil of split to give the first slab more elements.
	dataSize := e.size
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, e := range e.elems {
		elemSize := e.Size()
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

	rightCount := len(e.elems) - leftCount

	// Create right slab elements
	rightElements := &hkeyElements{}

	rightElements.hkeys = make([]Digest, rightCount)
	copy(rightElements.hkeys, e.hkeys[leftCount:])

	rightElements.elems = make([]element, rightCount)
	copy(rightElements.elems, e.elems[leftCount:])

	rightElements.size = dataSize - leftSize

	e.hkeys = e.hkeys[:leftCount]
	e.elems = e.elems[:leftCount]
	e.size = leftSize

	// NOTE: prevent memory leak
	for i := leftCount; i < len(e.hkeys); i++ {
		e.elems[i] = nil
	}

	return e, rightElements, nil
}

// LendToRight rebalances elements by moving elements from left to right
func (e *hkeyElements) LendToRight(re elements) error {

	minSize := minThreshold - mapDataSlabPrefixSize

	rightElements := re.(*hkeyElements)

	if e.level != rightElements.level {
		panic(fmt.Sprintf("failed to lend elements because they have different levels %d vs %d", e.level, rightElements.level))
	}

	count := len(e.elems) + len(rightElements.elems)
	size := e.size + rightElements.size

	leftCount := len(e.elems)
	leftSize := e.size

	midPoint := (size + 1) >> 1

	// Left elements size is as close to midPoint as possible while right elements size >= minThreshold
	for i := len(e.elems) - 1; i >= 0; i-- {
		elemSize := e.elems[i].Size()
		if leftSize-elemSize < midPoint && size-leftSize >= uint32(minSize) {
			break
		}
		leftSize -= elemSize
		leftCount--
	}

	// Update the right elements
	//
	// It is easier and less error-prone to realloc elements for the right elements.

	hkeys := make([]Digest, count-leftCount)
	n := copy(hkeys, e.hkeys[leftCount:])
	copy(hkeys[n:], rightElements.hkeys)

	elements := make([]element, count-leftCount)
	n = copy(elements, e.elems[leftCount:])
	copy(elements[n:], rightElements.elems)

	rightElements.hkeys = hkeys
	rightElements.elems = elements
	rightElements.size = size - leftSize

	// Update left slab
	// NOTE: prevent memory leak
	for i := leftCount; i < len(e.elems); i++ {
		e.elems[i] = nil
	}
	e.hkeys = e.hkeys[:leftCount]
	e.elems = e.elems[:leftCount]
	e.size = leftSize

	return nil
}

// BorrowFromRight rebalances slabs by moving elements from right slab to left slab.
func (e *hkeyElements) BorrowFromRight(re elements) error {

	minSize := minThreshold - mapDataSlabPrefixSize

	rightElements := re.(*hkeyElements)

	if e.level != rightElements.level {
		panic(fmt.Sprintf("failed to borrow elements because they have different levels %d vs %d", e.level, rightElements.level))
	}

	size := e.size + rightElements.size

	leftCount := len(e.elems)
	leftSize := e.size

	midPoint := (size + 1) >> 1

	for _, e := range rightElements.elems {
		elemSize := e.Size()
		if leftSize+elemSize > midPoint {
			if size-leftSize-elemSize >= uint32(minSize) {
				// Include this element in left elements
				leftSize += elemSize
				leftCount++
			}
			break
		}
		leftSize += elemSize
		leftCount++
	}

	rightStartIndex := leftCount - len(e.elems)

	// Update left elements
	e.hkeys = append(e.hkeys, rightElements.hkeys[:rightStartIndex]...)
	e.elems = append(e.elems, rightElements.elems[:rightStartIndex]...)
	e.size = leftSize

	// Update right slab
	// TODO: copy elements to front instead?
	// NOTE: prevent memory leak
	for i := 0; i < rightStartIndex; i++ {
		rightElements.elems[i] = nil
	}
	rightElements.hkeys = rightElements.hkeys[rightStartIndex:]
	rightElements.elems = rightElements.elems[rightStartIndex:]
	rightElements.size = size - leftSize

	return nil
}

func (e *hkeyElements) CanLendToLeft(size uint32) bool {
	if len(e.elems) == 0 {
		panic("empty hkeyElements")
	}

	if len(e.elems) < 2 {
		return false
	}

	minSize := minThreshold - mapDataSlabPrefixSize
	if e.size-size < uint32(minSize) {
		return false
	}

	lendSize := uint32(0)
	for i := 0; i < len(e.elems); i++ {
		lendSize += e.elems[i].Size()
		if e.size-lendSize < uint32(minSize) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

func (e *hkeyElements) CanLendToRight(size uint32) bool {
	if len(e.elems) == 0 {
		panic("empty hkeyElements")
	}

	if len(e.elems) < 2 {
		return false
	}

	minSize := minThreshold - mapDataSlabPrefixSize
	if e.size-size < uint32(minSize) {
		return false
	}

	lendSize := uint32(0)
	for i := len(e.elems) - 1; i >= 0; i-- {
		lendSize += e.elems[i].Size()
		if e.size-lendSize < uint32(minSize) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

func (e *hkeyElements) Size() uint32 {
	return e.size
}

func (e *hkeyElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *hkeyElements) firstKey() Digest {
	if len(e.hkeys) > 0 {
		return e.hkeys[0]
	}
	return 0
}

func (e *hkeyElements) String() string {
	var s []string
	s = append(s, fmt.Sprintf("(level %v)", e.level))

	if len(e.elems) <= 6 {
		for i := 0; i < len(e.elems); i++ {
			s = append(s, fmt.Sprintf("%d:%s", e.hkeys[i], e.elems[i].String()))
		}
		return strings.Join(s, " ")
	}

	for i := 0; i < 3; i++ {
		s = append(s, fmt.Sprintf("%d:%s", e.hkeys[i], e.elems[i].String()))
	}

	s = append(s, "...")

	elemLength := len(e.elems)
	for i := elemLength - 3; i < elemLength; i++ {
		s = append(s, fmt.Sprintf("%d:%s", e.hkeys[i], e.elems[i].String()))
	}

	return strings.Join(s, " ")
}

func (e *singleElements) Get(storage SlabStorage, digester Digester, level int, _ Digest, key ComparableValue) (MapValue, error) {

	if level != digester.Levels() {
		panic(fmt.Sprintf("singleElements.Get() level %d, expect %d", level, digester.Levels()))
	}

	// linear search by key
	for _, elem := range e.elems {
		ek, err := elem.key.StoredValue(storage)
		if err != nil {
			return nil, err
		}
		if key.Equal(ek) {
			return elem.value, nil
		}
	}

	return nil, fmt.Errorf("key %s not found", key)
}

func (e *singleElements) Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level int, _ Digest, key ComparableValue, value MapValue) (bool, error) {

	if level != digester.Levels() {
		panic(fmt.Sprintf("singleElements.Set() level %d, expect %d", level, digester.Levels()))
	}

	// linear search key and update value
	for i := 0; i < len(e.elems); i++ {
		elem := e.elems[i]
		ek, err := elem.key.StoredValue(storage)
		if err != nil {
			return false, err
		}
		if key.Equal(ek) {
			oldSize := elem.Size()

			elem.value = value
			elem.size = elem.key.ByteSize() + elem.value.ByteSize()

			e.size += elem.Size() - oldSize

			return false, nil
		}
	}

	ks, err := key.Storable(storage, address)
	if err != nil {
		return false, err
	}

	// no matching key, append new element to the end.
	newElem := newSingleElement(ks, value)
	e.elems = append(e.elems, newElem)
	e.size += newElem.size

	return true, nil
}

func (e *singleElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, errors.New("index out of bounds")
	}
	return e.elems[i], nil
}

func (e *singleElements) Merge(elems elements) error {
	mElems, ok := elems.(*singleElements)
	if !ok {
		panic(fmt.Sprintf("singleElements.Merge() elems type %T, want *hkeyElements", elems))
	}

	e.elems = append(e.elems, mElems.elems...)
	e.size += mElems.size

	// Set merged elements to nil to prevent memory leak
	for i := 0; i < len(mElems.elems); i++ {
		mElems.elems[i] = nil
	}

	return nil
}

func (e *singleElements) Split() (elements, elements, error) {
	if len(e.elems) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split elements with less than 2 elements")
	}

	// This computes the ceil of split to give the first slab more elements.
	dataSize := e.size
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, e := range e.elems {
		elemSize := e.Size()
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

	rightCount := len(e.elems) - leftCount

	// Create right slab elements
	rightElements := &singleElements{}

	rightElements.elems = make([]*singleElement, rightCount)
	copy(rightElements.elems, e.elems[leftCount:])

	rightElements.size = dataSize - leftSize

	e.elems = e.elems[:leftCount]
	e.size = leftSize

	// NOTE: prevent memory leak
	for i := leftCount; i < len(e.elems); i++ {
		e.elems[i] = nil
	}

	return e, rightElements, nil
}

func (e *singleElements) LendToRight(re elements) error {
	return errors.New("not applicable")
}

func (e *singleElements) BorrowFromRight(re elements) error {
	return errors.New("not applicable")
}

func (e *singleElements) CanLendToLeft(size uint32) bool {
	return false
}

func (e *singleElements) CanLendToRight(size uint32) bool {
	return false
}

func (e *singleElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *singleElements) firstKey() Digest {
	return 0
}

func (e *singleElements) Size() uint32 {
	return e.size
}

func (e *singleElements) String() string {
	var s []string
	s = append(s, fmt.Sprintf("(level %v)", e.level))

	if len(e.elems) <= 6 {
		for i := 0; i < len(e.elems); i++ {
			s = append(s, fmt.Sprintf(":%s", e.elems[i].String()))
		}
		return strings.Join(s, " ")
	}

	for i := 0; i < 3; i++ {
		s = append(s, fmt.Sprintf(":%s", e.elems[i].String()))
	}

	s = append(s, "...")

	elemLength := len(e.elems)
	for i := elemLength - 3; i < elemLength; i++ {
		s = append(s, fmt.Sprintf(":%s", e.elems[i].String()))
	}

	return strings.Join(s, " ")
}

func (m *MapDataSlab) Encode(enc *Encoder) error {
	// TODO: implement me
	return errors.New("not implemented")
}

// TODO: need to set DigesterBuilder for OrderedMap
func (m *MapDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	return &OrderedMap{
		storage: storage,
		root:    m,
	}, nil
}

func (m *MapDataSlab) Set(storage SlabStorage, b DigesterBuilder, digester Digester, level int, hkey Digest, key ComparableValue, value MapValue) (bool, error) {

	isNewElem, err := m.elements.Set(storage, m.ID().Address, b, digester, level, hkey, key, value)
	if err != nil {
		return false, err
	}

	// Adjust header's first key
	m.header.firstKey = m.elements.firstKey()

	// Adjust header's slab size
	m.header.size = mapDataSlabPrefixSize + m.elements.Size()

	// Store modified slab
	err = storage.Store(m.header.id, m)
	if err != nil {
		return false, err
	}

	return isNewElem, nil
}

func (m *MapDataSlab) Remove(storage SlabStorage, level int, hkey Digest, key ComparableValue) (MapValue, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *MapDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	leftElements, rightElements, err := m.elements.Split()
	if err != nil {
		return nil, nil, err
	}

	// Create new right slab
	rightSlab := &MapDataSlab{
		header: MapSlabHeader{
			id:       storage.GenerateStorageID(m.ID().Address),
			size:     mapDataSlabPrefixSize + rightElements.Size(),
			firstKey: rightElements.firstKey(),
		},
		prev:     m.header.id,
		next:     m.next,
		elements: rightElements,
		anySize:  m.anySize,
	}

	// Modify left (original) slab
	m.header.size = mapDataSlabPrefixSize + leftElements.Size()
	m.next = rightSlab.header.id
	m.elements = leftElements

	return m, rightSlab, nil
}

func (m *MapDataSlab) Merge(slab Slab) error {

	rightSlab := slab.(*MapDataSlab)

	err := m.elements.Merge(rightSlab.elements)
	if err != nil {
		return err
	}

	m.header.size = mapDataSlabPrefixSize + m.elements.Size()

	m.next = rightSlab.next

	return nil
}

func (m *MapDataSlab) LendToRight(slab Slab) error {

	rightSlab := slab.(*MapDataSlab)

	if m.anySize || rightSlab.anySize {
		panic("oversized data slab shouldn't be asked to LendToRight")
	}

	rightElements := rightSlab.elements
	err := m.elements.LendToRight(rightElements)
	if err != nil {
		return err
	}

	// Update right slab
	rightSlab.elements = rightElements
	rightSlab.header.size = mapDataSlabPrefixSize + rightElements.Size()
	rightSlab.header.firstKey = rightElements.firstKey()

	// Update left slab
	m.header.size = mapDataSlabPrefixSize + m.elements.Size()

	return nil
}

func (m *MapDataSlab) BorrowFromRight(slab Slab) error {

	rightSlab := slab.(*MapDataSlab)

	if m.anySize || rightSlab.anySize {
		panic("oversized data slab shouldn't be asked to BorrowFromRight")
	}

	rightElements := rightSlab.elements
	err := m.elements.BorrowFromRight(rightElements)
	if err != nil {
		return err
	}

	// Update right slab
	rightSlab.elements = rightElements
	rightSlab.header.size = mapDataSlabPrefixSize + rightElements.Size()
	rightSlab.header.firstKey = rightElements.firstKey()

	// Update left slab
	m.header.size = mapDataSlabPrefixSize + m.elements.Size()

	return nil
}

func (m *MapDataSlab) IsFull() bool {
	if m.anySize {
		return false
	}
	return m.header.size > uint32(maxThreshold)
}

// IsUnderflow returns the number of bytes needed for the data slab
// to reach the min threshold.
// Returns true if the min threshold has not been reached yet.
//
func (m *MapDataSlab) IsUnderflow() (uint32, bool) {
	if m.anySize {
		return 0, false
	}
	if uint32(minThreshold) > m.header.size {
		return uint32(minThreshold) - m.header.size, true
	}
	return 0, false
}

// CanLendToLeft returns true if elements on the left of the slab could be removed
// so that the slab still stores more than the min threshold.
//
func (m *MapDataSlab) CanLendToLeft(size uint32) bool {
	if m.anySize {
		return false
	}
	return m.elements.CanLendToLeft(size)
}

// CanLendToRight returns true if elements on the right of the slab could be removed
// so that the slab still stores more than the min threshold.
//
func (m *MapDataSlab) CanLendToRight(size uint32) bool {
	if m.anySize {
		return false
	}
	return m.elements.CanLendToRight(size)
}

func (m *MapDataSlab) DeepRemove(storage SlabStorage) error {
	return storage.Remove(m.ID())
}

func (m *MapDataSlab) SetID(id StorageID) {
	m.header.id = id
}

func (m *MapDataSlab) Header() MapSlabHeader {
	return m.header
}

func (m *MapDataSlab) IsData() bool {
	return true
}

func (m *MapDataSlab) ID() StorageID {
	return m.header.id
}

func (m *MapDataSlab) ByteSize() uint32 {
	return m.header.size
}

func (m *MapDataSlab) ExtraData() *MapExtraData {
	return m.extraData
}

func (m *MapDataSlab) RemoveExtraData() *MapExtraData {
	extraData := m.extraData
	m.extraData = nil
	return extraData
}

func (m *MapDataSlab) SetExtraData(extraData *MapExtraData) {
	m.extraData = extraData
}

func (m *MapDataSlab) String() string {
	return fmt.Sprintf("{%s}", m.elements.String())
}

func (m *MapMetaDataSlab) Encode(enc *Encoder) error {
	// TODO: implement me
	return errors.New("not implemented")
}

// TODO: need to set DigesterBuilder for OrderedMap
func (m *MapMetaDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	return &OrderedMap{
		storage: storage,
		root:    m,
	}, nil
}

func (m *MapMetaDataSlab) Get(storage SlabStorage, digester Digester, level int, hkey Digest, key ComparableValue) (MapValue, error) {

	ans := -1
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	if ans == -1 {
		return nil, fmt.Errorf("key %v not found", key)
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].id

	child, err := getMapSlab(storage, childID)
	if err != nil {
		return nil, err
	}

	return child.Get(storage, digester, level, hkey, key)
}

func (m *MapMetaDataSlab) Set(storage SlabStorage, b DigesterBuilder, digester Digester, level int, hkey Digest, key ComparableValue, value MapValue) (bool, error) {

	ans := 0
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].id

	child, err := getMapSlab(storage, childID)
	if err != nil {
		return false, err
	}

	isNewElem, err := child.Set(storage, b, digester, level, hkey, key, value)
	if err != nil {
		return false, err
	}

	m.childrenHeaders[childHeaderIndex] = child.Header()

	if childHeaderIndex == 0 {
		// Update firstKey.  May not be necessary.
		m.header.firstKey = m.childrenHeaders[childHeaderIndex].firstKey
	}

	if child.IsFull() {
		err := m.SplitChildSlab(storage, child, childHeaderIndex)
		if err != nil {
			return false, err
		}
		return isNewElem, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err := m.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			return false, err
		}
		return isNewElem, nil
	}

	err = storage.Store(m.header.id, m)
	if err != nil {
		return false, err
	}
	return isNewElem, nil
}

func (m *MapMetaDataSlab) Remove(storage SlabStorage, level int, hkey Digest, key ComparableValue) (MapValue, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *MapMetaDataSlab) SplitChildSlab(storage SlabStorage, child MapSlab, childHeaderIndex int) error {
	leftSlab, rightSlab, err := child.Split(storage)
	if err != nil {
		return err
	}

	left := leftSlab.(MapSlab)
	right := rightSlab.(MapSlab)

	// Add new child slab (right) to childrenHeaders
	m.childrenHeaders = append(m.childrenHeaders, MapSlabHeader{})
	if childHeaderIndex < len(m.childrenHeaders)-2 {
		copy(m.childrenHeaders[childHeaderIndex+2:], m.childrenHeaders[childHeaderIndex+1:])
	}
	m.childrenHeaders[childHeaderIndex] = left.Header()
	m.childrenHeaders[childHeaderIndex+1] = right.Header()

	// Increase header size
	m.header.size += mapSlabHeaderSize

	// Store modified slabs
	err = storage.Store(left.ID(), left)
	if err != nil {
		return err
	}

	err = storage.Store(right.ID(), right)
	if err != nil {
		return err
	}

	return storage.Store(m.header.id, m)
}

// MergeOrRebalanceChildSlab merges or rebalances child slab.
// parent slab's data is adjusted.
// If merged, then parent slab's data is adjusted.
//
// +-----------------------+-----------------------+----------------------+-----------------------+
// |			   | no left sibling (sib) | left sib can't lend  | left sib can lend     |
// +=======================+=======================+======================+=======================+
// | no right sib          | panic                 | merge with left      | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can't lend  | merge with right      | merge with smaller   | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can lend    | rebalance with right  | rebalance with right | rebalance with bigger |
// +-----------------------+-----------------------+----------------------+-----------------------+
func (m *MapMetaDataSlab) MergeOrRebalanceChildSlab(
	storage SlabStorage,
	child MapSlab,
	childHeaderIndex int,
	underflowSize uint32,
) error {

	// Retrieve left sibling of the same parent.
	var leftSib MapSlab
	if childHeaderIndex > 0 {
		leftSibID := m.childrenHeaders[childHeaderIndex-1].id

		var err error
		leftSib, err = getMapSlab(storage, leftSibID)
		if err != nil {
			return err
		}
	}

	// Retrieve right siblings of the same parent.
	var rightSib MapSlab
	if childHeaderIndex < len(m.childrenHeaders)-1 {
		rightSibID := m.childrenHeaders[childHeaderIndex+1].id

		var err error
		rightSib, err = getMapSlab(storage, rightSibID)
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

			m.childrenHeaders[childHeaderIndex] = child.Header()
			m.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Store modified slabs
			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}

			err = storage.Store(rightSib.ID(), rightSib)
			if err != nil {
				return err
			}

			return storage.Store(m.header.id, m)
		}

		// Rebalance with left sib
		if !rightCanLend {

			err := leftSib.LendToRight(child)
			if err != nil {
				return err
			}

			m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			m.childrenHeaders[childHeaderIndex] = child.Header()

			// Store modified slabs
			err = storage.Store(leftSib.ID(), leftSib)
			if err != nil {
				return err
			}

			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}

			return storage.Store(m.header.id, m)
		}

		// Rebalance with bigger sib
		if leftSib.ByteSize() > rightSib.ByteSize() {

			err := leftSib.LendToRight(child)
			if err != nil {
				return err
			}

			m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			m.childrenHeaders[childHeaderIndex] = child.Header()

			// Store modified slabs
			err = storage.Store(leftSib.ID(), leftSib)
			if err != nil {
				return err
			}

			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}

			return storage.Store(m.header.id, m)
		} else {
			// leftSib.ByteSize() <= rightSib.ByteSize

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				return err
			}

			m.childrenHeaders[childHeaderIndex] = child.Header()
			m.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Store modified slabs
			err = storage.Store(child.ID(), child)
			if err != nil {
				return err
			}

			err = storage.Store(rightSib.ID(), rightSib)
			if err != nil {
				return err
			}

			return storage.Store(m.header.id, m)
		}
	}

	// Child can't rebalance with any sibling.  It must merge with one sibling.

	if leftSib == nil {

		// Merge with right
		err := child.Merge(rightSib)
		if err != nil {
			return err
		}

		m.childrenHeaders[childHeaderIndex] = child.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex+1:], m.childrenHeaders[childHeaderIndex+2:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(child.ID(), child)
		if err != nil {
			return err
		}
		err = storage.Store(m.header.id, m)
		if err != nil {
			return err
		}

		// Remove right sib from storage
		return storage.Remove(rightSib.ID())
	}

	if rightSib == nil {

		// Merge with left
		err := leftSib.Merge(child)
		if err != nil {
			return err
		}

		m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex:], m.childrenHeaders[childHeaderIndex+1:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(leftSib.ID(), leftSib)
		if err != nil {
			return err
		}
		err = storage.Store(m.header.id, m)
		if err != nil {
			return err
		}

		// Remove child from storage
		return storage.Remove(child.ID())
	}

	// Merge with smaller sib
	if leftSib.ByteSize() < rightSib.ByteSize() {
		err := leftSib.Merge(child)
		if err != nil {
			return err
		}

		m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex:], m.childrenHeaders[childHeaderIndex+1:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(leftSib.ID(), leftSib)
		if err != nil {
			return err
		}
		err = storage.Store(m.header.id, m)
		if err != nil {
			return err
		}

		// Remove child from storage
		return storage.Remove(child.ID())
	} else {
		// leftSib.ByteSize() > rightSib.ByteSize

		err := child.Merge(rightSib)
		if err != nil {
			return err
		}

		m.childrenHeaders[childHeaderIndex] = child.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex+1:], m.childrenHeaders[childHeaderIndex+2:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(child.ID(), child)
		if err != nil {
			return err
		}
		err = storage.Store(m.header.id, m)
		if err != nil {
			return err
		}

		// Remove rightSib from storage
		return storage.Remove(rightSib.ID())
	}
}

func (m *MapMetaDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*MapMetaDataSlab)

	m.childrenHeaders = append(m.childrenHeaders, rightSlab.childrenHeaders...)
	m.header.size += rightSlab.header.size - mapMetaDataSlabPrefixSize

	return nil
}

func (m *MapMetaDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if len(m.childrenHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		panic("can't split meta slab with less than 2 headers")
	}

	leftChildrenCount := int(math.Ceil(float64(len(m.childrenHeaders)) / 2))
	leftSize := leftChildrenCount * mapSlabHeaderSize

	// Construct right slab
	rightSlab := &MapMetaDataSlab{
		header: MapSlabHeader{
			id:       storage.GenerateStorageID(m.ID().Address),
			size:     m.header.size - uint32(leftSize),
			firstKey: m.childrenHeaders[leftChildrenCount].firstKey,
		},
	}

	rightSlab.childrenHeaders = make([]MapSlabHeader, len(m.childrenHeaders)-leftChildrenCount)
	copy(rightSlab.childrenHeaders, m.childrenHeaders[leftChildrenCount:])

	// Modify left (original) slab
	m.childrenHeaders = m.childrenHeaders[:leftChildrenCount]
	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftSize)

	return m, rightSlab, nil
}

func (m *MapMetaDataSlab) LendToRight(slab Slab) error {

	rightSlab := slab.(*MapMetaDataSlab)

	childrenHeadersLen := len(m.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftChildrenHeadersLen := childrenHeadersLen / 2
	rightChildrenHeadersLen := childrenHeadersLen - leftChildrenHeadersLen

	// Update right slab childrenHeaders by prepending borrowed children headers
	rightChildrenHeaders := make([]MapSlabHeader, rightChildrenHeadersLen)
	n := copy(rightChildrenHeaders, m.childrenHeaders[leftChildrenHeadersLen:])
	copy(rightChildrenHeaders[n:], rightSlab.childrenHeaders)
	rightSlab.childrenHeaders = rightChildrenHeaders

	// Update right slab header
	rightSlab.header.size = mapMetaDataSlabPrefixSize + uint32(rightChildrenHeadersLen)*mapSlabHeaderSize
	rightSlab.header.firstKey = rightSlab.childrenHeaders[0].firstKey

	// Update left slab (original)
	m.childrenHeaders = m.childrenHeaders[:leftChildrenHeadersLen]

	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftChildrenHeadersLen)*mapSlabHeaderSize

	return nil
}

func (m *MapMetaDataSlab) BorrowFromRight(slab Slab) error {

	rightSlab := slab.(*MapMetaDataSlab)

	childrenHeadersLen := len(m.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftSlabHeaderLen := childrenHeadersLen / 2
	rightSlabHeaderLen := childrenHeadersLen - leftSlabHeaderLen

	// Update left slab (original)
	m.childrenHeaders = append(m.childrenHeaders, rightSlab.childrenHeaders[:leftSlabHeaderLen-len(m.childrenHeaders)]...)

	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*mapSlabHeaderSize

	// Update right slab
	rightSlab.childrenHeaders = rightSlab.childrenHeaders[len(rightSlab.childrenHeaders)-rightSlabHeaderLen:]

	rightSlab.header.size = mapMetaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*mapSlabHeaderSize
	rightSlab.header.firstKey = rightSlab.childrenHeaders[0].firstKey

	return nil
}

func (m MapMetaDataSlab) IsFull() bool {
	return m.header.size > uint32(maxThreshold)
}

func (m MapMetaDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > m.header.size {
		return uint32(minThreshold) - m.header.size, true
	}
	return 0, false
}

func (m *MapMetaDataSlab) CanLendToLeft(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / mapSlabHeaderSize))
	return m.header.size-mapSlabHeaderSize*n > uint32(minThreshold)
}

func (m *MapMetaDataSlab) CanLendToRight(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / mapSlabHeaderSize))
	return m.header.size-mapSlabHeaderSize*n > uint32(minThreshold)
}

func (m *MapMetaDataSlab) DeepRemove(storage SlabStorage) error {
	return storage.Remove(m.ID())
}

func (m MapMetaDataSlab) IsData() bool {
	return false
}

func (m *MapMetaDataSlab) SetID(id StorageID) {
	m.header.id = id
}

func (m *MapMetaDataSlab) Header() MapSlabHeader {
	return m.header
}

func (m *MapMetaDataSlab) ByteSize() uint32 {
	return m.header.size
}

func (m *MapMetaDataSlab) ID() StorageID {
	return m.header.id
}

func (m *MapMetaDataSlab) ExtraData() *MapExtraData {
	return m.extraData
}

func (m *MapMetaDataSlab) RemoveExtraData() *MapExtraData {
	extraData := m.extraData
	m.extraData = nil
	return extraData
}

func (m *MapMetaDataSlab) SetExtraData(extraData *MapExtraData) {
	m.extraData = extraData
}

func (m *MapMetaDataSlab) String() string {
	var hStr []string
	for _, h := range m.childrenHeaders {
		hStr = append(hStr, fmt.Sprintf("%+v", h))
	}
	return strings.Join(hStr, " ")
}

func NewMap(storage SlabStorage, address Address, digestBuilder DigesterBuilder, typeInfo string) (*OrderedMap, error) {

	extraData := &MapExtraData{TypeInfo: typeInfo}

	root := &MapDataSlab{
		header: MapSlabHeader{
			id:   storage.GenerateStorageID(address),
			size: mapDataSlabPrefixSize,
		},
		elements:  &hkeyElements{},
		extraData: extraData,
	}

	err := storage.Store(root.header.id, root)
	if err != nil {
		return nil, err
	}

	return &OrderedMap{
		storage:         storage,
		root:            root,
		digesterBuilder: digestBuilder,
	}, nil
}

func (m *OrderedMap) Has(key ComparableValue) (bool, error) {
	_, err := m.Get(key)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (m *OrderedMap) Get(key ComparableValue) (Value, error) {

	keyDigest, err := m.digesterBuilder.Digest(key)
	if err != nil {
		return nil, err
	}

	level := 0

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		return nil, err
	}

	v, err := m.root.Get(m.storage, keyDigest, level, hkey, key)
	if err != nil {
		return nil, err
	}

	return v.StoredValue(m.storage)
}

func (m *OrderedMap) Set(key ComparableValue, value Value) error {

	keyDigest, err := m.digesterBuilder.Digest(key)
	if err != nil {
		return err
	}

	level := 0

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		return err
	}

	valueStorable, err := value.Storable(m.storage, m.Address())
	if err != nil {
		return err
	}

	isNewElem, err := m.root.Set(m.storage, m.digesterBuilder, keyDigest, level, hkey, key, valueStorable)
	if err != nil {
		return err
	}

	if isNewElem {
		m.root.ExtraData().incrementCount()
	}

	if !m.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := m.root.(*MapMetaDataSlab)
		if len(root.childrenHeaders) == 1 {

			extraData := root.RemoveExtraData()

			rootID := root.header.id

			childID := root.childrenHeaders[0].id

			child, err := getMapSlab(m.storage, childID)
			if err != nil {
				return err
			}

			m.root = child

			m.root.SetID(rootID)

			m.root.SetExtraData(extraData)

			err = m.storage.Store(rootID, m.root)
			if err != nil {
				return err
			}

			err = m.storage.Remove(childID)
			if err != nil {
				return err
			}
		}
	}

	if m.root.IsFull() {

		// Get old root's extra data and reset it to nil in old root
		extraData := m.root.RemoveExtraData()

		// Save root node id
		rootID := m.root.ID()

		// Assign a new storage id to old root before splitting it.
		oldRoot := m.root
		oldRoot.SetID(m.storage.GenerateStorageID(m.Address()))

		// Split old root
		leftSlab, rightSlab, err := oldRoot.Split(m.storage)
		if err != nil {
			return err
		}

		left := leftSlab.(MapSlab)
		right := rightSlab.(MapSlab)

		// Create new MapMetaDataSlab with the old root's storage ID
		newRoot := &MapMetaDataSlab{
			header: MapSlabHeader{
				id:       rootID,
				size:     mapMetaDataSlabPrefixSize + mapSlabHeaderSize*2,
				firstKey: left.Header().firstKey,
			},
			childrenHeaders: []MapSlabHeader{left.Header(), right.Header()},
			extraData:       extraData,
		}

		m.root = newRoot

		err = m.storage.Store(left.ID(), left)
		if err != nil {
			return err
		}
		err = m.storage.Store(right.ID(), right)
		if err != nil {
			return err
		}
		err = m.storage.Store(m.root.ID(), m.root)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *OrderedMap) Remove(key ComparableValue) (Value, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *OrderedMap) StorageID() StorageID {
	return m.root.Header().id
}

func (m *OrderedMap) DeepCopy(_ SlabStorage, _ Address) (Value, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *OrderedMap) DeepRemove(storage SlabStorage) error {
	// TODO: implement me
	return errors.New("not implemented")
}

func (m *OrderedMap) StoredValue(_ SlabStorage) (Value, error) {
	return m, nil
}

func (m *OrderedMap) Storable(_ SlabStorage, _ Address) (Storable, error) {
	return StorageIDStorable(m.StorageID()), nil
}

func (m *OrderedMap) Count() uint64 {
	return m.root.ExtraData().Count
}

func (m *OrderedMap) Address() Address {
	return m.root.ID().Address
}

func (m *OrderedMap) Type() string {
	if extraData := m.root.ExtraData(); extraData != nil {
		return extraData.TypeInfo
	}
	return ""
}

func (m *OrderedMap) String() string {
	if m.root.IsData() {
		return m.root.String()
	}
	meta := m.root.(*MapMetaDataSlab)
	return m.string(meta)
}

func (m *OrderedMap) string(meta *MapMetaDataSlab) string {
	var elemsStr []string

	for _, h := range meta.childrenHeaders {
		child, err := getMapSlab(m.storage, h.id)
		if err != nil {
			return err.Error()
		}
		if child.IsData() {
			data := child.(*MapDataSlab)
			elemsStr = append(elemsStr, data.String())
		} else {
			meta := child.(*MapMetaDataSlab)
			elemsStr = append(elemsStr, m.string(meta))
		}
	}
	return strings.Join(elemsStr, " ")
}

func getMapSlab(storage SlabStorage, id StorageID) (MapSlab, error) {
	slab, _, err := storage.Retrieve(id)

	if mapSlab, ok := slab.(MapSlab); ok {
		return mapSlab, nil
	}

	return nil, MapSlabNotFoundError{id, err}
}

func firstMapDataSlab(storage SlabStorage, slab MapSlab) (MapSlab, error) {
	if slab.IsData() {
		return slab, nil
	}
	meta := slab.(*MapMetaDataSlab)
	firstChildID := meta.childrenHeaders[0].id
	firstChild, err := getMapSlab(storage, firstChildID)
	if err != nil {
		return nil, err
	}
	return firstMapDataSlab(storage, firstChild)
}

func (extra *MapExtraData) incrementCount() {
	extra.Count += 1
}

type MapElementIterator struct {
	storage        SlabStorage
	elements       elements
	index          int
	nestedIterator *MapElementIterator
}

var ErrEOE = errors.New("end of elements")

func (i *MapElementIterator) Next() (key Value, value Value, err error) {

	if i.nestedIterator != nil {
		key, value, err = i.nestedIterator.Next()
		if err != ErrEOE {
			return key, value, err
		}
		i.nestedIterator = nil
	}

	if i.index >= int(i.elements.Count()) {
		return nil, nil, ErrEOE
	}

	e, err := i.elements.Element(i.index)
	if err != nil {
		return nil, nil, err
	}

	if group, ok := e.(elementGroup); ok {

		elems, err := group.Elements(i.storage)
		if err != nil {
			return nil, nil, err
		}

		i.nestedIterator = &MapElementIterator{
			storage:  i.storage,
			elements: elems,
		}

		i.index++

		return i.nestedIterator.Next()
	}

	se, ok := e.(*singleElement)
	if !ok {
		panic("iterate over an element that is not a elementGroup and not a singleElement")
	}

	k, err := se.key.StoredValue(i.storage)
	if err != nil {
		return nil, nil, err
	}

	v, err := se.value.StoredValue(i.storage)
	if err != nil {
		return nil, nil, err
	}

	i.index++

	return k, v, nil
}

type MapIterationFunc func(Value, Value) (resume bool, err error)

type MapIterator struct {
	storage      SlabStorage
	id           StorageID
	elemIterator *MapElementIterator
}

func (i *MapIterator) Next() (key Value, value Value, err error) {
	if i.elemIterator == nil {
		if i.id == StorageIDUndefined {
			return nil, nil, nil
		}

		slab, found, err := i.storage.Retrieve(i.id)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			return nil, nil, fmt.Errorf("slab %d not found", i.id)
		}

		dataSlab := slab.(*MapDataSlab)

		i.id = dataSlab.next

		i.elemIterator = &MapElementIterator{
			storage:  i.storage,
			elements: dataSlab.elements,
		}
	}

	key, value, err = i.elemIterator.Next()
	if err != ErrEOE {
		return key, value, err
	}

	i.elemIterator = nil

	return i.Next()
}

func (m *OrderedMap) Iterator() (*MapIterator, error) {
	slab, err := firstMapDataSlab(m.storage, m.root)
	if err != nil {
		return nil, err
	}

	return &MapIterator{
		storage: m.storage,
		id:      slab.ID(),
	}, nil
}

func (m *OrderedMap) Iterate(fn MapIterationFunc) error {

	iterator, err := m.Iterator()
	if err != nil {
		return err
	}

	for {
		key, value, err := iterator.Next()
		if err != nil {
			return err
		}
		if key == nil {
			return nil
		}
		resume, err := fn(key, value)
		if err != nil {
			return err
		}
		if !resume {
			return nil
		}
	}
}
