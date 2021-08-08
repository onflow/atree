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

type MapKey interface {
	Storable
	Value
	Hashable
	Equal(other Storable) bool
}

type MapValue Storable

// element is one indivisible unit that must stay together (e.g. collision group)
type element interface {
	fmt.Stringer

	Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error)

	// Set returns updated element, which may be a different type of element because of hash collision.
	// Caller needs to save returned element.
	Set(storage SlabStorage, address Address, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error)

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

	Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error)
	Set(storage SlabStorage, address Address, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) error

	Merge(elements) error
	Split() (elements, elements, error)

	Element(int) (element, error)

	firstKey() uint64

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

var _ elementGroup = &inlineCollisionGroup{}

type externalCollisionGroup struct {
	id   StorageID
	size uint32
}

var _ elementGroup = &externalCollisionGroup{}

type hkeyElements struct {
	hkeyPrefixes []uint64  // all elements share the same hkeyPrefixes
	hkeys        []uint64  // sorted list of unique hashed keys
	elems        []element // elements corresponding to hkeys
	size         uint32    // total byte sizes
}

var _ elements = &hkeyElements{}

type singleElements struct {
	hkeyPrefixes []uint64         // all elements share the same hkeyPrefixes
	elems        []*singleElement // list of key+value pairs
	size         uint32           // total key+value byte sizes
}

var _ elements = &singleElements{}

type MapSlabHeader struct {
	id       StorageID // id is used to retrieve slab from storage
	size     uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	firstKey uint64    // firstKey (first hashed key) is used to lookup value
}

type MapExtraData struct {
	_        struct{} `cbor:",toarray"`
	TypeInfo string
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

	Has(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (bool, error)
	Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error)
	Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) error
	Remove(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error)

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
	storage SlabStorage
	root    MapSlab
	hasher  Hasher
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

func (e *singleElement) Get(_ SlabStorage, _ Hasher, _ []uint64, _ []uint64, key MapKey) (MapValue, error) {
	if e.key.Equal(key) {
		return e.value, nil
	}
	return nil, fmt.Errorf("key %s not found", key)
}

// Set updates value if key matches, otherwise returns inlineCollisionGroup with existing and new elements.
// NOTE: Existing key needs to be rehashed because we store minimum digest for non-collision element.
//       Rehashing only happens when we create new inlineCollisionGroup.
//       Adding new element to existing inlineCollisionGroup doesn't require rehashing.
func (e *singleElement) Set(storage SlabStorage, address Address, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error) {
	if e.key.Equal(key) {
		e.value = value
		e.size = e.key.ByteSize() + e.value.ByteSize()
		return e, nil
	}

	// Hash collision detected

	// Rehash existing key if needed (see function comment)
	var existingKeyHkey []uint64
	if len(hkeys) > 0 {
		v, err := h.Hash(e.key)
		if err != nil {
			return nil, err
		}
		existingKeyHkey = v[len(hkeyPrefixes):]
	}

	// Collision group's hkeyPrefixes includes hkeyPrefixes + collided hkey[0].
	collisionHkeyPrefixes := append(hkeyPrefixes, hkeys[0])

	// Create collision group with existing and new elements
	var elements elements
	if len(hkeys) == 1 {
		elements = &singleElements{hkeyPrefixes: collisionHkeyPrefixes}
	} else {
		elements = &hkeyElements{hkeyPrefixes: collisionHkeyPrefixes}
	}

	var err error
	var newElem element

	newElem = &inlineCollisionGroup{elements: elements}

	newElem, err = newElem.Set(storage, address, h, hkeyPrefixes, existingKeyHkey, e.key, e.value)
	if err != nil {
		return nil, err
	}

	newElem, err = newElem.Set(storage, address, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return nil, err
	}

	return newElem, nil
}

func (e *singleElement) Size() uint32 {
	return e.size
}

func (e *singleElement) String() string {
	return fmt.Sprintf("%s:%s", e.key, e.value)
}

func (e *inlineCollisionGroup) Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error) {

	// Adjust hkeyPrefixes and hkey for collision group
	hkeyPrefixes = append(hkeyPrefixes, hkeys[0])
	hkeys = hkeys[1:]

	// Search key in collision group with adjusted hkeyPrefix and hkey
	return e.elements.Get(storage, h, hkeyPrefixes, hkeys, key)
}

func (e *inlineCollisionGroup) Set(storage SlabStorage, address Address, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error) {

	// Adjust hkeyPrefixes and hkey for collision group
	hkeyPrefixes = append(hkeyPrefixes, hkeys[0])
	hkeys = hkeys[1:]

	err := e.elements.Set(storage, address, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return nil, err
	}

	if len(hkeyPrefixes) == 1 {
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
				return nil, err
			}

			// Create and return externalCollisionGroup (wrapper of newly created MapDataSlab)
			return &externalCollisionGroup{
				id:   id,
				size: StorageIDStorable(id).ByteSize(),
			}, nil
		}
	}

	return e, nil
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

func (e *externalCollisionGroup) Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error) {
	slab, err := getMapSlab(storage, e.id)
	if err != nil {
		return nil, err
	}

	// Adjust hkeyPrefixes and hkey for collision group
	hkeyPrefixes = append(hkeyPrefixes, hkeys[0])
	hkeys = hkeys[1:]

	// Search key in collision group with adjusted hkeyPrefix and hkey
	return slab.Get(storage, h, hkeyPrefixes, hkeys, key)
}

func (e *externalCollisionGroup) Set(storage SlabStorage, address Address, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error) {
	slab, err := getMapSlab(storage, e.id)
	if err != nil {
		return nil, err
	}

	// Adjust hkeyPrefixes and hkey for collision group
	hkeyPrefixes = append(hkeyPrefixes, hkeys[0])
	hkeys = hkeys[1:]

	err = slab.Set(storage, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return nil, err
	}
	return e, nil
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

func (e *hkeyElements) Get(storage SlabStorage, h Hasher, hkeyPrefixes, hkeys []uint64, key MapKey) (MapValue, error) {

	// Check hkeys are not empty
	if len(hkeys) == 0 {
		panic(fmt.Sprintf("hkeyElements.Get(%v, %v) hkeys are empty", hkeyPrefixes, hkeys))
	}

	hkey := hkeys[0]

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

	return elem.Get(storage, h, hkeyPrefixes, hkeys, key)
}

func (e *hkeyElements) Set(storage SlabStorage, address Address, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) error {

	// Check hkeys are not empty
	if len(hkeys) == 0 {
		panic(fmt.Sprintf("hkeyElements.Set(%v, %v) hkeys are empty", hkeyPrefixes, hkeys))
	}

	newElem := newSingleElement(key, value)

	hkey := hkeys[0]

	if len(e.hkeys) == 0 {
		// only element

		e.hkeys = []uint64{hkey}

		e.elems = []element{newElem}

		e.size += newElem.size

		return nil
	}

	if hkey < e.hkeys[0] {
		// prepend key and value

		e.hkeys = append(e.hkeys, uint64(0))
		copy(e.hkeys[1:], e.hkeys)
		e.hkeys[0] = hkey

		e.elems = append(e.elems, nil)
		copy(e.elems[1:], e.elems)
		e.elems[0] = newElem

		e.size += newElem.size

		return nil
	}

	if hkey > e.hkeys[len(e.hkeys)-1] {
		// append key and value

		e.hkeys = append(e.hkeys, hkey)

		e.elems = append(e.elems, newElem)

		e.size += newElem.size

		return nil
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

		elem, err := elem.Set(storage, address, h, hkeyPrefixes, hkeys, key, value)
		if err != nil {
			return err
		}

		e.elems[equalIndex] = elem

		e.size += elem.Size() - oldElemSize

		return nil
	}

	// No matching hkey

	// insert into sorted hkeys
	e.hkeys = append(e.hkeys, uint64(0))
	copy(e.hkeys[lessThanIndex+1:], e.hkeys[lessThanIndex:])
	e.hkeys[lessThanIndex] = hkey

	// insert into sorted elements
	e.elems = append(e.elems, nil)
	copy(e.elems[lessThanIndex+1:], e.elems[lessThanIndex:])
	e.elems[lessThanIndex] = newElem

	e.size += newElem.Size()

	return nil
}

func (e *hkeyElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, errors.New("index out of bounds")
	}
	return e.elems[i], nil
}

func (e *hkeyElements) Merge(elems elements) error {
	// TODO: verify that both elements have the same hkey prefixes

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

	rightElements.hkeyPrefixes = make([]uint64, len(e.hkeyPrefixes))
	copy(rightElements.hkeyPrefixes, e.hkeyPrefixes)

	rightElements.hkeys = make([]uint64, rightCount)
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

func (e *hkeyElements) Size() uint32 {
	return e.size
}

func (e *hkeyElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *hkeyElements) firstKey() uint64 {
	if len(e.hkeys) > 0 {
		return e.hkeys[0]
	}
	return 0
}

func (e *hkeyElements) String() string {
	var s []string
	s = append(s, fmt.Sprintf("(prefix %v)", e.hkeyPrefixes))
	if len(e.elems) <= 6 {
		var s []string
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

func (e *singleElements) Get(storage SlabStorage, h Hasher, hkeyPrefixes, hkeys []uint64, key MapKey) (MapValue, error) {

	if len(hkeys) > 0 {
		panic(fmt.Sprintf("singleElements.Get(%v, %v) hkeys are not empty", hkeyPrefixes, hkeys))
	}

	// linear search by key
	for _, elem := range e.elems {
		if elem.key.Equal(key) {
			return elem.value, nil
		}
	}

	return nil, fmt.Errorf("key %s not found", key)
}

func (e *singleElements) Set(storage SlabStorage, address Address, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) error {

	if len(hkeys) > 0 {
		panic(fmt.Sprintf("singleElements.Set(%v, %v) hkeys are not empty", hkeyPrefixes, hkeys))
	}

	// linear search key and update value
	for i := 0; i < len(e.elems); i++ {
		elem := e.elems[i]
		if elem.key.Equal(key) {
			oldSize := elem.Size()

			elem.value = value
			elem.size = elem.key.ByteSize() + elem.value.ByteSize()

			e.size += elem.Size() - oldSize

			return nil
		}
	}

	// no matching key, append new element to the end.
	newElem := newSingleElement(key, value)
	e.elems = append(e.elems, newElem)
	e.size += newElem.size

	return nil
}

func (e *singleElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, errors.New("index out of bounds")
	}
	return e.elems[i], nil
}

func (e *singleElements) Merge(elems elements) error {
	// TODO: verify that both elements have the same hkey prefixes

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

	rightElements.hkeyPrefixes = make([]uint64, len(e.hkeyPrefixes))
	copy(rightElements.hkeyPrefixes, e.hkeyPrefixes)

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

func (e *singleElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *singleElements) firstKey() uint64 {
	return 0
}

func (e *singleElements) Size() uint32 {
	return e.size
}

func (e *singleElements) String() string {
	if len(e.elems) <= 6 {
		var s []string
		for i := 0; i < len(e.elems); i++ {
			s = append(s, fmt.Sprintf(":%s", e.elems[i].String()))
		}
		return strings.Join(s, " ")
	}

	var s []string
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

// TODO: need to set Hasher for OrderedMap
func (m *MapDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	return &OrderedMap{
		storage: storage,
		root:    m,
	}, nil
}

func (m *MapDataSlab) Has(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (bool, error) {
	_, err := m.Get(storage, h, hkeyPrefixes, hkeys, key)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (m *MapDataSlab) Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) error {

	err := m.elements.Set(storage, m.ID().Address, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return err
	}

	// Adjust header's first key
	m.header.firstKey = m.elements.firstKey()

	// Adjust header's slab size
	m.header.size = mapDataSlabPrefixSize + m.elements.Size()

	// Store modified slab
	return storage.Store(m.header.id, m)
}

func (m *MapDataSlab) Remove(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error) {
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
	// TODO: implement me
	return errors.New("not implemented")
}

func (m *MapDataSlab) BorrowFromRight(slab Slab) error {
	// TODO: implement me
	return errors.New("not implemented")
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
	// TODO: implement me
	return false
}

// CanLendToRight returns true if elements on the right of the slab could be removed
// so that the slab still stores more than the min threshold.
//
func (m *MapDataSlab) CanLendToRight(size uint32) bool {
	if m.anySize {
		return false
	}
	// TODO: implement me
	return false
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

// TODO: need to set Hasher for OrderedMap
func (m *MapMetaDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	return &OrderedMap{
		storage: storage,
		root:    m,
	}, nil
}

func (m *MapMetaDataSlab) Has(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (bool, error) {
	_, err := m.Get(storage, h, hkeyPrefixes, hkeys, key)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (m *MapMetaDataSlab) Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error) {
	if len(hkeys) == 0 {
		panic("MapMetaDataSlab.Get() hkeys is empty")
	}

	hkey := hkeys[0]

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

	return child.Get(storage, h, hkeyPrefixes, hkeys, key)
}

func (m *MapMetaDataSlab) Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) error {
	if len(hkeys) == 0 {
		panic("MapMetaDataSlab.Set() hkeys is empty")
	}

	hkey := hkeys[0]

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
		return err
	}

	err = child.Set(storage, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return err
	}

	m.childrenHeaders[childHeaderIndex] = child.Header()

	if childHeaderIndex == 0 {
		// Update firstKey.  May not be necessary.
		m.header.firstKey = m.childrenHeaders[childHeaderIndex].firstKey
	}

	if child.IsFull() {
		return m.SplitChildSlab(storage, child, childHeaderIndex)
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		return m.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
	}

	return storage.Store(m.header.id, m)
}

func (m *MapMetaDataSlab) Remove(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error) {
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
		storage.Remove(rightSib.ID())

		return nil
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
		storage.Remove(child.ID())

		return nil
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
		storage.Remove(child.ID())

		return nil
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
		storage.Remove(rightSib.ID())

		return nil
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
	// TODO: implement me
	return errors.New("not implemented")
}

func (m *MapMetaDataSlab) BorrowFromRight(slab Slab) error {
	// TODO: implement me
	return errors.New("not implemented")
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
	// TODO: implement me
	return false
}

func (m *MapMetaDataSlab) CanLendToRight(size uint32) bool {
	// TODO: implement me
	return false
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

func NewMap(storage SlabStorage, address Address, hasher Hasher, typeInfo string) (*OrderedMap, error) {

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
		storage: storage,
		root:    root,
		hasher:  hasher,
	}, nil
}

func (m *OrderedMap) Has(key MapKey) (bool, error) {
	hkeys, err := m.hasher.Hash(key)
	if err != nil {
		return false, err
	}
	return m.root.Has(m.storage, m.hasher, nil, hkeys, key)
}

func (m *OrderedMap) Get(key MapKey) (Value, error) {
	hkeys, err := m.hasher.Hash(key)
	if err != nil {
		return nil, err
	}

	v, err := m.root.Get(m.storage, m.hasher, nil, hkeys, key)
	if err != nil {
		return nil, err
	}

	return v.StoredValue(m.storage)
}

func (m *OrderedMap) Set(key MapKey, value Value) error {

	storable, err := value.Storable(m.storage, m.Address())
	if err != nil {
		return err
	}

	hkeys, err := m.hasher.Hash(key)
	if err != nil {
		return err
	}

	err = m.root.Set(m.storage, m.hasher, nil, hkeys, key, storable)
	if err != nil {
		return err
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

			m.storage.Remove(childID)
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

func (m *OrderedMap) Remove(key MapKey) (Value, error) {
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

func (m *OrderedMap) StoredValue(_ SlabStorage) (Value, error) {
	return m, nil
}

func (m *OrderedMap) Storable(_ SlabStorage, _ Address) (Storable, error) {
	return StorageIDStorable(m.StorageID()), nil
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
