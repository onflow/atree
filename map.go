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
	Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error)

	// Set returns updated element, which may be a different type of element because of hash collision.
	// Caller needs to save returned element.
	Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error)

	Equal(MapKey) bool // elementGroup returns false

	MapKey() (MapKey, error)     // elementGroup returns nil and error
	MapValue() (MapValue, error) // elementGroup returns nil and error

	Size() uint32

	String() string
}

// elementGroup is a group of elements that must stay together during splitting or rebalancing.
type elementGroup interface {
	element

	Inline() bool

	// Elements returns underlying elements and their meta data.
	// Returned elements are no longer treated as a group.
	Elements(storage SlabStorage) (elements, error)
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

type elements struct {
	hkeyPrefixes []uint64  // all elements share the same hkeyPrefixes
	hkeys        []uint64  // sorted list of unique hashed keys
	elems        []element // key+value pairs corresponding to hkeys
	size         uint32    // total key+value byte sizes
}

type MapSlabHeader struct {
	id       StorageID // id is used to retrieve slab from storage
	size     uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	firstKey uint64    // firstKey (first hashed key) is used to lookup value
}

// MapDataSlab is leaf node, implementing MapSlab.
// anySize is true for data slab that isn't restricted by size requirement.
type MapDataSlab struct {
	prev   StorageID
	next   StorageID
	header MapSlabHeader
	elements
	anySize bool
}

var _ MapSlab = &MapDataSlab{}

// MapMetaDataSlab is internal node, implementing MapSlab.
type MapMetaDataSlab struct {
	header          MapSlabHeader
	childrenHeaders []MapSlabHeader
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
}

type OrderedMap struct {
	storage SlabStorage
	address Address
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
func (e *singleElement) Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error) {
	if e.key.Equal(key) {
		e.value = value
		e.size = e.key.ByteSize() + e.value.ByteSize()
		return e, nil
	}

	// Hash collision detected

	// Adjust hkeyPrefixes and hkeys for new inlineCollisionGroup
	hkeyPrefixes = append(hkeyPrefixes, hkeys[0])
	hkeys = hkeys[1:]

	// Rehash existing key if needed
	var existingKeyHkey []uint64
	if len(hkeys) > 0 {
		v, err := h.Hash(e.key)
		if err != nil {
			return nil, err
		}
		existingKeyHkey = v[len(hkeyPrefixes):]
	}

	// Create collision group with existing and new elements
	inlineCollisionGroup := newInlineCollisionGroup(hkeyPrefixes)
	inlineCollisionGroup.Set(storage, h, hkeyPrefixes, existingKeyHkey, e.key, e.value)
	inlineCollisionGroup.Set(storage, h, hkeyPrefixes, hkeys, key, value)

	return inlineCollisionGroup, nil
}

func (e *singleElement) Equal(key MapKey) bool {
	return e.key.Equal(key)
}

func (e *singleElement) MapKey() (MapKey, error) {
	return e.key, nil
}

func (e *singleElement) MapValue() (MapValue, error) {
	return e.value, nil
}

func (e *singleElement) Size() uint32 {
	return e.size
}

func (e *singleElement) String() string {
	return fmt.Sprintf("%s:%s", e.key, e.value)
}

func newInlineCollisionGroup(prefix []uint64) *inlineCollisionGroup {
	return &inlineCollisionGroup{
		elements: elements{hkeyPrefixes: prefix},
	}
}

func (e *inlineCollisionGroup) Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error) {
	_, err := e.elements.Set(storage, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *inlineCollisionGroup) Equal(MapKey) bool {
	return false
}

func (e *inlineCollisionGroup) MapKey() (MapKey, error) {
	return nil, errors.New("not applicable")
}

func (e *inlineCollisionGroup) MapValue() (MapValue, error) {
	return nil, errors.New("not applicable")
}

func (e *inlineCollisionGroup) Size() uint32 {
	return e.elements.size
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

func newExternalCollisionGroup(storage SlabStorage, address Address, elems elements) (*externalCollisionGroup, error) {
	// Create new any size MapDataSlab
	slab := newMapDataSlab(storage, address, true)

	// Set slab elements
	// Copy isn't necessary because inline collision group is converted into external collision group
	slab.elements = elems

	// Adjust slab size and first key
	slab.header.size += elems.size

	if len(elems.hkeys) > 0 {
		slab.header.firstKey = elems.hkeys[0]
	}

	id := slab.ID()

	err := storage.Store(id, slab)
	if err != nil {
		return nil, err
	}

	// Create and return wrapper (extCollisionGroup) of any size MapDataSlab
	return &externalCollisionGroup{
		id:   id,
		size: StorageIDStorable(id).ByteSize(),
	}, nil
}

func (e *externalCollisionGroup) Get(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error) {
	slab, err := getMapSlab(storage, e.id)
	if err != nil {
		return nil, err
	}
	return slab.Get(storage, h, hkeyPrefixes, hkeys, key)
}

func (e *externalCollisionGroup) Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (element, error) {
	slab, err := getMapSlab(storage, e.id)
	if err != nil {
		return nil, err
	}
	err = slab.Set(storage, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *externalCollisionGroup) Equal(_ MapKey) bool {
	return false
}

func (e *externalCollisionGroup) MapKey() (MapKey, error) {
	return nil, errors.New("not applicable")
}

func (e *externalCollisionGroup) MapValue() (MapValue, error) {
	return nil, errors.New("not applicable")
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
		return elements{}, err
	}
	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return elements{}, fmt.Errorf("expect MapDataSlab, got %T", slab)
	}
	return dataSlab.elements, nil
}

func (e *externalCollisionGroup) String() string {
	return fmt.Sprintf("external group(%d)", e.id)
}

func (e *elements) Get(storage SlabStorage, h Hasher, hkeyPrefixes, hkeys []uint64, key MapKey) (MapValue, error) {

	// Check for mismatch between hkeyPrefixes/hkeys and elements' state
	if len(hkeys) == 0 && len(e.hkeys) > 0 {
		panic(fmt.Sprintf("elements.Get(hkeyPrefixes %v, hkeys %v) internal hkeys are not empty", hkeyPrefixes, hkeys))
	}
	if len(hkeys) > 0 && len(e.hkeys) == 0 {
		panic(fmt.Sprintf("elements.Get(hkeyPrefixes %v, hkeys %v) internal hkeys are empty", hkeyPrefixes, hkeys))
	}

	if len(hkeys) == 0 {
		// linear search by key
		for _, elem := range e.elems {
			if elem.Equal(key) {
				return elem.MapValue()
			}
		}

		return nil, fmt.Errorf("key %s not found", key)
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

	if group, ok := elem.(elementGroup); ok {
		// Search key in collision group with adjusted hkeyPrefix and hkey
		return group.Get(storage, h, append(hkeyPrefixes, hkey), hkeys[1:], key)
	}

	return elem.Get(storage, h, hkeyPrefixes, hkeys, key)
}

// Set returns updated element index in e.elems.
// Caller can separate modified element into a separate slab.
func (e *elements) Set(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey, value MapValue) (int, error) {

	// Check for mismatch between hkeyPrefixes/hkeys and elements' state
	if len(hkeys) == 0 && len(e.hkeys) > 0 {
		panic(fmt.Sprintf("elements.Set(hkeyPrefixes %v, hkeys %v) internal hkeys are not empty", hkeyPrefixes, hkeys))
	}
	if len(hkeys) > 0 && len(e.hkeys) == 0 && len(e.elems) > 0 {
		panic(fmt.Sprintf("elements.Set(hkeyPrefixes %v, hkeys %v) elements internal hkeys are empty", hkeyPrefixes, hkeys))
	}

	newElem := newSingleElement(key, value)

	if len(hkeys) == 0 {
		// linear search key and update value
		for i := 0; i < len(e.elems); i++ {
			elem := e.elems[i]
			if elem.Equal(key) {
				oldSize := elem.Size()

				elem, err := elem.Set(storage, h, hkeyPrefixes, hkeys, key, value)
				if err != nil {
					return 0, err
				}

				e.elems[i] = elem
				e.size += elem.Size() - oldSize
				return i, nil
			}
		}

		// no matching key, append new element to the end.
		e.elems = append(e.elems, newElem)
		e.size += newElem.size
		return len(e.elems) - 1, nil
	}

	hkey := hkeys[0]

	if len(e.hkeys) == 0 {
		// only element

		e.hkeys = []uint64{hkey}

		e.elems = []element{newElem}

		e.size += newElem.size

		return 0, nil
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

		return 0, nil
	}

	if hkey > e.hkeys[len(e.hkeys)-1] {
		// append key and value

		e.hkeys = append(e.hkeys, hkey)

		e.elems = append(e.elems, newElem)

		e.size += newElem.size

		return len(e.elems) - 1, nil
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

		if _, ok := elem.(elementGroup); ok {
			// Adjust hkeyPrefixes and hkey for collision group
			hkeyPrefixes = append(hkeyPrefixes, hkeys[0])
			hkeys = hkeys[1:]
		}

		elem, err := elem.Set(storage, h, hkeyPrefixes, hkeys, key, value)
		if err != nil {
			return 0, err
		}

		e.elems[equalIndex] = elem

		e.size += elem.Size() - oldElemSize

		return equalIndex, nil
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

	return lessThanIndex, nil
}

func (e *elements) Iterate(storage SlabStorage, fn MapIterationFunc) error {
	for _, e := range e.elems {

		if c, ok := e.(elementGroup); ok {

			elem, err := c.Elements(storage)
			if err != nil {
				return err
			}
			err = elem.Iterate(storage, fn)
			if err != nil {
				return err
			}

		} else {

			k, err := e.MapKey()
			if err != nil {
				return err
			}
			kv, err := k.StoredValue(storage)
			if err != nil {
				return err
			}

			v, err := e.MapValue()
			if err != nil {
				return err
			}
			vv, err := v.StoredValue(storage)
			if err != nil {
				return err
			}

			resume, err := fn(kv, vv)
			if err != nil {
				return err
			}
			if !resume {
				return nil
			}
		}
	}
	return nil
}

func (e *elements) String() string {
	var s []string
	s = append(s, fmt.Sprintf("(prefix %v)", e.hkeyPrefixes))
	for i := 0; i < len(e.elems); i++ {
		if len(e.hkeys) > 0 {
			s = append(s, fmt.Sprintf("%d:%s", e.hkeys[i], e.elems[i].String()))
		} else {
			s = append(s, fmt.Sprintf(":%s", e.elems[i].String()))
		}
	}
	return strings.Join(s, " ")
}

func newMapDataSlab(storage SlabStorage, address Address, anySize bool) *MapDataSlab {
	return &MapDataSlab{
		header: MapSlabHeader{
			id:   storage.GenerateStorageID(address),
			size: mapDataSlabPrefixSize,
		},
		anySize: anySize,
	}
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
		address: m.ID().Address,
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

	elemIndex, err := m.elements.Set(storage, h, hkeyPrefixes, hkeys, key, value)
	if err != nil {
		return err
	}

	// Separate inline collision group into any sized data slab if it's too large and data slab is size restricted.
	if !m.anySize {
		if group, ok := m.elements.elems[elemIndex].(elementGroup); ok {
			if group.Inline() && group.Size() > uint32(MaxInlineElementSize) {

				oldSize := m.elements.elems[elemIndex].Size()

				elements, err := group.Elements(storage)
				if err != nil {
					return err
				}

				ext, err := newExternalCollisionGroup(storage, m.ID().Address, elements)
				if err != nil {
					return err
				}

				m.elements.elems[elemIndex] = ext

				m.elements.size += ext.Size() - oldSize
			}
		}
	}

	// Adjust header's first key
	if elemIndex == 0 {
		m.header.firstKey = m.elements.hkeys[0]
	}

	// Adjust header's slab size
	m.header.size = mapDataSlabPrefixSize + m.elements.size

	// Store modified slab
	return storage.Store(m.header.id, m)
}

func (m *MapDataSlab) Remove(storage SlabStorage, h Hasher, hkeyPrefixes []uint64, hkeys []uint64, key MapKey) (MapValue, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *MapDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if len(m.elems) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split slab with less than 2 elements")
	}

	// This computes the ceil of split to give the first slab more elements.
	dataSize := m.elements.size
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, e := range m.elements.elems {
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

	rightCount := len(m.elems) - leftCount

	// Create right slab elements
	rightElements := elements{}

	rightElements.hkeyPrefixes = make([]uint64, len(m.hkeyPrefixes))
	copy(rightElements.hkeyPrefixes, m.hkeyPrefixes)

	rightElements.hkeys = make([]uint64, rightCount)
	copy(rightElements.hkeys, m.hkeys[leftCount:])

	rightElements.elems = make([]element, rightCount)
	copy(rightElements.elems, m.elements.elems[leftCount:])

	rightElements.size = dataSize - leftSize

	// Create new right slab
	rightSlab := &MapDataSlab{
		header: MapSlabHeader{
			id:       storage.GenerateStorageID(m.ID().Address),
			size:     mapDataSlabPrefixSize + rightElements.size,
			firstKey: m.hkeys[leftCount],
		},
		prev:     m.header.id,
		next:     m.next,
		elements: rightElements,
		anySize:  m.anySize,
	}

	// Modify left (original) slab
	m.header.size = mapDataSlabPrefixSize + leftSize
	m.next = rightSlab.header.id
	m.elements.hkeys = m.elements.hkeys[:leftCount]
	m.elements.elems = m.elements.elems[:leftCount]
	m.elements.size = leftSize

	// NOTE: prevent memory leak
	for i := leftCount; i < len(m.elements.hkeys); i++ {
		m.elements.hkeys[i] = 0
	}
	for i := leftCount; i < len(m.elements.elems); i++ {
		m.elements.elems[i] = nil
	}

	return m, rightSlab, nil
}

func (m *MapDataSlab) Merge(slab Slab) error {
	// TODO: verify that both slabs have the same hkeyPrefixes

	rightSlab := slab.(*MapDataSlab)

	m.elements.hkeys = append(m.elements.hkeys, rightSlab.elements.hkeys...)
	m.elements.elems = append(m.elements.elems, rightSlab.elements.elems...)
	m.elements.size += rightSlab.elements.size

	m.header.size = mapDataSlabPrefixSize + m.elements.size

	// Set right slab elements to nil to prevent memory leak
	for i := 0; i < len(rightSlab.elems); i++ {
		rightSlab.elems[i] = nil
	}

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

func (m *MapDataSlab) String() string {
	elems := m.elements.elems

	if len(elems) <= 6 {
		return m.elements.String()
	}

	var str []string
	for i := 0; i < 3; i++ {
		str = append(str, elems[i].String())
	}

	str = append(str, "...")

	elemLength := len(elems)
	for i := elemLength - 3; i < elemLength; i++ {
		str = append(str, elems[i].String())
	}

	return fmt.Sprintf("{%s}", strings.Join(str, " "))
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
		address: m.ID().Address,
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

func (m *MapMetaDataSlab) String() string {
	var hStr []string
	for _, h := range m.childrenHeaders {
		hStr = append(hStr, fmt.Sprintf("%+v", h))
	}
	return strings.Join(hStr, " ")
}

func NewMap(storage SlabStorage, address Address, hasher Hasher) (*OrderedMap, error) {
	root := newMapDataSlab(storage, address, false)

	err := storage.Store(root.header.id, root)
	if err != nil {
		return nil, err
	}

	return &OrderedMap{
		storage: storage,
		address: address,
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

	if m.root.IsFull() {

		// Save root node id
		rootID := m.root.ID()

		// Assign a new storage id to old root before splitting it.
		oldRoot := m.root
		oldRoot.SetID(m.storage.GenerateStorageID(m.address))

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

type MapIterationFunc func(Value, Value) (resume bool, err error)

func (m *OrderedMap) Iterate(fn MapIterationFunc) error {
	slab, err := firstMapDataSlab(m.storage, m.root)
	if err != nil {
		return err
	}

	id := slab.ID()

	for id != StorageIDUndefined {
		slab, found, err := m.storage.Retrieve(id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("slab %d not found", id)
		}

		dataSlab := slab.(*MapDataSlab)

		err = dataSlab.elements.Iterate(m.storage, fn)
		if err != nil {
			return err
		}

		id = dataSlab.next
	}

	return nil
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
	return m.address
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
