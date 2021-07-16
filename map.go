/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/aead/siphash"
)

type Key Storable
type Value Storable

type MapSlabHeader struct {
	id       StorageID // id is used to retrieve slab from storage
	size     uint32    // size is used to split and merge; leaf: size of all element; internal: size of all headers
	firstKey uint64    // firstKey (first hashed key) is used to lookup value
}

// MapDataSlab is leaf node, implementing MapSlab.
type MapDataSlab struct {
	prev   StorageID
	next   StorageID
	header MapSlabHeader
	hkeys  []uint64 // sorted list of hashed keys
	keys   []Key    // keys correspond to hkeys (in case of hash collision, ordered by insertion order)
	values []Value
}

// MapMetaDataSlab is internal node, implementing MapSlab.
type MapMetaDataSlab struct {
	header          MapSlabHeader
	childrenHeaders []MapSlabHeader
}

type MapSlab interface {
	Has(storage SlabStorage, hkey uint64, key Key) (bool, error)
	Get(storage SlabStorage, hkey uint64, key Key) (Value, error)
	Set(storage SlabStorage, hkey uint64, key Key, value Value) error
	Remove(storage SlabStorage, hkey uint64, key Key) (Value, error)

	ShallowCloneWithNewID() MapSlab

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetID(StorageID)

	Header() MapSlabHeader

	Slab
}

type OrderedMap struct {
	storage   SlabStorage
	root      MapSlab
	secretKey [16]byte
}

const (
	// slab header size: storage id (8 bytes) + size (4 bytes) + hashed first key (8 bytes)
	mapSlabHeaderSize = 8 + 4 + 8

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + child header count (2 bytes)
	mapMetaDataSlabPrefixSize = 1 + 1 + 2

	// version (1 byte) + flag (1 byte) + prev id (8 bytes) + next id (8 bytes) + CBOR array size (3 bytes) for keys and values
	// (3 bytes of array size support up to 65535 array elements)
	mapDataSlabPrefixSize = 2 + 8 + 8 + 3
)

func newMapDataSlab() *MapDataSlab {
	return &MapDataSlab{
		header: MapSlabHeader{
			id:   generateStorageID(),
			size: mapDataSlabPrefixSize,
		},
	}
}

func (m *MapDataSlab) Encode(enc *Encoder) error {
	// TODO: implement me
	return errors.New("not implemented")
}

func (m *MapDataSlab) ShallowCloneWithNewID() MapSlab {
	return &MapDataSlab{
		header: MapSlabHeader{
			id:       generateStorageID(),
			size:     m.header.size,
			firstKey: m.header.firstKey,
		},
		prev:   m.prev,
		next:   m.next,
		hkeys:  m.hkeys,
		keys:   m.keys,
		values: m.values,
	}
}

func (m *MapDataSlab) Has(storage SlabStorage, hkey uint64, key Key) (bool, error) {
	_, err := m.Get(storage, hkey, key)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (m *MapDataSlab) Get(storage SlabStorage, hkey uint64, key Key) (Value, error) {
	// Find first index that m.hkeys[h] == hkey
	ans := -1
	i, j := 0, len(m.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.hkeys[h] > hkey {
			j = h
		} else if m.hkeys[h] < hkey {
			i = h + 1
		} else {
			ans = h
			j = h
		}
	}

	if ans == -1 {
		return nil, fmt.Errorf("key %s not found", key)
	}

	// Check key equality and handle hash collision
	for i := ans; i < len(m.hkeys) && m.hkeys[i] == hkey; i++ {
		if m.keys[i].Equal(key) {
			return m.values[i], nil
		}
	}

	return nil, fmt.Errorf("key %s not found", key)
}

// Set replace existing value or insert new key-value.
func (m *MapDataSlab) Set(storage SlabStorage, hkey uint64, key Key, value Value) error {

	if len(m.hkeys) == 0 {
		m.hkeys = []uint64{hkey}
		m.keys = []Key{key}
		m.values = []Value{value}

		// Adjust first key
		m.header.firstKey = hkey

		// Adjust slab size
		m.header.size += key.ByteSize() + value.ByteSize()

		// Store modified slab
		storage.Store(m.header.id, m)

		return nil
	}

	if hkey < m.hkeys[0] {
		// prepend key and value

		m.hkeys = append(m.hkeys, uint64(0))
		copy(m.hkeys[1:], m.hkeys)
		m.hkeys[0] = hkey

		m.keys = append(m.keys, nil)
		copy(m.keys[1:], m.keys)
		m.keys[0] = key

		m.values = append(m.values, nil)
		copy(m.values[1:], m.values)
		m.values[0] = value

		// Adjust first key
		m.header.firstKey = hkey

		// Adjust slab size
		m.header.size += key.ByteSize() + value.ByteSize()

		// Store modified slab
		storage.Store(m.header.id, m)

		return nil
	}

	if hkey > m.hkeys[len(m.hkeys)-1] {
		// append key and value

		m.hkeys = append(m.hkeys, hkey)

		m.keys = append(m.keys, key)

		m.values = append(m.values, value)

		// Adjust slab size
		m.header.size += key.ByteSize() + value.ByteSize()

		// Store modified slab
		storage.Store(m.header.id, m)

		return nil
	}

	equalIndex := -1   // first index that m.hkeys[h] == hkey
	lessThanIndex := 0 // first index that m.hkeys[h] > hkey
	i, j := 0, len(m.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.hkeys[h] > hkey {
			lessThanIndex = h
			j = h
		} else if m.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			j = h
		}
	}

	if equalIndex != -1 {
		// Check existing key values and handle hash collision
		i := equalIndex
		for ; i < len(m.hkeys) && m.hkeys[i] == hkey; i++ {
			if m.keys[i].Equal(key) {
				// Save old value
				oldValue := m.values[i]

				// Update value
				m.values[i] = value

				if i == 0 {
					// Adjust first key
					m.header.firstKey = hkey
				}

				// Adjust slab size
				m.header.size += value.ByteSize() - oldValue.ByteSize()

				// Store modified slab
				storage.Store(m.header.id, m)

				return nil
			}
		}

		// no matching key, insert to the end of idential hash key
		lessThanIndex = i
	}

	// insert into sorted hkeys
	m.hkeys = append(m.hkeys, uint64(0))
	copy(m.hkeys[lessThanIndex+1:], m.hkeys[lessThanIndex:])
	m.hkeys[lessThanIndex] = hkey

	// insert into sorted keys
	m.keys = append(m.keys, nil)
	copy(m.keys[lessThanIndex+1:], m.keys[lessThanIndex:])
	m.keys[lessThanIndex] = key

	// insert into sorted values
	m.values = append(m.values, nil)
	copy(m.values[lessThanIndex+1:], m.values[lessThanIndex:])
	m.values[lessThanIndex] = value

	// Update header's firstKey
	if lessThanIndex == 0 {
		m.header.firstKey = hkey
	}

	// Adjust slab size
	m.header.size += key.ByteSize() + value.ByteSize()

	// Store modified slab
	storage.Store(m.header.id, m)

	return nil
}

func (m *MapDataSlab) Remove(storage SlabStorage, hkey uint64, key Key) (Value, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *MapDataSlab) Split() (Slab, Slab, error) {
	if len(m.keys) < 2 {
		// Can't split slab with less than two elements
		return nil, nil, fmt.Errorf("can't split slab with less than 2 elements")
	}

	// This computes the ceil of split to give the first slab more elements.
	dataSize := m.header.size - mapDataSlabPrefixSize
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, k := range m.keys {
		v := m.values[i]
		elemSize := k.ByteSize() + v.ByteSize()
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

	rightCount := len(m.hkeys) - leftCount

	// Create new right slab
	rightSlab := &MapDataSlab{
		header: MapSlabHeader{
			id:       generateStorageID(),
			size:     mapDataSlabPrefixSize + dataSize - leftSize,
			firstKey: m.hkeys[leftCount],
		},
		prev: m.header.id,
		next: m.next,
	}

	// Create right slab hkeys
	rightSlab.hkeys = make([]uint64, rightCount)
	copy(rightSlab.hkeys, m.hkeys[leftCount:])

	// Create right slab keys
	rightSlab.keys = make([]Key, rightCount)
	copy(rightSlab.keys, m.keys[leftCount:])

	// Create right slab values and remove those values from left slab
	rightSlab.values = make([]Value, rightCount)
	copy(rightSlab.values, m.values[leftCount:])

	// Modify left (original) slab
	m.header.size = mapDataSlabPrefixSize + leftSize
	m.next = rightSlab.header.id
	m.hkeys = m.hkeys[:leftCount]
	m.keys = m.keys[:leftCount]
	m.values = m.values[:leftCount]

	return m, rightSlab, nil
}

func (m *MapDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*MapDataSlab)

	m.hkeys = append(m.hkeys, rightSlab.hkeys...)

	m.keys = append(m.keys, rightSlab.keys...)

	for k, v := range rightSlab.values {
		m.values[k] = v
	}

	m.header.size = m.header.size + rightSlab.header.size - mapDataSlabPrefixSize

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
	return m.header.size > uint32(maxThreshold)
}

func (m *MapDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > m.header.size {
		return uint32(minThreshold) - m.header.size, true
	}
	return 0, false
}

func (m *MapDataSlab) CanLendToLeft(size uint32) bool {
	// TODO: implement me
	return false
}

func (m *MapDataSlab) CanLendToRight(size uint32) bool {
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

func (m *MapDataSlab) Mutable() bool {
	return true
}

func (m *MapDataSlab) ID() StorageID {
	return m.header.id
}

func (v *MapDataSlab) Equal(other Storable) bool {
	otherSlab, ok := other.(*MapDataSlab)
	if !ok {
		return false
	}
	return otherSlab.ID() == v.ID()
}

func (m *MapDataSlab) ByteSize() uint32 {
	return m.header.size
}

func (m *MapDataSlab) String() string {
	var kvStr []string
	if len(m.keys) <= 6 {
		for i, k := range m.keys {
			kvStr = append(kvStr, fmt.Sprintf("%d:%s:%s", m.hkeys[i], k, m.values[i]))
		}
	} else {
		for i := 0; i < 3; i++ {
			k := m.keys[i]
			kvStr = append(kvStr, fmt.Sprintf("%d:%s:%s", m.hkeys[i], k, m.values[i]))
		}
		kvStr = append(kvStr, "...")
		for i := len(m.keys) - 3; i < len(m.keys); i++ {
			k := m.keys[i]
			kvStr = append(kvStr, fmt.Sprintf("%d:%s:%s", m.hkeys[i], k, m.values[i]))
		}
	}

	return fmt.Sprintf("{%s}", strings.Join(kvStr, " "))
}

func (m *MapMetaDataSlab) Encode(enc *Encoder) error {
	// TODO: implement me
	return errors.New("not implemented")
}

func (m *MapMetaDataSlab) ShallowCloneWithNewID() MapSlab {
	return &MapMetaDataSlab{
		header: MapSlabHeader{
			id:       generateStorageID(),
			size:     m.header.size,
			firstKey: m.header.firstKey,
		},
		childrenHeaders: m.childrenHeaders,
	}
}

func (m *MapMetaDataSlab) Has(storage SlabStorage, hkey uint64, key Key) (bool, error) {
	_, err := m.Get(storage, hkey, key)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *MapMetaDataSlab) Get(storage SlabStorage, hkey uint64, key Key) (Value, error) {
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

	return child.Get(storage, hkey, key)
}

func (m *MapMetaDataSlab) Set(storage SlabStorage, hkey uint64, key Key, value Value) error {
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

	err = child.Set(storage, hkey, key, value)
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

	storage.Store(m.header.id, m)

	return nil
}

func (m *MapMetaDataSlab) Remove(storage SlabStorage, hkey uint64, key Key) (Value, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *MapMetaDataSlab) SplitChildSlab(storage SlabStorage, child MapSlab, childHeaderIndex int) error {
	leftSlab, rightSlab, err := child.Split()
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
	storage.Store(left.ID(), left)
	storage.Store(right.ID(), right)
	storage.Store(m.header.id, m)

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
func (m *MapMetaDataSlab) MergeOrRebalanceChildSlab(storage SlabStorage, child MapSlab, childHeaderIndex int, underflowSize uint32) error {

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
			storage.Store(child.ID(), child)
			storage.Store(rightSib.ID(), rightSib)
			storage.Store(m.header.id, m)
			return nil
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
			storage.Store(leftSib.ID(), leftSib)
			storage.Store(child.ID(), child)
			storage.Store(m.header.id, m)
			return nil
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
			storage.Store(leftSib.ID(), leftSib)
			storage.Store(child.ID(), child)
			storage.Store(m.header.id, m)
			return nil
		}

		err := child.BorrowFromRight(rightSib)
		if err != nil {
			return err
		}

		m.childrenHeaders[childHeaderIndex] = child.Header()
		m.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

		// Store modified slabs
		storage.Store(child.ID(), child)
		storage.Store(rightSib.ID(), rightSib)
		storage.Store(m.header.id, m)
		return nil
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
		storage.Store(child.ID(), child)
		storage.Store(m.header.id, m)

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
		storage.Store(leftSib.ID(), leftSib)
		storage.Store(m.header.id, m)

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
		storage.Store(leftSib.ID(), leftSib)
		storage.Store(m.header.id, m)

		// Remove child from storage
		storage.Remove(child.ID())

		return nil
	}

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
	storage.Store(child.ID(), child)
	storage.Store(m.header.id, m)

	// Remove rightSib from storage
	storage.Remove(rightSib.ID())

	return nil
}

func (m *MapMetaDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*MapMetaDataSlab)

	m.childrenHeaders = append(m.childrenHeaders, rightSlab.childrenHeaders...)
	m.header.size += rightSlab.header.size - mapMetaDataSlabPrefixSize

	return nil
}

func (m *MapMetaDataSlab) Split() (Slab, Slab, error) {
	if len(m.childrenHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		return nil, nil, fmt.Errorf("can't split meta slab with less than 2 headers")
	}

	leftChildrenCount := int(math.Ceil(float64(len(m.childrenHeaders)) / 2))
	leftSize := leftChildrenCount * mapSlabHeaderSize

	// Construct right slab
	rightSlab := &MapMetaDataSlab{
		header: MapSlabHeader{
			id:       generateStorageID(),
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

func (m *MapMetaDataSlab) Mutable() bool {
	return true
}

func (m *MapMetaDataSlab) ID() StorageID {
	return m.header.id
}

func (v *MapMetaDataSlab) Equal(other Storable) bool {
	otherSlab, ok := other.(*MapMetaDataSlab)
	if !ok {
		return false
	}
	return otherSlab.ID() == v.ID()
}

func (m *MapMetaDataSlab) String() string {
	var hStr []string
	for _, h := range m.childrenHeaders {
		hStr = append(hStr, fmt.Sprintf("%+v", h))
	}
	return strings.Join(hStr, " ")
}

func NewMap(storage SlabStorage, secretkey [16]byte) *OrderedMap {
	root := newMapDataSlab()

	storage.Store(root.header.id, root)

	return &OrderedMap{
		storage:   storage,
		root:      root,
		secretKey: secretkey,
	}
}

func (m *OrderedMap) Has(key Key) (bool, error) {
	hkey := siphash.Sum64([]byte(key.String()), &m.secretKey)
	return m.root.Has(m.storage, hkey, key)
}

func (m *OrderedMap) Get(key Key) (Value, error) {
	hkey := siphash.Sum64([]byte(key.String()), &m.secretKey)

	v, err := m.root.Get(m.storage, hkey, key)
	if err != nil {
		return nil, err
	}

	// TODO: optimize this
	if idValue, ok := v.(StorageIDValue); ok {
		id := StorageID(idValue)
		if id == StorageIDUndefined {
			return nil, fmt.Errorf("invalid storage id")
		}
		slab, found, err := m.storage.Retrieve(id)
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

func (m *OrderedMap) Set(key Key, value Value) error {
	if value.Mutable() || value.ByteSize() > uint32(maxInlineElementSize) {
		value = StorageIDValue(value.ID())
	}

	hkey := siphash.Sum64([]byte(key.String()), &m.secretKey)

	err := m.root.Set(m.storage, hkey, key, value)
	if err != nil {
		return err
	}

	if m.root.IsFull() {

		copiedRoot := m.root.ShallowCloneWithNewID()

		// Split copied root
		leftSlab, rightSlab, err := copiedRoot.Split()
		if err != nil {
			return err
		}

		left := leftSlab.(MapSlab)
		right := rightSlab.(MapSlab)

		if m.root.IsData() {
			// Create new MapMetaDataSlab with the same storage ID as root
			rootID := m.root.ID()
			m.root = &MapMetaDataSlab{
				header: MapSlabHeader{
					id: rootID,
				},
			}
		}

		root := m.root.(*MapMetaDataSlab)
		root.childrenHeaders = []MapSlabHeader{left.Header(), right.Header()}
		root.header.size = mapMetaDataSlabPrefixSize + mapSlabHeaderSize*uint32(len(root.childrenHeaders))
		root.header.firstKey = left.Header().firstKey

		m.storage.Store(left.ID(), left)
		m.storage.Store(right.ID(), right)
		m.storage.Store(m.root.ID(), m.root)
	}

	return nil
}

func (m *OrderedMap) Remove(key Key) (Value, error) {
	// TODO: implement me
	return nil, errors.New("not implemented")
}

func (m *OrderedMap) Iterate(fn func(Key, Storable)) error {
	slab, err := firstMapDataSlab(m.storage, m.root)
	if err != nil {
		return nil
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

		for i, k := range dataSlab.keys {
			fn(k, dataSlab.values[i])
		}

		id = dataSlab.next
	}

	return nil
}

func (m *OrderedMap) StorageID() StorageID {
	return m.root.Header().id
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

	return nil, SlabNotFoundError{id, err}
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
