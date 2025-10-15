/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright Flow Foundation
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
	"errors"
	"fmt"
	"strings"

	"github.com/fxamacker/circlehash"
)

const (
	// typicalRandomConstant is a 64-bit value that has qualities
	// of a typical random value (e.g. hamming weight, number of
	// consecutive groups of 1-bits, etc.) so it can be useful as
	// a const part of a seed, round constant inside a permutation, etc.
	// CAUTION: We only store 64-bit seed, so some hashes with 64-bit seed like
	// CircleHash64f don't use this const.  However, other hashes such as
	// CircleHash64fx and SipHash might use this const as part of their
	// 128-bit seed (when they don't use 64-bit -> 128-bit seed expansion func).
	typicalRandomConstant = uint64(0x1BD11BDAA9FC1A22) // DO NOT MODIFY
)

// OrderedMap is an ordered map of key-value pairs; keys can be any hashable type
// and values can be any serializable value type. It supports heterogeneous key
// or value types (e.g. first key storing a boolean and second key storing a string).
// OrderedMap keeps values in specific sorted order and operations are deterministic
// so the state of the segments after a sequence of operations are always unique.
//
// OrderedMap key-value pairs can be stored in one or more relatively fixed-sized segments.
//
// OrderedMap can be inlined into its parent container when the entire content fits in
// parent container's element size limit.  Specifically, OrderedMap with one segment
// which fits in size limit can be inlined, while OrderedMap with multiple segments
// can't be inlined.
type OrderedMap struct {
	Storage         SlabStorage
	root            MapSlab
	digesterBuilder DigesterBuilder

	// parentUpdater is a callback that notifies parent container when this map is modified.
	// If this callback is nil, this map has no parent.  Otherwise, this map has parent
	// and this callback must be used when this map is changed by Set and Remove.
	//
	// parentUpdater acts like "parent pointer".  It is not stored physically and is only in memory.
	// It is setup when child map is returned from parent's Get.  It is also setup when
	// new child is added to parent through Set or Insert.
	parentUpdater parentUpdater
}

var _ Value = &OrderedMap{}
var _ mutableValueNotifier = &OrderedMap{}

// Create, copy, and load array

func NewMap(storage SlabStorage, address Address, digestBuilder DigesterBuilder, typeInfo TypeInfo) (*OrderedMap, error) {

	// Create root slab ID
	sID, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	// Create seed for non-crypto hash algos (CircleHash64, SipHash) to use.
	// Ideally, seed should be a nondeterministic 128-bit secret because
	// these hashes rely on its key being secret for its security.  Since
	// we handle collisions and based on other factors such as storage space,
	// the team decided we can use a 64-bit non-secret key instead of
	// a 128-bit secret key. And for performance reasons, we first use
	// noncrypto hash algos and fall back to crypto algo after collisions.
	// This is for creating the seed, so the seed used here is OK to be 0.
	// LittleEndian is needed for compatibility (same digest from []byte and
	// two uint64).
	a := binary.LittleEndian.Uint64(sID.address[:])
	b := binary.LittleEndian.Uint64(sID.index[:])
	k0 := circlehash.Hash64Uint64x2(a, b, uint64(0))

	// To save storage space, only store 64-bits of the seed.
	// Use a 64-bit const for the unstored half to create 128-bit seed.
	k1 := typicalRandomConstant

	digestBuilder.SetSeed(k0, k1)

	// Create extra data with type info and seed
	extraData := &MapExtraData{TypeInfo: typeInfo, Seed: k0}

	root := &MapDataSlab{
		header: MapSlabHeader{
			slabID: sID,
			size:   mapRootDataSlabPrefixSize + hkeyElementsPrefixSize,
		},
		elements:  newHkeyElements(0),
		extraData: extraData,
	}

	err = storeSlab(storage, root)
	if err != nil {
		return nil, err
	}

	return &OrderedMap{
		Storage:         storage,
		root:            root,
		digesterBuilder: digestBuilder,
	}, nil
}

func NewMapWithRootID(storage SlabStorage, rootID SlabID, digestBuilder DigesterBuilder) (*OrderedMap, error) {
	if rootID == SlabIDUndefined {
		return nil, NewSlabIDErrorf("cannot create OrderedMap from undefined slab ID")
	}

	root, err := getMapSlab(storage, rootID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}

	extraData := root.ExtraData()
	if extraData == nil {
		return nil, NewNotValueError(rootID)
	}

	digestBuilder.SetSeed(extraData.Seed, typicalRandomConstant)

	return &OrderedMap{
		Storage:         storage,
		root:            root,
		digesterBuilder: digestBuilder,
	}, nil
}

type MapElementProvider func() (Value, Value, error)

// NewMapFromBatchData returns a new map with elements provided by fn callback.
// Provided seed must be the same seed used to create the original map.
// And callback function must return elements in the same order as the original map.
// New map uses and stores the same seed as the original map.
// This function should only be used for copying a map.
func NewMapFromBatchData(
	storage SlabStorage,
	address Address,
	digesterBuilder DigesterBuilder,
	typeInfo TypeInfo,
	comparator ValueComparator,
	hip HashInputProvider,
	seed uint64,
	fn MapElementProvider,
) (
	*OrderedMap,
	error,
) {

	const defaultElementCountInSlab = 32

	if seed == 0 {
		return nil, NewHashSeedUninitializedError()
	}

	// Seed digester
	digesterBuilder.SetSeed(seed, typicalRandomConstant)

	var slabs []MapSlab

	id, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	elements := &hkeyElements{
		level: 0,
		size:  hkeyElementsPrefixSize,
		hkeys: make([]Digest, 0, defaultElementCountInSlab),
		elems: make([]element, 0, defaultElementCountInSlab),
	}

	count := uint64(0)

	var prevHkey Digest

	// Appends all elements
	for {
		key, value, err := fn()
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapElementProvider callback.
			return nil, wrapErrorAsExternalErrorIfNeeded(err)
		}
		if key == nil {
			break
		}

		digester, err := digesterBuilder.Digest(hip, key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
		}

		hkey, err := digester.Digest(0)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Digester interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to generate map key digest for level 0")
		}

		if hkey < prevHkey {
			// a valid map will always have sorted digests
			return nil, NewHashError(fmt.Errorf("digest isn't sorted (found %d before %d)", prevHkey, hkey))
		}

		if hkey == prevHkey && count > 0 {
			// found collision

			lastElementIndex := len(elements.elems) - 1

			prevElem := elements.elems[lastElementIndex]
			prevElemSize := prevElem.Size()

			elem, _, existingMapValueStorable, err := prevElem.Set(storage, address, digesterBuilder, digester, 0, hkey, comparator, hip, key, value)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by element.Set().
				return nil, err
			}
			if existingMapValueStorable != nil {
				return nil, NewDuplicateKeyError(key)
			}

			elements.elems[lastElementIndex] = elem
			elements.size += elem.Size() - prevElemSize

			putDigester(digester)

			count++

			continue
		}

		// no collision

		putDigester(digester)

		elem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElememt().
			return nil, err
		}

		// Finalize data slab
		currentSlabSize := mapDataSlabPrefixSize + elements.Size()
		newElementSize := digestSize + elem.Size()
		if currentSlabSize >= targetThreshold ||
			currentSlabSize+newElementSize > maxThreshold {

			// Generate storage id for next data slab
			nextID, err := storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			// Create data slab
			dataSlab := &MapDataSlab{
				header: MapSlabHeader{
					slabID:   id,
					size:     mapDataSlabPrefixSize + elements.Size(),
					firstKey: elements.firstKey(),
				},
				elements: elements,
				next:     nextID,
			}

			// Append data slab to dataSlabs
			slabs = append(slabs, dataSlab)

			// Save id
			id = nextID

			// Create new elements for next data slab
			elements = &hkeyElements{
				level: 0,
				size:  hkeyElementsPrefixSize,
				hkeys: make([]Digest, 0, defaultElementCountInSlab),
				elems: make([]element, 0, defaultElementCountInSlab),
			}
		}

		elements.hkeys = append(elements.hkeys, hkey)
		elements.elems = append(elements.elems, elem)
		elements.size += digestSize + elem.Size()

		prevHkey = hkey

		count++
	}

	// Create last data slab
	dataSlab := &MapDataSlab{
		header: MapSlabHeader{
			slabID:   id,
			size:     mapDataSlabPrefixSize + elements.Size(),
			firstKey: elements.firstKey(),
		},
		elements: elements,
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
					// Don't need to wrap error as external error because err is already categorized by MapSlab.LendToRight().
					return nil, err
				}

			} else {

				// Merge with left
				err := leftSib.Merge(lastSlab)
				if err != nil {
					// Don't need to wrap error as external error because err is already categorized by MapSlab.Merge().
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
			err = storeSlab(storage, slab)
			if err != nil {
				return nil, err
			}
		}

		// Get next level meta slabs
		slabs, err = nextLevelMapSlabs(storage, address, slabs)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by nextLevelMapSlabs().
			return nil, err
		}

	}

	// found root slab
	root := slabs[0]

	// root is data slab, adjust its size
	if dataSlab, ok := root.(*MapDataSlab); ok {
		dataSlab.header.size = dataSlab.header.size - mapDataSlabPrefixSize + mapRootDataSlabPrefixSize
	}

	extraData := &MapExtraData{TypeInfo: typeInfo, Count: count, Seed: seed}

	// Set extra data in root
	root.SetExtraData(extraData)

	// Store root
	err = storeSlab(storage, root)
	if err != nil {
		return nil, err
	}

	return &OrderedMap{
		Storage:         storage,
		root:            root,
		digesterBuilder: digesterBuilder,
	}, nil
}

// nextLevelMapSlabs returns next level meta data slabs from slabs.
// slabs must have at least 2 elements.  It is reused and returned as next level slabs.
// Caller is responsible for rebalance last slab and storing returned slabs in storage.
func nextLevelMapSlabs(storage SlabStorage, address Address, slabs []MapSlab) ([]MapSlab, error) {

	maxNumberOfHeadersInMetaSlab := (maxThreshold - mapMetaDataSlabPrefixSize) / mapSlabHeaderSize

	nextLevelSlabsIndex := 0

	// Generate storage id
	id, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	childrenCount := min(len(slabs), int(maxNumberOfHeadersInMetaSlab))

	metaSlab := &MapMetaDataSlab{
		header: MapSlabHeader{
			slabID:   id,
			size:     mapMetaDataSlabPrefixSize,
			firstKey: slabs[0].Header().firstKey,
		},
		childrenHeaders: make([]MapSlabHeader, 0, childrenCount),
	}

	for i, slab := range slabs {

		if len(metaSlab.childrenHeaders) == int(maxNumberOfHeadersInMetaSlab) {

			slabs[nextLevelSlabsIndex] = metaSlab
			nextLevelSlabsIndex++

			// compute number of children for next meta data slab
			childrenCount = min(len(slabs)-i, int(maxNumberOfHeadersInMetaSlab))

			// Generate storage id for next meta data slab
			id, err = storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			metaSlab = &MapMetaDataSlab{
				header: MapSlabHeader{
					slabID:   id,
					size:     mapMetaDataSlabPrefixSize,
					firstKey: slab.Header().firstKey,
				},
				childrenHeaders: make([]MapSlabHeader, 0, childrenCount),
			}
		}

		metaSlab.header.size += mapSlabHeaderSize

		metaSlab.childrenHeaders = append(metaSlab.childrenHeaders, slab.Header())
	}

	// Append last meta slab to slabs
	slabs[nextLevelSlabsIndex] = metaSlab
	nextLevelSlabsIndex++

	return slabs[:nextLevelSlabsIndex], nil
}

// Map operations (has, get, set, remove, and pop iterate)

func (m *OrderedMap) Has(comparator ValueComparator, hip HashInputProvider, key Value) (bool, error) {
	_, _, err := m.get(comparator, hip, key)
	if err != nil {
		var knf *KeyNotFoundError
		if errors.As(err, &knf) {
			return false, nil
		}
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Get().
		return false, err
	}
	return true, nil
}

func (m *OrderedMap) Get(comparator ValueComparator, hip HashInputProvider, key Value) (Value, error) {

	keyStorable, valueStorable, err := m.get(comparator, hip, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
		return nil, err
	}

	v, err := valueStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	// As a parent, this map (m) sets up notification callback with child
	// value (v) so this map can be notified when child value is modified.
	maxInlineSize := maxInlineMapValueSize(keyStorable.ByteSize())
	m.setCallbackWithChild(comparator, hip, key, v, maxInlineSize)

	return v, nil
}

func (m *OrderedMap) get(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, Storable, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return m.root.Get(m.Storage, keyDigest, level, hkey, comparator, key)
}

func (m *OrderedMap) getElementAndNextKey(comparator ValueComparator, hip HashInputProvider, key Value) (Value, Value, Value, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	keyStorable, valueStorable, nextKeyStorable, err := m.root.getElementAndNextKey(m.Storage, keyDigest, level, hkey, comparator, key)
	if err != nil {
		return nil, nil, nil, err
	}

	k, err := keyStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	v, err := valueStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	var nextKey Value
	if nextKeyStorable != nil {
		nextKey, err = nextKeyStorable.StoredValue(m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}
	}

	// As a parent, this map (m) sets up notification callback with child
	// value (v) so this map can be notified when child value is modified.
	maxInlineSize := maxInlineMapValueSize(keyStorable.ByteSize())
	m.setCallbackWithChild(comparator, hip, key, v, maxInlineSize)

	return k, v, nextKey, nil
}

func (m *OrderedMap) getNextKey(comparator ValueComparator, hip HashInputProvider, key Value) (Value, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	_, _, nextKeyStorable, err := m.root.getElementAndNextKey(m.Storage, keyDigest, level, hkey, comparator, key)
	if err != nil {
		return nil, err
	}

	if nextKeyStorable == nil {
		return nil, nil
	}

	nextKey, err := nextKeyStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	return nextKey, nil
}

func (m *OrderedMap) Set(comparator ValueComparator, hip HashInputProvider, key Value, value Value) (Storable, error) {
	storable, err := m.set(comparator, hip, key, value)
	if err != nil {
		return nil, err
	}

	// If overwritten storable is an inlined slab, uninline the slab and store it in storage.
	// This is to prevent potential data loss because the overwritten inlined slab was not in
	// storage and any future changes to it would have been lost.

	storable, _, _, err = uninlineStorableIfNeeded(m.Storage, storable)
	if err != nil {
		return nil, err
	}

	return storable, nil
}

func (m *OrderedMap) set(comparator ValueComparator, hip HashInputProvider, key Value, value Value) (Storable, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	keyStorable, existingMapValueStorable, err := m.root.Set(m.Storage, m.digesterBuilder, keyDigest, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, err
	}

	if existingMapValueStorable == nil {
		m.root.ExtraData().incrementCount()
	}

	if !m.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := m.root.(*MapMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err := m.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by OrderedMap.promoteChildAsNewRoot().
				return nil, err
			}
		}
	}

	if m.root.IsFull() {
		err := m.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.splitRoot().
			return nil, err
		}
	}

	// This map (m) is a parent to the new child (value), and this map
	// can also be a child in another container.
	//
	// As a parent, this map needs to setup notification callback with
	// the new child value, so it can be notified when child is modified.
	//
	// If this map is a child, it needs to notify its parent because its
	// content (maybe also its size) is changed by this "Set" operation.

	// If this map is a child, it notifies parent by invoking callback because
	// this map is changed by setting new child.
	err = m.notifyParentIfNeeded()
	if err != nil {
		return nil, err
	}

	// As a parent, this map sets up notification callback with child value
	// so this map can be notified when child value is modified.
	//
	// Setting up notification with new child value can happen at any time
	// (either before or after this map notifies its parent) because
	// setting up notification doesn't trigger any read/write ops on parent or child.
	maxInlineSize := maxInlineMapValueSize(keyStorable.ByteSize())
	m.setCallbackWithChild(comparator, hip, key, value, maxInlineSize)

	return existingMapValueStorable, nil
}

func (m *OrderedMap) Remove(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, Storable, error) {
	keyStorable, valueStorable, err := m.remove(comparator, hip, key)
	if err != nil {
		return nil, nil, err
	}

	// If overwritten storable is an inlined slab, uninline the slab and store it in storage.
	// This is to prevent potential data loss because the overwritten inlined slab was not in
	// storage and any future changes to it would have been lost.

	keyStorable, _, _, err = uninlineStorableIfNeeded(m.Storage, keyStorable)
	if err != nil {
		return nil, nil, err
	}

	valueStorable, _, _, err = uninlineStorableIfNeeded(m.Storage, valueStorable)
	if err != nil {
		return nil, nil, err
	}

	return keyStorable, valueStorable, nil
}

func (m *OrderedMap) remove(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, Storable, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to create map key digest at level %d", level))
	}

	k, v, err := m.root.Remove(m.Storage, keyDigest, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Remove().
		return nil, nil, err
	}

	m.root.ExtraData().decrementCount()

	if !m.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := m.root.(*MapMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err := m.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by OrderedMap.promoteChildAsNewRoot().
				return nil, nil, err
			}
		}
	}

	if m.root.IsFull() {
		err := m.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.splitRoot().
			return nil, nil, err
		}
	}

	// If this map is a child, it notifies parent by invoking callback because
	// this map is changed by removing element.
	err = m.notifyParentIfNeeded()
	if err != nil {
		return nil, nil, err
	}

	return k, v, nil
}

type MapPopIterationFunc func(Storable, Storable)

// PopIterate iterates and removes elements backward.
// Each element is passed to MapPopIterationFunc callback before removal.
func (m *OrderedMap) PopIterate(fn MapPopIterationFunc) error {

	err := m.root.PopIterate(m.Storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.PopIterate().
		return err
	}

	rootID := m.root.SlabID()

	// Set map count to 0 in extraData
	extraData := m.root.ExtraData()
	extraData.Count = 0

	inlined := m.root.Inlined()

	prefixSize := uint32(mapRootDataSlabPrefixSize)
	if inlined {
		prefixSize = uint32(inlinedMapDataSlabPrefixSize)
	}

	// Set root to empty data slab
	m.root = &MapDataSlab{
		header: MapSlabHeader{
			slabID: rootID,
			size:   prefixSize + hkeyElementsPrefixSize,
		},
		elements:  newHkeyElements(0),
		extraData: extraData,
		inlined:   inlined,
	}

	if !m.Inlined() {
		// Save root slab
		err = storeSlab(m.Storage, m.root)
		if err != nil {
			return err
		}
	}

	return nil
}

// Slab operations (split root, promote child slab to root)

func (m *OrderedMap) splitRoot() error {

	if m.root.IsData() {
		// Adjust root data slab size before splitting
		dataSlab := m.root.(*MapDataSlab)
		dataSlab.header.size = dataSlab.header.size - mapRootDataSlabPrefixSize + mapDataSlabPrefixSize
	}

	// Get old root's extra data and reset it to nil in old root
	extraData := m.root.RemoveExtraData()

	// Save root node id
	rootID := m.root.SlabID()

	// Assign a new slab ID to old root before splitting it.
	sID, err := m.Storage.GenerateSlabID(m.Address())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", m.Address()))
	}

	oldRoot := m.root
	oldRoot.SetSlabID(sID)

	// Split old root
	leftSlab, rightSlab, err := oldRoot.Split(m.Storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Split().
		return err
	}

	left := leftSlab.(MapSlab)
	right := rightSlab.(MapSlab)

	// Create new MapMetaDataSlab with the old root's slab ID
	newRoot := &MapMetaDataSlab{
		header: MapSlabHeader{
			slabID:   rootID,
			size:     mapMetaDataSlabPrefixSize + mapSlabHeaderSize*2,
			firstKey: left.Header().firstKey,
		},
		childrenHeaders: []MapSlabHeader{left.Header(), right.Header()},
		extraData:       extraData,
	}

	m.root = newRoot

	err = storeSlab(m.Storage, left)
	if err != nil {
		return err
	}

	err = storeSlab(m.Storage, right)
	if err != nil {
		return err
	}

	return storeSlab(m.Storage, m.root)
}

func (m *OrderedMap) promoteChildAsNewRoot(childID SlabID) error {

	child, err := getMapSlab(m.Storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return err
	}

	if child.IsData() {
		// Adjust data slab size before promoting non-root data slab to root
		dataSlab := child.(*MapDataSlab)
		dataSlab.header.size = dataSlab.header.size - mapDataSlabPrefixSize + mapRootDataSlabPrefixSize
	}

	extraData := m.root.RemoveExtraData()

	rootID := m.root.SlabID()

	m.root = child

	m.root.SetSlabID(rootID)

	m.root.SetExtraData(extraData)

	err = storeSlab(m.Storage, m.root)
	if err != nil {
		return err
	}

	err = m.Storage.Remove(childID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
	}
	return nil
}

// mutableValue operations (parent updater callback, mutableElementIndex, etc)

func (m *OrderedMap) Inlined() bool {
	return m.root.Inlined()
}

func (m *OrderedMap) Inlinable(maxInlineSize uint32) bool {
	return m.root.Inlinable(maxInlineSize)
}

func (m *OrderedMap) setParentUpdater(f parentUpdater) {
	m.parentUpdater = f
}

// setCallbackWithChild sets up callback function with child value (child)
// so parent map (m) can be notified when child value is modified.
func (m *OrderedMap) setCallbackWithChild(
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	child Value,
	maxInlineSize uint32,
) {
	// Unwrap child value if needed (e.g. interpreter.SomeValue)
	unwrappedChild, wrapperSize := unwrapValue(child)

	c, ok := unwrappedChild.(mutableValueNotifier)
	if !ok {
		return
	}

	if maxInlineSize < wrapperSize {
		maxInlineSize = 0
	} else {
		maxInlineSize -= wrapperSize
	}

	vid := c.ValueID()

	c.setParentUpdater(func() (found bool, err error) {

		// Avoid unnecessary write operation on parent container.
		// Child value was stored as SlabIDStorable (not inlined) in parent container,
		// and continues to be stored as SlabIDStorable (still not inlinable),
		// so no update to parent container is needed.
		if !c.Inlined() && !c.Inlinable(maxInlineSize) {
			return true, nil
		}

		// Retrieve element value under the same key and
		// verify retrieved value is this child (c).
		_, valueStorable, err := m.get(comparator, hip, key)
		if err != nil {
			var knf *KeyNotFoundError
			if errors.As(err, &knf) {
				return false, nil
			}
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.Get().
			return false, err
		}

		valueStorable = unwrapStorable(valueStorable)

		// Verify retrieved element value is either SlabIDStorable or Slab, with identical value ID.
		switch valueStorable := valueStorable.(type) {
		case SlabIDStorable:
			sid := SlabID(valueStorable)
			if !vid.equal(sid) {
				return false, nil
			}

		case Slab:
			sid := valueStorable.SlabID()
			if !vid.equal(sid) {
				return false, nil
			}

		default:
			return false, nil
		}

		// NOTE: Must reset child using original child (not unwrapped child)

		// Set child value with parent map using same key.
		// Set() calls child.Storable() which returns inlined or not-inlined child storable.
		existingValueStorable, err := m.set(comparator, hip, key, child)
		if err != nil {
			return false, err
		}

		// Verify overwritten storable has identical value ID.

		existingValueStorable = unwrapStorable(existingValueStorable)

		switch existingValueStorable := existingValueStorable.(type) {
		case SlabIDStorable:
			sid := SlabID(existingValueStorable)
			if !vid.equal(sid) {
				return false, NewFatalError(
					fmt.Errorf(
						"failed to reset child value in parent updater callback: overwritten SlabIDStorable %s != value ID %s",
						sid,
						vid))
			}

		case Slab:
			sid := existingValueStorable.SlabID()
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

// notifyParentIfNeeded calls parent updater if this map (m) is a child
// element in another container.
func (m *OrderedMap) notifyParentIfNeeded() error {
	if m.parentUpdater == nil {
		return nil
	}

	// If parentUpdater() doesn't find child map (m), then no-op on parent container
	// and unset parentUpdater callback in child map.  This can happen when child
	// map is an outdated reference (removed or overwritten in parent container).
	found, err := m.parentUpdater()
	if err != nil {
		return err
	}
	if !found {
		m.parentUpdater = nil
	}
	return nil
}

// Value operations

// Storable returns OrderedMap m as either:
// - SlabIDStorable, or
// - inlined data slab storable
func (m *OrderedMap) Storable(_ SlabStorage, _ Address, maxInlineSize uint32) (Storable, error) {

	inlined := m.root.Inlined()
	inlinable := m.root.Inlinable(maxInlineSize)

	switch {

	case inlinable && inlined:
		// Root slab is inlinable and was inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return m.root, nil

	case !inlinable && !inlined:
		// Root slab is not inlinable and was not inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return SlabIDStorable(m.SlabID()), nil

	case inlinable && !inlined:
		// Root slab is inlinable and was NOT inlined.

		// Inline root data slab.
		err := m.root.Inline(m.Storage)
		if err != nil {
			return nil, err
		}

		return m.root, nil

	case !inlinable && inlined:
		// Root slab is NOT inlinable and was inlined.

		// Uninline root slab.
		err := m.root.Uninline(m.Storage)
		if err != nil {
			return nil, err
		}

		return SlabIDStorable(m.SlabID()), nil

	default:
		panic(NewUnreachableError())
	}
}

// Iterators

// Iterator returns mutable iterator for map elements.
// Mutable iterator handles:
// - indirect element mutation, such as modifying nested container
// - direct element mutation, such as overwriting existing element with new element
// Mutable iterator doesn't handle:
// - inserting new elements into the map
// - removing existing elements from the map
// NOTE: Use readonly iterator if mutation is not needed for better performance.
func (m *OrderedMap) Iterator(comparator ValueComparator, hip HashInputProvider) (MapIterator, error) {
	if m.Count() == 0 {
		return emptyMutableMapIterator, nil
	}

	keyStorable, err := firstKeyInMapSlab(m.Storage, m.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstKeyInMapSlab().
		return nil, err
	}

	if keyStorable == nil {
		// This should never happen because m.Count() > 0.
		return nil, NewSlabDataErrorf("failed to find first key in map while map count > 0")
	}

	key, err := keyStorable.StoredValue(m.Storage)
	if err != nil {
		return nil, err
	}

	return &mutableMapIterator{
		m:          m,
		comparator: comparator,
		hip:        hip,
		nextKey:    key,
	}, nil
}

// ReadOnlyIterator returns readonly iterator for map elements.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use ReadOnlyIteratorWithMutationCallback().
func (m *OrderedMap) ReadOnlyIterator() (MapIterator, error) {
	return m.ReadOnlyIteratorWithMutationCallback(nil, nil)
}

// ReadOnlyIteratorWithMutationCallback returns readonly iterator for map elements.
// keyMutatinCallback and valueMutationCallback are useful for logging, etc. with
// more context when mutation occurs.  Mutation handling here is the same with or
// without these callbacks.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback and valueMutationCallback are called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use ReadOnlyIterator().
func (m *OrderedMap) ReadOnlyIteratorWithMutationCallback(
	keyMutatinCallback ReadOnlyMapIteratorMutationCallback,
	valueMutationCallback ReadOnlyMapIteratorMutationCallback,
) (MapIterator, error) {
	if m.Count() == 0 {
		return emptyReadOnlyMapIterator, nil
	}

	dataSlab, err := firstMapDataSlab(m.Storage, m.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstMapDataSlab().
		return nil, err
	}

	if keyMutatinCallback == nil {
		keyMutatinCallback = defaultReadOnlyMapIteratorMutatinCallback
	}

	if valueMutationCallback == nil {
		valueMutationCallback = defaultReadOnlyMapIteratorMutatinCallback
	}

	return &readOnlyMapIterator{
		m:              m,
		nextDataSlabID: dataSlab.next,
		elemIterator: &mapElementIterator{
			storage:  m.Storage,
			elements: dataSlab.elements,
		},
		keyMutationCallback:   keyMutatinCallback,
		valueMutationCallback: valueMutationCallback,
	}, nil
}

// ReadOnlyLoadedValueIterator returns iterator to iterate loaded map elements.
func (m *OrderedMap) ReadOnlyLoadedValueIterator() (*MapLoadedValueIterator, error) {
	switch slab := m.root.(type) {

	case *MapDataSlab:
		// Create a data iterator from root slab.
		dataIterator := &mapLoadedElementIterator{
			storage:  m.Storage,
			elements: slab.elements,
		}

		// Create iterator with data iterator (no parents).
		iterator := &MapLoadedValueIterator{
			storage:      m.Storage,
			dataIterator: dataIterator,
		}

		return iterator, nil

	case *MapMetaDataSlab:
		// Create a slab iterator from root slab.
		slabIterator := &mapLoadedSlabIterator{
			storage: m.Storage,
			slab:    slab,
		}

		// Create iterator with parent (data iterater is uninitialized).
		iterator := &MapLoadedValueIterator{
			storage: m.Storage,
			parents: []*mapLoadedSlabIterator{slabIterator},
		}

		return iterator, nil

	default:
		return nil, NewSlabDataErrorf("slab %s isn't MapSlab", slab.SlabID())
	}
}

// Iterate functions with callbacks

func (m *OrderedMap) Iterate(comparator ValueComparator, hip HashInputProvider, fn MapEntryIterationFunc) error {
	iterator, err := m.Iterator(comparator, hip)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}
	return iterateMap(iterator, fn)
}

// IterateReadOnly iterates readonly map elements.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyWithMutationCallback().
func (m *OrderedMap) IterateReadOnly(
	fn MapEntryIterationFunc,
) error {
	return m.IterateReadOnlyWithMutationCallback(fn, nil, nil)
}

// IterateReadOnlyWithMutationCallback iterates readonly map elements.
// keyMutatinCallback and valueMutationCallback are useful for logging, etc. with
// more context when mutation occurs.  Mutation handling here is the same with or
// without these callbacks.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback/valueMutationCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnly().
func (m *OrderedMap) IterateReadOnlyWithMutationCallback(
	fn MapEntryIterationFunc,
	keyMutatinCallback ReadOnlyMapIteratorMutationCallback,
	valueMutationCallback ReadOnlyMapIteratorMutationCallback,
) error {
	iterator, err := m.ReadOnlyIteratorWithMutationCallback(keyMutatinCallback, valueMutationCallback)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.ReadOnlyIterator().
		return err
	}
	return iterateMap(iterator, fn)
}

func (m *OrderedMap) IterateKeys(comparator ValueComparator, hip HashInputProvider, fn MapElementIterationFunc) error {
	iterator, err := m.Iterator(comparator, hip)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}
	return iterateMapKeys(iterator, fn)
}

// IterateReadOnlyKeys iterates readonly map keys.
// If keys are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of key containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyKeysWithMutationCallback().
func (m *OrderedMap) IterateReadOnlyKeys(
	fn MapElementIterationFunc,
) error {
	return m.IterateReadOnlyKeysWithMutationCallback(fn, nil)
}

// IterateReadOnlyKeysWithMutationCallback iterates readonly map keys.
// keyMutatinCallback is useful for logging, etc. with more context
// when mutation occurs.  Mutation handling here is the same with or
// without this callback.
// If keys are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of key containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnlyKeys().
func (m *OrderedMap) IterateReadOnlyKeysWithMutationCallback(
	fn MapElementIterationFunc,
	keyMutatinCallback ReadOnlyMapIteratorMutationCallback,
) error {
	iterator, err := m.ReadOnlyIteratorWithMutationCallback(keyMutatinCallback, nil)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.ReadOnlyIterator().
		return err
	}
	return iterateMapKeys(iterator, fn)
}

func (m *OrderedMap) IterateValues(comparator ValueComparator, hip HashInputProvider, fn MapElementIterationFunc) error {
	iterator, err := m.Iterator(comparator, hip)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}
	return iterateMapValues(iterator, fn)
}

// IterateReadOnlyValues iterates readonly map values.
// If values are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyValuesWithMutationCallback().
func (m *OrderedMap) IterateReadOnlyValues(
	fn MapElementIterationFunc,
) error {
	return m.IterateReadOnlyValuesWithMutationCallback(fn, nil)
}

// IterateReadOnlyValuesWithMutationCallback iterates readonly map values.
// valueMutationCallback is useful for logging, etc. with more context
// when mutation occurs.  Mutation handling here is the same with or
// without this callback.
// If values are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnlyValues().
func (m *OrderedMap) IterateReadOnlyValuesWithMutationCallback(
	fn MapElementIterationFunc,
	valueMutationCallback ReadOnlyMapIteratorMutationCallback,
) error {
	iterator, err := m.ReadOnlyIteratorWithMutationCallback(nil, valueMutationCallback)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.ReadOnlyIterator().
		return err
	}
	return iterateMapValues(iterator, fn)
}

// IterateReadOnlyLoadedValues iterates loaded map values.
func (m *OrderedMap) IterateReadOnlyLoadedValues(fn MapEntryIterationFunc) error {
	iterator, err := m.ReadOnlyLoadedValueIterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.LoadedValueIterator().
		return err
	}

	var key, value Value
	for {
		key, value, err = iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapLoadedValueIterator.Next().
			return err
		}
		if key == nil {
			return nil
		}
		resume, err := fn(key, value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapEntryIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}

// Other operations

func (m *OrderedMap) Seed() uint64 {
	return m.root.ExtraData().Seed
}

func (m *OrderedMap) Count() uint64 {
	return m.root.ExtraData().Count
}

func (m *OrderedMap) Address() Address {
	return m.root.SlabID().address
}

func (m *OrderedMap) Type() TypeInfo {
	if extraData := m.root.ExtraData(); extraData != nil {
		return extraData.TypeInfo
	}
	return nil
}

func (m *OrderedMap) SetType(typeInfo TypeInfo) error {
	extraData := m.root.ExtraData()
	extraData.TypeInfo = typeInfo

	m.root.SetExtraData(extraData)

	if m.Inlined() {
		// Map is inlined.

		// Notify parent container so parent slab is saved in storage with updated TypeInfo of inlined array.
		return m.notifyParentIfNeeded()
	}

	// Map is standalone.

	// Store modified root slab in storage since typeInfo is part of extraData stored in root slab.
	return storeSlab(m.Storage, m.root)
}

func (m *OrderedMap) String() string {
	iterator, err := m.ReadOnlyIterator()
	if err != nil {
		return err.Error()
	}

	var elemsStr []string
	for {
		k, v, err := iterator.Next()
		if err != nil {
			return err.Error()
		}
		if k == nil {
			break
		}
		elemsStr = append(elemsStr, fmt.Sprintf("%s:%s", k, v))
	}

	return fmt.Sprintf("[%s]", strings.Join(elemsStr, " "))
}

func (m *MapExtraData) incrementCount() {
	m.Count++
}

func (m *MapExtraData) decrementCount() {
	m.Count--
}
func (m *OrderedMap) rootSlab() MapSlab {
	return m.root
}

func (m *OrderedMap) getDigesterBuilder() DigesterBuilder {
	return m.digesterBuilder
}

func (m *OrderedMap) SlabID() SlabID {
	if m.root.Inlined() {
		return SlabIDUndefined
	}
	return m.root.SlabID()
}

func (m *OrderedMap) ValueID() ValueID {
	return slabIDToValueID(m.root.SlabID())
}
