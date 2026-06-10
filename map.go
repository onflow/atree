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
//
// Multiple *OrderedMap Go instances can exist for the same logical container;
// they share an *orderedMapState via the SlabStorage-backed registry,
// so structural mutations are observed by all siblings.
// See map_state.go for rationale.
type OrderedMap struct {
	Storage         SlabStorage
	digesterBuilder DigesterBuilder

	// state holds the mutable per-logical-container state (root pointer).
	// Shared across siblings.
	state *orderedMapState

	// parentUpdater is per-instance,
	// see Array.parentUpdater for rationale.
	parentUpdater parentUpdater

	// parentUpdaterIsReadOnlyMutationCallback indicates parentUpdater is a
	// read-only iterator's trap callback rather than a real parent-notification callback.
	// See Array.parentUpdaterIsReadOnlyMutationCallback for rationale.
	parentUpdaterIsReadOnlyMutationCallback bool
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

	state := newOrderedMapState(root)
	storage.SetOrderedMapState(sID, state)

	return &OrderedMap{
		Storage:         storage,
		state:           state,
		digesterBuilder: digestBuilder,
	}, nil
}

func NewMapWithRootID(storage SlabStorage, rootID SlabID, digestBuilder DigesterBuilder) (*OrderedMap, error) {
	if rootID == SlabIDUndefined {
		return nil, NewSlabIDErrorf("cannot create OrderedMap from undefined slab ID")
	}

	// If another *OrderedMap instance for this container already exists,
	// reuse its shared state so structural changes propagate.
	state := storage.OrderedMapState(rootID)

	// A registered state can outlive its container:
	// storage.Remove is called both when a container is inlined (still alive)
	// and when it is destroyed,
	// and the registry deliberately survives Remove for the inline case.
	// A non-inlined root must still have its slab in storage —
	// if it doesn't, the container was destroyed,
	// and returning the leftover state would resurrect it as a zombie.
	// An inlined root legitimately has no standalone slab, so it is not checked
	// (a container destroyed WHILE inlined never had a slab to remove,
	// so it cannot be detected here; its root slab ID is not referenced
	// by any remaining storable, so nothing should dereference it).
	if state != nil && !state.root.Inlined() {
		_, found, err := storage.Retrieve(rootID)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", rootID))
		}
		if !found {
			storage.RemoveStateForSlab(rootID)
			return nil, NewSlabNotFoundErrorf(rootID, "map slab not found")
		}
	}

	if state == nil {
		root, err := getMapSlab(storage, rootID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return nil, err
		}

		state = newOrderedMapState(root)
		storage.SetOrderedMapState(rootID, state)
	}

	// Re-seed the caller's digester from the canonical extra data so
	// hkeys match the canonical map.
	if extraData := state.root.ExtraData(); extraData != nil {
		digestBuilder.SetSeed(extraData.Seed, typicalRandomConstant)
	}

	return &OrderedMap{
		Storage:         storage,
		state:           state,
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
			// This is safe from overflow because elements.size includes
			// prevElemSize, and slab size is bounded by maxThreshold.
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
		// This addition is safe from overflow because slab size is checked
		// against targetThreshold/maxThreshold before each append,
		// and a new data slab is created when the threshold is reached.
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

	state := newOrderedMapState(root)
	storage.SetOrderedMapState(root.SlabID(), state)

	return &OrderedMap{
		Storage:         storage,
		state:           state,
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

		// This addition is safe from overflow because metadata slab size
		// is checked against maxThreshold and is split if it exceeds the limit.
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
	return m.state.root.Get(m.Storage, keyDigest, level, hkey, comparator, key)
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

	keyStorable, valueStorable, nextKeyStorable, err := m.state.root.getElementAndNextKey(m.Storage, keyDigest, level, hkey, comparator, key)
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

	_, _, nextKeyStorable, err := m.state.root.getElementAndNextKey(m.Storage, keyDigest, level, hkey, comparator, key)
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

	keyStorable, existingMapValueStorable, err := m.state.root.Set(m.Storage, m.digesterBuilder, keyDigest, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, err
	}

	if existingMapValueStorable == nil {
		m.state.root.ExtraData().incrementCount()
	}

	if !m.state.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := m.state.root.(*MapMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err := m.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by OrderedMap.promoteChildAsNewRoot().
				return nil, err
			}
		}
	}

	if m.state.root.IsFull() {
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

	k, v, err := m.state.root.Remove(m.Storage, keyDigest, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Remove().
		return nil, nil, err
	}

	m.state.root.ExtraData().decrementCount()

	if !m.state.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := m.state.root.(*MapMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err := m.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by OrderedMap.promoteChildAsNewRoot().
				return nil, nil, err
			}
		}
	}

	if m.state.root.IsFull() {
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

	err := m.state.root.PopIterate(m.Storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.PopIterate().
		return err
	}

	rootID := m.state.root.SlabID()

	// Set map count to 0 in extraData
	extraData := m.state.root.ExtraData()
	extraData.Count = 0

	inlined := m.state.root.Inlined()

	prefixSize := uint32(mapRootDataSlabPrefixSize)
	if inlined {
		prefixSize = uint32(inlinedMapDataSlabPrefixSize)
	}

	// Bump the old root's mutation counter before swapping m.root
	// so that sibling wrappers whose .root still points to this orphaned slab
	// can detect the root swap.
	// See MapSlab.MutationCount.
	m.state.root.BumpMutationCount()

	// Set root to empty data slab
	m.state.root = &MapDataSlab{
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
		err = storeSlab(m.Storage, m.state.root)
		if err != nil {
			return err
		}
	}

	return nil
}

// Slab operations (split root, promote child slab to root)

func (m *OrderedMap) splitRoot() error {

	if m.state.root.IsData() {
		// Adjust root data slab size before splitting
		dataSlab := m.state.root.(*MapDataSlab)
		dataSlab.header.size = dataSlab.header.size - mapRootDataSlabPrefixSize + mapDataSlabPrefixSize
	}

	// Get old root's extra data and reset it to nil in old root
	extraData := m.state.root.RemoveExtraData()

	// Save root node id
	rootID := m.state.root.SlabID()

	// Assign a new slab ID to old root before splitting it.
	sID, err := m.Storage.GenerateSlabID(m.Address())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", m.Address()))
	}

	oldRoot := m.state.root
	oldRoot.SetSlabID(sID)
	// Intentionally NOT calling oldRoot.BumpMutationCount() here:
	// MapSlab.Split reuses the receiver as the LEFT child
	// (see MapDataSlab.Split / MapMetaDataSlab.Split — both return (receiver, rightSlab)),
	// so the "old root" is not orphaned — it stays in the tree as the left child.
	// If a later promoteChildAsNewRoot picks this slab,
	// MutationCount() on the live root would falsely report staleness for the initiating wrapper.

	// Split old root
	leftSlab, rightSlab, err := oldRoot.Split(m.Storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Split().
		return err
	}

	// Invariant: MapSlab.Split must return the receiver as the LEFT child.
	// The decision to skip BumpMutationCount above relies on this —
	// if Split ever returns a fresh struct for the left side,
	// the receiver becomes orphaned and splitRoot must behave like
	// promoteChildAsNewRoot (bump).
	if Slab(oldRoot) != leftSlab {
		panic(NewUnreachableError())
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

	m.state.root = newRoot

	err = storeSlab(m.Storage, left)
	if err != nil {
		return err
	}

	err = storeSlab(m.Storage, right)
	if err != nil {
		return err
	}

	return storeSlab(m.Storage, m.state.root)
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

	extraData := m.state.root.RemoveExtraData()

	rootID := m.state.root.SlabID()

	// Bump the old root's mutation counter before swapping m.root
	// so that sibling wrappers whose .root still points to this orphaned slab
	// can detect the root swap.
	// Promote does not perturb the orphaned old root's SlabID,
	// so this counter is the only signal sibling wrappers have.
	// See MapSlab.MutationCount.
	m.state.root.BumpMutationCount()

	m.state.root = child

	m.state.root.SetSlabID(rootID)

	m.state.root.SetExtraData(extraData)

	err = storeSlab(m.Storage, m.state.root)
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
	return m.state.root.Inlined()
}

func (m *OrderedMap) Inlinable(maxInlineSize uint32) bool {
	return m.state.root.Inlinable(maxInlineSize)
}

func (m *OrderedMap) setParentUpdater(f parentUpdater) {
	m.parentUpdater = f
	m.parentUpdaterIsReadOnlyMutationCallback = false
}

// setReadOnlyMutationCallback installs a trap callback that fires
// when the *OrderedMap is mutated through this instance,
// indicating the instance was loaded via a read-only iterator.
func (m *OrderedMap) setReadOnlyMutationCallback(f parentUpdater) {
	m.parentUpdater = f
	m.parentUpdaterIsReadOnlyMutationCallback = true
}

// HasParentUpdater reports whether a parent-notification (or read-only trap) callback is installed.
// Use HasReadOnlyMutationCallback to distinguish the two cases.
func (m *OrderedMap) HasParentUpdater() bool {
	return m.parentUpdater != nil
}

// HasReadOnlyMutationCallback reports whether the installed parentUpdater
// is a trap callback set by a read-only iterator
// (as opposed to a real parent-notification callback).
// Callers that want to share or canonicalize the *OrderedMap should consult this
// to avoid caching a trap-bearing instance.
func (m *OrderedMap) HasReadOnlyMutationCallback() bool {
	return m.parentUpdaterIsReadOnlyMutationCallback
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
		m.parentUpdaterIsReadOnlyMutationCallback = false
	}
	return nil
}

// Value operations

// Storable returns OrderedMap m as either:
// - SlabIDStorable, or
// - inlined data slab storable
func (m *OrderedMap) Storable(_ SlabStorage, _ Address, maxInlineSize uint32) (Storable, error) {

	inlined := m.state.root.Inlined()
	inlinable := m.state.root.Inlinable(maxInlineSize)

	switch {

	case inlinable && inlined:
		// Root slab is inlinable and was inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return m.state.root, nil

	case !inlinable && !inlined:
		// Root slab is not inlinable and was not inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return SlabIDStorable(m.SlabID()), nil

	case inlinable && !inlined:
		// Root slab is inlinable and was NOT inlined.

		// Inline root data slab.
		err := m.state.root.Inline(m.Storage)
		if err != nil {
			return nil, err
		}

		return m.state.root, nil

	case !inlinable && inlined:
		// Root slab is NOT inlinable and was inlined.

		// Uninline root slab.
		err := m.state.root.Uninline(m.Storage)
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

	keyStorable, err := firstKeyInMapSlab(m.Storage, m.state.root)
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

	dataSlab, err := firstMapDataSlab(m.Storage, m.state.root)
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
	switch slab := m.state.root.(type) {

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
	return m.state.root.ExtraData().Seed
}

func (m *OrderedMap) Count() uint64 {
	return m.state.root.ExtraData().Count
}

func (m *OrderedMap) Address() Address {
	return m.state.root.SlabID().address
}

func (m *OrderedMap) Type() TypeInfo {
	if extraData := m.state.root.ExtraData(); extraData != nil {
		return extraData.TypeInfo
	}
	return nil
}

func (m *OrderedMap) SetType(typeInfo TypeInfo) error {
	extraData := m.state.root.ExtraData()
	extraData.TypeInfo = typeInfo

	m.state.root.SetExtraData(extraData)

	if m.Inlined() {
		// Map is inlined.

		// Notify parent container so parent slab is saved in storage with updated TypeInfo of inlined array.
		return m.notifyParentIfNeeded()
	}

	// Map is standalone.

	// Store modified root slab in storage since typeInfo is part of extraData stored in root slab.
	return storeSlab(m.Storage, m.state.root)
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
	return m.state.root
}

func (m *OrderedMap) getDigesterBuilder() DigesterBuilder {
	return m.digesterBuilder
}

func (m *OrderedMap) SlabID() SlabID {
	if m.state.root.Inlined() {
		return SlabIDUndefined
	}
	return m.state.root.SlabID()
}

func (m *OrderedMap) ValueID() ValueID {
	return slabIDToValueID(m.state.root.SlabID())
}

// MutationCount returns the root slab's mutation counter.
// It is bumped on root replacement,
// and not on element-level or non-root structural changes.
// Callers cache the value to detect staleness later.
// See MapSlab.MutationCount.
func (m *OrderedMap) MutationCount() uint64 {
	return m.state.root.MutationCount()
}

// CanCopyNonRefSimple returns true if the map can be copied
// as a container with only non-reference and simple storables.
func (m *OrderedMap) CanCopyNonRefSimple() bool {
	return m.state.root.canCopyWithoutSlabID()
}

// CopyNonRefSimple returns a copy of the map that only
// contains non-reference and simple storables.
// NOTE: Please call CanCopyNonRefSimple() to confirm the copy operation
// is feasible for the map before calling CopyNonRefSimple().
func (m *OrderedMap) CopyNonRefSimple(address Address, digestBuilder DigesterBuilder) (*OrderedMap, error) {
	if !m.state.root.IsData() {
		return nil, newCopyMapErrorf("can't copy multi-slab map")
	}

	seed := m.state.root.ExtraData().Seed

	// Seed digester
	digestBuilder.SetSeed(seed, typicalRandomConstant)

	// Create root slab ID
	newID, err := m.Storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	copiedRoot, err := m.state.root.copyWithNewSlabID(newID)
	if err != nil {
		return nil, newCopyMapError(err)
	}

	err = storeSlab(m.Storage, copiedRoot)
	if err != nil {
		return nil, err
	}

	state := newOrderedMapState(copiedRoot)
	m.Storage.SetOrderedMapState(copiedRoot.SlabID(), state)

	return &OrderedMap{
		Storage:         m.Storage,
		digesterBuilder: digestBuilder,
		state:           state,
	}, nil
}

// IsWithinSingleSlab returns true if the map is stored in a single slab.
func (m *OrderedMap) IsWithinSingleSlab() bool {
	return m.state.root.IsData()
}
