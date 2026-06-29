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
	"fmt"
	"math"
	"strings"
)

const (
	// maxArrayElementCount is the max number of elements that can be stored in an array.
	// Currently, this limit is set to math.MaxUint32 (4,294,967,295 elements per array).
	// ArraySlabHeader.Count (uint32) is used to track element count in array slabs.
	// NOTE: MaxArrayElementCount is uint64 since we may want to increase the limit to be
	// greater than MaxUint32 in the future.
	maxArrayElementCount uint64 = math.MaxUint32
)

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
//
// Multiple *Array Go instances can exist for the same logical container;
// they all share the same *arrayState via the SlabStorage-backed registry,
// so structural mutations through any one of them are observed by all of them.
// See array_state.go for the rationale.
type Array struct {
	Storage SlabStorage

	// state holds the mutable per-logical-container state
	// (root pointer, mutableElementIndex).
	// Shared across siblings so structural changes propagate.
	state *arrayState

	// parentUpdater is the callback notifying the parent container
	// when this *Array instance triggers a mutation.
	// It is per-instance, not shared:
	// a Get-loaded instance has a real parent updater,
	// while a readonly-iterator-loaded instance has a trap callback
	// (see parentUpdaterIsReadOnlyMutationCallback).
	// A mutation through one instance must fire only its own callback.
	parentUpdater parentUpdater

	// parentUpdaterIsReadOnlyMutationCallback is true
	// when parentUpdater is a trap callback set by a read-only iterator
	// (rather than a real parent-notification callback set by setCallbackWithChild).
	// Callers that cache or alias this *Array
	// use this to avoid promoting a trap-bearing instance to a shared/canonical wrapper:
	// mutations through such a wrapper would trip the trap.
	parentUpdaterIsReadOnlyMutationCallback bool

	// loadedWithRootID is true when this wrapper was created by NewArrayWithRootID.
	// If such a wrapper later observes an inlined state,
	// it cannot safely mutate without a parentUpdater:
	// root-ID loading only proves access to a standalone root,
	// while an inlined value needs a parent write-back path.
	loadedWithRootID bool
}

var _ Value = &Array{}
var _ mutableValueNotifier = &Array{}

// Create, copy, and load array

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

	err = storeSlab(storage, root)
	if err != nil {
		return nil, err
	}

	state := newArrayState(root)
	storage.SetArrayState(sID, state)

	return &Array{
		Storage: storage,
		state:   state,
	}, nil
}

func NewArrayWithRootID(storage SlabStorage, rootID SlabID) (*Array, error) {
	if rootID == SlabIDUndefined {
		return nil, NewSlabIDErrorf("cannot create Array from undefined slab ID")
	}

	// If another *Array instance for this container already exists, reuse
	// its shared state so structural changes propagate.
	state := storage.ArrayState(rootID)

	// NewArrayWithRootID only loads standalone roots.
	// If the registry says this logical container is currently inlined,
	// rootID is no longer a live standalone slab ID.
	// Do not clear the state here:
	// the inlined container may still be alive inside its parent,
	// and dropping the registry would reintroduce sibling divergence.
	if state != nil && state.root.Inlined() {
		return nil, NewSlabNotFoundErrorf(rootID, "array slab is inlined")
	}

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
			return nil, NewSlabNotFoundErrorf(rootID, "array slab not found")
		}
	}

	if state == nil {
		root, err := getArraySlab(storage, rootID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return nil, err
		}

		extraData := root.ExtraData()
		if extraData == nil {
			return nil, NewNotValueError(rootID)
		}

		state = newArrayState(root)
		storage.SetArrayState(rootID, state)
	}

	return &Array{
		Storage:          storage,
		state:            state,
		loadedWithRootID: true,
	}, nil
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
		if dataSlab.header.size >= targetThreshold {

			// Generate storage id for next data slab
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
		// This addition is safe from overflow because slab size is
		// checked against targetThreshold before each batch append,
		// and a new data slab is created when the threshold is reached.
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
			err = storeSlab(storage, slab)
			if err != nil {
				return nil, err
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
	err = storeSlab(storage, root)
	if err != nil {
		return nil, err
	}

	state := newArrayState(root)
	storage.SetArrayState(root.SlabID(), state)

	return &Array{
		Storage: storage,
		state:   state,
	}, nil
}

// nextLevelArraySlabs returns next level meta data slabs from slabs.
// slabs must have at least 2 elements.  It is reused and returned as next level slabs.
// Caller is responsible for rebalance last slab and storing returned slabs in storage.
func nextLevelArraySlabs(storage SlabStorage, address Address, slabs []ArraySlab) ([]ArraySlab, error) {

	maxNumberOfHeadersInMetaSlab := (maxThreshold - arrayMetaDataSlabPrefixSize) / arraySlabHeaderSize

	nextLevelSlabsIndex := 0

	// Generate storage id
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

			// Generate storage id for next meta data slab
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

		// These additions are safe from overflow because metadata slab size
		// is checked against maxThreshold and is split if it exceeds the limit.
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

// Array operations (get, set, insert, remove, and pop iterate)

func (a *Array) Get(i uint64) (Value, error) {
	storable, err := a.state.root.Get(a.Storage, i)
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
	existingStorable, existingValueID, _, err = uninlineStorableIfNeeded(a.Storage, existingStorable)
	if err != nil {
		return nil, err
	}

	// Remove overwritten array/map's ValueID from mutableElementIndex if:
	// - new value isn't array/map, or
	// - new value is array/map with different value ID
	if existingValueID != emptyValueID {
		unwrappedValue, _ := unwrapValue(value)
		newValue, ok := unwrappedValue.(mutableValueNotifier)
		if !ok || existingValueID != newValue.ValueID() {
			delete(a.state.mutableElementIndex, existingValueID)
		}
	}

	return existingStorable, nil
}

func (a *Array) set(index uint64, value Value) (Storable, error) {
	err := a.ensureRootIDLoadedInlinedMutationAllowed()
	if err != nil {
		return nil, err
	}

	existingStorable, err := a.state.root.Set(a.Storage, a.Address(), index, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Set().
		return nil, err
	}

	if a.state.root.IsFull() {
		err = a.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by Array.splitRoot().
			return nil, err
		}
	}

	if !a.state.root.IsData() {
		root := a.state.root.(*ArrayMetaDataSlab)
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
	if a.Count() == maxArrayElementCount {
		// Don't insert new element if array already has max number of elements.
		return NewArrayElementCannotExceedMaxElementCountError(maxArrayElementCount)
	}

	err := a.ensureRootIDLoadedInlinedMutationAllowed()
	if err != nil {
		return err
	}

	err = a.state.root.Insert(a.Storage, a.Address(), index, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Insert().
		return err
	}

	if a.state.root.IsFull() {
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

	// If removed storable is an inlined slab, uninline the slab and store it in storage.
	// This is to prevent potential data loss because the overwritten inlined slab was not in
	// storage and any future changes to it would have been lost.
	removedStorable, removedValueID, _, err := uninlineStorableIfNeeded(a.Storage, storable)
	if err != nil {
		return nil, err
	}

	// Delete removed element ValueID from mutableElementIndex
	if removedValueID != emptyValueID {
		delete(a.state.mutableElementIndex, removedValueID)
	}

	return removedStorable, nil
}

func (a *Array) remove(index uint64) (Storable, error) {
	err := a.ensureRootIDLoadedInlinedMutationAllowed()
	if err != nil {
		return nil, err
	}

	storable, err := a.state.root.Remove(a.Storage, index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Remove().
		return nil, err
	}

	if !a.state.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := a.state.root.(*ArrayMetaDataSlab)
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

type ArrayPopIterationFunc func(Storable)

// PopIterate iterates and removes elements backward.
// Each element is passed to ArrayPopIterationFunc callback before removal.
func (a *Array) PopIterate(fn ArrayPopIterationFunc) error {

	err := a.ensureRootIDLoadedInlinedMutationAllowed()
	if err != nil {
		return err
	}

	err = a.state.root.PopIterate(a.Storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.PopIterate().
		return err
	}

	rootID := a.state.root.SlabID()

	extraData := a.state.root.ExtraData()

	inlined := a.state.root.Inlined()

	size := uint32(arrayRootDataSlabPrefixSize)
	if inlined {
		size = inlinedArrayDataSlabPrefixSize
	}

	// Bump the old root's mutation counter before swapping a.root
	// so that sibling wrappers whose .root still points to this orphaned slab
	// can detect the root swap. See ArraySlab.MutationCount.
	a.state.root.BumpMutationCount()

	// Set root to empty data slab
	a.state.root = &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: rootID,
			size:   size,
		},
		extraData: extraData,
		inlined:   inlined,
	}

	// Save root slab
	if !a.Inlined() {
		err = storeSlab(a.Storage, a.state.root)
		if err != nil {
			return err
		}
	}

	return nil
}

// Slab operations (split root, promote child slab to root)

func (a *Array) splitRoot() error {

	if a.state.root.IsData() {
		// Adjust root data slab size before splitting
		dataSlab := a.state.root.(*ArrayDataSlab)
		dataSlab.header.size = dataSlab.header.size - arrayRootDataSlabPrefixSize + arrayDataSlabPrefixSize
	}

	// Get old root's extra data and reset it to nil in old root
	extraData := a.state.root.RemoveExtraData()

	// Save root node id
	rootID := a.state.root.SlabID()

	// Assign a new slab ID to old root before splitting it.
	sID, err := a.Storage.GenerateSlabID(a.Address())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", a.Address()))
	}

	oldRoot := a.state.root
	oldRoot.SetSlabID(sID)
	// Intentionally NOT calling oldRoot.BumpMutationCount() here:
	// ArraySlab.Split reuses the receiver as the LEFT child
	// (see ArrayDataSlab.Split / ArrayMetaDataSlab.Split — both return (receiver, rightSlab)),
	// so the "old root" is not orphaned — it stays in the tree as the left child.
	// If a later promoteChildAsNewRoot picks this slab,
	// MutationCount() on the live root would falsely report staleness for the initiating wrapper.

	// Split old root
	leftSlab, rightSlab, err := oldRoot.Split(a.Storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Split().
		return err
	}

	// Invariant: ArraySlab.Split must return the receiver as the LEFT child.
	// The decision to skip BumpMutationCount above relies on this —
	// if Split ever returns a fresh struct for the left side,
	// the receiver becomes orphaned and splitRoot must behave like
	// promoteChildAsNewRoot (bump).
	if Slab(oldRoot) != leftSlab {
		panic(NewUnreachableError())
	}

	left := leftSlab.(ArraySlab)
	right := rightSlab.(ArraySlab)

	// Create new ArrayMetaDataSlab with the old root's slab ID
	// The count and size computations are safe from overflow because the
	// total count was the root's count before splitting, which already fit in uint32.
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

	a.state.root = newRoot

	err = storeSlab(a.Storage, left)
	if err != nil {
		return err
	}

	err = storeSlab(a.Storage, right)
	if err != nil {
		return err
	}

	return storeSlab(a.Storage, a.state.root)
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

	extraData := a.state.root.RemoveExtraData()

	rootID := a.state.root.SlabID()

	// Bump the old root's mutation counter before swapping a.root
	// so that sibling wrappers whose .root still points to this orphaned slab
	// can detect the root swap.
	// Promote does not perturb the orphaned old root's SlabID,
	// so this counter is the only signal sibling wrappers have.
	// See ArraySlab.MutationCount.
	a.state.root.BumpMutationCount()

	a.state.root = child

	a.state.root.SetSlabID(rootID)

	a.state.root.SetExtraData(extraData)

	err = storeSlab(a.Storage, a.state.root)
	if err != nil {
		return err
	}

	err = a.Storage.Remove(childID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
	}

	return nil
}

// mutableValue operations (parent updater callback, mutableElementIndex, etc)

// TODO: maybe optimize this
func (a *Array) incrementIndexFrom(index uint64) error {
	// Although range loop over Go map is not deterministic, it is OK
	// to use here because this operation is free of side-effect and
	// leads to the same results independent of map order.
	for id, i := range a.state.mutableElementIndex {
		if i >= index {
			if a.state.mutableElementIndex[id]+1 >= a.Count() {
				return NewFatalError(fmt.Errorf("failed to increment index of ValueID %s in array %s: new index exceeds array count", id, a.ValueID()))
			}
			a.state.mutableElementIndex[id]++
		}
	}
	return nil
}

// TODO: maybe optimize this
func (a *Array) decrementIndexFrom(index uint64) error {
	// Although range loop over Go map is not deterministic, it is OK
	// to use here because this operation is free of side-effect and
	// leads to the same results independent of map order.
	for id, i := range a.state.mutableElementIndex {
		if i > index {
			if a.state.mutableElementIndex[id] <= 0 {
				return NewFatalError(fmt.Errorf("failed to decrement index of ValueID %s in array %s: new index < 0", id, a.ValueID()))
			}
			a.state.mutableElementIndex[id]--
		}
	}
	return nil
}

func (a *Array) getIndexByValueID(id ValueID) (uint64, bool) {
	index, exist := a.state.mutableElementIndex[id]
	return index, exist
}

func (a *Array) setParentUpdater(f parentUpdater) {
	a.parentUpdater = f
	a.parentUpdaterIsReadOnlyMutationCallback = false
}

// setReadOnlyMutationCallback installs a trap callback that fires
// when the *Array is mutated through this instance,
// indicating the instance was loaded via a read-only iterator.
func (a *Array) setReadOnlyMutationCallback(f parentUpdater) {
	a.parentUpdater = f
	a.parentUpdaterIsReadOnlyMutationCallback = true
}

// HasParentUpdater reports whether a parent-notification (or read-only trap) callback is installed.
// Use HasReadOnlyMutationCallback to distinguish the two cases.
func (a *Array) HasParentUpdater() bool {
	return a.parentUpdater != nil
}

// HasReadOnlyMutationCallback reports whether the installed parentUpdater
// is a trap callback set by a read-only iterator
// (as opposed to a real parent-notification callback).
// Callers that want to share or canonicalize the *Array should consult this
// to avoid caching a trap-bearing instance.
func (a *Array) HasReadOnlyMutationCallback() bool {
	return a.parentUpdaterIsReadOnlyMutationCallback
}

// setCallbackWithChild sets up callback function with child value (child)
// so parent array (a) can be notified when child value is modified.
func (a *Array) setCallbackWithChild(i uint64, child Value, maxInlineSize uint32) {
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

	// mutableElementIndex is lazily initialized.
	if a.state.mutableElementIndex == nil {
		a.state.mutableElementIndex = make(map[ValueID]uint64)
	}

	// Index i will be updated with array operations, which affects element index.
	a.state.mutableElementIndex[vid] = i

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

		storable, err := a.state.root.Get(a.Storage, adjustedIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.Get().
			return false, err
		}

		storable = unwrapStorable(storable)

		// Verify retrieved element is either SlabIDStorable or Slab, with identical value ID.
		switch storable := storable.(type) {
		case SlabIDStorable:
			sid := SlabID(storable)
			if !vid.equal(sid) {
				return false, nil
			}

		case Slab:
			sid := storable.SlabID()
			if !vid.equal(sid) {
				return false, nil
			}

		default:
			return false, nil
		}

		// NOTE: Must reset child using original child (not unwrapped child)

		// Set child value with parent array using updated index.
		// Set() calls child.Storable() which returns inlined or not-inlined child storable.
		existingValueStorable, err := a.set(adjustedIndex, child)
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
		a.parentUpdaterIsReadOnlyMutationCallback = false
	}
	return nil
}

// ensureRootIDLoadedInlinedMutationAllowed rejects mutations through a wrapper
// that was created by NewArrayWithRootID but whose container has since been
// inlined into a parent, when this wrapper has no parentUpdater to write the
// change back. Such a mutation would only change in-memory state that can never
// be persisted into the parent, silently diverging memory from storage.
//
// INVARIANT: every method that mutates the array's elements, structure, or
// extra data (currently set, Insert, remove, PopIterate, and SetType) MUST call
// this and return early on error before touching a.state.root. The protection
// only holds if all mutation entry points are guarded; any new mutating method
// that skips this check silently reopens the divergence hole described above.
func (a *Array) ensureRootIDLoadedInlinedMutationAllowed() error {
	if !a.loadedWithRootID || !a.state.root.Inlined() || a.parentUpdater != nil {
		return nil
	}

	return NewFatalError(
		fmt.Errorf(
			"cannot mutate inlined array %s loaded by root ID without parent updater",
			a.ValueID(),
		),
	)
}

func (a *Array) Inlined() bool {
	return a.state.root.Inlined()
}

func (a *Array) Inlinable(maxInlineSize uint32) bool {
	return a.state.root.Inlinable(maxInlineSize)
}

func (a *Array) getMutableElementIndexCount() uint64 {
	return uint64(len(a.state.mutableElementIndex))
}

func (a *Array) getMutableElementIndex() map[ValueID]uint64 {
	return a.state.mutableElementIndex
}

// Value operations

// Storable returns array a as either:
// - SlabIDStorable, or
// - inlined data slab storable
func (a *Array) Storable(_ SlabStorage, _ Address, maxInlineSize uint32) (Storable, error) {

	inlined := a.state.root.Inlined()
	inlinable := a.state.root.Inlinable(maxInlineSize)

	switch {
	case inlinable && inlined:
		// Root slab is inlinable and was inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return a.state.root, nil

	case !inlinable && !inlined:
		// Root slab is not inlinable and was not inlined.
		// Return root slab ID as storable, no size adjustment and change to storage.
		return SlabIDStorable(a.SlabID()), nil

	case inlinable && !inlined:
		// Root slab is inlinable and was NOT inlined.

		// Inline root data slab.
		err := a.state.root.Inline(a.Storage)
		if err != nil {
			return nil, err
		}

		return a.state.root, nil

	case !inlinable && inlined:

		// Root slab is NOT inlinable and was previously inlined.

		// Uninline root slab.
		err := a.state.root.Uninline(a.Storage)
		if err != nil {
			return nil, err
		}

		return SlabIDStorable(a.SlabID()), nil

	default:
		panic(NewUnreachableError())
	}
}

// Iterators

// Iterator returns mutable iterator for array elements.
// Mutable iterator handles:
// - indirect element mutation, such as modifying nested container
// - direct element mutation, such as overwriting existing element with new element
// Mutable iterator doesn't handle:
// - inserting new elements into the array
// - removing existing elements from the array
// NOTE: Use readonly iterator if mutation is not needed for better performance.
func (a *Array) Iterator() (ArrayIterator, error) {
	if a.Count() == 0 {
		return emptyMutableArrayIterator, nil
	}

	return &mutableArrayIterator{
		array:     a,
		lastIndex: a.Count(),
	}, nil
}

// ReadOnlyIterator returns readonly iterator for array elements.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use ReadOnlyIteratorWithMutationCallback().
func (a *Array) ReadOnlyIterator() (ArrayIterator, error) {
	return a.ReadOnlyIteratorWithMutationCallback(nil)
}

// ReadOnlyIteratorWithMutationCallback returns readonly iterator for array elements.
// valueMutationCallback is useful for logging, etc. with more context when mutation
// occurs.  Mutation handling here is the same with or without callback.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - valueMutationCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use ReadOnlyIterator().
func (a *Array) ReadOnlyIteratorWithMutationCallback(
	valueMutationCallback ReadOnlyArrayIteratorMutationCallback,
) (ArrayIterator, error) {
	if a.Count() == 0 {
		return emptyReadOnlyArrayIterator, nil
	}

	slab, err := firstArrayDataSlab(a.Storage, a.state.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstArrayDataSlab().
		return nil, err
	}

	if valueMutationCallback == nil {
		valueMutationCallback = defaultReadOnlyArrayIteratorMutatinCallback
	}

	return &readOnlyArrayIterator{
		array:                 a,
		dataSlab:              slab,
		remainingCount:        a.Count(),
		valueMutationCallback: valueMutationCallback,
	}, nil
}

func (a *Array) RangeIterator(startIndex uint64, endIndex uint64) (ArrayIterator, error) {
	count := a.Count()

	if startIndex > count || endIndex > count {
		return nil, NewSliceOutOfBoundsError(startIndex, endIndex, 0, count)
	}

	if startIndex > endIndex {
		return nil, NewInvalidSliceIndexError(startIndex, endIndex)
	}

	if endIndex == startIndex {
		return emptyMutableArrayIterator, nil
	}

	return &mutableArrayIterator{
		array:     a,
		nextIndex: startIndex,
		lastIndex: endIndex,
	}, nil
}

// ReadOnlyRangeIterator iterates readonly array elements from
// specified startIndex to endIndex.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use ReadOnlyRangeIteratorWithMutationCallback().
func (a *Array) ReadOnlyRangeIterator(
	startIndex uint64,
	endIndex uint64,
) (ArrayIterator, error) {
	return a.ReadOnlyRangeIteratorWithMutationCallback(startIndex, endIndex, nil)
}

// ReadOnlyRangeIteratorWithMutationCallback iterates readonly array elements
// from specified startIndex to endIndex.
// valueMutationCallback is useful for logging, etc. with more context when
// mutation occurs.  Mutation handling here is the same with or without callback.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - valueMutationCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use ReadOnlyRangeIterator().
func (a *Array) ReadOnlyRangeIteratorWithMutationCallback(
	startIndex uint64,
	endIndex uint64,
	valueMutationCallback ReadOnlyArrayIteratorMutationCallback,
) (ArrayIterator, error) {
	count := a.Count()

	if startIndex > count || endIndex > count {
		return nil, NewSliceOutOfBoundsError(startIndex, endIndex, 0, count)
	}

	if startIndex > endIndex {
		return nil, NewInvalidSliceIndexError(startIndex, endIndex)
	}

	numberOfElements := endIndex - startIndex

	if numberOfElements == 0 {
		return emptyReadOnlyArrayIterator, nil
	}

	var dataSlab *ArrayDataSlab
	index := startIndex

	if a.state.root.IsData() {
		dataSlab = a.state.root.(*ArrayDataSlab)
	} else if startIndex == 0 {
		var err error
		dataSlab, err = firstArrayDataSlab(a.Storage, a.state.root)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by firstArrayDataSlab().
			return nil, err
		}
	} else {
		var err error
		// getArrayDataSlabWithIndex returns data slab containing element at startIndex,
		// getArrayDataSlabWithIndex also returns adjusted index for this element at returned data slab.
		// Adjusted index must be used as index when creating ArrayIterator.
		dataSlab, index, err = getArrayDataSlabWithIndex(a.Storage, a.state.root, startIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArrayDataSlabWithIndex().
			return nil, err
		}
	}

	if valueMutationCallback == nil {
		valueMutationCallback = defaultReadOnlyArrayIteratorMutatinCallback
	}

	return &readOnlyArrayIterator{
		array:                 a,
		dataSlab:              dataSlab,
		indexInDataSlab:       index,
		remainingCount:        numberOfElements,
		valueMutationCallback: valueMutationCallback,
	}, nil
}

// ReadOnlyLoadedValueIterator returns iterator to iterate loaded array elements.
func (a *Array) ReadOnlyLoadedValueIterator() (*ArrayLoadedValueIterator, error) {
	switch slab := a.state.root.(type) {

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

// Iterate functions with callback

func (a *Array) Iterate(fn ArrayIterationFunc) error {
	iterator, err := a.Iterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Array.Iterator().
		return err
	}
	return iterateArray(iterator, fn)
}

// IterateReadOnly iterates readonly array elements.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyWithMutationCallback().
func (a *Array) IterateReadOnly(fn ArrayIterationFunc) error {
	return a.IterateReadOnlyWithMutationCallback(fn, nil)
}

// IterateReadOnlyWithMutationCallback iterates readonly array elements.
// valueMutationCallback is useful for logging, etc. with more context
// when mutation occurs.  Mutation handling here is the same with or
// without this callback.
// If values are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - valueMutatinCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnly().
func (a *Array) IterateReadOnlyWithMutationCallback(
	fn ArrayIterationFunc,
	valueMutationCallback ReadOnlyArrayIteratorMutationCallback,
) error {
	iterator, err := a.ReadOnlyIteratorWithMutationCallback(valueMutationCallback)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Array.ReadOnlyIterator().
		return err
	}
	return iterateArray(iterator, fn)
}

func (a *Array) IterateRange(startIndex uint64, endIndex uint64, fn ArrayIterationFunc) error {
	iterator, err := a.RangeIterator(startIndex, endIndex)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Array.RangeIterator().
		return err
	}
	return iterateArray(iterator, fn)
}

// IterateReadOnlyRange iterates readonly array elements from specified startIndex to endIndex.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyRangeWithMutatinoCallback().
func (a *Array) IterateReadOnlyRange(
	startIndex uint64,
	endIndex uint64,
	fn ArrayIterationFunc,
) error {
	return a.IterateReadOnlyRangeWithMutationCallback(startIndex, endIndex, fn, nil)
}

// IterateReadOnlyRangeWithMutationCallback iterates readonly array elements
// from specified startIndex to endIndex.
// valueMutationCallback is useful for logging, etc. with more context
// when mutation occurs.  Mutation handling here is the same with or
// without this callback.
// If values are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - valueMutatinCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnlyRange().
func (a *Array) IterateReadOnlyRangeWithMutationCallback(
	startIndex uint64,
	endIndex uint64,
	fn ArrayIterationFunc,
	valueMutationCallback ReadOnlyArrayIteratorMutationCallback,
) error {
	iterator, err := a.ReadOnlyRangeIteratorWithMutationCallback(startIndex, endIndex, valueMutationCallback)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by Array.ReadOnlyRangeIterator().
		return err
	}
	return iterateArray(iterator, fn)
}

// IterateReadOnlyLoadedValues iterates loaded array values.
func (a *Array) IterateReadOnlyLoadedValues(fn ArrayIterationFunc) error {
	iterator, err := a.ReadOnlyLoadedValueIterator()
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

// Other operations

func (a *Array) rootSlab() ArraySlab {
	return a.state.root
}

func (a *Array) Address() Address {
	return a.state.root.SlabID().address
}

func (a *Array) Count() uint64 {
	return uint64(a.state.root.Header().count)
}

func (a *Array) SlabID() SlabID {
	if a.state.root.Inlined() {
		return SlabIDUndefined
	}
	return a.state.root.SlabID()
}

func (a *Array) ValueID() ValueID {
	return slabIDToValueID(a.state.root.SlabID())
}

// MutationCount returns the root slab's mutation counter.
// It is bumped on root replacement,
// and not on element-level or non-root structural changes.
// Callers cache the value to detect staleness later.
// See ArraySlab.MutationCount.
func (a *Array) MutationCount() uint64 {
	return a.state.root.MutationCount()
}

func (a *Array) Type() TypeInfo {
	if extraData := a.state.root.ExtraData(); extraData != nil {
		return extraData.TypeInfo
	}
	return nil
}

func (a *Array) SetType(typeInfo TypeInfo) error {
	err := a.ensureRootIDLoadedInlinedMutationAllowed()
	if err != nil {
		return err
	}

	extraData := a.state.root.ExtraData()
	extraData.TypeInfo = typeInfo

	a.state.root.SetExtraData(extraData)

	if a.Inlined() {
		// Array is inlined.

		// Notify parent container so parent slab is saved in storage with updated TypeInfo of inlined array.
		return a.notifyParentIfNeeded()
	}

	// Array is standalone.

	// Store modified root slab in storage since typeInfo is part of extraData stored in root slab.
	return storeSlab(a.Storage, a.state.root)
}

func (a *Array) String() string {
	iterator, err := a.ReadOnlyIterator()
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

// CanCopyNonRefSimple returns true if the array can be copied
// as a container with only non-reference and simple storables.
func (a *Array) CanCopyNonRefSimple() bool {
	return a.state.root.canCopyWithoutSlabID()
}

// CopyNonRefSimple returns a copy of the array that only
// contains non-reference and simple storables.
// NOTE: Please call CanCopyNonRefSimple() to confirm the copy operation
// is feasible for the array before calling CopyNonRefSimple().
func (a *Array) CopyNonRefSimple(address Address) (*Array, error) {
	if !a.state.root.IsData() {
		return nil, newCopyArrayErrorf("can't copy multi-slab array")
	}

	newID, err := a.Storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	copiedRootSlab, err := a.state.root.copyWithNewSlabID(newID)
	if err != nil {
		return nil, newCopyArrayError(err)
	}

	err = storeSlab(a.Storage, copiedRootSlab)
	if err != nil {
		return nil, err
	}

	state := newArrayState(copiedRootSlab)
	a.Storage.SetArrayState(copiedRootSlab.SlabID(), state)

	return &Array{
		Storage: a.Storage,
		state:   state,
	}, nil
}

// IsWithinSingleSlab returns true if the array is stored in a single slab.
func (a *Array) IsWithinSingleSlab() bool {
	return a.state.root.IsData()
}
