/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2021-2022 Dapper Labs, Inc.
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
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

const LedgerBaseStorageSlabPrefix = "$"

type (
	Address      [8]byte
	StorageIndex [8]byte

	StorageID struct {
		Address Address
		Index   StorageIndex
	}
)

var (
	AddressUndefined      = Address{}
	StorageIndexUndefined = StorageIndex{}
	StorageIDUndefined    = StorageID{}
)

// Next returns new StorageIndex with index+1 value.
// The caller is responsible for preventing overflow
// by checking if the index value is valid before
// calling this function.
func (index StorageIndex) Next() StorageIndex {
	i := binary.BigEndian.Uint64(index[:])

	var next StorageIndex
	binary.BigEndian.PutUint64(next[:], i+1)

	return next
}

func NewStorageID(address Address, index StorageIndex) StorageID {
	return StorageID{address, index}
}

func NewStorageIDFromRawBytes(b []byte) (StorageID, error) {
	if len(b) < storageIDSize {
		return StorageID{}, NewStorageIDErrorf("incorrect storage id buffer length %d", len(b))
	}

	var address Address
	copy(address[:], b)

	var index StorageIndex
	copy(index[:], b[8:])

	return StorageID{address, index}, nil
}

func (id StorageID) ToRawBytes(b []byte) (int, error) {
	if len(b) < storageIDSize {
		return 0, NewStorageIDErrorf("incorrect storage id buffer length %d", len(b))
	}
	copy(b, id.Address[:])
	copy(b[8:], id.Index[:])
	return storageIDSize, nil
}

func (id StorageID) String() string {
	return fmt.Sprintf(
		"0x%x.%d",
		binary.BigEndian.Uint64(id.Address[:]),
		binary.BigEndian.Uint64(id.Index[:]),
	)
}

func (id StorageID) AddressAsUint64() uint64 {
	return binary.BigEndian.Uint64(id.Address[:])
}

func (id StorageID) IndexAsUint64() uint64 {
	return binary.BigEndian.Uint64(id.Index[:])
}

func (id StorageID) Valid() error {
	if id == StorageIDUndefined {
		return NewStorageIDError("undefined storage id")
	}
	if id.Index == StorageIndexUndefined {
		return NewStorageIDError("undefined storage index")
	}
	return nil
}

func (id StorageID) Compare(other StorageID) int {
	result := bytes.Compare(id.Address[:], other.Address[:])
	if result == 0 {
		return bytes.Compare(id.Index[:], other.Index[:])
	}
	return result
}

type BaseStorageUsageReporter interface {
	BytesRetrieved() int
	BytesStored() int
	SegmentsReturned() int
	SegmentsUpdated() int
	SegmentsTouched() int
	ResetReporter()
}

type BaseStorage interface {
	Store(StorageID, []byte) error
	Retrieve(StorageID) ([]byte, bool, error)
	Remove(StorageID) error
	GenerateStorageID(Address) (StorageID, error)
	SegmentCounts() int // number of segments stored in the storage
	Size() int          // total byte size stored
	BaseStorageUsageReporter
}

type Ledger interface {
	// GetValue gets a value for the given key in the storage, owned by the given account.
	GetValue(owner, key []byte) (value []byte, err error)
	// SetValue sets a value for the given key in the storage, owned by the given account.
	SetValue(owner, key, value []byte) (err error)
	// ValueExists returns true if the given key exists in the storage, owned by the given account.
	ValueExists(owner, key []byte) (exists bool, err error)
	// AllocateStorageIndex allocates a new storage index under the given account.
	AllocateStorageIndex(owner []byte) (StorageIndex, error)
}

type LedgerBaseStorage struct {
	ledger         Ledger
	bytesRetrieved int
	bytesStored    int
}

var _ BaseStorage = &LedgerBaseStorage{}

func NewLedgerBaseStorage(ledger Ledger) *LedgerBaseStorage {
	return &LedgerBaseStorage{
		ledger:         ledger,
		bytesRetrieved: 0,
		bytesStored:    0,
	}
}

func (s *LedgerBaseStorage) Retrieve(id StorageID) ([]byte, bool, error) {
	v, err := s.ledger.GetValue(id.Address[:], SlabIndexToLedgerKey(id.Index))
	s.bytesRetrieved += len(v)

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return nil, false, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
	}

	return v, len(v) > 0, nil
}

func (s *LedgerBaseStorage) Store(id StorageID, data []byte) error {
	s.bytesStored += len(data)
	err := s.ledger.SetValue(id.Address[:], SlabIndexToLedgerKey(id.Index), data)

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
	}

	return nil
}

func (s *LedgerBaseStorage) Remove(id StorageID) error {
	err := s.ledger.SetValue(id.Address[:], SlabIndexToLedgerKey(id.Index), nil)

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
	}

	return nil
}

func (s *LedgerBaseStorage) GenerateStorageID(address Address) (StorageID, error) {
	idx, err := s.ledger.AllocateStorageIndex(address[:])

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return StorageID{},
			wrapErrorfAsExternalErrorIfNeeded(
				err,
				fmt.Sprintf("failed to generate storage ID with address 0x%x", address),
			)
	}

	return NewStorageID(address, idx), nil
}

func SlabIndexToLedgerKey(ind StorageIndex) []byte {
	return []byte(LedgerBaseStorageSlabPrefix + string(ind[:]))
}

func LedgerKeyIsSlabKey(key string) bool {
	return strings.HasPrefix(key, LedgerBaseStorageSlabPrefix)
}

func (s *LedgerBaseStorage) BytesRetrieved() int {
	return s.bytesRetrieved
}

func (s *LedgerBaseStorage) BytesStored() int {
	return s.bytesStored
}

func (s *LedgerBaseStorage) SegmentCounts() int {
	// TODO
	return 0
}

func (s *LedgerBaseStorage) Size() int {
	// TODO
	return 0
}

func (s *LedgerBaseStorage) SegmentsReturned() int {
	// TODO
	return 0
}

func (s *LedgerBaseStorage) SegmentsUpdated() int {
	// TODO
	return 0
}

func (s *LedgerBaseStorage) SegmentsTouched() int {
	// TODO
	return 0
}

func (s *LedgerBaseStorage) ResetReporter() {
	s.bytesStored = 0
	s.bytesRetrieved = 0
}

type SlabIterator func() (StorageID, Slab)

type SlabStorage interface {
	Store(StorageID, Slab) error
	Retrieve(StorageID) (Slab, bool, error)
	RetrieveIfLoaded(StorageID) Slab
	Remove(StorageID) error
	GenerateStorageID(address Address) (StorageID, error)
	Count() int
	SlabIterator() (SlabIterator, error)
}

type BasicSlabStorage struct {
	Slabs          map[StorageID]Slab
	storageIndex   map[Address]StorageIndex
	DecodeStorable StorableDecoder
	DecodeTypeInfo TypeInfoDecoder
	cborEncMode    cbor.EncMode
	cborDecMode    cbor.DecMode
}

var _ SlabStorage = &BasicSlabStorage{}

func NewBasicSlabStorage(
	cborEncMode cbor.EncMode,
	cborDecMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) *BasicSlabStorage {
	return &BasicSlabStorage{
		Slabs:          make(map[StorageID]Slab),
		storageIndex:   make(map[Address]StorageIndex),
		cborEncMode:    cborEncMode,
		cborDecMode:    cborDecMode,
		DecodeStorable: decodeStorable,
		DecodeTypeInfo: decodeTypeInfo,
	}
}

func (s *BasicSlabStorage) GenerateStorageID(address Address) (StorageID, error) {
	index := s.storageIndex[address]
	nextIndex := index.Next()

	s.storageIndex[address] = nextIndex
	return NewStorageID(address, nextIndex), nil
}

func (s *BasicSlabStorage) RetrieveIfLoaded(id StorageID) Slab {
	return s.Slabs[id]
}

func (s *BasicSlabStorage) Retrieve(id StorageID) (Slab, bool, error) {
	slab, ok := s.Slabs[id]
	return slab, ok, nil
}

func (s *BasicSlabStorage) Store(id StorageID, slab Slab) error {
	s.Slabs[id] = slab
	return nil
}

func (s *BasicSlabStorage) Remove(id StorageID) error {
	delete(s.Slabs, id)
	return nil
}

func (s *BasicSlabStorage) Count() int {
	return len(s.Slabs)
}

func (s *BasicSlabStorage) StorageIDs() []StorageID {
	result := make([]StorageID, 0, len(s.Slabs))
	for storageID := range s.Slabs {
		result = append(result, storageID)
	}
	return result
}

// Encode returns serialized slabs in storage.
// This is currently used for testing.
func (s *BasicSlabStorage) Encode() (map[StorageID][]byte, error) {
	m := make(map[StorageID][]byte)
	for id, slab := range s.Slabs {
		b, err := Encode(slab, s.cborEncMode)
		if err != nil {
			// err is already categorized by Encode().
			return nil, err
		}
		m[id] = b
	}
	return m, nil
}

func (s *BasicSlabStorage) SlabIterator() (SlabIterator, error) {
	var slabs []struct {
		StorageID
		Slab
	}

	for id, slab := range s.Slabs {
		slabs = append(slabs, struct {
			StorageID
			Slab
		}{
			StorageID: id,
			Slab:      slab,
		})
	}

	var i int

	return func() (StorageID, Slab) {
		if i >= len(slabs) {
			return StorageIDUndefined, nil
		}
		slabEntry := slabs[i]
		i++
		return slabEntry.StorageID, slabEntry.Slab
	}, nil
}

// CheckStorageHealth checks for the health of slab storage.
// It traverses the slabs and checks these factors:
// - All non-root slabs only has a single parent reference (no double referencing)
// - Every child of a parent shares the same ownership (childStorageID.Address == parentStorageID.Address)
// - The number of root slabs are equal to the expected number (skipped if expectedNumberOfRootSlabs is -1)
// This should be used for testing purposes only, as it might be slow to process
func CheckStorageHealth(storage SlabStorage, expectedNumberOfRootSlabs int) (map[StorageID]struct{}, error) {
	parentOf := make(map[StorageID]StorageID)
	leaves := make([]StorageID, 0)

	slabIterator, err := storage.SlabIterator()
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create slab iterator")
	}

	slabs := map[StorageID]Slab{}

	for {
		id, slab := slabIterator()
		if id == StorageIDUndefined {
			break
		}

		if _, ok := slabs[id]; ok {
			return nil, NewFatalError(fmt.Errorf("duplicate slab %s", id))
		}
		slabs[id] = slab

		atLeastOneExternalSlab := false
		childStorables := slab.ChildStorables()

		for len(childStorables) > 0 {

			var next []Storable

			for _, s := range childStorables {

				if sids, ok := s.(StorageIDStorable); ok {
					sid := StorageID(sids)
					if _, found := parentOf[sid]; found {
						return nil, NewFatalError(fmt.Errorf("two parents are captured for the slab %s", sid))
					}
					parentOf[sid] = id
					atLeastOneExternalSlab = true
				}

				next = append(next, s.ChildStorables()...)
			}

			childStorables = next
		}

		if !atLeastOneExternalSlab {
			leaves = append(leaves, id)
		}
	}

	rootsMap := make(map[StorageID]struct{})
	visited := make(map[StorageID]struct{})
	var id StorageID
	for _, leaf := range leaves {
		id = leaf
		if _, ok := visited[id]; ok {
			return nil, NewFatalError(fmt.Errorf("at least two references found to the leaf slab %s", id))
		}
		visited[id] = struct{}{}
		for {
			parentID, found := parentOf[id]
			if !found {
				// we reach the root
				rootsMap[id] = struct{}{}
				break
			}
			visited[parentID] = struct{}{}

			childSlab, ok, err := storage.Retrieve(id)
			if !ok {
				return nil, NewSlabNotFoundErrorf(id, "failed to get child slab")
			}
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve child slab %s", id))
			}

			parentSlab, ok, err := storage.Retrieve(parentID)
			if !ok {
				return nil, NewSlabNotFoundErrorf(id, "failed to get parent slab")
			}
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve parent slab %s", parentID))
			}

			childOwner := childSlab.ID().Address
			parentOwner := parentSlab.ID().Address

			if childOwner != parentOwner {
				return nil, NewFatalError(
					fmt.Errorf(
						"parent and child are not owned by the same account: child.owner %s, parent.owner %s",
						childOwner,
						parentOwner,
					))
			}
			id = parentID
		}
	}

	if len(visited) != len(slabs) {

		var unreachableID StorageID
		var unreachableSlab Slab

		for id, slab := range slabs {
			if _, ok := visited[id]; !ok {
				unreachableID = id
				unreachableSlab = slab
				break
			}
		}

		return nil, NewFatalError(
			fmt.Errorf(
				"slab was not reachable from leaves: %s: %s",
				unreachableID,
				unreachableSlab,
			))
	}

	if (expectedNumberOfRootSlabs >= 0) && (len(rootsMap) != expectedNumberOfRootSlabs) {
		return nil, NewFatalError(
			fmt.Errorf(
				"number of root slabs doesn't match: expected %d, got %d",
				expectedNumberOfRootSlabs,
				len(rootsMap),
			))
	}

	return rootsMap, nil
}

type PersistentSlabStorage struct {
	baseStorage      BaseStorage
	cache            map[StorageID]Slab
	deltas           map[StorageID]Slab
	tempStorageIndex uint64
	DecodeStorable   StorableDecoder
	DecodeTypeInfo   TypeInfoDecoder
	cborEncMode      cbor.EncMode
	cborDecMode      cbor.DecMode
}

var _ SlabStorage = &PersistentSlabStorage{}

func (s *PersistentSlabStorage) SlabIterator() (SlabIterator, error) {

	var slabs []struct {
		StorageID
		Slab
	}

	appendChildStorables := func(slab Slab) error {
		childStorables := slab.ChildStorables()

		for len(childStorables) > 0 {

			var nextChildStorables []Storable

			for _, childStorable := range childStorables {

				storageIDStorable, ok := childStorable.(StorageIDStorable)
				if !ok {
					continue
				}

				id := StorageID(storageIDStorable)

				if _, ok := s.deltas[id]; ok {
					continue
				}

				if _, ok := s.cache[id]; ok {
					continue
				}

				var err error
				slab, ok, err = s.RetrieveIgnoringDeltas(id)
				if !ok {
					return NewSlabNotFoundErrorf(id, "slab not found during slab iteration")
				}
				if err != nil {
					return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
				}

				slabs = append(slabs, struct {
					StorageID
					Slab
				}{
					StorageID: id,
					Slab:      slab,
				})

				nextChildStorables = append(
					nextChildStorables,
					slab.ChildStorables()...,
				)
			}

			childStorables = nextChildStorables
		}

		return nil
	}

	appendSlab := func(id StorageID, slab Slab) error {
		slabs = append(slabs, struct {
			StorageID
			Slab
		}{
			StorageID: id,
			Slab:      slab,
		})

		return appendChildStorables(slab)
	}

	for id, slab := range s.deltas {
		if slab == nil {
			continue
		}

		err := appendSlab(id, slab)
		if err != nil {
			return nil, err
		}
	}

	// Create a temporary copy of all the cached IDs,
	// as s.cache will get mutated inside the for-loop

	var cached []StorageID
	for id := range s.cache {
		cached = append(cached, id)
	}

	for _, id := range cached {
		slab := s.cache[id]

		if slab == nil {
			continue
		}

		if _, ok := s.deltas[id]; ok {
			continue
		}

		err := appendSlab(id, slab)
		if err != nil {
			return nil, err
		}
	}

	var i int

	return func() (StorageID, Slab) {
		if i >= len(slabs) {
			return StorageIDUndefined, nil
		}
		slabEntry := slabs[i]
		i++
		return slabEntry.StorageID, slabEntry.Slab
	}, nil
}

type StorageOption func(st *PersistentSlabStorage) *PersistentSlabStorage

func NewPersistentSlabStorage(
	base BaseStorage,
	cborEncMode cbor.EncMode,
	cborDecMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
	opts ...StorageOption,
) *PersistentSlabStorage {
	storage := &PersistentSlabStorage{baseStorage: base,
		cache:          make(map[StorageID]Slab),
		deltas:         make(map[StorageID]Slab),
		cborEncMode:    cborEncMode,
		cborDecMode:    cborDecMode,
		DecodeStorable: decodeStorable,
		DecodeTypeInfo: decodeTypeInfo,
	}

	for _, applyOption := range opts {
		storage = applyOption(storage)
	}

	return storage
}

func (s *PersistentSlabStorage) GenerateStorageID(address Address) (StorageID, error) {
	if address == AddressUndefined {
		var idx StorageIndex
		s.tempStorageIndex++
		binary.BigEndian.PutUint64(idx[:], s.tempStorageIndex)
		return NewStorageID(address, idx), nil
	}
	id, err := s.baseStorage.GenerateStorageID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
		return StorageID{}, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate storage ID for address 0x%x", address))
	}
	return id, nil
}

func (s *PersistentSlabStorage) sortedOwnedDeltaKeys() []StorageID {
	keysWithOwners := make([]StorageID, 0, len(s.deltas))
	for k := range s.deltas {
		// ignore the ones that are not owned by accounts
		if k.Address != AddressUndefined {
			keysWithOwners = append(keysWithOwners, k)
		}
	}

	sort.Slice(keysWithOwners, func(i, j int) bool {
		a := keysWithOwners[i]
		b := keysWithOwners[j]
		if a.Address == b.Address {
			return a.IndexAsUint64() < b.IndexAsUint64()
		}
		return a.AddressAsUint64() < b.AddressAsUint64()
	})
	return keysWithOwners
}

func (s *PersistentSlabStorage) Commit() error {
	var err error

	// this part ensures the keys are sorted so commit operation is deterministic
	keysWithOwners := s.sortedOwnedDeltaKeys()

	for _, id := range keysWithOwners {
		slab := s.deltas[id]

		// deleted slabs
		if slab == nil {
			err = s.baseStorage.Remove(id)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
			}
			// Deleted slabs are removed from deltas and added to read cache so that:
			// 1. next read is from in-memory read cache
			// 2. deleted slabs are not re-committed in next commit
			s.cache[id] = nil
			delete(s.deltas, id)
			continue
		}

		// serialize
		data, err := Encode(slab, s.cborEncMode)
		if err != nil {
			// err is categorized already by Encode()
			return err
		}

		// store
		err = s.baseStorage.Store(id, data)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
		}

		// add to read cache
		s.cache[id] = slab
		// It's safe to remove slab from deltas because
		// iteration is on non-temp slabs and temp slabs
		// are still in deltas.
		delete(s.deltas, id)
	}

	// Do NOT reset deltas because slabs with empty address are not saved.

	return nil
}

func (s *PersistentSlabStorage) FastCommit(numWorkers int) error {

	// this part ensures the keys are sorted so commit operation is deterministic
	keysWithOwners := s.sortedOwnedDeltaKeys()

	if len(keysWithOwners) == 0 {
		return nil
	}

	// limit the number of workers to the number of keys
	if numWorkers > len(keysWithOwners) {
		numWorkers = len(keysWithOwners)
	}

	// construct job queue
	jobs := make(chan StorageID, len(keysWithOwners))
	for _, id := range keysWithOwners {
		jobs <- id
	}
	close(jobs)

	type encodedSlabs struct {
		storageID StorageID
		data      []byte
		err       error
	}

	// construct result queue
	results := make(chan *encodedSlabs, len(keysWithOwners))

	// define encoders (workers) and launch them
	// encoders encodes slabs in parallel
	encoder := func(wg *sync.WaitGroup, done <-chan struct{}, jobs <-chan StorageID, results chan<- *encodedSlabs) {
		defer wg.Done()

		for id := range jobs {
			// Check if goroutine is signaled to stop before proceeding.
			select {
			case <-done:
				return
			default:
			}

			slab := s.deltas[id]
			if slab == nil {
				results <- &encodedSlabs{
					storageID: id,
					data:      nil,
					err:       nil,
				}
				continue
			}
			// serialize
			data, err := Encode(slab, s.cborEncMode)
			results <- &encodedSlabs{
				storageID: id,
				data:      data,
				err:       err,
			}
		}
	}

	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go encoder(&wg, done, jobs, results)
	}

	defer func() {
		// This ensures that all goroutines are stopped before output channel is closed.

		// Wait for all goroutines to finish
		wg.Wait()

		// Close output channel
		close(results)
	}()

	// process the results while encoders are working
	// we need to capture them inside a map
	// again so we can apply them in order of keys
	encSlabByID := make(map[StorageID][]byte)
	for i := 0; i < len(keysWithOwners); i++ {
		result := <-results
		// if any error return
		if result.err != nil {
			// Closing done channel signals goroutines to stop.
			close(done)
			// result.err is already categorized by Encode().
			return result.err
		}
		encSlabByID[result.storageID] = result.data
	}

	// at this stage all results has been processed
	// and ready to be passed to base storage layer
	for _, id := range keysWithOwners {
		data := encSlabByID[id]

		var err error
		// deleted slabs
		if data == nil {
			err = s.baseStorage.Remove(id)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
			}
			// Deleted slabs are removed from deltas and added to read cache so that:
			// 1. next read is from in-memory read cache
			// 2. deleted slabs are not re-committed in next commit
			s.cache[id] = nil
			delete(s.deltas, id)
			continue
		}

		// store
		err = s.baseStorage.Store(id, data)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
		}

		s.cache[id] = s.deltas[id]
		// It's safe to remove slab from deltas because
		// iteration is on non-temp slabs and temp slabs
		// are still in deltas.
		delete(s.deltas, id)
	}

	// Do NOT reset deltas because slabs with empty address are not saved.

	return nil
}

func (s *PersistentSlabStorage) DropDeltas() {
	s.deltas = make(map[StorageID]Slab)
}

func (s *PersistentSlabStorage) DropCache() {
	s.cache = make(map[StorageID]Slab)
}

func (s *PersistentSlabStorage) RetrieveIgnoringDeltas(id StorageID) (Slab, bool, error) {

	// check the read cache next
	if slab, ok := s.cache[id]; ok {
		return slab, slab != nil, nil
	}

	// fetch from base storage last
	data, ok, err := s.baseStorage.Retrieve(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
		return nil, ok, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
	}
	if !ok {
		return nil, ok, nil
	}

	slab, err := DecodeSlab(id, data, s.cborDecMode, s.DecodeStorable, s.DecodeTypeInfo)
	if err != nil {
		// err is already categorized by DecodeSlab().
		return nil, ok, err
	}

	// save decoded slab to cache
	s.cache[id] = slab

	return slab, ok, nil
}

func (s *PersistentSlabStorage) RetrieveIfLoaded(id StorageID) Slab {
	// check deltas first.
	if slab, ok := s.deltas[id]; ok {
		return slab
	}

	// check the read cache next.
	if slab, ok := s.cache[id]; ok {
		return slab
	}

	// Don't fetch from base storage.
	return nil
}

func (s *PersistentSlabStorage) Retrieve(id StorageID) (Slab, bool, error) {
	// check deltas first
	if slab, ok := s.deltas[id]; ok {
		return slab, slab != nil, nil
	}

	// Don't need to wrap error as external error because err is already categorized by PersistentSlabStorage.RetrieveIgnoringDeltas().
	return s.RetrieveIgnoringDeltas(id)
}

func (s *PersistentSlabStorage) Store(id StorageID, slab Slab) error {
	// add to deltas
	s.deltas[id] = slab
	return nil
}

func (s *PersistentSlabStorage) Remove(id StorageID) error {
	// add to nil to deltas under that id
	s.deltas[id] = nil
	return nil
}

// Warning Counts doesn't consider new segments in the deltas and only returns committed values
func (s *PersistentSlabStorage) Count() int {
	return s.baseStorage.SegmentCounts()
}

// Deltas returns number of uncommitted slabs, including slabs with temp addresses.
func (s *PersistentSlabStorage) Deltas() uint {
	return uint(len(s.deltas))
}

// DeltasWithoutTempAddresses returns number of uncommitted slabs, excluding slabs with temp addresses.
func (s *PersistentSlabStorage) DeltasWithoutTempAddresses() uint {
	deltas := uint(0)
	for k := range s.deltas {
		// exclude the ones that are not owned by accounts
		if k.Address != AddressUndefined {
			deltas++
		}
	}
	return deltas
}

// DeltasSizeWithoutTempAddresses returns total size of uncommitted slabs (in bytes), excluding slabs with temp addresses.
func (s *PersistentSlabStorage) DeltasSizeWithoutTempAddresses() uint64 {
	size := uint64(0)
	for k, slab := range s.deltas {
		// Exclude slabs that are not owned by accounts.
		if k.Address == AddressUndefined || slab == nil {
			continue
		}

		size += uint64(slab.ByteSize())
	}
	return size
}

func storeSlab(storage SlabStorage, slab Slab) error {
	id := slab.ID()
	err := storage.Store(id, slab)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
	}
	return nil
}

// FixLoadedBrokenReferences traverses loaded slabs and fixes broken references in maps.
// A broken reference is a StorageID referencing a non-existent slab.
// To fix a map containing broken references, this function replaces broken map with
// empty map having the same StorageID and also removes all slabs in the old map.
// Limitations:
// - only fix broken references in map
// - only traverse loaded slabs in deltas and cache
// NOTE: The intended use case is to enable migration programs in onflow/flow-go to
// fix broken references.  As of April 2024, only 10 registers in testnet (not mainnet)
// were found to have broken references and they seem to have resulted from a bug
// that was fixed 2 years ago by https://github.com/onflow/cadence/pull/1565.
func (s *PersistentSlabStorage) FixLoadedBrokenReferences() ([]StorageID, error) {

	// parentOf is used to find root slab from non-root slab.
	// Broken reference can be in non-root slab, and we need StorageID of root slab
	// to replace broken map by creating an empty new map with same StorageID.
	parentOf := make(map[StorageID]StorageID)

	getRootSlabID := func(id StorageID) StorageID {
		for {
			parentID, ok := parentOf[id]
			if ok {
				id = parentID
			} else {
				return id
			}
		}
	}

	hasBrokenReferenceInSlab := func(id StorageID, slab Slab) bool {
		if slab == nil {
			return false
		}

		var isMetaDataSlab bool

		switch slab.(type) {
		case *ArrayMetaDataSlab, *MapMetaDataSlab:
			isMetaDataSlab = true
		}

		var foundBrokenRef bool
		for _, childStorable := range slab.ChildStorables() {

			storageIDStorable, ok := childStorable.(StorageIDStorable)
			if !ok {
				continue
			}

			childID := StorageID(storageIDStorable)

			// Track parent-child relationship of root slabs and non-root slabs.
			if isMetaDataSlab {
				parentOf[childID] = id
			}

			if s.existIfLoaded(childID) {
				continue
			}

			foundBrokenRef = true

			if !isMetaDataSlab {
				return true
			}
		}

		return foundBrokenRef
	}

	var brokenStorageIDs []StorageID

	// Iterate delta slabs.
	for id, slab := range s.deltas {
		if hasBrokenReferenceInSlab(id, slab) {
			brokenStorageIDs = append(brokenStorageIDs, id)
		}
	}

	// Iterate cache slabs.
	for id, slab := range s.cache {
		if _, ok := s.deltas[id]; ok {
			continue
		}
		if hasBrokenReferenceInSlab(id, slab) {
			brokenStorageIDs = append(brokenStorageIDs, id)
		}
	}

	if len(brokenStorageIDs) == 0 {
		return nil, nil
	}

	rootSlabStorageIDsWithBrokenData := make(map[StorageID]struct{})
	var errs []error

	// Find StorageIDs of root slab for slabs containing broken references.
	for _, id := range brokenStorageIDs {
		rootID := getRootSlabID(id)
		if rootID == StorageIDUndefined {
			errs = append(errs, fmt.Errorf("failed to get root slab id for slab %s", id))
			continue
		}
		rootSlabStorageIDsWithBrokenData[rootID] = struct{}{}
	}

	for rootSlabID := range rootSlabStorageIDsWithBrokenData {
		rootSlab := s.RetrieveIfLoaded(rootSlabID)
		if rootSlab == nil {
			errs = append(errs, fmt.Errorf("failed to retrieve loaded root slab %s", rootSlabID))
			continue
		}

		switch rootSlab := rootSlab.(type) {
		case MapSlab:
			if rootSlab.ExtraData() == nil {
				errs = append(errs, fmt.Errorf("failed to fix broken references because slab %s isn't root slab", rootSlab.ID()))
				continue
			}

			err := s.fixBrokenReferencesInMap(rootSlab)
			if err != nil {
				errs = append(errs, err)
			}

		default:
			// IMPORTANT: Only handle map slabs for now.  DO NOT silently fix currently unknown problems.
			errs = append(errs, fmt.Errorf("failed to fix broken references in non-map slab %s (%T)", rootSlab.ID(), rootSlab))
		}
	}

	return brokenStorageIDs, errors.Join(errs...)
}

// fixBrokenReferencesInMap replaces replaces broken map with empty map
// having the same StorageID and also removes all slabs in the old map.
func (s *PersistentSlabStorage) fixBrokenReferencesInMap(old MapSlab) error {
	id := old.ID()

	oldExtraData := old.ExtraData()

	// Create an empty map with the same StorgeID, type, and seed as the old map.
	new := &MapDataSlab{
		header: MapSlabHeader{
			id:   id,
			size: mapRootDataSlabPrefixSize + hkeyElementsPrefixSize,
		},
		extraData: &MapExtraData{
			TypeInfo: oldExtraData.TypeInfo,
			Seed:     oldExtraData.Seed,
		},
		elements: newHkeyElements(0),
	}

	// Store new empty map with the same StorageID.
	err := s.Store(id, new)
	if err != nil {
		return err
	}

	// Remove all slabs and references in old map.
	references, _, err := s.getAllChildReferences(old)
	if err != nil {
		return err
	}

	for _, childID := range references {
		err = s.Remove(childID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *PersistentSlabStorage) existIfLoaded(id StorageID) bool {
	// Check deltas.
	if slab, ok := s.deltas[id]; ok {
		return slab != nil
	}

	// Check read cache.
	if slab, ok := s.cache[id]; ok {
		return slab != nil
	}

	return false
}

// getAllChildReferences returns child references of given slab (all levels).
func (s *PersistentSlabStorage) getAllChildReferences(slab Slab) (
	references []StorageID,
	brokenReferences []StorageID,
	err error,
) {
	childStorables := slab.ChildStorables()

	for len(childStorables) > 0 {

		var nextChildStorables []Storable

		for _, childStorable := range childStorables {

			storageIDStorable, ok := childStorable.(StorageIDStorable)
			if !ok {
				continue
			}

			childID := StorageID(storageIDStorable)

			childSlab, ok, err := s.Retrieve(childID)
			if err != nil {
				return nil, nil, err
			}
			if !ok {
				brokenReferences = append(brokenReferences, childID)
				continue
			}

			references = append(references, childID)

			nextChildStorables = append(
				nextChildStorables,
				childSlab.ChildStorables()...,
			)
		}

		childStorables = nextChildStorables
	}

	return references, brokenReferences, nil
}
