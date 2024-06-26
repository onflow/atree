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
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

const LedgerBaseStorageSlabPrefix = "$"

// ValueID identifies Array and OrderedMap.
type ValueID [16]byte

type (
	Address   [8]byte
	SlabIndex [8]byte

	// SlabID identifies slab in storage.
	// SlabID should only be used to retrieve,
	// store, and remove slab in storage.
	SlabID struct {
		address Address
		index   SlabIndex
	}
)

var (
	AddressUndefined   = Address{}
	SlabIndexUndefined = SlabIndex{}
	SlabIDUndefined    = SlabID{}
)

// Next returns new SlabIndex with index+1 value.
// The caller is responsible for preventing overflow
// by checking if the index value is valid before
// calling this function.
func (index SlabIndex) Next() SlabIndex {
	i := binary.BigEndian.Uint64(index[:])

	var next SlabIndex
	binary.BigEndian.PutUint64(next[:], i+1)

	return next
}

func NewSlabID(address Address, index SlabIndex) SlabID {
	return SlabID{address, index}
}

func NewSlabIDFromRawBytes(b []byte) (SlabID, error) {
	if len(b) < slabIDSize {
		return SlabID{}, NewSlabIDErrorf("incorrect slab ID buffer length %d", len(b))
	}

	var address Address
	copy(address[:], b)

	var index SlabIndex
	copy(index[:], b[8:])

	return SlabID{address, index}, nil
}

func (id SlabID) ToRawBytes(b []byte) (int, error) {
	if len(b) < slabIDSize {
		return 0, NewSlabIDErrorf("incorrect slab ID buffer length %d", len(b))
	}
	copy(b, id.address[:])
	copy(b[8:], id.index[:])
	return slabIDSize, nil
}

func (id SlabID) String() string {
	return fmt.Sprintf(
		"0x%x.%d",
		binary.BigEndian.Uint64(id.address[:]),
		binary.BigEndian.Uint64(id.index[:]),
	)
}

func (id SlabID) AddressAsUint64() uint64 {
	return binary.BigEndian.Uint64(id.address[:])
}

func (id SlabID) IndexAsUint64() uint64 {
	return binary.BigEndian.Uint64(id.index[:])
}

func (id SlabID) HasTempAddress() bool {
	return id.address == AddressUndefined
}

func (id SlabID) Index() SlabIndex {
	return id.index
}

func (id SlabID) Valid() error {
	if id == SlabIDUndefined {
		return NewSlabIDError("undefined slab ID")
	}
	if id.index == SlabIndexUndefined {
		return NewSlabIDError("undefined slab index")
	}
	return nil
}

func (id SlabID) Compare(other SlabID) int {
	result := bytes.Compare(id.address[:], other.address[:])
	if result == 0 {
		return bytes.Compare(id.index[:], other.index[:])
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
	Store(SlabID, []byte) error
	Retrieve(SlabID) ([]byte, bool, error)
	Remove(SlabID) error
	GenerateSlabID(Address) (SlabID, error)
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
	// AllocateSlabIndex allocates a new slab index under the given account.
	AllocateSlabIndex(owner []byte) (SlabIndex, error)
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

func (s *LedgerBaseStorage) Retrieve(id SlabID) ([]byte, bool, error) {
	v, err := s.ledger.GetValue(id.address[:], SlabIndexToLedgerKey(id.index))
	s.bytesRetrieved += len(v)

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return nil, false, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
	}

	return v, len(v) > 0, nil
}

func (s *LedgerBaseStorage) Store(id SlabID, data []byte) error {
	s.bytesStored += len(data)
	err := s.ledger.SetValue(id.address[:], SlabIndexToLedgerKey(id.index), data)

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
	}

	return nil
}

func (s *LedgerBaseStorage) Remove(id SlabID) error {
	err := s.ledger.SetValue(id.address[:], SlabIndexToLedgerKey(id.index), nil)

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
	}

	return nil
}

func (s *LedgerBaseStorage) GenerateSlabID(address Address) (SlabID, error) {
	idx, err := s.ledger.AllocateSlabIndex(address[:])

	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Ledger interface.
		return SlabID{},
			wrapErrorfAsExternalErrorIfNeeded(
				err,
				fmt.Sprintf("failed to generate slab ID with address 0x%x", address),
			)
	}

	return NewSlabID(address, idx), nil
}

func SlabIndexToLedgerKey(ind SlabIndex) []byte {
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

type SlabIterator func() (SlabID, Slab)

type SlabStorage interface {
	Store(SlabID, Slab) error
	Retrieve(SlabID) (Slab, bool, error)
	RetrieveIfLoaded(SlabID) Slab
	Remove(SlabID) error
	GenerateSlabID(address Address) (SlabID, error)
	Count() int
	SlabIterator() (SlabIterator, error)
}

type BasicSlabStorage struct {
	Slabs          map[SlabID]Slab
	slabIndex      map[Address]SlabIndex
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
		Slabs:          make(map[SlabID]Slab),
		slabIndex:      make(map[Address]SlabIndex),
		cborEncMode:    cborEncMode,
		cborDecMode:    cborDecMode,
		DecodeStorable: decodeStorable,
		DecodeTypeInfo: decodeTypeInfo,
	}
}

func (s *BasicSlabStorage) GenerateSlabID(address Address) (SlabID, error) {
	index := s.slabIndex[address]
	nextIndex := index.Next()

	s.slabIndex[address] = nextIndex
	return NewSlabID(address, nextIndex), nil
}

func (s *BasicSlabStorage) RetrieveIfLoaded(id SlabID) Slab {
	return s.Slabs[id]
}

func (s *BasicSlabStorage) Retrieve(id SlabID) (Slab, bool, error) {
	slab, ok := s.Slabs[id]
	return slab, ok, nil
}

func (s *BasicSlabStorage) Store(id SlabID, slab Slab) error {
	s.Slabs[id] = slab
	return nil
}

func (s *BasicSlabStorage) Remove(id SlabID) error {
	delete(s.Slabs, id)
	return nil
}

func (s *BasicSlabStorage) Count() int {
	return len(s.Slabs)
}

func (s *BasicSlabStorage) SlabIDs() []SlabID {
	result := make([]SlabID, 0, len(s.Slabs))
	for slabID := range s.Slabs {
		result = append(result, slabID)
	}
	return result
}

// Encode returns serialized slabs in storage.
// This is currently used for testing.
func (s *BasicSlabStorage) Encode() (map[SlabID][]byte, error) {
	m := make(map[SlabID][]byte)
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
	type slabEntry struct {
		SlabID
		Slab
	}

	var slabs []slabEntry

	if len(s.Slabs) > 0 {
		slabs = make([]slabEntry, 0, len(s.Slabs))
	}

	for id, slab := range s.Slabs {
		slabs = append(slabs, slabEntry{
			SlabID: id,
			Slab:   slab,
		})
	}

	var i int

	return func() (SlabID, Slab) {
		if i >= len(slabs) {
			return SlabIDUndefined, nil
		}
		slabEntry := slabs[i]
		i++
		return slabEntry.SlabID, slabEntry.Slab
	}, nil
}

// CheckStorageHealth checks for the health of slab storage.
// It traverses the slabs and checks these factors:
// - All non-root slabs only has a single parent reference (no double referencing)
// - Every child of a parent shares the same ownership (childSlabID.Address == parentSlabID.Address)
// - The number of root slabs are equal to the expected number (skipped if expectedNumberOfRootSlabs is -1)
// This should be used for testing purposes only, as it might be slow to process
func CheckStorageHealth(storage SlabStorage, expectedNumberOfRootSlabs int) (map[SlabID]struct{}, error) {
	parentOf := make(map[SlabID]SlabID)
	leaves := make([]SlabID, 0)

	slabIterator, err := storage.SlabIterator()
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create slab iterator")
	}

	slabs := map[SlabID]Slab{}

	for {
		id, slab := slabIterator()
		if id == SlabIDUndefined {
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

				if sids, ok := s.(SlabIDStorable); ok {
					sid := SlabID(sids)
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

	rootsMap := make(map[SlabID]struct{})
	visited := make(map[SlabID]struct{})
	var id SlabID
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

			childOwner := childSlab.SlabID().address
			parentOwner := parentSlab.SlabID().address

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

		var unreachableID SlabID
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
	baseStorage    BaseStorage
	cache          map[SlabID]Slab
	deltas         map[SlabID]Slab
	tempSlabIndex  uint64
	DecodeStorable StorableDecoder
	DecodeTypeInfo TypeInfoDecoder
	cborEncMode    cbor.EncMode
	cborDecMode    cbor.DecMode
}

var _ SlabStorage = &PersistentSlabStorage{}

func (s *PersistentSlabStorage) SlabIterator() (SlabIterator, error) {

	var slabs []struct {
		SlabID
		Slab
	}

	appendChildStorables := func(slab Slab) error {
		childStorables := slab.ChildStorables()

		for len(childStorables) > 0 {

			var nextChildStorables []Storable

			for _, childStorable := range childStorables {

				slabIDStorable, ok := childStorable.(SlabIDStorable)
				if !ok {
					continue
				}

				id := SlabID(slabIDStorable)

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
					SlabID
					Slab
				}{
					SlabID: id,
					Slab:   slab,
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

	appendSlab := func(id SlabID, slab Slab) error {
		slabs = append(slabs, struct {
			SlabID
			Slab
		}{
			SlabID: id,
			Slab:   slab,
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

	cached := make([]SlabID, 0, len(s.cache))
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

	return func() (SlabID, Slab) {
		if i >= len(slabs) {
			return SlabIDUndefined, nil
		}
		slabEntry := slabs[i]
		i++
		return slabEntry.SlabID, slabEntry.Slab
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
	storage := &PersistentSlabStorage{
		baseStorage:    base,
		cache:          make(map[SlabID]Slab),
		deltas:         make(map[SlabID]Slab),
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

func (s *PersistentSlabStorage) GenerateSlabID(address Address) (SlabID, error) {
	if address == AddressUndefined {
		var idx SlabIndex
		s.tempSlabIndex++
		binary.BigEndian.PutUint64(idx[:], s.tempSlabIndex)
		return NewSlabID(address, idx), nil
	}
	id, err := s.baseStorage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
		return SlabID{}, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}
	return id, nil
}

func (s *PersistentSlabStorage) sortedOwnedDeltaKeys() []SlabID {
	keysWithOwners := make([]SlabID, 0, len(s.deltas))
	for k := range s.deltas {
		// ignore the ones that are not owned by accounts
		if k.address != AddressUndefined {
			keysWithOwners = append(keysWithOwners, k)
		}
	}

	sort.Slice(keysWithOwners, func(i, j int) bool {
		a := keysWithOwners[i]
		b := keysWithOwners[j]
		if a.address == b.address {
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
	jobs := make(chan SlabID, len(keysWithOwners))
	for _, id := range keysWithOwners {
		jobs <- id
	}
	close(jobs)

	type encodedSlabs struct {
		slabID SlabID
		data   []byte
		err    error
	}

	// construct result queue
	results := make(chan *encodedSlabs, len(keysWithOwners))

	// define encoders (workers) and launch them
	// encoders encodes slabs in parallel
	encoder := func(wg *sync.WaitGroup, done <-chan struct{}, jobs <-chan SlabID, results chan<- *encodedSlabs) {
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
					slabID: id,
					data:   nil,
					err:    nil,
				}
				continue
			}
			// serialize
			data, err := Encode(slab, s.cborEncMode)
			results <- &encodedSlabs{
				slabID: id,
				data:   data,
				err:    err,
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
	encSlabByID := make(map[SlabID][]byte)
	for i := 0; i < len(keysWithOwners); i++ {
		result := <-results
		// if any error return
		if result.err != nil {
			// Closing done channel signals goroutines to stop.
			close(done)
			// result.err is already categorized by Encode().
			return result.err
		}
		encSlabByID[result.slabID] = result.data
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
	s.deltas = make(map[SlabID]Slab)
}

func (s *PersistentSlabStorage) DropCache() {
	s.cache = make(map[SlabID]Slab)
}

func (s *PersistentSlabStorage) RetrieveIgnoringDeltas(id SlabID) (Slab, bool, error) {

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

func (s *PersistentSlabStorage) RetrieveIfLoaded(id SlabID) Slab {
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

func (s *PersistentSlabStorage) Retrieve(id SlabID) (Slab, bool, error) {
	// check deltas first
	if slab, ok := s.deltas[id]; ok {
		return slab, slab != nil, nil
	}

	// Don't need to wrap error as external error because err is already categorized by PersistentSlabStorage.RetrieveIgnoringDeltas().
	return s.RetrieveIgnoringDeltas(id)
}

func (s *PersistentSlabStorage) Store(id SlabID, slab Slab) error {
	// add to deltas
	s.deltas[id] = slab
	return nil
}

func (s *PersistentSlabStorage) Remove(id SlabID) error {
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
		if k.address != AddressUndefined {
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
		if k.address == AddressUndefined || slab == nil {
			continue
		}

		size += uint64(slab.ByteSize())
	}
	return size
}
