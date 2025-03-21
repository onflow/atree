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
	"sort"
	"strings"
	"sync"

	"github.com/SophisticaSean/cbor/v2"
)

const LedgerBaseStorageSlabPrefix = "$"

func LedgerKeyIsSlabKey(key string) bool {
	return strings.HasPrefix(key, LedgerBaseStorageSlabPrefix)
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

// LedgerBaseStorage

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

// BasicSlabStorage

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
		b, err := EncodeSlab(slab, s.cborEncMode)
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

// PersistentSlabStorage

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

func (s *PersistentSlabStorage) SlabIterator() (SlabIterator, error) {

	var slabs []struct {
		SlabID
		Slab
	}

	// Get slabs connected to slab from base storage and append those slabs to slabs slice.
	appendChildStorables := func(slab Slab) error {
		childStorables := slab.ChildStorables()

		for len(childStorables) > 0 {

			var nextChildStorables []Storable

			for _, childStorable := range childStorables {

				slabIDStorable, ok := childStorable.(SlabIDStorable)
				if !ok {
					// Append child storables of this childStorable to handle inlined slab containing SlabIDStorable.
					nextChildStorables = append(
						nextChildStorables,
						childStorable.ChildStorables()...,
					)
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
				// Don't cache retrieved child slabs during slab iteration to prevent changes to storage cache.
				slab, ok, err = s.RetrieveIgnoringDeltas(id, false)
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

	// Append slab and slabs connected to it to slabs slice.
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

	// this part ensures the keys are sorted so commit operation is deterministic
	keysWithOwners := s.sortedOwnedDeltaKeys()

	return s.commit(keysWithOwners)
}

func (s *PersistentSlabStorage) commit(keys []SlabID) error {
	var err error

	for _, id := range keys {
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
		data, err := EncodeSlab(slab, s.cborEncMode)
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
			data, err := EncodeSlab(slab, s.cborEncMode)
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

	for range numWorkers {
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
	encSlabByID := make(map[SlabID][]byte, len(keysWithOwners))
	for range len(keysWithOwners) {
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

// NondeterministicFastCommit commits changed slabs in nondeterministic order.
// Encoded slab data is deterministic (e.g. array and map iteration is deterministic).
// IMPORTANT: This function is used by migration programs when commit order of slabs
// is not required to be deterministic (while preserving deterministic array and map iteration).
func (s *PersistentSlabStorage) NondeterministicFastCommit(numWorkers int) error {
	// No changes
	if len(s.deltas) == 0 {
		return nil
	}

	type slabToBeEncoded struct {
		slabID SlabID
		slab   Slab
	}

	type encodedSlab struct {
		slabID SlabID
		data   []byte
		err    error
	}

	// Define encoder (worker) to encode slabs in parallel
	encoder := func(
		wg *sync.WaitGroup,
		done <-chan struct{},
		jobs <-chan slabToBeEncoded,
		results chan<- encodedSlab,
	) {
		defer wg.Done()

		for job := range jobs {
			// Check if goroutine is signaled to stop before proceeding.
			select {
			case <-done:
				return
			default:
			}

			id := job.slabID
			slab := job.slab

			if slab == nil {
				results <- encodedSlab{
					slabID: id,
					data:   nil,
					err:    nil,
				}
				continue
			}

			// Serialize
			data, err := EncodeSlab(slab, s.cborEncMode)
			results <- encodedSlab{
				slabID: id,
				data:   data,
				err:    err,
			}
		}
	}

	// slabIDsWithOwner contains slab IDs with owner:
	// - modified slab IDs are stored from front to back
	// - deleted slab IDs are stored from back to front
	// This is to avoid extra allocations.
	slabIDsWithOwner := make([]SlabID, len(s.deltas))

	// Modified slabs need to be encoded (in parallel) and stored in underlying storage.
	modifiedSlabCount := 0
	// Deleted slabs need to be removed from underlying storage.
	deletedSlabCount := 0
	for id, slab := range s.deltas {
		// Ignore slabs not owned by accounts
		if id.address == AddressUndefined {
			continue
		}
		if slab == nil {
			// Set deleted slab ID from the end of slabIDsWithOwner.
			index := len(slabIDsWithOwner) - 1 - deletedSlabCount
			slabIDsWithOwner[index] = id
			deletedSlabCount++
		} else {
			// Set modified slab ID from the start of slabIDsWithOwner.
			slabIDsWithOwner[modifiedSlabCount] = id
			modifiedSlabCount++
		}
	}

	modifiedSlabIDs := slabIDsWithOwner[:modifiedSlabCount]

	deletedSlabIDs := slabIDsWithOwner[len(slabIDsWithOwner)-deletedSlabCount:]

	if modifiedSlabCount == 0 && deletedSlabCount == 0 {
		return nil
	}

	if modifiedSlabCount < 2 {
		// Avoid goroutine overhead.
		// Return after committing modified and deleted slabs.
		ids := modifiedSlabIDs
		ids = append(ids, deletedSlabIDs...)
		return s.commit(ids)
	}

	if numWorkers > modifiedSlabCount {
		numWorkers = modifiedSlabCount
	}

	var wg sync.WaitGroup

	// Create done signal channel
	done := make(chan struct{})

	// Create job queue
	jobs := make(chan slabToBeEncoded, modifiedSlabCount)

	// Create result queue
	results := make(chan encodedSlab, modifiedSlabCount)

	defer func() {
		// This ensures that all goroutines are stopped before output channel is closed.

		// Wait for all goroutines to finish
		wg.Wait()

		// Close output channel
		close(results)
	}()

	// Launch workers to encode slabs
	wg.Add(numWorkers)
	for range numWorkers {
		go encoder(&wg, done, jobs, results)
	}

	// Send jobs
	for _, id := range modifiedSlabIDs {
		jobs <- slabToBeEncoded{id, s.deltas[id]}
	}
	close(jobs)

	// Remove deleted slabs from underlying storage.
	for _, id := range deletedSlabIDs {

		err := s.baseStorage.Remove(id)
		if err != nil {
			// Closing done channel signals goroutines to stop.
			close(done)
			// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
		}

		// Deleted slabs are removed from deltas and added to read cache so that:
		// 1. next read is from in-memory read cache
		// 2. deleted slabs are not re-committed in next commit
		s.cache[id] = nil
		delete(s.deltas, id)
	}

	// Process encoded slabs
	for range modifiedSlabCount {
		result := <-results

		if result.err != nil {
			// Closing done channel signals goroutines to stop.
			close(done)
			// result.err is already categorized by Encode().
			return result.err
		}

		id := result.slabID
		data := result.data

		if data == nil {
			// Closing done channel signals goroutines to stop.
			close(done)
			// This is unexpected because deleted slabs are processed separately.
			return NewEncodingErrorf("unexpectd encoded empty data")
		}

		// Store
		err := s.baseStorage.Store(id, data)
		if err != nil {
			// Closing done channel signals goroutines to stop.
			close(done)
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

func (s *PersistentSlabStorage) RetrieveIgnoringDeltas(id SlabID, cache bool) (Slab, bool, error) {

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
	if cache {
		s.cache[id] = slab
	}

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
	return s.RetrieveIgnoringDeltas(id, true)
}

func (s *PersistentSlabStorage) Store(id SlabID, slab Slab) error {
	if id == SlabIDUndefined {
		return NewSlabIDError("failed to store slab with undefined slab ID")
	}
	// add to deltas
	s.deltas[id] = slab
	return nil
}

func (s *PersistentSlabStorage) Remove(id SlabID) error {
	if id == SlabIDUndefined {
		return NewSlabIDError("failed to remove slab with undefined slab ID")
	}
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

// FixLoadedBrokenReferences traverses loaded slabs and fixes broken references in maps.
// A broken reference is a SlabID referencing a non-existent slab.
// To fix a map containing broken references, this function replaces broken map with
// empty map having the same SlabID and also removes all slabs in the old map.
// Limitations:
// - only fix broken references in map
// - only traverse loaded slabs in deltas and cache
// NOTE: The intended use case is to enable migration programs in onflow/flow-go to
// fix broken references.  As of April 2024, only 10 registers in testnet (not mainnet)
// were found to have broken references and they seem to have resulted from a bug
// that was fixed 2 years ago by https://github.com/onflow/cadence/pull/1565.
func (s *PersistentSlabStorage) FixLoadedBrokenReferences(needToFix func(old Value) bool) (
	fixedSlabIDs map[SlabID][]SlabID, // key: root slab ID, value: slab IDs containing broken refs
	skippedSlabIDs map[SlabID][]SlabID, // key: root slab ID, value: slab IDs containing broken refs
	err error,
) {

	// parentOf is used to find root slab from non-root slab.
	// Broken reference can be in non-root slab, and we need SlabID of root slab
	// to replace broken map by creating an empty new map with same SlabID.
	parentOf := make(map[SlabID]SlabID)

	getRootSlabID := func(id SlabID) SlabID {
		for {
			parentID, ok := parentOf[id]
			if ok {
				id = parentID
			} else {
				return id
			}
		}
	}

	hasBrokenReferenceInSlab := func(id SlabID, slab Slab) bool {
		if slab == nil {
			return false
		}

		switch slab.(type) {
		case *ArrayMetaDataSlab, *MapMetaDataSlab: // metadata slabs
			var foundBrokenRef bool

			for _, childStorable := range slab.ChildStorables() {

				if slabIDStorable, ok := childStorable.(SlabIDStorable); ok {

					childID := SlabID(slabIDStorable)

					// Track parent-child relationship of root slabs and non-root slabs.
					parentOf[childID] = id

					if !s.existIfLoaded(childID) {
						foundBrokenRef = true
					}

					// Continue with remaining child storables to track parent-child relationship.
				}
			}

			return foundBrokenRef

		default: // data slabs
			childStorables := slab.ChildStorables()

			for len(childStorables) > 0 {

				var nextChildStorables []Storable

				for _, childStorable := range childStorables {

					if slabIDStorable, ok := childStorable.(SlabIDStorable); ok {

						if !s.existIfLoaded(SlabID(slabIDStorable)) {
							return true
						}

						continue
					}

					// Append child storables of this childStorable to
					// handle nested SlabIDStorable, such as Cadence SomeValue.
					nextChildStorables = append(
						nextChildStorables,
						childStorable.ChildStorables()...,
					)
				}

				childStorables = nextChildStorables
			}

			return false
		}
	}

	var brokenSlabIDs []SlabID

	// Iterate delta slabs.
	for id, slab := range s.deltas {
		if hasBrokenReferenceInSlab(id, slab) {
			brokenSlabIDs = append(brokenSlabIDs, id)
		}
	}

	// Iterate cache slabs.
	for id, slab := range s.cache {
		if _, ok := s.deltas[id]; ok {
			continue
		}
		if hasBrokenReferenceInSlab(id, slab) {
			brokenSlabIDs = append(brokenSlabIDs, id)
		}
	}

	if len(brokenSlabIDs) == 0 {
		return nil, nil, nil
	}

	rootSlabIDsWithBrokenData := make(map[SlabID][]SlabID)
	var errs []error

	// Find SlabIDs of root slab for slabs containing broken references.
	for _, id := range brokenSlabIDs {
		rootID := getRootSlabID(id)
		if rootID == SlabIDUndefined {
			errs = append(errs, fmt.Errorf("failed to get root slab id for slab %s", id))
			continue
		}
		rootSlabIDsWithBrokenData[rootID] = append(rootSlabIDsWithBrokenData[rootID], id)
	}

	for rootSlabID, brokenSlabIDs := range rootSlabIDsWithBrokenData {
		rootSlab := s.RetrieveIfLoaded(rootSlabID)
		if rootSlab == nil {
			errs = append(errs, fmt.Errorf("failed to retrieve loaded root slab %s", rootSlabID))
			continue
		}

		switch rootSlab := rootSlab.(type) {
		case MapSlab:
			value, err := rootSlab.StoredValue(s)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to convert slab %s into value", rootSlab.SlabID()))
				continue
			}

			if needToFix(value) {
				err := s.fixBrokenReferencesInMap(rootSlab)
				if err != nil {
					errs = append(errs, err)
					continue
				}
			} else {
				if skippedSlabIDs == nil {
					skippedSlabIDs = make(map[SlabID][]SlabID)
				}
				skippedSlabIDs[rootSlabID] = brokenSlabIDs
			}

		default:
			// IMPORTANT: Only handle map slabs for now.  DO NOT silently fix currently unknown problems.
			errs = append(errs, fmt.Errorf("failed to fix broken references in non-map slab %s (%T)", rootSlab.SlabID(), rootSlab))
		}
	}

	for id := range skippedSlabIDs {
		delete(rootSlabIDsWithBrokenData, id)
	}

	return rootSlabIDsWithBrokenData, skippedSlabIDs, errors.Join(errs...)
}

// fixBrokenReferencesInMap replaces replaces broken map with empty map
// having the same SlabID and also removes all slabs in the old map.
func (s *PersistentSlabStorage) fixBrokenReferencesInMap(old MapSlab) error {
	id := old.SlabID()

	oldExtraData := old.ExtraData()

	// Create an empty map with the same StorgeID, type, and seed as the old map.
	new := &MapDataSlab{
		header: MapSlabHeader{
			slabID: id,
			size:   mapRootDataSlabPrefixSize + hkeyElementsPrefixSize,
		},
		extraData: &MapExtraData{
			TypeInfo: oldExtraData.TypeInfo,
			Seed:     oldExtraData.Seed,
		},
		elements: newHkeyElements(0),
	}

	// Store new empty map with the same SlabID.
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

func (s *PersistentSlabStorage) existIfLoaded(id SlabID) bool {
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

// GetAllChildReferences returns child references of given slab (all levels),
// including nested container and theirs child references.
func (s *PersistentSlabStorage) GetAllChildReferences(id SlabID) (
	references []SlabID,
	brokenReferences []SlabID,
	err error,
) {
	slab, found, err := s.Retrieve(id)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, NewSlabNotFoundErrorf(id, "failed to get root slab by id %s", id)
	}
	return s.getAllChildReferences(slab)
}

// getAllChildReferences returns child references of given slab (all levels).
func (s *PersistentSlabStorage) getAllChildReferences(slab Slab) (
	references []SlabID,
	brokenReferences []SlabID,
	err error,
) {
	childStorables := slab.ChildStorables()

	for len(childStorables) > 0 {

		var nextChildStorables []Storable

		for _, childStorable := range childStorables {

			slabIDStorable, ok := childStorable.(SlabIDStorable)
			if !ok {
				nextChildStorables = append(
					nextChildStorables,
					childStorable.ChildStorables()...,
				)

				continue
			}

			childID := SlabID(slabIDStorable)

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

// BatchPreload decodeds and caches slabs of given ids in parallel.
// This is useful for storage health or data validation in migration programs.
func (s *PersistentSlabStorage) BatchPreload(ids []SlabID, numWorkers int) error {
	if len(ids) == 0 {
		return nil
	}

	// Use 11 for min slab count for parallel decoding because micro benchmarks showed
	// performance regression for <= 10 slabs when decoding slabs in parallel.
	const minCountForBatchPreload = 11
	if len(ids) < minCountForBatchPreload {

		for _, id := range ids {
			// fetch from base storage last
			data, ok, err := s.baseStorage.Retrieve(id)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
			}
			if !ok {
				continue
			}

			slab, err := DecodeSlab(id, data, s.cborDecMode, s.DecodeStorable, s.DecodeTypeInfo)
			if err != nil {
				// err is already categorized by DecodeSlab().
				return err
			}

			// save decoded slab to cache
			s.cache[id] = slab
		}

		return nil
	}

	type slabToBeDecoded struct {
		slabID SlabID
		data   []byte
	}

	type decodedSlab struct {
		slabID SlabID
		slab   Slab
		err    error
	}

	// Define decoder (worker) to decode slabs in parallel
	decoder := func(wg *sync.WaitGroup, done <-chan struct{}, jobs <-chan slabToBeDecoded, results chan<- decodedSlab) {
		defer wg.Done()

		for slabData := range jobs {
			// Check if goroutine is signaled to stop before proceeding.
			select {
			case <-done:
				return
			default:
			}

			id := slabData.slabID
			data := slabData.data

			slab, err := DecodeSlab(id, data, s.cborDecMode, s.DecodeStorable, s.DecodeTypeInfo)
			// err is already categorized by DecodeSlab().
			results <- decodedSlab{
				slabID: id,
				slab:   slab,
				err:    err,
			}
		}
	}

	if numWorkers > len(ids) {
		numWorkers = len(ids)
	}

	var wg sync.WaitGroup

	// Construct done signal channel
	done := make(chan struct{})

	// Construct job queue
	jobs := make(chan slabToBeDecoded, len(ids))

	// Construct result queue
	results := make(chan decodedSlab, len(ids))

	defer func() {
		// This ensures that all goroutines are stopped before output channel is closed.

		// Wait for all goroutines to finish
		wg.Wait()

		// Close output channel
		close(results)
	}()

	// Preallocate cache map if empty
	if len(s.cache) == 0 {
		s.cache = make(map[SlabID]Slab, len(ids))
	}

	// Launch workers
	wg.Add(numWorkers)
	for range numWorkers {
		go decoder(&wg, done, jobs, results)
	}

	// Send jobs
	jobCount := 0
	{
		// Need to close input channel (jobs) here because
		// if there isn't any job in jobs channel,
		// done is never processed inside loop "for slabData := range jobs".
		defer close(jobs)

		for _, id := range ids {
			// fetch from base storage last
			data, ok, err := s.baseStorage.Retrieve(id)
			if err != nil {
				// Closing done channel signals goroutines to stop.
				close(done)
				// Wrap err as external error (if needed) because err is returned by BaseStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
			}
			if !ok {
				continue
			}

			jobs <- slabToBeDecoded{id, data}
			jobCount++
		}
	}

	// Process results
	for range jobCount {
		result := <-results

		if result.err != nil {
			// Closing done channel signals goroutines to stop.
			close(done)
			// result.err is already categorized by DecodeSlab().
			return result.err
		}

		// save decoded slab to cache
		s.cache[result.slabID] = result.slab
	}

	return nil
}

// HasUnsavedChanges returns true if there are any modified and unsaved slabs in storage with given address.
func (s *PersistentSlabStorage) HasUnsavedChanges(address Address) bool {
	for k := range s.deltas {
		if k.address == address {
			return true
		}
	}
	return false
}

func (s *PersistentSlabStorage) getBaseStorage() BaseStorage {
	return s.baseStorage
}

func (s *PersistentSlabStorage) getCache() map[SlabID]Slab {
	return s.cache
}

func (s *PersistentSlabStorage) getDeltas() map[SlabID]Slab {
	return s.deltas
}

func (s *PersistentSlabStorage) getCBOREncMode() cbor.EncMode {
	return s.cborEncMode
}

func (s *PersistentSlabStorage) getCBORDecMode() cbor.DecMode {
	return s.cborDecMode
}

func storeSlab(storage SlabStorage, slab Slab) error {
	id := slab.SlabID()
	err := storage.Store(id, slab)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
	}
	return nil
}
