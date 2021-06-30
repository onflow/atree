/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

type StorageID uint64

const StorageIDUndefined = StorageID(0)

var storageIDCounter uint64

func generateStorageID() StorageID {
	storageIDCounter++
	return StorageID(storageIDCounter)
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
	Remove(StorageID)
	SegmentCounts() int // number of segments stored in the storage
	Size() int          // total byte size stored
	BaseStorageUsageReporter
}

type InMemBaseStorage struct {
	segments         map[StorageID][]byte
	bytesRetrieved   int
	bytesStored      int
	segmentsReturned map[StorageID]interface{}
	segmentsUpdated  map[StorageID]interface{}
	segmentsTouched  map[StorageID]interface{}
}

func NewInMemBaseStorage() *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:         make(map[StorageID][]byte),
		segmentsReturned: make(map[StorageID]interface{}),
		segmentsUpdated:  make(map[StorageID]interface{}),
		segmentsTouched:  make(map[StorageID]interface{}),
	}
}

func NewInMemBaseStorageFromMap(segments map[StorageID][]byte) *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:         segments,
		segmentsReturned: make(map[StorageID]interface{}),
		segmentsUpdated:  make(map[StorageID]interface{}),
		segmentsTouched:  make(map[StorageID]interface{}),
	}
}

func (s *InMemBaseStorage) Retrieve(id StorageID) ([]byte, bool, error) {
	seg, ok := s.segments[id]
	s.bytesRetrieved += len(seg)
	s.segmentsReturned[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return seg, ok, nil
}

func (s *InMemBaseStorage) Store(id StorageID, data []byte) error {
	s.segments[id] = data
	s.bytesStored += len(data)
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return nil
}

func (s *InMemBaseStorage) Remove(id StorageID) {
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	delete(s.segments, id)
}

func (s *InMemBaseStorage) SegmentCounts() int {
	return len(s.segments)
}

func (s *InMemBaseStorage) Size() int {
	total := 0
	for _, seg := range s.segments {
		total += len(seg)
	}
	return total
}

func (s *InMemBaseStorage) BytesRetrieved() int {
	return s.bytesRetrieved
}

func (s *InMemBaseStorage) BytesStored() int {
	return s.bytesStored
}

func (s *InMemBaseStorage) SegmentsReturned() int {
	return len(s.segmentsReturned)
}

func (s *InMemBaseStorage) SegmentsUpdated() int {
	return len(s.segmentsUpdated)
}

func (s *InMemBaseStorage) SegmentsTouched() int {
	return len(s.segmentsTouched)
}

func (s *InMemBaseStorage) ResetReporter() {
	s.bytesStored = 0
	s.bytesRetrieved = 0
	s.segmentsReturned = make(map[StorageID]interface{})
	s.segmentsUpdated = make(map[StorageID]interface{})
	s.segmentsTouched = make(map[StorageID]interface{})
}

type SlabStorage interface {
	Store(StorageID, Slab) error
	Retrieve(StorageID) (Slab, bool, error)
	Remove(StorageID)

	Count() int
}

type BasicSlabStorage struct {
	slabs map[StorageID]Slab
}

func NewBasicSlabStorage() *BasicSlabStorage {
	return &BasicSlabStorage{slabs: make(map[StorageID]Slab)}
}

func (s *BasicSlabStorage) Retrieve(id StorageID) (Slab, bool, error) {
	slab, ok := s.slabs[id]
	return slab, ok, nil
}

func (s *BasicSlabStorage) Store(id StorageID, slab Slab) error {
	s.slabs[id] = slab
	return nil
}

func (s *BasicSlabStorage) Remove(id StorageID) {
	delete(s.slabs, id)
}

func (s *BasicSlabStorage) Count() int {
	return len(s.slabs)
}

// Encode returns serialized slabs in storage.
// This is currently used for testing.
func (s *BasicSlabStorage) Encode() (map[StorageID][]byte, error) {
	m := make(map[StorageID][]byte)
	for id, slab := range s.slabs {
		b, err := slab.Bytes()
		if err != nil {
			return nil, err
		}
		m[id] = b
	}
	return m, nil
}

// Load deserializes encoded slabs and stores in storage.
// This is currently used for testing.
func (s *BasicSlabStorage) Load(m map[StorageID][]byte) error {
	for id, data := range m {
		slab, err := decodeSlab(id, data)
		if err != nil {
			return err
		}
		s.slabs[id] = slab
	}
	return nil
}

type PersistentSlabStorage struct {
	baseStorage BaseStorage
}

func NewPersistentSlabStorage(base BaseStorage) *PersistentSlabStorage {
	return &PersistentSlabStorage{baseStorage: base}
}

func (s *PersistentSlabStorage) Retrieve(id StorageID) (Slab, bool, error) {
	var slab Slab
	data, ok, err := s.baseStorage.Retrieve(id)
	if err != nil {
		return nil, false, err
	}
	slab, err = decodeSlab(id, data)
	return slab, ok, err
}

func (s *PersistentSlabStorage) Store(id StorageID, slab Slab) error {
	data, err := slab.Bytes()
	if err != nil {
		return err
	}
	return s.baseStorage.Store(id, data)
}

func (s *PersistentSlabStorage) Remove(id StorageID) {
	s.baseStorage.Remove(id)
}

func (s *PersistentSlabStorage) Count() int {
	return s.baseStorage.SegmentCounts()
}
