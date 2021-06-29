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
