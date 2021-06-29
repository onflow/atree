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
