package main

type StorageID uint64

const StorageIDUndefined = StorageID(0)

var storageIDCounter uint64

func generateStorageID() StorageID {
	storageIDCounter++
	return StorageID(storageIDCounter)
}

type Storage interface {
	Store(StorageID, Slab) error
	Retrieve(StorageID) (Slab, bool, error)
	Remove(StorageID)
}

type BasicStorage struct {
	slabs map[StorageID]Slab
}

func NewBasicStorage() *BasicStorage {
	return &BasicStorage{slabs: make(map[StorageID]Slab)}
}

func (s *BasicStorage) Retrieve(id StorageID) (Slab, bool, error) {
	slab, ok := s.slabs[id]
	return slab, ok, nil
}

func (s *BasicStorage) Store(id StorageID, slab Slab) error {
	s.slabs[id] = slab
	return nil
}

func (s *BasicStorage) Remove(id StorageID) {
	delete(s.slabs, id)
}
