/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

type (
	Address      [8]byte
	StorageIndex [8]byte

	StorageID struct {
		Address Address
		Index   StorageIndex
	}
)

func (id StorageID) String() string {
	return fmt.Sprintf(
		"0x%x.%d",
		binary.BigEndian.Uint64(id.Address[:]),
		binary.BigEndian.Uint64(id.Index[:]),
	)
}

var (
	AddressUndefined      = Address{}
	StorageIndexUndefined = StorageIndex{}
	StorageIDUndefined    = StorageID{}
)

var (
	ErrStorageID    = errors.New("invalid storage id")
	ErrStorageIndex = errors.New("invalid storage index")
)

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
		return StorageID{}, fmt.Errorf("invalid storage id length %d", len(b))
	}

	var address Address
	copy(address[:], b)

	var index StorageIndex
	copy(index[:], b[8:])

	return StorageID{address, index}, nil
}

func (id StorageID) ToRawBytes(b []byte) (int, error) {
	if len(b) < storageIDSize {
		return 0, fmt.Errorf("storage id raw buffer is too short")
	}
	copy(b, id.Address[:])
	copy(b[8:], id.Index[:])
	return storageIDSize, nil
}

func (id StorageID) Valid() error {
	if id == StorageIDUndefined {
		return ErrStorageID
	}
	if id.Index == StorageIndexUndefined {
		return ErrStorageIndex
	}
	return nil
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
	SegmentCounts() int // number of segments stored in the storage
	Size() int          // total byte size stored
	BaseStorageUsageReporter
}

type InMemBaseStorage struct {
	segments         map[StorageID][]byte
	bytesRetrieved   int
	bytesStored      int
	segmentsReturned map[StorageID]struct{}
	segmentsUpdated  map[StorageID]struct{}
	segmentsTouched  map[StorageID]struct{}
}

func NewInMemBaseStorage() *InMemBaseStorage {
	return NewInMemBaseStorageFromMap(
		make(map[StorageID][]byte),
	)
}

func NewInMemBaseStorageFromMap(segments map[StorageID][]byte) *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:         segments,
		segmentsReturned: make(map[StorageID]struct{}),
		segmentsUpdated:  make(map[StorageID]struct{}),
		segmentsTouched:  make(map[StorageID]struct{}),
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

func (s *InMemBaseStorage) Remove(id StorageID) error {
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	delete(s.segments, id)
	return nil
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
	s.segmentsReturned = make(map[StorageID]struct{})
	s.segmentsUpdated = make(map[StorageID]struct{})
	s.segmentsTouched = make(map[StorageID]struct{})
}

type Ledger interface {
	GetValue(owner, key []byte) (value []byte, err error)
	SetValue(owner, key, value []byte) (err error)
}

type LedgerBaseStorage struct {
	ledger         Ledger
	bytesRetrieved int
	bytesStored    int
}

func NewLedgerBaseStorage(ledger Ledger) *LedgerBaseStorage {
	return &LedgerBaseStorage{
		ledger:         ledger,
		bytesRetrieved: 0,
		bytesStored:    0,
	}
}

func (s *LedgerBaseStorage) Retrieve(id StorageID) ([]byte, bool, error) {
	v, err := s.ledger.GetValue(id.Address[:], id.Index[:])
	s.bytesRetrieved += len(v)
	return v, len(v) > 0, err
}

func (s *LedgerBaseStorage) Store(id StorageID, data []byte) error {
	s.bytesStored += len(data)
	return s.ledger.SetValue(id.Address[:], id.Index[:], data)
}

func (s *LedgerBaseStorage) Remove(id StorageID) error {
	return s.ledger.SetValue(id.Address[:], id.Index[:], nil)
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

type SlabStorage interface {
	Store(StorageID, Slab) error
	Retrieve(StorageID) (Slab, bool, error)
	Remove(StorageID) error

	Count() int
	GenerateStorageID(address Address) StorageID
}

type BasicSlabStorage struct {
	Slabs          map[StorageID]Slab
	storageIndex   map[Address]StorageIndex
	DecodeStorable StorableDecoder
	cborEncMode    cbor.EncMode
	cborDecMode    cbor.DecMode
}

var _ SlabStorage = &BasicSlabStorage{}

func NewBasicSlabStorage(cborEncMode cbor.EncMode, cborDecMode cbor.DecMode) *BasicSlabStorage {
	return &BasicSlabStorage{
		Slabs:        make(map[StorageID]Slab),
		storageIndex: make(map[Address]StorageIndex),
		cborEncMode:  cborEncMode,
		cborDecMode:  cborDecMode,
	}
}

func (s *BasicSlabStorage) GenerateStorageID(address Address) StorageID {
	index := s.storageIndex[address]
	nextIndex := index.Next()

	s.storageIndex[address] = nextIndex
	return NewStorageID(address, nextIndex)
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
		slab, err := decodeSlab(id, data, s.cborDecMode, s.DecodeStorable)
		if err != nil {
			return err
		}
		s.Slabs[id] = slab
	}
	return nil
}

type PersistentSlabStorage struct {
	baseStorage    BaseStorage
	cache          map[StorageID]Slab
	deltas         map[StorageID]Slab
	storageIndex   map[Address]StorageIndex
	DecodeStorable StorableDecoder
	cborEncMode    cbor.EncMode
	cborDecMode    cbor.DecMode
	autoCommit     bool // flag to call commit after each operation
}

var _ SlabStorage = &PersistentSlabStorage{}

type StorageOption func(st *PersistentSlabStorage) *PersistentSlabStorage

func NewPersistentSlabStorage(
	base BaseStorage,
	cborEncMode cbor.EncMode,
	cborDecMode cbor.DecMode,
	opts ...StorageOption,
) *PersistentSlabStorage {
	storage := &PersistentSlabStorage{baseStorage: base,
		cache:        make(map[StorageID]Slab),
		deltas:       make(map[StorageID]Slab),
		storageIndex: make(map[Address]StorageIndex),
		cborEncMode:  cborEncMode,
		cborDecMode:  cborDecMode,
		autoCommit:   true,
	}

	for _, applyOption := range opts {
		storage = applyOption(storage)
	}

	return storage
}

// WithNoAutoCommit sets the autocommit functionality off
func WithNoAutoCommit() StorageOption {
	return func(st *PersistentSlabStorage) *PersistentSlabStorage {
		st.autoCommit = false
		return st
	}
}

func (s *PersistentSlabStorage) GenerateStorageID(address Address) StorageID {
	index := s.storageIndex[address]
	nextIndex := index.Next()

	s.storageIndex[address] = nextIndex
	return NewStorageID(address, nextIndex)
}

func (s *PersistentSlabStorage) Commit() error {
	for id, slab := range s.deltas {
		if id.Address != AddressUndefined {
			// deleted slabs
			if slab == nil {
				s.baseStorage.Remove(id)
				continue
			}

			// serialize
			data, err := Encode(slab, s.cborEncMode)
			if err != nil {
				return err
			}

			// store
			err = s.baseStorage.Store(id, data)
			if err != nil {
				return err
			}

			// add to read cache
			s.cache[id] = slab
		}
	}
	// reset deltas
	s.deltas = make(map[StorageID]Slab)
	return nil
}

func (s *PersistentSlabStorage) DropDeltas() {
	s.deltas = make(map[StorageID]Slab)
}

func (s *PersistentSlabStorage) DropCache() {
	s.cache = make(map[StorageID]Slab)
}

func (s *PersistentSlabStorage) Retrieve(id StorageID) (Slab, bool, error) {
	var slab Slab

	// check deltas first
	if slab, ok := s.deltas[id]; ok {
		return slab, slab != nil, nil
	}

	// check the read cache next
	if slab, ok := s.cache[id]; ok {
		return slab, true, nil
	}

	// fetch from base storage last
	data, ok, err := s.baseStorage.Retrieve(id)
	if err != nil {
		return nil, false, err
	}
	slab, err = decodeSlab(id, data, s.cborDecMode, s.DecodeStorable)
	if err == nil {
		// save decoded slab to cache
		s.cache[id] = slab
	}
	return slab, ok, err
}

func (s *PersistentSlabStorage) Store(id StorageID, slab Slab) error {
	if s.autoCommit {
		data, err := Encode(slab, s.cborEncMode)
		if err != nil {
			return err
		}
		err = s.baseStorage.Store(id, data)
		if err != nil {
			return err
		}
		s.cache[id] = slab
		return nil
	}

	// add to deltas
	s.deltas[id] = slab
	return nil
}

func (s *PersistentSlabStorage) Remove(id StorageID) error {
	if s.autoCommit {
		s.baseStorage.Remove(id)
	}

	// add to nil to deltas under that id
	s.deltas[id] = nil
	return nil
}

// Warning Counts doesn't consider new segments in the deltas and only returns commited values
func (s *PersistentSlabStorage) Count() int {
	return s.baseStorage.SegmentCounts()
}
