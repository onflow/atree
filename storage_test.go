/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"math/rand"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

func TestPersistentStorage(t *testing.T) {

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	t.Run("TestTempAllocation", func(t *testing.T) {

		baseStorage := NewInMemBaseStorage()
		storage := NewPersistentSlabStorage(baseStorage, encMode, decMode)

		tempStorageID, err := NewStorageIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		permStorageID, err := NewStorageIDFromRawBytes([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		slab1 := &ArrayMetaDataSlab{childrenHeaders: []ArraySlabHeader{{size: uint32(100), count: uint32(1)}}}
		slab2 := &ArrayMetaDataSlab{childrenHeaders: []ArraySlabHeader{{size: uint32(100), count: uint32(2)}}}

		// no temp ids should be in the base storage
		err = storage.Store(tempStorageID, slab1)
		require.NoError(t, err)

		err = storage.Store(permStorageID, slab2)
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		_, found, err := baseStorage.Retrieve(tempStorageID)
		require.NoError(t, err)
		require.False(t, found)

		_, found, err = baseStorage.Retrieve(permStorageID)
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("Test commit functionality", func(t *testing.T) {
		numberOfAccounts := 100
		numberOfSlabsPerAccount := 10

		baseStorage := newAccessOrderTrackerBaseStorage()
		storage := NewPersistentSlabStorage(baseStorage, encMode, decMode)
		baseStorage2 := newAccessOrderTrackerBaseStorage()
		storageWithFastCommit := NewPersistentSlabStorage(baseStorage2, encMode, decMode)

		simpleMap := make(map[StorageID][]byte)
		// test random updates apply commit and check the order of commited values
		for i := 0; i < numberOfAccounts; i++ {
			for j := 0; j < numberOfSlabsPerAccount; j++ {
				addr := generateRandomAddress()
				slab := generateRandomSlab()

				storageID, err := storage.GenerateStorageID(addr)
				require.NoError(t, err)
				err = storage.Store(storageID, slab)
				require.NoError(t, err)

				storageID2, err := storageWithFastCommit.GenerateStorageID(addr)
				require.NoError(t, err)
				err = storageWithFastCommit.Store(storageID2, slab)
				require.NoError(t, err)

				// capture data for accuracy testing
				simpleMap[storageID], err = Encode(slab, encMode)
				require.NoError(t, err)
			}
		}

		err = storage.Commit()
		require.NoError(t, err)

		err = storageWithFastCommit.FastCommit(10)
		require.NoError(t, err)

		// check update functionality
		for sid, value := range simpleMap {
			storedValue, found, err := baseStorage.Retrieve(sid)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, value, storedValue)

			storedValue, found, err = baseStorage2.Retrieve(sid)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, value, storedValue)
		}

		// compare orders
		require.Equal(t, baseStorage.SegTouchOrder(), baseStorage2.SegTouchOrder())
	})
}

func generateRandomSlab() Slab {
	return &ArrayMetaDataSlab{childrenHeaders: []ArraySlabHeader{{size: rand.Uint32(), count: rand.Uint32()}}}
}

func generateRandomAddress() Address {
	address := Address{}
	rand.Read(address[:])
	return address
}

type accessOrderTrackerBaseStorage struct {
	segTouchOrder []StorageID
	indexReqOrder []Address
	segments      map[StorageID][]byte
	storageIndex  map[Address]StorageIndex
}

func newAccessOrderTrackerBaseStorage() *accessOrderTrackerBaseStorage {
	return &accessOrderTrackerBaseStorage{
		segTouchOrder: make([]StorageID, 0),
		indexReqOrder: make([]Address, 0),
		segments:      make(map[StorageID][]byte),
		storageIndex:  make(map[Address]StorageIndex),
	}
}

func (s *accessOrderTrackerBaseStorage) SegTouchOrder() []StorageID {
	return s.segTouchOrder
}

func (s *accessOrderTrackerBaseStorage) Retrieve(id StorageID) ([]byte, bool, error) {
	s.segTouchOrder = append(s.segTouchOrder, id)
	seg, ok := s.segments[id]
	return seg, ok, nil
}

func (s *accessOrderTrackerBaseStorage) Store(id StorageID, data []byte) error {
	s.segTouchOrder = append(s.segTouchOrder, id)
	s.segments[id] = data
	return nil
}

func (s *accessOrderTrackerBaseStorage) Remove(id StorageID) error {
	s.segTouchOrder = append(s.segTouchOrder, id)
	delete(s.segments, id)
	return nil
}

func (s *accessOrderTrackerBaseStorage) GenerateStorageID(address Address) (StorageID, error) {
	s.indexReqOrder = append(s.indexReqOrder, address)

	index := s.storageIndex[address]
	nextIndex := index.Next()

	s.storageIndex[address] = nextIndex
	return NewStorageID(address, nextIndex), nil
}

func (s *accessOrderTrackerBaseStorage) SegmentCounts() int { return 0 }

func (s *accessOrderTrackerBaseStorage) Size() int { return 0 }

func (s *accessOrderTrackerBaseStorage) BytesRetrieved() int { return 0 }

func (s *accessOrderTrackerBaseStorage) BytesStored() int { return 0 }

func (s *accessOrderTrackerBaseStorage) SegmentsReturned() int { return 0 }

func (s *accessOrderTrackerBaseStorage) SegmentsUpdated() int { return 0 }

func (s *accessOrderTrackerBaseStorage) SegmentsTouched() int { return 0 }

func (s *accessOrderTrackerBaseStorage) ResetReporter() {}
