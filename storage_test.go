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
	"errors"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

func TestStorageIndexNext(t *testing.T) {
	index := StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}
	want := StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}
	require.Equal(t, want, index.Next())
}

func TestNewStorageID(t *testing.T) {
	t.Run("temp address", func(t *testing.T) {
		want := StorageID{Address: Address{}, Index: StorageIndex{1}}
		require.Equal(t, want, NewStorageID(Address{}, StorageIndex{1}))
	})
	t.Run("perm address", func(t *testing.T) {
		want := StorageID{Address: Address{1}, Index: StorageIndex{1}}
		require.Equal(t, want, NewStorageID(Address{1}, StorageIndex{1}))
	})
}

func TestNewStorageIDFromRawBytes(t *testing.T) {
	t.Run("data length < storage id size", func(t *testing.T) {
		var fatalError *FatalError
		var storageIDError *StorageIDError

		id, err := NewStorageIDFromRawBytes(nil)
		require.Equal(t, StorageIDUndefined, id)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &storageIDError)
		require.ErrorAs(t, fatalError, &storageIDError)

		id, err = NewStorageIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2})
		require.Equal(t, StorageIDUndefined, id)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &storageIDError)
		require.ErrorAs(t, fatalError, &storageIDError)
	})

	t.Run("data length == storage id size", func(t *testing.T) {
		id, err := NewStorageIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2})

		want := StorageID{
			Address: Address{0, 0, 0, 0, 0, 0, 0, 1},
			Index:   StorageIndex{0, 0, 0, 0, 0, 0, 0, 2},
		}
		require.Equal(t, want, id)
		require.NoError(t, err)
	})
	t.Run("data length > storage id size", func(t *testing.T) {
		id, err := NewStorageIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2, 3, 4, 5, 6, 7, 8})

		want := StorageID{
			Address: Address{0, 0, 0, 0, 0, 0, 0, 1},
			Index:   StorageIndex{0, 0, 0, 0, 0, 0, 0, 2},
		}
		require.Equal(t, want, id)
		require.NoError(t, err)
	})
}

func TestStorageIDToRawBytes(t *testing.T) {
	t.Run("buffer nil", func(t *testing.T) {
		var fatalError *FatalError
		var storageIDError *StorageIDError

		size, err := StorageIDUndefined.ToRawBytes(nil)
		require.Equal(t, 0, size)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &storageIDError)
		require.ErrorAs(t, fatalError, &storageIDError)
	})

	t.Run("buffer too short", func(t *testing.T) {
		var fatalError *FatalError
		var storageIDError *StorageIDError

		b := make([]byte, 8)
		size, err := StorageIDUndefined.ToRawBytes(b)
		require.Equal(t, 0, size)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &storageIDError)
		require.ErrorAs(t, fatalError, &storageIDError)
	})

	t.Run("undefined", func(t *testing.T) {
		b := make([]byte, storageIDSize)
		size, err := StorageIDUndefined.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		require.Equal(t, want, b)
		require.Equal(t, storageIDSize, size)
	})

	t.Run("temp address", func(t *testing.T) {
		id := NewStorageID(Address{0, 0, 0, 0, 0, 0, 0, 0}, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1})
		b := make([]byte, storageIDSize)
		size, err := id.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
		require.Equal(t, want, b)
		require.Equal(t, storageIDSize, size)
	})

	t.Run("temp index", func(t *testing.T) {
		id := NewStorageID(Address{0, 0, 0, 0, 0, 0, 0, 1}, StorageIndex{0, 0, 0, 0, 0, 0, 0, 0})
		b := make([]byte, storageIDSize)
		size, err := id.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}
		require.Equal(t, want, b)
		require.Equal(t, storageIDSize, size)
	})

	t.Run("perm", func(t *testing.T) {
		id := NewStorageID(Address{0, 0, 0, 0, 0, 0, 0, 1}, StorageIndex{0, 0, 0, 0, 0, 0, 0, 2})
		b := make([]byte, storageIDSize)
		size, err := id.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2}
		require.Equal(t, want, b)
		require.Equal(t, storageIDSize, size)
	})
}

func TestStorageIDAddressAsUint64(t *testing.T) {
	t.Run("temp", func(t *testing.T) {
		id := NewStorageID(Address{}, StorageIndex{1})
		require.Equal(t, uint64(0), id.AddressAsUint64())
	})
	t.Run("perm", func(t *testing.T) {
		id := NewStorageID(Address{0, 0, 0, 0, 0, 0, 0, 1}, StorageIndex{1})
		require.Equal(t, uint64(1), id.AddressAsUint64())
	})
}

func TestStorageIDIndexAsUint64(t *testing.T) {
	t.Run("temp", func(t *testing.T) {
		id := NewStorageID(Address{}, StorageIndex{})
		require.Equal(t, uint64(0), id.IndexAsUint64())
	})
	t.Run("perm", func(t *testing.T) {
		id := NewStorageID(Address{}, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1})
		require.Equal(t, uint64(1), id.IndexAsUint64())
	})
}

func TestStorageIDValid(t *testing.T) {
	t.Run("undefined", func(t *testing.T) {
		id := StorageIDUndefined
		err := id.Valid()

		var fatalError *FatalError
		var storageIDError *StorageIDError
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &storageIDError)
		require.ErrorAs(t, fatalError, &storageIDError)
	})
	t.Run("temp index", func(t *testing.T) {
		id := StorageID{Address: Address{1}, Index: StorageIndexUndefined}
		err := id.Valid()

		var fatalError *FatalError
		var storageIDError *StorageIDError
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &storageIDError)
		require.ErrorAs(t, fatalError, &storageIDError)
	})
	t.Run("temp address", func(t *testing.T) {
		id := StorageID{Address: AddressUndefined, Index: StorageIndex{1}}
		require.NoError(t, id.Valid())
	})
	t.Run("valid", func(t *testing.T) {
		id := StorageID{Address: Address{1}, Index: StorageIndex{2}}
		require.NoError(t, id.Valid())
	})
}

func TestStorageIDCompare(t *testing.T) {
	t.Run("same", func(t *testing.T) {
		id1 := NewStorageID(Address{1}, StorageIndex{1})
		id2 := NewStorageID(Address{1}, StorageIndex{1})
		require.Equal(t, 0, id1.Compare(id2))
		require.Equal(t, 0, id2.Compare(id1))
	})

	t.Run("different address", func(t *testing.T) {
		id1 := NewStorageID(Address{1}, StorageIndex{1})
		id2 := NewStorageID(Address{2}, StorageIndex{1})
		require.Equal(t, -1, id1.Compare(id2))
		require.Equal(t, 1, id2.Compare(id1))
	})

	t.Run("different index", func(t *testing.T) {
		id1 := NewStorageID(Address{1}, StorageIndex{1})
		id2 := NewStorageID(Address{1}, StorageIndex{2})
		require.Equal(t, -1, id1.Compare(id2))
		require.Equal(t, 1, id2.Compare(id1))
	})

	t.Run("different address and index", func(t *testing.T) {
		id1 := NewStorageID(Address{1}, StorageIndex{1})
		id2 := NewStorageID(Address{2}, StorageIndex{2})
		require.Equal(t, -1, id1.Compare(id2))
		require.Equal(t, 1, id2.Compare(id1))
	})
}

func TestLedgerBaseStorageStore(t *testing.T) {
	ledger := newTestLedger()
	baseStorage := NewLedgerBaseStorage(ledger)

	bytesStored := 0
	values := map[StorageID][]byte{
		{Address{1}, StorageIndex{1}}: {1, 2, 3},
		{Address{1}, StorageIndex{2}}: {4, 5, 6},
	}

	// Store values
	for id, value := range values {
		err := baseStorage.Store(id, value)
		bytesStored += len(value)
		require.NoError(t, err)
	}

	// Overwrite stored values
	for id := range values {
		values[id] = append(values[id], []byte{1, 2, 3}...)
		bytesStored += len(values[id])
		err := baseStorage.Store(id, values[id])
		require.NoError(t, err)
	}

	require.Equal(t, bytesStored, baseStorage.BytesStored())
	require.Equal(t, 0, baseStorage.BytesRetrieved())

	iterator := ledger.Iterator()

	count := 0
	for {
		owner, key, value := iterator()
		if owner == nil {
			break
		}
		var id StorageID
		copy(id.Address[:], owner)
		copy(id.Index[:], key[1:])

		require.True(t, LedgerKeyIsSlabKey(string(key)))
		require.Equal(t, values[id], value)

		count++
	}
	require.Equal(t, len(values), count)
}

func TestLedgerBaseStorageRetrieve(t *testing.T) {
	ledger := newTestLedger()
	baseStorage := NewLedgerBaseStorage(ledger)

	id := StorageID{Address: Address{1}, Index: StorageIndex{1}}
	value := []byte{1, 2, 3}
	bytesStored := 0
	bytesRetrieved := 0

	// Retrieve value from empty storage
	b, found, err := baseStorage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, 0, len(b))

	bytesStored += len(value)
	err = baseStorage.Store(id, value)
	require.NoError(t, err)

	// Retrieve stored value
	b, found, err = baseStorage.Retrieve(id)
	bytesRetrieved += len(b)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, b)

	// Retrieve non-existent value
	id = StorageID{Address: Address{1}, Index: StorageIndex{2}}
	b, found, err = baseStorage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, b)

	require.Equal(t, bytesStored, baseStorage.BytesStored())
	require.Equal(t, bytesRetrieved, baseStorage.BytesRetrieved())
}

func TestLedgerBaseStorageRemove(t *testing.T) {
	ledger := newTestLedger()
	baseStorage := NewLedgerBaseStorage(ledger)

	id := StorageID{Address: Address{1}, Index: StorageIndex{1}}
	value := []byte{1, 2, 3}

	// Remove value from empty storage
	err := baseStorage.Remove(id)
	require.NoError(t, err)

	err = baseStorage.Store(id, value)
	require.NoError(t, err)

	// Remove stored value
	err = baseStorage.Remove(id)
	require.NoError(t, err)

	// Remove removed value
	err = baseStorage.Remove(id)
	require.NoError(t, err)

	// Remove non-existent value
	err = baseStorage.Remove(StorageID{Address: id.Address, Index: id.Index.Next()})
	require.NoError(t, err)

	// Retrieve removed value
	slab, found, err := baseStorage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, slab)

	iterator := ledger.Iterator()

	count := 0
	for {
		owner, key, value := iterator()
		if owner == nil {
			break
		}
		var id StorageID
		copy(id.Address[:], owner)
		copy(id.Index[:], key[1:])

		require.True(t, LedgerKeyIsSlabKey(string(key)))
		require.Nil(t, value)

		count++
	}
	require.Equal(t, 2, count)
}

func TestLedgerBaseStorageGenerateStorageID(t *testing.T) {
	ledger := newTestLedger()
	baseStorage := NewLedgerBaseStorage(ledger)

	address1 := Address{1}
	address2 := Address{2}

	id, err := baseStorage.GenerateStorageID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.Address)
	require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.Index)

	id, err = baseStorage.GenerateStorageID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.Address)
	require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.Index)

	id, err = baseStorage.GenerateStorageID(address2)
	require.NoError(t, err)
	require.Equal(t, address2, id.Address)
	require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.Index)
}

func TestBasicSlabStorageStore(t *testing.T) {
	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	r := newRand(t)
	address := Address{1}
	slabs := map[StorageID]Slab{
		{address, StorageIndex{1}}: generateRandomSlab(address, r),
		{address, StorageIndex{2}}: generateRandomSlab(address, r),
	}

	// Store values
	for id, slab := range slabs {
		err := storage.Store(id, slab)
		require.NoError(t, err)
	}

	// Overwrite stored values
	for id := range slabs {
		slab := generateRandomSlab(id.Address, r)
		slabs[id] = slab
		err := storage.Store(id, slab)
		require.NoError(t, err)
	}

	require.Equal(t, len(slabs), storage.Count())

	// Retrieve slabs
	for id, want := range slabs {
		slab, found, err := storage.Retrieve(id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, want, slab)
	}
}

func TestBasicSlabStorageRetrieve(t *testing.T) {
	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	r := newRand(t)
	id := StorageID{Address{1}, StorageIndex{1}}
	slab := generateRandomSlab(id.Address, r)

	// Retrieve value from empty storage
	retrievedSlab, found, err := storage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, retrievedSlab)

	err = storage.Store(id, slab)
	require.NoError(t, err)

	// Retrieve stored value
	retrievedSlab, found, err = storage.Retrieve(id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, slab, retrievedSlab)

	// Retrieve non-existent value
	id = StorageID{Address: Address{1}, Index: StorageIndex{2}}
	retrievedSlab, found, err = storage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, retrievedSlab)
}

func TestBasicSlabStorageRemove(t *testing.T) {
	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	r := newRand(t)
	id := StorageID{Address{1}, StorageIndex{1}}
	slab := generateRandomSlab(id.Address, r)

	// Remove value from empty storage
	err := storage.Remove(id)
	require.NoError(t, err)

	err = storage.Store(id, slab)
	require.NoError(t, err)

	// Remove stored value
	err = storage.Remove(id)
	require.NoError(t, err)

	// Remove removed value
	err = storage.Remove(id)
	require.NoError(t, err)

	// Remove non-existent value
	err = storage.Remove(StorageID{Address: id.Address, Index: id.Index.Next()})
	require.NoError(t, err)

	// Retrieve removed value
	slab, found, err := storage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, slab)

	require.Equal(t, 0, storage.Count())
}

func TestBasicSlabStorageGenerateStorageID(t *testing.T) {
	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	address1 := Address{1}
	address2 := Address{2}

	id, err := storage.GenerateStorageID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.Address)
	require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.Index)

	id, err = storage.GenerateStorageID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.Address)
	require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.Index)

	id, err = storage.GenerateStorageID(address2)
	require.NoError(t, err)
	require.Equal(t, address2, id.Address)
	require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.Index)
}

func TestBasicSlabStorageStorageIDs(t *testing.T) {
	r := newRand(t)
	address := Address{1}
	index := StorageIndex{0, 0, 0, 0, 0, 0, 0, 0}
	wantIDs := map[StorageID]bool{
		{Address: address, Index: index.Next()}: true,
		{Address: address, Index: index.Next()}: true,
		{Address: address, Index: index.Next()}: true,
	}

	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	// Get storage ids from empty storgae
	ids := storage.StorageIDs()
	require.Equal(t, 0, len(ids))

	// Store values
	for id := range wantIDs {
		err := storage.Store(id, generateRandomSlab(id.Address, r))
		require.NoError(t, err)
	}

	// Get storage ids from non-empty storgae
	ids = storage.StorageIDs()
	require.Equal(t, len(wantIDs), len(ids))

	for _, id := range ids {
		require.True(t, wantIDs[id])
	}
}

func TestBasicSlabStorageSlabIterat(t *testing.T) {
	r := newRand(t)
	address := Address{1}
	index := StorageIndex{0, 0, 0, 0, 0, 0, 0, 0}

	id1 := StorageID{Address: address, Index: index.Next()}
	id2 := StorageID{Address: address, Index: index.Next()}
	id3 := StorageID{Address: address, Index: index.Next()}

	want := map[StorageID]Slab{
		id1: generateRandomSlab(id1.Address, r),
		id2: generateRandomSlab(id2.Address, r),
		id3: generateRandomSlab(id3.Address, r),
	}

	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	// Store values
	for id, slab := range want {
		err := storage.Store(id, slab)
		require.NoError(t, err)
	}

	iterator, err := storage.SlabIterator()
	require.NoError(t, err)

	count := 0
	for {
		id, slab := iterator()
		if id == StorageIDUndefined {
			break
		}
		require.NotNil(t, want[id])
		require.Equal(t, want[id], slab)
		count++
	}
	require.Equal(t, len(want), count)
}

func TestPersistentStorage(t *testing.T) {

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	t.Run("empty storage", func(t *testing.T) {
		baseStorage := NewInMemBaseStorage()
		storage := NewPersistentSlabStorage(baseStorage, encMode, decMode, nil, nil)

		tempStorageID, err := NewStorageIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		permStorageID, err := NewStorageIDFromRawBytes([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		_, found, err := storage.Retrieve(tempStorageID)
		require.NoError(t, err)
		require.False(t, found)

		_, found, err = storage.Retrieve(permStorageID)
		require.NoError(t, err)
		require.False(t, found)

		require.Equal(t, uint(0), storage.DeltasWithoutTempAddresses())
		require.Equal(t, uint(0), storage.Deltas())
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())
	})

	t.Run("temp address", func(t *testing.T) {

		r := newRand(t)

		baseStorage := NewInMemBaseStorage()
		storage := NewPersistentSlabStorage(baseStorage, encMode, decMode, nil, nil)

		tempStorageID, err := NewStorageIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		permStorageID, err := NewStorageIDFromRawBytes([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		slab1 := generateRandomSlab(tempStorageID.Address, r)
		slab2 := generateRandomSlab(permStorageID.Address, r)

		// no temp ids should be in the base storage
		err = storage.Store(tempStorageID, slab1)
		require.NoError(t, err)
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())

		err = storage.Store(permStorageID, slab2)
		require.NoError(t, err)

		require.Equal(t, uint(1), storage.DeltasWithoutTempAddresses())
		require.Equal(t, uint(2), storage.Deltas())
		require.True(t, storage.DeltasSizeWithoutTempAddresses() > 0)
		require.Equal(t, uint64(slab2.ByteSize()), storage.DeltasSizeWithoutTempAddresses())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, uint(0), storage.DeltasWithoutTempAddresses())
		require.Equal(t, uint(1), storage.Deltas())
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())

		// Slab with temp storage id is NOT persisted in base storage.
		_, found, err := baseStorage.Retrieve(tempStorageID)
		require.NoError(t, err)
		require.False(t, found)

		// Slab with temp storage id is cached in storage.
		_, found, err = storage.Retrieve(tempStorageID)
		require.NoError(t, err)
		require.True(t, found)

		// Slab with perm storage id is persisted in base storage.
		_, found, err = baseStorage.Retrieve(permStorageID)
		require.NoError(t, err)
		require.True(t, found)

		// Slab with perm storage id is cached in storage.
		_, found, err = storage.Retrieve(permStorageID)
		require.NoError(t, err)
		require.True(t, found)

		// Remove slab with perm storage id
		err = storage.Remove(permStorageID)
		require.NoError(t, err)

		// Remove slab with temp storage id
		err = storage.Remove(tempStorageID)
		require.NoError(t, err)

		require.Equal(t, uint(1), storage.DeltasWithoutTempAddresses())
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())

		err = storage.Commit()
		require.NoError(t, err)

		// Slab with perm storage id is removed from base storage.
		_, found, err = baseStorage.Retrieve(permStorageID)
		require.NoError(t, err)
		require.False(t, found)

		// Slab with perm storage id is removed from cache in storage.
		_, found, err = storage.Retrieve(permStorageID)
		require.NoError(t, err)
		require.False(t, found)

		// Slab with temp storage id is removed from cache in storage.
		_, found, err = storage.Retrieve(tempStorageID)
		require.NoError(t, err)
		require.False(t, found)

		require.Equal(t, uint(0), storage.DeltasWithoutTempAddresses())
		require.Equal(t, uint(1), storage.Deltas())
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())
	})

	t.Run("commit", func(t *testing.T) {
		numberOfAccounts := 100
		numberOfSlabsPerAccount := 10

		r := newRand(t)
		baseStorage := newAccessOrderTrackerBaseStorage()
		storage := NewPersistentSlabStorage(baseStorage, encMode, decMode, nil, nil)
		baseStorage2 := newAccessOrderTrackerBaseStorage()
		storageWithFastCommit := NewPersistentSlabStorage(baseStorage2, encMode, decMode, nil, nil)

		simpleMap := make(map[StorageID][]byte)
		slabSize := uint64(0)
		// test random updates apply commit and check the order of committed values
		for i := 0; i < numberOfAccounts; i++ {
			for j := 0; j < numberOfSlabsPerAccount; j++ {
				addr := generateRandomAddress(r)
				slab := generateRandomSlab(addr, r)
				slabSize += uint64(slab.ByteSize())

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

		require.True(t, storage.DeltasSizeWithoutTempAddresses() > 0)
		require.Equal(t, slabSize, storage.DeltasSizeWithoutTempAddresses())
		require.True(t, storageWithFastCommit.DeltasSizeWithoutTempAddresses() > 0)
		require.Equal(t, slabSize, storageWithFastCommit.DeltasSizeWithoutTempAddresses())

		err = storage.Commit()
		require.NoError(t, err)
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())

		err = storageWithFastCommit.FastCommit(10)
		require.NoError(t, err)
		require.Equal(t, uint64(0), storageWithFastCommit.DeltasSizeWithoutTempAddresses())

		require.Equal(t, len(simpleMap), storage.Count())
		require.Equal(t, len(simpleMap), storageWithFastCommit.Count())

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

		// remove all slabs from storage
		for sid := range simpleMap {
			err = storage.Remove(sid)
			require.NoError(t, err)
			require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())

			err = storageWithFastCommit.Remove(sid)
			require.NoError(t, err)
			require.Equal(t, uint64(0), storageWithFastCommit.DeltasSizeWithoutTempAddresses())
		}

		require.Equal(t, uint(len(simpleMap)), storage.DeltasWithoutTempAddresses())
		require.Equal(t, uint(len(simpleMap)), storageWithFastCommit.DeltasWithoutTempAddresses())

		err = storage.Commit()
		require.NoError(t, err)

		err = storageWithFastCommit.FastCommit(10)
		require.NoError(t, err)

		require.Equal(t, 0, storage.Count())
		require.Equal(t, 0, storageWithFastCommit.Count())
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())
		require.Equal(t, uint64(0), storageWithFastCommit.DeltasSizeWithoutTempAddresses())

		// check remove functionality
		for sid := range simpleMap {
			storedValue, found, err := storage.Retrieve(sid)
			require.NoError(t, err)
			require.False(t, found)
			require.Nil(t, storedValue)

			storedValue, found, err = storageWithFastCommit.Retrieve(sid)
			require.NoError(t, err)
			require.False(t, found)
			require.Nil(t, storedValue)
		}

		// compare orders
		require.Equal(t, baseStorage.SegTouchOrder(), baseStorage2.SegTouchOrder())
	})

	t.Run("commit with error", func(t *testing.T) {

		baseStorage := NewInMemBaseStorage()
		storage := NewPersistentSlabStorage(baseStorage, encMode, decMode, nil, nil)

		// Encoding slabWithNonStorable returns error.
		slabWithNonStorable := &ArrayDataSlab{
			header:   ArraySlabHeader{size: uint32(1), count: uint32(1)},
			elements: []Storable{nonStorable{}},
		}
		// Encoding slabWithSlowStorable takes some time which delays
		// sending encoding result to results channel.
		slabWithSlowStorable := &ArrayDataSlab{
			header:   ArraySlabHeader{size: uint32(3), count: uint32(1)},
			elements: []Storable{newSlowStorable(1)},
		}

		address := Address{1}

		id, err := storage.GenerateStorageID(address)
		require.NoError(t, err)

		err = storage.Store(id, slabWithNonStorable)
		require.NoError(t, err)

		for i := 0; i < 500; i++ {
			id, err := storage.GenerateStorageID(address)
			require.NoError(t, err)

			err = storage.Store(id, slabWithSlowStorable)
			require.NoError(t, err)
		}

		err = storage.FastCommit(2)
		require.Equal(t, 1, errorCategorizationCount(err))

		var externalError *ExternalError
		require.ErrorAs(t, err, &externalError)
		require.ErrorIs(t, err, errEncodeNonStorable)
		require.ErrorIs(t, externalError.Unwrap(), errEncodeNonStorable)
	})
}

func TestPersistentStorageSlabIterator(t *testing.T) {
	t.Run("empty storage", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		iterator, err := storage.SlabIterator()
		require.NoError(t, err)

		count := 0
		for {
			id, _ := iterator()
			if id == StorageIDUndefined {
				break
			}
			count++
		}
		require.Equal(t, 0, count)
	})

	t.Run("not-empty storage", func(t *testing.T) {

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		id1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		id4 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		data := map[StorageID][]byte{
			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:270 count:11} ]
			id1: {
				// extra data
				// version
				0x00,
				// extra data flag
				0x81,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// version
				0x00,
				// array meta data slab flag
				0x81,
				// child header count
				0x00, 0x02,
				// child header 1 (storage id, count, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x09,
				0x00, 0x00, 0x00, 0xe4,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x01, 0x0e,
			},

			// (data slab) next: 3, data: [aaaaaaaaaaaaaaaaaaaaaa ... aaaaaaaaaaaaaaaaaaaaaa]
			id2: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x09,
				// CBOR encoded array elements
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			},

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... StorageID(...)]
			id3: {
				// version
				0x00,
				// array data slab flag
				0x40,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0b,
				// CBOR encoded array elements
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
			},

			// (data slab) next: 0, data: [0]
			id4: {
				// extra data
				// version
				0x00,
				// extra data flag
				0x80,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2b,

				// version
				0x00,
				// array data slab flag
				0x80,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x01,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
			},
		}

		storage := newTestPersistentStorageWithData(t, data)

		_, err := NewArrayWithRootID(storage, id1)
		require.NoError(t, err)

		iterator, err := storage.SlabIterator()
		require.NoError(t, err)

		count := 0
		for {
			id, slab := iterator()
			if id == StorageIDUndefined {
				break
			}

			encodedSlab, err := Encode(slab, storage.cborEncMode)
			require.NoError(t, err)

			require.Equal(t, encodedSlab, data[id])
			count++
		}
		require.Equal(t, len(data), count)
	})
}

func TestPersistentStorageGenerateStorageID(t *testing.T) {
	baseStorage := NewInMemBaseStorage()
	storage := NewPersistentSlabStorage(baseStorage, nil, nil, nil, nil)

	t.Run("temp address", func(t *testing.T) {
		address := Address{}

		id, err := storage.GenerateStorageID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.Address)
		require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.Index)

		id, err = storage.GenerateStorageID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.Address)
		require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.Index)
	})
	t.Run("perm address", func(t *testing.T) {
		address := Address{1}

		id, err := storage.GenerateStorageID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.Address)
		require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.Index)

		id, err = storage.GenerateStorageID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.Address)
		require.Equal(t, StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.Index)
	})
}

func generateRandomSlab(address Address, r *rand.Rand) Slab {
	storable := Uint64Value(r.Uint64())

	return &ArrayDataSlab{
		header: ArraySlabHeader{
			id:    NewStorageID(address, StorageIndex{1}),
			size:  arrayRootDataSlabPrefixSize + storable.ByteSize(),
			count: 1,
		},
		elements: []Storable{storable},
	}
}

func generateRandomAddress(r *rand.Rand) Address {
	address := Address{}
	r.Read(address[:])
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

func (s *accessOrderTrackerBaseStorage) SegmentCounts() int { return len(s.segments) }

func (s *accessOrderTrackerBaseStorage) Size() int { return 0 }

func (s *accessOrderTrackerBaseStorage) BytesRetrieved() int { return 0 }

func (s *accessOrderTrackerBaseStorage) BytesStored() int { return 0 }

func (s *accessOrderTrackerBaseStorage) SegmentsReturned() int { return 0 }

func (s *accessOrderTrackerBaseStorage) SegmentsUpdated() int { return 0 }

func (s *accessOrderTrackerBaseStorage) SegmentsTouched() int { return 0 }

func (s *accessOrderTrackerBaseStorage) ResetReporter() {}

type testLedger struct {
	values map[string][]byte
	index  map[string]StorageIndex
}

var _ Ledger = &testLedger{}

func newTestLedger() *testLedger {
	return &testLedger{
		values: make(map[string][]byte),
		index:  make(map[string]StorageIndex),
	}
}

func (l *testLedger) GetValue(owner, key []byte) (value []byte, err error) {
	value = l.values[l.key(owner, key)]
	return value, nil
}

func (l *testLedger) SetValue(owner, key, value []byte) (err error) {
	l.values[l.key(owner, key)] = value
	return nil
}

func (l *testLedger) ValueExists(owner, key []byte) (exists bool, err error) {
	value := l.values[l.key(owner, key)]
	return len(value) > 0, nil
}

func (l *testLedger) AllocateStorageIndex(owner []byte) (StorageIndex, error) {
	index := l.index[string(owner)]
	next := index.Next()
	l.index[string(owner)] = next
	return next, nil
}

func (l *testLedger) key(owner, key []byte) string {
	return strings.Join([]string{string(owner), string(key)}, "|")
}

type ledgerIterationFunc func() (owner, key, value []byte)

func (l *testLedger) Iterator() ledgerIterationFunc {
	keys := make([]string, 0, len(l.values))
	for k := range l.values {
		keys = append(keys, k)
	}

	i := 0
	return func() (owner, key, value []byte) {
		if i >= len(keys) {
			return nil, nil, nil
		}

		s := strings.Split(keys[i], "|")
		owner, key = []byte(s[0]), []byte(s[1])
		value = l.values[keys[i]]
		i++
		return owner, key, value
	}
}

func (l *testLedger) Count() int {
	return len(l.values)
}

var errEncodeNonStorable = errors.New("failed to encode non-storable")

// nonStorable can't be encoded successfully.
type nonStorable struct{}

func (nonStorable) Encode(_ *Encoder) error {
	return errEncodeNonStorable
}

func (nonStorable) ByteSize() uint32 {
	return 1
}

func (v nonStorable) StoredValue(_ SlabStorage) (Value, error) {
	return v, nil
}

func (nonStorable) ChildStorables() []Storable {
	return nil
}

func (v nonStorable) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
	return v, nil
}

type slowStorable struct {
	Uint8Value
}

func newSlowStorable(i uint8) slowStorable {
	return slowStorable{
		Uint8Value: Uint8Value(i),
	}
}

// slowStorable.Encode is used to reproduce a
// panic. It needs to be slow enough
// when called by FastCommit() compared to
// to a non-slow Encode function returning an error.
// See Atree issue #240.
func (s slowStorable) Encode(encoder *Encoder) error {
	// Use division in a loop to slow down this function
	n := 1.0
	for i := 0; i < 2000; i++ {
		n = (n + float64(i)) / 3.14
	}
	runtime.KeepAlive(n)
	return s.Uint8Value.Encode(encoder)
}

func TestFixLoadedBrokenReferences(t *testing.T) {
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("healthy", func(t *testing.T) {

		// Create a health storage with arrays and maps
		mapMetaDataRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		mapDataNonRootID1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		mapDataNonRootID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		nestedArrayID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		emptyMapDataRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 5}}

		mapDataRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 6}}

		emptyArrayDataRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 7}}

		arrayDataRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 8}}

		arrayMetaDataRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 9}}
		arrayDataNonRootID1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 10}}
		arrayDataNonRootID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 11}}
		nestedArrayID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 12}}

		rootIDs := []StorageID{
			mapMetaDataRootID,
			emptyMapDataRootID,
			mapDataRootID,
			emptyArrayDataRootID,
			arrayDataRootID,
			arrayMetaDataRootID,
		}

		data := map[StorageID][]byte{
			// root map metadata slab
			// metadata slab
			mapMetaDataRootID: {
				// extra data
				// version
				0x00,
				// flag: root + map meta
				0x89,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + meta
				0x89,
				// child header count
				0x00, 0x02,
				// child header 1 (storage id, first key, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x01, 0x02,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0x00, 0x00, 0xfe,
			},

			// data slab
			mapDataNonRootID1: {
				// version
				0x00,
				// flag: map data
				0x08,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:dddddddddddddddddddddd]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
			},

			// data slab
			mapDataNonRootID2: {
				// version
				0x00,
				// flag: has pointer + map data
				0x48,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:StorageID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
			},

			// array data slab
			nestedArrayID: {
				// extra data
				// version
				0x00,
				// flag: root + array data
				0x80,
				// extra data (CBOR encoded array of 1 elements)
				0x81,
				// type info
				0x18, 0x2b,

				// version
				0x00,
				// flag: root + array data
				0x80,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x01,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
			},

			// empty map
			emptyMapDataRootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},

			// root map data slab
			mapDataRootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 1
				0x01,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// element: [uint64(0):uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
			},

			// empty array
			emptyArrayDataRootID: {
				// extra data
				// version
				0x00,
				// extra data flag
				0x80,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// version
				0x00,
				// array data slab flag
				0x80,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x00,
			},

			// root array data slab
			arrayDataRootID: {
				// extra data
				// version
				0x00,
				// extra data flag
				0x80,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// version
				0x00,
				// array data slab flag
				0x80,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x01,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
			},

			// root array metadata slab
			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:270 count:11} ]
			arrayMetaDataRootID: {
				// extra data
				// version
				0x00,
				// extra data flag
				0x81,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// version
				0x00,
				// array meta data slab flag
				0x81,
				// child header count
				0x00, 0x02,
				// child header 1 (storage id, count, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
				0x00, 0x00, 0x00, 0x09,
				0x00, 0x00, 0x00, 0xe4,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x01, 0x0e,
			},

			// (data slab) next: 3, data: [aaaaaaaaaaaaaaaaaaaaaa ... aaaaaaaaaaaaaaaaaaaaaa]
			arrayDataNonRootID1: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x09,
				// CBOR encoded array elements
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			},

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... StorageID(...)]
			arrayDataNonRootID2: {
				// version
				0x00,
				// array data slab flag
				0x40,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0b,
				// CBOR encoded array elements
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c,
			},

			// (data slab) next: 0, data: [0]
			nestedArrayID2: {
				// extra data
				// version
				0x00,
				// extra data flag
				0x80,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2b,

				// version
				0x00,
				// array data slab flag
				0x80,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x01,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
			},
		}

		storage := newTestPersistentStorageWithData(t, data)

		// Load data in storage
		for id := range data {
			_, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
		}

		// Check health before fixing broken reference
		rootIDSet, err := CheckStorageHealth(storage, -1)
		require.NoError(t, err)
		require.Equal(t, len(rootIDs), len(rootIDSet))

		for _, rootID := range rootIDs {
			_, found := rootIDSet[rootID]
			require.True(t, found)
		}

		var fixedRootIDs map[StorageID][]StorageID
		var skippedRootIDs map[StorageID][]StorageID

		// Don't fix any broken references
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return false
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(fixedRootIDs))
		require.Equal(t, 0, len(skippedRootIDs))

		// No data is modified because no fix happened
		require.Equal(t, 0, len(storage.deltas))

		// Fix broken reference
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return true
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(fixedRootIDs))
		require.Equal(t, 0, len(skippedRootIDs))

		// No data is modified during fixing broken reference
		require.Equal(t, 0, len(storage.deltas))

		// Check health after fixing broken reference
		rootIDSet, err = CheckStorageHealth(storage, -1)
		require.NoError(t, err)
		require.Equal(t, len(rootIDs), len(rootIDSet))

	})

	t.Run("broken root map data slab", func(t *testing.T) {

		rootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}

		brokenRefs := map[StorageID][]StorageID{
			rootID: {rootID},
		}

		data := map[StorageID][]byte{
			rootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 1
				0x01,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// element: [StorageID(0x0.1):uint64(0)]
				0x82,
				0xd8, 0xff, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				0xd8, 0xa4, 0x00,
			},
		}

		fixedData := map[StorageID][]byte{
			rootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 0)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		}

		storage := newTestPersistentStorageWithData(t, data)

		// Load data in storage
		for id := range data {
			_, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
		}

		// Check health before fixing broken reference
		_, err := CheckStorageHealth(storage, -1)
		require.ErrorContains(t, err, "slab (0x0.1) not found: slab not found during slab iteration")

		var fixedRootIDs map[StorageID][]StorageID
		var skippedRootIDs map[StorageID][]StorageID

		// Don't fix any broken references
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return false
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(fixedRootIDs))
		require.Equal(t, len(brokenRefs), len(skippedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, skippedRootIDs[rootID])
		}

		// No data is modified because no fix happened
		require.Equal(t, 0, len(storage.deltas))

		// Fix broken references
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return true
		})
		require.NoError(t, err)
		require.Equal(t, len(brokenRefs), len(fixedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, fixedRootIDs[rootID])
		}

		require.Equal(t, 0, len(skippedRootIDs))
		require.Equal(t, 1, len(storage.deltas))

		// Check health after fixing broken reference
		rootIDs, err := CheckStorageHealth(storage, -1)
		require.NoError(t, err)
		require.Equal(t, 1, len(rootIDs))

		_, ok := rootIDs[rootID]
		require.True(t, ok)

		// Save data in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)
		require.Equal(t, 0, len(storage.deltas))

		// Check encoded data
		baseStorage := storage.baseStorage.(*InMemBaseStorage)
		require.Equal(t, 1, len(baseStorage.segments))

		savedData, found, err := baseStorage.Retrieve(rootID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, fixedData[rootID], savedData)
	})

	t.Run("broken non-root map data slab", func(t *testing.T) {
		rootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		nonRootDataID1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		nonRootDataID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}

		brokenRefs := map[StorageID][]StorageID{
			rootID: {nonRootDataID2},
		}

		// Expected serialized slab data with storage id
		data := map[StorageID][]byte{

			// metadata slab
			rootID: {
				// extra data
				// version
				0x00,
				// flag: root + map meta
				0x89,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + meta
				0x89,
				// child header count
				0x00, 0x02,
				// child header 1 (storage id, first key, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x01, 0x02,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0x00, 0x00, 0xfe,
			},

			// data slab
			nonRootDataID1: {
				// version
				0x00,
				// flag: map data
				0x08,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:dddddddddddddddddddddd]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
			},

			// data slab
			nonRootDataID2: {
				// version
				0x00,
				// flag: has pointer + map data
				0x48,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:StorageID(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xff, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
			},
		}

		fixedData := map[StorageID][]byte{
			rootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 0)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		}

		storage := newTestPersistentStorageWithData(t, data)

		// Load data in storage
		for id := range data {
			_, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
		}

		// Check health before fixing broken reference
		_, err := CheckStorageHealth(storage, -1)
		require.ErrorContains(t, err, "slab (0x0.1) not found: slab not found during slab iteration")

		var fixedRootIDs map[StorageID][]StorageID
		var skippedRootIDs map[StorageID][]StorageID

		// Don't fix any broken references
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return false
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(fixedRootIDs))
		require.Equal(t, len(brokenRefs), len(skippedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, skippedRootIDs[rootID])
		}

		// No data is modified because no fix happened
		require.Equal(t, 0, len(storage.deltas))

		// Fix broken reference
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return true
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(fixedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, fixedRootIDs[rootID])
		}

		require.Equal(t, 0, len(skippedRootIDs))
		require.Equal(t, 3, len(storage.deltas))

		// Check health after fixing broken reference
		rootIDs, err := CheckStorageHealth(storage, -1)
		require.NoError(t, err)
		require.Equal(t, 1, len(rootIDs))

		_, ok := rootIDs[rootID]
		require.True(t, ok)

		// Save data in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)
		require.Equal(t, 0, len(storage.deltas))

		// Check encoded data
		baseStorage := storage.baseStorage.(*InMemBaseStorage)
		require.Equal(t, 1, len(baseStorage.segments))

		savedData, found, err := baseStorage.Retrieve(rootID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, fixedData[rootID], savedData)
	})

	t.Run("multiple data slabs with broken reference in the same map", func(t *testing.T) {
		rootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		nonRootDataID1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		nonRootDataID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}

		brokenRefs := map[StorageID][]StorageID{
			rootID: {nonRootDataID1, nonRootDataID2},
		}

		data := map[StorageID][]byte{

			// metadata slab
			rootID: {
				// extra data
				// version
				0x00,
				// flag: root + map meta
				0x89,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + meta
				0x89,
				// child header count
				0x00, 0x02,
				// child header 1 (storage id, first key, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x01, 0x02,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0x00, 0x00, 0xfe,
			},

			// data slab
			nonRootDataID1: {
				// version
				0x00,
				// flag: map data
				0x08,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:StorageID(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1)]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0xd8, 0xff, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
			},

			// data slab
			nonRootDataID2: {
				// version
				0x00,
				// flag: has pointer + map data
				0x48,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:StorageID(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xff, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			},
		}

		fixedData := map[StorageID][]byte{
			rootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 0)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		}

		storage := newTestPersistentStorageWithData(t, data)

		// Load data in storage
		for id := range data {
			_, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
		}

		// Check health before fixing broken reference
		_, err := CheckStorageHealth(storage, -1)
		require.ErrorContains(t, err, "slab not found during slab iteration")

		var fixedRootIDs map[StorageID][]StorageID
		var skippedRootIDs map[StorageID][]StorageID

		// Don't fix any broken references
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return false
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(fixedRootIDs))
		require.Equal(t, len(brokenRefs), len(skippedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, skippedRootIDs[rootID])
		}

		// No data is modified because no fix happened
		require.Equal(t, 0, len(storage.deltas))

		// Fix broken reference
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return true
		})
		require.NoError(t, err)
		require.Equal(t, len(brokenRefs), len(fixedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, fixedRootIDs[rootID])
		}

		require.Equal(t, 0, len(skippedRootIDs))
		require.Equal(t, 3, len(storage.deltas))

		// Check health after fixing broken reference
		rootIDs, err := CheckStorageHealth(storage, -1)
		require.NoError(t, err)
		require.Equal(t, 1, len(rootIDs))

		_, ok := rootIDs[rootID]
		require.True(t, ok)

		// Save data in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)
		require.Equal(t, 0, len(storage.deltas))

		// Check encoded data
		baseStorage := storage.baseStorage.(*InMemBaseStorage)
		require.Equal(t, 1, len(baseStorage.segments))

		savedData, found, err := baseStorage.Retrieve(rootID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, fixedData[rootID], savedData)
	})

	t.Run("broken reference in nested container", func(t *testing.T) {
		parentContainerRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		nonRootDataID1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		nonRootDataID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		nestedContainerRootID := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		brokenRefs := map[StorageID][]StorageID{
			nestedContainerRootID: {nestedContainerRootID},
		}

		data := map[StorageID][]byte{

			// metadata slab
			parentContainerRootID: {
				// extra data
				// version
				0x00,
				// flag: root + map meta
				0x89,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + meta
				0x89,
				// child header count
				0x00, 0x02,
				// child header 1 (storage id, first key, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x01, 0x02,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0x00, 0x00, 0xfe,
			},

			// data slab
			nonRootDataID1: {
				// version
				0x00,
				// flag: map data
				0x08,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:dddddddddddddddddddddd]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
			},

			// data slab
			nonRootDataID2: {
				// version
				0x00,
				// flag: has pointer + map data
				0x48,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:StorageID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
			},

			// map data slab
			nestedContainerRootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 1
				0x01,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// element: [uint64(0):StorageID(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1)]
				0x82,
				0xd8, 0xa4, 0x00,
				0xd8, 0xff, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
			},
		}

		fixedData := map[StorageID][]byte{
			// map data slab
			nestedContainerRootID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 0)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		}

		storage := newTestPersistentStorageWithData(t, data)

		// Load data in storage
		for id := range data {
			_, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
		}

		// Check health before fixing broken reference
		_, err := CheckStorageHealth(storage, -1)
		require.ErrorContains(t, err, "slab (0x0.1) not found: slab not found during slab iteration")

		var fixedRootIDs map[StorageID][]StorageID
		var skippedRootIDs map[StorageID][]StorageID

		// Don't fix any broken references
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return false
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(fixedRootIDs))
		require.Equal(t, len(brokenRefs), len(skippedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, skippedRootIDs[rootID])
		}

		// No data is modified because no fix happened
		require.Equal(t, 0, len(storage.deltas))

		// Fix broken reference
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return true
		})
		require.NoError(t, err)
		require.Equal(t, len(brokenRefs), len(fixedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, fixedRootIDs[rootID])
		}

		require.Equal(t, 0, len(skippedRootIDs))
		require.Equal(t, 1, len(storage.deltas))

		// Check health after fixing broken reference
		rootIDs, err := CheckStorageHealth(storage, -1)
		require.NoError(t, err)
		require.Equal(t, 1, len(rootIDs))

		_, ok := rootIDs[parentContainerRootID]
		require.True(t, ok)

		// Save data in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)
		require.Equal(t, 0, len(storage.deltas))

		// Check encoded data
		baseStorage := storage.baseStorage.(*InMemBaseStorage)
		require.Equal(t, 4, len(baseStorage.segments))

		savedData, found, err := baseStorage.Retrieve(nestedContainerRootID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, fixedData[nestedContainerRootID], savedData)
	})

	t.Run("selectively fix maps", func(t *testing.T) {
		rootID1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		nonRootDataID1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		nonRootDataID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}} // containing broken ref

		rootID2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 4}} // containing broken ref

		rootIDs := []StorageID{rootID1, rootID2}

		brokenRefs := map[StorageID][]StorageID{
			rootID1: {nonRootDataID2},
			rootID2: {rootID2},
		}

		data := map[StorageID][]byte{
			// metadata slab
			rootID1: {
				// extra data
				// version
				0x00,
				// flag: root + map meta
				0x89,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + meta
				0x89,
				// child header count
				0x00, 0x02,
				// child header 1 (storage id, first key, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x01, 0x02,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0x00, 0x00, 0xfe,
			},

			// data slab
			nonRootDataID1: {
				// version
				0x00,
				// flag: map data
				0x08,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:dddddddddddddddddddddd]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
			},

			// data slab
			nonRootDataID2: {
				// version
				0x00,
				// flag: has pointer + map data
				0x48,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:StorageID(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xff, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
			},

			// map data slab
			rootID2: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 1
				0x01,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x4a,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// element: [uint64(0):StorageID(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1)]
				0x82,
				0xd8, 0xa4, 0x00,
				0xd8, 0xff, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
			},
		}

		fixedData := map[StorageID][]byte{
			rootID1: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 0)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},

			rootID2: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x4a,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 0)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		}

		storage := newTestPersistentStorageWithData(t, data)

		// Load data in storage
		for id := range data {
			_, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
		}

		// Check health before fixing broken reference
		_, err := CheckStorageHealth(storage, -1)
		require.ErrorContains(t, err, "slab not found during slab iteration")

		var fixedRootIDs map[StorageID][]StorageID
		var skippedRootIDs map[StorageID][]StorageID

		// Don't fix any broken references
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(_ Value) bool {
			return false
		})
		require.NoError(t, err)
		require.Equal(t, 0, len(fixedRootIDs))
		require.Equal(t, len(brokenRefs), len(skippedRootIDs))

		for rootID, slabIDsWithBrokenRef := range brokenRefs {
			require.ElementsMatch(t, slabIDsWithBrokenRef, skippedRootIDs[rootID])
		}

		// No data is modified because no fix happened
		require.Equal(t, 0, len(storage.deltas))

		// Only fix one map with broken reference
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(v Value) bool {
			m, ok := v.(*OrderedMap)
			require.True(t, ok)
			return rootID1 == m.StorageID()
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(fixedRootIDs))
		require.Equal(t, brokenRefs[rootID1], fixedRootIDs[rootID1])
		require.Equal(t, 1, len(skippedRootIDs))
		require.Equal(t, brokenRefs[rootID2], skippedRootIDs[rootID2])
		require.Equal(t, 3, len(storage.deltas))

		// Save data in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)
		require.Equal(t, 0, len(storage.deltas))

		// Check health after only fixing one map with broken reference
		_, err = CheckStorageHealth(storage, -1)
		require.ErrorContains(t, err, "slab not found during slab iteration")

		// Fix remaining map with broken reference
		fixedRootIDs, skippedRootIDs, err = storage.FixLoadedBrokenReferences(func(v Value) bool {
			return true
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(fixedRootIDs))
		require.Equal(t, brokenRefs[rootID2], fixedRootIDs[rootID2])
		require.Equal(t, 0, len(skippedRootIDs))
		require.Equal(t, 1, len(storage.deltas))

		// Check health after fixing remaining maps with broken reference
		returnedRootIDs, err := CheckStorageHealth(storage, -1)
		require.NoError(t, err)
		require.Equal(t, len(rootIDs), len(returnedRootIDs))

		// Save data in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)
		require.Equal(t, 0, len(storage.deltas))

		// Check encoded data
		baseStorage := storage.baseStorage.(*InMemBaseStorage)
		require.Equal(t, 2, len(baseStorage.segments))

		savedData, found, err := baseStorage.Retrieve(rootID1)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, fixedData[rootID1], savedData)

		savedData, found, err = baseStorage.Retrieve(rootID2)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, fixedData[rootID2], savedData)
	})
}
