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
	"errors"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

func TestStorageIndexNext(t *testing.T) {
	index := SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}
	want := SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}
	require.Equal(t, want, index.Next())
}

func TestNewSlabID(t *testing.T) {
	t.Run("temp address", func(t *testing.T) {
		want := SlabID{address: Address{}, index: SlabIndex{1}}
		require.Equal(t, want, NewSlabID(Address{}, SlabIndex{1}))
	})
	t.Run("perm address", func(t *testing.T) {
		want := SlabID{address: Address{1}, index: SlabIndex{1}}
		require.Equal(t, want, NewSlabID(Address{1}, SlabIndex{1}))
	})
}

func TestNewSlabIDFromRawBytes(t *testing.T) {
	t.Run("data length < slab id size", func(t *testing.T) {
		var fatalError *FatalError
		var slabIDError *SlabIDError

		id, err := NewSlabIDFromRawBytes(nil)
		require.Equal(t, SlabIDUndefined, id)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &slabIDError)
		require.ErrorAs(t, fatalError, &slabIDError)

		id, err = NewSlabIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 2})
		require.Equal(t, SlabIDUndefined, id)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &slabIDError)
		require.ErrorAs(t, fatalError, &slabIDError)
	})

	t.Run("data length == slab id size", func(t *testing.T) {
		id, err := NewSlabIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2})

		want := SlabID{
			address: Address{0, 0, 0, 0, 0, 0, 0, 1},
			index:   SlabIndex{0, 0, 0, 0, 0, 0, 0, 2},
		}
		require.Equal(t, want, id)
		require.NoError(t, err)
	})
	t.Run("data length > slab id size", func(t *testing.T) {
		id, err := NewSlabIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2, 3, 4, 5, 6, 7, 8})

		want := SlabID{
			address: Address{0, 0, 0, 0, 0, 0, 0, 1},
			index:   SlabIndex{0, 0, 0, 0, 0, 0, 0, 2},
		}
		require.Equal(t, want, id)
		require.NoError(t, err)
	})
}

func TestSlabIDToRawBytes(t *testing.T) {
	t.Run("buffer nil", func(t *testing.T) {
		var fatalError *FatalError
		var slabIDError *SlabIDError

		size, err := SlabIDUndefined.ToRawBytes(nil)
		require.Equal(t, 0, size)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &slabIDError)
		require.ErrorAs(t, fatalError, &slabIDError)
	})

	t.Run("buffer too short", func(t *testing.T) {
		var fatalError *FatalError
		var slabIDError *SlabIDError

		b := make([]byte, 8)
		size, err := SlabIDUndefined.ToRawBytes(b)
		require.Equal(t, 0, size)
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &slabIDError)
		require.ErrorAs(t, fatalError, &slabIDError)
	})

	t.Run("undefined", func(t *testing.T) {
		b := make([]byte, slabIDSize)
		size, err := SlabIDUndefined.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		require.Equal(t, want, b)
		require.Equal(t, slabIDSize, size)
	})

	t.Run("temp address", func(t *testing.T) {
		id := NewSlabID(Address{0, 0, 0, 0, 0, 0, 0, 0}, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		b := make([]byte, slabIDSize)
		size, err := id.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
		require.Equal(t, want, b)
		require.Equal(t, slabIDSize, size)
	})

	t.Run("temp index", func(t *testing.T) {
		id := NewSlabID(Address{0, 0, 0, 0, 0, 0, 0, 1}, SlabIndex{0, 0, 0, 0, 0, 0, 0, 0})
		b := make([]byte, slabIDSize)
		size, err := id.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}
		require.Equal(t, want, b)
		require.Equal(t, slabIDSize, size)
	})

	t.Run("perm", func(t *testing.T) {
		id := NewSlabID(Address{0, 0, 0, 0, 0, 0, 0, 1}, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		b := make([]byte, slabIDSize)
		size, err := id.ToRawBytes(b)
		require.NoError(t, err)

		want := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2}
		require.Equal(t, want, b)
		require.Equal(t, slabIDSize, size)
	})
}

func TestSlabIDAddressAsUint64(t *testing.T) {
	t.Run("temp", func(t *testing.T) {
		id := NewSlabID(Address{}, SlabIndex{1})
		require.Equal(t, uint64(0), id.AddressAsUint64())
	})
	t.Run("perm", func(t *testing.T) {
		id := NewSlabID(Address{0, 0, 0, 0, 0, 0, 0, 1}, SlabIndex{1})
		require.Equal(t, uint64(1), id.AddressAsUint64())
	})
}

func TestSlabIDIndexAsUint64(t *testing.T) {
	t.Run("temp", func(t *testing.T) {
		id := NewSlabID(Address{}, SlabIndex{})
		require.Equal(t, uint64(0), id.IndexAsUint64())
	})
	t.Run("perm", func(t *testing.T) {
		id := NewSlabID(Address{}, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		require.Equal(t, uint64(1), id.IndexAsUint64())
	})
}

func TestSlabIDValid(t *testing.T) {
	t.Run("undefined", func(t *testing.T) {
		id := SlabIDUndefined
		err := id.Valid()

		var fatalError *FatalError
		var slabIDError *SlabIDError
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &slabIDError)
		require.ErrorAs(t, fatalError, &slabIDError)
	})

	t.Run("temp index", func(t *testing.T) {
		id := SlabID{address: Address{1}, index: SlabIndexUndefined}
		err := id.Valid()

		var fatalError *FatalError
		var slabIDError *SlabIDError
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &fatalError)
		require.ErrorAs(t, err, &slabIDError)
		require.ErrorAs(t, fatalError, &slabIDError)
	})

	t.Run("temp address", func(t *testing.T) {
		id := SlabID{address: AddressUndefined, index: SlabIndex{1}}
		require.NoError(t, id.Valid())
	})

	t.Run("valid", func(t *testing.T) {
		id := SlabID{address: Address{1}, index: SlabIndex{2}}
		require.NoError(t, id.Valid())
	})
}

func TestSlabIDCompare(t *testing.T) {
	t.Run("same", func(t *testing.T) {
		id1 := NewSlabID(Address{1}, SlabIndex{1})
		id2 := NewSlabID(Address{1}, SlabIndex{1})
		require.Equal(t, 0, id1.Compare(id2))
		require.Equal(t, 0, id2.Compare(id1))
	})

	t.Run("different address", func(t *testing.T) {
		id1 := NewSlabID(Address{1}, SlabIndex{1})
		id2 := NewSlabID(Address{2}, SlabIndex{1})
		require.Equal(t, -1, id1.Compare(id2))
		require.Equal(t, 1, id2.Compare(id1))
	})

	t.Run("different index", func(t *testing.T) {
		id1 := NewSlabID(Address{1}, SlabIndex{1})
		id2 := NewSlabID(Address{1}, SlabIndex{2})
		require.Equal(t, -1, id1.Compare(id2))
		require.Equal(t, 1, id2.Compare(id1))
	})

	t.Run("different address and index", func(t *testing.T) {
		id1 := NewSlabID(Address{1}, SlabIndex{1})
		id2 := NewSlabID(Address{2}, SlabIndex{2})
		require.Equal(t, -1, id1.Compare(id2))
		require.Equal(t, 1, id2.Compare(id1))
	})
}

func TestLedgerBaseStorageStore(t *testing.T) {
	ledger := newTestLedger()
	baseStorage := NewLedgerBaseStorage(ledger)

	bytesStored := 0
	values := map[SlabID][]byte{
		{Address{1}, SlabIndex{1}}: {1, 2, 3},
		{Address{1}, SlabIndex{2}}: {4, 5, 6},
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
		var id SlabID
		copy(id.address[:], owner)
		copy(id.index[:], key[1:])

		require.True(t, LedgerKeyIsSlabKey(string(key)))
		require.Equal(t, values[id], value)

		count++
	}
	require.Equal(t, len(values), count)
}

func TestLedgerBaseStorageRetrieve(t *testing.T) {
	ledger := newTestLedger()
	baseStorage := NewLedgerBaseStorage(ledger)

	id := SlabID{address: Address{1}, index: SlabIndex{1}}
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
	id = SlabID{address: Address{1}, index: SlabIndex{2}}
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

	id := SlabID{address: Address{1}, index: SlabIndex{1}}
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
	err = baseStorage.Remove(SlabID{address: id.address, index: id.index.Next()})
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
		var id SlabID
		copy(id.address[:], owner)
		copy(id.index[:], key[1:])

		require.True(t, LedgerKeyIsSlabKey(string(key)))
		require.Nil(t, value)

		count++
	}
	require.Equal(t, 2, count)
}

func TestLedgerBaseStorageGenerateSlabID(t *testing.T) {
	ledger := newTestLedger()
	baseStorage := NewLedgerBaseStorage(ledger)

	address1 := Address{1}
	address2 := Address{2}

	id, err := baseStorage.GenerateSlabID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.address)
	require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.index)

	id, err = baseStorage.GenerateSlabID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.address)
	require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.index)

	id, err = baseStorage.GenerateSlabID(address2)
	require.NoError(t, err)
	require.Equal(t, address2, id.address)
	require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.index)
}

func TestBasicSlabStorageStore(t *testing.T) {
	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	r := newRand(t)
	address := Address{1}
	slabs := map[SlabID]Slab{
		{address, SlabIndex{1}}: generateRandomSlab(address, r),
		{address, SlabIndex{2}}: generateRandomSlab(address, r),
	}

	// Store values
	for id, slab := range slabs {
		err := storage.Store(id, slab)
		require.NoError(t, err)
	}

	// Overwrite stored values
	for id := range slabs {
		slab := generateRandomSlab(id.address, r)
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
	id := SlabID{Address{1}, SlabIndex{1}}
	slab := generateRandomSlab(id.address, r)

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
	id = SlabID{address: Address{1}, index: SlabIndex{2}}
	retrievedSlab, found, err = storage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, retrievedSlab)
}

func TestBasicSlabStorageRemove(t *testing.T) {
	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	r := newRand(t)
	id := SlabID{Address{1}, SlabIndex{1}}
	slab := generateRandomSlab(id.address, r)

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
	err = storage.Remove(SlabID{address: id.address, index: id.index.Next()})
	require.NoError(t, err)

	// Retrieve removed value
	slab, found, err := storage.Retrieve(id)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, slab)

	require.Equal(t, 0, storage.Count())
}

func TestBasicSlabStorageGenerateSlabID(t *testing.T) {
	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	address1 := Address{1}
	address2 := Address{2}

	id, err := storage.GenerateSlabID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.address)
	require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.index)

	id, err = storage.GenerateSlabID(address1)
	require.NoError(t, err)
	require.Equal(t, address1, id.address)
	require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.index)

	id, err = storage.GenerateSlabID(address2)
	require.NoError(t, err)
	require.Equal(t, address2, id.address)
	require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.index)
}

func TestBasicSlabStorageSlabIDs(t *testing.T) {
	r := newRand(t)
	address := Address{1}
	index := SlabIndex{0, 0, 0, 0, 0, 0, 0, 0}
	wantIDs := map[SlabID]bool{
		{address: address, index: index.Next()}: true,
		{address: address, index: index.Next()}: true,
		{address: address, index: index.Next()}: true,
	}

	storage := NewBasicSlabStorage(nil, nil, nil, nil)

	// Get slab ids from empty storgae
	ids := storage.SlabIDs()
	require.Equal(t, 0, len(ids))

	// Store values
	for id := range wantIDs {
		err := storage.Store(id, generateRandomSlab(id.address, r))
		require.NoError(t, err)
	}

	// Get slab ids from non-empty storgae
	ids = storage.SlabIDs()
	require.Equal(t, len(wantIDs), len(ids))

	for _, id := range ids {
		require.True(t, wantIDs[id])
	}
}

func TestBasicSlabStorageSlabIterat(t *testing.T) {
	r := newRand(t)
	address := Address{1}
	index := SlabIndex{0, 0, 0, 0, 0, 0, 0, 0}

	id1 := SlabID{address: address, index: index.Next()}
	id2 := SlabID{address: address, index: index.Next()}
	id3 := SlabID{address: address, index: index.Next()}

	want := map[SlabID]Slab{
		id1: generateRandomSlab(id1.address, r),
		id2: generateRandomSlab(id2.address, r),
		id3: generateRandomSlab(id3.address, r),
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
		if id == SlabIDUndefined {
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

		tempSlabID, err := NewSlabIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		permSlabID, err := NewSlabIDFromRawBytes([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		_, found, err := storage.Retrieve(tempSlabID)
		require.NoError(t, err)
		require.False(t, found)

		_, found, err = storage.Retrieve(permSlabID)
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

		tempSlabID, err := NewSlabIDFromRawBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		permSlabID, err := NewSlabIDFromRawBytes([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
		require.NoError(t, err)

		slab1 := generateRandomSlab(tempSlabID.address, r)
		slab2 := generateRandomSlab(permSlabID.address, r)

		// no temp ids should be in the base storage
		err = storage.Store(tempSlabID, slab1)
		require.NoError(t, err)
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())

		err = storage.Store(permSlabID, slab2)
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

		// Slab with temp slab id is NOT persisted in base storage.
		_, found, err := baseStorage.Retrieve(tempSlabID)
		require.NoError(t, err)
		require.False(t, found)

		// Slab with temp slab id is cached in storage.
		_, found, err = storage.Retrieve(tempSlabID)
		require.NoError(t, err)
		require.True(t, found)

		// Slab with perm slab id is persisted in base storage.
		_, found, err = baseStorage.Retrieve(permSlabID)
		require.NoError(t, err)
		require.True(t, found)

		// Slab with perm slab id is cached in storage.
		_, found, err = storage.Retrieve(permSlabID)
		require.NoError(t, err)
		require.True(t, found)

		// Remove slab with perm slab id
		err = storage.Remove(permSlabID)
		require.NoError(t, err)

		// Remove slab with temp slab id
		err = storage.Remove(tempSlabID)
		require.NoError(t, err)

		require.Equal(t, uint(1), storage.DeltasWithoutTempAddresses())
		require.Equal(t, uint64(0), storage.DeltasSizeWithoutTempAddresses())

		err = storage.Commit()
		require.NoError(t, err)

		// Slab with perm slab id is removed from base storage.
		_, found, err = baseStorage.Retrieve(permSlabID)
		require.NoError(t, err)
		require.False(t, found)

		// Slab with perm slab id is removed from cache in storage.
		_, found, err = storage.Retrieve(permSlabID)
		require.NoError(t, err)
		require.False(t, found)

		// Slab with temp slab id is removed from cache in storage.
		_, found, err = storage.Retrieve(tempSlabID)
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

		simpleMap := make(map[SlabID][]byte)
		slabSize := uint64(0)
		// test random updates apply commit and check the order of committed values
		for i := 0; i < numberOfAccounts; i++ {
			for j := 0; j < numberOfSlabsPerAccount; j++ {
				addr := generateRandomAddress(r)
				slab := generateRandomSlab(addr, r)
				slabSize += uint64(slab.ByteSize())

				slabID, err := storage.GenerateSlabID(addr)
				require.NoError(t, err)
				err = storage.Store(slabID, slab)
				require.NoError(t, err)

				slabID2, err := storageWithFastCommit.GenerateSlabID(addr)
				require.NoError(t, err)
				err = storageWithFastCommit.Store(slabID2, slab)
				require.NoError(t, err)

				// capture data for accuracy testing
				simpleMap[slabID], err = Encode(slab, encMode)
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

		id, err := storage.GenerateSlabID(address)
		require.NoError(t, err)

		err = storage.Store(id, slabWithNonStorable)
		require.NoError(t, err)

		for i := 0; i < 500; i++ {
			id, err := storage.GenerateSlabID(address)
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
			if id == SlabIDUndefined {
				break
			}
			count++
		}
		require.Equal(t, 0, count)
	})

	t.Run("not-empty storage", func(t *testing.T) {

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		id4 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		data := map[SlabID][]byte{
			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:270 count:11} ]
			id1: {
				// extra data
				// version
				0x10,
				// extra data flag
				0x81,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// child shared storage address
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				// child header count
				0x00, 0x02,
				// child header 1 (slab id, count, size)
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x09,
				0x00, 0xe4,
				// child header 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x0b,
				0x01, 0x0e,
			},

			// (data slab) next: 3, data: [aaaaaaaaaaaaaaaaaaaaaa ... aaaaaaaaaaaaaaaaaaaaaa]
			id2: {
				// version
				0x12,
				// array data slab flag
				0x00,
				// next slab id
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

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... SlabID(...)]
			id3: {
				// version
				0x10,
				// array data slab flag
				0x40,
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
				0x10,
				// extra data flag
				0x80,
				// array of extra data
				0x81,
				// type info
				0x18, 0x2b,

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
			if id == SlabIDUndefined {
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

func TestPersistentStorageGenerateSlabID(t *testing.T) {
	baseStorage := NewInMemBaseStorage()
	storage := NewPersistentSlabStorage(baseStorage, nil, nil, nil, nil)

	t.Run("temp address", func(t *testing.T) {
		address := Address{}

		id, err := storage.GenerateSlabID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.address)
		require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.index)

		id, err = storage.GenerateSlabID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.address)
		require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.index)
	})
	t.Run("perm address", func(t *testing.T) {
		address := Address{1}

		id, err := storage.GenerateSlabID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.address)
		require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}, id.index)

		id, err = storage.GenerateSlabID(address)
		require.NoError(t, err)
		require.Equal(t, address, id.address)
		require.Equal(t, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}, id.index)
	})
}

func generateRandomSlab(address Address, r *rand.Rand) Slab {
	storable := Uint64Value(r.Uint64())

	return &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: NewSlabID(address, SlabIndex{1}),
			size:   arrayRootDataSlabPrefixSize + storable.ByteSize(),
			count:  1,
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
	segTouchOrder []SlabID
	indexReqOrder []Address
	segments      map[SlabID][]byte
	storageIndex  map[Address]SlabIndex
}

func newAccessOrderTrackerBaseStorage() *accessOrderTrackerBaseStorage {
	return &accessOrderTrackerBaseStorage{
		segTouchOrder: make([]SlabID, 0),
		indexReqOrder: make([]Address, 0),
		segments:      make(map[SlabID][]byte),
		storageIndex:  make(map[Address]SlabIndex),
	}
}

func (s *accessOrderTrackerBaseStorage) SegTouchOrder() []SlabID {
	return s.segTouchOrder
}

func (s *accessOrderTrackerBaseStorage) Retrieve(id SlabID) ([]byte, bool, error) {
	s.segTouchOrder = append(s.segTouchOrder, id)
	seg, ok := s.segments[id]
	return seg, ok, nil
}

func (s *accessOrderTrackerBaseStorage) Store(id SlabID, data []byte) error {
	s.segTouchOrder = append(s.segTouchOrder, id)
	s.segments[id] = data
	return nil
}

func (s *accessOrderTrackerBaseStorage) Remove(id SlabID) error {
	s.segTouchOrder = append(s.segTouchOrder, id)
	delete(s.segments, id)
	return nil
}

func (s *accessOrderTrackerBaseStorage) GenerateSlabID(address Address) (SlabID, error) {
	s.indexReqOrder = append(s.indexReqOrder, address)

	index := s.storageIndex[address]
	nextIndex := index.Next()

	s.storageIndex[address] = nextIndex
	return NewSlabID(address, nextIndex), nil
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
	index  map[string]SlabIndex
}

var _ Ledger = &testLedger{}

func newTestLedger() *testLedger {
	return &testLedger{
		values: make(map[string][]byte),
		index:  make(map[string]SlabIndex),
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

func (l *testLedger) AllocateSlabIndex(owner []byte) (SlabIndex, error) {
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
