/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
	//"golang.org/x/exp/rand"
)

var (
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

// Seed only once and print seed for easier debugging.
func init() {
	//seed := uint64(0x9E3779B97F4A7C15) // goldenRatio
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	fmt.Printf("seed: 0x%x\n", seed)
}

func randStr(n int) string {
	r := make([]rune, n)
	for i := range r {
		r[i] = letters[rand.Intn(len(letters))]
	}
	return string(r)
}

func newTestPersistentStorage(t testing.TB) *PersistentSlabStorage {
	baseStorage := NewInMemBaseStorage()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	storage := NewPersistentSlabStorage(baseStorage, encMode, decMode, WithNoAutoCommit())
	storage.DecodeStorable = decodeStorable
	return storage
}

func TestAppendAndGet(t *testing.T) {

	t.Parallel()

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	const arraySize = 256 * 256

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i, uint64(v))
	}

	require.Equal(t, typeInfo, array.Type())

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := array.Stats()
	require.Equal(t,
		stats.DataSlabCount+stats.MetaDataSlabCount,
		uint64(array.Storage.Count()),
	)
}

func TestSetAndGet(t *testing.T) {

	const arraySize = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		err := array.Set(i, Uint64Value(i*10))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i*10, uint64(v))
	}

	require.Equal(t, typeInfo, array.Type())

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestInsertAndGet(t *testing.T) {
	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Insert(0, Uint64Value(arraySize-i-1))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Insert(i, Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i += 2 {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(1); i < arraySize; i += 2 {
			err := array.Insert(i, Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestRemove(t *testing.T) {
	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("remove-first", func(t *testing.T) {
		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		require.Equal(t, typeInfo, array.Type())

		for i := uint64(0); i < arraySize; i++ {
			v, err := array.Remove(0)
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))

			require.Equal(t, arraySize-i-1, array.Count())

			require.Equal(t, typeInfo, array.Type())

			if i%256 == 0 {
				verified, err := array.valid(typeInfo)
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("remove-last", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		require.Equal(t, typeInfo, array.Type())

		for i := arraySize - 1; i >= 0; i-- {
			v, err := array.Remove(uint64(i))
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, uint64(i), uint64(e))

			require.Equal(t, uint64(i), array.Count())

			require.Equal(t, typeInfo, array.Type())

			if i%256 == 0 {
				verified, err := array.valid(typeInfo)
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("remove", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		require.Equal(t, typeInfo, array.Type())

		// Remove every other elements
		for i := uint64(0); i < array.Count(); i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, err := array.Remove(i)
			require.NoError(t, err)

			require.Equal(t, e, v)

			require.Equal(t, typeInfo, array.Type())

			if i%256 == 0 {
				verified, err := array.valid(typeInfo)
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		for i, j := uint64(0), uint64(1); i < array.Count(); i, j = i+1, j+2 {
			v, err := array.Get(i)
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, j, uint64(e))
		}

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestSplit(t *testing.T) {
	t.Run("data slab as root", func(t *testing.T) {
		const arraySize = 50

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.NotNil(t, array.root)
		require.True(t, array.root.IsData())
		require.Equal(t, uint32(50), array.root.Header().count)
		require.Equal(t, typeInfo, array.Type())

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("metdata slab as root", func(t *testing.T) {
		SetThreshold(60)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 50

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.NotNil(t, array.root)
		require.False(t, array.root.IsData())
		require.Equal(t, uint32(50), array.root.Header().count)
		require.Equal(t, typeInfo, array.Type())

		root := array.root.(*ArrayMetaDataSlab)
		for _, h := range root.childrenHeaders {
			id := h.id
			slab, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
			arraySlab, ok := slab.(ArraySlab)
			require.True(t, ok)
			require.False(t, arraySlab.IsData())
		}

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		i := uint64(0)
		err = array.Iterate(func(v Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(0))
	})

	t.Run("append", func(t *testing.T) {
		SetThreshold(100)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		err = array.Iterate(func(v Value) (bool, error) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(arraySize))
	})

	t.Run("set", func(t *testing.T) {
		SetThreshold(100)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			err := array.Set(i, Uint64Value(i*10))
			require.NoError(t, err)
		}

		i := uint64(0)
		err = array.Iterate(func(v Value) (bool, error) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i*10, uint64(e))
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(arraySize))
	})

	t.Run("insert", func(t *testing.T) {
		SetThreshold(100)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i += 2 {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(1); i < arraySize; i += 2 {
			err := array.Insert(i, Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		err = array.Iterate(func(v Value) (bool, error) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(arraySize))
	})

	t.Run("remove", func(t *testing.T) {
		SetThreshold(100)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		// Remove every other elements
		for i := uint64(0); i < array.Count(); i++ {
			_, err := array.Remove(i)
			require.NoError(t, err)
		}

		i := uint64(1)
		err = array.Iterate(func(v Value) (bool, error) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))
			i += 2
			return true, nil
		})
		require.NoError(t, err)
	})

	t.Run("stop", func(t *testing.T) {

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const count = 10

		for i := uint64(0); i < count; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(count), array.Count())

		i := 0
		err = array.Iterate(func(_ Value) (bool, error) {
			if i == count/2 {
				return false, nil
			}

			i++

			return true, nil
		})
		require.NoError(t, err)

		require.Equal(t, count/2, i)
	})

	t.Run("error", func(t *testing.T) {

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const count = 10

		for i := uint64(0); i < count; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(count), array.Count())

		testErr := errors.New("test")

		i := 0
		err = array.Iterate(func(_ Value) (bool, error) {
			if i == count/2 {
				return false, testErr
			}

			i++

			return true, nil
		})
		require.Error(t, err)
		require.Equal(t, testErr, err)

		require.Equal(t, count/2, i)
	})
}

func TestDeepCopy(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize uint64 = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address1 := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address1, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	require.Equal(t, typeInfo, array.Type())

	address2 := Address{11, 12, 13, 14, 15, 16, 17, 18}

	copied, err := array.DeepCopy(storage, address2)
	require.NoError(t, err)
	require.IsType(t, &Array{}, copied)

	arrayCopy := copied.(*Array)

	// Modify the original array
	err = array.Insert(0, Uint64Value(42))
	require.NoError(t, err)

	require.Equal(t, arraySize, arrayCopy.Count())

	for i := uint64(0); i < arraySize; i++ {
		value, err := arrayCopy.Get(i)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i), value)
	}

	require.Equal(t, typeInfo, arrayCopy.Type())

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	verified, err = arrayCopy.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)
}

func TestDeepRemove(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	typeInfo1 := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array1, err := NewArray(storage, address, typeInfo1)
	require.NoError(t, err)

	err = array1.Append(Uint64Value(42))
	require.NoError(t, err)

	const stringSize = 256 * 256

	err = array1.Append(NewStringValue(randStr(stringSize)))
	require.NoError(t, err)

	typeInfo2 := cbor.RawMessage{0x18, 0x2B} // unsigned(43)

	array2, err := NewArray(storage, address, typeInfo2)
	require.NoError(t, err)

	err = array2.Append(NewStringValue(randStr(stringSize)))
	require.NoError(t, err)

	err = array1.Append(array2)
	require.NoError(t, err)

	require.Equal(t, typeInfo1, array1.Type())

	require.Equal(t, typeInfo2, array2.Type())

	verified, err := array1.valid(typeInfo1)
	require.NoError(t, err)
	require.True(t, verified)

	verified, err = array2.valid(typeInfo2)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	require.Equal(t, 4, storage.Count())

	err = array1.DeepRemove(storage)
	require.NoError(t, err)

	err = storage.Commit()
	require.NoError(t, err)

	require.Equal(t, 1, storage.Count())
}

func TestConstRootStorageID(t *testing.T) {
	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	err = array.Append(Uint64Value(0))
	require.NoError(t, err)

	savedRootID := array.root.Header().id
	require.NotEqual(t, StorageIDUndefined, savedRootID)

	for i := uint64(1); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	rootID := array.root.Header().id
	require.Equal(t, savedRootID, rootID)
	require.Equal(t, typeInfo, array.Type())

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i, uint64(v))
	}
}

func TestSetRandomValue(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]uint64, arraySize)

	for i := uint64(0); i < arraySize; i++ {
		k := rand.Intn(int(i) + 1)
		v := rand.Uint64()

		copy(values[k+1:], values[k:])
		values[k] = v

		err := array.Insert(uint64(k), Uint64Value(v))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		k := rand.Intn(arraySize)
		v := rand.Uint64()

		values[k] = v

		err := array.Set(uint64(k), Uint64Value(v))
		require.NoError(t, err)
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, v, uint64(ev))
	}

	require.Equal(t, typeInfo, array.Type())

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestInsertRandomValue(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]uint64, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			v := rand.Uint64()
			values[arraySize-i-1] = v

			err := array.Insert(0, Uint64Value(v))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, values[i], uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]uint64, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			v := rand.Uint64()
			values[i] = v

			err := array.Insert(i, Uint64Value(v))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, values[i], uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert-random", func(t *testing.T) {

		const arraySize = 256 * 256

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]uint64, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			k := rand.Intn(int(i) + 1)
			v := rand.Uint64()

			copy(values[k+1:], values[k:])
			values[k] = v

			err := array.Insert(uint64(k), Uint64Value(v))
			require.NoError(t, err)
		}

		for k, v := range values {
			e, err := array.Get(uint64(k))
			require.NoError(t, err)

			ev, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, v, uint64(ev))
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestRemoveRandomElement(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]uint64, arraySize)

	// Insert n random values into array
	for i := uint64(0); i < arraySize; i++ {
		v := rand.Uint64()
		values[i] = v

		err := array.Insert(i, Uint64Value(v))
		require.NoError(t, err)
	}

	require.Equal(t, uint64(arraySize), array.Count())

	require.Equal(t, typeInfo, array.Type())

	// Remove n elements at random index
	for i := uint64(0); i < arraySize; i++ {
		k := rand.Intn(int(array.Count()))

		v, err := array.Remove(uint64(k))
		require.NoError(t, err)

		ev, ok := v.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, values[k], uint64(ev))

		copy(values[k:], values[k+1:])
		values = values[:len(values)-1]
	}

	require.Equal(t, uint64(0), array.Count())
	require.Equal(t, uint64(0), uint64(len(values)))
	require.Equal(t, typeInfo, array.Type())

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestRandomAppendSetInsertRemove(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const actionCount = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]uint64, 0, actionCount)

	for i := uint64(0); i < actionCount; i++ {

		nextAction := rand.Intn(MaxAction)

		switch nextAction {

		case AppendAction:
			v := rand.Uint64()

			values = append(values, v)

			err := array.Append(Uint64Value(v))
			require.NoError(t, err)

		case SetAction:
			if array.Count() == 0 {
				continue
			}
			k := rand.Intn(int(array.Count()))
			v := rand.Uint64()

			values[k] = v

			err := array.Set(uint64(k), Uint64Value(v))
			require.NoError(t, err)

		case InsertAction:
			k := rand.Intn(int(array.Count() + 1))
			v := rand.Uint64()

			if k == int(array.Count()) {
				values = append(values, v)
			} else {
				values = append(values, 0)
				copy(values[k+1:], values[k:])
				values[k] = v
			}

			err := array.Insert(uint64(k), Uint64Value(v))
			require.NoError(t, err)

		case RemoveAction:
			if array.Count() > 0 {
				k := rand.Intn(int(array.Count()))

				v, err := array.Remove(uint64(k))
				require.NoError(t, err)

				ev, ok := v.(Uint64Value)
				require.True(t, ok)
				require.Equal(t, values[k], uint64(ev))

				copy(values[k:], values[k+1:])
				values = values[:len(values)-1]
			}
		}

		require.Equal(t, array.Count(), uint64(len(values)))
		require.Equal(t, typeInfo, array.Type())
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, v, uint64(ev))
	}

	i := 0
	err = array.Iterate(func(v Value) (bool, error) {
		e, ok := v.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, values[i], uint64(e))
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(values), i)

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestRandomAppendSetInsertRemoveUint8(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const actionCount = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]uint8, 0, actionCount)

	for i := uint64(0); i < actionCount; i++ {

		nextAction := rand.Intn(MaxAction)

		switch nextAction {

		case AppendAction:
			v := rand.Intn(math.MaxUint8 + 1)

			values = append(values, uint8(v))

			err := array.Append(Uint8Value(v))
			require.NoError(t, err)

		case SetAction:
			if array.Count() == 0 {
				continue
			}
			k := rand.Intn(int(array.Count()))
			v := rand.Intn(math.MaxUint8 + 1)

			values[k] = uint8(v)

			err := array.Set(uint64(k), Uint8Value(v))
			require.NoError(t, err)

		case InsertAction:
			k := rand.Intn(int(array.Count() + 1))
			v := rand.Intn(math.MaxUint8 + 1)

			if k == int(array.Count()) {
				values = append(values, uint8(v))
			} else {
				values = append(values, 0)
				copy(values[k+1:], values[k:])
				values[k] = uint8(v)
			}

			err := array.Insert(uint64(k), Uint8Value(v))
			require.NoError(t, err)

		case RemoveAction:
			if array.Count() > 0 {
				k := rand.Intn(int(array.Count()))

				v, err := array.Remove(uint64(k))
				require.NoError(t, err)

				ev, ok := v.(Uint8Value)
				require.True(t, ok)
				require.Equal(t, values[k], uint8(ev))

				copy(values[k:], values[k+1:])
				values = values[:len(values)-1]
			}
		}

		require.Equal(t, array.Count(), uint64(len(values)))
		require.Equal(t, typeInfo, array.Type())
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint8Value)
		require.True(t, ok)
		require.Equal(t, v, uint8(ev))
	}

	i := 0
	err = array.Iterate(func(v Value) (bool, error) {
		e, ok := v.(Uint8Value)
		require.True(t, ok)
		require.Equal(t, values[i], uint8(e))
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(values), i)

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestRandomAppendSetInsertRemoveMixedTypes(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	const (
		Uint8Type = iota
		Uint16Type
		Uint32Type
		Uint64Type
		MaxType
	)

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const actionCount = 256 * 256

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, 0, actionCount)

	for i := uint64(0); i < actionCount; i++ {

		var v Value

		switch rand.Intn(MaxType) {
		case Uint8Type:
			n := rand.Intn(math.MaxUint8 + 1)
			v = Uint8Value(n)
		case Uint16Type:
			n := rand.Intn(math.MaxUint16 + 1)
			v = Uint16Value(n)
		case Uint32Type:
			v = Uint32Value(rand.Uint32())
		case Uint64Type:
			v = Uint64Value(rand.Uint64())
		}

		switch rand.Intn(MaxAction) {

		case AppendAction:
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)

		case SetAction:
			if array.Count() == 0 {
				continue
			}
			k := rand.Intn(int(array.Count()))

			values[k] = v

			err := array.Set(uint64(k), v)
			require.NoError(t, err)

		case InsertAction:
			k := rand.Intn(int(array.Count() + 1))

			if k == int(array.Count()) {
				values = append(values, v)
			} else {
				values = append(values, nil)
				copy(values[k+1:], values[k:])
				values[k] = v
			}

			err := array.Insert(uint64(k), v)
			require.NoError(t, err)

		case RemoveAction:
			if array.Count() > 0 {
				k := rand.Intn(int(array.Count()))

				v, err := array.Remove(uint64(k))
				require.NoError(t, err)

				require.Equal(t, values[k], v)

				copy(values[k:], values[k+1:])
				values = values[:len(values)-1]
			}
		}

		require.Equal(t, array.Count(), uint64(len(values)))
		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	i := 0
	err = array.Iterate(func(v Value) (bool, error) {
		require.Equal(t, values[i], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(values), i)

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestNestedArray(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("small", func(t *testing.T) {

		const arraySize = 256 * 256

		nestedTypeInfo := cbor.RawMessage{0x18, 0x2B} // unsigned(43)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		nestedArrays := make([]*Array, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested, err := NewArray(storage, address, nestedTypeInfo)
			require.NoError(t, err)

			err = nested.Append(Uint64Value(i * 2))
			require.NoError(t, err)

			err = nested.Append(Uint64Value(i*2 + 1))
			require.NoError(t, err)

			require.True(t, nested.root.IsData())

			nestedArrays[i] = nested
		}

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)
	})

	t.Run("big", func(t *testing.T) {

		const arraySize = 256 * 256

		nestedTypeInfo := cbor.RawMessage{0x18, 0x2B} // unsigned(43)

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		nestedArrays := make([]*Array, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested, err := NewArray(storage, address, nestedTypeInfo)
			require.NoError(t, err)

			for i := uint64(0); i < 50; i++ {
				err := nested.Append(Uint64Value(i))
				require.NoError(t, err)
			}
			require.False(t, nested.root.IsData())

			nestedArrays[i] = nested
		}

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)
	})
}

func TestEncode(t *testing.T) {

	SetThreshold(60)
	defer func() {
		SetThreshold(1024)
	}()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	t.Run("no pointers", func(t *testing.T) {
		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		// Create and populate array in memory
		storage := NewBasicSlabStorage(encMode, decMode)
		storage.DecodeStorable = decodeStorable

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}
		require.Equal(t, uint64(arraySize), array.Count())

		id1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}

		// Expected serialized slab data with storage id
		expected := map[StorageID][]byte{

			// (metadata slab) headers: [{id:2 size:50 count:10} {id:3 size:50 count:10} ]
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
				0x00, 0x00, 0x00, 0x40,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x00, 0x46,
			},

			// (data slab) prev: 0, next: 3, data: [0 1 2 ... 7 8 9]
			id2: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// prev storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x09,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
				0xd8, 0xa4, 0x01,
				0xd8, 0xa4, 0x02,
				0xd8, 0xa4, 0x03,
				0xd8, 0xa4, 0x04,
				0xd8, 0xa4, 0x05,
				0xd8, 0xa4, 0x06,
				0xd8, 0xa4, 0x07,
				0xd8, 0xa4, 0x08,
			},

			// (data slab) prev: 2, next: 0, data: [10 11 12 ... 17 18 19]
			id3: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// prev storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0b,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x09,
				0xd8, 0xa4, 0x0a,
				0xd8, 0xa4, 0x0b,
				0xd8, 0xa4, 0x0c,
				0xd8, 0xa4, 0x0d,
				0xd8, 0xa4, 0x0e,
				0xd8, 0xa4, 0x0f,
				0xd8, 0xa4, 0x10,
				0xd8, 0xa4, 0x11,
				0xd8, 0xa4, 0x12,
				0xd8, 0xa4, 0x13,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, expected, m)
	})

	t.Run("has pointers", func(t *testing.T) {
		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		// Create and populate array in memory
		storage := NewBasicSlabStorage(encMode, decMode)
		storage.DecodeStorable = decodeStorable

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		id1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		id4 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		for i := uint64(0); i < arraySize-1; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		typeInfo2 := cbor.RawMessage{0x18, 0x2B} // unsigned(43)

		array2, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		err = array2.Append(Uint64Value(0))
		require.NoError(t, err)

		err = array.Append(array2)
		require.NoError(t, err)

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, uint64(1), array2.Count())

		// Expected serialized slab data with storage id
		expected := map[StorageID][]byte{

			// (metadata slab) headers: [{id:2 size:50 count:10} {id:3 size:50 count:10} ]
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
				0x00, 0x00, 0x00, 0x40,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x00, 0x56,
			},

			// (data slab) prev: 0, next: 3, data: [0 1 2 ... 7 8 9]
			id2: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// prev storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x09,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
				0xd8, 0xa4, 0x01,
				0xd8, 0xa4, 0x02,
				0xd8, 0xa4, 0x03,
				0xd8, 0xa4, 0x04,
				0xd8, 0xa4, 0x05,
				0xd8, 0xa4, 0x06,
				0xd8, 0xa4, 0x07,
				0xd8, 0xa4, 0x08,
			},

			// (data slab) prev: 2, next: 0, data: [10 11 12 ... 17 18 19]
			id3: {
				// version
				0x00,
				// array data slab flag
				0x40,
				// prev storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0b,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x09,
				0xd8, 0xa4, 0x0a,
				0xd8, 0xa4, 0x0b,
				0xd8, 0xa4, 0x0c,
				0xd8, 0xa4, 0x0d,
				0xd8, 0xa4, 0x0e,
				0xd8, 0xa4, 0x0f,
				0xd8, 0xa4, 0x10,
				0xd8, 0xa4, 0x11,
				0xd8, 0xa4, 0x12,
				//0xd8, 0xa4, 0x13,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
			},

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
				// prev storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x01,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		//require.Equal(t, expected, m)
		require.Equal(t, expected[id1], m[id1])
		require.Equal(t, expected[id2], m[id2])
		require.Equal(t, expected[id3], m[id3])
		require.Equal(t, expected[id4], m[id4])
	})
}

func TestDecodeEncode(t *testing.T) {

	SetThreshold(60)
	defer func() {
		SetThreshold(1024)
	}()

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	id1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
	id2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
	id3 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}

	data := map[StorageID][]byte{

		// (metadata slab) headers: [{id:2 size:50 count:10} {id:3 size:50 count:10} ]
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
			0x00, 0x00, 0x00, 0x40,
			// child header 2
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			0x00, 0x00, 0x00, 0x0b,
			0x00, 0x00, 0x00, 0x46,
		},

		// (data slab) prev: 0, next: 3, data: [0 1 2 ... 7 8 9]
		id2: {
			// version
			0x00,
			// array data slab flag
			0x00,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x09,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00,
			0xd8, 0xa4, 0x01,
			0xd8, 0xa4, 0x02,
			0xd8, 0xa4, 0x03,
			0xd8, 0xa4, 0x04,
			0xd8, 0xa4, 0x05,
			0xd8, 0xa4, 0x06,
			0xd8, 0xa4, 0x07,
			0xd8, 0xa4, 0x08,
		},

		// (data slab) prev: 2, next: 0, data: [10 11 12 ... 17 18 19]
		id3: {
			// version
			0x00,
			// array data slab flag
			0x00,
			// prev storage id
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0b,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x09,
			0xd8, 0xa4, 0x0a,
			0xd8, 0xa4, 0x0b,
			0xd8, 0xa4, 0x0c,
			0xd8, 0xa4, 0x0d,
			0xd8, 0xa4, 0x0e,
			0xd8, 0xa4, 0x0f,
			0xd8, 0xa4, 0x10,
			0xd8, 0xa4, 0x11,
			0xd8, 0xa4, 0x12,
			0xd8, 0xa4, 0x13,
		},
	}

	// Decode serialized slabs and store them in storage
	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	storage := NewBasicSlabStorage(encMode, decMode)
	storage.DecodeStorable = decodeStorable

	err = storage.Load(data)
	require.NoError(t, err)

	// Check metadata slab (storage id 1)
	slab, err := getArraySlab(storage, id1)
	require.NoError(t, err)
	require.False(t, slab.IsData())
	require.NotNil(t, slab.ExtraData())
	require.Equal(t, typeInfo, slab.ExtraData().TypeInfo)

	meta := slab.(*ArrayMetaDataSlab)
	require.Equal(t, 2, len(meta.childrenHeaders))
	require.Equal(t, id2, meta.childrenHeaders[0].id)
	require.Equal(t, id3, meta.childrenHeaders[1].id)

	// Check data slab (storage id 2)
	slab, err = getArraySlab(storage, id2)
	require.NoError(t, err)
	require.True(t, slab.IsData())
	require.Nil(t, slab.ExtraData())

	dataSlab := slab.(*ArrayDataSlab)
	require.Equal(t, 9, len(dataSlab.elements))
	require.Equal(t, Uint64Value(0), dataSlab.elements[0])
	require.Equal(t, Uint64Value(8), dataSlab.elements[8])

	// Check data slab (storage id 3)
	slab, err = getArraySlab(storage, id3)
	require.NoError(t, err)
	require.True(t, slab.IsData())
	require.Nil(t, slab.ExtraData())

	dataSlab = slab.(*ArrayDataSlab)
	require.Equal(t, 11, len(dataSlab.elements))
	require.Equal(t, Uint64Value(9), dataSlab.elements[0])
	require.Equal(t, Uint64Value(19), dataSlab.elements[10])

	// Encode storaged slabs and compare encoded bytes
	encodedData, err := storage.Encode()
	require.NoError(t, err)
	require.Equal(t, data, encodedData)
}

func TestDecodeEncodeRandomData(t *testing.T) {
	const (
		Uint8Type = iota
		Uint16Type
		Uint32Type
		Uint64Type
		MaxType
	)

	SetThreshold(60)
	defer func() {
		SetThreshold(1024)
	}()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := NewBasicSlabStorage(encMode, decMode)
	storage.DecodeStorable = decodeStorable

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	const arraySize = 256 * 256
	values := make([]Value, arraySize)
	for i := uint64(0); i < arraySize; i++ {

		var v Value

		switch rand.Intn(MaxType) {
		case Uint8Type:
			n := rand.Intn(math.MaxUint8 + 1)
			v = Uint8Value(n)
		case Uint16Type:
			n := rand.Intn(math.MaxUint16 + 1)
			v = Uint16Value(n)
		case Uint32Type:
			v = Uint32Value(rand.Uint32())
		case Uint64Type:
			v = Uint64Value(rand.Uint64())
		}

		values[i] = v

		err := array.Append(v)
		require.NoError(t, err)
	}

	require.Equal(t, typeInfo, array.Type())

	verified, err := array.valid(typeInfo)
	require.NoError(t, err)
	require.True(t, verified)

	rootID := array.root.Header().id

	// Encode slabs with random data of mixed types
	m1, err := storage.Encode()
	require.NoError(t, err)

	// Decode data to new storage
	storage2 := NewBasicSlabStorage(encMode, decMode)
	storage2.DecodeStorable = decodeStorable

	err = storage2.Load(m1)
	require.NoError(t, err)

	// Create new array from new storage
	array2, err := NewArrayWithRootID(storage2, rootID)
	require.NoError(t, err)

	require.Equal(t, typeInfo, array2.Type())

	// Get and check every element from new array.
	for i := uint64(0); i < arraySize; i++ {
		e, err := array2.Get(i)
		require.NoError(t, err)
		require.Equal(t, values[i], e)
	}
}

func TestEmptyArray(t *testing.T) {

	t.Parallel()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	storage := NewBasicSlabStorage(encMode, decMode)
	storage.DecodeStorable = decodeStorable

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	rootID := array.root.Header().id

	require.Equal(t, 1, storage.Count())

	t.Run("get", func(t *testing.T) {
		_, err := array.Get(0)
		require.Error(t, err, IndexOutOfRangeError{})
	})

	t.Run("set", func(t *testing.T) {
		err := array.Set(0, Uint64Value(0))
		require.Error(t, err, IndexOutOfRangeError{})
	})

	t.Run("insert", func(t *testing.T) {
		err := array.Insert(1, Uint64Value(0))
		require.Error(t, err, IndexOutOfRangeError{})
	})

	t.Run("remove", func(t *testing.T) {
		_, err := array.Remove(0)
		require.Error(t, err, IndexOutOfRangeError{})
	})

	t.Run("iterate", func(t *testing.T) {
		i := uint64(0)
		err := array.Iterate(func(v Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
	})

	t.Run("count", func(t *testing.T) {
		count := array.Count()
		require.Equal(t, uint64(0), count)
	})

	t.Run("type", func(t *testing.T) {
		require.Equal(t, typeInfo, array.Type())
	})

	t.Run("encode", func(t *testing.T) {
		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 1, len(m))

		data, ok := m[rootID]
		require.True(t, ok)

		expectedData := []byte{
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
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x00,
		}

		require.Equal(t, expectedData, data)
	})

	t.Run("decode", func(t *testing.T) {
		array2, err := NewArrayWithRootID(storage, rootID)
		require.NoError(t, err)
		require.True(t, array2.root.IsData())
		require.Equal(t, uint32(0), array2.root.Header().count)
		require.Equal(t, typeInfo, array2.Type())
		require.Equal(t, uint32(arrayDataSlabPrefixSize), array2.root.Header().size)
	})
}

func TestStringElement(t *testing.T) {

	t.Parallel()

	t.Run("inline", func(t *testing.T) {

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		const arraySize = 256 * 256

		const stringSize = 32

		strs := make([]string, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			s := randStr(stringSize)
			strs[i] = s
		}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(NewStringValue(strs[i]))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(*StringValue)
			require.True(t, ok)
			require.Equal(t, strs[i], v.str)
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	t.Run("external slab", func(t *testing.T) {

		typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

		const arraySize = 256 * 256

		const stringSize = 512

		strs := make([]string, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			s := randStr(stringSize)
			strs[i] = s
		}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(NewStringValue(strs[i]))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(*StringValue)
			require.True(t, ok)
			require.Equal(t, strs[i], v.str)
		}

		require.Equal(t, typeInfo, array.Type())

		verified, err := array.valid(typeInfo)
		require.NoError(t, err)
		require.True(t, verified)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := array.Stats()
		require.Equal(t,
			stats.DataSlabCount+stats.MetaDataSlabCount+arraySize,
			uint64(array.Storage.Count()),
		)
	})
}
