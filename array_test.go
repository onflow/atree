/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Seed only once and print seed for easier debugging.
func init() {
	seed := time.Now().UnixNano()
	fmt.Printf("seed: %d\n", seed)
	rand.Seed(seed)
}

func resetStorageID() {
	storageIDCounter = 0
}

func TestAppendAndGet(t *testing.T) {

	const arraySize = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)

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

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
}

func TestSetAndGet(t *testing.T) {

	const arraySize = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)

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

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
}

func TestInsertAndGet(t *testing.T) {
	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

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

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

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

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

	t.Run("insert", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

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

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})
}

func TestRemove(t *testing.T) {
	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("remove-first", func(t *testing.T) {
		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		for i := uint64(0); i < arraySize; i++ {
			v, err := array.Remove(0)
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))

			require.Equal(t, arraySize-i-1, array.Count())

			if i%256 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

	t.Run("remove-last", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		for i := arraySize - 1; i >= 0; i-- {
			v, err := array.Remove(uint64(i))
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, uint64(i), uint64(e))

			require.Equal(t, uint64(i), array.Count())

			if i%256 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

	t.Run("remove", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		// Remove every other elements
		for i := uint64(0); i < array.Count(); i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, err := array.Remove(i)
			require.NoError(t, err)

			require.Equal(t, e, v)

			if i%256 == 0 {
				verified, err := array.valid()
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

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})
}

func TestSplit(t *testing.T) {
	t.Run("data slab as root", func(t *testing.T) {
		const arraySize = 50

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.NotNil(t, array.root)
		require.True(t, array.root.IsData())
		require.Equal(t, uint32(50), array.root.Header().count)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

	t.Run("metdata slab as root", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		const arraySize = 50

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.NotNil(t, array.root)
		require.False(t, array.root.IsData())
		require.Equal(t, uint32(50), array.root.Header().count)

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

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})
}

func TestIterate(t *testing.T) {
	t.Run("append", func(t *testing.T) {
		setThreshold(60)
		defer func() {
			setThreshold(1024)
		}()

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		err := array.Iterate(func(v Storable) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))
			i++
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(arraySize))
	})

	t.Run("set", func(t *testing.T) {
		setThreshold(60)
		defer func() {
			setThreshold(1024)
		}()

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			err := array.Set(i, Uint64Value(i*10))
			require.NoError(t, err)
		}

		i := uint64(0)
		err := array.Iterate(func(v Storable) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i*10, uint64(e))
			i++
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(arraySize))
	})

	t.Run("insert", func(t *testing.T) {
		setThreshold(60)
		defer func() {
			setThreshold(1024)
		}()

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i += 2 {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(1); i < arraySize; i += 2 {
			err := array.Insert(i, Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		err := array.Iterate(func(v Storable) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))
			i++
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(arraySize))
	})

	t.Run("remove", func(t *testing.T) {
		setThreshold(60)
		defer func() {
			setThreshold(1024)
		}()

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

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
		err := array.Iterate(func(v Storable) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))
			i += 2
		})
		require.NoError(t, err)
	})
}

func TestConstRootStorageID(t *testing.T) {
	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)
	err := array.Append(Uint64Value(0))
	require.NoError(t, err)

	savedRootID := array.root.Header().id
	require.NotEqual(t, StorageIDUndefined, savedRootID)

	for i := uint64(1); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	rootID := array.root.Header().id
	require.Equal(t, savedRootID, rootID)

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i, uint64(v))
	}
}

func TestSetRandomValue(t *testing.T) {

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)

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

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
}

func TestInsertRandomValue(t *testing.T) {

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

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

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

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

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

	t.Run("insert-random", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		array := NewArray(storage)

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

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
	})

}

func TestRemoveRandomElement(t *testing.T) {

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)

	values := make([]uint64, arraySize)

	// Insert n random values into array
	for i := uint64(0); i < arraySize; i++ {
		v := rand.Uint64()
		values[i] = v

		err := array.Insert(i, Uint64Value(v))
		require.NoError(t, err)
	}

	require.Equal(t, uint64(arraySize), array.Count())

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

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
}

func TestRandomAppendSetInsertRemove(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	const actionCount = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)

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
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, v, uint64(ev))
	}

	i := 0
	err := array.Iterate(func(v Storable) {
		e, ok := v.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, values[i], uint64(e))
		i++
	})
	require.NoError(t, err)
	require.Equal(t, len(values), i)

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
}

func TestRandomAppendSetInsertRemoveUint8(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	const actionCount = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)

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
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint8Value)
		require.True(t, ok)
		require.Equal(t, v, uint8(ev))
	}

	i := 0
	err := array.Iterate(func(v Storable) {
		e, ok := v.(Uint8Value)
		require.True(t, ok)
		require.Equal(t, values[i], uint8(e))
		i++
	})
	require.NoError(t, err)
	require.Equal(t, len(values), i)

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
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

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	const actionCount = 256 * 256

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage)

	array := NewArray(storage)

	values := make([]Storable, 0, actionCount)

	for i := uint64(0); i < actionCount; i++ {

		var v Storable

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

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	i := 0
	err := array.Iterate(func(v Storable) {
		require.Equal(t, values[i], v)
		i++
	})
	require.NoError(t, err)
	require.Equal(t, len(values), i)

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.storage.Count()))
}

func TestNestedArray(t *testing.T) {

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("small", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		nestedArrays := make([]Storable, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested := NewArray(storage)

			err := nested.Append(Uint64Value(i * 2))
			require.NoError(t, err)

			err = nested.Append(Uint64Value(i*2 + 1))
			require.NoError(t, err)

			require.True(t, nested.root.IsData())

			nestedArrays[i] = nested.root
		}

		array := NewArray(storage)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})

	t.Run("big", func(t *testing.T) {

		const arraySize = 256 * 256

		baseStorage := NewInMemBaseStorage()

		storage := NewPersistentSlabStorage(baseStorage)

		nestedArrays := make([]Storable, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested := NewArray(storage)
			for i := uint64(0); i < 50; i++ {
				err := nested.Append(Uint64Value(i))
				require.NoError(t, err)
			}
			require.False(t, nested.root.IsData())

			nestedArrays[i] = nested.root
		}

		array := NewArray(storage)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})
}

func TestEncode(t *testing.T) {

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	// Reset storage id for consistent storage id
	resetStorageID()

	// Create and populate array in memory
	storage := NewBasicSlabStorage()
	array := NewArray(storage)

	const arraySize = 20
	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(arraySize), array.Count())

	// Expected serialized slab data with storage id
	expected := map[StorageID][]byte{
		1:
		// (metadata slab) headers: [{id:2 size:50 count:10} {id:3 size:50 count:10} ]
		{
			// array meta data slab flag
			0x05,
			// child header count
			0x00, 0x00, 0x00, 0x02,
			// child header 1 (storage id, count, size)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,
			// child header 2
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,
		},
		2:
		// (data slab) prev: 0, next: 3, data: [0 1 2 ... 7 8 9]
		{
			// array data slab flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x08, 0xd8, 0xa4, 0x09,
		},
		3:
		// (data slab) prev: 2, next: 0, data: [10 11 12 ... 17 18 19]
		{
			// array data slab flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x0a, 0xd8, 0xa4, 0x0b, 0xd8, 0xa4, 0x0c, 0xd8, 0xa4, 0x0d, 0xd8, 0xa4, 0x0e, 0xd8, 0xa4, 0x0f, 0xd8, 0xa4, 0x10, 0xd8, 0xa4, 0x11, 0xd8, 0xa4, 0x12, 0xd8, 0xa4, 0x13,
		},
	}

	m, err := storage.Encode()
	require.NoError(t, err)
	require.Equal(t, expected, m)
}

func TestDecodeEncode(t *testing.T) {

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	data := map[StorageID][]byte{
		1:
		// (metadata slab) headers: [{id:2 size:50 count:10} {id:3 size:50 count:10} ]
		{
			// array meta data slab flag
			0x05,
			// child header count
			0x00, 0x00, 0x00, 0x02,
			// child header 1 (storage id, count, size)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,
			// child header 2
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,
		},
		2:
		// (data slab) prev: 0, next: 3, data: [0 1 2 ... 7 8 9]
		{
			// array data slab flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x08, 0xd8, 0xa4, 0x09,
		},
		3:
		// (data slab) prev: 2, next: 0, data: [10 11 12 ... 17 18 19]
		{
			// array data slab flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x0a, 0xd8, 0xa4, 0x0b, 0xd8, 0xa4, 0x0c, 0xd8, 0xa4, 0x0d, 0xd8, 0xa4, 0x0e, 0xd8, 0xa4, 0x0f, 0xd8, 0xa4, 0x10, 0xd8, 0xa4, 0x11, 0xd8, 0xa4, 0x12, 0xd8, 0xa4, 0x13,
		},
	}

	// Decode serialized slabs and store them in storage
	storage := NewBasicSlabStorage()
	err := storage.Load(data)
	require.NoError(t, err)

	// Check metadata slab (storage id 1)
	slab, err := getArraySlab(storage, 1)
	require.NoError(t, err)
	require.False(t, slab.IsData())

	meta := slab.(*ArrayMetaDataSlab)
	require.Equal(t, 2, len(meta.childrenHeaders))
	require.Equal(t, StorageID(2), meta.childrenHeaders[0].id)
	require.Equal(t, StorageID(3), meta.childrenHeaders[1].id)

	// Check data slab (storage id 2)
	slab, err = getArraySlab(storage, 2)
	require.NoError(t, err)
	require.True(t, slab.IsData())

	dataSlab := slab.(*ArrayDataSlab)
	require.Equal(t, 10, len(dataSlab.elements))
	require.Equal(t, Uint64Value(0), dataSlab.elements[0])
	require.Equal(t, Uint64Value(9), dataSlab.elements[9])

	// Check data slab (storage id 3)
	slab, err = getArraySlab(storage, 3)
	require.NoError(t, err)
	require.True(t, slab.IsData())

	dataSlab = slab.(*ArrayDataSlab)
	require.Equal(t, 10, len(dataSlab.elements))
	require.Equal(t, Uint64Value(10), dataSlab.elements[0])
	require.Equal(t, Uint64Value(19), dataSlab.elements[9])

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

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	storage := NewBasicSlabStorage()
	array := NewArray(storage)

	const arraySize = 256 * 256
	values := make([]Storable, arraySize)
	for i := uint64(0); i < arraySize; i++ {

		var v Storable

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

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	rootID := array.root.Header().id

	// Encode slabs with random data of mixed types
	m1, err := storage.Encode()
	require.NoError(t, err)

	// Decode data to new storage
	storage2 := NewBasicSlabStorage()
	err = storage2.Load(m1)
	require.NoError(t, err)

	// Create new array from new storage
	array2, err := NewArrayWithRootID(storage2, rootID)
	require.NoError(t, err)

	// Get and check every element from new array.
	for i := uint64(0); i < arraySize; i++ {
		e, err := array2.Get(i)
		require.NoError(t, err)
		require.Equal(t, values[i], e)
	}
}

func TestEmptyArray(t *testing.T) {

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

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
		err := array.Iterate(func(v Storable) {
			i++
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
	})

	t.Run("count", func(t *testing.T) {
		count := array.Count()
		require.Equal(t, uint64(0), count)
	})

	t.Run("encode", func(t *testing.T) {
		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 1, len(m))

		data, ok := m[rootID]
		require.True(t, ok)

		expectedData := []byte{
			// array data slab flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
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
		require.Equal(t, uint32(dataSlabPrefixSize), array2.root.Header().size)
	})
}
