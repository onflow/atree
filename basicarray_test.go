/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"math"
	"math/rand"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

func newTestBasicStorage(t testing.TB) *BasicSlabStorage {
	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	//baseStorage := NewInMemBaseStorage()

	//storage := NewPersistentSlabStorage(baseStorage)

	storage := NewBasicSlabStorage(encMode, decMode, decodeStorable, decodeTypeInfo)
	return storage
}

func TestBasicArrayAppendAndGet(t *testing.T) {

	const arraySize = 1024 * 16

	storage := newTestBasicStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array := NewBasicArray(storage, address)

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
}

func TestBasicArraySetAndGet(t *testing.T) {

	const arraySize = 1024 * 16

	storage := newTestBasicStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array := NewBasicArray(storage, address)

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
}

func TestBasicArrayInsertAndGet(t *testing.T) {
	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 1024 * 16

		storage := newTestBasicStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array := NewBasicArray(storage, address)

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
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 1024 * 16

		storage := newTestBasicStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array := NewBasicArray(storage, address)

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
	})

	t.Run("insert", func(t *testing.T) {

		const arraySize = 1024 * 16

		storage := newTestBasicStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array := NewBasicArray(storage, address)

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
	})
}

func TestBasicArrayRemove(t *testing.T) {

	t.Run("remove-first", func(t *testing.T) {

		const arraySize = 1024 * 16

		storage := newTestBasicStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array := NewBasicArray(storage, address)

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
		}

		require.Equal(t, uint64(0), array.Count())
	})

	t.Run("remove-last", func(t *testing.T) {

		const arraySize = 1024 * 16

		storage := newTestBasicStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array := NewBasicArray(storage, address)

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
		}

		require.Equal(t, uint64(0), array.Count())
	})

	t.Run("remove", func(t *testing.T) {

		const arraySize = 1024 * 16

		storage := newTestBasicStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array := NewBasicArray(storage, address)

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
		}

		for i, j := uint64(0), uint64(1); i < array.Count(); i, j = i+1, j+2 {
			v, err := array.Get(i)
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, j, uint64(e))
		}
	})
}

func TestBasicArrayRandomAppendSetInsertRemoveMixedTypes(t *testing.T) {

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

	const actionCount = 1024 * 16

	storage := newTestBasicStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array := NewBasicArray(storage, address)

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
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}
}

func TestBasicArrayDecodeEncodeRandomData(t *testing.T) {
	const (
		Uint8Type = iota
		Uint16Type
		Uint32Type
		Uint64Type
		MaxType
	)

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	storage := NewBasicSlabStorage(encMode, decMode, decodeStorable, decodeTypeInfo)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array := NewBasicArray(storage, address)

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

	rootID := array.root.Header().id

	// Encode slabs with random data of mixed types
	m1, err := storage.Encode()
	require.NoError(t, err)

	// Decode data to new storage

	storage2 := NewBasicSlabStorage(encMode, decMode, decodeStorable, decodeTypeInfo)

	err = storage2.Load(m1)
	require.NoError(t, err)

	// Create new array from new storage
	array2, err := NewBasicArrayWithRootID(storage2, rootID)
	require.NoError(t, err)

	// Get and check every element from new array.
	for i := uint64(0); i < arraySize; i++ {
		e, err := array2.Get(i)
		require.NoError(t, err)
		require.Equal(t, values[i], e)
	}
}
