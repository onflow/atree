/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2021 Dapper Labs, Inc.
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
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
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

	return NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		decodeStorable,
		decodeTypeInfo,
	)
}

func TestArrayAppendAndGet(t *testing.T) {

	t.Parallel()

	typeInfo := testTypeInfo{42}

	const arraySize = 1024 * 4

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

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := GetArrayStats(array)
	require.Equal(t,
		stats.DataSlabCount+stats.MetaDataSlabCount,
		uint64(array.Storage.Count()),
	)
}

func TestArraySetAndGet(t *testing.T) {

	t.Run("set", func(t *testing.T) {
		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			existingStorable, err := array.Set(i, Uint64Value(i*10))
			require.NoError(t, err)

			existingElem, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingElem)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i*10, uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("root-split", func(t *testing.T) {
		const arraySize = 60

		SetThreshold(100)
		defer func() {
			SetThreshold(1024)
		}()

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			// Overwrite elements with larger values that require more bytes to make slab split
			existingStorable, err := array.Set(i, Uint64Value(math.MaxUint64-i))
			require.NoError(t, err)

			existingElem, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingElem)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, math.MaxUint64-i, uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("child-as-new-root", func(t *testing.T) {
		const arraySize = 20

		SetThreshold(128)
		defer func() {
			SetThreshold(1024)
		}()

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(NewStringValue(randStr(40)))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			// Overwrite elements with smaller values that require less bytes to make slab merge
			existingStorable, err := array.Set(i, Uint64Value(i))
			require.NoError(t, err)

			existingElem, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.NotNil(t, existingElem)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(v))
		}

		require.Equal(t, typeInfo, array.Type())

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestArrayInsertAndGet(t *testing.T) {
	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestArrayRemove(t *testing.T) {
	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("remove-first", func(t *testing.T) {
		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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
				err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
				if err != nil {
					PrintArray(array)
				}
				require.NoError(t, err)

				err = validArraySerialization(array, storage)
				if err != nil {
					PrintArray(array)
				}
				require.NoError(t, err)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("remove-last", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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
				err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
				if err != nil {
					PrintArray(array)
				}
				require.NoError(t, err)

				err = validArraySerialization(array, storage)
				if err != nil {
					PrintArray(array)
				}
				require.NoError(t, err)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("remove", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

			removedStorable, err := array.Remove(i)
			require.NoError(t, err)

			require.Equal(t, e, removedStorable)

			require.Equal(t, typeInfo, array.Type())

			if i%256 == 0 {
				err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
				if err != nil {
					PrintArray(array)
				}
				require.NoError(t, err)

				err = validArraySerialization(array, storage)
				if err != nil {
					PrintArray(array)
				}
				require.NoError(t, err)
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

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestArraySplit(t *testing.T) {
	t.Run("data slab as root", func(t *testing.T) {
		const arraySize = 50

		typeInfo := testTypeInfo{42}

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

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("metdata slab as root", func(t *testing.T) {
		SetThreshold(60)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 50

		typeInfo := testTypeInfo{42}

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

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestArrayIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

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

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			existingStorable, err := array.Set(i, Uint64Value(i*10))
			require.NoError(t, err)

			existingElem, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingElem)
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

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		typeInfo := testTypeInfo{42}

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

		typeInfo := testTypeInfo{42}

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

func TestArrayConstRootStorageID(t *testing.T) {
	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 1024 * 4

	typeInfo := testTypeInfo{42}

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

func TestArraySetRandomValue(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 1024 * 4

	typeInfo := testTypeInfo{42}

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

		oldV := values[k]

		values[k] = v

		existingStorable, err := array.Set(uint64(k), Uint64Value(v))
		require.NoError(t, err)

		existingElem, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(oldV), existingElem)
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, v, uint64(ev))
	}

	require.Equal(t, typeInfo, array.Type())

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := GetArrayStats(array)
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestArrayInsertRandomValue(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})

	t.Run("insert-random", func(t *testing.T) {

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

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

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
	})
}

func TestArrayRemoveRandomElement(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 1024 * 4

	typeInfo := testTypeInfo{42}

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

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := GetArrayStats(array)
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestArrayRandomAppendSetInsertRemove(t *testing.T) {

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

	const actionCount = 1024 * 4

	typeInfo := testTypeInfo{42}

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

			oldV := values[k]

			values[k] = v

			existingStorable, err := array.Set(uint64(k), Uint64Value(v))
			require.NoError(t, err)

			existingElem, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(oldV), existingElem)

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

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := GetArrayStats(array)
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestArrayRandomAppendSetInsertRemoveUint8(t *testing.T) {

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

	const actionCount = 1024 * 4

	typeInfo := testTypeInfo{42}

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

			oldV := values[k]

			values[k] = uint8(v)

			existingStorable, err := array.Set(uint64(k), Uint8Value(v))
			require.NoError(t, err)

			existingElem, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint8Value(oldV), existingElem)

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

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := GetArrayStats(array)
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestArrayRandomAppendSetInsertRemoveMixedTypes(t *testing.T) {

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

	const actionCount = 1024 * 4

	typeInfo := testTypeInfo{42}

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

			oldV := values[k]

			values[k] = v

			existingStorable, err := array.Set(uint64(k), v)
			require.NoError(t, err)

			existingElem, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, oldV, existingElem)

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

				s, err := array.Remove(uint64(k))
				require.NoError(t, err)

				v, err := s.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, values[k], v)

				copy(values[k:], values[k+1:])
				values = values[:len(values)-1]
			}
		}

		require.Equal(t, array.Count(), uint64(len(values)))
		require.Equal(t, typeInfo, array.Type())
	}

	for k, v := range values {
		s, err := array.Get(uint64(k))
		require.NoError(t, err)

		e, err := s.StoredValue(storage)
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

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = storage.Commit()
	require.NoError(t, err)

	stats, _ := GetArrayStats(array)
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(array.Storage.Count()))
}

func TestNestedArray(t *testing.T) {

	SetThreshold(100)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("small", func(t *testing.T) {

		const arraySize = 1024 * 4

		nestedTypeInfo := testTypeInfo{43}

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

		typeInfo := testTypeInfo{42}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			s, err := array.Get(i)
			require.NoError(t, err)

			e, err := s.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		require.Equal(t, typeInfo, array.Type())

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)
	})

	t.Run("big", func(t *testing.T) {

		const arraySize = 1024 * 4

		nestedTypeInfo := testTypeInfo{43}

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

		typeInfo := testTypeInfo{42}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			s, err := array.Get(i)
			require.NoError(t, err)

			e, err := s.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		require.Equal(t, typeInfo, array.Type())

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)
	})
}

func TestArrayEncode(t *testing.T) {

	SetThreshold(60)
	defer func() {
		SetThreshold(1024)
	}()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	t.Run("no pointers", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		// Create and populate array in memory
		storage := NewBasicSlabStorage(encMode, decMode, decodeStorable, decodeTypeInfo)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 30
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

			// (metadata slab) headers: [{id:2 size:66 count:15} {id:3 size:72 count:15} ]
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
				0x00, 0x00, 0x00, 0x0f,
				0x00, 0x00, 0x00, 0x42,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x0f,
				0x00, 0x00, 0x00, 0x48,
			},

			// (data slab) next: 3, data: [0 1 2 ... 9 10 11 12 13 14]
			id2: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0f,
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
				0xd8, 0xa4, 0x09,
				0xd8, 0xa4, 0x0a,
				0xd8, 0xa4, 0x0b,
				0xd8, 0xa4, 0x0c,
				0xd8, 0xa4, 0x0d,
				0xd8, 0xa4, 0x0e,
			},

			// (data slab) next: 0, data: [15 16 17 ... 27 28 29]
			id3: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0f,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x0f,
				0xd8, 0xa4, 0x10,
				0xd8, 0xa4, 0x11,
				0xd8, 0xa4, 0x12,
				0xd8, 0xa4, 0x13,
				0xd8, 0xa4, 0x14,
				0xd8, 0xa4, 0x15,
				0xd8, 0xa4, 0x16,
				0xd8, 0xa4, 0x17,
				0xd8, 0xa4, 0x18, 0x18,
				0xd8, 0xa4, 0x18, 0x19,
				0xd8, 0xa4, 0x18, 0x1a,
				0xd8, 0xa4, 0x18, 0x1b,
				0xd8, 0xa4, 0x18, 0x1c,
				0xd8, 0xa4, 0x18, 0x1d,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 3, len(m))
		require.Equal(t, expected[id1], m[id1])
		require.Equal(t, expected[id2], m[id2])
		require.Equal(t, expected[id3], m[id3])
	})

	t.Run("has pointers", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		// Create and populate array in memory
		storage := NewBasicSlabStorage(encMode, decMode, decodeStorable, decodeTypeInfo)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		id1 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		id4 := StorageID{Address: address, Index: StorageIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 25
		for i := uint64(0); i < arraySize-1; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		typeInfo2 := testTypeInfo{43}

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

			// (metadata slab) headers: [{id:2 size:66 count:15} {id:3 size:67 count:10} ]
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
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x0f,
				0x00, 0x00, 0x00, 0x42,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0x00, 0x00, 0x0a,
				0x00, 0x00, 0x00, 0x43,
			},

			// (data slab) next: 3, data: [0 1 2 ... 9 10 11]
			id3: {
				// version
				0x00,
				// array data slab flag
				0x00,
				// next storage id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0f,
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
				0xd8, 0xa4, 0x09,
				0xd8, 0xa4, 0x0a,
				0xd8, 0xa4, 0x0b,
				0xd8, 0xa4, 0x0c,
				0xd8, 0xa4, 0x0d,
				0xd8, 0xa4, 0x0e,
			},

			// (data slab) next: 0, data: [12 13 14 ... 22 23 StorageID(...)]
			id4: {
				// version
				0x00,
				// array data slab flag
				0x40,
				// next storage id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x0a,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x0f,
				0xd8, 0xa4, 0x10,
				0xd8, 0xa4, 0x11,
				0xd8, 0xa4, 0x12,
				0xd8, 0xa4, 0x13,
				0xd8, 0xa4, 0x14,
				0xd8, 0xa4, 0x15,
				0xd8, 0xa4, 0x16,
				0xd8, 0xa4, 0x17,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			},
			id2: {
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

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 4, len(m))
		require.Equal(t, expected[id1], m[id1])
		require.Equal(t, expected[id2], m[id2])
		require.Equal(t, expected[id3], m[id3])
		require.Equal(t, expected[id4], m[id4])
	})
}

func TestArrayDecodeEncode(t *testing.T) {

	SetThreshold(60)
	defer func() {
		SetThreshold(1024)
	}()

	typeInfo := testTypeInfo{42}

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

		// (data slab) next: 3, data: [0 1 2 ... 7 8 9]
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

		// (data slab) next: 0, data: [10 11 12 ... 17 18 19]
		id3: {
			// version
			0x00,
			// array data slab flag
			0x00,
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

	storage := NewBasicSlabStorage(encMode, decMode, decodeStorable, decodeTypeInfo)

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

func TestArrayDecodeEncodeRandomData(t *testing.T) {
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

	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	const arraySize = 1024 * 4
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

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	rootID := array.root.Header().id

	err = storage.Commit()
	require.NoError(t, err)

	storage.DropCache()

	// Create new array from storage
	array2, err := NewArrayWithRootID(storage, rootID)
	require.NoError(t, err)

	require.Equal(t, typeInfo, array2.Type())

	// Get and check every element from new array.
	for i := uint64(0); i < arraySize; i++ {
		s, err := array2.Get(i)
		require.NoError(t, err)

		e, err := s.StoredValue(storage)
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

	typeInfo := testTypeInfo{42}

	storage := NewBasicSlabStorage(encMode, decMode, decodeStorable, decodeTypeInfo)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	rootID := array.root.Header().id

	require.Equal(t, 1, storage.Count())

	t.Run("get", func(t *testing.T) {
		s, err := array.Get(0)
		require.Error(t, err, IndexOutOfBoundsError{})
		require.Nil(t, s)
	})

	t.Run("set", func(t *testing.T) {
		existingStorable, err := array.Set(0, Uint64Value(0))
		require.Error(t, err, IndexOutOfBoundsError{})
		require.Nil(t, existingStorable)
	})

	t.Run("insert", func(t *testing.T) {
		err := array.Insert(1, Uint64Value(0))
		require.Error(t, err, IndexOutOfBoundsError{})
	})

	t.Run("remove", func(t *testing.T) {
		s, err := array.Remove(0)
		require.Error(t, err, IndexOutOfBoundsError{})
		require.Nil(t, s)
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
		require.Equal(t, uint32(arrayRootDataSlabPrefixSize), array2.root.Header().size)
	})
}

func TestArrayStringElement(t *testing.T) {

	t.Parallel()

	t.Run("inline", func(t *testing.T) {

		typeInfo := testTypeInfo{42}

		const arraySize = 1024 * 4

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

			v, ok := e.(StringValue)
			require.True(t, ok)
			require.Equal(t, strs[i], v.str)
		}

		require.Equal(t, typeInfo, array.Type())

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	t.Run("external slab", func(t *testing.T) {

		typeInfo := testTypeInfo{42}

		const arraySize = 1024 * 4

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
			s, err := array.Get(i)
			require.NoError(t, err)

			e, err := s.StoredValue(storage)
			require.NoError(t, err)

			v, ok := e.(StringValue)
			require.True(t, ok)
			require.Equal(t, strs[i], v.str)
		}

		require.Equal(t, typeInfo, array.Type())

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		stats, _ := GetArrayStats(array)
		require.Equal(t,
			stats.DataSlabCount+stats.MetaDataSlabCount+arraySize,
			uint64(array.Storage.Count()),
		)
	})
}

func TestArrayStoredValue(t *testing.T) {

	const arraySize = 1024 * 4

	typeInfo := testTypeInfo{42}

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	storage := newTestPersistentStorage(t)

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	value, err := array.root.StoredValue(storage)
	require.NoError(t, err)

	array2, ok := value.(*Array)
	require.True(t, ok)

	require.Equal(t, uint64(arraySize), array2.Count())
	require.Equal(t, typeInfo, array2.Type())

	for i := uint64(0); i < arraySize; i++ {
		v, err := array2.Get(i)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i), v)
	}

	firstDataSlab, err := firstArrayDataSlab(storage, array.root)
	require.NoError(t, err)

	if firstDataSlab.ID() != array.StorageID() {
		_, err = firstDataSlab.StoredValue(storage)
		require.Error(t, err)
	}
}

func TestArrayPopIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())

		i := uint64(0)
		err = array.PopIterate(func(v Storable) {
			i++
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())
	})

	t.Run("root-dataslab", func(t *testing.T) {
		SetThreshold(1024)

		const arraySize = 10

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())

		i := uint64(0)
		err = array.PopIterate(func(v Storable) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, arraySize-i-1, uint64(e))
			i++
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(arraySize))

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		require.Equal(t, uint64(0), array.Count())
		require.Equal(t, typeInfo, array.Type())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())
	})

	t.Run("root-metaslab", func(t *testing.T) {
		SetThreshold(100)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 1024 * 4

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		require.True(t, storage.Count() > 1)

		i := uint64(0)
		err = array.PopIterate(func(v Storable) {
			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, uint64(arraySize)-i-1, uint64(e))
			i++
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), i)

		err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
		}
		require.NoError(t, err)

		require.Equal(t, uint64(0), array.Count())
		require.Equal(t, typeInfo, array.Type())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())
	})
}

func TestArrayFromBatchData(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(
			newTestPersistentStorage(t),
			address,
			typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())

		iter, err := array.Iterator()
		require.NoError(t, err)

		// Create a new array with new storage, new address, and original array's elements.
		address = Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})
		require.NoError(t, err)
		require.Equal(t, uint64(0), copied.Count())
		require.Equal(t, array.Type(), copied.Type())
		require.Equal(t, address, copied.Address())
		require.NotEqual(t, copied.StorageID(), array.StorageID())

		// Iterate through copied array to test data slab's next
		i := 0
		err = copied.Iterate(func(v Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)

		err = ValidArray(copied, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)

		err = validArraySerialization(copied, storage)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)
	})

	t.Run("root-dataslab", func(t *testing.T) {

		const arraySize = 10

		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		array, err := NewArray(
			newTestPersistentStorage(t),
			address,
			typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())

		iter, err := array.Iterator()
		require.NoError(t, err)

		// Create a new array with new storage, new address, and original array's elements.
		address = Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), copied.Count())
		require.Equal(t, typeInfo, copied.Type())
		require.Equal(t, address, copied.Address())
		require.NotEqual(t, copied.StorageID(), array.StorageID())

		// Get copied array's element to test tree traversal.
		for i := uint64(0); i < arraySize; i++ {
			storable, err := copied.Get(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), storable)
		}

		// Iterate through copied array to test data slab's next
		i := 0
		err = copied.Iterate(func(v Value) (bool, error) {
			require.Equal(t, Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		err = ValidArray(copied, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)

		err = validArraySerialization(copied, storage)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(
			newTestPersistentStorage(t),
			address,
			typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())

		iter, err := array.Iterator()
		require.NoError(t, err)

		address = Address{2, 3, 4, 5, 6, 7, 8, 9}

		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), copied.Count())
		require.Equal(t, typeInfo, copied.Type())
		require.Equal(t, address, copied.Address())
		require.NotEqual(t, array.StorageID(), copied.StorageID())

		// Get copied array's element to test tree traversal.
		for i := uint64(0); i < arraySize; i++ {
			storable, err := copied.Get(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), storable)
		}

		// Iterate through copied array to test data slab's next
		i := 0
		err = copied.Iterate(func(v Value) (bool, error) {
			require.Equal(t, Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		err = ValidArray(copied, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)

		err = validArraySerialization(copied, storage)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)
	})

	t.Run("rebalance two data slabs", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(
			newTestPersistentStorage(t),
			address,
			typeInfo)
		require.NoError(t, err)

		err = array.Insert(0, NewStringValue(strings.Repeat("a", int(MaxInlineArrayElementSize-2))))
		require.NoError(t, err)
		for i := 0; i < 35; i++ {
			err = array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(36), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())

		iter, err := array.Iterator()
		require.NoError(t, err)

		address = Address{2, 3, 4, 5, 6, 7, 8, 9}

		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.Equal(t, array.Count(), copied.Count())
		require.Equal(t, typeInfo, copied.Type())
		require.Equal(t, address, copied.Address())
		require.NotEqual(t, array.StorageID(), copied.StorageID())

		// Get copied array's element to test tree traversal.
		for i := uint64(0); i < array.Count(); i++ {
			originalStorable, err := array.Get(i)
			require.NoError(t, err)

			originalValue, err := originalStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			storable, err := copied.Get(i)
			require.NoError(t, err)

			value, err := storable.StoredValue(copied.Storage)
			require.NoError(t, err)

			require.Equal(t, originalValue, value)
		}

		// Iterate through copied array to test data slab's next
		i := uint64(0)
		err = copied.Iterate(func(v Value) (bool, error) {
			storable, err := array.Get(i)
			require.NoError(t, err)

			value, err := storable.StoredValue(array.Storage)
			require.NoError(t, err)

			require.Equal(t, value, v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, array.Count(), i)

		err = ValidArray(copied, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)

		err = validArraySerialization(copied, storage)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)
	})

	t.Run("merge two data slabs", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(
			newTestPersistentStorage(t),
			address,
			typeInfo)
		require.NoError(t, err)

		for i := 0; i < 35; i++ {
			err = array.Append(Uint64Value(i))
			require.NoError(t, err)
		}
		err = array.Insert(25, NewStringValue(strings.Repeat("a", int(MaxInlineArrayElementSize-2))))
		require.NoError(t, err)

		require.Equal(t, uint64(36), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())

		iter, err := array.Iterator()
		require.NoError(t, err)

		address = Address{2, 3, 4, 5, 6, 7, 8, 9}

		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.Equal(t, array.Count(), copied.Count())
		require.Equal(t, typeInfo, copied.Type())
		require.Equal(t, address, copied.Address())
		require.NotEqual(t, array.StorageID(), copied.StorageID())

		// Get copied array's element to test tree traversal.
		for i := uint64(0); i < array.Count(); i++ {
			originalStorable, err := array.Get(i)
			require.NoError(t, err)

			originalValue, err := originalStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			storable, err := copied.Get(i)
			require.NoError(t, err)

			value, err := storable.StoredValue(copied.Storage)
			require.NoError(t, err)

			require.Equal(t, originalValue, value)
		}

		// Iterate through copied array to test data slab's next
		i := uint64(0)
		err = copied.Iterate(func(v Value) (bool, error) {
			storable, err := array.Get(i)
			require.NoError(t, err)

			value, err := storable.StoredValue(array.Storage)
			require.NoError(t, err)

			require.Equal(t, value, v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, array.Count(), i)

		err = ValidArray(copied, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)

		err = validArraySerialization(copied, storage)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)
	})

	t.Run("random", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(
			newTestPersistentStorage(t),
			address,
			typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := RandomValue()
			values[i] = v

			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())

		iter, err := array.Iterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)

		address = Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), copied.Count())
		require.Equal(t, typeInfo, copied.Type())
		require.Equal(t, address, copied.Address())
		require.NotEqual(t, array.StorageID(), copied.StorageID())

		// Get copied array's element to test tree traversal.
		for i := uint64(0); i < arraySize; i++ {
			storable, err := copied.Get(i)
			require.NoError(t, err)

			v, err := storable.StoredValue(storage)
			require.NoError(t, err)

			require.Equal(t, values[i], v)
		}

		// Iterate through copied array to test data slab's next
		i := 0
		err = copied.Iterate(func(v Value) (bool, error) {
			require.Equal(t, values[i], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		err = ValidArray(copied, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)

		err = validArraySerialization(copied, storage)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)
	})

	t.Run("data slab too large", func(t *testing.T) {

		SetThreshold(100)
		defer func() {
			SetThreshold(1024)
		}()

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(
			newTestPersistentStorage(t),
			address,
			typeInfo)
		require.NoError(t, err)

		var values []Value
		var v Value

		v = NewStringValue(randStr(26))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		v = NewStringValue(randStr(int(MaxInlineArrayElementSize - 2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		v = NewStringValue(randStr(int(MaxInlineArrayElementSize - 2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		iter, err := array.Iterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)

		address = Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.StorageID(), copied.StorageID())
		require.Equal(t, array.Count(), copied.Count())
		require.Equal(t, typeInfo, copied.Type())
		require.Equal(t, address, copied.Address())

		// Get copied array's element to test tree traversal.
		for i := uint64(0); i < copied.Count(); i++ {
			storable, err := copied.Get(i)
			require.NoError(t, err)

			v, err := storable.StoredValue(storage)
			require.NoError(t, err)

			require.Equal(t, values[i], v)
		}

		// Iterate through copied array to test data slab's next
		i := uint64(0)
		err = copied.Iterate(func(v Value) (bool, error) {
			require.Equal(t, values[i], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, array.Count(), i)

		err = ValidArray(copied, typeInfo, typeInfoComparator, hashInputProvider)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)

		err = validArraySerialization(copied, storage)
		if err != nil {
			PrintArray(copied)
		}
		require.NoError(t, err)
	})
}

func validArraySerialization(array *Array, storage *PersistentSlabStorage) error {
	return ValidArraySerialization(
		array,
		storage.cborDecMode,
		storage.cborEncMode,
		storage.DecodeStorable,
		storage.DecodeTypeInfo,
		func(a, b Storable) bool {
			return reflect.DeepEqual(a, b)
		},
	)
}

func TestArrayNestedStorables(t *testing.T) {

	t.Parallel()

	typeInfo := testTypeInfo{42}

	const arraySize = 1024 * 4

	storage := newTestBasicStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {

		s := strings.Repeat("a", int(i))
		v := SomeValue{Value: NewStringValue(s)}

		err := array.Append(v)
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, err := e.StoredValue(storage)
		require.NoError(t, err)

		someValue, ok := v.(SomeValue)
		require.True(t, ok)

		s, ok := someValue.Value.(StringValue)
		require.True(t, ok)

		require.Equal(t, strings.Repeat("a", int(i)), s.str)
	}

	require.Equal(t, typeInfo, array.Type())

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = ValidArraySerialization(
		array,
		storage.cborDecMode,
		storage.cborEncMode,
		storage.DecodeStorable,
		storage.DecodeTypeInfo,
		func(a, b Storable) bool {
			return reflect.DeepEqual(a, b)
		},
	)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	_, err = CheckStorageHealth(storage, 1)
	if err != nil {
		fmt.Printf("CheckStorageHealth %s\n", err)
	}
	require.NoError(t, err)
}

func TestArrayMaxInlineElement(t *testing.T) {
	t.Parallel()

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	var values []Value
	for i := 0; i < 2; i++ {
		// String length is MaxInlineArrayElementSize - 3 to account for string encoding overhead.
		v := NewStringValue(randStr(int(MaxInlineArrayElementSize - 3)))
		values = append(values, v)

		err = array.Append(v)
		require.NoError(t, err)
	}

	for i := 0; i < len(values); i++ {
		existingStorable, err := array.Get(uint64(i))
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(array.Storage)
		require.NoError(t, err)
		require.Equal(t, values[i], existingValue)
	}

	require.True(t, array.root.IsData())

	// Size of root data slab with two elements of max inlined size is target slab size minus
	// storage id size (next storage id is omitted in root slab), and minus 1 byte
	// (for rounding when computing max inline array element size).
	require.Equal(t, targetThreshold-storageIDSize-1, uint64(array.root.Header().size))

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	err = validArraySerialization(array, storage)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)
}
