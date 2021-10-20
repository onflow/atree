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

	"github.com/stretchr/testify/require"
)

// Seed only once and print seed for easier debugging.
func init() {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	fmt.Printf("seed: 0x%x\n", seed)
}

func TestArrayAppendAndGet(t *testing.T) {
	// With slab size 256 bytes, number of array elements equal 4096,
	// element values equal 0-4095, array tree will be 3 levels,
	// with 14 metadata slabs, and 109 data slabs.

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 4096

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
		e, err := array.Get(i)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i), e)
	}

	require.Equal(t, typeInfo, array.Type())
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(arraySize), array.Count())

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

	stats, err := GetArrayStats(array)
	require.NoError(t, err)
	require.Equal(
		t,
		stats.DataSlabCount+stats.MetaDataSlabCount,
		uint64(array.Storage.Count()),
	)
}

func TestArraySetAndGet(t *testing.T) {

	t.Run("new elements with similar bytesize", func(t *testing.T) {
		const arraySize = 4096

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

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingValue)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i*10), e)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	// This tests slabs splitting and root slab reassignment caused by Set operation.
	t.Run("new elements with larger bytesize", func(t *testing.T) {
		// With slab size 256 bytes, number of array elements equal 50,
		// element values equal 0-49, array tree will be 1 level,
		// with 0 metadata slab, and 1 data slab (root).
		// When elements are overwritten with values from math.MaxUint64-49 to math.MaxUint64,
		// array tree is 2 levels, with 1 metadata slab, and 2 data slabs.

		const arraySize = 50

		SetThreshold(256)
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
			existingStorable, err := array.Set(i, Uint64Value(math.MaxUint64-arraySize+i+1))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingValue)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(math.MaxUint64-arraySize+i+1), e)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	// This tests slabs merging and root slab reassignment caused by Set operation.
	t.Run("new elements with smaller bytesize", func(t *testing.T) {

		// With slab size 256 bytes, number of array elements equal 50,
		// element values equal math.MaxUint64-49 to math.MaxUint64,
		// array tree is 2 levels, with 1 metadata slab, and 2 data slabs.
		// When elements are overwritten with values from 0-49,
		// array tree will be 1 level, with 0 metadata slab, and 1 data slab (root).

		const arraySize = 50

		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(math.MaxUint64 - arraySize + i + 1))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			existingStorable, err := array.Set(i, Uint64Value(i))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(math.MaxUint64-arraySize+i+1), existingValue)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), e)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})
}

func TestArrayInsertAndGet(t *testing.T) {

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 4096

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
			require.Equal(t, Uint64Value(i), e)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 4096

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
			require.Equal(t, Uint64Value(i), e)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	t.Run("insert", func(t *testing.T) {

		const arraySize = 4096

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
			require.Equal(t, Uint64Value(i), e)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})
}

func TestArrayRemove(t *testing.T) {
	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("remove-first", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

		for i := uint64(0); i < arraySize; i++ {
			v, err := array.Remove(0)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), v)

			require.Equal(t, arraySize-i-1, array.Count())

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

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	t.Run("remove-last", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

		for i := arraySize - 1; i >= 0; i-- {
			v, err := array.Remove(uint64(i))
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), v)

			require.Equal(t, uint64(i), array.Count())

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

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
	})

	t.Run("remove", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

		// Remove every other elements
		for i, j := uint64(0), uint64(0); i < array.Count(); i, j = i+1, j+2 {
			v, err := array.Remove(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(j), v)

			require.Equal(t, uint64(arraySize-i-1), array.Count())

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

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize/2), array.Count())

		for i, j := uint64(0), uint64(1); i < array.Count(); i, j = i+1, j+2 {
			v, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(j), v)
		}

		err = storage.Commit()
		require.NoError(t, err)

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount,
			uint64(array.Storage.Count()),
		)
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
		require.Equal(t, uint64(0), i)
	})

	t.Run("append", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 4096

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
			require.Equal(t, Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), i)
	})

	t.Run("set", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(0))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			existingStorable, err := array.Set(i, Uint64Value(i))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(0), existingValue)
		}

		i := uint64(0)
		err = array.Iterate(func(v Value) (bool, error) {
			require.Equal(t, Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), i)
	})

	t.Run("insert", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 4096

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
			require.Equal(t, Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), i)
	})

	t.Run("remove", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		// Remove every other elements
		for i := uint64(0); i < array.Count(); i++ {
			storable, err := array.Remove(i)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i*2), storable)
		}

		i := uint64(0)
		j := uint64(1)
		err = array.Iterate(func(v Value) (bool, error) {
			require.Equal(t, Uint64Value(j), v)
			i++
			j += 2
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arraySize/2), i)
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

func TestArrayRootStorageID(t *testing.T) {
	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 4096

	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	savedRootID := array.StorageID()
	require.NotEqual(t, StorageIDUndefined, savedRootID)

	// Append elements
	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
		require.Equal(t, savedRootID, array.StorageID())
	}

	require.Equal(t, typeInfo, array.Type())
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(arraySize), array.Count())

	// Remove elements
	for i := uint64(0); i < arraySize; i++ {
		storable, err := array.Remove(0)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i), storable)
		require.Equal(t, savedRootID, array.StorageID())
	}

	require.Equal(t, typeInfo, array.Type())
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(0), array.Count())
}

func TestArraySetRandomValues(t *testing.T) {

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 4096

	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	values := make([]Value, arraySize)

	for i := uint64(0); i < arraySize; i++ {
		v := randomValue(int(MaxInlineElementSize))
		values[i] = v

		existingStorable, err := array.Set(i, v)
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i), existingValue)
	}

	for k, v := range values {
		existingStorable, err := array.Get(uint64(k))
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		require.Equal(t, v, existingValue)
	}

	require.Equal(t, typeInfo, array.Type())
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(arraySize), array.Count())

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

	stats, err := GetArrayStats(array)
	require.NoError(t, err)
	require.True(
		t,
		stats.DataSlabCount+stats.MetaDataSlabCount <= uint64(array.Storage.Count()),
	)
}

func TestArrayInsertRandomValues(t *testing.T) {

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			v := randomValue(int(MaxInlineElementSize))
			values[arraySize-i-1] = v

			err := array.Insert(0, v)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			existingStorable, err := array.Get(i)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, values[i], existingValue)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.True(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount <= uint64(array.Storage.Count()),
		)
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			v := randomValue(int(MaxInlineElementSize))
			values[i] = v

			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			existingStorable, err := array.Get(i)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, values[i], existingValue)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.True(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount <= uint64(array.Storage.Count()),
		)
	})

	t.Run("insert-random", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			k := rand.Intn(int(i) + 1)
			v := randomValue(int(MaxInlineElementSize))

			copy(values[k+1:], values[k:])
			values[k] = v

			err := array.Insert(uint64(k), v)
			require.NoError(t, err)
		}

		for k, v := range values {
			existingStorable, err := array.Get(uint64(k))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.True(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount <= uint64(array.Storage.Count()),
		)
	})
}

func TestArrayRemoveRandomValues(t *testing.T) {

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	const arraySize = 4096

	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)

	// Insert n random values into array
	for i := uint64(0); i < arraySize; i++ {
		v := randomValue(int(MaxInlineElementSize))
		values[i] = v

		err := array.Insert(i, v)
		require.NoError(t, err)
	}

	require.Equal(t, uint64(arraySize), array.Count())
	require.Equal(t, typeInfo, array.Type())
	require.Equal(t, address, array.Address())

	// Remove n elements at random index
	for i := uint64(0); i < arraySize; i++ {
		k := rand.Intn(int(array.Count()))

		existingStorable, err := array.Remove(uint64(k))
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		require.Equal(t, values[k], existingValue)

		copy(values[k:], values[k+1:])
		values = values[:len(values)-1]
	}

	require.Equal(t, uint64(0), array.Count())
	require.Equal(t, typeInfo, array.Type())
	require.Equal(t, address, array.Address())

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

	stats, err := GetArrayStats(array)
	require.NoError(t, err)
	require.True(
		t,
		stats.DataSlabCount+stats.MetaDataSlabCount <= uint64(array.Storage.Count()),
	)
}

func TestArrayAppendSetInsertRemoveRandomValues(t *testing.T) {

	const (
		ArrayAppendOp = iota
		ArrayInsertOp
		ArraySetOp
		ArrayRemoveOp
		MaxArrayOp
	)

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	const opCount = 4096

	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, 0, opCount)

	for i := uint64(0); i < opCount; i++ {

		var nextOp int

		for {
			nextOp = rand.Intn(MaxArrayOp)

			if array.Count() > 0 || (nextOp != ArrayRemoveOp && nextOp != ArraySetOp) {
				break
			}
		}

		switch nextOp {

		case ArrayAppendOp:
			v := randomValue(int(MaxInlineElementSize))
			values = append(values, v)

			err := array.Append(v)
			require.NoError(t, err)

		case ArraySetOp:
			k := rand.Intn(int(array.Count()))
			v := randomValue(int(MaxInlineElementSize))

			oldV := values[k]

			values[k] = v

			existingStorable, err := array.Set(uint64(k), v)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, oldV, existingValue)

		case ArrayInsertOp:
			k := rand.Intn(int(array.Count() + 1))
			v := randomValue(int(MaxInlineElementSize))

			if k == int(array.Count()) {
				values = append(values, v)
			} else {
				values = append(values, nil)
				copy(values[k+1:], values[k:])
				values[k] = v
			}

			err := array.Insert(uint64(k), v)
			require.NoError(t, err)

		case ArrayRemoveOp:
			k := rand.Intn(int(array.Count()))

			existingStorable, err := array.Remove(uint64(k))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, values[k], existingValue)

			copy(values[k:], values[k+1:])
			values = values[:len(values)-1]
		}

		require.Equal(t, uint64(len(values)), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
	}

	for k, v := range values {
		existingStorable, err := array.Get(uint64(k))
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		require.Equal(t, v, existingValue)
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

	stats, err := GetArrayStats(array)
	require.NoError(t, err)
	require.True(
		t,
		stats.DataSlabCount+stats.MetaDataSlabCount <= uint64(array.Storage.Count()),
	)
}

func TestNestedArray(t *testing.T) {

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("small", func(t *testing.T) {

		const arraySize = 4096

		nestedTypeInfo := testTypeInfo{43}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create a list of arrays with 2 elements.
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
			existingStorable, err := array.Get(i)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], existingValue)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(
			t,
			stats.DataSlabCount+stats.MetaDataSlabCount+arraySize,
			uint64(array.Storage.Count()),
		)
	})

	t.Run("big", func(t *testing.T) {

		const arraySize = 4096

		nestedTypeInfo := testTypeInfo{43}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		nestedArrays := make([]*Array, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested, err := NewArray(storage, address, nestedTypeInfo)
			require.NoError(t, err)

			for i := uint64(0); i < 40; i++ {
				err := nested.Append(Uint64Value(math.MaxUint64))
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
			existingStorable, err := array.Get(i)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], existingValue)
		}

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

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

func TestArrayStringElement(t *testing.T) {

	t.Parallel()

	testCases := []struct {
		name              string
		strLen            int
		arraySize         int
		externalSlabCount int
	}{
		{name: "inline", strLen: 32, arraySize: 4096, externalSlabCount: 0},
		{name: "external", strLen: 1024, arraySize: 4096, externalSlabCount: 4096},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {

			typeInfo := testTypeInfo{42}

			storage := newTestPersistentStorage(t)

			address := Address{1, 2, 3, 4, 5, 6, 7, 8}

			array, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			values := make([]Value, tc.arraySize)
			for i := 0; i < tc.arraySize; i++ {
				v := NewStringValue(randStr(tc.strLen))
				values[i] = v

				err := array.Append(v)
				require.NoError(t, err)
			}

			for i := 0; i < tc.arraySize; i++ {
				existingStorable, err := array.Get(uint64(i))
				require.NoError(t, err)

				existingValue, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, values[i], existingValue)
			}

			require.Equal(t, typeInfo, array.Type())
			require.Equal(t, address, array.Address())
			require.Equal(t, uint64(tc.arraySize), array.Count())

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

			stats, err := GetArrayStats(array)
			require.NoError(t, err)
			require.Equal(
				t,
				stats.DataSlabCount+stats.MetaDataSlabCount+uint64(tc.externalSlabCount),
				uint64(array.Storage.Count()),
			)
		})
	}
}

func TestArrayEncodeDecode(t *testing.T) {

	SetThreshold(60)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("no pointers", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		storage := newTestBasicStorage(t)

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

		// Create a new storage with serialized slab data
		storage2 := newTestBasicStorage(t)
		storage2.Load(m)

		// Decode slabs from storage
		rootSlab, ok, err := storage2.Retrieve(id1)
		require.NoError(t, err)
		require.True(t, ok)

		v, err := rootSlab.StoredValue(storage2)
		require.NoError(t, err)

		array2, ok := v.(*Array)
		require.True(t, ok)

		// Compare original array with decoded array from storage
		require.Equal(t, typeInfo, array2.Type())
		require.Equal(t, uint64(arraySize), array2.Count())
		require.Equal(t, address, array2.Address())
		require.Equal(t, id1, array2.StorageID())

		for i := 0; i < arraySize; i++ {
			existingStorable, err := array2.Get(uint64(i))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage2)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingValue)
		}
	})

	t.Run("has pointers", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		storage := newTestBasicStorage(t)

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

		nestedArray, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		err = nestedArray.Append(Uint64Value(0))
		require.NoError(t, err)

		err = array.Append(nestedArray)
		require.NoError(t, err)

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, uint64(1), nestedArray.Count())

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

			// (data slab) next: 0, data: [0]
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

		// Create a new storage with serialized slab data
		storage2 := newTestBasicStorage(t)
		storage2.Load(m)

		// Decode slabs from storage
		rootSlab, ok, err := storage2.Retrieve(id1)
		require.NoError(t, err)
		require.True(t, ok)

		v, err := rootSlab.StoredValue(storage2)
		require.NoError(t, err)

		array2, ok := v.(*Array)
		require.True(t, ok)

		// Compare original array with decoded array from storage
		require.Equal(t, typeInfo, array2.Type())
		require.Equal(t, uint64(arraySize), array2.Count())
		require.Equal(t, address, array2.Address())
		require.Equal(t, id1, array2.StorageID())

		for i := 0; i < arraySize; i++ {
			existingStorable, err := array2.Get(uint64(i))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage2)
			require.NoError(t, err)

			if i == arraySize-1 {
				nestedArray, ok := existingValue.(*Array)
				require.True(t, ok)

				require.Equal(t, typeInfo2, nestedArray.Type())
				require.Equal(t, uint64(1), nestedArray.Count())
				require.Equal(t, address, nestedArray.Address())
				require.Equal(t, id2, nestedArray.StorageID())

				s, err := nestedArray.Get(uint64(0))
				require.NoError(t, err)

				v, err := s.StoredValue(storage2)
				require.NoError(t, err)
				require.Equal(t, Uint64Value(0), v)

			} else {
				require.Equal(t, Uint64Value(i), existingValue)
			}
		}
	})
}

func TestArrayEncodeDecodeRandomValues(t *testing.T) {

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	const arraySize = 8192
	values := make([]Value, arraySize)
	for i := uint64(0); i < arraySize; i++ {
		v := randomValue(int(MaxInlineElementSize))

		values[i] = v

		err := array.Append(v)
		require.NoError(t, err)
	}

	require.Equal(t, typeInfo, array.Type())
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(arraySize), array.Count())

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

	rootID := array.StorageID()

	err = storage.Commit()
	require.NoError(t, err)

	storage.DropCache()

	// Create new array from storage
	array2, err := NewArrayWithRootID(storage, rootID)
	require.NoError(t, err)

	require.Equal(t, typeInfo, array2.Type())
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(arraySize), array.Count())

	// Get and check every element from new array.
	for i := uint64(0); i < arraySize; i++ {
		existingStorable, err := array2.Get(i)
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		require.Equal(t, values[i], existingValue)
	}
}

func TestEmptyArray(t *testing.T) {

	t.Parallel()

	typeInfo := testTypeInfo{42}

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	storage := newTestBasicStorage(t)

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	t.Run("get", func(t *testing.T) {
		s, err := array.Get(0)
		require.Error(t, err, IndexOutOfBoundsError{})
		require.Nil(t, s)
	})

	t.Run("set", func(t *testing.T) {
		s, err := array.Set(0, Uint64Value(0))
		require.Error(t, err, IndexOutOfBoundsError{})
		require.Nil(t, s)
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

	t.Run("encode-decode", func(t *testing.T) {
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

		slabData, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 1, len(slabData))
		require.Equal(t, expectedData, slabData[array.StorageID()])

		storage2 := newTestBasicStorage(t)
		storage2.Load(slabData)

		array2, err := NewArrayWithRootID(storage2, array.StorageID())
		require.NoError(t, err)
		require.Equal(t, uint64(0), array2.Count())
		require.Equal(t, typeInfo, array2.Type())
		require.Equal(t, address, array2.Address())
	})
}

func TestArrayStoredValue(t *testing.T) {

	const arraySize = 4096

	typeInfo := testTypeInfo{42}

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	storage := newTestBasicStorage(t)

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	rootID := array.StorageID()

	for id := range storage.Slabs {
		slab, ok, err := storage.Retrieve(id)
		require.NoError(t, err)
		require.True(t, ok)

		value, err := slab.StoredValue(storage)

		if id == rootID {
			require.NoError(t, err)

			array2, ok := value.(*Array)
			require.True(t, ok)

			require.Equal(t, uint64(arraySize), array2.Count())
			require.Equal(t, typeInfo, array2.Type())
			require.Equal(t, address, array2.Address())

			for i := uint64(0); i < arraySize; i++ {
				v, err := array2.Get(i)
				require.NoError(t, err)
				require.Equal(t, Uint64Value(i), v)
			}
		} else {
			require.Error(t, err)
			require.Nil(t, value)
		}
	}
}

func TestArrayPopIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

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

		require.Equal(t, typeInfo, array.Type())
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(0), array.Count())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())
	})

	t.Run("root-dataslab", func(t *testing.T) {

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

		i := uint64(0)
		err = array.PopIterate(func(v Storable) {
			require.Equal(t, Uint64Value(arraySize-i-1), v)
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
		require.Equal(t, address, array.Address())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())
	})

	t.Run("root-metaslab", func(t *testing.T) {
		SetThreshold(256)
		defer func() {
			SetThreshold(1024)
		}()

		const arraySize = 4096

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
		err = array.PopIterate(func(v Storable) {
			require.Equal(t, Uint64Value(arraySize-i-1), v)
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
		require.Equal(t, address, array.Address())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())
	})
}

func TestArrayBatchAppend(t *testing.T) {

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
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
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
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
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
			PrintArray(array)
		}
		require.NoError(t, err)

		err = validArraySerialization(array, storage)
		if err != nil {
			PrintArray(array)
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
			v := randomValue(int(MaxInlineElementSize))
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

	err = storage.CheckHealth(1)
	if err != nil {
		fmt.Printf("CheckHealth %s\n", err)
	}
	require.NoError(t, err)
}

func TestArrayString(t *testing.T) {

	SetThreshold(128)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("small", func(t *testing.T) {
		const arraySize = 6

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		want := `[0 1 2 3 4 5]`
		require.Equal(t, want, array.String())
	})

	t.Run("large", func(t *testing.T) {
		const arraySize = 190

		typeInfo := testTypeInfo{42}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		wantArrayString := `[0 1 2 ... 27 28 29] [30 31 32 ... 49 50 51] [52 53 54 ... 71 72 73] [74 75 76 ... 93 94 95] [96 97 98 ... 115 116 117] [118 119 120 ... 137 138 139] [140 141 142 ... 159 160 161] [162 163 164 ... 187 188 189]`
		require.Equal(t, wantArrayString, array.String())

		wantMetaDataSlabString := `[{id:0x102030405060708.10 size:100 count:96} {id:0x102030405060708.11 size:100 count:94}]`
		require.Equal(t, wantMetaDataSlabString, array.root.String())
	})
}
