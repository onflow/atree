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
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func verifyEmptyArray(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	array *Array,
) {
	verifyArray(t, storage, typeInfo, address, array, nil, false)
}

// verifyArray verifies array elements and validates serialization and in-memory slab tree.
func verifyArray(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	array *Array,
	values []Value,
	hasNestedArrayMapElement bool,
) {
	require.True(t, typeInfoComparator(typeInfo, array.Type()))
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(len(values)), array.Count())

	var err error

	// Verify array elements
	for i, v := range values {
		e, err := array.Get(uint64(i))
		require.NoError(t, err)

		valueEqual(t, typeInfoComparator, v, e)
	}

	// Verify array elements by iterator
	i := 0
	err = array.Iterate(func(v Value) (bool, error) {
		valueEqual(t, typeInfoComparator, values[i], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(values), i)

	// Verify in-memory slabs
	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	// Verify slab serializations
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

	// Check storage slab tree
	rootIDSet, err := CheckStorageHealth(storage, 1)
	require.NoError(t, err)

	rootIDs := make([]SlabID, 0, len(rootIDSet))
	for id := range rootIDSet {
		rootIDs = append(rootIDs, id)
	}
	require.Equal(t, 1, len(rootIDs))
	require.Equal(t, array.SlabID(), rootIDs[0])

	if !hasNestedArrayMapElement {
		// Need to call Commit before calling storage.Count() for PersistentSlabStorage.
		err = storage.Commit()
		require.NoError(t, err)

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(t, stats.SlabCount(), uint64(storage.Count()))

		if len(values) == 0 {
			// Verify slab count for empty array
			require.Equal(t, uint64(1), stats.DataSlabCount)
			require.Equal(t, uint64(0), stats.MetaDataSlabCount)
			require.Equal(t, uint64(0), stats.StorableSlabCount)
		}
	}
}

func TestArrayAppendAndGet(t *testing.T) {
	// With slab size 256 bytes, number of array elements equal 4096,
	// element values equal 0-4095, array tree will be 3 levels,
	// with 14 metadata slabs, and 109 data slabs.

	SetThreshold(256)
	defer SetThreshold(1024)

	const arraySize = 4096

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	for i := uint64(0); i < arraySize; i++ {
		v := Uint64Value(i)
		values[i] = v
		err := array.Append(v)
		require.NoError(t, err)
	}

	e, err := array.Get(array.Count())
	require.Nil(t, e)
	require.Equal(t, 1, errorCategorizationCount(err))

	var userError *UserError
	var indexOutOfBoundsError *IndexOutOfBoundsError
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &indexOutOfBoundsError)
	require.ErrorAs(t, userError, &indexOutOfBoundsError)

	verifyArray(t, storage, typeInfo, address, array, values, false)
}

func TestArraySetAndGet(t *testing.T) {

	t.Run("new elements with similar bytesize", func(t *testing.T) {
		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arraySize; i++ {
			oldValue := values[i]
			newValue := Uint64Value(i * 10)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, oldValue, existingValue)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
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
		defer SetThreshold(1024)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arraySize; i++ {
			oldValue := values[i]
			newValue := Uint64Value(math.MaxUint64 - arraySize + i + 1)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, oldValue, existingValue)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
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
		defer SetThreshold(1024)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(math.MaxUint64 - arraySize + i + 1)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arraySize; i++ {
			oldValue := values[i]
			newValue := Uint64Value(i)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, oldValue, existingValue)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("index out of bounds", func(t *testing.T) {

		const arraySize = 1024

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, 0, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)
		}

		r := newRand(t)

		v := NewStringValue(randStr(r, 1024))
		storable, err := array.Set(array.Count(), v)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))

		var userError *UserError
		var indexOutOfBoundsError *IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestArrayInsertAndGet(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(arraySize - i - 1)
			values[arraySize-i-1] = v
			err := array.Insert(0, v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
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
			v := Uint64Value(i)
			values[i] = v
			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("insert", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, 0, arraySize)
		for i := uint64(0); i < arraySize; i += 2 {
			v := Uint64Value(i)
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)
		}

		for i := uint64(1); i < arraySize; i += 2 {
			v := Uint64Value(i)

			values = append(values, nil)
			copy(values[i+1:], values[i:])
			values[i] = v

			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("index out of bounds", func(t *testing.T) {

		const arraySize = 1024

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, 0, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)
		}

		r := newRand(t)

		v := NewStringValue(randStr(r, 1024))
		err = array.Insert(array.Count()+1, v)
		require.Equal(t, 1, errorCategorizationCount(err))

		var userError *UserError
		var indexOutOfBoundsError *IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestArrayRemove(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("remove-first", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.True(t, typeInfoComparator(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

		for i := uint64(0); i < arraySize; i++ {
			existingStorable, err := array.Remove(0)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			valueEqual(t, typeInfoComparator, values[i], existingValue)

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = array.Storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			require.Equal(t, arraySize-i-1, array.Count())

			if i%256 == 0 {
				verifyArray(t, storage, typeInfo, address, array, values[i+1:], false)
			}
		}

		verifyEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("remove-last", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.True(t, typeInfoComparator(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

		for i := arraySize - 1; i >= 0; i-- {
			existingStorable, err := array.Remove(uint64(i))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			valueEqual(t, typeInfoComparator, values[i], existingValue)

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = array.Storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			require.Equal(t, uint64(i), array.Count())

			if i%256 == 0 {
				verifyArray(t, storage, typeInfo, address, array, values[:i], false)
			}
		}

		verifyEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("remove", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.True(t, typeInfoComparator(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arraySize), array.Count())

		// Remove every other elements
		for i := uint64(0); i < arraySize/2; i++ {
			v := values[i]

			existingStorable, err := array.Remove(i)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			valueEqual(t, typeInfoComparator, v, existingValue)

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = array.Storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			copy(values[i:], values[i+1:])
			values = values[:len(values)-1]

			require.Equal(t, uint64(len(values)), array.Count())

			if i%256 == 0 {
				verifyArray(t, storage, typeInfo, address, array, values, false)
			}
		}

		require.Equal(t, arraySize/2, len(values))

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("index out of bounds", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		storable, err := array.Remove(array.Count())
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))

		var userError *UserError
		var indexOutOfBounds *IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBounds)
		require.ErrorAs(t, userError, &indexOutOfBounds)

		verifyArray(t, storage, typeInfo, address, array, values, false)
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
		defer SetThreshold(1024)

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
		defer SetThreshold(1024)

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
		defer SetThreshold(1024)

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
		defer SetThreshold(1024)

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
		// err is testErr wrapped in ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())

		require.Equal(t, count/2, i)
	})
}

func testArrayIterateRange(t *testing.T, array *Array, values []Value) {
	var i uint64
	var err error
	var sliceOutOfBoundsError *SliceOutOfBoundsError
	var invalidSliceIndexError *InvalidSliceIndexError

	count := array.Count()

	// If startIndex > count, IterateRange returns SliceOutOfBoundsError
	err = array.IterateRange(count+1, count+1, func(v Value) (bool, error) {
		i++
		return true, nil
	})
	require.Equal(t, 1, errorCategorizationCount(err))

	var userError *UserError
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &sliceOutOfBoundsError)
	require.ErrorAs(t, userError, &sliceOutOfBoundsError)
	require.Equal(t, uint64(0), i)

	// If endIndex > count, IterateRange returns SliceOutOfBoundsError
	err = array.IterateRange(0, count+1, func(v Value) (bool, error) {
		i++
		return true, nil
	})
	require.Equal(t, 1, errorCategorizationCount(err))
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &sliceOutOfBoundsError)
	require.ErrorAs(t, userError, &sliceOutOfBoundsError)
	require.Equal(t, uint64(0), i)

	// If startIndex > endIndex, IterateRange returns InvalidSliceIndexError
	if count > 0 {
		err = array.IterateRange(1, 0, func(v Value) (bool, error) {
			i++
			return true, nil
		})
		require.Equal(t, 1, errorCategorizationCount(err))
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &invalidSliceIndexError)
		require.ErrorAs(t, userError, &invalidSliceIndexError)
		require.Equal(t, uint64(0), i)
	}

	// IterateRange returns no error and iteration function is called on sliced array
	for startIndex := uint64(0); startIndex <= count; startIndex++ {
		for endIndex := startIndex; endIndex <= count; endIndex++ {
			i = uint64(0)
			err = array.IterateRange(startIndex, endIndex, func(v Value) (bool, error) {
				valueEqual(t, typeInfoComparator, v, values[int(startIndex+i)])
				i++
				return true, nil
			})
			require.NoError(t, err)
			require.Equal(t, endIndex-startIndex, i)
		}
	}
}

func TestArrayIterateRange(t *testing.T) {
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		testArrayIterateRange(t, array, []Value{})
	})

	t.Run("dataslab as root", func(t *testing.T) {
		const arraySize = 10

		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			value := Uint64Value(i)
			values[i] = value
			err := array.Append(value)
			require.NoError(t, err)
		}

		testArrayIterateRange(t, array, values)
	})

	t.Run("metadataslab as root", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 1024

		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			value := Uint64Value(i)
			values[i] = value
			err := array.Append(value)
			require.NoError(t, err)
		}

		testArrayIterateRange(t, array, values)
	})

	t.Run("stop", func(t *testing.T) {
		const arraySize = 10

		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		startIndex := uint64(1)
		endIndex := uint64(5)
		count := endIndex - startIndex
		err = array.IterateRange(startIndex, endIndex, func(_ Value) (bool, error) {
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
		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 10
		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		testErr := errors.New("test")

		i := uint64(0)
		startIndex := uint64(1)
		endIndex := uint64(5)
		count := endIndex - startIndex
		err = array.IterateRange(startIndex, endIndex, func(_ Value) (bool, error) {
			if i == count/2 {
				return false, testErr
			}
			i++
			return true, nil
		})
		// err is testErr wrapped in ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())
		require.Equal(t, count/2, i)
	})
}

func TestArrayRootSlabID(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

	const arraySize = 4096

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	savedRootID := array.SlabID()
	require.NotEqual(t, SlabIDUndefined, savedRootID)

	// Append elements
	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
		require.Equal(t, savedRootID, array.SlabID())
	}

	require.True(t, typeInfoComparator(typeInfo, array.Type()))
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(arraySize), array.Count())

	// Remove elements
	for i := uint64(0); i < arraySize; i++ {
		storable, err := array.Remove(0)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i), storable)
		require.Equal(t, savedRootID, array.SlabID())
	}

	require.True(t, typeInfoComparator(typeInfo, array.Type()))
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(0), array.Count())
	require.Equal(t, savedRootID, array.SlabID())
}

func TestArraySetRandomValues(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	const arraySize = 4096

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	for i := uint64(0); i < arraySize; i++ {
		v := Uint64Value(i)
		values[i] = v
		err := array.Append(v)
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		oldValue := values[i]
		newValue := randomValue(r, int(maxInlineArrayElementSize))
		values[i] = newValue

		existingStorable, err := array.Set(i, newValue)
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, typeInfoComparator, oldValue, existingValue)
	}

	verifyArray(t, storage, typeInfo, address, array, values, false)
}

func TestArrayInsertRandomValues(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 4096

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := randomValue(r, int(maxInlineArrayElementSize))
			values[arraySize-i-1] = v

			err := array.Insert(0, v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 4096

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := randomValue(r, int(maxInlineArrayElementSize))
			values[i] = v

			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("insert-random", func(t *testing.T) {

		const arraySize = 4096

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			k := r.Intn(int(i) + 1)
			v := randomValue(r, int(maxInlineArrayElementSize))

			copy(values[k+1:], values[k:])
			values[k] = v

			err := array.Insert(uint64(k), v)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestArrayRemoveRandomValues(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	const arraySize = 4096

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	// Insert n random values into array
	for i := uint64(0); i < arraySize; i++ {
		v := randomValue(r, int(maxInlineArrayElementSize))
		values[i] = v

		err := array.Insert(i, v)
		require.NoError(t, err)
	}

	verifyArray(t, storage, typeInfo, address, array, values, false)

	// Remove n elements at random index
	for i := uint64(0); i < arraySize; i++ {
		k := r.Intn(int(array.Count()))

		existingStorable, err := array.Remove(uint64(k))
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, typeInfoComparator, values[k], existingValue)

		copy(values[k:], values[k+1:])
		values = values[:len(values)-1]

		if id, ok := existingStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}
	}

	verifyEmptyArray(t, storage, typeInfo, address, array)
}

func testArrayAppendSetInsertRemoveRandomValues(
	t *testing.T,
	r *rand.Rand,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	opCount int,
) (*Array, []Value) {
	const (
		ArrayAppendOp = iota
		ArrayInsertOp
		ArraySetOp
		ArrayRemoveOp
		MaxArrayOp
	)

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, 0, opCount)
	for i := 0; i < opCount; i++ {

		var nextOp int

		for {
			nextOp = r.Intn(MaxArrayOp)

			if array.Count() > 0 || (nextOp != ArrayRemoveOp && nextOp != ArraySetOp) {
				break
			}
		}

		switch nextOp {

		case ArrayAppendOp:
			v := randomValue(r, int(maxInlineArrayElementSize))
			values = append(values, v)

			err := array.Append(v)
			require.NoError(t, err)

		case ArraySetOp:
			k := r.Intn(int(array.Count()))
			v := randomValue(r, int(maxInlineArrayElementSize))

			oldV := values[k]

			values[k] = v

			existingStorable, err := array.Set(uint64(k), v)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, oldV, existingValue)

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

		case ArrayInsertOp:
			k := r.Intn(int(array.Count() + 1))
			v := randomValue(r, int(maxInlineArrayElementSize))

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
			k := r.Intn(int(array.Count()))

			existingStorable, err := array.Remove(uint64(k))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, values[k], existingValue)

			copy(values[k:], values[k+1:])
			values = values[:len(values)-1]

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = storage.Remove(SlabID(id))
				require.NoError(t, err)
			}
		}

		require.Equal(t, uint64(len(values)), array.Count())
		require.True(t, typeInfoComparator(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
	}

	return array, values
}

func TestArrayAppendSetInsertRemoveRandomValues(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	const opCount = 4096

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, values := testArrayAppendSetInsertRemoveRandomValues(t, r, storage, typeInfo, address, opCount)
	verifyArray(t, storage, typeInfo, address, array, values, false)
}

func TestArrayNestedArrayMap(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("small array", func(t *testing.T) {

		const arraySize = 4096

		nestedTypeInfo := testTypeInfo{43}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create a list of arrays with 2 elements.
		nestedArrays := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested, err := NewArray(storage, address, nestedTypeInfo)
			require.NoError(t, err)

			err = nested.Append(Uint64Value(i))
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

		verifyArray(t, storage, typeInfo, address, array, nestedArrays, false)
	})

	t.Run("big array", func(t *testing.T) {

		const arraySize = 4096

		nestedTypeInfo := testTypeInfo{43}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested, err := NewArray(storage, address, nestedTypeInfo)
			require.NoError(t, err)

			for i := uint64(0); i < 40; i++ {
				err := nested.Append(Uint64Value(math.MaxUint64))
				require.NoError(t, err)
			}

			require.False(t, nested.root.IsData())

			values[i] = nested
		}

		typeInfo := testTypeInfo{42}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		for _, a := range values {
			err := array.Append(a)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, true)
	})

	t.Run("small map", func(t *testing.T) {

		const arraySize = 4096

		nestedTypeInfo := testTypeInfo{43}

		storage := newTestPersistentStorage(t)

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		nestedMaps := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested, err := NewMap(storage, address, NewDefaultDigesterBuilder(), nestedTypeInfo)
			require.NoError(t, err)

			storable, err := nested.Set(compare, hashInputProvider, Uint64Value(i), Uint64Value(i*2))
			require.NoError(t, err)
			require.Nil(t, storable)

			require.True(t, nested.root.IsData())

			nestedMaps[i] = nested
		}

		typeInfo := testTypeInfo{42}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for _, a := range nestedMaps {
			err := array.Append(a)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, nestedMaps, false)
	})

	t.Run("big map", func(t *testing.T) {

		const arraySize = 4096

		nestedTypeInfo := testTypeInfo{43}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested, err := NewMap(storage, address, NewDefaultDigesterBuilder(), nestedTypeInfo)
			require.NoError(t, err)

			for i := uint64(0); i < 25; i++ {
				storable, err := nested.Set(compare, hashInputProvider, Uint64Value(i), Uint64Value(i*2))
				require.NoError(t, err)
				require.Nil(t, storable)
			}

			require.False(t, nested.root.IsData())

			values[i] = nested
		}

		typeInfo := testTypeInfo{42}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		for _, a := range values {
			err := array.Append(a)
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, true)
	})
}

func TestArrayDecodeV0(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		arraySlabID := SlabID{
			address: address,
			index:   SlabIndex{0, 0, 0, 0, 0, 0, 0, 1},
		}

		slabData := map[SlabID][]byte{
			arraySlabID: {
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
		}

		// Decode data to new storage
		storage := newTestPersistentStorageWithData(t, slabData)

		// Test new array from storage
		array, err := NewArrayWithRootID(storage, arraySlabID)
		require.NoError(t, err)

		verifyEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		arraySlabID := SlabID{
			address: address,
			index:   SlabIndex{0, 0, 0, 0, 0, 0, 0, 1},
		}

		values := []Value{
			Uint64Value(0),
		}

		slabData := map[SlabID][]byte{
			arraySlabID: {
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
		}

		// Decode data to new storage
		storage := newTestPersistentStorageWithData(t, slabData)

		// Test new array from storage
		array, err := NewArrayWithRootID(storage, arraySlabID)
		require.NoError(t, err)

		verifyArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("metadataslab as root", func(t *testing.T) {
		storage := newTestBasicStorage(t)
		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		arraySlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		arrayDataSlabID1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		arrayDataSlabID2 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		nestedArraySlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		const arraySize = 20
		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize-1; i++ {
			values[i] = NewStringValue(strings.Repeat("a", 22))
		}

		typeInfo2 := testTypeInfo{43}

		nestedArray, err := NewArray(storage, address, typeInfo2)
		nestedArray.root.SetSlabID(nestedArraySlabID)
		require.NoError(t, err)

		err = nestedArray.Append(Uint64Value(0))
		require.NoError(t, err)

		values[arraySize-1] = nestedArray

		slabData := map[SlabID][]byte{
			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:270 count:11} ]
			arraySlabID: {
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
				// child header 1 (slab id, count, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x09,
				0x00, 0x00, 0x00, 0xe4,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x01, 0x0e,
			},

			// (data slab) next: 3, data: [aaaaaaaaaaaaaaaaaaaaaa ... aaaaaaaaaaaaaaaaaaaaaa]
			arrayDataSlabID1: {
				// version
				0x00,
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
			arrayDataSlabID2: {
				// version
				0x00,
				// array data slab flag
				0x40,
				// next slab id
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
			nestedArraySlabID: {
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

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, slabData)

		// Test new array from storage2
		array, err := NewArrayWithRootID(storage2, arraySlabID)
		require.NoError(t, err)

		verifyArray(t, storage2, typeInfo, address, array, values, false)
	})
}

func TestArrayEncodeDecode(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedData := []byte{
			// version
			0x10,
			// flag
			0x80,

			// extra data
			// array of extra data
			0x81,
			// type info
			0x18, 0x2a,

			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x00,
		}

		slabData, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 1, len(slabData))
		require.Equal(t, expectedData, slabData[array.SlabID()])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, slabData)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		verifyEmptyArray(t, storage2, typeInfo, address, array2)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		v := Uint64Value(0)
		values := []Value{v}
		err = array.Append(v)
		require.NoError(t, err)

		expectedData := []byte{
			// version
			0x10,
			// flag
			0x80,

			// extra data
			// array of extra data
			0x81,
			// type info
			0x18, 0x2a,

			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x01,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00,
		}

		slabData, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 1, len(slabData))
		require.Equal(t, expectedData, slabData[array.SlabID()])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, slabData)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		verifyArray(t, storage2, typeInfo, address, array2, values, false)
	})

	t.Run("has pointers", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize-1; i++ {
			v := NewStringValue(strings.Repeat("a", 22))
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		typeInfo2 := testTypeInfo{43}

		nestedArray, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		err = nestedArray.Append(Uint64Value(0))
		require.NoError(t, err)

		values[arraySize-1] = nestedArray

		err = array.Append(nestedArray)
		require.NoError(t, err)

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, uint64(1), nestedArray.Count())

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		id4 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:270 count:11} ]
			id1: {
				// version
				0x10,
				// flag
				0x81,

				// extra data
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// child shared address
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				// child header count
				0x00, 0x02,
				// child header 1 (slab index, count, size)
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
				// version
				0x10,
				// extra data flag
				0x80,

				// extra data
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

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])
		require.Equal(t, expected[id2], m[id2])
		require.Equal(t, expected[id3], m[id3])
		require.Equal(t, expected[id4], m[id4])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		verifyArray(t, storage2, typeInfo, address, array2, values, false)
	})
}

func TestArrayEncodeDecodeRandomValues(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	const opCount = 8192

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, values := testArrayAppendSetInsertRemoveRandomValues(t, r, storage, typeInfo, address, opCount)

	verifyArray(t, storage, typeInfo, address, array, values, false)

	// Decode data to new storage
	storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

	// Test new array from storage2
	array2, err := NewArrayWithRootID(storage2, array.SlabID())
	require.NoError(t, err)

	verifyArray(t, storage2, typeInfo, address, array2, values, false)
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
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var indexOutOfBoundsError *IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
		require.Nil(t, s)
	})

	t.Run("set", func(t *testing.T) {
		s, err := array.Set(0, Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var indexOutOfBoundsError *IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
		require.Nil(t, s)
	})

	t.Run("insert", func(t *testing.T) {
		err := array.Insert(1, Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var indexOutOfBoundsError *IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
	})

	t.Run("remove", func(t *testing.T) {
		s, err := array.Remove(0)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var indexOutOfBoundsError *IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
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
		require.True(t, typeInfoComparator(typeInfo, array.Type()))
	})

	// TestArrayEncodeDecode/empty tests empty array encoding and decoding
}

func TestArrayStringElement(t *testing.T) {

	t.Parallel()

	t.Run("inline", func(t *testing.T) {

		const arraySize = 4096

		r := newRand(t)

		stringSize := int(maxInlineArrayElementSize - 3)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			s := randStr(r, stringSize)
			values[i] = NewStringValue(s)
		}

		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		typeInfo := testTypeInfo{42}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(values[i])
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(t, uint64(0), stats.StorableSlabCount)
	})

	t.Run("external slab", func(t *testing.T) {

		const arraySize = 4096

		r := newRand(t)

		stringSize := int(maxInlineArrayElementSize + 512)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			s := randStr(r, stringSize)
			values[i] = NewStringValue(s)
		}

		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		typeInfo := testTypeInfo{42}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(values[i])
			require.NoError(t, err)
		}

		verifyArray(t, storage, typeInfo, address, array, values, false)

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(t, uint64(arraySize), stats.StorableSlabCount)
	})
}

func TestArrayStoredValue(t *testing.T) {

	const arraySize = 4096

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	for i := uint64(0); i < arraySize; i++ {
		v := Uint64Value(i)
		values[i] = v
		err := array.Append(v)
		require.NoError(t, err)
	}

	rootID := array.SlabID()

	slabIterator, err := storage.SlabIterator()
	require.NoError(t, err)

	for {
		id, slab := slabIterator()

		if id == SlabIDUndefined {
			break
		}

		value, err := slab.StoredValue(storage)

		if id == rootID {
			require.NoError(t, err)

			array2, ok := value.(*Array)
			require.True(t, ok)

			verifyArray(t, storage, typeInfo, address, array2, values, false)
		} else {
			require.Equal(t, 1, errorCategorizationCount(err))
			var fatalError *FatalError
			var notValueError *NotValueError
			require.ErrorAs(t, err, &fatalError)
			require.ErrorAs(t, err, &notValueError)
			require.ErrorAs(t, fatalError, &notValueError)
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

		verifyEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("root-dataslab", func(t *testing.T) {

		const arraySize = 10

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		i := 0
		err = array.PopIterate(func(v Storable) {
			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, values[arraySize-i-1], vv)
			i++
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		verifyEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		i := 0
		err = array.PopIterate(func(v Storable) {
			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, values[arraySize-i-1], vv)
			i++
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		verifyEmptyArray(t, storage, typeInfo, address, array)
	})
}

func TestArrayFromBatchData(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		array, err := NewArray(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())

		iter, err := array.Iterator()
		require.NoError(t, err)

		// Create a new array with new storage, new address, and original array's elements.
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})
		require.NoError(t, err)
		require.NotEqual(t, copied.SlabID(), array.SlabID())

		verifyEmptyArray(t, storage, typeInfo, address, copied)
	})

	t.Run("root-dataslab", func(t *testing.T) {

		const arraySize = 10

		typeInfo := testTypeInfo{42}
		array, err := NewArray(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		iter, err := array.Iterator()
		require.NoError(t, err)

		// Create a new array with new storage, new address, and original array's elements.
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, copied.SlabID(), array.SlabID())

		verifyArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 4096

		typeInfo := testTypeInfo{42}

		array, err := NewArray(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		iter, err := array.Iterator()
		require.NoError(t, err)

		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		verifyArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("rebalance two data slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		typeInfo := testTypeInfo{42}

		array, err := NewArray(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		var values []Value
		var v Value

		v = NewStringValue(strings.Repeat("a", int(maxInlineArrayElementSize-2)))
		values = append(values, v)

		err = array.Insert(0, v)
		require.NoError(t, err)

		for i := 0; i < 35; i++ {
			v = Uint64Value(i)
			values = append(values, v)

			err = array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(36), array.Count())

		iter, err := array.Iterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		verifyArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("merge two data slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		typeInfo := testTypeInfo{42}

		array, err := NewArray(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		var values []Value
		var v Value
		for i := 0; i < 35; i++ {
			v = Uint64Value(i)
			values = append(values, v)
			err = array.Append(v)
			require.NoError(t, err)
		}

		v = NewStringValue(strings.Repeat("a", int(maxInlineArrayElementSize-2)))
		values = append(values, nil)
		copy(values[25+1:], values[25:])
		values[25] = v

		err = array.Insert(25, v)
		require.NoError(t, err)

		require.Equal(t, uint64(36), array.Count())

		iter, err := array.Iterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		verifyArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("random", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 4096

		r := newRand(t)

		typeInfo := testTypeInfo{42}

		array, err := NewArray(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := randomValue(r, int(maxInlineArrayElementSize))
			values[i] = v

			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		iter, err := array.Iterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)

		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		verifyArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("data slab too large", func(t *testing.T) {
		// Slab size must not exceed maxThreshold.
		// We cannot make this problem happen after Atree Issue #193
		// was fixed by PR #194 & PR #197. This test is to catch regressions.

		SetThreshold(256)
		defer SetThreshold(1024)

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		array, err := NewArray(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		var values []Value
		var v Value

		v = NewStringValue(randStr(r, int(maxInlineArrayElementSize-2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		v = NewStringValue(randStr(r, int(maxInlineArrayElementSize-2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		v = NewStringValue(randStr(r, int(maxInlineArrayElementSize-2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		iter, err := array.Iterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		verifyArray(t, storage, typeInfo, address, copied, values, false)
	})
}

func TestArrayNestedStorables(t *testing.T) {

	t.Parallel()

	typeInfo := testTypeInfo{42}

	const arraySize = 1024 * 4

	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	for i := uint64(0); i < arraySize; i++ {
		s := strings.Repeat("a", int(i))
		v := SomeValue{Value: NewStringValue(s)}
		values[i] = v

		err := array.Append(v)
		require.NoError(t, err)
	}

	verifyArray(t, storage, typeInfo, address, array, values, true)
}

func TestArrayMaxInlineElement(t *testing.T) {
	t.Parallel()

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	var values []Value
	for i := 0; i < 2; i++ {
		// String length is MaxInlineArrayElementSize - 3 to account for string encoding overhead.
		v := NewStringValue(randStr(r, int(maxInlineArrayElementSize-3)))
		values = append(values, v)

		err = array.Append(v)
		require.NoError(t, err)
	}

	require.True(t, array.root.IsData())

	// Size of root data slab with two elements of max inlined size is target slab size minus
	// slab id size (next slab id is omitted in root slab), and minus 1 byte
	// (for rounding when computing max inline array element size).
	require.Equal(t, targetThreshold-slabIDSize-1, uint64(array.root.Header().size))

	verifyArray(t, storage, typeInfo, address, array, values, false)
}

func TestArrayString(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

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
		const arraySize = 120

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		want := `[0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119]`
		require.Equal(t, want, array.String())
	})
}

func TestArraySlabDump(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

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

		want := []string{
			"level 1, ArrayDataSlab id:0x102030405060708.1 size:23 count:6 elements: [0 1 2 3 4 5]",
		}
		dumps, err := DumpArraySlabs(array)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("large", func(t *testing.T) {
		const arraySize = 120

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		want := []string{
			"level 1, ArrayMetaDataSlab id:0x102030405060708.1 size:40 count:120 children: [{id:0x102030405060708.2 size:213 count:54} {id:0x102030405060708.3 size:285 count:66}]",
			"level 2, ArrayDataSlab id:0x102030405060708.2 size:213 count:54 elements: [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53]",
			"level 2, ArrayDataSlab id:0x102030405060708.3 size:285 count:66 elements: [54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119]",
		}

		dumps, err := DumpArraySlabs(array)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("overflow", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(NewStringValue(strings.Repeat("a", int(maxInlineArrayElementSize))))
		require.NoError(t, err)

		want := []string{
			"level 1, ArrayDataSlab id:0x102030405060708.1 size:24 count:1 elements: [SlabIDStorable({[1 2 3 4 5 6 7 8] [0 0 0 0 0 0 0 2]})]",
			"StorableSlab id:0x102030405060708.2 storable:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}

		dumps, err := DumpArraySlabs(array)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})
}

func errorCategorizationCount(err error) int {
	var fatalError *FatalError
	var userError *UserError
	var externalError *ExternalError

	count := 0
	if errors.As(err, &fatalError) {
		count++
	}
	if errors.As(err, &userError) {
		count++
	}
	if errors.As(err, &externalError) {
		count++
	}
	return count
}

func TestArrayLoadedValueIterator(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// parent array: 1 root data slab
		require.Equal(t, 1, len(storage.deltas))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, nil)
	})

	t.Run("root data slab with simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 3
		array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root data slab
		require.Equal(t, 1, len(storage.deltas))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)
	})

	t.Run("root data slab with composite values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 3
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+arraySize, len(storage.deltas))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)
	})

	t.Run("root data slab with composite values, unload composite element from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 3
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+arraySize, len(storage.deltas))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		// Unload composite element from front to back
		for i := 0; i < len(values); i++ {
			v := values[i]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[i+1:]
			verifyArrayLoadedElements(t, array, expectedValues)
		}
	})

	t.Run("root data slab with composite values, unload composite element from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 3
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+arraySize, len(storage.deltas))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			v := values[i]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[:i]
			verifyArrayLoadedElements(t, array, expectedValues)
		}
	})

	t.Run("root data slab with composite values, unload composite element in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 3
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+arraySize, len(storage.deltas))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		// Unload composite element in the middle
		unloadValueIndex := 1

		v := values[unloadValueIndex]

		nestedArray, ok := v.(*Array)
		require.True(t, ok)

		err := storage.Remove(nestedArray.SlabID())
		require.NoError(t, err)

		copy(values[unloadValueIndex:], values[unloadValueIndex+1:])
		values = values[:len(values)-1]

		verifyArrayLoadedElements(t, array, values)
	})

	t.Run("root data slab with composite values, unload composite elements during iteration", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 3
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+arraySize, len(storage.deltas))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		i := 0
		err := array.IterateLoadedValues(func(v Value) (bool, error) {
			// At this point, iterator returned first element (v).

			// Remove all other nested composite elements (except first element) from storage.
			for _, value := range values[1:] {
				nestedArray, ok := value.(*Array)
				require.True(t, ok)

				err := storage.Remove(nestedArray.SlabID())
				require.NoError(t, err)
			}

			require.Equal(t, 0, i)
			valueEqual(t, typeInfoComparator, values[0], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 1, i) // Only first element is iterated because other elements are remove during iteration.
	})

	t.Run("root data slab with simple and composite values, unload composite element", func(t *testing.T) {
		const arraySize = 3

		// Create an array with nested composite value at specified index
		for nestedCompositeIndex := 0; nestedCompositeIndex < arraySize; nestedCompositeIndex++ {
			storage := newTestPersistentStorage(t)

			array, values := createArrayWithSimpleAndCompositeValues(t, storage, address, typeInfo, arraySize, nestedCompositeIndex)

			// parent array: 1 root data slab
			// nested composite element: 1 root data slab
			require.Equal(t, 2, len(storage.deltas))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			verifyArrayLoadedElements(t, array, values)

			// Unload composite element
			v := values[nestedCompositeIndex].(*Array)

			err := storage.Remove(v.SlabID())
			require.NoError(t, err)

			copy(values[nestedCompositeIndex:], values[nestedCompositeIndex+1:])
			values = values[:len(values)-1]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab with simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 20
		array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root metadata slab, 2 data slabs
		require.Equal(t, 3, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)
	})

	t.Run("root metadata slab with composite values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 20
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root metadata slab, 2 data slabs
		// nested composite value element: 1 root data slab for each
		require.Equal(t, 3+arraySize, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)
	})

	t.Run("root metadata slab with composite values, unload composite element from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 20
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root metadata slab, 2 data slabs
		// nested composite value element: 1 root data slab for each
		require.Equal(t, 3+arraySize, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		// Unload composite element from front to back
		for i := 0; i < len(values); i++ {
			v := values[i]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[i+1:]
			verifyArrayLoadedElements(t, array, expectedValues)
		}
	})

	t.Run("root metadata slab with composite values, unload composite element from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 20
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root metadata slab, 2 data slabs
		// nested composite value element: 1 root data slab for each
		require.Equal(t, 3+arraySize, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			v := values[i]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[:i]
			verifyArrayLoadedElements(t, array, expectedValues)
		}
	})

	t.Run("root metadata slab with composite values, unload composite element in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 20
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array: 1 root metadata slab, 2 data slabs
		// nested composite value element: 1 root data slab for each
		require.Equal(t, 3+arraySize, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		// Unload composite element in the middle
		for _, index := range []int{4, 14} {

			v := values[index]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			copy(values[index:], values[index+1:])
			values = values[:len(values)-1]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab with simple and composite values, unload composite element", func(t *testing.T) {
		const arraySize = 20

		// Create an array with composite value at specified index.
		for nestedCompositeIndex := 0; nestedCompositeIndex < arraySize; nestedCompositeIndex++ {
			storage := newTestPersistentStorage(t)

			array, values := createArrayWithSimpleAndCompositeValues(t, storage, address, typeInfo, arraySize, nestedCompositeIndex)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+1, len(storage.deltas))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			verifyArrayLoadedElements(t, array, values)

			// Unload composite value
			v := values[nestedCompositeIndex].(*Array)

			err := storage.Remove(v.SlabID())
			require.NoError(t, err)

			copy(values[nestedCompositeIndex:], values[nestedCompositeIndex+1:])
			values = values[:len(values)-1]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab, unload data slab from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 30
		array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize)

		// parent array (2 levels): 1 root metadata slab, 3 data slabs
		require.Equal(t, 4, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		metaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
		require.True(t, ok)

		// Unload data slabs from front to back
		for i := 0; i < len(metaDataSlab.childrenHeaders); i++ {

			childHeader := metaDataSlab.childrenHeaders[i]

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			values = values[childHeader.count:]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab, unload data slab from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 30
		array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize)

		// parent array (2 levels): 1 root metadata slab, 3 data slabs
		require.Equal(t, 4, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		metaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
		require.True(t, ok)

		// Unload data slabs from back to front
		for i := len(metaDataSlab.childrenHeaders) - 1; i >= 0; i-- {

			childHeader := metaDataSlab.childrenHeaders[i]

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			values = values[:len(values)-int(childHeader.count)]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab, unload data slab in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 30
		array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize)

		// parent array (2 levels): 1 root metadata slab, 3 data slabs
		require.Equal(t, 4, len(storage.deltas))
		require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

		verifyArrayLoadedElements(t, array, values)

		metaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
		require.True(t, ok)

		require.True(t, len(metaDataSlab.childrenHeaders) > 2)

		index := 1
		childHeader := metaDataSlab.childrenHeaders[index]

		err := storage.Remove(childHeader.slabID)
		require.NoError(t, err)

		copy(values[metaDataSlab.childrenCountSum[index-1]:], values[metaDataSlab.childrenCountSum[index]:])
		values = values[:array.Count()-uint64(childHeader.count)]

		verifyArrayLoadedElements(t, array, values)
	})

	t.Run("root metadata slab, unload non-root metadata slab from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 250
		array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize)

		// parent array (3 levels): 1 root metadata slab, 2 non-root metadata slabs, n data slabs
		require.Equal(t, 3, getArrayMetaDataSlabCount(storage))

		rootMetaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
		require.True(t, ok)

		// Unload non-root metadata slabs from front to back
		for i := 0; i < len(rootMetaDataSlab.childrenHeaders); i++ {

			childHeader := rootMetaDataSlab.childrenHeaders[i]

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			values = values[childHeader.count:]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab, unload non-root metadata slab from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const arraySize = 250
		array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize)

		// parent array (3 levels): 1 root metadata slab, 2 child metadata slabs, n data slabs
		require.Equal(t, 3, getArrayMetaDataSlabCount(storage))

		rootMetaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
		require.True(t, ok)

		// Unload non-root metadata slabs from back to front
		for i := len(rootMetaDataSlab.childrenHeaders) - 1; i >= 0; i-- {

			childHeader := rootMetaDataSlab.childrenHeaders[i]

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			values = values[childHeader.count:]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab with composite values, unload random composite value", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const arraySize = 500
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
		// nested composite elements: 1 root data slab for each
		require.True(t, len(storage.deltas) > 1+arraySize)
		require.True(t, getArrayMetaDataSlabCount(storage) > 1)

		verifyArrayLoadedElements(t, array, values)

		r := newRand(t)

		// Unload random composite element
		for len(values) > 0 {

			i := r.Intn(len(values))

			v := values[i]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			copy(values[i:], values[i+1:])
			values = values[:len(values)-1]

			verifyArrayLoadedElements(t, array, values)
		}
	})

	t.Run("root metadata slab with composite values, unload random data slab", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const arraySize = 500
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
		// nested composite elements: 1 root data slab for each
		require.True(t, len(storage.deltas) > 1+arraySize)
		require.True(t, getArrayMetaDataSlabCount(storage) > 1)

		verifyArrayLoadedElements(t, array, values)

		rootMetaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
		require.True(t, ok)

		type slabInfo struct {
			id         SlabID
			startIndex int
			count      int
		}

		count := 0
		var dataSlabInfos []*slabInfo
		for _, mheader := range rootMetaDataSlab.childrenHeaders {
			nonrootMetaDataSlab, ok := storage.deltas[mheader.slabID].(*ArrayMetaDataSlab)
			require.True(t, ok)

			for _, h := range nonrootMetaDataSlab.childrenHeaders {
				dataSlabInfo := &slabInfo{id: h.slabID, startIndex: count, count: int(h.count)}
				dataSlabInfos = append(dataSlabInfos, dataSlabInfo)
				count += int(h.count)
			}
		}

		r := newRand(t)

		// Unload random data slab.
		for len(dataSlabInfos) > 0 {
			indexToUnload := r.Intn(len(dataSlabInfos))

			slabInfoToUnload := dataSlabInfos[indexToUnload]

			// Update startIndex for all data slabs after indexToUnload.
			for i := indexToUnload + 1; i < len(dataSlabInfos); i++ {
				dataSlabInfos[i].startIndex -= slabInfoToUnload.count
			}

			// Remove slabInfo to be unloaded from dataSlabInfos.
			copy(dataSlabInfos[indexToUnload:], dataSlabInfos[indexToUnload+1:])
			dataSlabInfos = dataSlabInfos[:len(dataSlabInfos)-1]

			err := storage.Remove(slabInfoToUnload.id)
			require.NoError(t, err)

			copy(values[slabInfoToUnload.startIndex:], values[slabInfoToUnload.startIndex+slabInfoToUnload.count:])
			values = values[:len(values)-slabInfoToUnload.count]

			verifyArrayLoadedElements(t, array, values)
		}

		require.Equal(t, 0, len(values))
	})

	t.Run("root metadata slab with composite values, unload random slab", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const arraySize = 500
		array, values := createArrayWithCompositeValues(t, storage, address, typeInfo, arraySize)

		// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
		// nested composite elements: 1 root data slab for each
		require.True(t, len(storage.deltas) > 1+arraySize)
		require.True(t, getArrayMetaDataSlabCount(storage) > 1)

		verifyArrayLoadedElements(t, array, values)

		type slabInfo struct {
			id         SlabID
			startIndex int
			count      int
			children   []*slabInfo
		}

		rootMetaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
		require.True(t, ok)

		var dataSlabCount, metadataSlabCount int
		nonrootMetadataSlabInfos := make([]*slabInfo, len(rootMetaDataSlab.childrenHeaders))
		for i, mheader := range rootMetaDataSlab.childrenHeaders {

			nonrootMetadataSlabInfo := &slabInfo{
				id:         mheader.slabID,
				startIndex: metadataSlabCount,
				count:      int(mheader.count),
			}
			metadataSlabCount += int(mheader.count)

			nonrootMetadataSlab, ok := storage.deltas[mheader.slabID].(*ArrayMetaDataSlab)
			require.True(t, ok)

			children := make([]*slabInfo, len(nonrootMetadataSlab.childrenHeaders))
			for i, h := range nonrootMetadataSlab.childrenHeaders {
				children[i] = &slabInfo{
					id:         h.slabID,
					startIndex: dataSlabCount,
					count:      int(h.count),
				}
				dataSlabCount += int(h.count)
			}

			nonrootMetadataSlabInfo.children = children
			nonrootMetadataSlabInfos[i] = nonrootMetadataSlabInfo
		}

		r := newRand(t)

		const (
			metadataSlabType int = iota
			dataSlabType
			maxSlabType
		)

		for len(nonrootMetadataSlabInfos) > 0 {

			var slabInfoToBeRemoved *slabInfo
			var isLastSlab bool

			// Unload random metadata or data slab.
			switch r.Intn(maxSlabType) {

			case metadataSlabType:
				// Unload metadata slab at random index.
				metadataSlabIndex := r.Intn(len(nonrootMetadataSlabInfos))

				isLastSlab = metadataSlabIndex == len(nonrootMetadataSlabInfos)-1

				slabInfoToBeRemoved = nonrootMetadataSlabInfos[metadataSlabIndex]

				count := slabInfoToBeRemoved.count

				// Update startIndex for subsequence metadata and data slabs.
				for i := metadataSlabIndex + 1; i < len(nonrootMetadataSlabInfos); i++ {
					nonrootMetadataSlabInfos[i].startIndex -= count

					for j := 0; j < len(nonrootMetadataSlabInfos[i].children); j++ {
						nonrootMetadataSlabInfos[i].children[j].startIndex -= count
					}
				}

				copy(nonrootMetadataSlabInfos[metadataSlabIndex:], nonrootMetadataSlabInfos[metadataSlabIndex+1:])
				nonrootMetadataSlabInfos = nonrootMetadataSlabInfos[:len(nonrootMetadataSlabInfos)-1]

			case dataSlabType:
				// Unload data slab at randome index.
				metadataSlabIndex := r.Intn(len(nonrootMetadataSlabInfos))

				metaSlabInfo := nonrootMetadataSlabInfos[metadataSlabIndex]

				dataSlabIndex := r.Intn(len(metaSlabInfo.children))

				slabInfoToBeRemoved = metaSlabInfo.children[dataSlabIndex]

				isLastSlab = (metadataSlabIndex == len(nonrootMetadataSlabInfos)-1) &&
					(dataSlabIndex == len(metaSlabInfo.children)-1)

				count := slabInfoToBeRemoved.count

				// Update startIndex for subsequence data slabs.
				for i := dataSlabIndex + 1; i < len(metaSlabInfo.children); i++ {
					metaSlabInfo.children[i].startIndex -= count
				}

				copy(metaSlabInfo.children[dataSlabIndex:], metaSlabInfo.children[dataSlabIndex+1:])
				metaSlabInfo.children = metaSlabInfo.children[:len(metaSlabInfo.children)-1]

				metaSlabInfo.count -= count

				// Update startIndex for all subsequence metadata slabs.
				for i := metadataSlabIndex + 1; i < len(nonrootMetadataSlabInfos); i++ {
					nonrootMetadataSlabInfos[i].startIndex -= count

					for j := 0; j < len(nonrootMetadataSlabInfos[i].children); j++ {
						nonrootMetadataSlabInfos[i].children[j].startIndex -= count
					}
				}

				if len(metaSlabInfo.children) == 0 {
					copy(nonrootMetadataSlabInfos[metadataSlabIndex:], nonrootMetadataSlabInfos[metadataSlabIndex+1:])
					nonrootMetadataSlabInfos = nonrootMetadataSlabInfos[:len(nonrootMetadataSlabInfos)-1]
				}
			}

			err := storage.Remove(slabInfoToBeRemoved.id)
			require.NoError(t, err)

			if isLastSlab {
				values = values[:slabInfoToBeRemoved.startIndex]
			} else {
				copy(values[slabInfoToBeRemoved.startIndex:], values[slabInfoToBeRemoved.startIndex+slabInfoToBeRemoved.count:])
				values = values[:len(values)-slabInfoToBeRemoved.count]
			}

			verifyArrayLoadedElements(t, array, values)
		}

		require.Equal(t, 0, len(values))
	})
}

func createArrayWithSimpleValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	arraySize int,
) (*Array, []Value) {

	// Create parent array
	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	r := rune('a')
	for i := 0; i < arraySize; i++ {
		values[i] = NewStringValue(strings.Repeat(string(r), 20))

		err := array.Append(values[i])
		require.NoError(t, err)
	}

	return array, values
}

func createArrayWithCompositeValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	arraySize int,
) (*Array, []Value) {

	// Create parent array
	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	expectedValues := make([]Value, arraySize)
	for i := 0; i < arraySize; i++ {
		// Create nested array
		nested, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nested.Append(Uint64Value(i))
		require.NoError(t, err)

		expectedValues[i] = nested

		// Append nested array to parent
		err = array.Append(nested)
		require.NoError(t, err)
	}

	return array, expectedValues
}

func createArrayWithSimpleAndCompositeValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	arraySize int,
	compositeValueIndex int,
) (*Array, []Value) {
	require.True(t, compositeValueIndex < arraySize)

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	r := 'a'
	for i := 0; i < arraySize; i++ {

		if compositeValueIndex == i {
			// Create nested array with one element
			a, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = a.Append(Uint64Value(i))
			require.NoError(t, err)

			values[i] = a
		} else {
			values[i] = NewStringValue(strings.Repeat(string(r), 20))
			r++
		}

		err = array.Append(values[i])
		require.NoError(t, err)
	}

	return array, values
}

func verifyArrayLoadedElements(t *testing.T, array *Array, expectedValues []Value) {
	i := 0
	err := array.IterateLoadedValues(func(v Value) (bool, error) {
		require.True(t, i < len(expectedValues))
		valueEqual(t, typeInfoComparator, expectedValues[i], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expectedValues), i)
}

func getArrayMetaDataSlabCount(storage *PersistentSlabStorage) int {
	var counter int
	for _, slab := range storage.deltas {
		if _, ok := slab.(*ArrayMetaDataSlab); ok {
			counter++
		}
	}
	return counter
}

func TestArrayID(t *testing.T) {
	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	sid := array.SlabID()
	id := array.ValueID()

	require.Equal(t, sid.address[:], id[:8])
	require.Equal(t, sid.index[:], id[8:])
}

func TestSlabSizeWhenResettingMutableStorable(t *testing.T) {
	const (
		arraySize           = 3
		initialStorableSize = 1
		mutatedStorableSize = 5
	)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]*mutableValue, arraySize)
	for i := uint64(0); i < arraySize; i++ {
		v := newMutableValue(initialStorableSize)
		values[i] = v

		err := array.Append(v)
		require.NoError(t, err)
	}

	require.True(t, array.root.IsData())

	expectedArrayRootDataSlabSize := arrayRootDataSlabPrefixSize + initialStorableSize*arraySize
	require.Equal(t, uint32(expectedArrayRootDataSlabSize), array.root.ByteSize())

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	require.NoError(t, err)

	for i := uint64(0); i < arraySize; i++ {
		mv := values[i]
		mv.updateStorableSize(mutatedStorableSize)

		existingStorable, err := array.Set(i, mv)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
	}

	require.True(t, array.root.IsData())

	expectedArrayRootDataSlabSize = arrayRootDataSlabPrefixSize + mutatedStorableSize*arraySize
	require.Equal(t, uint32(expectedArrayRootDataSlabSize), array.root.ByteSize())

	err = ValidArray(array, typeInfo, typeInfoComparator, hashInputProvider)
	require.NoError(t, err)
}
