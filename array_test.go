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
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testEmptyArrayV0(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	array *Array,
) {
	testArrayV0(t, storage, typeInfo, address, array, nil, false)
}

func testEmptyArray(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	array *Array,
) {
	testArray(t, storage, typeInfo, address, array, nil, false)
}

func testArrayV0(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	array *Array,
	values []Value,
	hasNestedArrayMapElement bool,
) {
	_testArray(t, storage, typeInfo, address, array, values, hasNestedArrayMapElement, false)
}

func testArray(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	array *Array,
	values []Value,
	hasNestedArrayMapElement bool,
) {
	_testArray(t, storage, typeInfo, address, array, values, hasNestedArrayMapElement, true)
}

// _testArray tests array elements, serialization, and in-memory slab tree.
func _testArray(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	array *Array,
	expectedValues arrayValue,
	hasNestedArrayMapElement bool,
	inlineEnabled bool,
) {
	require.True(t, typeInfoComparator(typeInfo, array.Type()))
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(len(expectedValues)), array.Count())

	var err error

	// Verify array elements
	for i, expected := range expectedValues {
		actual, err := array.Get(uint64(i))
		require.NoError(t, err)

		valueEqual(t, expected, actual)
	}

	// Verify array elements by iterator
	i := 0
	err = array.IterateReadOnly(func(v Value) (bool, error) {
		valueEqual(t, expectedValues[i], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expectedValues), i)

	// Verify in-memory slabs
	err = VerifyArray(array, address, typeInfo, typeInfoComparator, hashInputProvider, inlineEnabled)
	if err != nil {
		PrintArray(array)
	}
	require.NoError(t, err)

	// Verify slab serializations
	err = VerifyArraySerialization(
		array,
		GetCBORDecMode(storage),
		GetCBOREncMode(storage),
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

	// Encode all non-nil slab
	deltas := GetDeltas(storage)
	encodedSlabs := make(map[SlabID][]byte)
	for id, slab := range deltas {
		if slab != nil {
			b, err := EncodeSlab(slab, GetCBOREncMode(storage))
			require.NoError(t, err)
			encodedSlabs[id] = b
		}
	}

	// Test decoded array from new storage to force slab decoding
	decodedArray, err := NewArrayWithRootID(
		newTestPersistentStorageWithBaseStorageAndDeltas(t, GetBaseStorage(storage), encodedSlabs),
		array.SlabID())
	require.NoError(t, err)

	// Verify decoded array elements
	for i, expected := range expectedValues {
		actual, err := decodedArray.Get(uint64(i))
		require.NoError(t, err)

		valueEqual(t, expected, actual)
	}

	if !hasNestedArrayMapElement {
		// Need to call Commit before calling storage.Count() for PersistentSlabStorage.
		err = storage.Commit()
		require.NoError(t, err)

		stats, err := GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(t, stats.SlabCount(), uint64(storage.Count()))

		if len(expectedValues) == 0 {
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

	testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arraySize; i++ {
			oldValue := values[i]
			newValue := Uint64Value(i * 10)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, oldValue, existingValue)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arraySize; i++ {
			oldValue := values[i]
			newValue := Uint64Value(math.MaxUint64 - arraySize + i + 1)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, oldValue, existingValue)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arraySize; i++ {
			oldValue := values[i]
			newValue := Uint64Value(i)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, oldValue, existingValue)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

			valueEqual(t, values[i], existingValue)

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = array.Storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			require.Equal(t, arraySize-i-1, array.Count())

			if i%256 == 0 {
				testArray(t, storage, typeInfo, address, array, values[i+1:], false)
			}
		}

		testEmptyArray(t, storage, typeInfo, address, array)
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

			valueEqual(t, values[i], existingValue)

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = array.Storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			require.Equal(t, uint64(i), array.Count())

			if i%256 == 0 {
				testArray(t, storage, typeInfo, address, array, values[:i], false)
			}
		}

		testEmptyArray(t, storage, typeInfo, address, array)
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

			valueEqual(t, v, existingValue)

			if id, ok := existingStorable.(SlabIDStorable); ok {
				err = array.Storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			copy(values[i:], values[i+1:])
			values = values[:len(values)-1]

			require.Equal(t, uint64(len(values)), array.Count())

			if i%256 == 0 {
				testArray(t, storage, typeInfo, address, array, values, false)
			}
		}

		require.Equal(t, arraySize/2, len(values))

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestReadOnlyArrayIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		i := uint64(0)
		err = array.IterateReadOnly(func(v Value) (bool, error) {
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
		err = array.IterateReadOnly(func(v Value) (bool, error) {
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
		err = array.IterateReadOnly(func(v Value) (bool, error) {
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
		err = array.IterateReadOnly(func(v Value) (bool, error) {
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
		err = array.IterateReadOnly(func(v Value) (bool, error) {
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
		err = array.IterateReadOnly(func(_ Value) (bool, error) {
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
		err = array.IterateReadOnly(func(_ Value) (bool, error) {
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

func TestMutateElementFromReadOnlyArrayIterator(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	var mutationError *ReadOnlyIteratorElementMutationError

	t.Run("mutate inlined element from IterateReadOnly", func(t *testing.T) {
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.False(t, childArray.Inlined())

		// parent array [[]]
		err = parentArray.Append(childArray)
		require.NoError(t, err)
		require.True(t, childArray.Inlined())

		// Iterate and modify element
		var valueMutationCallbackCalled bool
		err = parentArray.IterateReadOnlyWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*Array)
				require.True(t, ok)
				require.True(t, c.Inlined())

				err = c.Append(Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined element from IterateReadOnlyRange", func(t *testing.T) {
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.False(t, childArray.Inlined())

		// parent array [[]]
		err = parentArray.Append(childArray)
		require.NoError(t, err)
		require.True(t, childArray.Inlined())

		// Iterate and modify element
		var valueMutationCallbackCalled bool
		err = parentArray.IterateReadOnlyRangeWithMutationCallback(
			0,
			parentArray.Count(),
			func(v Value) (resume bool, err error) {
				c, ok := v.(*Array)
				require.True(t, ok)
				require.True(t, c.Inlined())

				err = c.Append(Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined array element from IterateReadOnly", func(t *testing.T) {
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.False(t, childArray.Inlined())

		// parent array [[]]
		err = parentArray.Append(childArray)
		require.NoError(t, err)
		require.True(t, childArray.Inlined())

		// Inserting elements into childArray so it can't be inlined
		for i := 0; childArray.Inlined(); i++ {
			v := Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)
		}

		// Iterate and modify element
		var valueMutationCallbackCalled bool
		err = parentArray.IterateReadOnlyWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*Array)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingStorable, err := c.Remove(0)
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined array element from IterateReadOnlyRange", func(t *testing.T) {
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.False(t, childArray.Inlined())

		// parent array [[]]
		err = parentArray.Append(childArray)
		require.NoError(t, err)
		require.True(t, childArray.Inlined())

		// Inserting elements into childArray so it can't be inlined
		for i := 0; childArray.Inlined(); i++ {
			v := Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)
		}

		// Iterate and modify element
		var valueMutationCallbackCalled bool
		err = parentArray.IterateReadOnlyRangeWithMutationCallback(
			0,
			parentArray.Count(),
			func(v Value) (resume bool, err error) {
				c, ok := v.(*Array)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingStorable, err := c.Remove(0)
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})
}

func TestMutableArrayIterate(t *testing.T) {

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

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
		}
		require.True(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			require.Equal(t, Uint64Value(i), v)

			// Mutate primitive array elements by overwritting existing elements of similar byte size.
			newValue := Uint64Value(i * 2)
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingValue)

			expectedValues[i] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 1024

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := Uint64Value(i)
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
		}
		require.False(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			require.Equal(t, Uint64Value(i), v)

			// Mutate primitive array elements by overwritting existing elements with elements of similar size.
			newValue := Uint64Value(i * 2)
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(i), existingValue)

			expectedValues[i] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.False(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		r := rune('a')
		for i := uint64(0); i < arraySize; i++ {
			v := NewStringValue(string(r))
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
			r++
		}
		require.True(t, array.root.IsData())

		i := 0
		r = rune('a')
		err = array.Iterate(func(v Value) (bool, error) {
			require.Equal(t, NewStringValue(string(r)), v)

			// Mutate primitive array elements by overwritting existing elements with larger elements.
			// Larger elements causes slabs to split.
			newValue := NewStringValue(strings.Repeat(string(r), 25))
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, NewStringValue(string(r)), existingValue)

			expectedValues[i] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.False(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 200

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		r := rune('a')
		for i := uint64(0); i < arraySize; i++ {
			v := NewStringValue(string(r))
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
			r++
		}
		require.False(t, array.root.IsData())

		i := 0
		r = rune('a')
		err = array.Iterate(func(v Value) (bool, error) {
			require.Equal(t, NewStringValue(string(r)), v)

			// Mutate primitive array elements by overwritting existing elements with larger elements.
			// Larger elements causes slabs to split.
			newValue := NewStringValue(strings.Repeat(string(r), 25))
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, NewStringValue(string(r)), existingValue)

			expectedValues[i] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.False(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 80

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		r := rune('a')
		for i := uint64(0); i < arraySize; i++ {
			v := NewStringValue(strings.Repeat(string(r), 25))
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
			r++
		}
		require.False(t, array.root.IsData())

		i := 0
		r = rune('a')
		err = array.Iterate(func(v Value) (bool, error) {
			require.Equal(t, NewStringValue(strings.Repeat(string(r), 25)), v)

			// Mutate primitive array elements by overwritting existing elements with smaller elements.
			// Smaller elements causes slabs to merge.
			newValue := NewStringValue(string(r))
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, NewStringValue(strings.Repeat(string(r), 25)), existingValue)

			expectedValues[i] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			v := Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{v}
		}
		require.True(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(1), childArray.Count())
			require.True(t, childArray.Inlined())

			// Mutate array elements by inserting more elements to child arrays.
			newElement := Uint64Value(0)
			err := childArray.Append(newElement)
			require.NoError(t, err)
			require.Equal(t, uint64(2), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 25

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			v := Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{v}
		}
		require.False(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(1), childArray.Count())
			require.True(t, childArray.Inlined())

			// Mutate array elements by inserting more elements to child arrays.
			newElement := Uint64Value(0)
			err := childArray.Append(newElement)
			require.NoError(t, err)
			require.Equal(t, uint64(2), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.False(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			arraySize             = 15
			childArraySize        = 1
			mutatedChildArraySize = 4
		)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue arrayValue
			for j := i; j < i+childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}
		require.True(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			// Mutate array elements by inserting more elements to child arrays.
			for j := i; j < i+mutatedChildArraySize-childArraySize; j++ {
				newElement := Uint64Value(j)

				err := childArray.Append(newElement)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			}

			require.Equal(t, uint64(mutatedChildArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.False(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			arraySize             = 25
			childArraySize        = 1
			mutatedChildArraySize = 4
		)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue arrayValue
			for j := i; j < i+childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}
		require.False(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			// Mutate array elements by inserting more elements to child arrays.
			for j := i; j < i+mutatedChildArraySize-childArraySize; j++ {
				newElement := Uint64Value(j)

				err := childArray.Append(newElement)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			}

			require.Equal(t, uint64(mutatedChildArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.False(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			arraySize             = 10
			childArraySize        = 10
			mutatedChildArraySize = 1
		)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue arrayValue
			for j := i; j < i+childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.False(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			for j := childArraySize - 1; j > mutatedChildArraySize-1; j-- {
				existingStorble, err := childArray.Remove(uint64(j))
				require.NoError(t, err)

				existingValue, err := existingStorble.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, Uint64Value(i+j), existingValue)
			}

			require.Equal(t, uint64(mutatedChildArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues[:mutatedChildArraySize]

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			arraySize             = 2
			childArraySize        = 1
			mutatedChildArraySize = 50
		)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue arrayValue
			for j := i; j < i+childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.True(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			for j := childArraySize; j < mutatedChildArraySize; j++ {
				v := Uint64Value(i + j)

				err := childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, v)
			}

			require.Equal(t, uint64(mutatedChildArraySize), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			arraySize             = 10
			childArraySize        = 10
			mutatedChildArraySize = 50
		)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue arrayValue

			for j := i; j < i+childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.False(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			for j := childArraySize; j < mutatedChildArraySize; j++ {
				v := Uint64Value(i + j)

				err := childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, v)
			}

			require.Equal(t, uint64(mutatedChildArraySize), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)
		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			arraySize             = 2
			childArraySize        = 50
			mutatedChildArraySize = 1
		)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue arrayValue

			for j := i; j < i+childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.True(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArraySize), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			for j := childArraySize - 1; j > mutatedChildArraySize-1; j-- {
				existingStorable, err := childArray.Remove(uint64(j))
				require.NoError(t, err)

				value, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, Uint64Value(i+j), value)
			}

			require.Equal(t, uint64(mutatedChildArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues[:1]

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			arraySize             = 4
			childArraySize        = 50
			mutatedChildArraySize = 25
		)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue arrayValue

			for j := i; j < i+childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.True(t, array.root.IsData())

		i := 0
		err = array.Iterate(func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArraySize), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			for j := childArraySize - 1; j >= mutatedChildArraySize; j-- {
				existingStorable, err := childArray.Remove(uint64(j))
				require.NoError(t, err)

				value, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, Uint64Value(i+j), value)
			}

			require.Equal(t, uint64(mutatedChildArraySize), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues[:mutatedChildArraySize]

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		require.False(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
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
	err = array.IterateReadOnlyRange(count+1, count+1, func(v Value) (bool, error) {
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
	err = array.IterateReadOnlyRange(0, count+1, func(v Value) (bool, error) {
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
		err = array.IterateReadOnlyRange(1, 0, func(v Value) (bool, error) {
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
			err = array.IterateReadOnlyRange(startIndex, endIndex, func(v Value) (bool, error) {
				valueEqual(t, v, values[int(startIndex+i)])
				i++
				return true, nil
			})
			require.NoError(t, err)
			require.Equal(t, endIndex-startIndex, i)
		}
	}
}

func TestReadOnlyArrayIterateRange(t *testing.T) {
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
		err = array.IterateReadOnlyRange(startIndex, endIndex, func(_ Value) (bool, error) {
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
		err = array.IterateReadOnlyRange(startIndex, endIndex, func(_ Value) (bool, error) {
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

func TestMutableArrayIterateRange(t *testing.T) {

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		i := 0
		err = array.IterateRange(0, 0, func(v Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const arraySize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			v := Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{v}
		}
		require.True(t, array.root.IsData())

		sizeBeforeMutation := array.root.Header().size

		i := 0
		startIndex := uint64(1)
		endIndex := array.Count() - 2
		newElement := Uint64Value(0)
		err = array.IterateRange(startIndex, endIndex, func(v Value) (bool, error) {
			childArray, ok := v.(*Array)
			require.True(t, ok)
			require.Equal(t, uint64(1), childArray.Count())
			require.True(t, childArray.Inlined())

			err := childArray.Append(newElement)
			require.NoError(t, err)

			index := int(startIndex) + i
			expectedChildArrayValues, ok := expectedValues[index].(arrayValue)
			require.True(t, ok)

			expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			expectedValues[index] = expectedChildArrayValues

			i++

			require.Equal(t, array.root.Header().size, sizeBeforeMutation+uint32(i)*newElement.ByteSize())

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, endIndex-startIndex, uint64(i))
		require.True(t, array.root.IsData())

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
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
		valueEqual(t, oldValue, existingValue)
	}

	testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

		testArray(t, storage, typeInfo, address, array, values, false)
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

	testArray(t, storage, typeInfo, address, array, values, false)

	// Remove n elements at random index
	for i := uint64(0); i < arraySize; i++ {
		k := r.Intn(int(array.Count()))

		existingStorable, err := array.Remove(uint64(k))
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, values[k], existingValue)

		copy(values[k:], values[k+1:])
		values = values[:len(values)-1]

		if id, ok := existingStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}
	}

	testEmptyArray(t, storage, typeInfo, address, array)
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
			valueEqual(t, oldV, existingValue)

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
			valueEqual(t, values[k], existingValue)

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
	testArray(t, storage, typeInfo, address, array, values, false)
}

func TestArrayWithChildArrayMap(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("small array", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		childTypeInfo := testTypeInfo{43}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Create child arrays with 1 element.
		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, childTypeInfo)
			require.NoError(t, err)

			v := Uint64Value(i)

			err = childArray.Append(v)
			require.NoError(t, err)

			require.True(t, childArray.root.IsData())
			require.False(t, childArray.Inlined())

			err = array.Append(childArray)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedValues[i] = arrayValue{v}
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("big array", func(t *testing.T) {

		const arraySize = 4096
		const childArraySize = 40

		typeInfo := testTypeInfo{42}
		childTypeInfo := testTypeInfo{43}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Create child arrays with 40 element.
		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childArray, err := NewArray(storage, address, childTypeInfo)
			require.NoError(t, err)

			expectedChildArrayValues := make([]Value, childArraySize)
			for i := uint64(0); i < childArraySize; i++ {
				v := Uint64Value(math.MaxUint64)

				err := childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues[i] = v
			}

			require.False(t, childArray.root.IsData())

			err = array.Append(childArray)
			require.NoError(t, err)
			require.False(t, childArray.Inlined())

			expectedValues[i] = arrayValue(expectedChildArrayValues)
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("small map", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		childArayTypeInfo := testTypeInfo{43}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childArayTypeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)
			v := Uint64Value(i * 2)
			storable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)

			require.True(t, childMap.root.IsData())

			err = array.Append(childMap)
			require.NoError(t, err)

			expectedValues[i] = mapValue{k: v}
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("big map", func(t *testing.T) {

		const arraySize = 4096

		typeInfo := testTypeInfo{42}
		nestedTypeInfo := testTypeInfo{43}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {

			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), nestedTypeInfo)
			require.NoError(t, err)

			expectedChildMapValues := mapValue{}
			for i := uint64(0); i < 25; i++ {
				k := Uint64Value(i)
				v := Uint64Value(i * 2)

				storable, err := childMap.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, storable)

				expectedChildMapValues[k] = v
			}

			require.False(t, childMap.root.IsData())

			err = array.Append(childMap)
			require.NoError(t, err)

			expectedValues[i] = expectedChildMapValues
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayDecodeV0(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		arraySlabID := NewSlabID(
			address,
			SlabIndex{0, 0, 0, 0, 0, 0, 0, 1},
		)

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

		testEmptyArrayV0(t, storage, typeInfo, address, array)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		arraySlabID := NewSlabID(
			address,
			SlabIndex{0, 0, 0, 0, 0, 0, 0, 1},
		)

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

		testArrayV0(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("metadataslab as root", func(t *testing.T) {
		storage := newTestBasicStorage(t)
		typeInfo := testTypeInfo{42}
		childTypeInfo := testTypeInfo{43}

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		arraySlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		arrayDataSlabID1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		arrayDataSlabID2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		childArraySlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

		const arraySize = 20
		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize-1; i++ {
			values[i] = NewStringValue(strings.Repeat("a", 22))
		}

		childArray, err := NewArray(storage, address, childTypeInfo)
		childArray.root.SetSlabID(childArraySlabID)
		require.NoError(t, err)

		v := Uint64Value(0)
		err = childArray.Append(v)
		require.NoError(t, err)

		values[arraySize-1] = arrayValue{v}

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
			childArraySlabID: {
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

		testArrayV0(t, storage2, typeInfo, address, array, values, false)
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

		testEmptyArray(t, storage2, typeInfo, address, array2)
	})

	t.Run("root dataslab", func(t *testing.T) {
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

		testArray(t, storage2, typeInfo, address, array2, values, false)
	})

	t.Run("root metadata slab", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 18
		values := make([]Value, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			v := NewStringValue(strings.Repeat("a", 22))
			values[i] = v

			err := array.Append(v)
			require.NoError(t, err)
		}

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

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
				0x00, 0x00, 0x00, 0x09,
				0x00, 0xe4,
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

			// (data slab) data: [aaaaaaaaaaaaaaaaaaaaaa ... aaaaaaaaaaaaaaaaaaaaaa]
			id3: {
				// version
				0x10,
				// array data slab flag
				0x00,
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
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])
		require.Equal(t, expected[id2], m[id2])
		require.Equal(t, expected[id3], m[id3])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, values, false)
	})

	// Same type info is reused.
	t.Run("root data slab, inlined child array of same type", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		childTypeInfo := testTypeInfo{43}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 2
		expectedValues := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			v := Uint64Value(i)

			childArray, err := NewArray(storage, address, childTypeInfo)
			require.NoError(t, err)

			err = childArray.Append(v)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{v}
		}

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			// (data slab) data: [[0] [1]]
			id1: {
				// version
				0x11,
				// array data slab flag
				0x80,

				// extra data
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// inlined extra data
				0x82,
				// element 0: array of type info
				0x80,
				// element 1: array of extra data
				0x81,
				// array extra data
				0xd8, 0xf7,
				0x81,
				// array type info ref
				0x18, 0x2b,

				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x02,
				// CBOR encoded array elements
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x00,
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x01,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	// Different type info are encoded.
	t.Run("root data slab, inlined array of different type", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		typeInfo2 := testTypeInfo{43}
		typeInfo3 := testTypeInfo{44}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 2
		expectedValues := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			v := Uint64Value(i)

			var ti TypeInfo
			if i == 0 {
				ti = typeInfo3
			} else {
				ti = typeInfo2
			}
			childArray, err := NewArray(storage, address, ti)
			require.NoError(t, err)

			err = childArray.Append(v)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{v}
		}

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			// (data slab) data: [[0] [1]]
			id1: {
				// version
				0x11,
				// array data slab flag
				0x80,

				// extra data
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x82,
				// inlined array extra data
				0xd8, 0xf7,
				0x81,
				0x18, 0x2c,
				// inlined array extra data
				0xd8, 0xf7,
				0x81,
				0x18, 0x2b,

				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x02,
				// CBOR encoded array elements
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x00,
				0xd8, 0xfa, 0x83, 0x18, 0x01, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x01,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	// Same type info is reused.
	t.Run("root data slab, multiple levels of inlined array of same type", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		typeInfo2 := testTypeInfo{43}
		typeInfo3 := testTypeInfo{44}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 2
		expectedValues := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			v := Uint64Value(i)

			gchildArray, err := NewArray(storage, address, typeInfo2)
			require.NoError(t, err)

			err = gchildArray.Append(v)
			require.NoError(t, err)

			childArray, err := NewArray(storage, address, typeInfo3)
			require.NoError(t, err)

			err = childArray.Append(gchildArray)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{
				arrayValue{
					v,
				},
			}
		}

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			// (data slab) data: [[0] [1]]
			id1: {
				// version
				0x11,
				// array data slab flag
				0x80,

				// extra data
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x82,
				// inlined array extra data
				0xd8, 0xf7,
				0x81,
				0x18, 0x2c,
				0xd8, 0xf7,
				0x81,
				0x18, 0x2b,

				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x02,
				// CBOR encoded array elements
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x99, 0x00, 0x01, 0xd8, 0xfa, 0x83, 0x18, 0x01, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x00,
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x99, 0x00, 0x01, 0xd8, 0xfa, 0x83, 0x18, 0x01, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x01,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("root data slab, multiple levels of inlined array of different type", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		typeInfo2 := testTypeInfo{43}
		typeInfo3 := testTypeInfo{44}
		typeInfo4 := testTypeInfo{45}
		typeInfo5 := testTypeInfo{46}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 2
		expectedValues := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			v := Uint64Value(i)

			var ti TypeInfo
			if i == 0 {
				ti = typeInfo2
			} else {
				ti = typeInfo4
			}
			gchildArray, err := NewArray(storage, address, ti)
			require.NoError(t, err)

			err = gchildArray.Append(v)
			require.NoError(t, err)

			if i == 0 {
				ti = typeInfo3
			} else {
				ti = typeInfo5
			}
			childArray, err := NewArray(storage, address, ti)
			require.NoError(t, err)

			err = childArray.Append(gchildArray)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{
				arrayValue{
					v,
				},
			}
		}

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			// (data slab) data: [[0] [1]]
			id1: {
				// version
				0x11,
				// array data slab flag
				0x80,

				// extra data
				// array of extra data
				0x81,
				// type info
				0x18, 0x2a,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x84,
				// typeInfo3
				0xd8, 0xf7,
				0x81,
				0x18, 0x2c,
				// typeInfo2
				0xd8, 0xf7,
				0x81,
				0x18, 0x2b,
				// typeInfo5
				0xd8, 0xf7,
				0x81,
				0x18, 0x2e,
				// typeInfo4
				0xd8, 0xf7,
				0x81,
				0x18, 0x2d,

				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x02,
				// CBOR encoded array elements
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x99, 0x00, 0x01, 0xd8, 0xfa, 0x83, 0x18, 0x01, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x00,
				0xd8, 0xfa, 0x83, 0x18, 0x02, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x99, 0x00, 0x01, 0xd8, 0xfa, 0x83, 0x18, 0x03, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x01,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("root metadata slab, inlined array of same type", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		typeInfo2 := testTypeInfo{43}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		expectedValues := make([]Value, 0, arraySize)
		for i := uint64(0); i < arraySize-2; i++ {
			v := NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		for i := 0; i < 2; i++ {
			childArray, err := NewArray(storage, address, typeInfo2)
			require.NoError(t, err)

			v := Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues = append(expectedValues, arrayValue{v})
		}

		require.Equal(t, uint64(arraySize), array.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:268 count:11} ]
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
				0x01, 0x0c,
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

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... [0] [1]]
			id3: {
				// version
				0x11,
				// array data slab flag
				0x00,
				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x81,
				// inlined array extra data
				0xd8, 0xf7,
				0x81,
				0x18, 0x2b,
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
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x0,
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x1,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])
		require.Equal(t, expected[id2], m[id2])
		require.Equal(t, expected[id3], m[id3])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("root metadata slab, inlined array of different type", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		typeInfo2 := testTypeInfo{43}
		typeInfo3 := testTypeInfo{44}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		expectedValues := make([]Value, 0, arraySize)
		for i := uint64(0); i < arraySize-2; i++ {
			v := NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		for i := 0; i < 2; i++ {
			var ti TypeInfo
			if i == 0 {
				ti = typeInfo3
			} else {
				ti = typeInfo2
			}

			childArray, err := NewArray(storage, address, ti)
			require.NoError(t, err)

			v := Uint64Value(i)

			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues = append(expectedValues, arrayValue{v})
		}

		require.Equal(t, uint64(arraySize), array.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:268 count:11} ]
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
				0x01, 0x0c,
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

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... [0] [1]]
			id3: {
				// version
				0x11,
				// array data slab flag
				0x00,
				// inlined extra data
				0x82,
				// element 0: array of inlined extra data
				0x80,
				// element 1: array of inlined extra data
				0x82,
				// inlined array extra data
				0xd8, 0xf7,
				0x81,
				0x18, 0x2c,
				0xd8, 0xf7,
				0x81,
				0x18, 0x2b,
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
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x0,
				0xd8, 0xfa, 0x83, 0x18, 0x01, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x1,
			},
		}

		m, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(m))
		require.Equal(t, expected[id1], m[id1])
		require.Equal(t, expected[id2], m[id2])
		require.Equal(t, expected[id3], m[id3])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, m)

		// Test new array from storage2
		array2, err := NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("has pointers", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		typeInfo2 := testTypeInfo{43}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		expectedValues := make([]Value, 0, arraySize)
		for i := uint64(0); i < arraySize-1; i++ {
			v := NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		const childArraySize = 5

		childArray, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		expectedChildArrayValues := make([]Value, childArraySize)
		for i := 0; i < childArraySize; i++ {
			v := NewStringValue(strings.Repeat("b", 22))
			err = childArray.Append(v)
			require.NoError(t, err)
			expectedChildArrayValues[i] = v
		}

		err = array.Append(childArray)
		require.NoError(t, err)

		expectedValues = append(expectedValues, arrayValue(expectedChildArrayValues))

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, uint64(5), childArray.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		id4 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

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
				// version (no next slab ID, no inlined slabs)
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

			// (data slab) next: 0, data: [bbbbbbbbbbbbbbbbbbbbbb ...]
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
				0x99, 0x00, 0x05,
				// CBOR encoded array elements
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
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

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("has pointers in inlined slab", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		typeInfo2 := testTypeInfo{43}
		typeInfo3 := testTypeInfo{44}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		expectedValues := make([]Value, 0, arraySize)
		for i := uint64(0); i < arraySize-1; i++ {
			v := NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		childArray, err := NewArray(storage, address, typeInfo3)
		require.NoError(t, err)

		gchildArray, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		const gchildArraySize = 5

		expectedGChildArrayValues := make([]Value, gchildArraySize)
		for i := 0; i < gchildArraySize; i++ {
			v := NewStringValue(strings.Repeat("b", 22))

			err = gchildArray.Append(v)
			require.NoError(t, err)

			expectedGChildArrayValues[i] = v
		}

		err = childArray.Append(gchildArray)
		require.NoError(t, err)

		err = array.Append(childArray)
		require.NoError(t, err)

		expectedValues = append(expectedValues, arrayValue{
			arrayValue(expectedGChildArrayValues),
		})

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, uint64(1), childArray.Count())
		require.Equal(t, uint64(5), gchildArray.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		id4 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 5})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// (metadata slab) headers: [{id:2 size:228 count:9} {id:3 size:287 count:11} ]
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
				0x01, 0x1f,
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

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... [SlabID(...)]]
			id3: {
				// version (no next slab ID, has inlined slabs)
				0x11,
				// array data slab flag (has pointer)
				0x40,

				// inlined extra data
				0x82,
				// element 0: array of type info
				0x80,
				// element 1: array of extra data
				0x81,
				// type info
				0xd8, 0xf7,
				0x81,
				0x18, 0x2c,

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
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x99, 0x00, 0x01,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
			},

			// (data slab) data: [bbbbbbbbbbbbbbbbbbbbbb ...]
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
				0x99, 0x00, 0x05,
				// CBOR encoded array elements
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
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

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
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

	testArray(t, storage, typeInfo, address, array, values, false)

	// Decode data to new storage
	storage2 := newTestPersistentStorageWithBaseStorage(t, GetBaseStorage(storage))

	// Test new array from storage2
	array2, err := NewArrayWithRootID(storage2, array.SlabID())
	require.NoError(t, err)

	testArray(t, storage2, typeInfo, address, array2, values, false)
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

	t.Run("readonly iterate", func(t *testing.T) {
		i := uint64(0)
		err := array.IterateReadOnly(func(v Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
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

		testArray(t, storage, typeInfo, address, array, values, false)

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

		testArray(t, storage, typeInfo, address, array, values, false)

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

			testArray(t, storage, typeInfo, address, array2, values, false)
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

		testEmptyArray(t, storage, typeInfo, address, array)
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
			valueEqual(t, values[arraySize-i-1], vv)
			i++
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		testEmptyArray(t, storage, typeInfo, address, array)
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
			valueEqual(t, values[arraySize-i-1], vv)
			i++
		})
		require.NoError(t, err)
		require.Equal(t, arraySize, i)

		testEmptyArray(t, storage, typeInfo, address, array)
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

		iter, err := array.ReadOnlyIterator()
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

		testEmptyArray(t, storage, typeInfo, address, copied)
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

		iter, err := array.ReadOnlyIterator()
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

		testArray(t, storage, typeInfo, address, copied, values, false)
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

		iter, err := array.ReadOnlyIterator()
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

		testArray(t, storage, typeInfo, address, copied, values, false)
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

		iter, err := array.ReadOnlyIterator()
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

		testArray(t, storage, typeInfo, address, copied, values, false)
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

		iter, err := array.ReadOnlyIterator()
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

		testArray(t, storage, typeInfo, address, copied, values, false)
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

		iter, err := array.ReadOnlyIterator()
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

		testArray(t, storage, typeInfo, address, copied, values, false)
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

		iter, err := array.ReadOnlyIterator()
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

		testArray(t, storage, typeInfo, address, copied, values, false)
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
		values[i] = someValue{NewStringValue(s)}

		err := array.Append(v)
		require.NoError(t, err)
	}

	testArray(t, storage, typeInfo, address, array, values, true)
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
	require.Equal(t, targetThreshold-SlabIDLength-1, uint64(array.root.Header().size))

	testArray(t, storage, typeInfo, address, array, values, false)
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

	runTest := func(name string, f func(useWrapperValue bool) func(*testing.T)) {
		for _, useWrapperValue := range []bool{false, true} {
			if useWrapperValue {
				name += ", use wrapper value"
			}

			t.Run(name, f(useWrapperValue))
		}
	}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// parent array: 1 root data slab
		require.Equal(t, 1, GetDeltasCount(storage))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		testArrayLoadedElements(t, array, nil)
	})

	runTest("root data slab with simple values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 3
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root data slab
			require.Equal(t, 1, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root data slab with composite values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 3
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arraySize, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root data slab with composite values, unload composite element from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arraySize, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			// Unload composite element from front to back
			for i := 0; i < len(values); i++ {
				slabID := childSlabIDs[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				expectedValues := values[i+1:]
				testArrayLoadedElements(t, array, expectedValues)
			}
		}
	})

	runTest("root data slab with composite values, unload composite element from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arraySize, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			// Unload composite element from back to front
			for i := len(values) - 1; i >= 0; i-- {
				slabID := childSlabIDs[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				expectedValues := values[:i]
				testArrayLoadedElements(t, array, expectedValues)
			}
		}
	})

	runTest("root data slab with composite values, unload composite element in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arraySize, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			// Unload composite element in the middle
			unloadValueIndex := 1

			slabID := childSlabIDs[unloadValueIndex]

			err := storage.Remove(slabID)
			require.NoError(t, err)

			copy(values[unloadValueIndex:], values[unloadValueIndex+1:])
			values = values[:len(values)-1]

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root data slab with composite values, unload composite elements during iteration", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arraySize, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			i := 0
			err := array.IterateReadOnlyLoadedValues(func(v Value) (bool, error) {
				// At this point, iterator returned first element (v).

				// Remove all other nested composite elements (except first element) from storage.
				for _, slabID := range childSlabIDs[1:] {
					err := storage.Remove(slabID)
					require.NoError(t, err)
				}

				require.Equal(t, 0, i)
				valueEqual(t, values[0], v)
				i++
				return true, nil
			})

			require.NoError(t, err)
			require.Equal(t, 1, i) // Only first element is iterated because other elements are remove during iteration.
		}
	})

	runTest("root data slab with simple and composite values, unload composite element", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			const arraySize = 3

			// Create an array with nested composite value at specified index
			for childArrayIndex := 0; childArrayIndex < arraySize; childArrayIndex++ {
				storage := newTestPersistentStorage(t)

				array, values, childSlabID := createArrayWithSimpleAndChildArrayValues(t, storage, address, typeInfo, arraySize, childArrayIndex, useWrapperValue)

				// parent array: 1 root data slab
				// nested composite element: 1 root data slab
				require.Equal(t, 2, GetDeltasCount(storage))
				require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

				testArrayLoadedElements(t, array, values)

				// Unload composite element
				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				copy(values[childArrayIndex:], values[childArrayIndex+1:])
				values = values[:len(values)-1]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab with simple values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 20
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			require.Equal(t, 3, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root metadata slab with composite values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 20
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arraySize, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root metadata slab with composite values, unload composite element from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 20
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arraySize, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			// Unload composite element from front to back
			for i := 0; i < len(childSlabIDs); i++ {
				slabID := childSlabIDs[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				expectedValues := values[i+1:]
				testArrayLoadedElements(t, array, expectedValues)
			}
		}
	})

	runTest("root metadata slab with composite values, unload composite element from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 20
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arraySize, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			// Unload composite element from back to front
			for i := len(childSlabIDs) - 1; i >= 0; i-- {
				slabID := childSlabIDs[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				expectedValues := values[:i]
				testArrayLoadedElements(t, array, expectedValues)
			}
		}
	})

	runTest("root metadata slab with composite values, unload composite element in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 20
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arraySize, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			// Unload composite element in the middle
			for _, index := range []int{4, 14} {

				slabID := childSlabIDs[index]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				copy(values[index:], values[index+1:])
				values = values[:len(values)-1]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab with simple and composite values, unload composite element", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			const arraySize = 20

			// Create an array with composite value at specified index.
			for childArrayIndex := 0; childArrayIndex < arraySize; childArrayIndex++ {
				storage := newTestPersistentStorage(t)

				array, values, childSlabID := createArrayWithSimpleAndChildArrayValues(t, storage, address, typeInfo, arraySize, childArrayIndex, useWrapperValue)

				// parent array: 1 root metadata slab, 2 data slabs
				// nested composite value element: 1 root data slab for each
				require.Equal(t, 3+1, GetDeltasCount(storage))
				require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

				testArrayLoadedElements(t, array, values)

				// Unload composite value
				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				copy(values[childArrayIndex:], values[childArrayIndex+1:])
				values = values[:len(values)-1]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab, unload data slab from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 30
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			metaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
			require.True(t, ok)

			// Unload data slabs from front to back
			for i := 0; i < len(metaDataSlab.childrenHeaders); i++ {

				childHeader := metaDataSlab.childrenHeaders[i]

				err := storage.Remove(childHeader.slabID)
				require.NoError(t, err)

				values = values[childHeader.count:]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab, unload data slab from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 30
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			metaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
			require.True(t, ok)

			// Unload data slabs from back to front
			for i := len(metaDataSlab.childrenHeaders) - 1; i >= 0; i-- {

				childHeader := metaDataSlab.childrenHeaders[i]

				err := storage.Remove(childHeader.slabID)
				require.NoError(t, err)

				values = values[:len(values)-int(childHeader.count)]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab, unload data slab in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 30
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			metaDataSlab, ok := array.root.(*ArrayMetaDataSlab)
			require.True(t, ok)

			require.True(t, len(metaDataSlab.childrenHeaders) > 2)

			index := 1
			childHeader := metaDataSlab.childrenHeaders[index]

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			copy(values[metaDataSlab.childrenCountSum[index-1]:], values[metaDataSlab.childrenCountSum[index]:])
			values = values[:array.Count()-uint64(childHeader.count)]

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root metadata slab, unload non-root metadata slab from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 250
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize, useWrapperValue)

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

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab, unload non-root metadata slab from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 250
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arraySize, useWrapperValue)

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

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab with composite values, unload random composite value", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 500
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// nested composite elements: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+arraySize)
			require.True(t, getArrayMetaDataSlabCount(storage) > 1)

			testArrayLoadedElements(t, array, values)

			r := newRand(t)

			// Unload random composite element
			for len(values) > 0 {

				i := r.Intn(len(values))

				slabID := childSlabIDs[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				copy(values[i:], values[i+1:])
				values = values[:len(values)-1]

				copy(childSlabIDs[i:], childSlabIDs[i+1:])
				childSlabIDs = childSlabIDs[:len(childSlabIDs)-1]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab with composite values, unload random data slab", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 500
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// nested composite elements: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+arraySize)
			require.True(t, getArrayMetaDataSlabCount(storage) > 1)

			testArrayLoadedElements(t, array, values)

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
				nonrootMetaDataSlab, ok := GetDeltas(storage)[mheader.slabID].(*ArrayMetaDataSlab)
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

				testArrayLoadedElements(t, array, values)
			}

			require.Equal(t, 0, len(values))
		}
	})

	runTest("root metadata slab with composite values, unload random slab", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arraySize = 500
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arraySize, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// nested composite elements: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+arraySize)
			require.True(t, getArrayMetaDataSlabCount(storage) > 1)

			testArrayLoadedElements(t, array, values)

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

				nonrootMetadataSlab, ok := GetDeltas(storage)[mheader.slabID].(*ArrayMetaDataSlab)
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

				testArrayLoadedElements(t, array, values)
			}

			require.Equal(t, 0, len(values))
		}
	})
}

func createArrayWithSimpleValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	arraySize int,
	useWrapperValue bool,
) (*Array, []Value) {

	// Create parent array
	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]Value, arraySize)
	r := rune('a')
	for i := 0; i < arraySize; i++ {
		s := NewStringValue(strings.Repeat(string(r), 20))

		if useWrapperValue {
			err := array.Append(SomeValue{s})
			require.NoError(t, err)

			values[i] = someValue{s}
		} else {
			err := array.Append(s)
			require.NoError(t, err)

			values[i] = s
		}
	}

	return array, values
}

func createArrayWithChildArrays(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	arraySize int,
	useWrapperValue bool,
) (*Array, []Value, []SlabID) {
	const childArraySize = 50

	// Create parent array
	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	expectedValues := make([]Value, arraySize)
	childSlabIDs := make([]SlabID, arraySize)

	for i := 0; i < arraySize; i++ {
		// Create child array
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedChildArrayValues := make([]Value, childArraySize)
		for j := 0; j < childArraySize; j++ {
			v := Uint64Value(j)
			err = childArray.Append(v)
			require.NoError(t, err)

			expectedChildArrayValues[j] = v
		}

		childSlabIDs[i] = childArray.SlabID()

		// Append nested array to parent
		if useWrapperValue {
			err = array.Append(SomeValue{childArray})
			require.NoError(t, err)

			expectedValues[i] = someValue{arrayValue(expectedChildArrayValues)}
		} else {
			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = arrayValue(expectedChildArrayValues)
		}
	}

	return array, expectedValues, childSlabIDs
}

func createArrayWithSimpleAndChildArrayValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	arraySize int,
	compositeValueIndex int,
	useWrapperValue bool,
) (*Array, []Value, SlabID) {
	const childArraySize = 50

	require.True(t, compositeValueIndex < arraySize)

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	expectedValues := make([]Value, arraySize)
	var childSlabID SlabID
	r := 'a'
	for i := 0; i < arraySize; i++ {

		if compositeValueIndex == i {
			// Create child array with one element
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			expectedChildArrayValues := make([]Value, childArraySize)
			for j := 0; j < childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues[j] = v
			}

			if useWrapperValue {
				err = array.Append(SomeValue{childArray})
				require.NoError(t, err)

				expectedValues[i] = someValue{arrayValue(expectedChildArrayValues)}
			} else {
				err = array.Append(childArray)
				require.NoError(t, err)

				expectedValues[i] = arrayValue(expectedChildArrayValues)
			}

			childSlabID = childArray.SlabID()
		} else {
			v := NewStringValue(strings.Repeat(string(r), 20))
			r++

			if useWrapperValue {
				err = array.Append(SomeValue{v})
				require.NoError(t, err)

				expectedValues[i] = someValue{v}
			} else {
				err = array.Append(v)
				require.NoError(t, err)

				expectedValues[i] = v
			}
		}
	}

	return array, expectedValues, childSlabID
}

func testArrayLoadedElements(t *testing.T, array *Array, expectedValues []Value) {
	i := 0
	err := array.IterateReadOnlyLoadedValues(func(v Value) (bool, error) {
		require.True(t, i < len(expectedValues))
		valueEqual(t, expectedValues[i], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expectedValues), i)
}

func getArrayMetaDataSlabCount(storage *PersistentSlabStorage) int {
	var counter int
	deltas := GetDeltas(storage)
	for _, slab := range deltas {
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

	require.Equal(t, sid.address[:], id[:SlabAddressLength])
	require.Equal(t, sid.index[:], id[SlabAddressLength:])
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

	values := make([]*testMutableValue, arraySize)
	for i := uint64(0); i < arraySize; i++ {
		v := newTestMutableValue(initialStorableSize)
		values[i] = v

		err := array.Append(v)
		require.NoError(t, err)
	}

	require.True(t, array.root.IsData())

	expectedArrayRootDataSlabSize := arrayRootDataSlabPrefixSize + initialStorableSize*arraySize
	require.Equal(t, uint32(expectedArrayRootDataSlabSize), array.root.ByteSize())

	err = VerifyArray(array, address, typeInfo, typeInfoComparator, hashInputProvider, true)
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

	err = VerifyArray(array, address, typeInfo, typeInfoComparator, hashInputProvider, true)
	require.NoError(t, err)
}

func TestChildArrayInlinabilityInParentArray(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("parent is root data slab, with one child array", func(t *testing.T) {
		const arraySize = 1

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element.
		parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arraySize)

		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())
		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.True(t, parentArray.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 empty inlined child arrays
		expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*arraySize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Get inlined child array
		e, err := parentArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		expectedChildValues, ok := expectedValues[0].(arrayValue)
		require.True(t, ok)

		childArray, ok := e.(*Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:SlabAddressLength])
		require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

		v := NewStringValue(strings.Repeat("a", 9))
		vSize := v.size

		// Appending 10 elements to child array so that inlined child array reaches max inlined size as array element.
		for i := 0; i < 10; i++ {
			err = childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), childArray.Count())

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))
			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test parent slab size
			expectedParentSize := arrayRootDataSlabPrefixSize + expectedInlinedSize
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test parent array's mutableElementIndex
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		err = childArray.Append(v)
		require.NoError(t, err)

		expectedChildValues = append(expectedChildValues, v)
		expectedValues[0] = expectedChildValues

		require.False(t, childArray.Inlined())
		require.Equal(t, 2, getStoredDeltas(storage)) // There are 2 stored slab because child array is no longer inlined.

		expectedSlabID := valueIDToSlabID(valueID)
		require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
		require.Equal(t, valueID, childArray.ValueID())       // Value ID is unchanged

		expectedStandaloneSlabSize := arrayRootDataSlabPrefixSize + uint32(childArray.Count())*vSize
		require.Equal(t, expectedStandaloneSlabSize, childArray.root.ByteSize())

		expectedParentSize = arrayRootDataSlabPrefixSize + SlabIDStorable(expectedSlabID).ByteSize()
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test parent array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove elements from child array which triggers standalone array slab becomes inlined slab again.
		for childArray.Count() > 0 {
			existingStorable, err := childArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues = expectedChildValues[1:]
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))
			require.Equal(t, SlabIDUndefined, childArray.SlabID())
			require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			expectedParentSize := arrayRootDataSlabPrefixSize + expectedInlinedSize
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test parent array's mutableElementIndex
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, uint64(0), childArray.Count())
		require.Equal(t, uint64(arraySize), parentArray.Count())
	})

	t.Run("parent is root data slab, with two child arrays", func(t *testing.T) {
		const arraySize = 2

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element.
		parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arraySize)

		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 2 empty inlined child arrays
		expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*arraySize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test parent array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		children := make([]struct {
			array   *Array
			valueID ValueID
		}, arraySize)

		for i := 0; i < arraySize; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:SlabAddressLength])
			require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

			children[i].array = childArray
			children[i].valueID = valueID
		}

		v := NewStringValue(strings.Repeat("a", 9))
		vSize := v.size

		// Appending 10 elements to child array so that inlined child array reaches max inlined size as array element.
		for i := 0; i < 10; i++ {
			for j, child := range children {
				childArray := child.array
				childValueID := child.valueID

				err := childArray.Append(v)
				require.NoError(t, err)
				require.Equal(t, uint64(i+1), childArray.Count())

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedChildValues = append(expectedChildValues, v)
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))
				require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

				// Test inlined child slab size
				expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
				require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

				// Test parent slab size
				expectedParentSize += vSize
				require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

				// Test parent array's mutableElementIndex
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		expectedStoredDeltas := 1

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			err := childArray.Append(v)
			require.NoError(t, err)
			require.False(t, childArray.Inlined())

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[i] = expectedChildValues

			expectedStoredDeltas++
			require.Equal(t, expectedStoredDeltas, getStoredDeltas(storage)) // There are more stored slab because child array is no longer inlined.

			expectedSlabID := valueIDToSlabID(childValueID)
			require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, childValueID, childArray.ValueID())  // Value ID is unchanged

			expectedStandaloneSlabSize := arrayRootDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedStandaloneSlabSize, childArray.root.ByteSize())

			//expectedParentSize := arrayRootDataSlabPrefixSize + SlabIDStorable(expectedSlabID).ByteSize()
			expectedParentSize -= inlinedArrayDataSlabPrefixSize + uint32(childArray.Count()-1)*vSize
			expectedParentSize += SlabIDStorable(expectedSlabID).ByteSize()
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test parent array's mutableElementIndex
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Remove one element from child array which triggers standalone array slab becomes inlined slab again.
		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			existingStorable, err := childArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildValues = expectedChildValues[1:]
			expectedValues[i] = expectedChildValues

			require.True(t, childArray.Inlined())

			expectedStoredDeltas--
			require.Equal(t, expectedStoredDeltas, getStoredDeltas(storage))

			require.Equal(t, SlabIDUndefined, childArray.SlabID())
			require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			expectedParentSize -= SlabIDStorable{}.ByteSize()
			expectedParentSize += expectedInlinedSize
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test parent array's mutableElementIndex
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Remove remaining elements from inlined child array
		childArrayCount := children[0].array.Count()
		for i := 0; i < int(childArrayCount); i++ {
			for j, child := range children {
				childArray := child.array
				childValueID := child.valueID

				existingStorable, err := childArray.Remove(0)
				require.NoError(t, err)
				require.Equal(t, NewStringValue(strings.Repeat("a", 9)), existingStorable)

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedChildValues = expectedChildValues[1:]
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))
				require.Equal(t, SlabIDUndefined, childArray.SlabID())
				require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

				expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
				require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

				expectedParentSize -= vSize
				require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

				// Test parent array's mutableElementIndex
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		for _, child := range children {
			require.Equal(t, uint64(0), child.array.Count())
		}
		require.Equal(t, uint64(arraySize), parentArray.Count())
	})

	t.Run("parent is root metadata slab, with four child arrays", func(t *testing.T) {
		const arraySize = 4

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element.
		parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arraySize)

		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.True(t, parentArray.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 4 empty inlined child arrays
		expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*arraySize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test parent array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		children := make([]struct {
			array   *Array
			valueID ValueID
		}, arraySize)

		for i := 0; i < arraySize; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:SlabAddressLength])
			require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

			children[i].array = childArray
			children[i].valueID = valueID
		}

		v := NewStringValue(strings.Repeat("a", 9))
		vSize := v.size

		// Appending 10 elements to child array so that inlined child array reaches max inlined size as array element.
		for i := 0; i < 10; i++ {
			for j, child := range children {
				childArray := child.array
				childValueID := child.valueID

				err := childArray.Append(v)
				require.NoError(t, err)
				require.Equal(t, uint64(i+1), childArray.Count())

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedChildValues = append(expectedChildValues, v)
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

				// Test inlined child slab size
				expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
				require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

				// Test parent array's mutableElementIndex
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		// Parent array has 1 meta data slab and 2 data slabs.
		// All child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, parentArray.root.IsData())

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			err := childArray.Append(v)
			require.NoError(t, err)
			require.False(t, childArray.Inlined())

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[i] = expectedChildValues

			expectedSlabID := valueIDToSlabID(childValueID)
			require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, childValueID, childArray.ValueID())  // Value ID is unchanged

			expectedStandaloneSlabSize := arrayRootDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedStandaloneSlabSize, childArray.root.ByteSize())

			// Test parent array's mutableElementIndex
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has one data slab and all child arrays are not inlined.
		require.Equal(t, 1+arraySize, getStoredDeltas(storage))
		require.True(t, parentArray.root.IsData())

		// Remove one element from child array which triggers standalone array slab becomes inlined slab again.
		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			existingStorable, err := childArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildValues = expectedChildValues[1:]
			expectedValues[i] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID())
			require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test parent array's mutableElementIndex
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has 1 meta data slab and 2 data slabs.
		// All child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, parentArray.root.IsData())

		// Remove remaining elements from inlined child array
		childArrayCount := children[0].array.Count()
		for i := 0; i < int(childArrayCount); i++ {
			for j, child := range children {
				childArray := child.array
				childValueID := child.valueID

				existingStorable, err := childArray.Remove(0)
				require.NoError(t, err)
				require.Equal(t, NewStringValue(strings.Repeat("a", 9)), existingStorable)

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedChildValues = expectedChildValues[1:]
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, SlabIDUndefined, childArray.SlabID())
				require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

				expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
				require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

				// Test parent array's mutableElementIndex
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		// Parent array has 1 data slab.
		// All child arrays are inlined.
		require.Equal(t, 1, getStoredDeltas(storage))
		require.True(t, parentArray.root.IsData())

		for _, child := range children {
			require.Equal(t, uint64(0), child.array.Count())
		}
		require.Equal(t, uint64(arraySize), parentArray.Count())
	})
}

func TestNestedThreeLevelChildArrayInlinabilityInParentArray(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("parent is root data slab, one child array, one grand child array, changes to grand child array triggers child array slab to become standalone slab", func(t *testing.T) {
		const arraySize = 1

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element, which has empty child array.
		parentArray, expectedValues := createArrayWithEmpty2LevelChildArray(t, storage, address, typeInfo, arraySize)

		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.True(t, parentArray.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 inlined child array
		expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*2*arraySize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test parent array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Get inlined child array
		e, err := parentArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		childArray, ok := e.(*Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:SlabAddressLength])
		require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

		// Get inlined grand child array
		e, err = childArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		gchildArray, ok := e.(*Array)
		require.True(t, ok)
		require.True(t, gchildArray.Inlined())
		require.Equal(t, SlabIDUndefined, gchildArray.SlabID())

		gValueID := gchildArray.ValueID()
		require.Equal(t, address[:], gValueID[:SlabAddressLength])
		require.NotEqual(t, SlabIndexUndefined[:], gValueID[SlabAddressLength:])
		require.NotEqual(t, valueID[SlabAddressLength:], gValueID[SlabAddressLength:])

		v := NewStringValue(strings.Repeat("a", 9))
		vSize := v.size

		// Appending 8 elements to grand child array so that inlined grand child array reaches max inlined size as array element.
		for i := 0; i < 8; i++ {
			err = gchildArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), gchildArray.Count())
			require.Equal(t, uint64(1), childArray.Count())

			expectedChildValues, ok := expectedValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues = append(expectedGChildValues, v)
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			// Test inlined child slab size
			expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
			require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

			// Test parent slab size
			expectedParentSize := arrayRootDataSlabPrefixSize + expectedInlinedChildSize
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Add one more element to grand child array which triggers inlined child array slab (NOT grand child array slab) becomes standalone slab
		err = gchildArray.Append(v)
		require.NoError(t, err)

		expectedChildValues, ok := expectedValues[0].(arrayValue)
		require.True(t, ok)

		expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
		require.True(t, ok)

		expectedGChildValues = append(expectedGChildValues, v)
		expectedChildValues[0] = expectedGChildValues
		expectedValues[0] = expectedChildValues

		require.True(t, gchildArray.Inlined())
		require.False(t, childArray.Inlined())
		require.Equal(t, 2, getStoredDeltas(storage)) // There are 2 stored slab because child array is no longer inlined.

		expectedSlabID := valueIDToSlabID(valueID)
		require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
		require.Equal(t, valueID, childArray.ValueID())       // Value ID is unchanged

		require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
		require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

		// Test inlined grand child slab size
		expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
		require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

		expectedStandaloneSlabSize := arrayRootDataSlabPrefixSize + expectedInlinedGrandChildSize
		require.Equal(t, expectedStandaloneSlabSize, childArray.root.ByteSize())

		expectedParentSize = arrayRootDataSlabPrefixSize + SlabIDStorable(expectedSlabID).ByteSize()
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test array's mutableElementIndex
		require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
		require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove elements from grand child array which triggers standalone child array slab becomes inlined slab again.
		for gchildArray.Count() > 0 {
			existingStorable, err := gchildArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues, ok := expectedValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues = expectedGChildValues[1:]
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, SlabIDUndefined, childArray.SlabID())
			require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID())
			require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			// Test inlined child slab size
			expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
			require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

			// Test parent slab size
			expectedParentSize := arrayRootDataSlabPrefixSize + expectedInlinedChildSize
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, uint64(0), gchildArray.Count())
		require.Equal(t, uint64(1), childArray.Count())
		require.Equal(t, uint64(arraySize), parentArray.Count())
	})

	t.Run("parent is root data slab, one child array, one grand child array, changes to grand child array triggers grand child array slab to become standalone slab", func(t *testing.T) {
		const arraySize = 1

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element, which has empty child array.
		parentArray, expectedValues := createArrayWithEmpty2LevelChildArray(t, storage, address, typeInfo, arraySize)

		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.True(t, parentArray.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 inlined child array
		expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*2*arraySize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test parent array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Get inlined child array
		e, err := parentArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		childArray, ok := e.(*Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:SlabAddressLength])
		require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

		// Get inlined grand child array
		e, err = childArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		gchildArray, ok := e.(*Array)
		require.True(t, ok)
		require.True(t, gchildArray.Inlined())
		require.Equal(t, SlabIDUndefined, gchildArray.SlabID())

		gValueID := gchildArray.ValueID()
		require.Equal(t, address[:], gValueID[:SlabAddressLength])
		require.NotEqual(t, SlabIndexUndefined[:], gValueID[SlabAddressLength:])
		require.NotEqual(t, valueID[SlabAddressLength:], gValueID[SlabAddressLength:])

		v := NewStringValue(strings.Repeat("a", 9))
		vSize := v.size

		// Appending 8 elements to grand child array so that inlined grand child array reaches max inlined size as array element.
		for i := 0; i < 8; i++ {
			err = gchildArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), gchildArray.Count())
			require.Equal(t, uint64(1), childArray.Count())

			expectedChildValues, ok := expectedValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues = append(expectedGChildValues, v)
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			// Test inlined child slab size
			expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
			require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

			// Test parent slab size
			expectedParentSize := arrayRootDataSlabPrefixSize + expectedInlinedChildSize
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Add one more element to grand child array which triggers inlined grand child array slab (NOT child array slab) becomes standalone slab
		largeValue := NewStringValue(strings.Repeat("b", 20))
		largeValueSize := largeValue.ByteSize()
		err = gchildArray.Append(largeValue)
		require.NoError(t, err)

		expectedChildValues, ok := expectedValues[0].(arrayValue)
		require.True(t, ok)

		expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
		require.True(t, ok)

		expectedGChildValues = append(expectedGChildValues, largeValue)
		expectedChildValues[0] = expectedGChildValues
		expectedValues[0] = expectedChildValues

		require.False(t, gchildArray.Inlined())
		require.True(t, childArray.Inlined())
		require.Equal(t, 2, getStoredDeltas(storage)) // There are 2 stored slab because child array is no longer inlined.

		require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
		require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

		expectedSlabID := valueIDToSlabID(gValueID)
		require.Equal(t, expectedSlabID, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
		require.Equal(t, gValueID, gchildArray.ValueID())      // Value ID is unchanged

		// Test inlined grand child slab size
		expectedInlinedGrandChildSize := arrayRootDataSlabPrefixSize + uint32(gchildArray.Count()-1)*vSize + largeValueSize
		require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

		expectedStandaloneSlabSize := inlinedArrayDataSlabPrefixSize + SlabIDStorable(expectedSlabID).ByteSize()
		require.Equal(t, expectedStandaloneSlabSize, childArray.root.ByteSize())

		expectedParentSize = arrayRootDataSlabPrefixSize + expectedStandaloneSlabSize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test array's mutableElementIndex
		require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
		require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove elements from grand child array which triggers standalone child array slab becomes inlined slab again.
		for gchildArray.Count() > 0 {
			_, err := gchildArray.Remove(gchildArray.Count() - 1)
			require.NoError(t, err)

			expectedChildValues, ok := expectedValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues = expectedGChildValues[:len(expectedGChildValues)-1]
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, SlabIDUndefined, childArray.SlabID())
			require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID())
			require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			// Test inlined child slab size
			expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
			require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

			// Test parent slab size
			expectedParentSize := arrayRootDataSlabPrefixSize + expectedInlinedChildSize
			require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, uint64(0), gchildArray.Count())
		require.Equal(t, uint64(1), childArray.Count())
		require.Equal(t, uint64(arraySize), parentArray.Count())
	})

	t.Run("parent is root data slab, two child array, one grand child array each, changes to child array triggers child array slab to become standalone slab", func(t *testing.T) {
		const arraySize = 2

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		v := NewStringValue(strings.Repeat("a", 9))
		vSize := v.size

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			// Create child array
			child, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			// Create grand child array
			gchild, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			// Append element to grand child array
			err = gchild.Append(v)
			require.NoError(t, err)

			// Append grand child array to child array
			err = child.Append(gchild)
			require.NoError(t, err)

			// Append child array to parent
			err = parentArray.Append(child)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{arrayValue{v}}
		}

		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.True(t, parentArray.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 inlined child array
		expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*2*arraySize + vSize*arraySize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test parent array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		type arrayInfo struct {
			array   *Array
			valueID ValueID
			child   *arrayInfo
		}

		children := make([]arrayInfo, arraySize)

		for i := 0; i < arraySize; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:SlabAddressLength])
			require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

			e, err = childArray.Get(0)
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			gchildArray, ok := e.(*Array)
			require.True(t, ok)
			require.True(t, gchildArray.Inlined())
			require.Equal(t, SlabIDUndefined, gchildArray.SlabID())

			gValueID := gchildArray.ValueID()
			require.Equal(t, address[:], gValueID[:SlabAddressLength])
			require.NotEqual(t, SlabIndexUndefined[:], gValueID[SlabAddressLength:])
			require.NotEqual(t, valueID[SlabAddressLength:], gValueID[SlabAddressLength:])

			children[i] = arrayInfo{
				array:   childArray,
				valueID: valueID,
				child:   &arrayInfo{array: gchildArray, valueID: gValueID},
			}
		}

		// Appending 7 elements to child array so that inlined child array reaches max inlined size as array element.
		for i := 0; i < 7; i++ {
			for j, child := range children {
				childArray := child.array
				valueID := child.valueID
				gchildArray := child.child.array
				gValueID := child.child.valueID

				err := childArray.Append(v)
				require.NoError(t, err)
				require.Equal(t, uint64(i+2), childArray.Count())

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedChildValues = append(expectedChildValues, v)

				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.True(t, gchildArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))

				require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

				require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

				// Test inlined grand child slab size (1 element, unchanged)
				expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + vSize
				require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

				// Test inlined child slab size
				expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize + vSize*uint32(i+1)
				require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

				// Test parent slab size
				expectedParentSize += vSize
				require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

				// Test array's mutableElementIndex
				require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
				require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		// Add one more element to child array which triggers inlined child array slab (NOT grand child array slab) becomes standalone slab
		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			err = childArray.Append(v)
			require.NoError(t, err)

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)

			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.False(t, childArray.Inlined())
			require.Equal(t, 2+i, getStoredDeltas(storage)) // There are >1 stored slab because child array is no longer inlined.

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())       // Value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			expectedStandaloneSlabSize := arrayRootDataSlabPrefixSize + expectedInlinedGrandChildSize + vSize*uint32(childArray.Count()-1)
			require.Equal(t, expectedStandaloneSlabSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because child array is no longer inlined.

		expectedParentSize = arrayRootDataSlabPrefixSize + SlabIDStorable(SlabID{}).ByteSize()*2
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Remove one elements from each child array to trigger child arrays being inlined again.
		expectedParentSize = arrayRootDataSlabPrefixSize

		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			_, err = childArray.Remove(childArray.Count() - 1)
			require.NoError(t, err)

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedChildValues = expectedChildValues[:len(expectedChildValues)-1]

			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, childArray.Inlined())
			require.Equal(t, 2-i, getStoredDeltas(storage))

			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			// Test inlined child slab size
			expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize + vSize*uint32(childArray.Count()-1)
			require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

			expectedParentSize += expectedInlinedChildSize

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Remove elements from child array.
		elementCount := children[0].array.Count()

		for i := uint64(0); i < elementCount-1; i++ {
			for j, child := range children {
				childArray := child.array
				valueID := child.valueID
				gchildArray := child.child.array
				gValueID := child.child.valueID

				existingStorable, err := childArray.Remove(childArray.Count() - 1)
				require.NoError(t, err)
				require.Equal(t, NewStringValue(strings.Repeat("a", 9)), existingStorable)

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedChildValues = expectedChildValues[:len(expectedChildValues)-1]

				expectedValues[j] = expectedChildValues

				require.True(t, gchildArray.Inlined())
				require.True(t, gchildArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))

				require.Equal(t, SlabIDUndefined, childArray.SlabID())
				require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

				require.Equal(t, SlabIDUndefined, gchildArray.SlabID())
				require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
				require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

				// Test inlined child slab size
				expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize + vSize*uint32(childArray.Count()-1)
				require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

				// Test parent slab size
				expectedParentSize -= vSize
				require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

				// Test array's mutableElementIndex
				require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
				require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		for _, child := range children {
			require.Equal(t, uint64(1), child.child.array.Count())
			require.Equal(t, uint64(1), child.array.Count())
		}
		require.Equal(t, uint64(arraySize), parentArray.Count())
	})

	t.Run("parent is root metadata slab, with four child arrays, each child array has grand child arrays", func(t *testing.T) {
		const arraySize = 4

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		v := NewStringValue(strings.Repeat("a", 9))
		vSize := v.size

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			// Create child array
			child, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			// Create grand child array
			gchild, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			// Append grand child array to child array
			err = child.Append(gchild)
			require.NoError(t, err)

			// Append child array to parent
			err = parentArray.Append(child)
			require.NoError(t, err)

			expectedValues[i] = arrayValue{arrayValue{}}
		}

		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.True(t, parentArray.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*2*arraySize
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

		// Test parent array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		type arrayInfo struct {
			array   *Array
			valueID ValueID
			child   *arrayInfo
		}

		children := make([]arrayInfo, arraySize)

		for i := 0; i < arraySize; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:SlabAddressLength])
			require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

			e, err = childArray.Get(0)
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			gchildArray, ok := e.(*Array)
			require.True(t, ok)
			require.True(t, gchildArray.Inlined())
			require.Equal(t, SlabIDUndefined, gchildArray.SlabID())

			gValueID := gchildArray.ValueID()
			require.Equal(t, address[:], gValueID[:SlabAddressLength])
			require.NotEqual(t, SlabIndexUndefined[:], gValueID[SlabAddressLength:])
			require.NotEqual(t, valueID[SlabAddressLength:], gValueID[SlabAddressLength:])

			children[i] = arrayInfo{
				array:   childArray,
				valueID: valueID,
				child:   &arrayInfo{array: gchildArray, valueID: gValueID},
			}
		}

		// Appending 6 elements to grand child array so that parent array root slab is metadata slab.
		for i := uint32(0); i < 6; i++ {
			for j, child := range children {
				childArray := child.array
				valueID := child.valueID
				gchildArray := child.child.array
				gValueID := child.child.valueID

				err := gchildArray.Append(v)
				require.NoError(t, err)
				require.Equal(t, uint64(i+1), gchildArray.Count())

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
				require.True(t, ok)

				expectedGChildValues = append(expectedGChildValues, v)

				expectedChildValues[0] = expectedGChildValues
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.True(t, gchildArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))

				require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

				require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

				// Test inlined grand child slab size
				expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + vSize*(i+1)
				require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

				// Test inlined child slab size
				expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
				require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

				// Test array's mutableElementIndex
				require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
				require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		// Add one more element to grand child array which triggers parent array slab becomes metadata slab (all elements are still inlined).
		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			err = gchildArray.Append(v)
			require.NoError(t, err)

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
			require.True(t, ok)

			expectedGChildValues = append(expectedGChildValues, v)

			expectedChildValues[0] = expectedGChildValues
			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, childArray.Inlined())
			require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because parent root slab is metdata.

			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			expectedInlinedChildSlabSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
			require.Equal(t, expectedInlinedChildSlabSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because child array is no longer inlined.
		require.False(t, parentArray.root.IsData())

		// Add one more element to grand child array which triggers
		// - child arrays become standalone slab (grand child arrays are still inlined)
		// - parent array slab becomes data slab
		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
			require.True(t, ok)

			for j := 0; j < 2; j++ {
				err = gchildArray.Append(v)
				require.NoError(t, err)

				expectedGChildValues = append(expectedGChildValues, v)
			}

			expectedChildValues[0] = expectedGChildValues
			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.False(t, childArray.Inlined())

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())       // Value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

			// Test standalone grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			expectedStandaloneChildSlabSize := arrayRootDataSlabPrefixSize + expectedInlinedGrandChildSize
			require.Equal(t, expectedStandaloneChildSlabSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has one root data slab, 4 grand child array with standalone root data slab.
		require.Equal(t, 1+arraySize, getStoredDeltas(storage))
		require.True(t, parentArray.root.IsData())

		// Remove elements from grand child array to trigger child array inlined again.
		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			expectedChildValues, ok := expectedValues[i].(arrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
			require.True(t, ok)

			for j := 0; j < 2; j++ {
				_, err = gchildArray.Remove(0)
				require.NoError(t, err)

				expectedGChildValues = expectedGChildValues[:len(expectedGChildValues)-1]
			}

			expectedChildValues[0] = expectedGChildValues
			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, childArray.Inlined())

			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())        // Value ID is unchanged

			require.Equal(t, SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())       // Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
			require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

			// Test inlined child slab size
			expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
			require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has 1 metadata slab, and two data slab, all child and grand child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, parentArray.root.IsData())

		// Remove elements from grand child array.
		elementCount := children[0].child.array.Count()

		for i := uint64(0); i < elementCount; i++ {
			for j, child := range children {
				childArray := child.array
				valueID := child.valueID
				gchildArray := child.child.array
				gValueID := child.child.valueID

				existingStorable, err := gchildArray.Remove(0)
				require.NoError(t, err)
				require.Equal(t, v, existingStorable)

				expectedChildValues, ok := expectedValues[j].(arrayValue)
				require.True(t, ok)

				expectedGChildValues, ok := expectedChildValues[0].(arrayValue)
				require.True(t, ok)

				expectedGChildValues = expectedGChildValues[1:]

				expectedChildValues[0] = expectedGChildValues
				expectedValues[j] = expectedChildValues

				require.True(t, gchildArray.Inlined())
				require.True(t, gchildArray.Inlined())

				require.Equal(t, SlabIDUndefined, childArray.SlabID())
				require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

				require.Equal(t, SlabIDUndefined, gchildArray.SlabID())
				require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedInlinedGrandChildSize := inlinedArrayDataSlabPrefixSize + uint32(gchildArray.Count())*vSize
				require.Equal(t, expectedInlinedGrandChildSize, gchildArray.root.ByteSize())

				// Test inlined child slab size
				expectedInlinedChildSize := inlinedArrayDataSlabPrefixSize + expectedInlinedGrandChildSize
				require.Equal(t, expectedInlinedChildSize, childArray.root.ByteSize())

				// Test array's mutableElementIndex
				require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
				require.True(t, uint64(len(gchildArray.mutableElementIndex)) <= gchildArray.Count())
				require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		for _, child := range children {
			require.Equal(t, uint64(0), child.child.array.Count())
			require.Equal(t, uint64(1), child.array.Count())
		}
		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.Equal(t, 1, getStoredDeltas(storage))

		expectedParentSize = uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*arraySize*2
		require.Equal(t, expectedParentSize, parentArray.root.ByteSize())
	})
}

func TestChildArrayWhenParentArrayIsModified(t *testing.T) {

	const arraySize = 2

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	// Create an array with empty child array as element.
	parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arraySize)

	require.Equal(t, uint64(arraySize), parentArray.Count())
	require.True(t, parentArray.root.IsData())
	require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

	// Test parent slab size with empty inlined child arrays
	expectedParentSize := uint32(arrayRootDataSlabPrefixSize) + uint32(inlinedArrayDataSlabPrefixSize)*arraySize
	require.Equal(t, expectedParentSize, parentArray.root.ByteSize())

	// Test array's mutableElementIndex
	require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

	testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

	children := make([]*struct {
		array       *Array
		valueID     ValueID
		parentIndex int
	}, arraySize)

	for i := 0; i < arraySize; i++ {
		e, err := parentArray.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		childArray, ok := e.(*Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:SlabAddressLength])
		require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])

		children[i] = &struct {
			array       *Array
			valueID     ValueID
			parentIndex int
		}{
			childArray, valueID, i,
		}
	}

	t.Run("insert elements in parent array", func(t *testing.T) {
		// insert value at index 0, so all child array indexes are moved by +1
		v := Uint64Value(0)
		err := parentArray.Insert(0, v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, nil)
		copy(expectedValues[1:], expectedValues)
		expectedValues[0] = v

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(1), childArray.Count())

			child.parentIndex = i + 1

			expectedChildValues, ok := expectedValues[child.parentIndex].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// insert value at index 2, so only second child array index is moved by +1
		v = Uint64Value(2)
		err = parentArray.Insert(2, v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, nil)
		copy(expectedValues[3:], expectedValues[2:])
		expectedValues[2] = v

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(2), childArray.Count())

			if i > 0 {
				child.parentIndex++
			}

			expectedChildValues, ok := expectedValues[child.parentIndex].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// insert value at index 4, so none of child array indexes are affected.
		v = Uint64Value(4)
		err = parentArray.Insert(4, v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, nil)
		expectedValues[4] = v

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(3), childArray.Count())

			expectedChildValues, ok := expectedValues[child.parentIndex].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}
	})

	t.Run("remove elements from parent array", func(t *testing.T) {
		// remove value at index 0, so all child array indexes are moved by -1.
		existingStorable, err := parentArray.Remove(0)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(0), existingStorable)

		copy(expectedValues, expectedValues[1:])
		expectedValues[len(expectedValues)-1] = nil
		expectedValues = expectedValues[:len(expectedValues)-1]

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(4), childArray.Count())

			child.parentIndex--

			expectedChildValues, ok := expectedValues[child.parentIndex].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Remove value at index 1, so only second child array index is moved by -1
		existingStorable, err = parentArray.Remove(1)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(2), existingStorable)

		copy(expectedValues[1:], expectedValues[2:])
		expectedValues[len(expectedValues)-1] = nil
		expectedValues = expectedValues[:len(expectedValues)-1]

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(5), childArray.Count())

			if i > 0 {
				child.parentIndex--
			}

			expectedChildValues, ok := expectedValues[child.parentIndex].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Remove value at index 2 (last element), so none of child array indexes are affected.
		existingStorable, err = parentArray.Remove(2)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(4), existingStorable)

		expectedValues[len(expectedValues)-1] = nil
		expectedValues = expectedValues[:len(expectedValues)-1]

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(6), childArray.Count())

			expectedChildValues, ok := expectedValues[child.parentIndex].(arrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())   // Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := inlinedArrayDataSlabPrefixSize + uint32(childArray.Count())*vSize
			require.Equal(t, expectedInlinedSize, childArray.root.ByteSize())

			// Test array's mutableElementIndex
			require.True(t, uint64(len(childArray.mutableElementIndex)) <= childArray.Count())
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}
	})
}

func createArrayWithEmptyChildArray(
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
		// Create child array
		child, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Append child array to parent
		err = array.Append(child)
		require.NoError(t, err)

		expectedValues[i] = arrayValue{}
	}

	return array, expectedValues
}

func createArrayWithEmpty2LevelChildArray(
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
		// Create child array
		child, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Create grand child array
		gchild, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Append grand child array to child array
		err = child.Append(gchild)
		require.NoError(t, err)

		// Append child array to parent
		err = array.Append(child)
		require.NoError(t, err)

		expectedValues[i] = arrayValue{arrayValue{}}
	}

	return array, expectedValues
}

func getStoredDeltas(storage *PersistentSlabStorage) int {
	count := 0
	deltas := GetDeltas(storage)
	for _, slab := range deltas {
		if slab != nil {
			count++
		}
	}
	return count
}

func TestArraySetReturnedValue(t *testing.T) {
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			var expectedChildValues arrayValue
			for {
				v := NewStringValue(strings.Repeat("a", 10))

				err = childArray.Append(v)
				require.NoError(t, err)

				expectedChildValues = append(expectedChildValues, v)

				if !childArray.Inlined() {
					break
				}
			}

			expectedValues = append(expectedValues, expectedChildValues)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child array value
		for i := 0; i < arraySize; i++ {
			existingStorable, err := parentArray.Set(uint64(i), Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)

			expectedValues[i] = Uint64Value(0)

			// Test array's mutableElementIndex
			require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())
		}

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			// Insert one element to child array
			v := NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedValues = append(expectedValues, arrayValue{v})
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child array value
		for i := 0; i < arraySize; i++ {
			existingStorable, err := parentArray.Set(uint64(i), Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			expectedValues[i] = Uint64Value(0)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(mapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			j := 0
			for {
				k := Uint64Value(j)
				v := NewStringValue(strings.Repeat("a", 10))
				j++

				existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildValues[k] = v

				if !childMap.Inlined() {
					break
				}
			}
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child map value
		for i := 0; i < arraySize; i++ {
			existingStorable, err := parentArray.Set(uint64(i), Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			expectedValues[i] = Uint64Value(0)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(mapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			v := NewStringValue(strings.Repeat("a", 10))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child map value
		for i := 0; i < arraySize; i++ {
			existingStorable, err := parentArray.Set(uint64(i), Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			expectedValues[i] = Uint64Value(0)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})
}

func TestArrayRemoveReturnedValue(t *testing.T) {
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			var expectedChildValues arrayValue
			for {
				v := NewStringValue(strings.Repeat("a", 10))

				err = childArray.Append(v)
				require.NoError(t, err)

				expectedChildValues = append(expectedChildValues, v)

				if !childArray.Inlined() {
					break
				}
			}

			expectedValues = append(expectedValues, expectedChildValues)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child array value
		for i := 0; i < arraySize; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, len(parentArray.mutableElementIndex))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			// Insert one element to child array
			v := NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedValues = append(expectedValues, arrayValue{v})
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child array value
		for i := 0; i < arraySize; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, len(parentArray.mutableElementIndex))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(mapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			j := 0
			for {
				k := Uint64Value(j)
				v := NewStringValue(strings.Repeat("a", 10))
				j++

				existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildValues[k] = v

				if !childMap.Inlined() {
					break
				}
			}
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child map value
		for i := 0; i < arraySize; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, len(parentArray.mutableElementIndex))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const arraySize = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		for i := 0; i < arraySize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(mapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			v := NewStringValue(strings.Repeat("a", 10))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(len(parentArray.mutableElementIndex)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child map value
		for i := 0; i < arraySize; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedValues[i], child)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, len(parentArray.mutableElementIndex))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})
}

func TestArrayWithOutdatedCallback(t *testing.T) {
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("overwritten child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		// Create child array
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Insert child array to parent array
		err = parentArray.Append(childArray)
		require.NoError(t, err)

		v := NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, arrayValue{v})

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite child array value from parent
		valueStorable, err := parentArray.Set(0, Uint64Value(0))
		require.NoError(t, err)

		id, ok := valueStorable.(SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		valueEqual(t, expectedValues[0], child)

		expectedValues[0] = Uint64Value(0)

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.NotNil(t, childArray.parentUpdater)

		// modify overwritten child array
		err = childArray.Append(Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.Nil(t, childArray.parentUpdater)

		// No-op on parent
		valueEqual(t, expectedValues, parentArray)
	})

	t.Run("removed child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues arrayValue

		// Create child array
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Insert child array to parent array
		err = parentArray.Append(childArray)
		require.NoError(t, err)

		v := NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, arrayValue{v})

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child array value from parent
		valueStorable, err := parentArray.Remove(0)
		require.NoError(t, err)

		id, ok := valueStorable.(SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		valueEqual(t, expectedValues[0], child)

		expectedValues = arrayValue{}

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.NotNil(t, childArray.parentUpdater)

		// modify removed child array
		err = childArray.Append(Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.Nil(t, childArray.parentUpdater)

		// No-op on parent
		valueEqual(t, expectedValues, parentArray)
	})
}

func TestArraySetType(t *testing.T) {
	typeInfo := testTypeInfo{42}
	newTypeInfo := testTypeInfo{43}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a new array in memory
		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.True(t, array.root.IsData())

		// Modify type info of new array
		err = array.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, array.Type())

		// Commit new array to storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingArraySetType(t, array.SlabID(), GetBaseStorage(storage), newTypeInfo, array.Count())
	})

	t.Run("data slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		arraySize := 10
		for i := 0; i < arraySize; i++ {
			v := Uint64Value(i)
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.True(t, array.root.IsData())

		err = array.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, array.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingArraySetType(t, array.SlabID(), GetBaseStorage(storage), newTypeInfo, array.Count())
	})

	t.Run("metadata slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		arraySize := 10_000
		for i := 0; i < arraySize; i++ {
			v := Uint64Value(i)
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.False(t, array.root.IsData())

		err = array.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, array.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingArraySetType(t, array.SlabID(), GetBaseStorage(storage), newTypeInfo, array.Count())
	})

	t.Run("inlined in parent container root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = parentArray.Append(childArray)
		require.NoError(t, err)

		require.Equal(t, uint64(1), parentArray.Count())
		require.Equal(t, typeInfo, parentArray.Type())
		require.True(t, parentArray.root.IsData())
		require.False(t, parentArray.Inlined())

		require.Equal(t, uint64(0), childArray.Count())
		require.Equal(t, typeInfo, childArray.Type())
		require.True(t, childArray.root.IsData())
		require.True(t, childArray.Inlined())

		err = childArray.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childArray.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedArraySetType(t, parentArray.SlabID(), 0, GetBaseStorage(storage), newTypeInfo, childArray.Count())
	})

	t.Run("inlined in parent container non-root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		arraySize := 10_000
		for i := 0; i < arraySize-1; i++ {
			v := Uint64Value(i)
			err := parentArray.Append(v)
			require.NoError(t, err)
		}

		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = parentArray.Append(childArray)
		require.NoError(t, err)

		require.Equal(t, uint64(arraySize), parentArray.Count())
		require.Equal(t, typeInfo, parentArray.Type())
		require.False(t, parentArray.root.IsData())
		require.False(t, parentArray.Inlined())

		require.Equal(t, uint64(0), childArray.Count())
		require.Equal(t, typeInfo, childArray.Type())
		require.True(t, childArray.root.IsData())
		require.True(t, childArray.Inlined())

		err = childArray.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childArray.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedArraySetType(t, parentArray.SlabID(), arraySize-1, GetBaseStorage(storage), newTypeInfo, childArray.Count())
	})
}

func testExistingArraySetType(
	t *testing.T,
	id SlabID,
	baseStorage BaseStorage,
	expectedTypeInfo testTypeInfo,
	expectedCount uint64,
) {
	newTypeInfo := testTypeInfo{value: expectedTypeInfo.value + 1}

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing array by ID
	array, err := NewArrayWithRootID(storage, id)
	require.NoError(t, err)
	require.Equal(t, expectedCount, array.Count())
	require.Equal(t, expectedTypeInfo, array.Type())

	// Modify type info of existing array
	err = array.SetType(newTypeInfo)
	require.NoError(t, err)
	require.Equal(t, expectedCount, array.Count())
	require.Equal(t, newTypeInfo, array.Type())

	// Commit data in storage
	err = storage.FastCommit(runtime.NumCPU())
	require.NoError(t, err)

	// Create storage from existing data
	storage2 := newTestPersistentStorageWithBaseStorage(t, GetBaseStorage(storage))

	// Load existing array again from storage
	array2, err := NewArrayWithRootID(storage2, id)
	require.NoError(t, err)
	require.Equal(t, expectedCount, array2.Count())
	require.Equal(t, newTypeInfo, array2.Type())
}

func testExistingInlinedArraySetType(
	t *testing.T,
	parentID SlabID,
	inlinedChildIndex int,
	baseStorage BaseStorage,
	expectedTypeInfo testTypeInfo,
	expectedCount uint64,
) {
	newTypeInfo := testTypeInfo{value: expectedTypeInfo.value + 1}

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing array by ID
	parentArray, err := NewArrayWithRootID(storage, parentID)
	require.NoError(t, err)

	element, err := parentArray.Get(uint64(inlinedChildIndex))
	require.NoError(t, err)

	childArray, ok := element.(*Array)
	require.True(t, ok)

	require.Equal(t, expectedCount, childArray.Count())
	require.Equal(t, expectedTypeInfo, childArray.Type())

	// Modify type info of existing array
	err = childArray.SetType(newTypeInfo)
	require.NoError(t, err)
	require.Equal(t, expectedCount, childArray.Count())
	require.Equal(t, newTypeInfo, childArray.Type())

	// Commit data in storage
	err = storage.FastCommit(runtime.NumCPU())
	require.NoError(t, err)

	// Create storage from existing data
	storage2 := newTestPersistentStorageWithBaseStorage(t, GetBaseStorage(storage))

	// Load existing array again from storage
	parentArray2, err := NewArrayWithRootID(storage2, parentID)
	require.NoError(t, err)

	element2, err := parentArray2.Get(uint64(inlinedChildIndex))
	require.NoError(t, err)

	childArray2, ok := element2.(*Array)
	require.True(t, ok)

	require.Equal(t, expectedCount, childArray2.Count())
	require.Equal(t, newTypeInfo, childArray2.Type())
}
