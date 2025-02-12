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

package atree_test

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
)

func testEmptyArrayV0(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	array *atree.Array,
) {
	testArrayV0(t, storage, typeInfo, address, array, nil, false)
}

func testEmptyArray(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	array *atree.Array,
) {
	testArray(t, storage, typeInfo, address, array, nil, false)
}

func testArrayV0(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	array *atree.Array,
	values []atree.Value,
	hasNestedArrayMapElement bool,
) {
	_testArray(t, storage, typeInfo, address, array, values, hasNestedArrayMapElement, false)
}

func testArray(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	array *atree.Array,
	values []atree.Value,
	hasNestedArrayMapElement bool,
) {
	_testArray(t, storage, typeInfo, address, array, values, hasNestedArrayMapElement, true)
}

// _testArray tests array elements, serialization, and in-memory slab tree.
func _testArray(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	array *atree.Array,
	expectedValues test_utils.ExpectedArrayValue,
	hasNestedArrayMapElement bool,
	inlineEnabled bool,
) {
	require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(len(expectedValues)), array.Count())

	var err error

	// Verify array elements
	for i, expected := range expectedValues {
		actual, err := array.Get(uint64(i))
		require.NoError(t, err)

		testValueEqual(t, expected, actual)
	}

	// Verify array elements by iterator
	i := 0
	err = array.IterateReadOnly(func(v atree.Value) (bool, error) {
		testValueEqual(t, expectedValues[i], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expectedValues), i)

	// Verify in-memory slabs
	err = atree.VerifyArray(array, address, typeInfo, test_utils.CompareTypeInfo, test_utils.GetHashInput, inlineEnabled)
	if err != nil {
		atree.PrintArray(array)
	}
	require.NoError(t, err)

	// Verify slab serializations
	err = atree.VerifyArraySerialization(
		array,
		atree.GetCBORDecMode(storage),
		atree.GetCBOREncMode(storage),
		storage.DecodeStorable,
		storage.DecodeTypeInfo,
		func(a, b atree.Storable) bool {
			return reflect.DeepEqual(a, b)
		},
	)
	if err != nil {
		atree.PrintArray(array)
	}
	require.NoError(t, err)

	// Check storage slab tree
	rootIDSet, err := atree.CheckStorageHealth(storage, 1)
	require.NoError(t, err)

	rootIDs := make([]atree.SlabID, 0, len(rootIDSet))
	for id := range rootIDSet {
		rootIDs = append(rootIDs, id)
	}
	require.Equal(t, 1, len(rootIDs))
	require.Equal(t, array.SlabID(), rootIDs[0])

	// Encode all non-nil slab
	deltas := atree.GetDeltas(storage)
	encodedSlabs := make(map[atree.SlabID][]byte)
	for id, slab := range deltas {
		if slab != nil {
			b, err := atree.EncodeSlab(slab, atree.GetCBOREncMode(storage))
			require.NoError(t, err)
			encodedSlabs[id] = b
		}
	}

	// Test decoded array from new storage to force slab decoding
	decodedArray, err := atree.NewArrayWithRootID(
		newTestPersistentStorageWithBaseStorageAndDeltas(t, atree.GetBaseStorage(storage), encodedSlabs),
		array.SlabID())
	require.NoError(t, err)

	// Verify decoded array elements
	for i, expected := range expectedValues {
		actual, err := decodedArray.Get(uint64(i))
		require.NoError(t, err)

		testValueEqual(t, expected, actual)
	}

	if !hasNestedArrayMapElement {
		// Need to call Commit before calling storage.Count() for atree.PersistentSlabStorage.
		err = storage.Commit()
		require.NoError(t, err)

		stats, err := atree.GetArrayStats(array)
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

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	const arrayCount = 4096

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]atree.Value, arrayCount)
	for i := uint64(0); i < arrayCount; i++ {
		v := test_utils.Uint64Value(i)
		values[i] = v
		err := array.Append(v)
		require.NoError(t, err)
	}

	e, err := array.Get(array.Count())
	require.Nil(t, e)
	require.Equal(t, 1, errorCategorizationCount(err))

	var userError *atree.UserError
	var indexOutOfBoundsError *atree.IndexOutOfBoundsError
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &indexOutOfBoundsError)
	require.ErrorAs(t, userError, &indexOutOfBoundsError)

	testArray(t, storage, typeInfo, address, array, values, false)
}

func TestArraySetAndGet(t *testing.T) {

	t.Run("new elements with similar bytesize", func(t *testing.T) {
		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arrayCount; i++ {
			oldValue := values[i]
			newValue := test_utils.Uint64Value(i * 10)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, oldValue, existingValue)
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

		const arrayCount = 50

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arrayCount; i++ {
			oldValue := values[i]
			newValue := test_utils.Uint64Value(math.MaxUint64 - arrayCount + i + 1)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, oldValue, existingValue)
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

		const arrayCount = 50

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(math.MaxUint64 - arrayCount + i + 1)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)

		for i := uint64(0); i < arrayCount; i++ {
			oldValue := values[i]
			newValue := test_utils.Uint64Value(i)
			values[i] = newValue

			existingStorable, err := array.Set(i, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, oldValue, existingValue)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("index out of bounds", func(t *testing.T) {

		const arrayCount = 1024

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, 0, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)
		}

		r := newRand(t)

		v := test_utils.NewStringValue(randStr(r, 1024))
		storable, err := array.Set(array.Count(), v)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))

		var userError *atree.UserError
		var indexOutOfBoundsError *atree.IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)

		testArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestArrayInsertAndGet(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("insert-first", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(arrayCount - i - 1)
			values[arrayCount-i-1] = v
			err := array.Insert(0, v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("insert-last", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("insert", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, 0, arrayCount)
		for i := uint64(0); i < arrayCount; i += 2 {
			v := test_utils.Uint64Value(i)
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)
		}

		for i := uint64(1); i < arrayCount; i += 2 {
			v := test_utils.Uint64Value(i)

			values = append(values, nil)
			copy(values[i+1:], values[i:])
			values[i] = v

			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("index out of bounds", func(t *testing.T) {

		const arrayCount = 1024

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, 0, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)
		}

		r := newRand(t)

		v := test_utils.NewStringValue(randStr(r, 1024))
		err = array.Insert(array.Count()+1, v)
		require.Equal(t, 1, errorCategorizationCount(err))

		var userError *atree.UserError
		var indexOutOfBoundsError *atree.IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)

		testArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestArrayRemove(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("remove-first", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arrayCount), array.Count())

		for i := uint64(0); i < arrayCount; i++ {
			existingStorable, err := array.Remove(0)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			testValueEqual(t, values[i], existingValue)

			if id, ok := existingStorable.(atree.SlabIDStorable); ok {
				err = array.Storage.Remove(atree.SlabID(id))
				require.NoError(t, err)
			}

			require.Equal(t, arrayCount-i-1, array.Count())

			if i%256 == 0 {
				testArray(t, storage, typeInfo, address, array, values[i+1:], false)
			}
		}

		testEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("remove-last", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arrayCount), array.Count())

		for i := arrayCount - 1; i >= 0; i-- {
			existingStorable, err := array.Remove(uint64(i))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			testValueEqual(t, values[i], existingValue)

			if id, ok := existingStorable.(atree.SlabIDStorable); ok {
				err = array.Storage.Remove(atree.SlabID(id))
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

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
		require.Equal(t, uint64(arrayCount), array.Count())

		// Remove every other elements
		for i := uint64(0); i < arrayCount/2; i++ {
			v := values[i]

			existingStorable, err := array.Remove(i)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(array.Storage)
			require.NoError(t, err)

			testValueEqual(t, v, existingValue)

			if id, ok := existingStorable.(atree.SlabIDStorable); ok {
				err = array.Storage.Remove(atree.SlabID(id))
				require.NoError(t, err)
			}

			copy(values[i:], values[i+1:])
			values = values[:len(values)-1]

			require.Equal(t, uint64(len(values)), array.Count())

			if i%256 == 0 {
				testArray(t, storage, typeInfo, address, array, values, false)
			}
		}

		require.Equal(t, arrayCount/2, len(values))

		testArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("index out of bounds", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		storable, err := array.Remove(array.Count())
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))

		var userError *atree.UserError
		var indexOutOfBounds *atree.IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBounds)
		require.ErrorAs(t, userError, &indexOutOfBounds)

		testArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestReadOnlyArrayIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		i := uint64(0)
		err = array.IterateReadOnly(func(atree.Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
	})

	t.Run("append", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		err = array.IterateReadOnly(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arrayCount), i)
	})

	t.Run("set", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(0))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arrayCount; i++ {
			existingStorable, err := array.Set(i, test_utils.Uint64Value(i))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, test_utils.Uint64Value(0), existingValue)
		}

		i := uint64(0)
		err = array.IterateReadOnly(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arrayCount), i)
	})

	t.Run("insert", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i += 2 {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(1); i < arrayCount; i += 2 {
			err := array.Insert(i, test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		err = array.IterateReadOnly(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.Uint64Value(i), v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arrayCount), i)
	})

	t.Run("remove", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		// Remove every other elements
		for i := uint64(0); i < array.Count(); i++ {
			storable, err := array.Remove(i)
			require.NoError(t, err)
			require.Equal(t, test_utils.Uint64Value(i*2), storable)
		}

		i := uint64(0)
		j := uint64(1)
		err = array.IterateReadOnly(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.Uint64Value(j), v)
			i++
			j += 2
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(arrayCount/2), i)
	})

	t.Run("stop", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const count = 10
		for i := uint64(0); i < count; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		i := 0
		err = array.IterateReadOnly(func(_ atree.Value) (bool, error) {
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

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const count = 10
		for i := uint64(0); i < count; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		testErr := errors.New("test")

		i := 0
		err = array.IterateReadOnly(func(_ atree.Value) (bool, error) {
			if i == count/2 {
				return false, testErr
			}
			i++
			return true, nil
		})
		// err is testErr wrapped in atree.ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *atree.ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())

		require.Equal(t, count/2, i)
	})
}

func TestMutateElementFromReadOnlyArrayIterator(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	var mutationError *atree.ReadOnlyIteratorElementMutationError

	t.Run("mutate inlined element from IterateReadOnly", func(t *testing.T) {
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.False(t, childArray.Inlined())

		// parent array [[]]
		err = parentArray.Append(childArray)
		require.NoError(t, err)
		require.True(t, childArray.Inlined())

		// Iterate and modify element
		var valueMutationCallbackCalled bool
		err = parentArray.IterateReadOnlyWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.Array)
				require.True(t, ok)
				require.True(t, c.Inlined())

				err = c.Append(test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)

				return true, err
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined element from IterateReadOnlyRange", func(t *testing.T) {
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := atree.NewArray(storage, address, typeInfo)
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
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.Array)
				require.True(t, ok)
				require.True(t, c.Inlined())

				err = c.Append(test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)

				return true, err
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined array element from IterateReadOnly", func(t *testing.T) {
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.False(t, childArray.Inlined())

		// parent array [[]]
		err = parentArray.Append(childArray)
		require.NoError(t, err)
		require.True(t, childArray.Inlined())

		// Inserting elements into childArray so it can't be inlined
		for i := 0; childArray.Inlined(); i++ {
			v := test_utils.Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)
		}

		// Iterate and modify element
		var valueMutationCallbackCalled bool
		err = parentArray.IterateReadOnlyWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.Array)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingStorable, err := c.Remove(0)
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined array element from IterateReadOnlyRange", func(t *testing.T) {
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// child array []
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.False(t, childArray.Inlined())

		// parent array [[]]
		err = parentArray.Append(childArray)
		require.NoError(t, err)
		require.True(t, childArray.Inlined())

		// Inserting elements into childArray so it can't be inlined
		for i := 0; childArray.Inlined(); i++ {
			v := test_utils.Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)
		}

		// Iterate and modify element
		var valueMutationCallbackCalled bool
		err = parentArray.IterateReadOnlyRangeWithMutationCallback(
			0,
			parentArray.Count(),
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.Array)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingStorable, err := c.Remove(0)
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})
}

func TestMutableArrayIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		i := uint64(0)
		err = array.Iterate(func(atree.Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
	})

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 15

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
		}
		require.True(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.Uint64Value(i), v)

			// Mutate primitive array elements by overwritting existing elements of similar byte size.
			newValue := test_utils.Uint64Value(i * 2)
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, test_utils.Uint64Value(i), existingValue)

			expectedValues[i] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 1024

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
		}
		require.False(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.Uint64Value(i), v)

			// Mutate primitive array elements by overwritting existing elements with elements of similar size.
			newValue := test_utils.Uint64Value(i * 2)
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, test_utils.Uint64Value(i), existingValue)

			expectedValues[i] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.False(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 15

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		r := rune('a')
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.NewStringValue(string(r))
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
			r++
		}
		require.True(t, IsArrayRootDataSlab(array))

		i := 0
		r = rune('a')
		err = array.Iterate(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.NewStringValue(string(r)), v)

			// Mutate primitive array elements by overwritting existing elements with larger elements.
			// Larger elements causes slabs to split.
			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, test_utils.NewStringValue(string(r)), existingValue)

			expectedValues[i] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.False(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 200

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		r := rune('a')
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.NewStringValue(string(r))
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
			r++
		}
		require.False(t, IsArrayRootDataSlab(array))

		i := 0
		r = rune('a')
		err = array.Iterate(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.NewStringValue(string(r)), v)

			// Mutate primitive array elements by overwritting existing elements with larger elements.
			// Larger elements causes slabs to split.
			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, test_utils.NewStringValue(string(r)), existingValue)

			expectedValues[i] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.False(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 80

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		r := rune('a')
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.NewStringValue(strings.Repeat(string(r), 25))
			err = array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = v
			r++
		}
		require.False(t, IsArrayRootDataSlab(array))

		i := 0
		r = rune('a')
		err = array.Iterate(func(v atree.Value) (bool, error) {
			require.Equal(t, test_utils.NewStringValue(strings.Repeat(string(r), 25)), v)

			// Mutate primitive array elements by overwritting existing elements with smaller elements.
			// Smaller elements causes slabs to merge.
			newValue := test_utils.NewStringValue(string(r))
			existingStorable, err := array.Set(uint64(i), newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, test_utils.NewStringValue(strings.Repeat(string(r), 25)), existingValue)

			expectedValues[i] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 15

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			v := test_utils.Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{v}
		}
		require.True(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(1), childArray.Count())
			require.True(t, childArray.Inlined())

			// Mutate array elements by inserting more elements to child arrays.
			newElement := test_utils.Uint64Value(0)
			err := childArray.Append(newElement)
			require.NoError(t, err)
			require.Equal(t, uint64(2), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 25

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			v := test_utils.Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{v}
		}
		require.False(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(1), childArray.Count())
			require.True(t, childArray.Inlined())

			// Mutate array elements by inserting more elements to child arrays.
			newElement := test_utils.Uint64Value(0)
			err := childArray.Append(newElement)
			require.NoError(t, err)
			require.Equal(t, uint64(2), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.False(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			arrayCount             = 15
			childArrayCount        = 1
			mutatedChildArrayCount = 4
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue test_utils.ExpectedArrayValue
			for j := i; j < i+childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}
		require.True(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			// Mutate array elements by inserting more elements to child arrays.
			for j := i; j < i+mutatedChildArrayCount-childArrayCount; j++ {
				newElement := test_utils.Uint64Value(j)

				err := childArray.Append(newElement)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			}

			require.Equal(t, uint64(mutatedChildArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.False(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			arrayCount             = 25
			childArrayCount        = 1
			mutatedChildArrayCount = 4
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue test_utils.ExpectedArrayValue
			for j := i; j < i+childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}
		require.False(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			// Mutate array elements by inserting more elements to child arrays.
			for j := i; j < i+mutatedChildArrayCount-childArrayCount; j++ {
				newElement := test_utils.Uint64Value(j)

				err := childArray.Append(newElement)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			}

			require.Equal(t, uint64(mutatedChildArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.False(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			arrayCount             = 10
			childArrayCount        = 10
			mutatedChildArrayCount = 1
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue test_utils.ExpectedArrayValue
			for j := i; j < i+childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.False(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			for j := childArrayCount - 1; j > mutatedChildArrayCount-1; j-- {
				existingStorble, err := childArray.Remove(uint64(j))
				require.NoError(t, err)

				existingValue, err := existingStorble.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, test_utils.Uint64Value(i+j), existingValue)
			}

			require.Equal(t, uint64(mutatedChildArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues[:mutatedChildArrayCount]

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			arrayCount             = 2
			childArrayCount        = 1
			mutatedChildArrayCount = 50
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue test_utils.ExpectedArrayValue
			for j := i; j < i+childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.True(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			for j := childArrayCount; j < mutatedChildArrayCount; j++ {
				v := test_utils.Uint64Value(i + j)

				err := childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, v)
			}

			require.Equal(t, uint64(mutatedChildArrayCount), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)

		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			arrayCount             = 10
			childArrayCount        = 10
			mutatedChildArrayCount = 50
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue test_utils.ExpectedArrayValue

			for j := i; j < i+childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.False(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			for j := childArrayCount; j < mutatedChildArrayCount; j++ {
				v := test_utils.Uint64Value(i + j)

				err := childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues = append(expectedChildArrayValues, v)
			}

			require.Equal(t, uint64(mutatedChildArrayCount), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)
		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			arrayCount             = 2
			childArrayCount        = 50
			mutatedChildArrayCount = 1
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue test_utils.ExpectedArrayValue

			for j := i; j < i+childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.True(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArrayCount), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			for j := childArrayCount - 1; j > mutatedChildArrayCount-1; j-- {
				existingStorable, err := childArray.Remove(uint64(j))
				require.NoError(t, err)

				value, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, test_utils.Uint64Value(i+j), value)
			}

			require.Equal(t, uint64(mutatedChildArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues[:1]

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)

		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			arrayCount             = 4
			childArrayCount        = 50
			mutatedChildArrayCount = 25
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var expectedValue test_utils.ExpectedArrayValue

			for j := i; j < i+childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedValue = append(expectedValue, v)
			}

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = expectedValue
		}

		require.True(t, IsArrayRootDataSlab(array))

		i := 0
		err = array.Iterate(func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(childArrayCount), childArray.Count())
			require.False(t, childArray.Inlined())

			expectedChildArrayValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			for j := childArrayCount - 1; j >= mutatedChildArrayCount; j-- {
				existingStorable, err := childArray.Remove(uint64(j))
				require.NoError(t, err)

				value, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)
				require.Equal(t, test_utils.Uint64Value(i+j), value)
			}

			require.Equal(t, uint64(mutatedChildArrayCount), childArray.Count())
			require.True(t, childArray.Inlined())

			expectedValues[i] = expectedChildArrayValues[:mutatedChildArrayCount]

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)

		require.False(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("stop", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const count = 10
		for i := uint64(0); i < count; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		i := 0
		err = array.Iterate(func(_ atree.Value) (bool, error) {
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

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const count = 10
		for i := uint64(0); i < count; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		testErr := errors.New("test")

		i := 0
		err = array.Iterate(func(_ atree.Value) (bool, error) {
			if i == count/2 {
				return false, testErr
			}
			i++
			return true, nil
		})
		// err is testErr wrapped in atree.ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *atree.ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())

		require.Equal(t, count/2, i)
	})
}

func testArrayIterateRange(t *testing.T, array *atree.Array, values []atree.Value) {
	var i uint64
	var err error
	var sliceOutOfBoundsError *atree.SliceOutOfBoundsError
	var invalidSliceIndexError *atree.InvalidSliceIndexError

	count := array.Count()

	// If startIndex > count, IterateRange returns atree.SliceOutOfBoundsError
	err = array.IterateReadOnlyRange(count+1, count+1, func(atree.Value) (bool, error) {
		i++
		return true, nil
	})
	require.Equal(t, 1, errorCategorizationCount(err))

	var userError *atree.UserError
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &sliceOutOfBoundsError)
	require.ErrorAs(t, userError, &sliceOutOfBoundsError)
	require.Equal(t, uint64(0), i)

	// If endIndex > count, IterateRange returns atree.SliceOutOfBoundsError
	err = array.IterateReadOnlyRange(0, count+1, func(atree.Value) (bool, error) {
		i++
		return true, nil
	})
	require.Equal(t, 1, errorCategorizationCount(err))
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &sliceOutOfBoundsError)
	require.ErrorAs(t, userError, &sliceOutOfBoundsError)
	require.Equal(t, uint64(0), i)

	// If startIndex > endIndex, IterateRange returns atree.InvalidSliceIndexError
	if count > 0 {
		err = array.IterateReadOnlyRange(1, 0, func(atree.Value) (bool, error) {
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
			err = array.IterateReadOnlyRange(startIndex, endIndex, func(v atree.Value) (bool, error) {
				testValueEqual(t, v, values[int(startIndex+i)])
				i++
				return true, nil
			})
			require.NoError(t, err)
			require.Equal(t, endIndex-startIndex, i)
		}
	}
}

func TestReadOnlyArrayIterateRange(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		testArrayIterateRange(t, array, []atree.Value{})
	})

	t.Run("dataslab as root", func(t *testing.T) {
		const arrayCount = 10

		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			value := test_utils.Uint64Value(i)
			values[i] = value
			err := array.Append(value)
			require.NoError(t, err)
		}

		testArrayIterateRange(t, array, values)
	})

	t.Run("metadataslab as root", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 1024

		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			value := test_utils.Uint64Value(i)
			values[i] = value
			err := array.Append(value)
			require.NoError(t, err)
		}

		testArrayIterateRange(t, array, values)
	})

	t.Run("stop", func(t *testing.T) {
		const arrayCount = 10

		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		startIndex := uint64(1)
		endIndex := uint64(5)
		count := endIndex - startIndex
		err = array.IterateReadOnlyRange(startIndex, endIndex, func(_ atree.Value) (bool, error) {
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

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 10
		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		testErr := errors.New("test")

		i := uint64(0)
		startIndex := uint64(1)
		endIndex := uint64(5)
		count := endIndex - startIndex
		err = array.IterateReadOnlyRange(startIndex, endIndex, func(_ atree.Value) (bool, error) {
			if i == count/2 {
				return false, testErr
			}
			i++
			return true, nil
		})
		// err is testErr wrapped in atree.ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *atree.ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())
		require.Equal(t, count/2, i)
	})
}

func TestMutableArrayIterateRange(t *testing.T) {

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		i := 0
		err = array.IterateRange(0, 0, func(atree.Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 15

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			v := test_utils.Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{v}
		}
		require.True(t, IsArrayRootDataSlab(array))

		sizeBeforeMutation := GetArrayRootSlabByteSize(array)

		i := 0
		startIndex := uint64(1)
		endIndex := array.Count() - 2
		newElement := test_utils.Uint64Value(0)
		err = array.IterateRange(startIndex, endIndex, func(v atree.Value) (bool, error) {
			childArray, ok := v.(*atree.Array)
			require.True(t, ok)
			require.Equal(t, uint64(1), childArray.Count())
			require.True(t, childArray.Inlined())

			err := childArray.Append(newElement)
			require.NoError(t, err)

			index := int(startIndex) + i
			expectedChildArrayValues, ok := expectedValues[index].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildArrayValues = append(expectedChildArrayValues, newElement)
			expectedValues[index] = expectedChildArrayValues

			i++

			require.Equal(t, GetArrayRootSlabByteSize(array), sizeBeforeMutation+uint32(i)*newElement.ByteSize())

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, endIndex-startIndex, uint64(i))
		require.True(t, IsArrayRootDataSlab(array))

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("stop", func(t *testing.T) {
		const arrayCount = 10

		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		i := uint64(0)
		startIndex := uint64(1)
		endIndex := uint64(5)
		count := endIndex - startIndex
		err = array.IterateRange(startIndex, endIndex, func(_ atree.Value) (bool, error) {
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

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 10
		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		testErr := errors.New("test")

		i := uint64(0)
		startIndex := uint64(1)
		endIndex := uint64(5)
		count := endIndex - startIndex
		err = array.IterateRange(startIndex, endIndex, func(_ atree.Value) (bool, error) {
			if i == count/2 {
				return false, testErr
			}
			i++
			return true, nil
		})
		// err is testErr wrapped in atree.ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *atree.ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())
		require.Equal(t, count/2, i)
	})
}

func TestArrayRootSlabID(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	const arrayCount = 4096

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	savedRootID := array.SlabID()
	require.NotEqual(t, atree.SlabIDUndefined, savedRootID)

	// Append elements
	for i := uint64(0); i < arrayCount; i++ {
		err := array.Append(test_utils.Uint64Value(i))
		require.NoError(t, err)
		require.Equal(t, savedRootID, array.SlabID())
	}

	require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(arrayCount), array.Count())

	// Remove elements
	for i := uint64(0); i < arrayCount; i++ {
		storable, err := array.Remove(0)
		require.NoError(t, err)
		require.Equal(t, test_utils.Uint64Value(i), storable)
		require.Equal(t, savedRootID, array.SlabID())
	}

	require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
	require.Equal(t, address, array.Address())
	require.Equal(t, uint64(0), array.Count())
	require.Equal(t, savedRootID, array.SlabID())
}

func TestArraySetRandomValues(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	const arrayCount = 4096

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]atree.Value, arrayCount)
	for i := uint64(0); i < arrayCount; i++ {
		v := test_utils.Uint64Value(i)
		values[i] = v
		err := array.Append(v)
		require.NoError(t, err)
	}

	for i := uint64(0); i < arrayCount; i++ {
		oldValue := values[i]
		newValue := randomValue(r, int(atree.MaxInlineArrayElementSize()))
		values[i] = newValue

		existingStorable, err := array.Set(i, newValue)
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, oldValue, existingValue)
	}

	testArray(t, storage, typeInfo, address, array, values, false)
}

func TestArrayInsertRandomValues(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("insert-first", func(t *testing.T) {

		const arrayCount = 4096

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))
			values[arrayCount-i-1] = v

			err := array.Insert(0, v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("insert-last", func(t *testing.T) {

		const arrayCount = 4096

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))
			values[i] = v

			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("insert-random", func(t *testing.T) {

		const arrayCount = 4096

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			k := r.Intn(int(i) + 1)
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))

			copy(values[k+1:], values[k:])
			values[k] = v

			err := array.Insert(uint64(k), v)
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)
	})
}

func TestArrayRemoveRandomValues(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	const arrayCount = 4096

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]atree.Value, arrayCount)
	// Insert n random values into array
	for i := uint64(0); i < arrayCount; i++ {
		v := randomValue(r, int(atree.MaxInlineArrayElementSize()))
		values[i] = v

		err := array.Insert(i, v)
		require.NoError(t, err)
	}

	testArray(t, storage, typeInfo, address, array, values, false)

	// Remove n elements at random index
	for i := uint64(0); i < arrayCount; i++ {
		k := r.Intn(int(array.Count()))

		existingStorable, err := array.Remove(uint64(k))
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, values[k], existingValue)

		copy(values[k:], values[k+1:])
		values = values[:len(values)-1]

		if id, ok := existingStorable.(atree.SlabIDStorable); ok {
			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}
	}

	testEmptyArray(t, storage, typeInfo, address, array)
}

func testArrayAppendSetInsertRemoveRandomValues(
	t *testing.T,
	r *rand.Rand,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	opCount int,
) (*atree.Array, []atree.Value) {
	const (
		ArrayAppendOp = iota
		ArrayInsertOp
		ArraySetOp
		ArrayRemoveOp
		MaxArrayOp
	)

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]atree.Value, 0, opCount)
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
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))
			values = append(values, v)

			err := array.Append(v)
			require.NoError(t, err)

		case ArraySetOp:
			k := r.Intn(int(array.Count()))
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))

			oldV := values[k]

			values[k] = v

			existingStorable, err := array.Set(uint64(k), v)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, oldV, existingValue)

			if id, ok := existingStorable.(atree.SlabIDStorable); ok {
				err = storage.Remove(atree.SlabID(id))
				require.NoError(t, err)
			}

		case ArrayInsertOp:
			k := r.Intn(int(array.Count() + 1))
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))

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
			testValueEqual(t, values[k], existingValue)

			copy(values[k:], values[k+1:])
			values = values[:len(values)-1]

			if id, ok := existingStorable.(atree.SlabIDStorable); ok {
				err = storage.Remove(atree.SlabID(id))
				require.NoError(t, err)
			}
		}

		require.Equal(t, uint64(len(values)), array.Count())
		require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
		require.Equal(t, address, array.Address())
	}

	return array, values
}

func TestArrayAppendSetInsertRemoveRandomValues(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	const opCount = 4096

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, values := testArrayAppendSetInsertRemoveRandomValues(t, r, storage, typeInfo, address, opCount)
	testArray(t, storage, typeInfo, address, array, values, false)
}

func TestArrayWithChildArrayMap(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("small array", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		childTypeInfo := test_utils.NewSimpleTypeInfo(43)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Create child arrays with 1 element.
		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, childTypeInfo)
			require.NoError(t, err)

			v := test_utils.Uint64Value(i)

			err = childArray.Append(v)
			require.NoError(t, err)

			require.True(t, IsArrayRootDataSlab(childArray))
			require.False(t, childArray.Inlined())

			err = array.Append(childArray)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedValues[i] = test_utils.ExpectedArrayValue{v}
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("big array", func(t *testing.T) {

		const arrayCount = 4096
		const childArrayCount = 40

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		childTypeInfo := test_utils.NewSimpleTypeInfo(43)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Create child arrays with 40 element.
		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childArray, err := atree.NewArray(storage, address, childTypeInfo)
			require.NoError(t, err)

			expectedChildArrayValues := make([]atree.Value, childArrayCount)
			for i := uint64(0); i < childArrayCount; i++ {
				v := test_utils.Uint64Value(math.MaxUint64)

				err := childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues[i] = v
			}

			require.False(t, IsArrayRootDataSlab(childArray))

			err = array.Append(childArray)
			require.NoError(t, err)
			require.False(t, childArray.Inlined())

			expectedValues[i] = test_utils.ExpectedArrayValue(expectedChildArrayValues)
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("small map", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		childArayTypeInfo := test_utils.NewSimpleTypeInfo(43)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childArayTypeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)
			storable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)

			require.True(t, IsMapRootDataSlab(childMap))

			err = array.Append(childMap)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedMapValue{k: v}
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, false)
	})

	t.Run("big map", func(t *testing.T) {

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		nestedTypeInfo := test_utils.NewSimpleTypeInfo(43)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), nestedTypeInfo)
			require.NoError(t, err)

			expectedChildMapValues := test_utils.ExpectedMapValue{}
			for i := uint64(0); i < 25; i++ {
				k := test_utils.Uint64Value(i)
				v := test_utils.Uint64Value(i * 2)

				storable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, storable)

				expectedChildMapValues[k] = v
			}

			require.False(t, IsMapRootDataSlab(childMap))

			err = array.Append(childMap)
			require.NoError(t, err)

			expectedValues[i] = expectedChildMapValues
		}

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayDecodeV0(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)

		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		arraySlabID := atree.NewSlabID(
			address,
			atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1},
		)

		slabData := map[atree.SlabID][]byte{
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
		array, err := atree.NewArrayWithRootID(storage, arraySlabID)
		require.NoError(t, err)

		testEmptyArrayV0(t, storage, typeInfo, address, array)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)

		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		arraySlabID := atree.NewSlabID(
			address,
			atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1},
		)

		values := []atree.Value{
			test_utils.Uint64Value(0),
		}

		slabData := map[atree.SlabID][]byte{
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
		array, err := atree.NewArrayWithRootID(storage, arraySlabID)
		require.NoError(t, err)

		testArrayV0(t, storage, typeInfo, address, array, values, false)
	})

	t.Run("metadataslab as root", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)

		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		arraySlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		arrayDataSlabID1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		arrayDataSlabID2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		childArraySlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

		const arrayCount = 20
		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount-1; i++ {
			values[i] = test_utils.NewStringValue(strings.Repeat("a", 22))
		}

		values[arrayCount-1] = test_utils.ExpectedArrayValue{test_utils.Uint64Value(0)}

		slabData := map[atree.SlabID][]byte{
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

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... atree.SlabID(...)]
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
		array, err := atree.NewArrayWithRootID(storage2, arraySlabID)
		require.NoError(t, err)

		testArrayV0(t, storage2, typeInfo, address, array, values, false)
	})
}

func TestArrayEncodeDecode(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
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
		array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testEmptyArray(t, storage2, typeInfo, address, array2)
	})

	t.Run("root dataslab", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		v := test_utils.Uint64Value(0)
		values := []atree.Value{v}
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
		array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, values, false)
	})

	t.Run("root metadata slab", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 18
		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.NewStringValue(strings.Repeat("a", 22))
			values[i] = v

			err := array.Append(v)
			require.NoError(t, err)
		}

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
		array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, values, false)
	})

	// Same type info is reused.
	t.Run("root data slab, inlined child array of same type", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		childTypeInfo := test_utils.NewSimpleTypeInfo(43)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 2
		expectedValues := make([]atree.Value, arrayCount)
		for i := 0; i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)

			childArray, err := atree.NewArray(storage, address, childTypeInfo)
			require.NoError(t, err)

			err = childArray.Append(v)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{v}
		}

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		array2, err := atree.NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	// Different type info are encoded.
	t.Run("root data slab, inlined array of different type", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)
		typeInfo3 := test_utils.NewSimpleTypeInfo(44)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 2
		expectedValues := make([]atree.Value, arrayCount)
		for i := 0; i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)

			var ti atree.TypeInfo
			if i == 0 {
				ti = typeInfo3
			} else {
				ti = typeInfo2
			}
			childArray, err := atree.NewArray(storage, address, ti)
			require.NoError(t, err)

			err = childArray.Append(v)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{v}
		}

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		array2, err := atree.NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	// Same type info is reused.
	t.Run("root data slab, multiple levels of inlined array of same type", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)
		typeInfo3 := test_utils.NewSimpleTypeInfo(44)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 2
		expectedValues := make([]atree.Value, arrayCount)
		for i := 0; i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)

			gchildArray, err := atree.NewArray(storage, address, typeInfo2)
			require.NoError(t, err)

			err = gchildArray.Append(v)
			require.NoError(t, err)

			childArray, err := atree.NewArray(storage, address, typeInfo3)
			require.NoError(t, err)

			err = childArray.Append(gchildArray)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{
				test_utils.ExpectedArrayValue{
					v,
				},
			}
		}

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		array2, err := atree.NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("root data slab, multiple levels of inlined array of different type", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)
		typeInfo3 := test_utils.NewSimpleTypeInfo(44)
		typeInfo4 := test_utils.NewSimpleTypeInfo(45)
		typeInfo5 := test_utils.NewSimpleTypeInfo(46)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 2
		expectedValues := make([]atree.Value, arrayCount)
		for i := 0; i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)

			var ti atree.TypeInfo
			if i == 0 {
				ti = typeInfo2
			} else {
				ti = typeInfo4
			}
			gchildArray, err := atree.NewArray(storage, address, ti)
			require.NoError(t, err)

			err = gchildArray.Append(v)
			require.NoError(t, err)

			if i == 0 {
				ti = typeInfo3
			} else {
				ti = typeInfo5
			}
			childArray, err := atree.NewArray(storage, address, ti)
			require.NoError(t, err)

			err = childArray.Append(gchildArray)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{
				test_utils.ExpectedArrayValue{
					v,
				},
			}
		}

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		array2, err := atree.NewArrayWithRootID(storage2, parentArray.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("root metadata slab, inlined array of same type", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 20
		expectedValues := make([]atree.Value, 0, arrayCount)
		for i := uint64(0); i < arrayCount-2; i++ {
			v := test_utils.NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		for i := 0; i < 2; i++ {
			childArray, err := atree.NewArray(storage, address, typeInfo2)
			require.NoError(t, err)

			v := test_utils.Uint64Value(i)
			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues = append(expectedValues, test_utils.ExpectedArrayValue{v})
		}

		require.Equal(t, uint64(arrayCount), array.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
		array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("root metadata slab, inlined array of different type", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)
		typeInfo3 := test_utils.NewSimpleTypeInfo(44)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 20
		expectedValues := make([]atree.Value, 0, arrayCount)
		for i := uint64(0); i < arrayCount-2; i++ {
			v := test_utils.NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		for i := 0; i < 2; i++ {
			var ti atree.TypeInfo
			if i == 0 {
				ti = typeInfo3
			} else {
				ti = typeInfo2
			}

			childArray, err := atree.NewArray(storage, address, ti)
			require.NoError(t, err)

			v := test_utils.Uint64Value(i)

			err = childArray.Append(v)
			require.NoError(t, err)

			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues = append(expectedValues, test_utils.ExpectedArrayValue{v})
		}

		require.Equal(t, uint64(arrayCount), array.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
		array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("has pointers", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 20
		expectedValues := make([]atree.Value, 0, arrayCount)
		for i := uint64(0); i < arrayCount-1; i++ {
			v := test_utils.NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		const childArrayCount = 5

		childArray, err := atree.NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		expectedChildArrayValues := make([]atree.Value, childArrayCount)
		for i := 0; i < childArrayCount; i++ {
			v := test_utils.NewStringValue(strings.Repeat("b", 22))
			err = childArray.Append(v)
			require.NoError(t, err)
			expectedChildArrayValues[i] = v
		}

		err = array.Append(childArray)
		require.NoError(t, err)

		expectedValues = append(expectedValues, test_utils.ExpectedArrayValue(expectedChildArrayValues))

		require.Equal(t, uint64(arrayCount), array.Count())
		require.Equal(t, uint64(5), childArray.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		id4 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... atree.SlabID(...)]
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
		array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})

	t.Run("has pointers in inlined slab", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)
		typeInfo3 := test_utils.NewSimpleTypeInfo(44)
		storage := newTestBasicStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arrayCount = 20
		expectedValues := make([]atree.Value, 0, arrayCount)
		for i := uint64(0); i < arrayCount-1; i++ {
			v := test_utils.NewStringValue(strings.Repeat("a", 22))

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, v)
		}

		childArray, err := atree.NewArray(storage, address, typeInfo3)
		require.NoError(t, err)

		gchildArray, err := atree.NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		const gchildArrayCount = 5

		expectedGChildArrayValues := make([]atree.Value, gchildArrayCount)
		for i := 0; i < gchildArrayCount; i++ {
			v := test_utils.NewStringValue(strings.Repeat("b", 22))

			err = gchildArray.Append(v)
			require.NoError(t, err)

			expectedGChildArrayValues[i] = v
		}

		err = childArray.Append(gchildArray)
		require.NoError(t, err)

		err = array.Append(childArray)
		require.NoError(t, err)

		expectedValues = append(expectedValues, test_utils.ExpectedArrayValue{
			test_utils.ExpectedArrayValue(expectedGChildArrayValues),
		})

		require.Equal(t, uint64(arrayCount), array.Count())
		require.Equal(t, uint64(1), childArray.Count())
		require.Equal(t, uint64(5), gchildArray.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		id4 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 5})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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

			// (data slab) next: 0, data: [aaaaaaaaaaaaaaaaaaaaaa ... [atree.SlabID(...)]]
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
		array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
		require.NoError(t, err)

		testArray(t, storage2, typeInfo, address, array2, expectedValues, false)
	})
}

func TestArrayEncodeDecodeRandomValues(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	const opCount = 8192

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, values := testArrayAppendSetInsertRemoveRandomValues(t, r, storage, typeInfo, address, opCount)

	testArray(t, storage, typeInfo, address, array, values, false)

	// Decode data to new storage
	storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

	// Test new array from storage2
	array2, err := atree.NewArrayWithRootID(storage2, array.SlabID())
	require.NoError(t, err)

	testArray(t, storage2, typeInfo, address, array2, values, false)
}

func TestEmptyArray(t *testing.T) {

	t.Parallel()

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestBasicStorage(t)

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	t.Run("get", func(t *testing.T) {
		s, err := array.Get(0)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var indexOutOfBoundsError *atree.IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
		require.Nil(t, s)
	})

	t.Run("set", func(t *testing.T) {
		s, err := array.Set(0, test_utils.Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var indexOutOfBoundsError *atree.IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
		require.Nil(t, s)
	})

	t.Run("insert", func(t *testing.T) {
		err := array.Insert(1, test_utils.Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var indexOutOfBoundsError *atree.IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
	})

	t.Run("remove", func(t *testing.T) {
		s, err := array.Remove(0)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var indexOutOfBoundsError *atree.IndexOutOfBoundsError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &indexOutOfBoundsError)
		require.ErrorAs(t, userError, &indexOutOfBoundsError)
		require.Nil(t, s)
	})

	t.Run("readonly iterate", func(t *testing.T) {
		i := uint64(0)
		err := array.IterateReadOnly(func(atree.Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)
	})

	t.Run("iterate", func(t *testing.T) {
		i := uint64(0)
		err := array.Iterate(func(atree.Value) (bool, error) {
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
		require.True(t, test_utils.CompareTypeInfo(typeInfo, array.Type()))
	})

	// TestArrayEncodeDecode/empty tests empty array encoding and decoding
}

func TestArrayStringElement(t *testing.T) {

	t.Parallel()

	t.Run("inline", func(t *testing.T) {

		const arrayCount = 4096

		r := newRand(t)

		stringSize := int(atree.MaxInlineArrayElementSize() - 3)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			s := randStr(r, stringSize)
			values[i] = test_utils.NewStringValue(s)
		}

		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		typeInfo := test_utils.NewSimpleTypeInfo(42)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(values[i])
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)

		stats, err := atree.GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(t, uint64(0), stats.StorableSlabCount)
	})

	t.Run("external slab", func(t *testing.T) {

		const arrayCount = 4096

		r := newRand(t)

		stringSize := int(atree.MaxInlineArrayElementSize() + 512)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			s := randStr(r, stringSize)
			values[i] = test_utils.NewStringValue(s)
		}

		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		typeInfo := test_utils.NewSimpleTypeInfo(42)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(values[i])
			require.NoError(t, err)
		}

		testArray(t, storage, typeInfo, address, array, values, false)

		stats, err := atree.GetArrayStats(array)
		require.NoError(t, err)
		require.Equal(t, uint64(arrayCount), stats.StorableSlabCount)
	})
}

func TestArrayStoredValue(t *testing.T) {

	const arrayCount = 4096

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]atree.Value, arrayCount)
	for i := uint64(0); i < arrayCount; i++ {
		v := test_utils.Uint64Value(i)
		values[i] = v
		err := array.Append(v)
		require.NoError(t, err)
	}

	rootID := array.SlabID()

	slabIterator, err := storage.SlabIterator()
	require.NoError(t, err)

	for {
		id, slab := slabIterator()

		if id == atree.SlabIDUndefined {
			break
		}

		value, err := slab.StoredValue(storage)

		if id == rootID {
			require.NoError(t, err)

			array2, ok := value.(*atree.Array)
			require.True(t, ok)

			testArray(t, storage, typeInfo, address, array2, values, false)
		} else {
			require.Equal(t, 1, errorCategorizationCount(err))
			var fatalError *atree.FatalError
			var notValueError *atree.NotValueError
			require.ErrorAs(t, err, &fatalError)
			require.ErrorAs(t, err, &notValueError)
			require.ErrorAs(t, fatalError, &notValueError)
			require.Nil(t, value)
		}
	}
}

func TestArrayPopIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		i := uint64(0)
		err = array.PopIterate(func(atree.Storable) {
			i++
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)

		testEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("root-dataslab", func(t *testing.T) {

		const arrayCount = 10

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		i := 0
		err = array.PopIterate(func(v atree.Storable) {
			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, values[arrayCount-i-1], vv)
			i++
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)

		testEmptyArray(t, storage, typeInfo, address, array)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		i := 0
		err = array.PopIterate(func(v atree.Storable) {
			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, values[arrayCount-i-1], vv)
			i++
		})
		require.NoError(t, err)
		require.Equal(t, arrayCount, i)

		testEmptyArray(t, storage, typeInfo, address, array)
	})
}

func TestArrayFromBatchData(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)

		array, err := atree.NewArray(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())

		iter, err := array.ReadOnlyIterator()
		require.NoError(t, err)

		// Create a new array with new storage, new address, and original array's elements.
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := atree.NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (atree.Value, error) {
				return iter.Next()
			})
		require.NoError(t, err)
		require.NotEqual(t, copied.SlabID(), array.SlabID())

		testEmptyArray(t, storage, typeInfo, address, copied)
	})

	t.Run("root-dataslab", func(t *testing.T) {

		const arrayCount = 10

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		array, err := atree.NewArray(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arrayCount), array.Count())

		iter, err := array.ReadOnlyIterator()
		require.NoError(t, err)

		// Create a new array with new storage, new address, and original array's elements.
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := atree.NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (atree.Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, copied.SlabID(), array.SlabID())

		testArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		array, err := atree.NewArray(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arrayCount), array.Count())

		iter, err := array.ReadOnlyIterator()
		require.NoError(t, err)

		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		storage := newTestPersistentStorage(t)
		copied, err := atree.NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (atree.Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		testArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("rebalance two data slabs", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		array, err := atree.NewArray(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		var values []atree.Value
		var v atree.Value

		v = test_utils.NewStringValue(strings.Repeat("a", int(atree.MaxInlineArrayElementSize()-2)))
		values = append(values, v)

		err = array.Insert(0, v)
		require.NoError(t, err)

		for i := 0; i < 35; i++ {
			v = test_utils.Uint64Value(i)
			values = append(values, v)

			err = array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(36), array.Count())

		iter, err := array.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := atree.NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (atree.Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		testArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("merge two data slabs", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		array, err := atree.NewArray(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		var values []atree.Value
		var v atree.Value
		for i := 0; i < 35; i++ {
			v = test_utils.Uint64Value(i)
			values = append(values, v)
			err = array.Append(v)
			require.NoError(t, err)
		}

		v = test_utils.NewStringValue(strings.Repeat("a", int(atree.MaxInlineArrayElementSize()-2)))
		values = append(values, nil)
		copy(values[25+1:], values[25:])
		values[25] = v

		err = array.Insert(25, v)
		require.NoError(t, err)

		require.Equal(t, uint64(36), array.Count())

		iter, err := array.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := atree.NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (atree.Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		testArray(t, storage, typeInfo, address, copied, values, false)
	})

	t.Run("random", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const arrayCount = 4096

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		array, err := atree.NewArray(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		values := make([]atree.Value, arrayCount)
		for i := uint64(0); i < arrayCount; i++ {
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))
			values[i] = v

			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arrayCount), array.Count())

		iter, err := array.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)

		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := atree.NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (atree.Value, error) {
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

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		array, err := atree.NewArray(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			typeInfo)
		require.NoError(t, err)

		var values []atree.Value
		var v atree.Value

		v = test_utils.NewStringValue(randStr(r, int(atree.MaxInlineArrayElementSize()-2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		v = test_utils.NewStringValue(randStr(r, int(atree.MaxInlineArrayElementSize()-2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		v = test_utils.NewStringValue(randStr(r, int(atree.MaxInlineArrayElementSize()-2)))
		values = append(values, v)
		err = array.Append(v)
		require.NoError(t, err)

		iter, err := array.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		copied, err := atree.NewArrayFromBatchData(
			storage,
			address,
			array.Type(),
			func() (atree.Value, error) {
				return iter.Next()
			})

		require.NoError(t, err)
		require.NotEqual(t, array.SlabID(), copied.SlabID())

		testArray(t, storage, typeInfo, address, copied, values, false)
	})
}

func TestArrayNestedStorables(t *testing.T) {

	t.Parallel()

	typeInfo := test_utils.NewSimpleTypeInfo(42)

	const arrayCount = 1024 * 4

	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]atree.Value, arrayCount)
	for i := uint64(0); i < arrayCount; i++ {
		s := strings.Repeat("a", int(i))
		v := test_utils.NewSomeValue(test_utils.NewStringValue(s))
		values[i] = test_utils.NewExpectedWrapperValue(test_utils.NewStringValue(s))

		err := array.Append(v)
		require.NoError(t, err)
	}

	testArray(t, storage, typeInfo, address, array, values, true)
}

func TestArrayMaxInlineElement(t *testing.T) {
	t.Parallel()

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	var values []atree.Value
	for i := 0; i < 2; i++ {
		// String length is atree.MaxInlineArrayElementSize - 3 to account for string encoding overhead.
		v := test_utils.NewStringValue(randStr(r, int(atree.MaxInlineArrayElementSize()-3)))
		values = append(values, v)

		err = array.Append(v)
		require.NoError(t, err)
	}

	require.True(t, IsArrayRootDataSlab(array))

	// Size of root data slab with two elements of max inlined size is target slab size minus
	// slab id size (next slab id is omitted in root slab), and minus 1 byte
	// (for rounding when computing max inline array element size).
	require.Equal(t, atree.TargetSlabSize()-atree.SlabIDLength-1, uint64(GetArrayRootSlabByteSize(array)))

	testArray(t, storage, typeInfo, address, array, values, false)
}

func TestArrayString(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("small", func(t *testing.T) {
		const arrayCount = 6

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		want := `[0 1 2 3 4 5]`
		require.Equal(t, want, array.String())
	})

	t.Run("large", func(t *testing.T) {
		const arrayCount = 120

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		want := `[0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119]`
		require.Equal(t, want, array.String())
	})
}

func TestArraySlabDump(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("small", func(t *testing.T) {
		const arrayCount = 6

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		want := []string{
			"level 1, ArrayDataSlab id:0x102030405060708.1 size:23 count:6 elements: [0 1 2 3 4 5]",
		}
		dumps, err := atree.DumpArraySlabs(array)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("large", func(t *testing.T) {
		const arrayCount = 120

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < arrayCount; i++ {
			err := array.Append(test_utils.Uint64Value(i))
			require.NoError(t, err)
		}

		want := []string{
			"level 1, ArrayMetaDataSlab id:0x102030405060708.1 size:40 count:120 children: [{id:0x102030405060708.2 size:213 count:54} {id:0x102030405060708.3 size:285 count:66}]",
			"level 2, ArrayDataSlab id:0x102030405060708.2 size:213 count:54 elements: [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53]",
			"level 2, ArrayDataSlab id:0x102030405060708.3 size:285 count:66 elements: [54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119]",
		}

		dumps, err := atree.DumpArraySlabs(array)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("overflow", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(test_utils.NewStringValue(strings.Repeat("a", int(atree.MaxInlineArrayElementSize()))))
		require.NoError(t, err)

		want := []string{
			"level 1, ArrayDataSlab id:0x102030405060708.1 size:24 count:1 elements: [SlabIDStorable({[1 2 3 4 5 6 7 8] [0 0 0 0 0 0 0 2]})]",
			"StorableSlab id:0x102030405060708.2 storable:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}

		dumps, err := atree.DumpArraySlabs(array)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})
}

func errorCategorizationCount(err error) int {
	var fatalError *atree.FatalError
	var userError *atree.UserError
	var externalError *atree.ExternalError

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

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

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

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// parent array: 1 root data slab
		require.Equal(t, 1, GetDeltasCount(storage))
		require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

		testArrayLoadedElements(t, array, nil)
	})

	runTest("root data slab with simple values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 3
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root data slab
			require.Equal(t, 1, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root data slab with composite values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 3
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arrayCount, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root data slab with composite values, unload composite element from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arrayCount, GetDeltasCount(storage))
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

			const arrayCount = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arrayCount, GetDeltasCount(storage))
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

			const arrayCount = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arrayCount, GetDeltasCount(storage))
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

			const arrayCount = 3
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+arrayCount, GetDeltasCount(storage))
			require.Equal(t, 0, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			i := 0
			err := array.IterateReadOnlyLoadedValues(func(v atree.Value) (bool, error) {
				// At this point, iterator returned first element (v).

				// Remove all other nested composite elements (except first element) from storage.
				for _, slabID := range childSlabIDs[1:] {
					err := storage.Remove(slabID)
					require.NoError(t, err)
				}

				require.Equal(t, 0, i)
				testValueEqual(t, values[0], v)
				i++
				return true, nil
			})

			require.NoError(t, err)
			require.Equal(t, 1, i) // Only first element is iterated because other elements are remove during iteration.
		}
	})

	runTest("root data slab with simple and composite values, unload composite element", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			const arrayCount = 3

			// Create an array with nested composite value at specified index
			for childArrayIndex := 0; childArrayIndex < arrayCount; childArrayIndex++ {
				storage := newTestPersistentStorage(t)

				array, values, childSlabID := createArrayWithSimpleAndChildArrayValues(t, storage, address, typeInfo, arrayCount, childArrayIndex, useWrapperValue)

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

			const arrayCount = 20
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			require.Equal(t, 3, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root metadata slab with composite values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 20
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arrayCount, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root metadata slab with composite values, unload composite element from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 20
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arrayCount, GetDeltasCount(storage))
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

			const arrayCount = 20
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arrayCount, GetDeltasCount(storage))
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

			const arrayCount = 20
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array: 1 root metadata slab, 2 data slabs
			// nested composite value element: 1 root data slab for each
			require.Equal(t, 3+arrayCount, GetDeltasCount(storage))
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
			const arrayCount = 20

			// Create an array with composite value at specified index.
			for childArrayIndex := 0; childArrayIndex < arrayCount; childArrayIndex++ {
				storage := newTestPersistentStorage(t)

				array, values, childSlabID := createArrayWithSimpleAndChildArrayValues(t, storage, address, typeInfo, arrayCount, childArrayIndex, useWrapperValue)

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

			const arrayCount = 30
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			metaDataSlab, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, childCounts := atree.GetArrayMetaDataSlabChildInfo(metaDataSlab)

			// Unload data slabs from front to back
			for i := 0; i < len(childSlabIDs); i++ {

				slabID := childSlabIDs[i]
				count := childCounts[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				values = values[count:]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab, unload data slab from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 30
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			metaDataSlab, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, childCounts := atree.GetArrayMetaDataSlabChildInfo(metaDataSlab)

			// Unload data slabs from back to front
			for i := len(childSlabIDs) - 1; i >= 0; i-- {

				slabID := childSlabIDs[i]
				count := childCounts[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				values = values[:len(values)-int(count)]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab, unload data slab in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 30
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getArrayMetaDataSlabCount(storage))

			testArrayLoadedElements(t, array, values)

			metaDataSlab, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, childCounts := atree.GetArrayMetaDataSlabChildInfo(metaDataSlab)

			require.True(t, len(childSlabIDs) > 2)

			const index = 1

			slabID := childSlabIDs[index]
			count := childCounts[index]

			err := storage.Remove(slabID)
			require.NoError(t, err)

			var accumulativeCount uint32
			for i := 0; i < index; i++ {
				accumulativeCount += childCounts[i]
			}
			copy(values[accumulativeCount:], values[accumulativeCount+count:])
			values = values[:array.Count()-uint64(count)]

			testArrayLoadedElements(t, array, values)
		}
	})

	runTest("root metadata slab, unload non-root metadata slab from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 250
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, 2 non-root metadata slabs, n data slabs
			require.Equal(t, 3, getArrayMetaDataSlabCount(storage))

			rootMetaDataSlab, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, childCounts := atree.GetArrayMetaDataSlabChildInfo(rootMetaDataSlab)

			// Unload non-root metadata slabs from front to back
			for i := 0; i < len(childSlabIDs); i++ {

				slabID := childSlabIDs[i]
				count := childCounts[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				values = values[count:]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab, unload non-root metadata slab from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const arrayCount = 250
			array, values := createArrayWithSimpleValues(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, 2 child metadata slabs, n data slabs
			require.Equal(t, 3, getArrayMetaDataSlabCount(storage))

			rootMetaDataSlab, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, childCounts := atree.GetArrayMetaDataSlabChildInfo(rootMetaDataSlab)

			// Unload non-root metadata slabs from back to front
			for i := len(childSlabIDs) - 1; i >= 0; i-- {

				slabID := childSlabIDs[i]
				count := childCounts[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				values = values[count:]

				testArrayLoadedElements(t, array, values)
			}
		}
	})

	runTest("root metadata slab with composite values, unload random composite value", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {

			storage := newTestPersistentStorage(t)

			const arrayCount = 500
			array, values, childSlabIDs := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// nested composite elements: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+arrayCount)
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

			const arrayCount = 500
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// nested composite elements: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+arrayCount)
			require.True(t, getArrayMetaDataSlabCount(storage) > 1)

			testArrayLoadedElements(t, array, values)

			rootMetaDataSlab, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _ := atree.GetArrayMetaDataSlabChildInfo(rootMetaDataSlab)

			type slabInfo struct {
				id         atree.SlabID
				startIndex int
				count      int
			}

			count := 0
			var dataSlabInfos []*slabInfo
			for _, slabID := range childSlabIDs {
				nonrootMetaDataSlab, ok := atree.GetDeltas(storage)[slabID].(*atree.ArrayMetaDataSlab)
				require.True(t, ok)

				nonrootChildSlabIDs, nonrootChildCounts := atree.GetArrayMetaDataSlabChildInfo(nonrootMetaDataSlab)

				for i := 0; i < len(nonrootChildSlabIDs); i++ {
					nonrootSlabID := nonrootChildSlabIDs[i]
					nonrootCount := nonrootChildCounts[i]

					dataSlabInfo := &slabInfo{id: nonrootSlabID, startIndex: count, count: int(
						nonrootCount)}
					dataSlabInfos = append(dataSlabInfos, dataSlabInfo)
					count += int(nonrootCount)
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

			const arrayCount = 500
			array, values, _ := createArrayWithChildArrays(t, storage, address, typeInfo, arrayCount, useWrapperValue)

			// parent array (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// nested composite elements: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+arrayCount)
			require.True(t, getArrayMetaDataSlabCount(storage) > 1)

			testArrayLoadedElements(t, array, values)

			type slabInfo struct {
				id         atree.SlabID
				startIndex int
				count      int
				children   []*slabInfo
			}

			rootMetaDataSlab, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, childCounts := atree.GetArrayMetaDataSlabChildInfo(rootMetaDataSlab)

			var dataSlabCount, metadataSlabCount int
			nonrootMetadataSlabInfos := make([]*slabInfo, len(childSlabIDs))
			for i := 0; i < len(childSlabIDs); i++ {

				slabID := childSlabIDs[i]
				count := childCounts[i]

				nonrootMetadataSlabInfo := &slabInfo{
					id:         slabID,
					startIndex: metadataSlabCount,
					count:      int(count),
				}
				metadataSlabCount += int(count)

				nonrootMetadataSlab, ok := atree.GetDeltas(storage)[slabID].(*atree.ArrayMetaDataSlab)
				require.True(t, ok)

				nonrootChildSlabIDs, nonrootChildCounts := atree.GetArrayMetaDataSlabChildInfo(nonrootMetadataSlab)

				children := make([]*slabInfo, len(nonrootChildSlabIDs))
				for j := 0; j < len(nonrootChildSlabIDs); j++ {

					nonrootSlabID := nonrootChildSlabIDs[j]
					nonrootCount := nonrootChildCounts[j]

					children[j] = &slabInfo{
						id:         nonrootSlabID,
						startIndex: dataSlabCount,
						count:      int(nonrootCount),
					}
					dataSlabCount += int(nonrootCount)
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
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	arrayCount int,
	useWrapperValue bool,
) (*atree.Array, []atree.Value) {

	// Create parent array
	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]atree.Value, arrayCount)
	r := rune('a')
	for i := 0; i < arrayCount; i++ {
		s := test_utils.NewStringValue(strings.Repeat(string(r), 20))

		if useWrapperValue {
			err := array.Append(test_utils.NewSomeValue(s))
			require.NoError(t, err)

			values[i] = test_utils.NewExpectedWrapperValue(s)
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
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	arrayCount int,
	useWrapperValue bool,
) (*atree.Array, []atree.Value, []atree.SlabID) {
	const childArrayCount = 50

	// Create parent array
	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	expectedValues := make([]atree.Value, arrayCount)
	childSlabIDs := make([]atree.SlabID, arrayCount)

	for i := 0; i < arrayCount; i++ {
		// Create child array
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedChildArrayValues := make([]atree.Value, childArrayCount)
		for j := 0; j < childArrayCount; j++ {
			v := test_utils.Uint64Value(j)
			err = childArray.Append(v)
			require.NoError(t, err)

			expectedChildArrayValues[j] = v
		}

		childSlabIDs[i] = childArray.SlabID()

		// Append nested array to parent
		if useWrapperValue {
			err = array.Append(test_utils.NewSomeValue(childArray))
			require.NoError(t, err)

			expectedValues[i] = test_utils.NewExpectedWrapperValue(test_utils.ExpectedArrayValue(expectedChildArrayValues))
		} else {
			err = array.Append(childArray)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue(expectedChildArrayValues)
		}
	}

	return array, expectedValues, childSlabIDs
}

func createArrayWithSimpleAndChildArrayValues(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	arrayCount int,
	compositeValueIndex int,
	useWrapperValue bool,
) (*atree.Array, []atree.Value, atree.SlabID) {
	const childArrayCount = 50

	require.True(t, compositeValueIndex < arrayCount)

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	expectedValues := make([]atree.Value, arrayCount)
	var childSlabID atree.SlabID
	r := 'a'
	for i := 0; i < arrayCount; i++ {

		if compositeValueIndex == i {
			// Create child array with one element
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			expectedChildArrayValues := make([]atree.Value, childArrayCount)
			for j := 0; j < childArrayCount; j++ {
				v := test_utils.Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedChildArrayValues[j] = v
			}

			if useWrapperValue {
				err = array.Append(test_utils.NewSomeValue(childArray))
				require.NoError(t, err)

				expectedValues[i] = test_utils.NewExpectedWrapperValue(test_utils.ExpectedArrayValue(expectedChildArrayValues))
			} else {
				err = array.Append(childArray)
				require.NoError(t, err)

				expectedValues[i] = test_utils.ExpectedArrayValue(expectedChildArrayValues)
			}

			childSlabID = childArray.SlabID()
		} else {
			v := test_utils.NewStringValue(strings.Repeat(string(r), 20))
			r++

			if useWrapperValue {
				err = array.Append(test_utils.NewSomeValue(v))
				require.NoError(t, err)

				expectedValues[i] = test_utils.NewExpectedWrapperValue(v)
			} else {
				err = array.Append(v)
				require.NoError(t, err)

				expectedValues[i] = v
			}
		}
	}

	return array, expectedValues, childSlabID
}

func testArrayLoadedElements(t *testing.T, array *atree.Array, expectedValues []atree.Value) {
	i := 0
	err := array.IterateReadOnlyLoadedValues(func(v atree.Value) (bool, error) {
		require.True(t, i < len(expectedValues))
		testValueEqual(t, expectedValues[i], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expectedValues), i)
}

func getArrayMetaDataSlabCount(storage *atree.PersistentSlabStorage) int {
	var counter int
	deltas := atree.GetDeltas(storage)
	for _, slab := range deltas {
		if _, ok := slab.(*atree.ArrayMetaDataSlab); ok {
			counter++
		}
	}
	return counter
}

func TestArrayID(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	slabID := array.SlabID()
	valueID := array.ValueID()

	testEqualValueIDAndSlabID(t, slabID, valueID)
}

func TestSlabSizeWhenResettingMutableStorable(t *testing.T) {
	const (
		arrayCount          = 3
		initialStorableSize = 1
		mutatedStorableSize = 5
	)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	values := make([]*test_utils.MutableValue, arrayCount)
	for i := uint64(0); i < arrayCount; i++ {
		v := test_utils.NewMutableValue(initialStorableSize)
		values[i] = v

		err := array.Append(v)
		require.NoError(t, err)
	}

	require.True(t, IsArrayRootDataSlab(array))

	expectedArrayRootDataSlabSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(initialStorableSize, arrayCount)
	require.Equal(t, expectedArrayRootDataSlabSize, GetArrayRootSlabByteSize(array))

	err = atree.VerifyArray(array, address, typeInfo, test_utils.CompareTypeInfo, test_utils.GetHashInput, true)
	require.NoError(t, err)

	for i := uint64(0); i < arrayCount; i++ {
		mv := values[i]
		mv.UpdateStorableSize(mutatedStorableSize)

		existingStorable, err := array.Set(i, mv)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
	}

	require.True(t, IsArrayRootDataSlab(array))

	expectedArrayRootDataSlabSize = atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(mutatedStorableSize, arrayCount)
	require.Equal(t, expectedArrayRootDataSlabSize, GetArrayRootSlabByteSize(array))

	err = atree.VerifyArray(array, address, typeInfo, test_utils.CompareTypeInfo, test_utils.GetHashInput, true)
	require.NoError(t, err)
}

func TestChildArrayInlinabilityInParentArray(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("parent is root data slab, with one child array", func(t *testing.T) {
		const arrayCount = 1

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element.
		parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arrayCount)

		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())
		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.True(t, IsArrayRootDataSlab(parentArray))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 empty inlined child arrays
		childArraySize := emptyInlinedArrayByteSize
		expectedParentSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArraySize, arrayCount)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Get inlined child array
		e, err := parentArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		expectedChildValues, ok := expectedValues[0].(test_utils.ExpectedArrayValue)
		require.True(t, ok)

		childArray, ok := e.(*atree.Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
		require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

		v := test_utils.NewStringValue(strings.Repeat("a", 9))
		vSize := v.ByteSize()

		// Appending 10 elements to child array so that inlined child array reaches max inlined size as array element.
		for i := 0; i < 10; i++ {
			err = childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), childArray.Count())

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test parent slab size
			expectedParentSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedSize})
			require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

			// Test parent array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

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
		require.Equal(t, valueID, childArray.ValueID())       // atree.Value ID is unchanged

		expectedStandaloneSlabSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
		require.Equal(t, expectedStandaloneSlabSize, GetArrayRootSlabByteSize(childArray))

		expectedParentSize = atree.ComputeArrayRootDataSlabByteSize([]uint32{slabIDStorableByteSize})
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test parent array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove elements from child array which triggers standalone array slab becomes inlined slab again.
		for childArray.Count() > 0 {
			existingStorable, err := childArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, test_utils.NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues = expectedChildValues[1:]
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
			require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			expectedParentSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedSize})
			require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

			// Test parent array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, uint64(0), childArray.Count())
		require.Equal(t, uint64(arrayCount), parentArray.Count())
	})

	t.Run("parent is root data slab, with two child arrays", func(t *testing.T) {
		const arrayCount = 2

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element.
		parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arrayCount)

		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 2 empty inlined child arrays
		childArrayByteSize := emptyInlinedArrayByteSize
		storableByteSizes := make([]uint32, arrayCount)
		for i := 0; i < arrayCount; i++ {
			storableByteSizes[i] = childArrayByteSize
		}
		expectedParentSize := atree.ComputeArrayRootDataSlabByteSize(storableByteSizes)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test parent array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		children := make([]struct {
			array   *atree.Array
			valueID atree.ValueID
		}, arrayCount)

		for i := 0; i < arrayCount; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*atree.Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
			require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

			children[i].array = childArray
			children[i].valueID = valueID
		}

		v := test_utils.NewStringValue(strings.Repeat("a", 9))
		vSize := v.ByteSize()

		// Appending 10 elements to child array so that inlined child array reaches max inlined size as array element.
		for i := 0; i < 10; i++ {
			for j, child := range children {
				childArray := child.array
				childValueID := child.valueID

				err := childArray.Append(v)
				require.NoError(t, err)
				require.Equal(t, uint64(i+1), childArray.Count())

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedChildValues = append(expectedChildValues, v)
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))
				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

				// Test inlined child slab size
				expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
				require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

				// Test parent slab size
				storableByteSizes[j] = expectedInlinedSize
				require.Equal(t, atree.ComputeArrayRootDataSlabByteSize(storableByteSizes), GetArrayRootSlabByteSize(parentArray))

				// Test parent array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

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

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[i] = expectedChildValues

			expectedStoredDeltas++
			require.Equal(t, expectedStoredDeltas, getStoredDeltas(storage)) // There are more stored slab because child array is no longer inlined.

			expectedSlabID := valueIDToSlabID(childValueID)
			require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, childValueID, childArray.ValueID())  // atree.Value ID is unchanged

			expectedStandaloneSlabSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedStandaloneSlabSize, GetArrayRootSlabByteSize(childArray))

			storableByteSizes[i] = slabIDStorableByteSize
			require.Equal(t, atree.ComputeArrayRootDataSlabByteSize(storableByteSizes), GetArrayRootSlabByteSize(parentArray))

			// Test parent array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Remove one element from child array which triggers standalone array slab becomes inlined slab again.
		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			existingStorable, err := childArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, test_utils.NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = expectedChildValues[1:]
			expectedValues[i] = expectedChildValues

			require.True(t, childArray.Inlined())

			expectedStoredDeltas--
			require.Equal(t, expectedStoredDeltas, getStoredDeltas(storage))

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
			require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			storableByteSizes[i] = expectedInlinedSize
			require.Equal(t, atree.ComputeArrayRootDataSlabByteSize(storableByteSizes), GetArrayRootSlabByteSize(parentArray))

			// Test parent array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

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
				require.Equal(t, test_utils.NewStringValue(strings.Repeat("a", 9)), existingStorable)

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedChildValues = expectedChildValues[1:]
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))
				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
				require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

				expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
				require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

				storableByteSizes[j] = expectedInlinedSize
				require.Equal(t, atree.ComputeArrayRootDataSlabByteSize(storableByteSizes), GetArrayRootSlabByteSize(parentArray))

				// Test parent array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		for _, child := range children {
			require.Equal(t, uint64(0), child.array.Count())
		}
		require.Equal(t, uint64(arrayCount), parentArray.Count())
	})

	t.Run("parent is root metadata slab, with four child arrays", func(t *testing.T) {
		const arrayCount = 4

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element.
		parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arrayCount)

		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.True(t, IsArrayRootDataSlab(parentArray))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 4 empty inlined child arrays
		childArrayByteSize := emptyInlinedArrayByteSize
		expectedParentSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArrayByteSize, arrayCount)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test parent array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		children := make([]struct {
			array   *atree.Array
			valueID atree.ValueID
		}, arrayCount)

		for i := 0; i < arrayCount; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*atree.Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
			require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

			children[i].array = childArray
			children[i].valueID = valueID
		}

		v := test_utils.NewStringValue(strings.Repeat("a", 9))
		vSize := v.ByteSize()

		// Appending 10 elements to child array so that inlined child array reaches max inlined size as array element.
		for i := 0; i < 10; i++ {
			for j, child := range children {
				childArray := child.array
				childValueID := child.valueID

				err := childArray.Append(v)
				require.NoError(t, err)
				require.Equal(t, uint64(i+1), childArray.Count())

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedChildValues = append(expectedChildValues, v)
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

				// Test inlined child slab size
				expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
				require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

				// Test parent array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		// Parent array has 1 meta data slab and 2 data slabs.
		// All child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, IsArrayRootDataSlab(parentArray))

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			err := childArray.Append(v)
			require.NoError(t, err)
			require.False(t, childArray.Inlined())

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[i] = expectedChildValues

			expectedSlabID := valueIDToSlabID(childValueID)
			require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, childValueID, childArray.ValueID())  // atree.Value ID is unchanged

			expectedStandaloneSlabSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedStandaloneSlabSize, GetArrayRootSlabByteSize(childArray))

			// Test parent array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has one data slab and all child arrays are not inlined.
		require.Equal(t, 1+arrayCount, getStoredDeltas(storage))
		require.True(t, IsArrayRootDataSlab(parentArray))

		// Remove one element from child array which triggers standalone array slab becomes inlined slab again.
		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			existingStorable, err := childArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, test_utils.NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = expectedChildValues[1:]
			expectedValues[i] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
			require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test parent array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has 1 meta data slab and 2 data slabs.
		// All child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, IsArrayRootDataSlab(parentArray))

		// Remove remaining elements from inlined child array
		childArrayCount := children[0].array.Count()
		for i := 0; i < int(childArrayCount); i++ {
			for j, child := range children {
				childArray := child.array
				childValueID := child.valueID

				existingStorable, err := childArray.Remove(0)
				require.NoError(t, err)
				require.Equal(t, test_utils.NewStringValue(strings.Repeat("a", 9)), existingStorable)

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedChildValues = expectedChildValues[1:]
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
				require.Equal(t, childValueID, childArray.ValueID()) // value ID is unchanged

				expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
				require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

				// Test parent array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		// Parent array has 1 data slab.
		// All child arrays are inlined.
		require.Equal(t, 1, getStoredDeltas(storage))
		require.True(t, IsArrayRootDataSlab(parentArray))

		for _, child := range children {
			require.Equal(t, uint64(0), child.array.Count())
		}
		require.Equal(t, uint64(arrayCount), parentArray.Count())
	})
}

func TestNestedThreeLevelChildArrayInlinabilityInParentArray(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("parent is root data slab, one child array, one grand child array, changes to grand child array triggers child array slab to become standalone slab", func(t *testing.T) {
		const arrayCount = 1

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element, which has empty child array.
		parentArray, expectedValues := createArrayWithEmpty2LevelChildArray(t, storage, address, typeInfo, arrayCount)

		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.True(t, IsArrayRootDataSlab(parentArray))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 inlined child array
		gchildArrayByteSize := emptyInlinedArrayByteSize
		childArrayByteSize := atree.ComputeInlinedArraySlabByteSize([]uint32{gchildArrayByteSize})
		expectedParentSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArrayByteSize, arrayCount)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test parent array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Get inlined child array
		e, err := parentArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		childArray, ok := e.(*atree.Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
		require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

		// Get inlined grand child array
		e, err = childArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		gchildArray, ok := e.(*atree.Array)
		require.True(t, ok)
		require.True(t, gchildArray.Inlined())
		require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())

		gValueID := gchildArray.ValueID()
		require.Equal(t, address[:], gValueID[:atree.SlabAddressLength])
		require.NotEqual(t, atree.SlabIndexUndefined[:], gValueID[atree.SlabAddressLength:])
		require.NotEqual(t, valueID[atree.SlabAddressLength:], gValueID[atree.SlabAddressLength:])

		v := test_utils.NewStringValue(strings.Repeat("a", 9))
		vSize := v.ByteSize()

		// Appending 8 elements to grand child array so that inlined grand child array reaches max inlined size as array element.
		for i := 0; i < 8; i++ {
			err = gchildArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), gchildArray.Count())
			require.Equal(t, uint64(1), childArray.Count())

			expectedChildValues, ok := expectedValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues = append(expectedGChildValues, v)
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

			// Test inlined grand child slab size

			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			// Test inlined child slab size
			expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
			require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

			// Test parent slab size
			expectedParentSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedChildSize})
			require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Add one more element to grand child array which triggers inlined child array slab (NOT grand child array slab) becomes standalone slab
		err = gchildArray.Append(v)
		require.NoError(t, err)

		expectedChildValues, ok := expectedValues[0].(test_utils.ExpectedArrayValue)
		require.True(t, ok)

		expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
		require.True(t, ok)

		expectedGChildValues = append(expectedGChildValues, v)
		expectedChildValues[0] = expectedGChildValues
		expectedValues[0] = expectedChildValues

		require.True(t, gchildArray.Inlined())
		require.False(t, childArray.Inlined())
		require.Equal(t, 2, getStoredDeltas(storage)) // There are 2 stored slab because child array is no longer inlined.

		expectedSlabID := valueIDToSlabID(valueID)
		require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
		require.Equal(t, valueID, childArray.ValueID())       // atree.Value ID is unchanged

		require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
		require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

		// Test inlined grand child slab size
		expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
		require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

		expectedStandaloneSlabSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedGrandChildSize})
		require.Equal(t, expectedStandaloneSlabSize, GetArrayRootSlabByteSize(childArray))

		expectedParentSize = atree.ComputeArrayRootDataSlabByteSize([]uint32{slabIDStorableByteSize})
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove elements from grand child array which triggers standalone child array slab becomes inlined slab again.
		for gchildArray.Count() > 0 {
			existingStorable, err := gchildArray.Remove(0)
			require.NoError(t, err)
			require.Equal(t, test_utils.NewStringValue(strings.Repeat("a", 9)), existingStorable)

			expectedChildValues, ok := expectedValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues = expectedGChildValues[1:]
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
			require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())
			require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			// Test inlined child slab size
			expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
			require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

			// Test parent slab size
			expectedParentSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedChildSize})
			require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, uint64(0), gchildArray.Count())
		require.Equal(t, uint64(1), childArray.Count())
		require.Equal(t, uint64(arrayCount), parentArray.Count())
	})

	t.Run("parent is root data slab, one child array, one grand child array, changes to grand child array triggers grand child array slab to become standalone slab", func(t *testing.T) {
		const arrayCount = 1

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		// Create an array with empty child array as element, which has empty child array.
		parentArray, expectedValues := createArrayWithEmpty2LevelChildArray(t, storage, address, typeInfo, arrayCount)

		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.True(t, IsArrayRootDataSlab(parentArray))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 inlined child array
		gchildArrayByteSize := emptyInlinedArrayByteSize
		childArrayByteSize := atree.ComputeInlinedArraySlabByteSize([]uint32{gchildArrayByteSize})
		expectedParentSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArrayByteSize, arrayCount)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test parent array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Get inlined child array
		e, err := parentArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		childArray, ok := e.(*atree.Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
		require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

		// Get inlined grand child array
		e, err = childArray.Get(0)
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		gchildArray, ok := e.(*atree.Array)
		require.True(t, ok)
		require.True(t, gchildArray.Inlined())
		require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())

		gValueID := gchildArray.ValueID()
		require.Equal(t, address[:], gValueID[:atree.SlabAddressLength])
		require.NotEqual(t, atree.SlabIndexUndefined[:], gValueID[atree.SlabAddressLength:])
		require.NotEqual(t, valueID[atree.SlabAddressLength:], gValueID[atree.SlabAddressLength:])

		v := test_utils.NewStringValue(strings.Repeat("a", 9))
		vSize := v.ByteSize()

		// Appending 8 elements to grand child array so that inlined grand child array reaches max inlined size as array element.
		for i := 0; i < 8; i++ {
			err = gchildArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), gchildArray.Count())
			require.Equal(t, uint64(1), childArray.Count())

			expectedChildValues, ok := expectedValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues = append(expectedGChildValues, v)
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			// Test inlined child slab size
			expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
			require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

			// Test parent slab size
			expectedParentSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedChildSize})
			require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Add one more element to grand child array which triggers inlined grand child array slab (NOT child array slab) becomes standalone slab
		largeValue := test_utils.NewStringValue(strings.Repeat("b", 20))
		largeValueSize := largeValue.ByteSize()
		err = gchildArray.Append(largeValue)
		require.NoError(t, err)

		expectedChildValues, ok := expectedValues[0].(test_utils.ExpectedArrayValue)
		require.True(t, ok)

		expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
		require.True(t, ok)

		expectedGChildValues = append(expectedGChildValues, largeValue)
		expectedChildValues[0] = expectedGChildValues
		expectedValues[0] = expectedChildValues

		require.False(t, gchildArray.Inlined())
		require.True(t, childArray.Inlined())
		require.Equal(t, 2, getStoredDeltas(storage)) // There are 2 stored slab because child array is no longer inlined.

		require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
		require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

		expectedSlabID := valueIDToSlabID(gValueID)
		require.Equal(t, expectedSlabID, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
		require.Equal(t, gValueID, gchildArray.ValueID())      // atree.Value ID is unchanged

		// Test inlined grand child slab size
		storableByteSizes := make([]uint32, int(gchildArray.Count()))
		storableByteSizes[0] = largeValueSize
		for i := 1; i < int(gchildArray.Count()); i++ {
			storableByteSizes[i] = vSize
		}
		expectedInlinedGrandChildSize := atree.ComputeArrayRootDataSlabByteSize(storableByteSizes)
		require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

		expectedStandaloneSlabSize := atree.ComputeInlinedArraySlabByteSize([]uint32{slabIDStorableByteSize})
		require.Equal(t, expectedStandaloneSlabSize, GetArrayRootSlabByteSize(childArray))

		expectedParentSize = atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedStandaloneSlabSize})
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove elements from grand child array which triggers standalone child array slab becomes inlined slab again.
		for gchildArray.Count() > 0 {
			_, err := gchildArray.Remove(gchildArray.Count() - 1)
			require.NoError(t, err)

			expectedChildValues, ok := expectedValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues = expectedGChildValues[:len(expectedGChildValues)-1]
			expectedChildValues[0] = expectedGChildValues
			expectedValues[0] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, gchildArray.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
			require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())
			require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			// Test inlined child slab size
			expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
			require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

			// Test parent slab size
			expectedParentSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedChildSize})
			require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, uint64(0), gchildArray.Count())
		require.Equal(t, uint64(1), childArray.Count())
		require.Equal(t, uint64(arrayCount), parentArray.Count())
	})

	t.Run("parent is root data slab, two child array, one grand child array each, changes to child array triggers child array slab to become standalone slab", func(t *testing.T) {
		const arrayCount = 2

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		v := test_utils.NewStringValue(strings.Repeat("a", 9))
		vSize := v.ByteSize()

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := 0; i < arrayCount; i++ {
			// Create child array
			child, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			// Create grand child array
			gchild, err := atree.NewArray(storage, address, typeInfo)
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

			expectedValues[i] = test_utils.ExpectedArrayValue{test_utils.ExpectedArrayValue{v}}
		}

		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.True(t, IsArrayRootDataSlab(parentArray))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		// Test parent slab size with 1 inlined child array
		gchildArraySize := atree.ComputeInlinedArraySlabByteSize([]uint32{vSize})
		childArraySize := atree.ComputeInlinedArraySlabByteSize([]uint32{gchildArraySize})
		expectedParentSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArraySize, arrayCount)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test parent array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		type arrayInfo struct {
			array   *atree.Array
			valueID atree.ValueID
			child   *arrayInfo
		}

		children := make([]arrayInfo, arrayCount)

		for i := 0; i < arrayCount; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*atree.Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
			require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

			e, err = childArray.Get(0)
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			gchildArray, ok := e.(*atree.Array)
			require.True(t, ok)
			require.True(t, gchildArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())

			gValueID := gchildArray.ValueID()
			require.Equal(t, address[:], gValueID[:atree.SlabAddressLength])
			require.NotEqual(t, atree.SlabIndexUndefined[:], gValueID[atree.SlabAddressLength:])
			require.NotEqual(t, valueID[atree.SlabAddressLength:], gValueID[atree.SlabAddressLength:])

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

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedChildValues = append(expectedChildValues, v)

				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.True(t, gchildArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))

				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

				require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

				// Test inlined grand child slab size (1 element, unchanged)
				expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{vSize})
				require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

				// Test inlined child slab size
				gchildArraySize := atree.ComputeInlinedArraySlabByteSize([]uint32{vSize})
				childStorableByteSizes := make([]uint32, int(childArray.Count()))
				childStorableByteSizes[0] = gchildArraySize
				for i := 1; i < int(childArray.Count()); i++ {
					childStorableByteSizes[i] = vSize
				}
				expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize(childStorableByteSizes)
				require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

				// Test parent slab size
				expectedParentSize += vSize
				require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

				// Test array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

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

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)

			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.False(t, childArray.Inlined())
			require.Equal(t, 2+i, getStoredDeltas(storage)) // There are >1 stored slab because child array is no longer inlined.

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())       // atree.Value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			childStorableByteSizes := make([]uint32, int(childArray.Count()))
			childStorableByteSizes[0] = expectedInlinedGrandChildSize
			for i := 1; i < int(childArray.Count()); i++ {
				childStorableByteSizes[i] = vSize
			}
			expectedStandaloneSlabSize := atree.ComputeArrayRootDataSlabByteSize(childStorableByteSizes)
			require.Equal(t, expectedStandaloneSlabSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because child array is no longer inlined.

		expectedParentSize = atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(slabIDStorableByteSize, 2)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Remove one elements from each child array to trigger child arrays being inlined again.

		storableByteSizes := make([]uint32, arrayCount)

		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			_, err = childArray.Remove(childArray.Count() - 1)
			require.NoError(t, err)

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = expectedChildValues[:len(expectedChildValues)-1]

			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, childArray.Inlined())
			require.Equal(t, 2-i, getStoredDeltas(storage))

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			// Test inlined child slab size
			childStorableByteSizes := make([]uint32, int(childArray.Count()))
			childStorableByteSizes[0] = expectedInlinedGrandChildSize
			for i := 1; i < int(childArray.Count()); i++ {
				childStorableByteSizes[i] = vSize
			}
			expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize(childStorableByteSizes)
			require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

			storableByteSizes[i] = expectedInlinedChildSize

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		expectedParentSize = atree.ComputeArrayRootDataSlabByteSize(storableByteSizes)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

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
				require.Equal(t, test_utils.NewStringValue(strings.Repeat("a", 9)), existingStorable)

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedChildValues = expectedChildValues[:len(expectedChildValues)-1]

				expectedValues[j] = expectedChildValues

				require.True(t, gchildArray.Inlined())
				require.True(t, gchildArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))

				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
				require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

				require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())
				require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
				require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

				// Test inlined child slab size
				childStorableByteSizes := make([]uint32, int(childArray.Count()))
				childStorableByteSizes[0] = expectedInlinedGrandChildSize
				for i := 1; i < int(childArray.Count()); i++ {
					childStorableByteSizes[i] = vSize
				}
				expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize(childStorableByteSizes)
				require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

				// Test parent slab size
				expectedParentSize -= vSize
				require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

				// Test array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		for _, child := range children {
			require.Equal(t, uint64(1), child.child.array.Count())
			require.Equal(t, uint64(1), child.array.Count())
		}
		require.Equal(t, uint64(arrayCount), parentArray.Count())
	})

	t.Run("parent is root metadata slab, with four child arrays, each child array has grand child arrays", func(t *testing.T) {
		const arrayCount = 4

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		v := test_utils.NewStringValue(strings.Repeat("a", 9))
		vSize := v.ByteSize()

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)
		for i := 0; i < arrayCount; i++ {
			// Create child array
			child, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			// Create grand child array
			gchild, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			// Append grand child array to child array
			err = child.Append(gchild)
			require.NoError(t, err)

			// Append child array to parent
			err = parentArray.Append(child)
			require.NoError(t, err)

			expectedValues[i] = test_utils.ExpectedArrayValue{test_utils.ExpectedArrayValue{}}
		}

		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.True(t, IsArrayRootDataSlab(parentArray))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

		gchildArraySize := emptyInlinedArrayByteSize
		childArraySize := atree.ComputeInlinedArraySlabByteSize([]uint32{gchildArraySize})
		expectedParentSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArraySize, arrayCount)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

		// Test parent array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		type arrayInfo struct {
			array   *atree.Array
			valueID atree.ValueID
			child   *arrayInfo
		}

		children := make([]arrayInfo, arrayCount)

		for i := 0; i < arrayCount; i++ {
			e, err := parentArray.Get(uint64(i))
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			childArray, ok := e.(*atree.Array)
			require.True(t, ok)
			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

			valueID := childArray.ValueID()
			require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
			require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

			e, err = childArray.Get(0)
			require.NoError(t, err)
			require.Equal(t, 1, getStoredDeltas(storage))

			gchildArray, ok := e.(*atree.Array)
			require.True(t, ok)
			require.True(t, gchildArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())

			gValueID := gchildArray.ValueID()
			require.Equal(t, address[:], gValueID[:atree.SlabAddressLength])
			require.NotEqual(t, atree.SlabIndexUndefined[:], gValueID[atree.SlabAddressLength:])
			require.NotEqual(t, valueID[atree.SlabAddressLength:], gValueID[atree.SlabAddressLength:])

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

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedGChildValues = append(expectedGChildValues, v)

				expectedChildValues[0] = expectedGChildValues
				expectedValues[j] = expectedChildValues

				require.True(t, childArray.Inlined())
				require.True(t, gchildArray.Inlined())
				require.Equal(t, 1, getStoredDeltas(storage))

				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

				require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

				// Test inlined grand child slab size
				expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
				require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

				// Test inlined child slab size
				expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
				require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

				// Test array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

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

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues = append(expectedGChildValues, v)

			expectedChildValues[0] = expectedGChildValues
			expectedValues[i] = expectedChildValues

			require.True(t, gchildArray.Inlined())
			require.True(t, childArray.Inlined())
			require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because parent root slab is metdata.

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			expectedInlinedChildSlabSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
			require.Equal(t, expectedInlinedChildSlabSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because child array is no longer inlined.
		require.False(t, IsArrayRootDataSlab(parentArray))

		// Add one more element to grand child array which triggers
		// - child arrays become standalone slab (grand child arrays are still inlined)
		// - parent array slab becomes data slab
		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
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
			require.Equal(t, valueID, childArray.ValueID())       // atree.Value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

			// Test standalone grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			expectedStandaloneChildSlabSize := atree.ComputeArrayRootDataSlabByteSize([]uint32{expectedInlinedGrandChildSize})
			require.Equal(t, expectedStandaloneChildSlabSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has one root data slab, 4 grand child array with standalone root data slab.
		require.Equal(t, 1+arrayCount, getStoredDeltas(storage))
		require.True(t, IsArrayRootDataSlab(parentArray))

		// Remove elements from grand child array to trigger child array inlined again.
		for i, child := range children {
			childArray := child.array
			valueID := child.valueID
			gchildArray := child.child.array
			gValueID := child.child.valueID

			expectedChildValues, ok := expectedValues[i].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
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

			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childArray.ValueID())              // atree.Value ID is unchanged

			require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildArray.ValueID())             // atree.Value ID is unchanged

			// Test inlined grand child slab size
			expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
			require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

			// Test inlined child slab size
			expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
			require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Parent array has 1 metadata slab, and two data slab, all child and grand child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, IsArrayRootDataSlab(parentArray))

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

				expectedChildValues, ok := expectedValues[j].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedGChildValues, ok := expectedChildValues[0].(test_utils.ExpectedArrayValue)
				require.True(t, ok)

				expectedGChildValues = expectedGChildValues[1:]

				expectedChildValues[0] = expectedGChildValues
				expectedValues[j] = expectedChildValues

				require.True(t, gchildArray.Inlined())
				require.True(t, gchildArray.Inlined())

				require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())
				require.Equal(t, valueID, childArray.ValueID()) // value ID is unchanged

				require.Equal(t, atree.SlabIDUndefined, gchildArray.SlabID())
				require.Equal(t, gValueID, gchildArray.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedInlinedGrandChildSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(gchildArray.Count()))
				require.Equal(t, expectedInlinedGrandChildSize, GetArrayRootSlabByteSize(gchildArray))

				// Test inlined child slab size
				expectedInlinedChildSize := atree.ComputeInlinedArraySlabByteSize([]uint32{expectedInlinedGrandChildSize})
				require.Equal(t, expectedInlinedChildSize, GetArrayRootSlabByteSize(childArray))

				// Test array's mutableElementIndex
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(gchildArray)) <= gchildArray.Count())
				require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

				testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
			}
		}

		for _, child := range children {
			require.Equal(t, uint64(0), child.child.array.Count())
			require.Equal(t, uint64(1), child.array.Count())
		}
		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.Equal(t, 1, getStoredDeltas(storage))

		gchildArraySize = emptyInlinedArrayByteSize
		childArraySize = atree.ComputeInlinedArraySlabByteSize([]uint32{gchildArraySize})
		expectedParentSize = atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArraySize, arrayCount)
		require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))
	})
}

func TestChildArrayWhenParentArrayIsModified(t *testing.T) {

	const arrayCount = 2

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	// Create an array with empty child array as element.
	parentArray, expectedValues := createArrayWithEmptyChildArray(t, storage, address, typeInfo, arrayCount)

	require.Equal(t, uint64(arrayCount), parentArray.Count())
	require.True(t, IsArrayRootDataSlab(parentArray))
	require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child array is inlined.

	// Test parent slab size with empty inlined child arrays
	childArraySize := emptyInlinedArrayByteSize
	expectedParentSize := atree.ComputeArrayRootDataSlabByteSizeWithFixSizedElement(childArraySize, arrayCount)
	require.Equal(t, expectedParentSize, GetArrayRootSlabByteSize(parentArray))

	// Test array's mutableElementIndex
	require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

	testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

	children := make([]*struct {
		array       *atree.Array
		valueID     atree.ValueID
		parentIndex int
	}, arrayCount)

	for i := 0; i < arrayCount; i++ {
		e, err := parentArray.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, 1, getStoredDeltas(storage))

		childArray, ok := e.(*atree.Array)
		require.True(t, ok)
		require.True(t, childArray.Inlined())
		require.Equal(t, atree.SlabIDUndefined, childArray.SlabID())

		valueID := childArray.ValueID()
		require.Equal(t, address[:], valueID[:atree.SlabAddressLength])
		require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])

		children[i] = &struct {
			array       *atree.Array
			valueID     atree.ValueID
			parentIndex int
		}{
			childArray, valueID, i,
		}
	}

	t.Run("insert elements in parent array", func(t *testing.T) {
		// insert value at index 0, so all child array indexes are moved by +1
		v := test_utils.Uint64Value(0)
		err := parentArray.Insert(0, v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, nil)
		copy(expectedValues[1:], expectedValues)
		expectedValues[0] = v

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := test_utils.Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(1), childArray.Count())

			child.parentIndex = i + 1

			expectedChildValues, ok := expectedValues[child.parentIndex].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// insert value at index 2, so only second child array index is moved by +1
		v = test_utils.Uint64Value(2)
		err = parentArray.Insert(2, v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, nil)
		copy(expectedValues[3:], expectedValues[2:])
		expectedValues[2] = v

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := test_utils.Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(2), childArray.Count())

			if i > 0 {
				child.parentIndex++
			}

			expectedChildValues, ok := expectedValues[child.parentIndex].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// insert value at index 4, so none of child array indexes are affected.
		v = test_utils.Uint64Value(4)
		err = parentArray.Insert(4, v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, nil)
		expectedValues[4] = v

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := test_utils.Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(3), childArray.Count())

			expectedChildValues, ok := expectedValues[child.parentIndex].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}
	})

	t.Run("remove elements from parent array", func(t *testing.T) {
		// remove value at index 0, so all child array indexes are moved by -1.
		existingStorable, err := parentArray.Remove(0)
		require.NoError(t, err)
		require.Equal(t, test_utils.Uint64Value(0), existingStorable)

		copy(expectedValues, expectedValues[1:])
		expectedValues[len(expectedValues)-1] = nil
		expectedValues = expectedValues[:len(expectedValues)-1]

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := test_utils.Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(4), childArray.Count())

			child.parentIndex--

			expectedChildValues, ok := expectedValues[child.parentIndex].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Remove value at index 1, so only second child array index is moved by -1
		existingStorable, err = parentArray.Remove(1)
		require.NoError(t, err)
		require.Equal(t, test_utils.Uint64Value(2), existingStorable)

		copy(expectedValues[1:], expectedValues[2:])
		expectedValues[len(expectedValues)-1] = nil
		expectedValues = expectedValues[:len(expectedValues)-1]

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := test_utils.Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(5), childArray.Count())

			if i > 0 {
				child.parentIndex--
			}

			expectedChildValues, ok := expectedValues[child.parentIndex].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}

		// Remove value at index 2 (last element), so none of child array indexes are affected.
		existingStorable, err = parentArray.Remove(2)
		require.NoError(t, err)
		require.Equal(t, test_utils.Uint64Value(4), existingStorable)

		expectedValues[len(expectedValues)-1] = nil
		expectedValues = expectedValues[:len(expectedValues)-1]

		for i, child := range children {
			childArray := child.array
			childValueID := child.valueID

			v := test_utils.Uint64Value(i)
			vSize := v.ByteSize()

			err := childArray.Append(v)
			require.NoError(t, err)
			require.Equal(t, uint64(6), childArray.Count())

			expectedChildValues, ok := expectedValues[child.parentIndex].(test_utils.ExpectedArrayValue)
			require.True(t, ok)

			expectedChildValues = append(expectedChildValues, v)
			expectedValues[child.parentIndex] = expectedChildValues

			require.True(t, childArray.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childArray.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, childValueID, childArray.ValueID())         // atree.Value ID is unchanged

			// Test inlined child slab size
			expectedInlinedSize := atree.ComputeInlinedArraySlabByteSizeWithFixSizedElement(vSize, int(childArray.Count()))
			require.Equal(t, expectedInlinedSize, GetArrayRootSlabByteSize(childArray))

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(childArray)) <= childArray.Count())
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

			testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
		}
	})
}

func createArrayWithEmptyChildArray(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	arrayCount int,
) (*atree.Array, []atree.Value) {

	// Create parent array
	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	expectedValues := make([]atree.Value, arrayCount)
	for i := 0; i < arrayCount; i++ {
		// Create child array
		child, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Append child array to parent
		err = array.Append(child)
		require.NoError(t, err)

		expectedValues[i] = test_utils.ExpectedArrayValue{}
	}

	return array, expectedValues
}

func createArrayWithEmpty2LevelChildArray(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	arrayCount int,
) (*atree.Array, []atree.Value) {

	// Create parent array
	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	expectedValues := make([]atree.Value, arrayCount)
	for i := 0; i < arrayCount; i++ {
		// Create child array
		child, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Create grand child array
		gchild, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Append grand child array to child array
		err = child.Append(gchild)
		require.NoError(t, err)

		// Append child array to parent
		err = array.Append(child)
		require.NoError(t, err)

		expectedValues[i] = test_utils.ExpectedArrayValue{test_utils.ExpectedArrayValue{}}
	}

	return array, expectedValues
}

func getStoredDeltas(storage *atree.PersistentSlabStorage) int {
	count := 0
	deltas := atree.GetDeltas(storage)
	for _, slab := range deltas {
		if slab != nil {
			count++
		}
	}
	return count
}

func TestArraySetReturnedValue(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			var expectedChildValues test_utils.ExpectedArrayValue
			for {
				v := test_utils.NewStringValue(strings.Repeat("a", 10))

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
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child array value
		for i := 0; i < arrayCount; i++ {
			existingStorable, err := parentArray.Set(uint64(i), test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)

			expectedValues[i] = test_utils.Uint64Value(0)

			// Test array's mutableElementIndex
			require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())
		}

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			// Insert one element to child array
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedValues = append(expectedValues, test_utils.ExpectedArrayValue{v})
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child array value
		for i := 0; i < arrayCount; i++ {
			existingStorable, err := parentArray.Set(uint64(i), test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			expectedValues[i] = test_utils.Uint64Value(0)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			j := 0
			for {
				k := test_utils.Uint64Value(j)
				v := test_utils.NewStringValue(strings.Repeat("a", 10))
				j++

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildValues[k] = v

				if !childMap.Inlined() {
					break
				}
			}
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child map value
		for i := 0; i < arrayCount; i++ {
			existingStorable, err := parentArray.Set(uint64(i), test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			expectedValues[i] = test_utils.Uint64Value(0)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite existing child map value
		for i := 0; i < arrayCount; i++ {
			existingStorable, err := parentArray.Set(uint64(i), test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			expectedValues[i] = test_utils.Uint64Value(0)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)
	})
}

func TestArrayRemoveReturnedValue(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			var expectedChildValues test_utils.ExpectedArrayValue
			for {
				v := test_utils.NewStringValue(strings.Repeat("a", 10))

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
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child array value
		for i := 0; i < arrayCount; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, atree.GetArrayMutableElementIndexCount(parentArray))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childArray)
			require.NoError(t, err)

			// Insert one element to child array
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedValues = append(expectedValues, test_utils.ExpectedArrayValue{v})
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child array value
		for i := 0; i < arrayCount; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, atree.GetArrayMutableElementIndexCount(parentArray))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			j := 0
			for {
				k := test_utils.Uint64Value(j)
				v := test_utils.NewStringValue(strings.Repeat("a", 10))
				j++

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildValues[k] = v

				if !childMap.Inlined() {
					break
				}
			}
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child map value
		for i := 0; i < arrayCount; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, atree.GetArrayMutableElementIndexCount(parentArray))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const arrayCount = 2

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		for i := 0; i < arrayCount; i++ {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			err = parentArray.Append(childMap)
			require.NoError(t, err)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedValues = append(expectedValues, expectedChildValues)

			// Insert into child map until child map is not inlined
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		// Test array's mutableElementIndex
		require.True(t, uint64(atree.GetArrayMutableElementIndexCount(parentArray)) <= parentArray.Count())

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child map value
		for i := 0; i < arrayCount; i++ {
			valueStorable, err := parentArray.Remove(uint64(0))
			require.NoError(t, err)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedValues[i], child)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		// Test array's mutableElementIndex
		require.Equal(t, 0, atree.GetArrayMutableElementIndexCount(parentArray))

		testEmptyArray(t, storage, typeInfo, address, parentArray)
	})
}

func TestArrayWithOutdatedCallback(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("overwritten child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		// Create child array
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Insert child array to parent array
		err = parentArray.Append(childArray)
		require.NoError(t, err)

		v := test_utils.NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, test_utils.ExpectedArrayValue{v})

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Overwrite child array value from parent
		valueStorable, err := parentArray.Set(0, test_utils.Uint64Value(0))
		require.NoError(t, err)

		id, ok := valueStorable.(atree.SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		testValueEqual(t, expectedValues[0], child)

		expectedValues[0] = test_utils.Uint64Value(0)

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.True(t, atree.ArrayHasParentUpdater(childArray))

		// modify overwritten child array
		err = childArray.Append(test_utils.Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.False(t, atree.ArrayHasParentUpdater(childArray))

		// No-op on parent
		testValueEqual(t, expectedValues, parentArray)
	})

	t.Run("removed child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent array
		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var expectedValues test_utils.ExpectedArrayValue

		// Create child array
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		// Insert child array to parent array
		err = parentArray.Append(childArray)
		require.NoError(t, err)

		v := test_utils.NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedValues = append(expectedValues, test_utils.ExpectedArrayValue{v})

		testArray(t, storage, typeInfo, address, parentArray, expectedValues, true)

		// Remove child array value from parent
		valueStorable, err := parentArray.Remove(0)
		require.NoError(t, err)

		id, ok := valueStorable.(atree.SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		testValueEqual(t, expectedValues[0], child)

		expectedValues = test_utils.ExpectedArrayValue{}

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.True(t, atree.ArrayHasParentUpdater(childArray))

		// modify removed child array
		err = childArray.Append(test_utils.Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.False(t, atree.ArrayHasParentUpdater(childArray))

		// No-op on parent
		testValueEqual(t, expectedValues, parentArray)
	})
}

func TestArraySetType(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	newTypeInfo := test_utils.NewSimpleTypeInfo(43)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a new array in memory
		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.True(t, IsArrayRootDataSlab(array))

		// Modify type info of new array
		err = array.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, array.Type())

		// Commit new array to storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingArraySetType(t, array.SlabID(), atree.GetBaseStorage(storage), newTypeInfo, array.Count())
	})

	t.Run("data slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		arrayCount := 10
		for i := 0; i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arrayCount), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.True(t, IsArrayRootDataSlab(array))

		err = array.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, array.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingArraySetType(t, array.SlabID(), atree.GetBaseStorage(storage), newTypeInfo, array.Count())
	})

	t.Run("metadata slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		arrayCount := 10_000
		for i := 0; i < arrayCount; i++ {
			v := test_utils.Uint64Value(i)
			err := array.Append(v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arrayCount), array.Count())
		require.Equal(t, typeInfo, array.Type())
		require.False(t, IsArrayRootDataSlab(array))

		err = array.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, array.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingArraySetType(t, array.SlabID(), atree.GetBaseStorage(storage), newTypeInfo, array.Count())
	})

	t.Run("inlined in parent container root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = parentArray.Append(childArray)
		require.NoError(t, err)

		require.Equal(t, uint64(1), parentArray.Count())
		require.Equal(t, typeInfo, parentArray.Type())
		require.True(t, IsArrayRootDataSlab(parentArray))
		require.False(t, parentArray.Inlined())

		require.Equal(t, uint64(0), childArray.Count())
		require.Equal(t, typeInfo, childArray.Type())
		require.True(t, IsArrayRootDataSlab(childArray))
		require.True(t, childArray.Inlined())

		err = childArray.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childArray.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedArraySetType(t, parentArray.SlabID(), 0, atree.GetBaseStorage(storage), newTypeInfo, childArray.Count())
	})

	t.Run("inlined in parent container non-root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		arrayCount := 10_000
		for i := 0; i < arrayCount-1; i++ {
			v := test_utils.Uint64Value(i)
			err := parentArray.Append(v)
			require.NoError(t, err)
		}

		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = parentArray.Append(childArray)
		require.NoError(t, err)

		require.Equal(t, uint64(arrayCount), parentArray.Count())
		require.Equal(t, typeInfo, parentArray.Type())
		require.False(t, IsArrayRootDataSlab(parentArray))
		require.False(t, parentArray.Inlined())

		require.Equal(t, uint64(0), childArray.Count())
		require.Equal(t, typeInfo, childArray.Type())
		require.True(t, IsArrayRootDataSlab(childArray))
		require.True(t, childArray.Inlined())

		err = childArray.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childArray.Type())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedArraySetType(t, parentArray.SlabID(), arrayCount-1, atree.GetBaseStorage(storage), newTypeInfo, childArray.Count())
	})
}

func testExistingArraySetType(
	t *testing.T,
	id atree.SlabID,
	baseStorage atree.BaseStorage,
	expectedTypeInfo test_utils.SimpleTypeInfo,
	expectedCount uint64,
) {
	newTypeInfo := test_utils.NewSimpleTypeInfo(expectedTypeInfo.Value() + 1)

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing array by ID
	array, err := atree.NewArrayWithRootID(storage, id)
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
	storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

	// Load existing array again from storage
	array2, err := atree.NewArrayWithRootID(storage2, id)
	require.NoError(t, err)
	require.Equal(t, expectedCount, array2.Count())
	require.Equal(t, newTypeInfo, array2.Type())
}

func testExistingInlinedArraySetType(
	t *testing.T,
	parentID atree.SlabID,
	inlinedChildIndex int,
	baseStorage atree.BaseStorage,
	expectedTypeInfo test_utils.SimpleTypeInfo,
	expectedCount uint64,
) {
	newTypeInfo := test_utils.NewSimpleTypeInfo(expectedTypeInfo.Value() + 1)

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing array by ID
	parentArray, err := atree.NewArrayWithRootID(storage, parentID)
	require.NoError(t, err)

	element, err := parentArray.Get(uint64(inlinedChildIndex))
	require.NoError(t, err)

	childArray, ok := element.(*atree.Array)
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
	storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

	// Load existing array again from storage
	parentArray2, err := atree.NewArrayWithRootID(storage2, parentID)
	require.NoError(t, err)

	element2, err := parentArray2.Get(uint64(inlinedChildIndex))
	require.NoError(t, err)

	childArray2, ok := element2.(*atree.Array)
	require.True(t, ok)

	require.Equal(t, expectedCount, childArray2.Count())
	require.Equal(t, newTypeInfo, childArray2.Type())
}
