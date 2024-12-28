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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrayWrapperValue(t *testing.T) {

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	// createArrayWithSomeValue creates an array in the format of [SomeValue(uint64)]
	createArrayWithSomeValue := func(
		storage SlabStorage,
		address Address,
		typeInfo TypeInfo,
		arraySize int,
	) (*Array, []Value) {
		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var values []Value

		for i := 0; i < arraySize; i++ {
			v := Uint64Value(i)

			err := array.Append(SomeValue{v})
			require.NoError(t, err)

			values = append(values, someValue{v})
		}

		return array, values
	}

	// createNestedArrayWithSomeValue creates an array in the format of [SomeValue([SomeValue(uint64)])]
	createNestedArrayWithSomeValue := func(
		storage SlabStorage,
		address Address,
		typeInfo TypeInfo,
		arraySize int,
		childArraySize int,
	) (*Array, []Value) {
		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		var values []Value

		for i := 0; i < arraySize; i++ {
			childArray, expectedChildValues := createArrayWithSomeValue(storage, address, typeInfo, childArraySize)

			err := array.Append(SomeValue{childArray})
			require.NoError(t, err)

			values = append(values, someValue{arrayValue(expectedChildValues)})
		}

		return array, values
	}

	t.Run("get and modify wrapper array in [SomeValue([SomeValue(value)])]", func(t *testing.T) {
		const (
			arraySize      = 3
			childArraySize = 2
		)

		typeInfo := testTypeInfo{42}

		createStorageWithSomeValue := func(arraySize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedValues []Value,
		) {
			storage := newTestPersistentStorage(t)

			array, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var values []Value
			for i := 0; i < arraySize; i++ {

				childArray, childValues := createArrayWithSomeValue(storage, address, typeInfo, childArraySize)

				err := array.Append(SomeValue{childArray})
				require.NoError(t, err)

				values = append(values, someValue{arrayValue(childValues)})
			}

			testArray(t, storage, typeInfo, address, array, values, false)

			err = storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, array.SlabID(), values
		}

		// Create a base storage with array in the format of
		// [SomeValue([SomeValue(uint64)])]
		baseStorage, rootSlabID, expectedValues := createStorageWithSomeValue(arraySize)
		require.Equal(t, arraySize, len(expectedValues))

		// Create a new storage with encoded array
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing array from storage
		array, err := NewArrayWithRootID(storage, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array.Count())

		// Get and verify first element as SomeValue(array)

		expectedValue := expectedValues[0]

		// Get array element (SomeValue)
		element, err := array.Get(uint64(0))
		require.NoError(t, err)

		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildArray, isArray := elementAsSomeValue.Value.(*Array)
		require.True(t, isArray)

		expectedValuesAsSomeValue, isSomeValue := expectedValue.(someValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildArray, isArrayValue := expectedValuesAsSomeValue.Value.(arrayValue)
		require.True(t, isArrayValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildArray)), unwrappedChildArray.Count())

		// Modify wrapped child array of SomeValue

		newValue := NewStringValue("x")
		err = unwrappedChildArray.Append(SomeValue{newValue})
		require.NoError(t, err)

		expectedUnwrappedChildArray = append(expectedUnwrappedChildArray, someValue{newValue})
		expectedValues[0] = someValue{expectedUnwrappedChildArray}

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child array of SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

		// Load existing array from storage
		array2, err := NewArrayWithRootID(storage2, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array2.Count())

		testArray(t, storage, typeInfo, address, array2, expectedValues, true)
	})

	t.Run("get and modify 2-level wrapper array in [SomeValue([SomeValue([SomeValue(uint64)])])]", func(t *testing.T) {
		const (
			arraySize       = 4
			childArraySize  = 3
			gchildArraySize = 2
		)

		typeInfo := testTypeInfo{42}

		createStorageWithNestedSomeValue := func(arraySize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedValues []Value,
		) {
			storage := newTestPersistentStorage(t)

			array, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			var values []Value
			for i := 0; i < arraySize; i++ {

				childArray, childValues := createNestedArrayWithSomeValue(storage, address, typeInfo, childArraySize, gchildArraySize)

				err := array.Append(SomeValue{childArray})
				require.NoError(t, err)

				values = append(values, someValue{arrayValue(childValues)})
			}

			testArray(t, storage, typeInfo, address, array, values, true)

			err = storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, array.SlabID(), values
		}

		// Create a base storage with array in the format of
		// [SomeValue([SomeValue([SomeValue(uint64)])])]
		baseStorage, rootSlabID, expectedValues := createStorageWithNestedSomeValue(arraySize)
		require.Equal(t, arraySize, len(expectedValues))

		// Create a new storage with encoded array
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing array from storage
		array, err := NewArrayWithRootID(storage, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array.Count())

		// Get and verify first element as SomeValue(array)

		expectedValue := expectedValues[0]

		// Get array element (SomeValue)
		element, err := array.Get(uint64(0))
		require.NoError(t, err)

		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildArray, isArray := elementAsSomeValue.Value.(*Array)
		require.True(t, isArray)

		expectedValuesAsSomeValue, isSomeValue := expectedValue.(someValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildArray, isArrayValue := expectedValuesAsSomeValue.Value.(arrayValue)
		require.True(t, isArrayValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildArray)), unwrappedChildArray.Count())

		// Get and verify nested child element as SomeValue(array)

		childArrayElement, err := unwrappedChildArray.Get(uint64(0))
		require.NoError(t, err)

		childArrayElementAsSomeValue, isSomeValue := childArrayElement.(SomeValue)
		require.True(t, isSomeValue)

		unwrappedGChildArray, isArray := childArrayElementAsSomeValue.Value.(*Array)
		require.True(t, isArray)

		expectedChildValuesAsSomeValue, isSomeValue := expectedUnwrappedChildArray[0].(someValue)
		require.True(t, isSomeValue)

		expectedUnwrappedGChildArray, isArrayValue := expectedChildValuesAsSomeValue.Value.(arrayValue)
		require.True(t, isArrayValue)

		require.Equal(t, uint64(len(expectedUnwrappedGChildArray)), unwrappedGChildArray.Count())

		// Modify wrapped gchild array of SomeValue

		newValue := NewStringValue("x")
		err = unwrappedGChildArray.Append(SomeValue{newValue})
		require.NoError(t, err)

		expectedUnwrappedGChildArray = append(expectedUnwrappedGChildArray, someValue{newValue})
		expectedValues[0].(someValue).Value.(arrayValue)[0] = someValue{expectedUnwrappedGChildArray}

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child array of SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

		// Load existing array from storage
		array2, err := NewArrayWithRootID(storage2, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array2.Count())

		testArray(t, storage, typeInfo, address, array2, expectedValues, true)
	})
}
