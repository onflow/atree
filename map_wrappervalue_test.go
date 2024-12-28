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

func TestMapWrapperValue(t *testing.T) {

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	// createMapWithSomeValue creates a map in the format of {uint64: SomeValue(string)}
	createMapWithSomeValue := func(
		storage SlabStorage,
		address Address,
		typeInfo TypeInfo,
		mapSize int,
	) (*OrderedMap, map[Value]Value) {
		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value)

		char := 'a'
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := NewStringValue(string(char))

			existingStorable, err := m.Set(compare, hashInputProvider, k, SomeValue{v})
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = someValue{v}
			char++
		}

		return m, keyValues
	}

	// createNestedMapWithSomeValue creates a map in the format of {uint64: SomeValue({uint64: SomeValue(string)})}
	createNestedMapWithSomeValue := func(
		storage SlabStorage,
		address Address,
		typeInfo TypeInfo,
		mapSize int,
		childMapSize int,
	) (*OrderedMap, map[Value]Value) {
		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			childMap, expectedChildKeyValues := createMapWithSomeValue(storage, address, typeInfo, childMapSize)

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, SomeValue{childMap})
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = someValue{mapValue(expectedChildKeyValues)}
		}

		return m, keyValues
	}

	t.Run("get and modify wrapper map in {key: SomeValue({key: SomeValue(value)})}", func(t *testing.T) {
		const (
			mapSize      = 3
			childMapSize = 2
		)

		typeInfo := testTypeInfo{42}

		createStorageWithSomeValue := func(mapSize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedKeyValues map[Value]Value,
		) {
			storage := newTestPersistentStorage(t)

			m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			keyValues := make(map[Value]Value)
			for i := 0; i < mapSize; i++ {

				childMap, childKeyValues := createMapWithSomeValue(storage, address, typeInfo, childMapSize)

				k := Uint64Value(uint64(i))
				v := childMap

				existingStorable, err := m.Set(compare, hashInputProvider, k, SomeValue{v})
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				keyValues[k] = someValue{mapValue(childKeyValues)}
			}

			testMap(t, storage, typeInfo, address, m, keyValues, nil, true)

			err = storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, m.SlabID(), keyValues
		}

		// Create a base storage with map in the format of
		// {uint64: SomeValue({uint64: SomeValue(string)})}
		baseStorage, rootSlabID, expectedKeyValues := createStorageWithSomeValue(mapSize)
		require.Equal(t, mapSize, len(expectedKeyValues))

		keys := make([]Value, 0, len(expectedKeyValues))
		for k := range expectedKeyValues {
			keys = append(keys, k)
		}

		// Create a new storage with encoded map
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing map from storage
		m, err := NewMapWithRootID(storage, rootSlabID, newBasicDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m.Count())

		// Get and verify first element as SomeValue(map)

		key := keys[0]
		expectedValues := expectedKeyValues[key]

		// Get map element (SomeValue)
		element, err := m.Get(compare, hashInputProvider, key)
		require.NoError(t, err)

		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildMap, isOrderedMap := elementAsSomeValue.Value.(*OrderedMap)
		require.True(t, isOrderedMap)

		expectedValuesAsSomeValue, isSomeValue := expectedValues.(someValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildMap, isMapValue := expectedValuesAsSomeValue.Value.(mapValue)
		require.True(t, isMapValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildMap)), unwrappedChildMap.Count())

		// Modify wrapped child map of SomeValue

		newKey := NewStringValue("x")
		newValue := NewStringValue("y")
		existingStorable, err := unwrappedChildMap.Set(compare, hashInputProvider, newKey, SomeValue{newValue})
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedChildMapValues := expectedKeyValues[key].(someValue).Value.(mapValue)
		expectedChildMapValues[newKey] = someValue{newValue}

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child map of SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

		// Load existing map from storage
		m2, err := NewMapWithRootID(storage2, rootSlabID, newBasicDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m2.Count())

		testMap(t, storage, typeInfo, address, m2, expectedKeyValues, nil, true)
	})

	t.Run("get and modify 2-level wrapper map in {key: SomeValue({key: SomeValue({key: SomeValue(value)})})}", func(t *testing.T) {
		const (
			mapSize       = 4
			childMapSize  = 3
			gchildMapSize = 2
		)

		typeInfo := testTypeInfo{42}

		createStorageWithNestedSomeValue := func(mapSize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedKeyValues map[Value]Value,
		) {
			storage := newTestPersistentStorage(t)

			m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			keyValues := make(map[Value]Value)
			for i := 0; i < mapSize; i++ {

				childMap, childKeyValues := createNestedMapWithSomeValue(storage, address, typeInfo, childMapSize, gchildMapSize)

				k := Uint64Value(uint64(i))
				v := childMap

				existingStorable, err := m.Set(compare, hashInputProvider, k, SomeValue{v})
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				keyValues[k] = someValue{mapValue(childKeyValues)}
			}

			testMap(t, storage, typeInfo, address, m, keyValues, nil, true)

			err = storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, m.SlabID(), keyValues
		}

		// Create a base storage with map in the format of
		// {uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(string)})})}
		baseStorage, rootSlabID, expectedKeyValues := createStorageWithNestedSomeValue(mapSize)
		require.Equal(t, mapSize, len(expectedKeyValues))

		keys := make([]Value, 0, len(expectedKeyValues))
		for k := range expectedKeyValues {
			keys = append(keys, k)
		}

		// Create a new storage with encoded map
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing map from storage
		m, err := NewMapWithRootID(storage, rootSlabID, newBasicDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m.Count())

		// Get and verify first element as SomeValue(map)

		key := keys[0]
		expectedValues := expectedKeyValues[key]

		// Get map element (SomeValue)
		element, err := m.Get(compare, hashInputProvider, key)
		require.NoError(t, err)

		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildMap, isOrderedMap := elementAsSomeValue.Value.(*OrderedMap)
		require.True(t, isOrderedMap)

		expectedValuesAsSomeValue, isSomeValue := expectedValues.(someValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildMap, isMapValue := expectedValuesAsSomeValue.Value.(mapValue)
		require.True(t, isMapValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildMap)), unwrappedChildMap.Count())

		// Get and verify nested child element as SomeValue(map)

		childMapKeys := make([]Value, 0, len(expectedUnwrappedChildMap))
		for k := range expectedUnwrappedChildMap {
			childMapKeys = append(childMapKeys, k)
		}

		childMapKey := childMapKeys[0]

		childMapElement, err := unwrappedChildMap.Get(compare, hashInputProvider, childMapKey)
		require.NoError(t, err)

		childMapElementAsSomeValue, isSomeValue := childMapElement.(SomeValue)
		require.True(t, isSomeValue)

		unwrappedGChildMap, isOrderedMap := childMapElementAsSomeValue.Value.(*OrderedMap)
		require.True(t, isOrderedMap)

		expectedChildValuesAsSomeValue, isSomeValue := expectedUnwrappedChildMap[childMapKey].(someValue)
		require.True(t, isSomeValue)

		expectedUnwrappedGChildMap, isMapValue := expectedChildValuesAsSomeValue.Value.(mapValue)
		require.True(t, isMapValue)

		require.Equal(t, uint64(len(expectedUnwrappedGChildMap)), unwrappedGChildMap.Count())

		// Modify wrapped gchild map of SomeValue

		newKey := NewStringValue("x")
		newValue := NewStringValue("y")
		existingStorable, err := unwrappedGChildMap.Set(compare, hashInputProvider, newKey, SomeValue{newValue})
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedChildMapValues := expectedKeyValues[key].(someValue).Value.(mapValue)
		expectedGChildMapValues := expectedChildMapValues[childMapKey].(someValue).Value.(mapValue)
		expectedGChildMapValues[newKey] = someValue{newValue}

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child map of SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

		// Load existing map from storage
		m2, err := NewMapWithRootID(storage2, rootSlabID, newBasicDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m2.Count())

		testMap(t, storage, typeInfo, address, m2, expectedKeyValues, nil, true)
	})
}
