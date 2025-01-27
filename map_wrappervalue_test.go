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
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

type newKeyFunc func(SlabStorage) (key Value, expected Value)

var newRandomUint64KeyFunc = func(r *rand.Rand) newKeyFunc {
	return func(SlabStorage) (key Value, expected Value) {
		v := Uint64Value(r.Intn(1844674407370955161))
		return v, v
	}
}

var newUint64KeyFunc = func() newKeyFunc {
	i := 0
	return func(SlabStorage) (key Value, expected Value) {
		v := Uint64Value(i)
		i++
		return v, v
	}
}

var newMapValueFunc = func(
	t *testing.T,
	address Address,
	typeInfo TypeInfo,
	mapSize int,
	newKey newKeyFunc,
	newValue newValueFunc,
) newValueFunc {
	return func(storage SlabStorage) (value Value, expected Value) {
		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			k, expectedK := newKey(storage)
			v, expectedV := newValue(storage)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[expectedK] = expectedV
		}

		return m, mapValue(keyValues)
	}
}

var modifyMapValueFunc = func(
	t *testing.T,
	needToResetModifiedValue bool,
	modifyValueFunc modifyValueFunc,
) modifyValueFunc {
	return func(
		storage SlabStorage,
		originalValue Value,
		expectedOrigianlValue Value,
	) (
		modifiedValue Value,
		expectedModifiedValue Value,
		err error,
	) {
		m, ok := originalValue.(*OrderedMap)
		require.True(t, ok)

		expectedValues, ok := expectedOrigianlValue.(mapValue)
		require.True(t, ok)

		require.Equal(t, uint64(len(expectedValues)), m.Count())
		require.True(t, m.Count() > 0)

		// Modify first element

		var firstKey Value
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			firstKey = k
			return false, nil
		})
		require.NoError(t, err)

		v, err := m.Get(compare, hashInputProvider, firstKey)
		require.NoError(t, err)

		modifiedV, expectedModifiedV, err := modifyValueFunc(storage, v, expectedValues[firstKey])
		if err != nil {
			return nil, nil, err
		}

		if modifiedV == nil {

			existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, firstKey)
			if err != nil {
				return nil, nil, err
			}
			require.NotNil(t, existingKeyStorable)
			require.NotNil(t, existingValueStorable)

			// Verify wrapped storable doesn't contain inlined slab

			wrappedStorable := unwrapStorable(existingValueStorable)

			var removedSlabID SlabID

			switch wrappedStorable := wrappedStorable.(type) {
			case ArraySlab, MapSlab:
				require.Fail(t, "removed storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingValueStorable)

			case SlabIDStorable:
				removedSlabID = SlabID(wrappedStorable)

				// Verify SlabID has the same address
				require.Equal(t, m.Address(), removedSlabID.Address())
			}

			existingValue, err := existingValueStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, expectedValues[firstKey], existingValue)

			// Remove slabs from storage
			if removedSlabID != SlabIDUndefined {
				err = storage.Remove(removedSlabID)
				require.NoError(t, err)
			}

			delete(expectedValues, firstKey)

		} else {

			if needToResetModifiedValue {
				existingStorable, err := m.Set(compare, hashInputProvider, firstKey, modifiedV)
				if err != nil {
					return nil, nil, err
				}
				require.NotNil(t, existingStorable)

				// Verify wrapped storable doesn't contain inlined slab

				wrappedStorable := unwrapStorable(existingStorable)

				var overwrittenSlabID SlabID

				switch wrappedStorable := wrappedStorable.(type) {
				case ArraySlab, MapSlab:
					require.Fail(t, "overwritten storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingStorable)

				case SlabIDStorable:
					overwrittenSlabID = SlabID(wrappedStorable)

					// Verify SlabID has the same address
					require.Equal(t, m.Address(), overwrittenSlabID.Address())
				}

				existingValue, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)

				valueEqual(t, expectedValues[firstKey], existingValue)

				if overwrittenSlabID != SlabIDUndefined {
					// Remove slabs from storage given we are not interested in removed element
					err = storage.Remove(overwrittenSlabID)
					require.NoError(t, err)
				}

				expectedValues[firstKey] = expectedModifiedV
			}
		}

		return m, expectedValues, nil
	}
}

type mapWrapperValueTestCase struct {
	name                        string
	modifyName                  string
	wrapperValueNestedLevels    int
	mustSetModifiedElementInMap bool
	newKey                      newKeyFunc
	newValue                    newValueFunc
	modifyValue                 modifyValueFunc
}

func newMapWrapperValueTestCases(
	t *testing.T,
	r *rand.Rand,
	address Address,
	typeInfo TypeInfo,
) []mapWrapperValueTestCase {

	return []mapWrapperValueTestCase{

		// Test maps {uint64: SomeValue(uint64)}
		{
			name:                        "{uint64: SomeValue(uint64)}",
			modifyName:                  "modify wrapped primitive",
			wrapperValueNestedLevels:    1,
			mustSetModifiedElementInMap: true,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue:                    newWrapperValueFunc(1, newRandomUint64ValueFunc(r)),
			modifyValue:                 modifyWrapperValueFunc(t, 1, modifyRandomUint64ValueFunc(r)),
		},

		// Test maps {uint64: SomeValue(SomeValue(uint64))}
		{
			name:                        "{uint64: SomeValue(SomeValue(uint64))}",
			modifyName:                  "modify wrapped primitive",
			wrapperValueNestedLevels:    2,
			mustSetModifiedElementInMap: true,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue:                    newWrapperValueFunc(2, newRandomUint64ValueFunc(r)),
			modifyValue:                 modifyWrapperValueFunc(t, 2, modifyRandomUint64ValueFunc(r)),
		},

		// Test maps {uint64: SomeValue({uint64: uint64}))}
		{
			name:                        "{uint64: SomeValue({uint64: uint64})}",
			modifyName:                  "modify wrapped map",
			wrapperValueNestedLevels:    1,
			mustSetModifiedElementInMap: false,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue: newWrapperValueFunc(
				1,
				newMapValueFunc(
					t,
					address,
					typeInfo,
					2,
					newRandomUint64KeyFunc(r),
					newRandomUint64ValueFunc(r))),
			modifyValue: modifyWrapperValueFunc(
				t,
				1,
				modifyMapValueFunc(
					t,
					true,
					modifyRandomUint64ValueFunc(r))),
		},

		// Test maps {uint64: SomeValue(SomeValue({uint64: uint64})))}
		{
			name:                        "{uint64: SomeValue(SomeValue({uint64: uint64}))}",
			modifyName:                  "modify wrapped map",
			wrapperValueNestedLevels:    2,
			mustSetModifiedElementInMap: false,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue: newWrapperValueFunc(
				2,
				newMapValueFunc(
					t,
					address,
					typeInfo,
					2,
					newRandomUint64KeyFunc(r),
					newRandomUint64ValueFunc(r))),
			modifyValue: modifyWrapperValueFunc(
				t,
				2,
				modifyMapValueFunc(
					t,
					true,
					modifyRandomUint64ValueFunc(r))),
		},

		// Test maps {uint64: SomeValue({uint64: SomeValue(uint64)}))}
		{
			name:                        "{uint64: SomeValue({uint64: SomeValue(uint64)})}",
			modifyName:                  "modify wrapped map",
			wrapperValueNestedLevels:    1,
			mustSetModifiedElementInMap: false,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue: newWrapperValueFunc(
				1,
				newMapValueFunc(
					t,
					address,
					typeInfo,
					2,
					newRandomUint64KeyFunc(r),
					newWrapperValueFunc(1, newRandomUint64ValueFunc(r)))),
			modifyValue: modifyWrapperValueFunc(
				t,
				1,
				modifyMapValueFunc(
					t,
					true,
					modifyWrapperValueFunc(
						t,
						1,
						modifyRandomUint64ValueFunc(r)))),
		},

		// Test maps {uint64: SomeValue(SomeValue({uint64: SomeValue(SomeValue(uint64))})))}
		{
			name:                        "{uint64: SomeValue(SomeValue({uint64: SomeValue(SomeValue(uint64))}))}",
			modifyName:                  "modify wrapped map",
			wrapperValueNestedLevels:    2,
			mustSetModifiedElementInMap: false,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue: newWrapperValueFunc(
				2,
				newMapValueFunc(
					t,
					address,
					typeInfo,
					2,
					newRandomUint64KeyFunc(r),
					newWrapperValueFunc(
						2,
						newRandomUint64ValueFunc(r)))),
			modifyValue: modifyWrapperValueFunc(
				t,
				2,
				modifyMapValueFunc(
					t,
					true,
					modifyWrapperValueFunc(
						t,
						2,
						modifyRandomUint64ValueFunc(r)))),
		},

		// Test maps {uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})}))} and modify innermost map
		{
			name:                        "{uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})})}",
			modifyName:                  "modify wrapped level-2 map",
			wrapperValueNestedLevels:    1,
			mustSetModifiedElementInMap: false,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue: newWrapperValueFunc(
				1,
				newMapValueFunc(
					t,
					address,
					typeInfo,
					2,
					newRandomUint64KeyFunc(r),
					newWrapperValueFunc(
						1,
						newMapValueFunc(
							t,
							address,
							typeInfo,
							2,
							newRandomUint64KeyFunc(r),
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))),
			modifyValue: modifyWrapperValueFunc(
				t,
				1,
				modifyMapValueFunc(
					t,
					false,
					modifyWrapperValueFunc(
						t,
						1,
						modifyMapValueFunc(
							t,
							true,
							modifyWrapperValueFunc(
								t,
								1,
								modifyRandomUint64ValueFunc(r)))))),
		},

		// Test maps {uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})})) and remove element from middle map
		{
			name:                        "{uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})})}",
			modifyName:                  "remove element from wrapped level-1 map",
			wrapperValueNestedLevels:    1,
			mustSetModifiedElementInMap: false,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue: newWrapperValueFunc(
				1,
				newMapValueFunc(
					t,
					address,
					typeInfo,
					2,
					newRandomUint64KeyFunc(r),
					newWrapperValueFunc(
						1,
						newMapValueFunc(
							t,
							address,
							typeInfo,
							2,
							newRandomUint64KeyFunc(r),
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))),
			modifyValue: modifyWrapperValueFunc(
				t,
				1,
				modifyMapValueFunc(
					t,
					true,
					replaceWithNewValueFunc(nilValueFunc()))),
		},

		// Test maps {uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})})) and modify element from middle map
		{
			name:                        "{uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})})}",
			modifyName:                  "modify element in wrapped level-1 map",
			wrapperValueNestedLevels:    1,
			mustSetModifiedElementInMap: false,
			newKey:                      newRandomUint64KeyFunc(r),
			newValue: newWrapperValueFunc(
				1,
				newMapValueFunc(
					t,
					address,
					typeInfo,
					2,
					newRandomUint64KeyFunc(r),
					newWrapperValueFunc(
						1,
						newMapValueFunc(
							t,
							address,
							typeInfo,
							2,
							newRandomUint64KeyFunc(r),
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))),
			modifyValue: modifyWrapperValueFunc(
				t,
				1,
				modifyMapValueFunc(
					t,
					true,
					replaceWithNewValueFunc(
						newWrapperValueFunc(
							1,
							newMapValueFunc(
								t,
								address,
								typeInfo,
								2,
								newRandomUint64KeyFunc(r),
								newWrapperValueFunc(
									1,
									newRandomUint64ValueFunc(r))))))),
		},
	}
}

// TestMapWrapperValueSetAndModify tests
// - setting WrapperValue to map
// - retrieving WrapperValue from map
// - modifing retrieved WrapperValue
// - setting modified WrapperValue
func TestMapWrapperValueSetAndModify(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapSize = 10
		largeMapSize = 512
	)

	mapSizeTestCases := []struct {
		name    string
		mapSize int
	}{
		{name: "small map", mapSize: smallMapSize},
		{name: "large map", mapSize: largeMapSize},
	}

	testCases := newMapWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, mapSizeTestCase := range mapSizeTestCases {

			mapSize := mapSizeTestCase.mapSize

			name := mapSizeTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += "," + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
				require.NoError(t, err)

				rootSlabID := m.SlabID()

				// Set WrapperValue
				expectedValues := make(map[Value]Value)
				for len(expectedValues) < mapSize {
					k, expectedK := tc.newKey(storage)

					if _, exists := expectedValues[expectedK]; exists {
						continue
					}

					v, expectedV := tc.newValue(storage)

					existingStorable, err := m.Set(compare, hashInputProvider, k, v)
					require.NoError(t, err)
					require.Nil(t, existingStorable)

					expectedValues[expectedK] = expectedV
				}

				require.Equal(t, uint64(mapSize), m.Count())

				testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

				// Retrieve and modify WrapperValue from map
				for key, expectedValue := range expectedValues {
					v, err := m.Get(compare, hashInputProvider, key)
					require.NoError(t, err)

					valueEqual(t, expectedValue, v)

					// Verify that v is WrapperValue
					testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

					// Modify element
					newV, newExpectedV, err := tc.modifyValue(storage, v, expectedValue)
					require.NoError(t, err)

					if tc.mustSetModifiedElementInMap {
						testSetElementInMap(t, storage, m, key, newV, expectedValue)
					}

					expectedValues[key] = newExpectedV
				}

				require.Equal(t, uint64(mapSize), m.Count())

				testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load map from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

				m2, err := NewMapWithRootID(storage2, rootSlabID, newBasicDigesterBuilder())
				require.NoError(t, err)
				require.Equal(t, uint64(mapSize), m2.Count())

				// Test loaded map
				testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
			})
		}
	}
}

// TestMapWrapperValueSetAndRemove tests
// - inserting WrapperValue to map
// - remove all elements
func TestMapWrapperValueSetAndRemove(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapSize = 10
		largeMapSize = 512
	)

	mapSizeTestCases := []struct {
		name    string
		mapSize int
	}{
		{name: "small map", mapSize: smallMapSize},
		{name: "large map", mapSize: largeMapSize},
	}

	modifyTestCases := []struct {
		name                string
		needToModifyElement bool
	}{
		{name: "modify elements", needToModifyElement: true},
		{name: "", needToModifyElement: false},
	}

	removeSizeTestCases := []struct {
		name               string
		removeAllElements  bool
		removeElementCount int
	}{
		{name: "remove all elements", removeAllElements: true},
		{name: "remove 1 element", removeElementCount: 1},
		{name: fmt.Sprintf("remove %d element", smallMapSize/2), removeElementCount: smallMapSize / 2},
	}

	testCases := newMapWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, mapSizeTestCase := range mapSizeTestCases {

			for _, modifyTestCase := range modifyTestCases {

				for _, removeSizeTestCase := range removeSizeTestCases {

					mapSize := mapSizeTestCase.mapSize

					needToModifyElement := modifyTestCase.needToModifyElement

					removeSize := removeSizeTestCase.removeElementCount
					if removeSizeTestCase.removeAllElements {
						removeSize = mapSize
					}

					name := mapSizeTestCase.name + " " + tc.name
					if modifyTestCase.needToModifyElement {
						name += ", " + tc.modifyName
					}
					if removeSizeTestCase.name != "" {
						name += ", " + removeSizeTestCase.name
					}

					t.Run(name, func(t *testing.T) {

						storage := newTestPersistentStorage(t)

						m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
						require.NoError(t, err)

						rootSlabID := m.SlabID()

						expectedValues := make(map[Value]Value)

						// Set WrapperValue in map
						for len(expectedValues) < mapSize {
							k, expectedK := tc.newKey(storage)

							if _, exists := expectedValues[expectedK]; exists {
								continue
							}

							v, expectedV := tc.newValue(storage)

							existingStorable, err := m.Set(compare, hashInputProvider, k, v)
							require.NoError(t, err)
							require.Nil(t, existingStorable)

							expectedValues[expectedK] = expectedV
						}

						require.Equal(t, uint64(mapSize), m.Count())

						testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

						// Retrieve and modify WrapperValue from map
						if needToModifyElement {
							for key, expected := range expectedValues {
								v, err := m.Get(compare, hashInputProvider, key)
								require.NoError(t, err)

								valueEqual(t, expected, v)

								// Verify that v is WrapperValue
								testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

								// Modify element
								newV, newExpectedV, err := tc.modifyValue(storage, v, expected)
								require.NoError(t, err)

								if tc.mustSetModifiedElementInMap {
									testSetElementInMap(t, storage, m, key, newV, expected)
								}

								expectedValues[key] = newExpectedV
							}

							require.Equal(t, uint64(mapSize), m.Count())

							testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
						}

						keys := make([]Value, 0, len(expectedValues))
						for key := range expectedValues {
							keys = append(keys, key)
						}

						// Remove random elements
						for i := 0; i < removeSize; i++ {

							removeKeyIndex := r.Intn(len(keys))
							removeKey := keys[removeKeyIndex]

							testRemoveElementFromMap(t, storage, m, removeKey, expectedValues[removeKey])

							delete(expectedValues, removeKey)

							keys = append(keys[:removeKeyIndex], keys[removeKeyIndex+1:]...)
						}

						require.Equal(t, uint64(mapSize-removeSize), m.Count())
						require.Equal(t, mapSize-removeSize, len(expectedValues))

						testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

						// Commit storage
						err = storage.FastCommit(runtime.NumCPU())
						require.NoError(t, err)

						// Load map from encoded data
						storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

						m2, err := NewMapWithRootID(storage2, rootSlabID, newBasicDigesterBuilder())
						require.NoError(t, err)
						require.Equal(t, uint64(mapSize-removeSize), m2.Count())

						// Test loaded map
						testMap(t, storage2, typeInfo, address, m2, expectedValues, nil, true)
					})
				}
			}
		}
	}
}

func TestMapWrapperValueReadOnlyIterate(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapSize = 10
		largeMapSize = 512
	)

	mapSizeTestCases := []struct {
		name    string
		mapSize int
	}{
		{name: "small map", mapSize: smallMapSize},
		{name: "large map", mapSize: largeMapSize},
	}

	modifyTestCases := []struct {
		name              string
		testModifyElement bool
	}{
		{name: "modify elements", testModifyElement: true},
		{name: "", testModifyElement: false},
	}

	testCases := newMapWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, mapSizeTestCase := range mapSizeTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && tc.mustSetModifiedElementInMap {
					continue
				}

				mapSize := mapSizeTestCase.mapSize

				testModifyElement := modifyTestCase.testModifyElement

				name := mapSizeTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
					require.NoError(t, err)

					expectedValues := make(map[Value]Value)

					// Set WrapperValue to map
					for len(expectedValues) < mapSize {
						k, expectedK := tc.newKey(storage)

						if _, exists := expectedValues[expectedK]; exists {
							continue
						}

						v, expectedV := tc.newValue(storage)

						existingStorable, err := m.Set(compare, hashInputProvider, k, v)
						require.NoError(t, err)
						require.Nil(t, existingStorable)

						expectedValues[expectedK] = expectedV
					}

					require.Equal(t, uint64(mapSize), m.Count())

					testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

					iterator, err := m.ReadOnlyIterator()
					require.NoError(t, err)

					count := 0
					for {
						nextKey, nextValue, err := iterator.Next()
						require.NoError(t, err)

						if nextKey == nil {
							break
						}

						expected := expectedValues[nextKey]

						testWrapperValueLevels(t, tc.wrapperValueNestedLevels, nextValue)

						valueEqual(t, expected, nextValue)

						// Test modifying elements that don't need to reset in parent container.
						if testModifyElement {
							_, _, err := tc.modifyValue(storage, nextValue, expected)
							var targetErr *ReadOnlyIteratorElementMutationError
							require.ErrorAs(t, err, &targetErr)
						}

						count++
					}
				})
			}
		}
	}
}

func TestMapWrapperValueIterate(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapSize = 10
		largeMapSize = 512
	)

	mapSizeTestCases := []struct {
		name    string
		mapSize int
	}{
		{name: "small map", mapSize: smallMapSize},
		{name: "large map", mapSize: largeMapSize},
	}

	modifyTestCases := []struct {
		name              string
		testModifyElement bool
	}{
		{name: "modify elements", testModifyElement: true},
		{name: "", testModifyElement: false},
	}

	testCases := newMapWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, mapSizeTestCase := range mapSizeTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				elementIsContainer := !tc.mustSetModifiedElementInMap

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && !elementIsContainer {
					continue
				}

				mapSize := mapSizeTestCase.mapSize

				testModifyElement := modifyTestCase.testModifyElement

				name := mapSizeTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
					require.NoError(t, err)

					expectedValues := make(map[Value]Value)

					// Set WrapperValue in map
					for len(expectedValues) < mapSize {
						k, expectedK := tc.newKey(storage)

						if _, exists := expectedValues[expectedK]; exists {
							continue
						}

						v, expectedV := tc.newValue(storage)

						existingStorable, err := m.Set(compare, hashInputProvider, k, v)
						require.NoError(t, err)
						require.Nil(t, existingStorable)

						expectedValues[expectedK] = expectedV
					}

					require.Equal(t, uint64(mapSize), m.Count())

					testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

					iterator, err := m.Iterator(compare, hashInputProvider)
					require.NoError(t, err)

					count := 0
					for {
						nextKey, nextValue, err := iterator.Next()
						require.NoError(t, err)

						if nextKey == nil {
							break
						}

						expected := expectedValues[nextKey]

						testWrapperValueLevels(t, tc.wrapperValueNestedLevels, nextValue)

						valueEqual(t, expected, nextValue)

						// Test modifying container elements.
						if testModifyElement {
							_, newExpectedV, err := tc.modifyValue(storage, nextValue, expected)
							require.NoError(t, err)

							expectedValues[nextKey] = newExpectedV
						}

						count++
					}

					require.Equal(t, uint64(mapSize), m.Count())

					testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
				})
			}
		}
	}
}

func TestMapWrapperValueInlineMapAtLevel1(t *testing.T) {

	testLevel1WrappedChildMapInlined := func(t *testing.T, m *OrderedMap, expectedInlined bool) {
		rootDataSlab, isDataSlab := m.root.(*MapDataSlab)
		require.True(t, isDataSlab)

		elements, isHkeyElements := rootDataSlab.elements.(*hkeyElements)
		require.True(t, isHkeyElements)

		require.Equal(t, 1, len(elements.elems))

		element, isSingleElement := elements.elems[0].(*singleElement)
		require.True(t, isSingleElement)

		value := element.value

		storabeleAsSomeStorable, isSomeStorable := value.(SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorable := storabeleAsSomeStorable.Storable

		switch wrappedStorable := wrappedStorable.(type) {
		case SlabIDStorable:
			inlined := false
			require.Equal(t, expectedInlined, inlined)

		case ArraySlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		case MapSlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		default:
			require.Fail(t, "wrapped storable has unexpected type: %T", wrappedStorable)
		}
	}

	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	expectedValues := make(mapValue)

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// Set WrapperValue SomeValue([]) in map
	{
		// Create standalone child map
		childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		require.False(t, childMap.Inlined())

		// Set child map (level-1 inlined map) in parent map
		key := Uint64Value(0)

		existingStorable, err := m.Set(compare, hashInputProvider, key, SomeValue{childMap})
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, childMap.Inlined())

		expectedValues[key] = someValue{mapValue{}}

		require.Equal(t, uint64(1), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		testLevel1WrappedChildMapInlined(t, m, true)
	}

	// Retrieve wrapped child map, and then insert new elements to child map.
	// Wrapped child map is expected to be unlined at the end of loop.

	const childMapSize = 8
	for i := 0; i <= childMapSize; i++ {
		// Get element
		element, err := m.Get(compare, hashInputProvider, Uint64Value(0))
		require.NoError(t, err)
		require.NotNil(t, element)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedMap, isMap := wrappedValue.(*OrderedMap)
		require.True(t, isMap)

		expectedWrappedValue := expectedValues[Uint64Value(0)].(someValue).Value

		expectedWrappedMap := expectedWrappedValue.(mapValue)

		// Insert new elements to wrapped child map

		k := Uint64Value(i)
		v := Uint64Value(r.Intn(256))

		existingStorable, err := wrappedMap.Set(compare, hashInputProvider, k, SomeValue{v})
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedWrappedMap[k] = someValue{v}

		expectedValues[Uint64Value(0)] = someValue{expectedWrappedMap}

		require.Equal(t, uint64(i+1), wrappedMap.Count())
		require.Equal(t, i+1, len(expectedWrappedMap))

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel1WrappedChildMapInlined(t, m, false)

	// Retrieve wrapped child map, and then remove elements from child map.
	// Wrapped child map is expected to be inlined at the end of loop.

	childMapSizeAfterRemoval := 2
	removeCount := childMapSize - childMapSizeAfterRemoval

	for i := 0; i < removeCount; i++ {
		// Get element
		element, err := m.Get(compare, hashInputProvider, Uint64Value(0))
		require.NoError(t, err)
		require.NotNil(t, element)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedMap, isMap := wrappedValue.(*OrderedMap)
		require.True(t, isMap)

		expectedWrappedValue := expectedValues[Uint64Value(0)].(someValue).Value

		expectedWrappedMap := expectedWrappedValue.(mapValue)

		// Remove element from wrapped child map

		existingKeyStorable, existingValueStorable, err := wrappedMap.Remove(compare, hashInputProvider, Uint64Value(i))
		require.NoError(t, err)
		require.NotNil(t, existingKeyStorable)
		require.NotNil(t, existingValueStorable)

		// Verify removed key and value

		existingValue, err := existingValueStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, expectedWrappedMap[Uint64Value(i)], existingValue)

		delete(expectedWrappedMap, Uint64Value(i))

		expectedValues[Uint64Value(0)] = someValue{expectedWrappedMap}

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel1WrappedChildMapInlined(t, m, true)

	testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
}

func TestMapWrapperValueInlineMapAtLevel2(t *testing.T) {

	testLevel2WrappedChildMapInlined := func(t *testing.T, m *OrderedMap, expectedInlined bool) {
		rootDataSlab, isDataSlab := m.root.(*MapDataSlab)
		require.True(t, isDataSlab)

		elementsAtLevel1, isHkeyElements := rootDataSlab.elements.(*hkeyElements)
		require.True(t, isHkeyElements)

		require.Equal(t, 1, len(elementsAtLevel1.elems))

		elementAtLevel1, isSingleElement := elementsAtLevel1.elems[0].(*singleElement)
		require.True(t, isSingleElement)

		// Get unwrapped value at level 1

		storableAtLevel1 := elementAtLevel1.value

		storabeleAsSomeStoable, isSomeStorable := storableAtLevel1.(SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorableAtLevel1 := storabeleAsSomeStoable.Storable

		wrappedMapAtlevel1, isMap := wrappedStorableAtLevel1.(*MapDataSlab)
		require.True(t, isMap)

		// Get unwrapped value at level 2

		elementsAtLevel2, isHkeyElements := wrappedMapAtlevel1.elements.(*hkeyElements)
		require.True(t, isHkeyElements)

		elementAtLevel2, isSingleElement := elementsAtLevel2.elems[0].(*singleElement)
		require.True(t, isSingleElement)

		storableAtLevel2 := elementAtLevel2.value

		storabeleAsSomeStoable, isSomeStorable = storableAtLevel2.(SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorableAtLevel2 := storabeleAsSomeStoable.Storable

		switch wrappedStorable := wrappedStorableAtLevel2.(type) {
		case SlabIDStorable:
			inlined := false
			require.Equal(t, expectedInlined, inlined)

		case ArraySlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		case MapSlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		default:
			require.Fail(t, "wrapped storable has unexpected type: %T", wrappedStorable)
		}
	}

	SetThreshold(256)
	defer SetThreshold(1024)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	r := newRand(t)

	storage := newTestPersistentStorage(t)

	expectedValues := make(mapValue)

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// Set WrapperValue SomeValue({SomeValue{}}) to map
	{
		// Create grand child map
		gchildMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		require.False(t, gchildMap.Inlined())

		// Create child map
		childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		require.False(t, childMap.Inlined())

		// Set grand child map to child map
		existingStorable, err := childMap.Set(compare, hashInputProvider, Uint64Value(0), SomeValue{gchildMap})
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, gchildMap.Inlined())

		// Append child map to map
		existingStorable, err = m.Set(compare, hashInputProvider, Uint64Value(0), SomeValue{childMap})
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, childMap.Inlined())

		expectedValues[Uint64Value(0)] = someValue{mapValue{Uint64Value(0): someValue{mapValue{}}}}

		require.Equal(t, uint64(1), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		testLevel2WrappedChildMapInlined(t, m, true)
	}

	// Retrieve wrapped gchild map, and then insert new elements to gchild map.
	// Wrapped gchild map is expected to be unlined at the end of loop.

	const gchildMapSize = 8
	for i := 0; i < gchildMapSize; i++ {
		// Get element at level 1

		elementAtLevel1, err := m.Get(compare, hashInputProvider, Uint64Value(0))
		require.NoError(t, err)
		require.NotNil(t, elementAtLevel1)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedMapAtLevel1, isMap := wrappedValueAtLevel1.(*OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel1 := expectedValues[Uint64Value(0)].(someValue).Value

		expectedWrappedMapAtLevel1 := expectedWrappedValueAtLevel1.(mapValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedMapAtLevel1.Get(compare, hashInputProvider, Uint64Value(0))
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedMapAtLevel2, isMap := wrappedValueAtLevel2.(*OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel2 := expectedWrappedMapAtLevel1[Uint64Value(0)].(someValue).Value

		expectedWrappedMapAtLevel2 := expectedWrappedValueAtLevel2.(mapValue)

		// Insert new elements to wrapped gchild map

		k := Uint64Value(i)
		v := Uint64Value(r.Intn(256))

		existingStorable, err := wrappedMapAtLevel2.Set(compare, hashInputProvider, k, SomeValue{v})
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedWrappedMapAtLevel2[k] = someValue{v}

		expectedValues[Uint64Value(0)] = someValue{mapValue{Uint64Value(0): someValue{expectedWrappedMapAtLevel2}}}

		require.Equal(t, uint64(i+1), wrappedMapAtLevel2.Count())
		require.Equal(t, i+1, len(expectedWrappedMapAtLevel2))

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel2WrappedChildMapInlined(t, m, false)

	// Retrieve wrapped gchild map, and then remove elements from gchild map.
	// Wrapped gchild map is expected to be inlined at the end of loop.

	gchildMapSizeAfterRemoval := 2
	removeCount := gchildMapSize - gchildMapSizeAfterRemoval

	for i := 0; i < removeCount; i++ {
		// Get elementAtLevel1
		elementAtLevel1, err := m.Get(compare, hashInputProvider, Uint64Value(0))
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedMapAtLevel1, isMap := wrappedValueAtLevel1.(*OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel1 := expectedValues[Uint64Value(0)].(someValue).Value

		expectedWrappedMapAtLevel1 := expectedWrappedValueAtLevel1.(mapValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedMapAtLevel1.Get(compare, hashInputProvider, Uint64Value(0))
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedMapAtLevel2, isMap := wrappedValueAtLevel2.(*OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel2 := expectedWrappedMapAtLevel1[Uint64Value(0)].(someValue).Value

		expectedWrappedMapAtLevel2 := expectedWrappedValueAtLevel2.(mapValue)

		// Remove first element from wrapped gchild map

		existingKeyStorable, existingValueStorable, err := wrappedMapAtLevel2.Remove(compare, hashInputProvider, Uint64Value(i))
		require.NoError(t, err)
		require.NotNil(t, existingKeyStorable)
		require.NotNil(t, existingValueStorable)

		// Verify removed value

		existingValue, err := existingValueStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, expectedWrappedMapAtLevel2[Uint64Value(i)], existingValue)

		delete(expectedWrappedMapAtLevel2, Uint64Value(i))

		expectedValues[Uint64Value(0)] = someValue{mapValue{Uint64Value(0): someValue{expectedWrappedMapAtLevel2}}}

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel2WrappedChildMapInlined(t, m, true)

	testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
}

func TestMapWrapperValueModifyNewMapAtLevel1(t *testing.T) {

	const (
		minWriteOperationSize = 124
		maxWriteOperationSize = 256
	)

	r := newRand(t)

	SetThreshold(256)
	defer SetThreshold(1024)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	newElementFuncs := []newValueFunc{
		// SomeValue(uint64)
		newWrapperValueFunc(1, newRandomUint64ValueFunc(r)),

		// SomeValue({uint64: SomeValue(uint64)})
		newWrapperValueFunc(
			1,
			newMapValueFunc(
				t,
				address,
				typeInfo,
				r.Intn(4),
				newRandomUint64KeyFunc(r),
				newWrapperValueFunc(
					1,
					newRandomUint64ValueFunc(r)))),

		// SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})})
		newWrapperValueFunc(
			1,
			newMapValueFunc(
				t,
				address,
				typeInfo,
				r.Intn(4),
				newRandomUint64KeyFunc(r),
				newWrapperValueFunc(
					1,
					newMapValueFunc(
						t,
						address,
						typeInfo,
						r.Intn(4),
						newRandomUint64KeyFunc(r),
						newWrapperValueFunc(
							1,
							newRandomUint64ValueFunc(r)))))),
	}

	storage := newTestPersistentStorage(t)

	expectedValues := make(mapValue)

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	actualMapSize := 0

	t.Run("set and remove", func(t *testing.T) {
		// Insert elements

		var setCount int
		for setCount < minWriteOperationSize {
			setCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualMapSize += setCount

		for i := 0; i < setCount; i++ {
			k := Uint64Value(i)

			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[k] = expected
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements

		var removeCount int
		minRemoveCount := int(m.Count()) / 2
		maxRemoveCount := int(m.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(m.Count()) + 1)
		}

		actualMapSize -= removeCount

		// Remove elements

		keys := make([]Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for i := 0; i < removeCount; i++ {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		keys := make([]Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for len(keys) > 0 {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(0), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})
}

func TestMapWrapperValueModifyNewMapAtLevel2(t *testing.T) {

	const (
		minWriteOperationSize = 124
		maxWriteOperationSize = 256
	)

	r := newRand(t)

	SetThreshold(256)
	defer SetThreshold(1024)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	// newValue creates value of type SomeValue({uint64: SomeValue(uint64)}).
	newValue :=
		newWrapperValueFunc(
			1,
			newMapValueFunc(
				t,
				address,
				typeInfo,
				r.Intn(4)+1, // at least one element
				newRandomUint64KeyFunc(r),
				newWrapperValueFunc(
					1,
					newRandomUint64ValueFunc(r))))

	// modifyValue modifies nested map's first element.
	modifyValue :=
		modifyWrapperValueFunc(
			t,
			1,
			modifyMapValueFunc(
				t,
				true,
				modifyWrapperValueFunc(
					t,
					1,
					modifyRandomUint64ValueFunc(r))))

	storage := newTestPersistentStorage(t)

	expectedValues := make(mapValue)

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	actualMapSize := 0

	t.Run("set and remove", func(t *testing.T) {
		// Set elements

		var setCount int
		for setCount < minWriteOperationSize {
			setCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualMapSize += setCount

		for i := 0; i < setCount; i++ {
			k := Uint64Value(i)
			v, expected := newValue(storage)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[k] = expected
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements (including one previously inserted element)

		var removeCount int
		minRemoveCount := int(m.Count()) / 2
		maxRemoveCount := int(m.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(m.Count()) + 1)
		}

		actualMapSize -= removeCount

		// Remove elements

		keys := make([]Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for i := 0; i < removeCount; i++ {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		var setCount int
		if m.Count() <= 10 {
			setCount = int(m.Count())
		} else {
			for setCount < int(m.Count())/2 {
				setCount = r.Intn(int(m.Count()) + 1)
			}
		}

		keys := make([]Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for i := 0; i < setCount; i++ {
			index := r.Intn(len(keys))
			setKey := keys[index]

			// Get element
			originalValue, err := m.Get(compare, hashInputProvider, setKey)
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[setKey])
			require.NoError(t, err)

			expectedValues[setKey] = modifiedExpectedValue
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements (including some previously set elements)

		var removeCount int
		minRemoveCount := int(m.Count()) / 2
		maxRemoveCount := int(m.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(m.Count()))
		}

		actualMapSize -= removeCount

		// Remove more elements

		for i := 0; i < removeCount; i++ {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		keys := make([]Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for len(keys) > 0 {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(0), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})
}

func TestMapWrapperValueModifyNewMapAtLevel3(t *testing.T) {

	const (
		minWriteOperationSize = 124
		maxWriteOperationSize = 256
	)

	r := newRand(t)

	SetThreshold(256)
	defer SetThreshold(1024)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	// newValue creates value of type SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})}))
	newValue :=
		newWrapperValueFunc(
			1,
			newMapValueFunc(
				t,
				address,
				typeInfo,
				2,
				newRandomUint64KeyFunc(r),
				newWrapperValueFunc(
					1,
					newMapValueFunc(
						t,
						address,
						typeInfo,
						2,
						newRandomUint64KeyFunc(r),
						newWrapperValueFunc(
							1,
							newRandomUint64ValueFunc(r))))))

	// modifyValue modifies innermost nested map's first element.
	modifyValue :=
		modifyWrapperValueFunc(
			t,
			1,
			modifyMapValueFunc(
				t,
				false,
				modifyWrapperValueFunc(
					t,
					1,
					modifyMapValueFunc(
						t,
						true,
						modifyWrapperValueFunc(
							t,
							1,
							modifyRandomUint64ValueFunc(r))))))

	storage := newTestPersistentStorage(t)

	expectedValues := make(mapValue)

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	actualMapSize := 0

	t.Run("set and remove", func(t *testing.T) {
		// Insert elements

		var setCount int
		for setCount < minWriteOperationSize {
			setCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualMapSize += setCount

		for i := 0; i < setCount; i++ {
			k := Uint64Value(i)
			v, expected := newValue(storage)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[k] = expected
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements

		var removeCount int
		for removeCount < int(m.Count())/2 {
			removeCount = r.Intn(int(m.Count()) + 1)
		}

		actualMapSize -= removeCount

		keys := make([]Value, 0, m.Count())
		err := m.IterateReadOnlyKeys(func(key Value) (resume bool, err error) {
			keys = append(keys, key)
			return true, nil
		})
		require.NoError(t, err)

		for i := 0; i < removeCount; i++ {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		var setCount int
		if m.Count() <= 10 {
			setCount = int(m.Count())
		} else {
			for setCount < int(m.Count())/2 {
				setCount = r.Intn(int(m.Count()) + 1)
			}
		}

		keys := make([]Value, 0, m.Count())
		err := m.IterateReadOnlyKeys(func(key Value) (resume bool, err error) {
			keys = append(keys, key)
			return true, nil
		})
		require.NoError(t, err)

		for i := 0; i < setCount; i++ {
			index := r.Intn(len(keys))
			key := keys[index]

			// Get element
			originalValue, err := m.Get(compare, hashInputProvider, key)
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[key])
			require.NoError(t, err)

			expectedValues[key] = modifiedExpectedValue
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements

		var removeCount int
		for removeCount < int(m.Count())/2 {
			removeCount = r.Intn(int(m.Count()))
		}

		actualMapSize -= removeCount

		for i := 0; i < removeCount; i++ {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(actualMapSize), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		keys := make([]Value, 0, m.Count())
		err := m.IterateReadOnlyKeys(func(key Value) (resume bool, err error) {
			keys = append(keys, key)
			return true, nil
		})
		require.NoError(t, err)

		for m.Count() > 0 {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, uint64(0), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})
}

func TestMapWrapperValueModifyExistingMap(t *testing.T) {

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("modify level-1 wrapper map in {uint64: SomeValue({uint64: SomeValue(uint64)})}", func(t *testing.T) {
		const (
			mapSize      = 3
			childMapSize = 2
		)

		typeInfo := testTypeInfo{42}

		r := newRand(t)

		createStorage := func(mapSize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedKeyValues map[Value]Value,
		) {
			storage := newTestPersistentStorage(t)

			createMapOfSomeValueOfMapOfSomeValueOfUint64 :=
				newMapValueFunc(
					t,
					address,
					typeInfo,
					mapSize,
					newUint64KeyFunc(),
					newWrapperValueFunc(
						1,
						newMapValueFunc(
							t,
							address,
							typeInfo,
							childMapSize,
							newUint64KeyFunc(),
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))

			v, expected := createMapOfSomeValueOfMapOfSomeValueOfUint64(storage)

			m := v.(*OrderedMap)
			expectedKeyValues = expected.(mapValue)

			testMap(t, storage, typeInfo, address, m, expectedKeyValues, nil, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, m.SlabID(), expectedKeyValues
		}

		// Create a base storage with map in the format of
		// {uint64: SomeValue({uint64: SomeValue(uint64)})}
		baseStorage, rootSlabID, expectedKeyValues := createStorage(mapSize)
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

	t.Run("get and modify 2-level wrapper map in {uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(uint64)})})}", func(t *testing.T) {
		const (
			mapSize       = 4
			childMapSize  = 3
			gchildMapSize = 2
		)

		typeInfo := testTypeInfo{42}

		r := newRand(t)

		createStorage := func(mapSize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedKeyValues map[Value]Value,
		) {
			storage := newTestPersistentStorage(t)

			createMapOfSomeValueOfMapOfSomeValueOfMapOfSomeValueOfUint64 :=
				newMapValueFunc(
					t,
					address,
					typeInfo,
					mapSize,
					newUint64KeyFunc(),
					newWrapperValueFunc(
						1,
						newMapValueFunc(
							t,
							address,
							typeInfo,
							mapSize,
							newUint64KeyFunc(),
							newWrapperValueFunc(
								1,
								newMapValueFunc(
									t,
									address,
									typeInfo,
									childMapSize,
									newUint64KeyFunc(),
									newWrapperValueFunc(
										1,
										newRandomUint64ValueFunc(r)))))))

			v, expected := createMapOfSomeValueOfMapOfSomeValueOfMapOfSomeValueOfUint64(storage)

			m := v.(*OrderedMap)
			expectedKeyValues = expected.(mapValue)

			testMap(t, storage, typeInfo, address, m, expectedKeyValues, nil, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, m.SlabID(), expectedKeyValues
		}

		// Create a base storage with map in the format of
		// {uint64: SomeValue({uint64: SomeValue({uint64: SomeValue(string)})})}
		baseStorage, rootSlabID, expectedKeyValues := createStorage(mapSize)
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

func testSetElementInMap(t *testing.T, storage SlabStorage, m *OrderedMap, key Value, newValue Value, expected Value) {
	existingStorable, err := m.Set(compare, hashInputProvider, key, newValue)
	require.NoError(t, err)
	require.NotNil(t, existingStorable)

	// var overwrittenSlabID SlabID

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := unwrapStorable(existingStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case ArraySlab, MapSlab:
		require.Fail(t, "overwritten storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingStorable)

	case SlabIDStorable:
		overwrittenSlabID := SlabID(wrappedStorable)

		// Verify SlabID has the same address
		require.Equal(t, m.Address(), overwrittenSlabID.Address())
	}

	// Verify overwritten value

	existingValue, err := existingStorable.StoredValue(storage)
	require.NoError(t, err)
	valueEqual(t, expected, existingValue)

	removeFromStorage(t, storage, existingValue)
}

func testRemoveElementFromMap(t *testing.T, storage SlabStorage, m *OrderedMap, key Value, expected Value) {
	existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, key)
	require.NoError(t, err)
	require.NotNil(t, existingKeyStorable)
	require.NotNil(t, existingValueStorable)

	// var removedSlabID SlabID

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := unwrapStorable(existingValueStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case ArraySlab, MapSlab:
		require.Fail(t, "removed storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingValueStorable)

	case SlabIDStorable:
		removedSlabID := SlabID(wrappedStorable)

		// Verify SlabID has the same address
		require.Equal(t, m.Address(), removedSlabID.Address())
	}

	// Verify removed value

	existingKey, err := existingKeyStorable.StoredValue(storage)
	require.NoError(t, err)
	valueEqual(t, key, existingKey)

	removeFromStorage(t, storage, existingKey)

	existingValue, err := existingValueStorable.StoredValue(storage)
	require.NoError(t, err)
	valueEqual(t, expected, existingValue)

	removeFromStorage(t, storage, existingValue)
}
