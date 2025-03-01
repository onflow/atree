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
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
)

type newKeyFunc func(atree.SlabStorage) (key atree.Value, expected atree.Value)

var newRandomUint64KeyFunc = func(r *rand.Rand) newKeyFunc {
	return func(atree.SlabStorage) (key atree.Value, expected atree.Value) {
		v := test_utils.Uint64Value(r.Intn(1844674407370955161))
		return v, v
	}
}

var newUint64KeyFunc = func() newKeyFunc {
	i := 0
	return func(atree.SlabStorage) (key atree.Value, expected atree.Value) {
		v := test_utils.Uint64Value(i)
		i++
		return v, v
	}
}

var newMapValueFunc = func(
	t *testing.T,
	address atree.Address,
	typeInfo atree.TypeInfo,
	mapCount int,
	newKey newKeyFunc,
	newValue newValueFunc,
) newValueFunc {
	return func(storage atree.SlabStorage) (value atree.Value, expected atree.Value) {
		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value)

		for range mapCount {
			k, expectedK := newKey(storage)
			v, expectedV := newValue(storage)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[expectedK] = expectedV
		}

		return m, test_utils.ExpectedMapValue(keyValues)
	}
}

var modifyMapValueFunc = func(
	t *testing.T,
	needToResetModifiedValue bool,
	modifyValueFunc modifyValueFunc,
) modifyValueFunc {
	return func(
		storage atree.SlabStorage,
		originalValue atree.Value,
		expectedOrigianlValue atree.Value,
	) (
		modifiedValue atree.Value,
		expectedModifiedValue atree.Value,
		err error,
	) {
		m, ok := originalValue.(*atree.OrderedMap)
		require.True(t, ok)

		expectedValues, ok := expectedOrigianlValue.(test_utils.ExpectedMapValue)
		require.True(t, ok)

		require.Equal(t, uint64(len(expectedValues)), m.Count())
		require.True(t, m.Count() > 0)

		// Modify first element

		var firstKey atree.Value
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			firstKey = k
			return false, nil
		})
		require.NoError(t, err)

		v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, firstKey)
		require.NoError(t, err)

		modifiedV, expectedModifiedV, err := modifyValueFunc(storage, v, expectedValues[firstKey])
		if err != nil {
			return nil, nil, err
		}

		if modifiedV == nil {

			existingKeyStorable, existingValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, firstKey)
			if err != nil {
				return nil, nil, err
			}
			require.NotNil(t, existingKeyStorable)
			require.NotNil(t, existingValueStorable)

			// Verify wrapped storable doesn't contain inlined slab

			wrappedStorable := atree.UnwrapStorable(existingValueStorable)

			var removedSlabID atree.SlabID

			switch wrappedStorable := wrappedStorable.(type) {
			case atree.ArraySlab, atree.MapSlab:
				require.Fail(t, "removed storable shouldn't be (wrapped) atree.ArraySlab or atree.MapSlab: %s", existingValueStorable)

			case atree.SlabIDStorable:
				removedSlabID = atree.SlabID(wrappedStorable)

				// Verify atree.SlabID has the same address
				require.Equal(t, m.Address(), removedSlabID.Address())
			}

			existingValue, err := existingValueStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, expectedValues[firstKey], existingValue)

			// Remove slabs from storage
			if removedSlabID != atree.SlabIDUndefined {
				err = storage.Remove(removedSlabID)
				require.NoError(t, err)
			}

			delete(expectedValues, firstKey)

		} else {

			if needToResetModifiedValue {
				existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, firstKey, modifiedV)
				if err != nil {
					return nil, nil, err
				}
				require.NotNil(t, existingStorable)

				// Verify wrapped storable doesn't contain inlined slab

				wrappedStorable := atree.UnwrapStorable(existingStorable)

				var overwrittenSlabID atree.SlabID

				switch wrappedStorable := wrappedStorable.(type) {
				case atree.ArraySlab, atree.MapSlab:
					require.Fail(t, "overwritten storable shouldn't be (wrapped) atree.ArraySlab or atree.MapSlab: %s", existingStorable)

				case atree.SlabIDStorable:
					overwrittenSlabID = atree.SlabID(wrappedStorable)

					// Verify atree.SlabID has the same address
					require.Equal(t, m.Address(), overwrittenSlabID.Address())
				}

				existingValue, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)

				testValueEqual(t, expectedValues[firstKey], existingValue)

				if overwrittenSlabID != atree.SlabIDUndefined {
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
	address atree.Address,
	typeInfo atree.TypeInfo,
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
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapCount = 10
		largeMapCount = 512
	)

	mapCountTestCases := []struct {
		name     string
		mapCount uint64
	}{
		{name: "small map", mapCount: smallMapCount},
		{name: "large map", mapCount: largeMapCount},
	}

	testCases := newMapWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, mapCountTestCase := range mapCountTestCases {

			mapCount := mapCountTestCase.mapCount

			name := mapCountTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += "," + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
				require.NoError(t, err)

				rootSlabID := m.SlabID()

				// Set WrapperValue
				expectedValues := make(map[atree.Value]atree.Value)
				for uint64(len(expectedValues)) < mapCount {
					k, expectedK := tc.newKey(storage)

					if _, exists := expectedValues[expectedK]; exists {
						continue
					}

					v, expectedV := tc.newValue(storage)

					existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
					require.NoError(t, err)
					require.Nil(t, existingStorable)

					expectedValues[expectedK] = expectedV
				}

				require.Equal(t, mapCount, m.Count())

				testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

				// Retrieve and modify WrapperValue from map
				for key, expectedValue := range expectedValues {
					v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, key)
					require.NoError(t, err)

					testValueEqual(t, expectedValue, v)

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

				require.Equal(t, mapCount, m.Count())

				testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load map from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

				m2, err := atree.NewMapWithRootID(storage2, rootSlabID, atree.NewDefaultDigesterBuilder())
				require.NoError(t, err)
				require.Equal(t, mapCount, m2.Count())

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
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapCount = 10
		largeMapCount = 512
	)

	mapCountTestCases := []struct {
		name     string
		mapCount uint64
	}{
		{name: "small map", mapCount: smallMapCount},
		{name: "large map", mapCount: largeMapCount},
	}

	modifyTestCases := []struct {
		name                string
		needToModifyElement bool
	}{
		{name: "modify elements", needToModifyElement: true},
		{name: "", needToModifyElement: false},
	}

	removeTestCases := []struct {
		name               string
		removeAllElements  bool
		removeElementCount uint64
	}{
		{name: "remove all elements", removeAllElements: true},
		{name: "remove 1 element", removeElementCount: 1},
		{name: fmt.Sprintf("remove %d element", smallMapCount/2), removeElementCount: smallMapCount / 2},
	}

	testCases := newMapWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, mapCountTestCase := range mapCountTestCases {

			for _, modifyTestCase := range modifyTestCases {

				for _, removeTestCase := range removeTestCases {

					mapCount := mapCountTestCase.mapCount

					needToModifyElement := modifyTestCase.needToModifyElement

					removeCount := removeTestCase.removeElementCount
					if removeTestCase.removeAllElements {
						removeCount = mapCount
					}

					name := mapCountTestCase.name + " " + tc.name
					if modifyTestCase.needToModifyElement {
						name += ", " + tc.modifyName
					}
					if removeTestCase.name != "" {
						name += ", " + removeTestCase.name
					}

					t.Run(name, func(t *testing.T) {

						storage := newTestPersistentStorage(t)

						m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
						require.NoError(t, err)

						rootSlabID := m.SlabID()

						expectedValues := make(map[atree.Value]atree.Value)

						// Set WrapperValue in map
						for uint64(len(expectedValues)) < mapCount {
							k, expectedK := tc.newKey(storage)

							if _, exists := expectedValues[expectedK]; exists {
								continue
							}

							v, expectedV := tc.newValue(storage)

							existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
							require.NoError(t, err)
							require.Nil(t, existingStorable)

							expectedValues[expectedK] = expectedV
						}

						require.Equal(t, mapCount, m.Count())

						testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

						// Retrieve and modify WrapperValue from map
						if needToModifyElement {
							for key, expected := range expectedValues {
								v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, key)
								require.NoError(t, err)

								testValueEqual(t, expected, v)

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

							require.Equal(t, mapCount, m.Count())

							testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
						}

						keys := make([]atree.Value, 0, len(expectedValues))
						for key := range expectedValues {
							keys = append(keys, key)
						}

						// Remove random elements
						for range removeCount {

							removeKeyIndex := r.Intn(len(keys))
							removeKey := keys[removeKeyIndex]

							testRemoveElementFromMap(t, storage, m, removeKey, expectedValues[removeKey])

							delete(expectedValues, removeKey)

							keys = append(keys[:removeKeyIndex], keys[removeKeyIndex+1:]...)
						}

						require.Equal(t, mapCount-removeCount, m.Count())
						require.Equal(t, mapCount-removeCount, uint64(len(expectedValues)))

						testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

						// Commit storage
						err = storage.FastCommit(runtime.NumCPU())
						require.NoError(t, err)

						// Load map from encoded data
						storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

						m2, err := atree.NewMapWithRootID(storage2, rootSlabID, atree.NewDefaultDigesterBuilder())
						require.NoError(t, err)
						require.Equal(t, mapCount-removeCount, m2.Count())

						// Test loaded map
						testMap(t, storage2, typeInfo, address, m2, expectedValues, nil, true)
					})
				}
			}
		}
	}
}

func TestMapWrapperValueReadOnlyIterate(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapCount = 10
		largeMapCount = 512
	)

	mapCountTestCases := []struct {
		name     string
		mapCount uint64
	}{
		{name: "small map", mapCount: smallMapCount},
		{name: "large map", mapCount: largeMapCount},
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

		for _, mapCountTestCase := range mapCountTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && tc.mustSetModifiedElementInMap {
					continue
				}

				mapCount := mapCountTestCase.mapCount

				testModifyElement := modifyTestCase.testModifyElement

				name := mapCountTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
					require.NoError(t, err)

					expectedValues := make(map[atree.Value]atree.Value)

					// Set WrapperValue to map
					for uint64(len(expectedValues)) < mapCount {
						k, expectedK := tc.newKey(storage)

						if _, exists := expectedValues[expectedK]; exists {
							continue
						}

						v, expectedV := tc.newValue(storage)

						existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
						require.NoError(t, err)
						require.Nil(t, existingStorable)

						expectedValues[expectedK] = expectedV
					}

					require.Equal(t, mapCount, m.Count())

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

						testValueEqual(t, expected, nextValue)

						// Test modifying elements that don't need to reset in parent container.
						if testModifyElement {
							_, _, err := tc.modifyValue(storage, nextValue, expected)
							var targetErr *atree.ReadOnlyIteratorElementMutationError
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
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallMapCount = 10
		largeMapCount = 512
	)

	mapCountTestCases := []struct {
		name     string
		mapCount uint64
	}{
		{name: "small map", mapCount: smallMapCount},
		{name: "large map", mapCount: largeMapCount},
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

		for _, mapCountTestCase := range mapCountTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				elementIsContainer := !tc.mustSetModifiedElementInMap

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && !elementIsContainer {
					continue
				}

				mapCount := mapCountTestCase.mapCount

				testModifyElement := modifyTestCase.testModifyElement

				name := mapCountTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
					require.NoError(t, err)

					expectedValues := make(map[atree.Value]atree.Value)

					// Set WrapperValue in map
					for uint64(len(expectedValues)) < mapCount {
						k, expectedK := tc.newKey(storage)

						if _, exists := expectedValues[expectedK]; exists {
							continue
						}

						v, expectedV := tc.newValue(storage)

						existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
						require.NoError(t, err)
						require.Nil(t, existingStorable)

						expectedValues[expectedK] = expectedV
					}

					require.Equal(t, mapCount, m.Count())

					testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

					iterator, err := m.Iterator(test_utils.CompareValue, test_utils.GetHashInput)
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

						testValueEqual(t, expected, nextValue)

						// Test modifying container elements.
						if testModifyElement {
							_, newExpectedV, err := tc.modifyValue(storage, nextValue, expected)
							require.NoError(t, err)

							expectedValues[nextKey] = newExpectedV
						}

						count++
					}

					require.Equal(t, mapCount, m.Count())

					testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
				})
			}
		}
	}
}

func TestMapWrapperValueInlineMapAtLevel1(t *testing.T) {

	testLevel1WrappedChildMapInlined := func(t *testing.T, m *atree.OrderedMap, expectedInlined bool) {
		require.True(t, IsMapRootDataSlab(m))

		keyAndValues := atree.GetMapRootSlabStorables(m)
		require.Equal(t, 2, len(keyAndValues))

		value := keyAndValues[1]

		storabeleAsSomeStorable, isSomeStorable := value.(test_utils.SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorable := storabeleAsSomeStorable.Storable

		switch wrappedStorable := wrappedStorable.(type) {
		case atree.SlabIDStorable:
			inlined := false
			require.Equal(t, expectedInlined, inlined)

		case atree.ArraySlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		case atree.MapSlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		default:
			require.Fail(t, "wrapped storable has unexpected type: %T", wrappedStorable)
		}
	}

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	storage := newTestPersistentStorage(t)

	expectedValues := make(test_utils.ExpectedMapValue)

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// Set WrapperValue test_utils.SomeValue([]) in map
	{
		// Create standalone child map
		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		require.False(t, childMap.Inlined())

		// Set child map (level-1 inlined map) in parent map
		key := test_utils.Uint64Value(0)

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, key, test_utils.NewSomeValue(childMap))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, childMap.Inlined())

		expectedValues[key] = test_utils.NewExpectedWrapperValue(test_utils.ExpectedMapValue{})

		require.Equal(t, uint64(1), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		testLevel1WrappedChildMapInlined(t, m, true)
	}

	// Retrieve wrapped child map, and then insert new elements to child map.
	// Wrapped child map is expected to be unlined at the end of loop.

	const childMapCount = uint64(8)
	for i := range childMapCount + 1 {
		// Get element
		element, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.NoError(t, err)
		require.NotNil(t, element)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedMap, isMap := wrappedValue.(*atree.OrderedMap)
		require.True(t, isMap)

		expectedWrappedValue := expectedValues[test_utils.Uint64Value(0)].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedMap := expectedWrappedValue.(test_utils.ExpectedMapValue)

		// Insert new elements to wrapped child map

		k := test_utils.Uint64Value(i)
		v := test_utils.Uint64Value(r.Intn(256))

		existingStorable, err := wrappedMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.NewSomeValue(v))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedWrappedMap[k] = test_utils.NewExpectedWrapperValue(v)

		expectedValues[test_utils.Uint64Value(0)] = test_utils.NewExpectedWrapperValue(expectedWrappedMap)

		require.Equal(t, i+1, wrappedMap.Count())
		require.Equal(t, i+1, uint64(len(expectedWrappedMap)))

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel1WrappedChildMapInlined(t, m, false)

	// Retrieve wrapped child map, and then remove elements from child map.
	// Wrapped child map is expected to be inlined at the end of loop.

	childMapCountAfterRemoval := uint64(2)
	removeCount := childMapCount - childMapCountAfterRemoval

	for i := range removeCount {
		// Get element
		element, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.NoError(t, err)
		require.NotNil(t, element)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedMap, isMap := wrappedValue.(*atree.OrderedMap)
		require.True(t, isMap)

		expectedWrappedValue := expectedValues[test_utils.Uint64Value(0)].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedMap := expectedWrappedValue.(test_utils.ExpectedMapValue)

		// Remove element from wrapped child map

		key := test_utils.Uint64Value(i)

		existingKeyStorable, existingValueStorable, err := wrappedMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, key)
		require.NoError(t, err)
		require.NotNil(t, existingKeyStorable)
		require.NotNil(t, existingValueStorable)

		// Verify removed key and value

		existingValue, err := existingValueStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, expectedWrappedMap[key], existingValue)

		delete(expectedWrappedMap, key)

		expectedValues[test_utils.Uint64Value(0)] = test_utils.NewExpectedWrapperValue(expectedWrappedMap)

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel1WrappedChildMapInlined(t, m, true)

	testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
}

func TestMapWrapperValueInlineMapAtLevel2(t *testing.T) {

	testLevel2WrappedChildMapInlined := func(t *testing.T, m *atree.OrderedMap, expectedInlined bool) {
		require.True(t, IsMapRootDataSlab(m))

		keyAndValuesAtLevel1 := atree.GetMapRootSlabStorables(m)

		require.Equal(t, 2, len(keyAndValuesAtLevel1))

		// Get unwrapped value at level 1

		storableAtLevel1 := keyAndValuesAtLevel1[1]

		storabeleAsSomeStoable, isSomeStorable := storableAtLevel1.(test_utils.SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorableAtLevel1 := storabeleAsSomeStoable.Storable

		wrappedMapAtlevel1, isMap := wrappedStorableAtLevel1.(*atree.MapDataSlab)
		require.True(t, isMap)

		// Get unwrapped value at level 2

		keyAndValuesAtLevel2 := atree.GetMapSlabStorables(wrappedMapAtlevel1)

		storableAtLevel2 := keyAndValuesAtLevel2[1]

		storabeleAsSomeStoable, isSomeStorable = storableAtLevel2.(test_utils.SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorableAtLevel2 := storabeleAsSomeStoable.Storable

		switch wrappedStorable := wrappedStorableAtLevel2.(type) {
		case atree.SlabIDStorable:
			inlined := false
			require.Equal(t, expectedInlined, inlined)

		case atree.ArraySlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		case atree.MapSlab:
			inlined := true
			require.Equal(t, expectedInlined, inlined)

		default:
			require.Fail(t, "wrapped storable has unexpected type: %T", wrappedStorable)
		}
	}

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	r := newRand(t)

	storage := newTestPersistentStorage(t)

	expectedValues := make(test_utils.ExpectedMapValue)

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// Set WrapperValue test_utils.SomeValue({test_utils.NewSomeValue(}}) to map
	{
		// Create grand child map
		gchildMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		require.False(t, gchildMap.Inlined())

		// Create child map
		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		require.False(t, childMap.Inlined())

		// Set grand child map to child map
		existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.NewSomeValue(gchildMap))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, gchildMap.Inlined())

		// Append child map to map
		existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.NewSomeValue(childMap))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, childMap.Inlined())

		expectedValues[test_utils.Uint64Value(0)] = test_utils.NewExpectedWrapperValue(
			test_utils.ExpectedMapValue{
				test_utils.Uint64Value(0): test_utils.NewExpectedWrapperValue(test_utils.ExpectedMapValue{})})

		require.Equal(t, uint64(1), m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		testLevel2WrappedChildMapInlined(t, m, true)
	}

	// Retrieve wrapped gchild map, and then insert new elements to gchild map.
	// Wrapped gchild map is expected to be unlined at the end of loop.

	const gchildMapCount = uint64(8)
	for i := range gchildMapCount {
		// Get element at level 1

		elementAtLevel1, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.NoError(t, err)
		require.NotNil(t, elementAtLevel1)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedMapAtLevel1, isMap := wrappedValueAtLevel1.(*atree.OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel1 := expectedValues[test_utils.Uint64Value(0)].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedMapAtLevel1 := expectedWrappedValueAtLevel1.(test_utils.ExpectedMapValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedMapAtLevel1.Get(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedMapAtLevel2, isMap := wrappedValueAtLevel2.(*atree.OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel2 := expectedWrappedMapAtLevel1[test_utils.Uint64Value(0)].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedMapAtLevel2 := expectedWrappedValueAtLevel2.(test_utils.ExpectedMapValue)

		// Insert new elements to wrapped gchild map

		k := test_utils.Uint64Value(i)
		v := test_utils.Uint64Value(r.Intn(256))

		existingStorable, err := wrappedMapAtLevel2.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.NewSomeValue(v))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedWrappedMapAtLevel2[k] = test_utils.NewExpectedWrapperValue(v)

		expectedValues[test_utils.Uint64Value(0)] = test_utils.NewExpectedWrapperValue(
			test_utils.ExpectedMapValue{
				test_utils.Uint64Value(0): test_utils.NewExpectedWrapperValue(expectedWrappedMapAtLevel2)})

		require.Equal(t, i+1, wrappedMapAtLevel2.Count())
		require.Equal(t, i+1, uint64(len(expectedWrappedMapAtLevel2)))

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel2WrappedChildMapInlined(t, m, false)

	// Retrieve wrapped gchild map, and then remove elements from gchild map.
	// Wrapped gchild map is expected to be inlined at the end of loop.

	gchildMapCountAfterRemoval := uint64(2)
	removeCount := gchildMapCount - gchildMapCountAfterRemoval

	for i := range removeCount {
		// Get elementAtLevel1
		elementAtLevel1, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedMapAtLevel1, isMap := wrappedValueAtLevel1.(*atree.OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel1 := expectedValues[test_utils.Uint64Value(0)].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedMapAtLevel1 := expectedWrappedValueAtLevel1.(test_utils.ExpectedMapValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedMapAtLevel1.Get(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedMapAtLevel2, isMap := wrappedValueAtLevel2.(*atree.OrderedMap)
		require.True(t, isMap)

		expectedWrappedValueAtLevel2 := expectedWrappedMapAtLevel1[test_utils.Uint64Value(0)].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedMapAtLevel2 := expectedWrappedValueAtLevel2.(test_utils.ExpectedMapValue)

		// Remove first element from wrapped gchild map

		key := test_utils.Uint64Value(i)

		existingKeyStorable, existingValueStorable, err := wrappedMapAtLevel2.Remove(test_utils.CompareValue, test_utils.GetHashInput, key)
		require.NoError(t, err)
		require.NotNil(t, existingKeyStorable)
		require.NotNil(t, existingValueStorable)

		// Verify removed value

		existingValue, err := existingValueStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, expectedWrappedMapAtLevel2[key], existingValue)

		delete(expectedWrappedMapAtLevel2, key)

		expectedValues[test_utils.Uint64Value(0)] = test_utils.NewExpectedWrapperValue(
			test_utils.ExpectedMapValue{
				test_utils.Uint64Value(0): test_utils.NewExpectedWrapperValue(expectedWrappedMapAtLevel2)})

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	}

	testLevel2WrappedChildMapInlined(t, m, true)

	testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
}

func TestMapWrapperValueModifyNewMapAtLevel1(t *testing.T) {

	const (
		minWriteOperationCount = 124
		maxWriteOperationCount = 256
	)

	r := newRand(t)

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	newElementFuncs := []newValueFunc{
		// test_utils.SomeValue(uint64)
		newWrapperValueFunc(1, newRandomUint64ValueFunc(r)),

		// test_utils.SomeValue({uint64: test_utils.SomeValue(uint64)})
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

		// test_utils.SomeValue({uint64: test_utils.SomeValue({uint64: test_utils.SomeValue(uint64)})})
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

	expectedValues := make(test_utils.ExpectedMapValue)

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	actualMapCount := uint64(0)

	t.Run("set and remove", func(t *testing.T) {
		// Insert elements

		setCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualMapCount += setCount

		for i := range setCount {
			k := test_utils.Uint64Value(i)

			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[k] = expected
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements
		mapCount := m.Count()

		minRemoveCount := mapCount / 2
		maxRemoveCount := mapCount / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualMapCount -= removeCount

		// Remove elements

		keys := make([]atree.Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for range removeCount {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		keys := make([]atree.Value, 0, len(expectedValues))
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
		minWriteOperationCount = 124
		maxWriteOperationCount = 256
	)

	r := newRand(t)

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	// newValue creates value of type test_utils.SomeValue({uint64: test_utils.SomeValue(uint64)}).
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

	expectedValues := make(test_utils.ExpectedMapValue)

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	actualMapCount := uint64(0)

	t.Run("set and remove", func(t *testing.T) {
		// Set elements

		setCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualMapCount += setCount

		for i := range setCount {
			k := test_utils.Uint64Value(i)
			v, expected := newValue(storage)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[k] = expected
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements (including one previously inserted element)

		mapCount := m.Count()

		minRemoveCount := mapCount / 2
		maxRemoveCount := mapCount / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualMapCount -= removeCount

		// Remove elements

		keys := make([]atree.Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for range removeCount {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		mapCount := m.Count()

		setCount := mapCount
		if m.Count() > 10 {
			setCount = getRandomUint64InRange(r, mapCount/2, mapCount)
		}

		keys := make([]atree.Value, 0, len(expectedValues))
		for k := range expectedValues {
			keys = append(keys, k)
		}

		for range setCount {
			index := r.Intn(len(keys))
			setKey := keys[index]

			// Get element
			originalValue, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, setKey)
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(test_utils.SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[setKey])
			require.NoError(t, err)

			expectedValues[setKey] = modifiedExpectedValue
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements (including some previously set elements)

		mapCount = m.Count()

		minRemoveCount := mapCount / 2
		maxRemoveCount := mapCount / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualMapCount -= removeCount

		// Remove more elements

		for range removeCount {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		keys := make([]atree.Value, 0, len(expectedValues))
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
		minWriteOperationCount = 124
		maxWriteOperationCount = 256
	)

	r := newRand(t)

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	// newValue creates value of type test_utils.SomeValue({uint64: test_utils.SomeValue({uint64: test_utils.SomeValue(uint64)})}))
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

	expectedValues := make(test_utils.ExpectedMapValue)

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	actualMapCount := uint64(0)

	t.Run("set and remove", func(t *testing.T) {
		// Insert elements

		setCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualMapCount += setCount

		for i := range setCount {
			k := test_utils.Uint64Value(i)
			v, expected := newValue(storage)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[k] = expected
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements

		mapCount := m.Count()

		removeCount := getRandomUint64InRange(r, mapCount/2, mapCount)

		actualMapCount -= removeCount

		keys := make([]atree.Value, 0, m.Count())
		err := m.IterateReadOnlyKeys(func(key atree.Value) (resume bool, err error) {
			keys = append(keys, key)
			return true, nil
		})
		require.NoError(t, err)

		for range removeCount {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		mapCount := m.Count()

		setCount := mapCount
		if m.Count() > 10 {
			setCount = getRandomUint64InRange(r, mapCount/2, mapCount)
		}

		keys := make([]atree.Value, 0, m.Count())
		err := m.IterateReadOnlyKeys(func(key atree.Value) (resume bool, err error) {
			keys = append(keys, key)
			return true, nil
		})
		require.NoError(t, err)

		for range setCount {
			index := r.Intn(len(keys))
			key := keys[index]

			// Get element
			originalValue, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, key)
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(test_utils.SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[key])
			require.NoError(t, err)

			expectedValues[key] = modifiedExpectedValue
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)

		// Remove some elements

		mapCount = m.Count()

		removeCount := getRandomUint64InRange(r, mapCount/2, mapCount)

		actualMapCount -= removeCount

		for range removeCount {
			index := r.Intn(len(keys))
			key := keys[index]

			testRemoveElementFromMap(t, storage, m, key, expectedValues[key])

			delete(expectedValues, key)
			keys = append(keys[:index], keys[index+1:]...)
		}

		require.Equal(t, actualMapCount, m.Count())

		testMap(t, storage, typeInfo, address, m, expectedValues, nil, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		keys := make([]atree.Value, 0, m.Count())
		err := m.IterateReadOnlyKeys(func(key atree.Value) (resume bool, err error) {
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

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("modify level-1 wrapper map in {uint64: test_utils.SomeValue({uint64: test_utils.SomeValue(uint64)})}", func(t *testing.T) {
		const (
			mapCount      = 3
			childMapCount = 2
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		r := newRand(t)

		createStorage := func(mapCount int) (
			_ atree.BaseStorage,
			rootSlabID atree.SlabID,
			expectedKeyValues map[atree.Value]atree.Value,
		) {
			storage := newTestPersistentStorage(t)

			createMapOfSomeValueOfMapOfSomeValueOfUint64 :=
				newMapValueFunc(
					t,
					address,
					typeInfo,
					mapCount,
					newUint64KeyFunc(),
					newWrapperValueFunc(
						1,
						newMapValueFunc(
							t,
							address,
							typeInfo,
							childMapCount,
							newUint64KeyFunc(),
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))

			v, expected := createMapOfSomeValueOfMapOfSomeValueOfUint64(storage)

			m := v.(*atree.OrderedMap)
			expectedKeyValues = expected.(test_utils.ExpectedMapValue)

			testMap(t, storage, typeInfo, address, m, expectedKeyValues, nil, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return atree.GetBaseStorage(storage), m.SlabID(), expectedKeyValues
		}

		// Create a base storage with map in the format of
		// {uint64: test_utils.SomeValue({uint64: test_utils.SomeValue(uint64)})}
		baseStorage, rootSlabID, expectedKeyValues := createStorage(mapCount)
		require.Equal(t, mapCount, len(expectedKeyValues))

		keys := make([]atree.Value, 0, len(expectedKeyValues))
		for k := range expectedKeyValues {
			keys = append(keys, k)
		}

		// Create a new storage with encoded map
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing map from storage
		m, err := atree.NewMapWithRootID(storage, rootSlabID, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m.Count())

		// Get and verify first element as test_utils.SomeValue(map)

		key := keys[0]
		expectedValues := expectedKeyValues[key]

		// Get map element (test_utils.SomeValue)
		element, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, key)
		require.NoError(t, err)

		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildMap, isOrderedMap := elementAsSomeValue.Value.(*atree.OrderedMap)
		require.True(t, isOrderedMap)

		expectedValuesAsSomeValue, isSomeValue := expectedValues.(test_utils.ExpectedWrapperValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildMap, isMapValue := expectedValuesAsSomeValue.Value.(test_utils.ExpectedMapValue)
		require.True(t, isMapValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildMap)), unwrappedChildMap.Count())

		// Modify wrapped child map of test_utils.SomeValue

		newKey := test_utils.NewStringValue("x")
		newValue := test_utils.NewStringValue("y")
		existingStorable, err := unwrappedChildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, newKey, test_utils.NewSomeValue(newValue))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedChildMapValues := expectedKeyValues[key].(test_utils.ExpectedWrapperValue).Value.(test_utils.ExpectedMapValue)
		expectedChildMapValues[newKey] = test_utils.NewExpectedWrapperValue(newValue)

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child map of test_utils.SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

		// Load existing map from storage
		m2, err := atree.NewMapWithRootID(storage2, rootSlabID, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m2.Count())

		testMap(t, storage, typeInfo, address, m2, expectedKeyValues, nil, true)
	})

	t.Run("get and modify 2-level wrapper map in {uint64: test_utils.SomeValue({uint64: test_utils.SomeValue({uint64: test_utils.SomeValue(uint64)})})}", func(t *testing.T) {
		const (
			mapCount       = 4
			childMapCount  = 3
			gchildMapCount = 2
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		r := newRand(t)

		createStorage := func(mapCount int) (
			_ atree.BaseStorage,
			rootSlabID atree.SlabID,
			expectedKeyValues map[atree.Value]atree.Value,
		) {
			storage := newTestPersistentStorage(t)

			createMapOfSomeValueOfMapOfSomeValueOfMapOfSomeValueOfUint64 :=
				newMapValueFunc(
					t,
					address,
					typeInfo,
					mapCount,
					newUint64KeyFunc(),
					newWrapperValueFunc(
						1,
						newMapValueFunc(
							t,
							address,
							typeInfo,
							mapCount,
							newUint64KeyFunc(),
							newWrapperValueFunc(
								1,
								newMapValueFunc(
									t,
									address,
									typeInfo,
									childMapCount,
									newUint64KeyFunc(),
									newWrapperValueFunc(
										1,
										newRandomUint64ValueFunc(r)))))))

			v, expected := createMapOfSomeValueOfMapOfSomeValueOfMapOfSomeValueOfUint64(storage)

			m := v.(*atree.OrderedMap)
			expectedKeyValues = expected.(test_utils.ExpectedMapValue)

			testMap(t, storage, typeInfo, address, m, expectedKeyValues, nil, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return atree.GetBaseStorage(storage), m.SlabID(), expectedKeyValues
		}

		// Create a base storage with map in the format of
		// {uint64: test_utils.SomeValue({uint64: test_utils.SomeValue({uint64: test_utils.SomeValue(string)})})}
		baseStorage, rootSlabID, expectedKeyValues := createStorage(mapCount)
		require.Equal(t, mapCount, len(expectedKeyValues))

		keys := make([]atree.Value, 0, len(expectedKeyValues))
		for k := range expectedKeyValues {
			keys = append(keys, k)
		}

		// Create a new storage with encoded map
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing map from storage
		m, err := atree.NewMapWithRootID(storage, rootSlabID, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m.Count())

		// Get and verify first element as test_utils.SomeValue(map)

		key := keys[0]
		expectedValues := expectedKeyValues[key]

		// Get map element (test_utils.SomeValue)
		element, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, key)
		require.NoError(t, err)

		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildMap, isOrderedMap := elementAsSomeValue.Value.(*atree.OrderedMap)
		require.True(t, isOrderedMap)

		expectedValuesAsSomeValue, isSomeValue := expectedValues.(test_utils.ExpectedWrapperValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildMap, isMapValue := expectedValuesAsSomeValue.Value.(test_utils.ExpectedMapValue)
		require.True(t, isMapValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildMap)), unwrappedChildMap.Count())

		// Get and verify nested child element as test_utils.SomeValue(map)

		childMapKeys := make([]atree.Value, 0, len(expectedUnwrappedChildMap))
		for k := range expectedUnwrappedChildMap {
			childMapKeys = append(childMapKeys, k)
		}

		childMapKey := childMapKeys[0]

		childMapElement, err := unwrappedChildMap.Get(test_utils.CompareValue, test_utils.GetHashInput, childMapKey)
		require.NoError(t, err)

		childMapElementAsSomeValue, isSomeValue := childMapElement.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		unwrappedGChildMap, isOrderedMap := childMapElementAsSomeValue.Value.(*atree.OrderedMap)
		require.True(t, isOrderedMap)

		expectedChildValuesAsSomeValue, isSomeValue := expectedUnwrappedChildMap[childMapKey].(test_utils.ExpectedWrapperValue)
		require.True(t, isSomeValue)

		expectedUnwrappedGChildMap, isMapValue := expectedChildValuesAsSomeValue.Value.(test_utils.ExpectedMapValue)
		require.True(t, isMapValue)

		require.Equal(t, uint64(len(expectedUnwrappedGChildMap)), unwrappedGChildMap.Count())

		// Modify wrapped gchild map of test_utils.SomeValue

		newKey := test_utils.NewStringValue("x")
		newValue := test_utils.NewStringValue("y")
		existingStorable, err := unwrappedGChildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, newKey, test_utils.NewSomeValue(newValue))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedChildMapValues := expectedKeyValues[key].(test_utils.ExpectedWrapperValue).Value.(test_utils.ExpectedMapValue)
		expectedGChildMapValues := expectedChildMapValues[childMapKey].(test_utils.ExpectedWrapperValue).Value.(test_utils.ExpectedMapValue)
		expectedGChildMapValues[newKey] = test_utils.NewExpectedWrapperValue(newValue)

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child map of test_utils.SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

		// Load existing map from storage
		m2, err := atree.NewMapWithRootID(storage2, rootSlabID, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedKeyValues)), m2.Count())

		testMap(t, storage, typeInfo, address, m2, expectedKeyValues, nil, true)
	})
}

func testSetElementInMap(t *testing.T, storage atree.SlabStorage, m *atree.OrderedMap, key atree.Value, newValue atree.Value, expected atree.Value) {
	existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, key, newValue)
	require.NoError(t, err)
	require.NotNil(t, existingStorable)

	// var overwrittenSlabID atree.SlabID

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := atree.UnwrapStorable(existingStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case atree.ArraySlab, atree.MapSlab:
		require.Fail(t, "overwritten storable shouldn't be (wrapped) atree.ArraySlab or atree.MapSlab: %s", existingStorable)

	case atree.SlabIDStorable:
		overwrittenSlabID := atree.SlabID(wrappedStorable)

		// Verify atree.SlabID has the same address
		require.Equal(t, m.Address(), overwrittenSlabID.Address())
	}

	// Verify overwritten value

	existingValue, err := existingStorable.StoredValue(storage)
	require.NoError(t, err)
	testValueEqual(t, expected, existingValue)

	removeFromStorage(t, storage, existingValue)
}

func testRemoveElementFromMap(t *testing.T, storage atree.SlabStorage, m *atree.OrderedMap, key atree.Value, expected atree.Value) {
	existingKeyStorable, existingValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, key)
	require.NoError(t, err)
	require.NotNil(t, existingKeyStorable)
	require.NotNil(t, existingValueStorable)

	// var removedSlabID atree.SlabID

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := atree.UnwrapStorable(existingValueStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case atree.ArraySlab, atree.MapSlab:
		require.Fail(t, "removed storable shouldn't be (wrapped) atree.ArraySlab or atree.MapSlab: %s", existingValueStorable)

	case atree.SlabIDStorable:
		removedSlabID := atree.SlabID(wrappedStorable)

		// Verify atree.SlabID has the same address
		require.Equal(t, m.Address(), removedSlabID.Address())
	}

	// Verify removed value

	existingKey, err := existingKeyStorable.StoredValue(storage)
	require.NoError(t, err)
	testValueEqual(t, key, existingKey)

	removeFromStorage(t, storage, existingKey)

	existingValue, err := existingValueStorable.StoredValue(storage)
	require.NoError(t, err)
	testValueEqual(t, expected, existingValue)

	removeFromStorage(t, storage, existingValue)
}
