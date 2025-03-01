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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
)

func newWrapperValue(
	nestedLevels int,
	wrappedValue atree.Value,
	expectedWrappedValue atree.Value,
) (wrapperValue atree.Value, expectedWrapperValue atree.Value) {

	wrapperValue = test_utils.NewSomeValue(wrappedValue)
	expectedWrapperValue = test_utils.NewExpectedWrapperValue(expectedWrappedValue)

	for i := 1; i < nestedLevels; i++ {
		wrapperValue = test_utils.NewSomeValue(wrapperValue)
		expectedWrapperValue = test_utils.NewExpectedWrapperValue(expectedWrapperValue)
	}

	return
}

func getWrappedValue(t *testing.T, v atree.Value, expected atree.Value) (atree.Value, atree.Value) {
	for {
		sw, vIsSomeValue := v.(test_utils.SomeValue)

		esw, expectedIsSomeValue := expected.(test_utils.ExpectedWrapperValue)

		require.Equal(t, vIsSomeValue, expectedIsSomeValue)

		if !vIsSomeValue {
			break
		}

		v = sw.Value
		expected = esw.Value
	}

	return v, expected
}

type newValueFunc func(atree.SlabStorage) (value atree.Value, expected atree.Value)

var nilValueFunc = func() newValueFunc {
	return func(_ atree.SlabStorage) (atree.Value, atree.Value) {
		return nil, nil
	}
}

var newWrapperValueFunc = func(
	nestedLevels int,
	newWrappedValue newValueFunc,
) newValueFunc {
	return func(storage atree.SlabStorage) (value atree.Value, expected atree.Value) {
		wrappedValue, expectedWrappedValue := newWrappedValue(storage)
		return newWrapperValue(nestedLevels, wrappedValue, expectedWrappedValue)
	}
}

var newRandomUint64ValueFunc = func(r *rand.Rand) newValueFunc {
	return func(atree.SlabStorage) (value atree.Value, expected atree.Value) {
		v := test_utils.Uint64Value(r.Intn(1844674407370955161))
		return v, v
	}
}

var newArrayValueFunc = func(
	t *testing.T,
	address atree.Address,
	typeInfo atree.TypeInfo,
	arrayCount int,
	newValue newValueFunc,
) newValueFunc {
	return func(storage atree.SlabStorage) (value atree.Value, expected atree.Value) {
		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]atree.Value, arrayCount)

		for i := range expectedValues {
			v, expectedV := newValue(storage)

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = expectedV
		}

		return array, test_utils.ExpectedArrayValue(expectedValues)
	}
}

type modifyValueFunc func(atree.SlabStorage, atree.Value, atree.Value) (value atree.Value, expected atree.Value, err error)

var replaceWithNewValueFunc = func(newValue newValueFunc) modifyValueFunc {
	return func(storage atree.SlabStorage, _ atree.Value, _ atree.Value) (atree.Value, atree.Value, error) {
		v, expected := newValue(storage)
		return v, expected, nil
	}
}

var modifyRandomUint64ValueFunc = func(r *rand.Rand) modifyValueFunc {
	return replaceWithNewValueFunc(newRandomUint64ValueFunc(r))
}

var modifyWrapperValueFunc = func(
	t *testing.T,
	nestedLevels int,
	modifyWrappedValue modifyValueFunc,
) modifyValueFunc {
	return func(
		storage atree.SlabStorage,
		v atree.Value,
		expected atree.Value,
	) (modifiedValue atree.Value, expectedModifiedValue atree.Value, err error) {
		wrappedValue, expectedWrappedValue := getWrappedValue(t, v, expected)

		newWrappedValue, expectedNewWrappedValue, err := modifyWrappedValue(storage, wrappedValue, expectedWrappedValue)
		if err != nil {
			return nil, nil, err
		}

		newWrapperValue, expectedNewWrapperValue := newWrapperValue(nestedLevels, newWrappedValue, expectedNewWrappedValue)

		return newWrapperValue, expectedNewWrapperValue, nil
	}
}

var modifyArrayValueFunc = func(
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
		array, ok := originalValue.(*atree.Array)
		require.True(t, ok)

		expectedValues, ok := expectedOrigianlValue.(test_utils.ExpectedArrayValue)
		require.True(t, ok)

		require.Equal(t, uint64(len(expectedValues)), array.Count())
		require.True(t, array.Count() > 0)

		// Modify first element

		index := 0

		v, err := array.Get(uint64(index))
		require.NoError(t, err)

		modifiedV, expectedModifiedV, err := modifyValueFunc(storage, v, expectedValues[index])
		if err != nil {
			return nil, nil, err
		}

		if modifiedV == nil {

			existingStorable, err := array.Remove(uint64(index))
			if err != nil {
				return nil, nil, err
			}
			require.NotNil(t, existingStorable)

			// Verify wrapped storable doesn't contain inlined slab

			wrappedStorable := atree.UnwrapStorable(existingStorable)

			var removedSlabID atree.SlabID

			switch wrappedStorable := wrappedStorable.(type) {
			case atree.ArraySlab, atree.MapSlab:
				require.Fail(t, "removed storable shouldn't be (wrapped) atree.ArraySlab or atree.MapSlab: %s", existingStorable)

			case atree.SlabIDStorable:
				removedSlabID = atree.SlabID(wrappedStorable)

				// Verify SlabID has the same address
				require.Equal(t, array.Address(), removedSlabID.Address())
			}

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, expectedValues[index], existingValue)

			// Remove slabs from storage
			if removedSlabID != atree.SlabIDUndefined {
				err = storage.Remove(removedSlabID)
				require.NoError(t, err)
			}

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)

		} else {

			if needToResetModifiedValue {
				existingStorable, err := array.Set(uint64(index), modifiedV)
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

					// Verify SlabID has the same address
					require.Equal(t, array.Address(), overwrittenSlabID.Address())
				}

				existingValue, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)

				testValueEqual(t, expectedValues[index], existingValue)

				if overwrittenSlabID != atree.SlabIDUndefined {
					// Remove slabs from storage given we are not interested in removed element
					err = storage.Remove(overwrittenSlabID)
					require.NoError(t, err)
				}

				expectedValues[index] = expectedModifiedV
			}
		}

		return array, expectedValues, nil
	}
}

type arrayWrapperValueTestCase struct {
	name                          string
	modifyName                    string
	wrapperValueNestedLevels      int
	mustSetModifiedElementInArray bool
	newElement                    newValueFunc
	modifyElement                 modifyValueFunc
}

func newArrayWrapperValueTestCases(
	t *testing.T,
	r *rand.Rand,
	address atree.Address,
	typeInfo atree.TypeInfo,
) []arrayWrapperValueTestCase {

	return []arrayWrapperValueTestCase{

		// Test arrays [SomeValue(uint64)]
		{
			name:                          "[SomeValue(uint64)]",
			modifyName:                    "modify wrapped primitive",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: true,
			newElement:                    newWrapperValueFunc(1, newRandomUint64ValueFunc(r)),
			modifyElement:                 modifyWrapperValueFunc(t, 1, modifyRandomUint64ValueFunc(r)),
		},

		// Test arrays [SomeValue(SomeValue(uint64))]
		{
			name:                          "[SomeValue(SomeValue(uint64))]",
			modifyName:                    "modify wrapped primitive",
			wrapperValueNestedLevels:      2,
			mustSetModifiedElementInArray: true,
			newElement:                    newWrapperValueFunc(2, newRandomUint64ValueFunc(r)),
			modifyElement:                 modifyWrapperValueFunc(t, 2, modifyRandomUint64ValueFunc(r)),
		},

		// Test arrays [SomeValue([uint64]))]
		{
			name:                          "[SomeValue([uint64])]",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(1, newArrayValueFunc(t, address, typeInfo, 2, newRandomUint64ValueFunc(r))),
			modifyElement:                 modifyWrapperValueFunc(t, 1, modifyArrayValueFunc(t, true, modifyRandomUint64ValueFunc(r))),
		},

		// Test arrays [SomeValue(SomeValue([uint64])))]
		{
			name:                          "[SomeValue(SomeValue([uint64]))]",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      2,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(2, newArrayValueFunc(t, address, typeInfo, 2, newRandomUint64ValueFunc(r))),
			modifyElement:                 modifyWrapperValueFunc(t, 2, modifyArrayValueFunc(t, true, modifyRandomUint64ValueFunc(r))),
		},

		// Test arrays [SomeValue([SomeValue(uint64)]))]
		{
			name:                          "[SomeValue([SomeValue(uint64)])]",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(1, newArrayValueFunc(t, address, typeInfo, 2, newWrapperValueFunc(1, newRandomUint64ValueFunc(r)))),
			modifyElement:                 modifyWrapperValueFunc(t, 1, modifyArrayValueFunc(t, true, modifyWrapperValueFunc(t, 1, modifyRandomUint64ValueFunc(r)))),
		},

		// Test arrays [SomeValue(SomeValue([SomeValue(SomeValue(uint64))])))]
		{
			name:                          "[SomeValue(SomeValue([SomeValue(SomeValue(uint64))]))]",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      2,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(2, newArrayValueFunc(t, address, typeInfo, 2, newWrapperValueFunc(2, newRandomUint64ValueFunc(r)))),
			modifyElement:                 modifyWrapperValueFunc(t, 2, modifyArrayValueFunc(t, true, modifyWrapperValueFunc(t, 2, modifyRandomUint64ValueFunc(r)))),
		},

		// Test arrays [SomeValue([SomeValue([SomeValue(uint64)])]))] and modify innermost array
		{
			name:                          "[SomeValue([SomeValue([SomeValue(uint64)])])]",
			modifyName:                    "modify wrapped level-2 array",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: false,
			newElement: newWrapperValueFunc(
				1,
				newArrayValueFunc(
					t,
					address,
					typeInfo,
					2,
					newWrapperValueFunc(
						1,
						newArrayValueFunc(
							t,
							address,
							typeInfo,
							2,
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))),
			modifyElement: modifyWrapperValueFunc(
				t,
				1,
				modifyArrayValueFunc(
					t,
					false,
					modifyWrapperValueFunc(
						t,
						1,
						modifyArrayValueFunc(
							t,
							true,
							modifyWrapperValueFunc(
								t,
								1,
								modifyRandomUint64ValueFunc(r)))))),
		},

		// Test arrays [SomeValue([SomeValue([SomeValue(uint64)])]))] and remove element from middle array
		{
			name:                          "[SomeValue([SomeValue([SomeValue(uint64)])])]",
			modifyName:                    "remove element from wrapped level-1 array",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: false,
			newElement: newWrapperValueFunc(
				1,
				newArrayValueFunc(
					t,
					address,
					typeInfo,
					2,
					newWrapperValueFunc(
						1,
						newArrayValueFunc(
							t,
							address,
							typeInfo,
							2,
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))),
			modifyElement: modifyWrapperValueFunc(
				t,
				1,
				modifyArrayValueFunc(
					t,
					true,
					replaceWithNewValueFunc(nilValueFunc()))),
		},

		{
			name:                          "[SomeValue([SomeValue([SomeValue(uint64)])])]",
			modifyName:                    "modify element in wrapped level-1 array",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: false,
			newElement: newWrapperValueFunc(
				1,
				newArrayValueFunc(
					t,
					address,
					typeInfo,
					2,
					newWrapperValueFunc(
						1,
						newArrayValueFunc(
							t,
							address,
							typeInfo,
							2,
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))),
			modifyElement: modifyWrapperValueFunc(
				t,
				1,
				modifyArrayValueFunc(
					t,
					true,
					replaceWithNewValueFunc(
						newWrapperValueFunc(
							1,
							newArrayValueFunc(
								t,
								address,
								typeInfo,
								2,
								newWrapperValueFunc(
									1,
									newRandomUint64ValueFunc(r))))))),
		},
	}
}

// TestArrayWrapperValueAppendAndModify tests
//   - appending WrapperValue to array
//   - retrieving WrapperValue from array
//   - modifing retrieved WrapperValue
func TestArrayWrapperValueAppendAndModify(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArrayCount = 10
		largeArrayCount = 512
	)

	arrayCountTestCases := []struct {
		name       string
		arrayCount uint64
	}{
		{name: "small array", arrayCount: smallArrayCount},
		{name: "large array", arrayCount: largeArrayCount},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arrayCountTestCase := range arrayCountTestCases {

			arrayCount := arrayCountTestCase.arrayCount

			name := arrayCountTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += ", " + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				array, err := atree.NewArray(storage, address, typeInfo)
				require.NoError(t, err)

				arraySlabID := array.SlabID()

				// Append WrapperValue to array
				expectedValues := make([]atree.Value, arrayCount)
				for i := range expectedValues {
					v, expectedV := tc.newElement(storage)

					err := array.Append(v)
					require.NoError(t, err)

					expectedValues[i] = expectedV
				}

				require.Equal(t, arrayCount, array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Retrieve and modify WrapperValue from array
				for i := range expectedValues {
					v, err := array.Get(uint64(i))
					require.NoError(t, err)

					expected := expectedValues[i]
					testValueEqual(t, expected, v)

					// Verify that v is WrapperValue
					testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

					// Modify element
					newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
					require.NoError(t, err)

					if tc.mustSetModifiedElementInArray {
						testSetElementInArray(t, storage, array, i, newV, expected)
					}

					expectedValues[i] = newExpectedV
				}

				require.Equal(t, arrayCount, array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load array from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

				array2, err := atree.NewArrayWithRootID(storage2, arraySlabID)
				require.NoError(t, err)
				require.Equal(t, arrayCount, array2.Count())

				// Test loaded array
				testArray(t, storage2, typeInfo, address, array2, expectedValues, true)
			})
		}
	}
}

// TestArrayWrapperValueInsertAndModify tests
// - inserting WrapperValue (in reverse order) to array
// - retrieving WrapperValue from array
// - modifing retrieved WrapperValue
func TestArrayWrapperValueInsertAndModify(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArrayCount = 10
		largeArrayCount = 512
	)

	arrayCountTestCases := []struct {
		name       string
		arrayCount uint64
	}{
		{name: "small array", arrayCount: smallArrayCount},
		{name: "large array", arrayCount: largeArrayCount},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arrayCountTestCase := range arrayCountTestCases {

			arrayCount := arrayCountTestCase.arrayCount

			name := arrayCountTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += "," + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				array, err := atree.NewArray(storage, address, typeInfo)
				require.NoError(t, err)

				arraySlabID := array.SlabID()

				// Insert WrapperValue in reverse order to array
				expectedValues := make([]atree.Value, arrayCount)
				for i := len(expectedValues) - 1; i >= 0; i-- {
					v, expectedV := tc.newElement(storage)

					err := array.Insert(0, v)
					require.NoError(t, err)

					expectedValues[i] = expectedV
				}

				require.Equal(t, arrayCount, array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Retrieve and modify WrapperValue from array
				for i := range expectedValues {
					v, err := array.Get(uint64(i))
					require.NoError(t, err)

					expected := expectedValues[i]
					testValueEqual(t, expected, v)

					// Verify that v is WrapperValue
					testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

					// Modify element
					newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
					require.NoError(t, err)

					if tc.mustSetModifiedElementInArray {
						testSetElementInArray(t, storage, array, i, newV, expected)
					}

					expectedValues[i] = newExpectedV
				}

				require.Equal(t, arrayCount, array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load array from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

				array2, err := atree.NewArrayWithRootID(storage2, arraySlabID)
				require.NoError(t, err)
				require.Equal(t, arrayCount, array2.Count())

				// Test loaded array
				testArray(t, storage2, typeInfo, address, array2, expectedValues, true)
			})
		}
	}
}

// TestArrayWrapperValueSetAndModify tests
// - inserting WrapperValue to array
// - retrieving WrapperValue from array
// - modifing retrieved WrapperValue
// - setting modified WrapperValue
func TestArrayWrapperValueSetAndModify(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArrayCount = 10
		largeArrayCount = 512
	)

	arrayCountTestCases := []struct {
		name       string
		arrayCount uint64
	}{
		{name: "small array", arrayCount: smallArrayCount},
		{name: "large array", arrayCount: largeArrayCount},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arrayCountTestCase := range arrayCountTestCases {

			arrayCount := arrayCountTestCase.arrayCount

			name := arrayCountTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += "," + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				array, err := atree.NewArray(storage, address, typeInfo)
				require.NoError(t, err)

				arraySlabID := array.SlabID()

				// Insert WrapperValue to array
				expectedValues := make([]atree.Value, arrayCount)
				for i := range expectedValues {
					v, expectedV := tc.newElement(storage)

					err := array.Insert(array.Count(), v)
					require.NoError(t, err)

					expectedValues[i] = expectedV
				}

				require.Equal(t, arrayCount, array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Set new WrapperValue in array
				for i := range expectedValues {
					v, expected := tc.newElement(storage)

					testSetElementInArray(t, storage, array, i, v, expectedValues[i])

					expectedValues[i] = expected
				}

				require.Equal(t, arrayCount, array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Retrieve and modify WrapperValue from array
				for i := range expectedValues {
					v, err := array.Get(uint64(i))
					require.NoError(t, err)

					expected := expectedValues[i]
					testValueEqual(t, expected, v)

					// Verify that v is WrapperValue
					testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

					// Modify element
					newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
					require.NoError(t, err)

					if tc.mustSetModifiedElementInArray {
						testSetElementInArray(t, storage, array, i, newV, expected)
					}

					expectedValues[i] = newExpectedV
				}

				require.Equal(t, arrayCount, array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load array from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

				array2, err := atree.NewArrayWithRootID(storage2, arraySlabID)
				require.NoError(t, err)
				require.Equal(t, arrayCount, array2.Count())

				// Test loaded array
				testArray(t, storage2, typeInfo, address, array2, expectedValues, true)
			})
		}
	}
}

// TestArrayWrapperValueInsertAndRemove tests
// - inserting WrapperValue to array
// - remove all elements
// - also test setting new elements before removal
func TestArrayWrapperValueInsertAndRemove(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArrayCount = 10
		largeArrayCount = 512
	)

	arrayCountTestCases := []struct {
		name       string
		arrayCount uint64
	}{
		{name: "small array", arrayCount: smallArrayCount},
		{name: "large array", arrayCount: largeArrayCount},
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
		{name: fmt.Sprintf("remove %d element", smallArrayCount/2), removeElementCount: smallArrayCount / 2},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arrayCountTestCase := range arrayCountTestCases {

			for _, modifyTestCase := range modifyTestCases {

				for _, removeTestCase := range removeTestCases {

					arrayCount := arrayCountTestCase.arrayCount

					needToModifyElement := modifyTestCase.needToModifyElement

					removeCount := removeTestCase.removeElementCount
					if removeTestCase.removeAllElements {
						removeCount = arrayCount
					}

					name := arrayCountTestCase.name + " " + tc.name
					if modifyTestCase.needToModifyElement {
						name += ", " + tc.modifyName
					}
					if removeTestCase.name != "" {
						name += ", " + removeTestCase.name
					}

					t.Run(name, func(t *testing.T) {

						storage := newTestPersistentStorage(t)

						array, err := atree.NewArray(storage, address, typeInfo)
						require.NoError(t, err)

						arraySlabID := array.SlabID()

						// Insert WrapperValue to array
						expectedValues := make([]atree.Value, arrayCount)
						for i := range expectedValues {
							v, expectedV := tc.newElement(storage)

							err := array.Insert(array.Count(), v)
							require.NoError(t, err)

							expectedValues[i] = expectedV
						}

						require.Equal(t, arrayCount, array.Count())

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Retrieve and modify WrapperValue from array
						if needToModifyElement {
							for i := range expectedValues {
								v, err := array.Get(uint64(i))
								require.NoError(t, err)

								expected := expectedValues[i]
								testValueEqual(t, expected, v)

								// Verify that v is WrapperValue
								testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

								// Modify element
								newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
								require.NoError(t, err)

								if tc.mustSetModifiedElementInArray {
									testSetElementInArray(t, storage, array, i, newV, expected)
								}

								expectedValues[i] = newExpectedV
							}

							require.Equal(t, arrayCount, array.Count())

							testArrayMutableElementIndex(t, array)

							testArray(t, storage, typeInfo, address, array, expectedValues, true)
						}

						// Remove random elements
						for range removeCount {

							removeIndex := getRandomArrayIndex(r, array)

							testRemoveElementFromArray(t, storage, array, removeIndex, expectedValues[removeIndex])

							expectedValues = append(expectedValues[:removeIndex], expectedValues[removeIndex+1:]...)
						}

						require.Equal(t, arrayCount-removeCount, array.Count())
						require.Equal(t, arrayCount-removeCount, uint64(len(expectedValues)))

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Commit storage
						err = storage.FastCommit(runtime.NumCPU())
						require.NoError(t, err)

						// Load array from encoded data
						storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

						array2, err := atree.NewArrayWithRootID(storage2, arraySlabID)
						require.NoError(t, err)
						require.Equal(t, arrayCount-removeCount, array2.Count())

						// Test loaded array
						testArray(t, storage2, typeInfo, address, array2, expectedValues, true)
					})
				}
			}
		}
	}
}

// TestArrayWrapperValueSetAndRemove tests
// - inserting WrapperValue to array
// - remove all elements
// - also test setting new elements before removal
func TestArrayWrapperValueSetAndRemove(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArrayCount = 10
		largeArrayCount = 512
	)

	arrayCountTestCases := []struct {
		name       string
		arrayCount uint64
	}{
		{name: "small array", arrayCount: smallArrayCount},
		{name: "large array", arrayCount: largeArrayCount},
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
		{name: fmt.Sprintf("remove %d element", smallArrayCount/2), removeElementCount: smallArrayCount / 2},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arrayCountTestCase := range arrayCountTestCases {

			for _, modifyTestCase := range modifyTestCases {

				for _, removeTestCase := range removeTestCases {

					arrayCount := arrayCountTestCase.arrayCount

					needToModifyElement := modifyTestCase.needToModifyElement

					removeCount := removeTestCase.removeElementCount
					if removeTestCase.removeAllElements {
						removeCount = arrayCount
					}

					name := arrayCountTestCase.name + " " + tc.name
					if modifyTestCase.needToModifyElement {
						name += ", " + tc.modifyName
					}
					if removeTestCase.name != "" {
						name += ", " + removeTestCase.name
					}

					t.Run(name, func(t *testing.T) {

						storage := newTestPersistentStorage(t)

						array, err := atree.NewArray(storage, address, typeInfo)
						require.NoError(t, err)

						arraySlabID := array.SlabID()

						expectedValues := make([]atree.Value, arrayCount)

						// Insert WrapperValue to array
						for i := range expectedValues {
							v, expectedV := tc.newElement(storage)

							err := array.Insert(array.Count(), v)
							require.NoError(t, err)

							expectedValues[i] = expectedV
						}

						require.Equal(t, arrayCount, array.Count())

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Set WrapperValue in array
						for i := range expectedValues {
							v, expectedV := tc.newElement(storage)

							testSetElementInArray(t, storage, array, i, v, expectedValues[i])

							expectedValues[i] = expectedV
						}

						require.Equal(t, arrayCount, array.Count())

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Retrieve and modify WrapperValue from array
						if needToModifyElement {
							for i := range expectedValues {
								v, err := array.Get(uint64(i))
								require.NoError(t, err)

								expected := expectedValues[i]
								testValueEqual(t, expected, v)

								// Verify that v is WrapperValue
								testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

								// Modify element
								newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
								require.NoError(t, err)

								if tc.mustSetModifiedElementInArray {
									testSetElementInArray(t, storage, array, i, newV, expected)
								}

								expectedValues[i] = newExpectedV
							}

							require.Equal(t, arrayCount, array.Count())

							testArrayMutableElementIndex(t, array)

							testArray(t, storage, typeInfo, address, array, expectedValues, true)
						}

						// Remove random elements
						for range removeCount {

							removeIndex := getRandomArrayIndex(r, array)

							testRemoveElementFromArray(t, storage, array, removeIndex, expectedValues[removeIndex])

							expectedValues = append(expectedValues[:removeIndex], expectedValues[removeIndex+1:]...)
						}

						require.Equal(t, arrayCount-removeCount, array.Count())
						require.Equal(t, arrayCount-removeCount, uint64(len(expectedValues)))

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Commit storage
						err = storage.FastCommit(runtime.NumCPU())
						require.NoError(t, err)

						// Load array from encoded data
						storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

						array2, err := atree.NewArrayWithRootID(storage2, arraySlabID)
						require.NoError(t, err)
						require.Equal(t, arrayCount-removeCount, array2.Count())

						// Test loaded array
						testArray(t, storage2, typeInfo, address, array2, expectedValues, true)
					})
				}
			}
		}
	}
}

func TestArrayWrapperValueReadOnlyIterate(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArrayCount = 10
		largeArrayCount = 512
	)

	arrayCountTestCases := []struct {
		name       string
		arrayCount uint64
	}{
		{name: "small array", arrayCount: smallArrayCount},
		{name: "large array", arrayCount: largeArrayCount},
	}

	modifyTestCases := []struct {
		name              string
		testModifyElement bool
	}{
		{name: "modify elements", testModifyElement: true},
		{name: "", testModifyElement: false},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arrayCountTestCase := range arrayCountTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && tc.mustSetModifiedElementInArray {
					continue
				}

				arrayCount := arrayCountTestCase.arrayCount

				testModifyElement := modifyTestCase.testModifyElement

				name := arrayCountTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					array, err := atree.NewArray(storage, address, typeInfo)
					require.NoError(t, err)

					expectedValues := make([]atree.Value, arrayCount)

					// Insert WrapperValue to array
					for i := range expectedValues {
						v, expectedV := tc.newElement(storage)

						err := array.Insert(array.Count(), v)
						require.NoError(t, err)

						expectedValues[i] = expectedV
					}

					require.Equal(t, arrayCount, array.Count())

					testArrayMutableElementIndex(t, array)

					testArray(t, storage, typeInfo, address, array, expectedValues, true)

					iterator, err := array.ReadOnlyIterator()
					require.NoError(t, err)

					count := 0
					for {
						next, err := iterator.Next()
						require.NoError(t, err)

						if next == nil {
							break
						}

						expected := expectedValues[count]

						testWrapperValueLevels(t, tc.wrapperValueNestedLevels, next)

						testValueEqual(t, expected, next)

						// Test modifying elements that don't need to reset in parent container.
						if testModifyElement {
							_, _, err := tc.modifyElement(storage, next, expected)
							var targetErr *atree.ReadOnlyIteratorElementMutationError
							require.ErrorAs(t, err, &targetErr)
						}

						count++
					}

					testArrayMutableElementIndex(t, array)
				})
			}
		}
	}
}

func TestArrayWrapperValueIterate(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArrayCount = 10
		largeArrayCount = 512
	)

	arrayCountTestCases := []struct {
		name       string
		arrayCount uint64
	}{
		{name: "small array", arrayCount: smallArrayCount},
		{name: "large array", arrayCount: largeArrayCount},
	}

	modifyTestCases := []struct {
		name              string
		testModifyElement bool
	}{
		{name: "modify elements", testModifyElement: true},
		{name: "", testModifyElement: false},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arrayCountTestCase := range arrayCountTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				elementIsContainer := !tc.mustSetModifiedElementInArray

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && !elementIsContainer {
					continue
				}

				arrayCount := arrayCountTestCase.arrayCount

				testModifyElement := modifyTestCase.testModifyElement

				name := arrayCountTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					array, err := atree.NewArray(storage, address, typeInfo)
					require.NoError(t, err)

					expectedValues := make([]atree.Value, arrayCount)

					// Insert WrapperValue to array
					for i := range expectedValues {
						v, expectedV := tc.newElement(storage)

						err := array.Insert(array.Count(), v)
						require.NoError(t, err)

						expectedValues[i] = expectedV
					}

					require.Equal(t, arrayCount, array.Count())

					testArrayMutableElementIndex(t, array)

					testArray(t, storage, typeInfo, address, array, expectedValues, true)

					iterator, err := array.Iterator()
					require.NoError(t, err)

					count := 0
					for {
						next, err := iterator.Next()
						require.NoError(t, err)

						if next == nil {
							break
						}

						expected := expectedValues[count]

						testWrapperValueLevels(t, tc.wrapperValueNestedLevels, next)

						testValueEqual(t, expected, next)

						// Test modifying container elements.
						if testModifyElement {
							_, newExpectedV, err := tc.modifyElement(storage, next, expected)
							require.NoError(t, err)

							expectedValues[count] = newExpectedV
						}

						count++
					}

					require.Equal(t, arrayCount, array.Count())

					testArrayMutableElementIndex(t, array)

					testArray(t, storage, typeInfo, address, array, expectedValues, true)
				})
			}
		}
	}
}

func TestArrayWrapperValueInlineArrayAtLevel1(t *testing.T) {

	testLevel1WrappedChildArrayInlined := func(t *testing.T, array *atree.Array, expectedInlined bool) {
		require.True(t, IsArrayRootDataSlab(array))

		elements := atree.GetArrayRootSlabStorables(array)

		require.Equal(t, 1, len(elements))

		storable := elements[0]

		storabeleAsSomeStoable, isSomeStorable := storable.(test_utils.SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorable := storabeleAsSomeStoable.Storable

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

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	storage := newTestPersistentStorage(t)

	var expectedValues test_utils.ExpectedArrayValue

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Append WrapperValue test_utils.SomeValue([]) to array
	{
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		require.False(t, childArray.Inlined())

		err = array.Append(test_utils.NewSomeValue(childArray))
		require.NoError(t, err)

		require.True(t, childArray.Inlined())

		expectedValues = append(expectedValues, test_utils.NewExpectedWrapperValue(test_utils.ExpectedArrayValue{}))

		require.Equal(t, uint64(1), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		testLevel1WrappedChildArrayInlined(t, array, true)
	}

	// Retrieve wrapped child array, and then append new elements to child array.
	// Wrapped child array is expected to be unlined at the end of loop.

	const childArrayCount = uint64(32)
	for i := range childArrayCount {
		// Get element
		element, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedArray, isArray := wrappedValue.(*atree.Array)
		require.True(t, isArray)

		expectedWrappedValue := expectedValues[0].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedArray := expectedWrappedValue.(test_utils.ExpectedArrayValue)

		// Append new elements to wrapped child array

		v := test_utils.Uint64Value(i)

		err = wrappedArray.Append(test_utils.NewSomeValue(v))
		require.NoError(t, err)

		expectedWrappedArray = append(expectedWrappedArray, test_utils.NewExpectedWrapperValue(v))

		expectedValues[0] = test_utils.NewExpectedWrapperValue(expectedWrappedArray)

		require.Equal(t, i+1, wrappedArray.Count())
		require.Equal(t, i+1, uint64(len(expectedWrappedArray)))

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel1WrappedChildArrayInlined(t, array, false)

	// Retrieve wrapped child array, and then remove elements to child array.
	// Wrapped child array is expected to be inlined at the end of loop.

	childArrayCountAfterRemoval := uint64(2)
	removeCount := childArrayCount - childArrayCountAfterRemoval

	for range removeCount {
		// Get element
		element, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedArray, isArray := wrappedValue.(*atree.Array)
		require.True(t, isArray)

		expectedWrappedValue := expectedValues[0].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedArray := expectedWrappedValue.(test_utils.ExpectedArrayValue)

		// Remove first element from wrapped child array

		existingStorable, err := wrappedArray.Remove(0)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)

		// Verify removed value

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, expectedWrappedArray[0], existingValue)

		expectedWrappedArray = expectedWrappedArray[1:]

		expectedValues[0] = test_utils.NewExpectedWrapperValue(expectedWrappedArray)

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel1WrappedChildArrayInlined(t, array, true)

	testArrayMutableElementIndex(t, array)

	testArray(t, storage, typeInfo, address, array, expectedValues, true)
}

func TestArrayWrapperValueInlineArrayAtLevel2(t *testing.T) {

	testLevel2WrappedChildArrayInlined := func(t *testing.T, array *atree.Array, expectedInlined bool) {
		require.True(t, IsArrayRootDataSlab(array))

		elements := atree.GetArrayRootSlabStorables(array)

		require.Equal(t, 1, len(elements))

		// Get unwrapped value at level 1

		storableAtLevel1 := elements[0]

		storabeleAsSomeStoable, isSomeStorable := storableAtLevel1.(test_utils.SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorableAtLevel1 := storabeleAsSomeStoable.Storable

		wrappedArrayAtlevel1, isArray := wrappedStorableAtLevel1.(*atree.ArrayDataSlab)
		require.True(t, isArray)

		// Get unwrapped value at level 2

		storableAtLevel2 := wrappedArrayAtlevel1.ChildStorables()[0]

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

	storage := newTestPersistentStorage(t)

	var expectedValues test_utils.ExpectedArrayValue

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Append WrapperValue test_utils.SomeValue([test_utils.SomeValue[]]) to array
	{
		// Create grand child array
		gchildArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		require.False(t, gchildArray.Inlined())

		// Create child array
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		require.False(t, childArray.Inlined())

		// Append grand child array to child array
		err = childArray.Append(test_utils.NewSomeValue(gchildArray))
		require.NoError(t, err)

		require.True(t, gchildArray.Inlined())

		// Append child array to array
		err = array.Append(test_utils.NewSomeValue(childArray))
		require.NoError(t, err)

		require.True(t, childArray.Inlined())

		expectedValues = append(
			expectedValues,
			test_utils.NewExpectedWrapperValue(test_utils.ExpectedArrayValue{test_utils.NewExpectedWrapperValue(test_utils.ExpectedArrayValue{})}))

		require.Equal(t, uint64(1), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		testLevel2WrappedChildArrayInlined(t, array, true)
	}

	// Retrieve wrapped gchild array, and then append new elements to gchild array.
	// Wrapped gchild array is expected to be unlined at the end of loop.

	const gchildArrayCount = uint64(32)
	for i := range gchildArrayCount {
		// Get element at level 1

		elementAtLevel1, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedArrayAtLevel1, isArray := wrappedValueAtLevel1.(*atree.Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel1 := expectedValues[0].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedArrayAtLevel1 := expectedWrappedValueAtLevel1.(test_utils.ExpectedArrayValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedArrayAtLevel1.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedArrayAtLevel2, isArray := wrappedValueAtLevel2.(*atree.Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel2 := expectedWrappedArrayAtLevel1[0].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedArrayAtLevel2 := expectedWrappedValueAtLevel2.(test_utils.ExpectedArrayValue)

		// Append new elements to wrapped gchild array

		v := test_utils.Uint64Value(i)

		err = wrappedArrayAtLevel2.Append(test_utils.NewSomeValue(v))
		require.NoError(t, err)

		expectedWrappedArrayAtLevel2 = append(expectedWrappedArrayAtLevel2, test_utils.NewExpectedWrapperValue(v))

		expectedValues[0] = test_utils.NewExpectedWrapperValue(
			test_utils.ExpectedArrayValue{
				test_utils.NewExpectedWrapperValue(expectedWrappedArrayAtLevel2)})

		require.Equal(t, i+1, wrappedArrayAtLevel2.Count())
		require.Equal(t, i+1, uint64(len(expectedWrappedArrayAtLevel2)))

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel2WrappedChildArrayInlined(t, array, false)

	// Retrieve wrapped gchild array, and then remove elements from gchild array.
	// Wrapped gchild array is expected to be inlined at the end of loop.

	gchildArrayCountAfterRemoval := uint64(2)
	removeCount := gchildArrayCount - gchildArrayCountAfterRemoval

	for range removeCount {
		// Get elementAtLevel1
		elementAtLevel1, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedArrayAtLevel1, isArray := wrappedValueAtLevel1.(*atree.Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel1 := expectedValues[0].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedArrayAtLevel1 := expectedWrappedValueAtLevel1.(test_utils.ExpectedArrayValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedArrayAtLevel1.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedArrayAtLevel2, isArray := wrappedValueAtLevel2.(*atree.Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel2 := expectedWrappedArrayAtLevel1[0].(test_utils.ExpectedWrapperValue).Value

		expectedWrappedArrayAtLevel2 := expectedWrappedValueAtLevel2.(test_utils.ExpectedArrayValue)

		// Remove first element from wrapped gchild array

		existingStorable, err := wrappedArrayAtLevel2.Remove(0)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)

		// Verify removed value

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, expectedWrappedArrayAtLevel2[0], existingValue)

		expectedWrappedArrayAtLevel2 = expectedWrappedArrayAtLevel2[1:]

		expectedValues[0] = test_utils.NewExpectedWrapperValue(
			test_utils.ExpectedArrayValue{
				test_utils.NewExpectedWrapperValue(expectedWrappedArrayAtLevel2)})

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel2WrappedChildArrayInlined(t, array, true)

	testArrayMutableElementIndex(t, array)

	testArray(t, storage, typeInfo, address, array, expectedValues, true)
}

func TestArrayWrapperValueModifyNewArrayAtLevel1(t *testing.T) {

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

		// test_utils.SomeValue([test_utils.SomeValue(uint64)])
		newWrapperValueFunc(
			1,
			newArrayValueFunc(
				t,
				address,
				typeInfo,
				r.Intn(4),
				newWrapperValueFunc(
					1,
					newRandomUint64ValueFunc(r)))),

		// test_utils.SomeValue([test_utils.SomeValue([test_utils.SomeValue(uint64)])])
		newWrapperValueFunc(
			1,
			newArrayValueFunc(
				t,
				address,
				typeInfo,
				r.Intn(4),
				newWrapperValueFunc(
					1,
					newArrayValueFunc(
						t,
						address,
						typeInfo,
						r.Intn(4),
						newWrapperValueFunc(
							1,
							newRandomUint64ValueFunc(r)))))),
	}

	storage := newTestPersistentStorage(t)

	var expectedValues test_utils.ExpectedArrayValue

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	actualArrayCount := uint64(0)

	t.Run("append and remove", func(t *testing.T) {

		// Append elements

		appendCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualArrayCount += appendCount

		for range appendCount {
			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			err = array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, expected)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove at least half of elements

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count() / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		removeIndex := getRandomArrayIndexes(r, array, int(removeCount))

		sort.Sort(sort.Reverse(uint64Slice(removeIndex)))

		for _, index := range removeIndex {
			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("insert and remove", func(t *testing.T) {
		// Insert elements

		insertCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualArrayCount += insertCount

		lowestInsertIndex := array.Count()

		for range insertCount {
			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			index := getRandomArrayIndex(r, array)

			if index < lowestInsertIndex {
				lowestInsertIndex = index
			}

			err = array.Insert(index, v)
			require.NoError(t, err)

			newExpectedValue := make([]atree.Value, len(expectedValues)+1)

			copy(newExpectedValue, expectedValues[:index])
			newExpectedValue[index] = expected
			copy(newExpectedValue[index+1:], expectedValues[index:])

			expectedValues = newExpectedValue
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including one previously inserted element)

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count() / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		// Remove previously inserted element first

		{
			index := lowestInsertIndex

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		// Remove more elements

		for i := uint64(1); i < removeCount; i++ {
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("set and remove", func(t *testing.T) {
		// Set elements

		setCount := array.Count()
		if array.Count() > 10 {
			setCount = getRandomUint64InRange(r, array.Count()/2, array.Count()+1)
		}

		setIndex := make([]uint64, 0, setCount)

		for range setCount {
			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			index := getRandomArrayIndex(r, array)

			testSetElementInArray(t, storage, array, int(index), v, expectedValues[index])

			expectedValues[index] = expected

			setIndex = append(setIndex, index)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including some previously set elements)

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count() / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		// Remove some previously set elements first

		// Reverse sort and deduplicate set index
		sort.Sort(sort.Reverse(uint64Slice(setIndex)))

		prev := setIndex[0]
		for i := 1; i < len(setIndex); {
			cur := setIndex[i]

			if prev != cur {
				prev = cur
				i++
			} else {
				setIndex = append(setIndex[:i], setIndex[i+1:]...)
			}
		}

		removeSetCount := removeCount / 2
		if uint64(len(setIndex)) < removeSetCount {
			removeSetCount = uint64(len(setIndex))
		}

		for _, index := range setIndex[:removeSetCount] {
			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		for range removeCount - removeSetCount {
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		for array.Count() > 0 {
			// Remove element at random index
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(0), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayWrapperValueModifyNewArrayAtLevel2(t *testing.T) {

	const (
		minWriteOperationCount = 124
		maxWriteOperationCount = 256
	)

	r := newRand(t)

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	// newValue creates value of type test_utils.SomeValue([test_utils.SomeValue(uint64)]).
	newValue :=
		newWrapperValueFunc(
			1,
			newArrayValueFunc(
				t,
				address,
				typeInfo,
				r.Intn(4)+1, // at least one element
				newWrapperValueFunc(
					1,
					newRandomUint64ValueFunc(r))))

	// modifyValue modifies nested array's first element.
	modifyValue :=
		modifyWrapperValueFunc(
			t,
			1,
			modifyArrayValueFunc(
				t,
				true,
				modifyWrapperValueFunc(
					t,
					1,
					modifyRandomUint64ValueFunc(r))))

	storage := newTestPersistentStorage(t)

	var expectedValues test_utils.ExpectedArrayValue

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	actualArrayCount := uint64(0)

	t.Run("append and remove", func(t *testing.T) {

		// Append elements

		appendCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualArrayCount += appendCount

		for range appendCount {
			v, expected := newValue(storage)

			err = array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, expected)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count() / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		removeIndex := getRandomArrayIndexes(r, array, int(removeCount))

		sort.Sort(sort.Reverse(uint64Slice(removeIndex)))

		for _, index := range removeIndex {
			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("insert and remove", func(t *testing.T) {
		// Insert elements

		insertCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualArrayCount += insertCount

		lowestInsertIndex := array.Count()

		for range insertCount {
			v, expected := newValue(storage)

			index := getRandomArrayIndex(r, array)

			if index < lowestInsertIndex {
				lowestInsertIndex = index
			}

			err = array.Insert(index, v)
			require.NoError(t, err)

			newExpectedValue := make([]atree.Value, len(expectedValues)+1)

			copy(newExpectedValue, expectedValues[:index])
			newExpectedValue[index] = expected
			copy(newExpectedValue[index+1:], expectedValues[index:])

			expectedValues = newExpectedValue
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including one previously inserted element)

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count() / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		// Remove previously inserted element first

		{
			index := lowestInsertIndex

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		// Remove more elements

		for i := 1; i < int(removeCount); i++ {
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		setCount := array.Count()
		if array.Count() > 10 {
			setCount = getRandomUint64InRange(r, array.Count()/2, array.Count()+1)
		}

		setIndex := make([]uint64, 0, setCount)

		for range setCount {

			index := getRandomArrayIndex(r, array)

			// Get element
			originalValue, err := array.Get(index)
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(test_utils.SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[index])
			require.NoError(t, err)

			expectedValues[index] = modifiedExpectedValue

			setIndex = append(setIndex, index)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including some previously set elements)

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count() / 4 * 3
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		// Remove some previously set elements first

		// Reverse sort and deduplicate set index
		sort.Sort(sort.Reverse(uint64Slice(setIndex)))

		prev := setIndex[0]
		for i := 1; i < len(setIndex); {
			cur := setIndex[i]

			if prev != cur {
				prev = cur
				i++
			} else {
				setIndex = append(setIndex[:i], setIndex[i+1:]...)
			}
		}

		removeSetCount := removeCount / 2
		if uint64(len(setIndex)) < removeSetCount {
			removeSetCount = uint64(len(setIndex))
		}

		for _, index := range setIndex[:removeSetCount] {
			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		for range removeCount - removeSetCount {
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		for array.Count() > 0 {
			// Remove element at random index
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(0), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayWrapperValueModifyNewArrayAtLevel3(t *testing.T) {

	const (
		minWriteOperationCount = 124
		maxWriteOperationCount = 256
	)

	r := newRand(t)

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := test_utils.NewSimpleTypeInfo(42)

	// newValue creates value of type test_utils.SomeValue([test_utils.SomeValue([test_utils.SomeValue(uint64)])]))
	newValue :=
		newWrapperValueFunc(
			1,
			newArrayValueFunc(
				t,
				address,
				typeInfo,
				2,
				newWrapperValueFunc(
					1,
					newArrayValueFunc(
						t,
						address,
						typeInfo,
						2,
						newWrapperValueFunc(
							1,
							newRandomUint64ValueFunc(r))))))

	// modifyValue modifies innermost nested array's first element.
	modifyValue :=
		modifyWrapperValueFunc(
			t,
			1,
			modifyArrayValueFunc(
				t,
				false,
				modifyWrapperValueFunc(
					t,
					1,
					modifyArrayValueFunc(
						t,
						true,
						modifyWrapperValueFunc(
							t,
							1,
							modifyRandomUint64ValueFunc(r))))))

	storage := newTestPersistentStorage(t)

	var expectedValues test_utils.ExpectedArrayValue

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	actualArrayCount := uint64(0)

	t.Run("append and remove", func(t *testing.T) {

		// Append elements

		appendCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualArrayCount += appendCount

		for range appendCount {
			v, expected := newValue(storage)

			err = array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, expected)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements

		arrayCount := array.Count()

		removeCount := getRandomUint64InRange(r, arrayCount/2, arrayCount)

		actualArrayCount -= removeCount

		removeIndex := getRandomArrayIndexes(r, array, int(removeCount))

		sort.Sort(sort.Reverse(uint64Slice(removeIndex)))

		for _, index := range removeIndex {
			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("insert and remove", func(t *testing.T) {
		// Insert elements

		insertCount := getRandomUint64InRange(r, minWriteOperationCount, maxWriteOperationCount+1)

		actualArrayCount += insertCount

		lowestInsertIndex := array.Count()

		for range insertCount {
			v, expected := newValue(storage)

			index := getRandomArrayIndex(r, array)

			if index < lowestInsertIndex {
				lowestInsertIndex = index
			}

			err = array.Insert(index, v)
			require.NoError(t, err)

			newExpectedValue := make([]atree.Value, len(expectedValues)+1)

			copy(newExpectedValue, expectedValues[:index])
			newExpectedValue[index] = expected
			copy(newExpectedValue[index+1:], expectedValues[index:])

			expectedValues = newExpectedValue
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including one previously inserted element)

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count()
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		// Remove previously inserted element first

		{
			index := lowestInsertIndex

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		// Remove more elements

		for range removeCount - 1 {
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		setCount := array.Count()
		if array.Count() > 10 {
			setCount = getRandomUint64InRange(r, array.Count()/2, array.Count()+1)
		}

		setIndex := make([]uint64, 0, setCount)

		for range setCount {

			index := getRandomArrayIndex(r, array)

			// Get element
			originalValue, err := array.Get(index)
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(test_utils.SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[index])
			require.NoError(t, err)

			expectedValues[index] = modifiedExpectedValue

			setIndex = append(setIndex, index)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including some previously set elements)

		minRemoveCount := array.Count() / 2
		maxRemoveCount := array.Count()
		removeCount := getRandomUint64InRange(r, minRemoveCount, maxRemoveCount)

		actualArrayCount -= removeCount

		// Remove some previously set elements first

		// Reverse sort and deduplicate set index
		sort.Sort(sort.Reverse(uint64Slice(setIndex)))

		prev := setIndex[0]
		for i := 1; i < len(setIndex); {
			cur := setIndex[i]

			if prev != cur {
				prev = cur
				i++
			} else {
				setIndex = append(setIndex[:i], setIndex[i+1:]...)
			}
		}

		removeSetCount := removeCount / 2
		if uint64(len(setIndex)) < removeSetCount {
			removeSetCount = uint64(len(setIndex))
		}

		for _, index := range setIndex[:removeSetCount] {
			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		for range removeCount - removeSetCount {
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, actualArrayCount, array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		for array.Count() > 0 {
			// Remove element at random index
			index := getRandomArrayIndex(r, array)

			testRemoveElementFromArray(t, storage, array, index, expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(0), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayWrapperValueModifyExistingArray(t *testing.T) {

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("modify level-1 wrapper array in [test_utils.SomeValue([test_utils.SomeValue(uint64)])]", func(t *testing.T) {
		const (
			arrayCount      = 3
			childArrayCount = 2
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		r := newRand(t)

		createStorage := func(arrayCount int) (
			_ atree.BaseStorage,
			rootSlabID atree.SlabID,
			expectedValues []atree.Value,
		) {
			storage := newTestPersistentStorage(t)

			createArrayOfSomeValueOfArrayOfSomeValueOfUint64 :=
				newArrayValueFunc(
					t,
					address,
					typeInfo,
					arrayCount,
					newWrapperValueFunc(
						1,
						newArrayValueFunc(
							t,
							address,
							typeInfo,
							childArrayCount,
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))

			v, expected := createArrayOfSomeValueOfArrayOfSomeValueOfUint64(storage)

			array := v.(*atree.Array)
			expectedValues = expected.(test_utils.ExpectedArrayValue)

			testArray(t, storage, typeInfo, address, array, expectedValues, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return atree.GetBaseStorage(storage), array.SlabID(), expectedValues
		}

		// Create a base storage with array in the format of
		// [test_utils.SomeValue([test_utils.SomeValue(uint64)])]
		baseStorage, rootSlabID, expectedValues := createStorage(arrayCount)
		require.Equal(t, arrayCount, len(expectedValues))

		// Create a new storage with encoded array
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing array from storage
		array, err := atree.NewArrayWithRootID(storage, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array.Count())

		// Get and verify first element as test_utils.SomeValue(array)

		expectedValue := expectedValues[0]

		// Get array element (test_utils.SomeValue)
		element, err := array.Get(uint64(0))
		require.NoError(t, err)

		// Test retrieved element type and value

		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildArray, isArray := elementAsSomeValue.Value.(*atree.Array)
		require.True(t, isArray)

		expectedValuesAsSomeValue, isSomeValue := expectedValue.(test_utils.ExpectedWrapperValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildArray, isArrayValue := expectedValuesAsSomeValue.Value.(test_utils.ExpectedArrayValue)
		require.True(t, isArrayValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildArray)), unwrappedChildArray.Count())

		// Modify wrapped child array of test_utils.SomeValue

		newValue := test_utils.NewStringValue("x")
		err = unwrappedChildArray.Append(test_utils.NewSomeValue(newValue))
		require.NoError(t, err)

		expectedUnwrappedChildArray = append(
			expectedUnwrappedChildArray,
			test_utils.NewExpectedWrapperValue(newValue))
		expectedValues[0] = test_utils.NewExpectedWrapperValue(expectedUnwrappedChildArray)

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child array of test_utils.SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

		// Load existing array from storage
		array2, err := atree.NewArrayWithRootID(storage2, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array2.Count())

		testArray(t, storage, typeInfo, address, array2, expectedValues, true)
	})

	t.Run("modify 2-level wrapper array in [test_utils.SomeValue([test_utils.SomeValue([test_utils.SomeValue(uint64)])])]", func(t *testing.T) {
		const (
			arrayCount       = 4
			childArrayCount  = 3
			gchildArrayCount = 2
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		createStorage := func(arrayCount int) (
			_ atree.BaseStorage,
			rootSlabID atree.SlabID,
			expectedValues []atree.Value,
		) {
			storage := newTestPersistentStorage(t)

			r := newRand(t)

			createArrayOfSomeValueOfArrayOfSomeValueOfArrayOfSomeValueOfUint64 :=
				newArrayValueFunc(
					t,
					address,
					typeInfo,
					arrayCount,
					newWrapperValueFunc(
						1,
						newArrayValueFunc(
							t,
							address,
							typeInfo,
							childArrayCount,
							newWrapperValueFunc(
								1,
								newArrayValueFunc(
									t,
									address,
									typeInfo,
									gchildArrayCount,
									newWrapperValueFunc(
										1,
										newRandomUint64ValueFunc(r)))))))

			v, expected := createArrayOfSomeValueOfArrayOfSomeValueOfArrayOfSomeValueOfUint64(storage)

			array := v.(*atree.Array)
			expectedValues = expected.(test_utils.ExpectedArrayValue)

			testArray(t, storage, typeInfo, address, array, expectedValues, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return atree.GetBaseStorage(storage), array.SlabID(), expectedValues
		}

		// Create a base storage with array in the format of
		// [test_utils.SomeValue([test_utils.SomeValue([test_utils.SomeValue(uint64)])])]
		baseStorage, rootSlabID, expectedValues := createStorage(arrayCount)
		require.Equal(t, arrayCount, len(expectedValues))

		// Create a new storage with encoded array
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing array from storage
		array, err := atree.NewArrayWithRootID(storage, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array.Count())

		// Get and verify first element as test_utils.SomeValue(array)

		expectedValue := expectedValues[0]

		element, err := array.Get(uint64(0))
		require.NoError(t, err)

		elementAsSomeValue, isSomeValue := element.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		unwrappedChildArray, isArray := elementAsSomeValue.Value.(*atree.Array)
		require.True(t, isArray)

		expectedValuesAsSomeValue, isSomeValue := expectedValue.(test_utils.ExpectedWrapperValue)
		require.True(t, isSomeValue)

		expectedUnwrappedChildArray, isArrayValue := expectedValuesAsSomeValue.Value.(test_utils.ExpectedArrayValue)
		require.True(t, isArrayValue)

		require.Equal(t, uint64(len(expectedUnwrappedChildArray)), unwrappedChildArray.Count())

		// Get and verify nested child element as test_utils.SomeValue(array)

		childArrayElement, err := unwrappedChildArray.Get(uint64(0))
		require.NoError(t, err)

		childArrayElementAsSomeValue, isSomeValue := childArrayElement.(test_utils.SomeValue)
		require.True(t, isSomeValue)

		unwrappedGChildArray, isArray := childArrayElementAsSomeValue.Value.(*atree.Array)
		require.True(t, isArray)

		expectedChildValuesAsSomeValue, isSomeValue := expectedUnwrappedChildArray[0].(test_utils.ExpectedWrapperValue)
		require.True(t, isSomeValue)

		expectedUnwrappedGChildArray, isArrayValue := expectedChildValuesAsSomeValue.Value.(test_utils.ExpectedArrayValue)
		require.True(t, isArrayValue)

		require.Equal(t, uint64(len(expectedUnwrappedGChildArray)), unwrappedGChildArray.Count())

		// Modify wrapped gchild array of test_utils.SomeValue

		newValue := test_utils.NewStringValue("x")
		err = unwrappedGChildArray.Append(test_utils.NewSomeValue(newValue))
		require.NoError(t, err)

		expectedUnwrappedGChildArray = append(
			expectedUnwrappedGChildArray,
			test_utils.NewExpectedWrapperValue(newValue))
		expectedValues[0].(test_utils.ExpectedWrapperValue).Value.(test_utils.ExpectedArrayValue)[0] = test_utils.NewExpectedWrapperValue(expectedUnwrappedGChildArray)

		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		// Verify modified wrapped child array of test_utils.SomeValue using new storage with committed data

		storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

		// Load existing array from storage
		array2, err := atree.NewArrayWithRootID(storage2, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array2.Count())

		testArray(t, storage, typeInfo, address, array2, expectedValues, true)
	})
}

func testWrapperValueLevels(t *testing.T, expectedNestedLevels int, v atree.Value) {
	nestedLevels := 0
	for {
		sw, ok := v.(test_utils.SomeValue)
		if !ok {
			break
		}
		v = sw.Value
		nestedLevels++
	}
	require.Equal(t, expectedNestedLevels, nestedLevels)
}

func testArrayMutableElementIndex(t *testing.T, v atree.Value) {
	v, _ = atree.UnwrapValue(v)

	array, ok := v.(*atree.Array)
	if !ok {
		return
	}

	originalMutableIndex := make(map[atree.ValueID]uint64)

	for vid, index := range atree.GetArrayMutableElementIndex(array) {
		originalMutableIndex[vid] = index
	}

	for i := range array.Count() {
		element, err := array.Get(i)
		require.NoError(t, err)

		element, _ = atree.UnwrapValue(element)

		switch element := element.(type) {
		case *atree.Array:
			vid := element.ValueID()
			index, exists := originalMutableIndex[vid]
			require.True(t, exists)
			require.Equal(t, i, index)

			delete(originalMutableIndex, vid)

		case *atree.OrderedMap:
			vid := element.ValueID()
			index, exists := originalMutableIndex[vid]
			require.True(t, exists)
			require.Equal(t, i, index)

			delete(originalMutableIndex, vid)
		}
	}

	require.Equal(t, 0, len(originalMutableIndex))
}

func testSetElementInArray(t *testing.T, storage atree.SlabStorage, array *atree.Array, index int, newValue atree.Value, expected atree.Value) {
	existingStorable, err := array.Set(uint64(index), newValue)
	require.NoError(t, err)
	require.NotNil(t, existingStorable)

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := atree.UnwrapStorable(existingStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case atree.ArraySlab, atree.MapSlab:
		require.Fail(t, "overwritten storable shouldn't be (wrapped) atree.ArraySlab or atree.MapSlab: %s", existingStorable)

	case atree.SlabIDStorable:
		overwrittenSlabID := atree.SlabID(wrappedStorable)

		// Verify SlabID has the same address
		require.Equal(t, array.Address(), overwrittenSlabID.Address())
	}

	// Verify overwritten value

	existingValue, err := existingStorable.StoredValue(storage)
	require.NoError(t, err)
	testValueEqual(t, expected, existingValue)

	removeFromStorage(t, storage, existingValue)
}

func testRemoveElementFromArray(t *testing.T, storage atree.SlabStorage, array *atree.Array, index uint64, expected atree.Value) {
	existingStorable, err := array.Remove(index)
	require.NoError(t, err)
	require.NotNil(t, existingStorable)

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := atree.UnwrapStorable(existingStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case atree.ArraySlab, atree.MapSlab:
		require.Fail(t, "removed storable shouldn't be (wrapped) atree.ArraySlab or atree.MapSlab: %s", existingStorable)

	case atree.SlabIDStorable:
		removedSlabID := atree.SlabID(wrappedStorable)

		// Verify SlabID has the same address
		require.Equal(t, array.Address(), removedSlabID.Address())
	}

	// Verify removed value

	existingValue, err := existingStorable.StoredValue(storage)
	require.NoError(t, err)
	testValueEqual(t, expected, existingValue)

	removeFromStorage(t, storage, existingValue)
}

func removeFromStorage(t *testing.T, storage atree.SlabStorage, v atree.Value) {
	switch v := v.(type) {
	case *atree.Array:
		rootSlabID := v.SlabID()

		// Remove all elements from storage
		for v.Count() > 0 {
			existingStorable, err := v.Remove(uint64(0))
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)

			removeFromStorage(t, storage, existingValue)
		}

		// Remove root slab from storage
		err := storage.Remove(rootSlabID)
		require.NoError(t, err)

	case *atree.OrderedMap:
		rootSlabID := v.SlabID()

		keys := make([]atree.Value, 0, v.Count())
		err := v.IterateReadOnlyKeys(func(key atree.Value) (bool, error) {
			keys = append(keys, key)
			return true, nil
		})
		require.NoError(t, err)

		for _, key := range keys {
			existingKeyStorable, existingValueStorable, err := v.Remove(test_utils.CompareValue, test_utils.GetHashInput, key)
			require.NoError(t, err)

			existingKey, err := existingKeyStorable.StoredValue(storage)
			require.NoError(t, err)

			removeFromStorage(t, storage, existingKey)

			existingValue, err := existingValueStorable.StoredValue(storage)
			require.NoError(t, err)

			removeFromStorage(t, storage, existingValue)
		}

		// Remove root slab from storage
		err = storage.Remove(rootSlabID)
		require.NoError(t, err)

	case atree.WrapperValue:
		wrappedValue, _ := v.UnwrapAtreeValue()
		removeFromStorage(t, storage, wrappedValue)
	}
}
