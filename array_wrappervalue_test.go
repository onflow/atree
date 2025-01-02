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
	"math"
	"math/rand"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func newWrapperValue(
	nestedLevels int,
	wrappedValue Value,
	expectedWrappedValue Value,
) (wrapperValue Value, expectedWrapperValue Value) {

	wrapperValue = SomeValue{wrappedValue}
	expectedWrapperValue = someValue{expectedWrappedValue}

	for i := 1; i < nestedLevels; i++ {
		wrapperValue = SomeValue{wrapperValue}
		expectedWrapperValue = someValue{expectedWrapperValue}
	}

	return
}

func getWrappedValue(t *testing.T, v Value, expected Value) (Value, Value) {
	for {
		sw, vIsSomeValue := v.(SomeValue)

		esw, expectedIsSomeValue := expected.(someValue)

		require.Equal(t, vIsSomeValue, expectedIsSomeValue)

		if !vIsSomeValue {
			break
		}

		v = sw.Value
		expected = esw.Value
	}

	return v, expected
}

type newValueFunc func(SlabStorage) (value Value, expected Value)

var nilValueFunc = func() newValueFunc {
	return func(_ SlabStorage) (Value, Value) {
		return nil, nil
	}
}

var newWrapperValueFunc = func(
	nestedLevels int,
	newWrappedValue newValueFunc,
) newValueFunc {
	return func(storage SlabStorage) (value Value, expected Value) {
		wrappedValue, expectedWrappedValue := newWrappedValue(storage)
		return newWrapperValue(nestedLevels, wrappedValue, expectedWrappedValue)
	}
}

var newRandomUint64ValueFunc = func(r *rand.Rand) newValueFunc {
	return func(SlabStorage) (value Value, expected Value) {
		v := Uint64Value(r.Intn(1844674407370955161))
		return v, v
	}
}

var newArrayValueFunc = func(
	t *testing.T,
	address Address,
	typeInfo TypeInfo,
	arraySize int,
	newValue newValueFunc,
) newValueFunc {
	return func(storage SlabStorage) (value Value, expected Value) {
		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make([]Value, arraySize)

		for i := 0; i < arraySize; i++ {
			v, expectedV := newValue(storage)

			err := array.Append(v)
			require.NoError(t, err)

			expectedValues[i] = expectedV
		}

		return array, arrayValue(expectedValues)
	}
}

type modifyValueFunc func(SlabStorage, Value, Value) (value Value, expected Value, err error)

var replaceWithNewValueFunc = func(newValue newValueFunc) modifyValueFunc {
	return func(storage SlabStorage, _ Value, _ Value) (Value, Value, error) {
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
		storage SlabStorage,
		v Value,
		expected Value,
	) (modifiedValue Value, expectedModifiedValue Value, err error) {
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
		storage SlabStorage,
		originalValue Value,
		expectedOrigianlValue Value,
	) (
		modifiedValue Value,
		expectedModifiedValue Value,
		err error,
	) {
		array, ok := originalValue.(*Array)
		require.True(t, ok)

		expectedValues, ok := expectedOrigianlValue.(arrayValue)
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

			wrappedStorable := unwrapStorable(existingStorable)

			var removedSlabID SlabID

			switch wrappedStorable := wrappedStorable.(type) {
			case ArraySlab, MapSlab:
				require.Fail(t, "removed storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingStorable)

			case SlabIDStorable:
				removedSlabID = SlabID(wrappedStorable)

				// Verify SlabID has the same address
				require.Equal(t, array.Address(), removedSlabID.Address())
			}

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, expectedValues[index], existingValue)

			// Remove slabs from storage
			if removedSlabID != SlabIDUndefined {
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

				wrappedStorable := unwrapStorable(existingStorable)

				var overwrittenSlabID SlabID

				switch wrappedStorable := wrappedStorable.(type) {
				case ArraySlab, MapSlab:
					require.Fail(t, "overwritten storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingStorable)

				case SlabIDStorable:
					overwrittenSlabID = SlabID(wrappedStorable)

					// Verify SlabID has the same address
					require.Equal(t, array.Address(), overwrittenSlabID.Address())
				}

				existingValue, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)

				valueEqual(t, expectedValues[index], existingValue)

				if overwrittenSlabID != SlabIDUndefined {
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
	address Address,
	typeInfo TypeInfo,
) []arrayWrapperValueTestCase {

	return []arrayWrapperValueTestCase{

		// Test arrays of SomeValue(uint64)
		{
			name:                          "SomeValue(uint64)",
			modifyName:                    "modify wrapped primitive",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: true,
			newElement:                    newWrapperValueFunc(1, newRandomUint64ValueFunc(r)),
			modifyElement:                 modifyWrapperValueFunc(t, 1, modifyRandomUint64ValueFunc(r)),
		},

		// Test arrays of SomeValue(SomeValue(uint64))
		{
			name:                          "SomeValue(SomeValue(uint64))",
			modifyName:                    "modify wrapped primitive",
			wrapperValueNestedLevels:      2,
			mustSetModifiedElementInArray: true,
			newElement:                    newWrapperValueFunc(2, newRandomUint64ValueFunc(r)),
			modifyElement:                 modifyWrapperValueFunc(t, 2, modifyRandomUint64ValueFunc(r)),
		},

		// Test arrays of SomeValue([uint64]))
		{
			name:                          "SomeValue([uint64])",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(1, newArrayValueFunc(t, address, typeInfo, 2, newRandomUint64ValueFunc(r))),
			modifyElement:                 modifyWrapperValueFunc(t, 1, modifyArrayValueFunc(t, true, modifyRandomUint64ValueFunc(r))),
		},

		// Test arrays of SomeValue(SomeValue([uint64])))
		{
			name:                          "SomeValue(SomeValue([uint64]))",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      2,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(2, newArrayValueFunc(t, address, typeInfo, 2, newRandomUint64ValueFunc(r))),
			modifyElement:                 modifyWrapperValueFunc(t, 2, modifyArrayValueFunc(t, true, modifyRandomUint64ValueFunc(r))),
		},

		// Test arrays of SomeValue([SomeValue(uint64)]))
		{
			name:                          "SomeValue([SomeValue(uint64)])",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      1,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(1, newArrayValueFunc(t, address, typeInfo, 2, newWrapperValueFunc(1, newRandomUint64ValueFunc(r)))),
			modifyElement:                 modifyWrapperValueFunc(t, 1, modifyArrayValueFunc(t, true, modifyWrapperValueFunc(t, 1, modifyRandomUint64ValueFunc(r)))),
		},

		// Test arrays of SomeValue(SomeValue([SomeValue(SomeValue(uint64))])))
		{
			name:                          "SomeValue(SomeValue([SomeValue(SomeValue(uint64))]))",
			modifyName:                    "modify wrapped array",
			wrapperValueNestedLevels:      2,
			mustSetModifiedElementInArray: false,
			newElement:                    newWrapperValueFunc(2, newArrayValueFunc(t, address, typeInfo, 2, newWrapperValueFunc(2, newRandomUint64ValueFunc(r)))),
			modifyElement:                 modifyWrapperValueFunc(t, 2, modifyArrayValueFunc(t, true, modifyWrapperValueFunc(t, 2, modifyRandomUint64ValueFunc(r)))),
		},

		// Test arrays of SomeValue([SomeValue([SomeValue(uint64)])])) and modify innermost array
		{
			name:                          "SomeValue([SomeValue([SomeValue(uint64)])])",
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

		// Test arrays of SomeValue([SomeValue([SomeValue(uint64)])])) and remove element from middle array
		{
			name:                          "SomeValue([SomeValue([SomeValue(uint64)])])",
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
			name:                          "SomeValue([SomeValue([SomeValue(uint64)])])",
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
//   - retrieveing WrapperValue from array
//   - modifing retrieved WrapperValue
func TestArrayWrapperValueAppendAndModify(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArraySize = 10
		largeArraySize = 512
	)

	arraySizeTestCases := []struct {
		name      string
		arraySize int
	}{
		{name: "small array", arraySize: smallArraySize},
		{name: "large array", arraySize: largeArraySize},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arraySizeTestCase := range arraySizeTestCases {

			arraySize := arraySizeTestCase.arraySize

			name := arraySizeTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += ", " + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				array, err := NewArray(storage, address, typeInfo)
				require.NoError(t, err)

				arraySlabID := array.SlabID()

				// Append WrapperValue to array
				expectedValues := make([]Value, arraySize)
				for i := 0; i < arraySize; i++ {
					v, expectedV := tc.newElement(storage)

					err := array.Append(v)
					require.NoError(t, err)

					expectedValues[i] = expectedV
				}

				require.Equal(t, uint64(arraySize), array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Retrieve and modify WrapperValue from array
				for i := uint64(0); i < array.Count(); i++ {
					v, err := array.Get(i)
					require.NoError(t, err)

					expected := expectedValues[i]
					valueEqual(t, expected, v)

					// Verify that v is WrapperValue
					testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

					// Modify element
					newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
					require.NoError(t, err)

					if tc.mustSetModifiedElementInArray {
						testSetElementFromArray(t, storage, array, i, newV, expected)
					}

					expectedValues[i] = newExpectedV
				}

				require.Equal(t, uint64(arraySize), array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load array from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

				array2, err := NewArrayWithRootID(storage2, arraySlabID)
				require.NoError(t, err)
				require.Equal(t, uint64(arraySize), array2.Count())

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

	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArraySize = 10
		largeArraySize = 512
	)

	arraySizeTestCases := []struct {
		name      string
		arraySize int
	}{
		{name: "small array", arraySize: smallArraySize},
		{name: "large array", arraySize: largeArraySize},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arraySizeTestCase := range arraySizeTestCases {

			arraySize := arraySizeTestCase.arraySize

			name := arraySizeTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += "," + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				array, err := NewArray(storage, address, typeInfo)
				require.NoError(t, err)

				arraySlabID := array.SlabID()

				// Insert WrapperValue in reverse order to array
				expectedValues := make([]Value, arraySize)
				for i := arraySize - 1; i >= 0; i-- {
					v, expectedV := tc.newElement(storage)

					err := array.Insert(0, v)
					require.NoError(t, err)

					expectedValues[i] = expectedV
				}

				require.Equal(t, uint64(arraySize), array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Retrieve and modify WrapperValue from array
				for i := uint64(0); i < array.Count(); i++ {
					v, err := array.Get(i)
					require.NoError(t, err)

					expected := expectedValues[i]
					valueEqual(t, expected, v)

					// Verify that v is WrapperValue
					testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

					// Modify element
					newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
					require.NoError(t, err)

					if tc.mustSetModifiedElementInArray {
						testSetElementFromArray(t, storage, array, i, newV, expected)
					}

					expectedValues[i] = newExpectedV
				}

				require.Equal(t, uint64(arraySize), array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load array from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

				array2, err := NewArrayWithRootID(storage2, arraySlabID)
				require.NoError(t, err)
				require.Equal(t, uint64(arraySize), array2.Count())

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
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArraySize = 10
		largeArraySize = 512
	)

	arraySizeTestCases := []struct {
		name      string
		arraySize int
	}{
		{name: "small array", arraySize: smallArraySize},
		{name: "large array", arraySize: largeArraySize},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arraySizeTestCase := range arraySizeTestCases {

			arraySize := arraySizeTestCase.arraySize

			name := arraySizeTestCase.name + " " + tc.name
			if tc.modifyName != "" {
				name += "," + tc.modifyName
			}

			t.Run(name, func(t *testing.T) {

				storage := newTestPersistentStorage(t)

				array, err := NewArray(storage, address, typeInfo)
				require.NoError(t, err)

				arraySlabID := array.SlabID()

				// Insert WrapperValue to array
				expectedValues := make([]Value, arraySize)
				for i := 0; i < arraySize; i++ {
					v, expectedV := tc.newElement(storage)

					err := array.Insert(array.Count(), v)
					require.NoError(t, err)

					expectedValues[i] = expectedV
				}

				require.Equal(t, uint64(arraySize), array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Set new WrapperValue in array
				for i := 0; i < arraySize; i++ {
					v, expected := tc.newElement(storage)

					testSetElementFromArray(t, storage, array, uint64(i), v, expectedValues[i])

					expectedValues[i] = expected
				}

				require.Equal(t, uint64(arraySize), array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Retrieve and modify WrapperValue from array
				for i := uint64(0); i < array.Count(); i++ {
					v, err := array.Get(i)
					require.NoError(t, err)

					expected := expectedValues[i]
					valueEqual(t, expected, v)

					// Verify that v is WrapperValue
					testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

					// Modify element
					newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
					require.NoError(t, err)

					if tc.mustSetModifiedElementInArray {
						testSetElementFromArray(t, storage, array, i, newV, expected)
					}

					expectedValues[i] = newExpectedV
				}

				require.Equal(t, uint64(arraySize), array.Count())

				testArrayMutableElementIndex(t, array)

				testArray(t, storage, typeInfo, address, array, expectedValues, true)

				// Commit storage
				err = storage.FastCommit(runtime.NumCPU())
				require.NoError(t, err)

				// Load array from encoded data
				storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

				array2, err := NewArrayWithRootID(storage2, arraySlabID)
				require.NoError(t, err)
				require.Equal(t, uint64(arraySize), array2.Count())

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
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArraySize = 10
		largeArraySize = 512
	)

	arraySizeTestCases := []struct {
		name      string
		arraySize int
	}{
		{name: "small array", arraySize: smallArraySize},
		{name: "large array", arraySize: largeArraySize},
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
		{name: fmt.Sprintf("remove %d element", smallArraySize/2), removeElementCount: smallArraySize / 2},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arraySizeTestCase := range arraySizeTestCases {

			for _, modifyTestCase := range modifyTestCases {

				for _, removeSizeTestCase := range removeSizeTestCases {

					arraySize := arraySizeTestCase.arraySize

					needToModifyElement := modifyTestCase.needToModifyElement

					removeSize := removeSizeTestCase.removeElementCount
					if removeSizeTestCase.removeAllElements {
						removeSize = arraySize
					}

					name := arraySizeTestCase.name + " " + tc.name
					if modifyTestCase.needToModifyElement {
						name += ", " + tc.modifyName
					}
					if removeSizeTestCase.name != "" {
						name += ", " + removeSizeTestCase.name
					}

					t.Run(name, func(t *testing.T) {

						storage := newTestPersistentStorage(t)

						array, err := NewArray(storage, address, typeInfo)
						require.NoError(t, err)

						arraySlabID := array.SlabID()

						// Insert WrapperValue to array
						expectedValues := make([]Value, arraySize)
						for i := 0; i < arraySize; i++ {
							v, expectedV := tc.newElement(storage)

							err := array.Insert(array.Count(), v)
							require.NoError(t, err)

							expectedValues[i] = expectedV
						}

						require.Equal(t, uint64(arraySize), array.Count())

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Retrieve and modify WrapperValue from array
						if needToModifyElement {
							for i := uint64(0); i < array.Count(); i++ {
								v, err := array.Get(i)
								require.NoError(t, err)

								expected := expectedValues[i]
								valueEqual(t, expected, v)

								// Verify that v is WrapperValue
								testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

								// Modify element
								newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
								require.NoError(t, err)

								if tc.mustSetModifiedElementInArray {
									testSetElementFromArray(t, storage, array, i, newV, expected)
								}

								expectedValues[i] = newExpectedV
							}

							require.Equal(t, uint64(arraySize), array.Count())

							testArrayMutableElementIndex(t, array)

							testArray(t, storage, typeInfo, address, array, expectedValues, true)
						}

						// Remove random elements
						for i := 0; i < removeSize; i++ {

							removeIndex := r.Intn(int(array.Count()))

							testRemoveElementFromArray(t, storage, array, uint64(removeIndex), expectedValues[removeIndex])

							expectedValues = append(expectedValues[:removeIndex], expectedValues[removeIndex+1:]...)
						}

						require.Equal(t, uint64(arraySize-removeSize), array.Count())
						require.Equal(t, arraySize-removeSize, len(expectedValues))

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Commit storage
						err = storage.FastCommit(runtime.NumCPU())
						require.NoError(t, err)

						// Load array from encoded data
						storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

						array2, err := NewArrayWithRootID(storage2, arraySlabID)
						require.NoError(t, err)
						require.Equal(t, uint64(arraySize-removeSize), array2.Count())

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
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArraySize = 10
		largeArraySize = 512
	)

	arraySizeTestCases := []struct {
		name      string
		arraySize int
	}{
		{name: "small array", arraySize: smallArraySize},
		{name: "large array", arraySize: largeArraySize},
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
		{name: fmt.Sprintf("remove %d element", smallArraySize/2), removeElementCount: smallArraySize / 2},
	}

	testCases := newArrayWrapperValueTestCases(t, r, address, typeInfo)

	for _, tc := range testCases {

		for _, arraySizeTestCase := range arraySizeTestCases {

			for _, modifyTestCase := range modifyTestCases {

				for _, removeSizeTestCase := range removeSizeTestCases {

					arraySize := arraySizeTestCase.arraySize

					needToModifyElement := modifyTestCase.needToModifyElement

					removeSize := removeSizeTestCase.removeElementCount
					if removeSizeTestCase.removeAllElements {
						removeSize = arraySize
					}

					name := arraySizeTestCase.name + " " + tc.name
					if modifyTestCase.needToModifyElement {
						name += ", " + tc.modifyName
					}
					if removeSizeTestCase.name != "" {
						name += ", " + removeSizeTestCase.name
					}

					t.Run(name, func(t *testing.T) {

						storage := newTestPersistentStorage(t)

						array, err := NewArray(storage, address, typeInfo)
						require.NoError(t, err)

						arraySlabID := array.SlabID()

						expectedValues := make([]Value, arraySize)

						// Insert WrapperValue to array
						for i := 0; i < arraySize; i++ {
							v, expectedV := tc.newElement(storage)

							err := array.Insert(array.Count(), v)
							require.NoError(t, err)

							expectedValues[i] = expectedV
						}

						require.Equal(t, uint64(arraySize), array.Count())

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Set WrapperValue in array
						for i := 0; i < arraySize; i++ {
							v, expectedV := tc.newElement(storage)

							testSetElementFromArray(t, storage, array, uint64(i), v, expectedValues[i])

							expectedValues[i] = expectedV
						}

						require.Equal(t, uint64(arraySize), array.Count())

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Retrieve and modify WrapperValue from array
						if needToModifyElement {
							for i := uint64(0); i < array.Count(); i++ {
								v, err := array.Get(i)
								require.NoError(t, err)

								expected := expectedValues[i]
								valueEqual(t, expected, v)

								// Verify that v is WrapperValue
								testWrapperValueLevels(t, tc.wrapperValueNestedLevels, v)

								// Modify element
								newV, newExpectedV, err := tc.modifyElement(storage, v, expected)
								require.NoError(t, err)

								if tc.mustSetModifiedElementInArray {
									testSetElementFromArray(t, storage, array, i, newV, expected)
								}

								expectedValues[i] = newExpectedV
							}

							require.Equal(t, uint64(arraySize), array.Count())

							testArrayMutableElementIndex(t, array)

							testArray(t, storage, typeInfo, address, array, expectedValues, true)
						}

						// Remove random elements
						for i := 0; i < removeSize; i++ {

							removeIndex := r.Intn(int(array.Count()))

							testRemoveElementFromArray(t, storage, array, uint64(removeIndex), expectedValues[removeIndex])

							expectedValues = append(expectedValues[:removeIndex], expectedValues[removeIndex+1:]...)
						}

						require.Equal(t, uint64(arraySize-removeSize), array.Count())
						require.Equal(t, arraySize-removeSize, len(expectedValues))

						testArrayMutableElementIndex(t, array)

						testArray(t, storage, typeInfo, address, array, expectedValues, true)

						// Commit storage
						err = storage.FastCommit(runtime.NumCPU())
						require.NoError(t, err)

						// Load array from encoded data
						storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

						array2, err := NewArrayWithRootID(storage2, arraySlabID)
						require.NoError(t, err)
						require.Equal(t, uint64(arraySize-removeSize), array2.Count())

						// Test loaded array
						testArray(t, storage2, typeInfo, address, array2, expectedValues, true)
					})
				}
			}
		}
	}
}

func TestArrayWrapperValueReadOnlyIterate(t *testing.T) {
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArraySize = 10
		largeArraySize = 512
	)

	arraySizeTestCases := []struct {
		name      string
		arraySize int
	}{
		{name: "small array", arraySize: smallArraySize},
		{name: "large array", arraySize: largeArraySize},
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

		for _, arraySizeTestCase := range arraySizeTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && tc.mustSetModifiedElementInArray {
					continue
				}

				arraySize := arraySizeTestCase.arraySize

				testModifyElement := modifyTestCase.testModifyElement

				name := arraySizeTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					array, err := NewArray(storage, address, typeInfo)
					require.NoError(t, err)

					expectedValues := make([]Value, arraySize)

					// Insert WrapperValue to array
					for i := 0; i < arraySize; i++ {
						v, expectedV := tc.newElement(storage)

						err := array.Insert(array.Count(), v)
						require.NoError(t, err)

						expectedValues[i] = expectedV
					}

					require.Equal(t, uint64(arraySize), array.Count())

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

						valueEqual(t, expected, next)

						// Test modifying elements that don't need to reset in parent container.
						if testModifyElement {
							_, _, err := tc.modifyElement(storage, next, expected)
							var targetErr *ReadOnlyIteratorElementMutationError
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
	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const (
		smallArraySize = 10
		largeArraySize = 512
	)

	arraySizeTestCases := []struct {
		name      string
		arraySize int
	}{
		{name: "small array", arraySize: smallArraySize},
		{name: "large array", arraySize: largeArraySize},
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

		for _, arraySizeTestCase := range arraySizeTestCases[:1] {

			for _, modifyTestCase := range modifyTestCases {

				elementIsContainer := !tc.mustSetModifiedElementInArray

				// Can't test modifying elements in readonly iteration if elements are not containers.
				if modifyTestCase.testModifyElement && !elementIsContainer {
					continue
				}

				arraySize := arraySizeTestCase.arraySize

				testModifyElement := modifyTestCase.testModifyElement

				name := arraySizeTestCase.name + " " + tc.name
				if modifyTestCase.testModifyElement {
					name += ", " + tc.modifyName
				}

				t.Run(name, func(t *testing.T) {

					storage := newTestPersistentStorage(t)

					array, err := NewArray(storage, address, typeInfo)
					require.NoError(t, err)

					expectedValues := make([]Value, arraySize)

					// Insert WrapperValue to array
					for i := 0; i < arraySize; i++ {
						v, expectedV := tc.newElement(storage)

						err := array.Insert(array.Count(), v)
						require.NoError(t, err)

						expectedValues[i] = expectedV
					}

					require.Equal(t, uint64(arraySize), array.Count())

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

						valueEqual(t, expected, next)

						// Test modifying container elements.
						if testModifyElement {
							_, newExpectedV, err := tc.modifyElement(storage, next, expected)
							require.NoError(t, err)

							expectedValues[count] = newExpectedV
						}

						count++
					}

					require.Equal(t, uint64(arraySize), array.Count())

					testArrayMutableElementIndex(t, array)

					testArray(t, storage, typeInfo, address, array, expectedValues, true)
				})
			}
		}
	}
}

func TestArrayWrapperValueInlineArrayAtLevel1(t *testing.T) {

	testLevel1WrappedChildArrayInlined := func(t *testing.T, array *Array, expectedInlined bool) {
		rootDataSlab, isDataSlab := array.root.(*ArrayDataSlab)
		require.True(t, isDataSlab)

		require.Equal(t, 1, len(rootDataSlab.elements))

		storable := rootDataSlab.elements[0]

		storabeleAsSomeStoable, isSomeStorable := storable.(SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorable := storabeleAsSomeStoable.Storable

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

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	storage := newTestPersistentStorage(t)

	var expectedValues arrayValue

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Append WrapperValue SomeValue([]) to array
	{
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		require.False(t, childArray.Inlined())

		err = array.Append(SomeValue{childArray})
		require.NoError(t, err)

		require.True(t, childArray.Inlined())

		expectedValues = append(expectedValues, someValue{arrayValue{}})

		require.Equal(t, uint64(1), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		testLevel1WrappedChildArrayInlined(t, array, true)
	}

	// Retrieve wrapped child array, and then append new elements to child array.
	// Wrapped child array is expected to be unlined at the end of loop.

	const childArraySize = 32
	for i := 0; i < childArraySize; i++ {
		// Get element
		element, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedArray, isArray := wrappedValue.(*Array)
		require.True(t, isArray)

		expectedWrappedValue := expectedValues[0].(someValue).Value

		expectedWrappedArray := expectedWrappedValue.(arrayValue)

		// Append new elements to wrapped child array

		v := Uint64Value(i)

		err = wrappedArray.Append(SomeValue{v})
		require.NoError(t, err)

		expectedWrappedArray = append(expectedWrappedArray, someValue{v})

		expectedValues[0] = someValue{expectedWrappedArray}

		require.Equal(t, uint64(i+1), wrappedArray.Count())
		require.Equal(t, i+1, len(expectedWrappedArray))

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel1WrappedChildArrayInlined(t, array, false)

	// Retrieve wrapped child array, and then remove elements to child array.
	// Wrapped child array is expected to be inlined at the end of loop.

	childArraySizeAfterRemoval := 2
	removeCount := childArraySize - childArraySizeAfterRemoval

	for i := 0; i < removeCount; i++ {
		// Get element
		element, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValue, isSomeValue := element.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValue := elementAsSomeValue.Value

		wrappedArray, isArray := wrappedValue.(*Array)
		require.True(t, isArray)

		expectedWrappedValue := expectedValues[0].(someValue).Value

		expectedWrappedArray := expectedWrappedValue.(arrayValue)

		// Remove first element from wrapped child array

		existingStorable, err := wrappedArray.Remove(0)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)

		// Verify removed value

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, expectedWrappedArray[0], existingValue)

		expectedWrappedArray = expectedWrappedArray[1:]

		expectedValues[0] = someValue{expectedWrappedArray}

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel1WrappedChildArrayInlined(t, array, true)

	testArrayMutableElementIndex(t, array)

	testArray(t, storage, typeInfo, address, array, expectedValues, true)
}

func TestArrayWrapperValueInlineArrayAtLevel2(t *testing.T) {

	testLevel2WrappedChildArrayInlined := func(t *testing.T, array *Array, expectedInlined bool) {
		rootDataSlab, isDataSlab := array.root.(*ArrayDataSlab)
		require.True(t, isDataSlab)

		require.Equal(t, 1, len(rootDataSlab.elements))

		// Get unwrapped value at level 1

		storableAtLevel1 := rootDataSlab.elements[0]

		storabeleAsSomeStoable, isSomeStorable := storableAtLevel1.(SomeStorable)
		require.True(t, isSomeStorable)

		wrappedStorableAtLevel1 := storabeleAsSomeStoable.Storable

		wrappedArrayAtlevel1, isArray := wrappedStorableAtLevel1.(*ArrayDataSlab)
		require.True(t, isArray)

		// Get unwrapped value at level 2

		storableAtLevel2 := wrappedArrayAtlevel1.elements[0]

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

	storage := newTestPersistentStorage(t)

	var expectedValues arrayValue

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Append WrapperValue SomeValue([SomeValue[]]) to array
	{
		// Create grand child array
		gchildArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		require.False(t, gchildArray.Inlined())

		// Create child array
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		require.False(t, childArray.Inlined())

		// Append grand child array to child array
		err = childArray.Append(SomeValue{gchildArray})
		require.NoError(t, err)

		require.True(t, gchildArray.Inlined())

		// Append child array to array
		err = array.Append(SomeValue{childArray})
		require.NoError(t, err)

		require.True(t, childArray.Inlined())

		expectedValues = append(expectedValues, someValue{arrayValue{someValue{arrayValue{}}}})

		require.Equal(t, uint64(1), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		testLevel2WrappedChildArrayInlined(t, array, true)
	}

	// Retrieve wrapped gchild array, and then append new elements to gchild array.
	// Wrapped gchild array is expected to be unlined at the end of loop.

	const gchildArraySize = 32
	for i := 0; i < gchildArraySize; i++ {
		// Get element at level 1

		elementAtLevel1, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedArrayAtLevel1, isArray := wrappedValueAtLevel1.(*Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel1 := expectedValues[0].(someValue).Value

		expectedWrappedArrayAtLevel1 := expectedWrappedValueAtLevel1.(arrayValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedArrayAtLevel1.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedArrayAtLevel2, isArray := wrappedValueAtLevel2.(*Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel2 := expectedWrappedArrayAtLevel1[0].(someValue).Value

		expectedWrappedArrayAtLevel2 := expectedWrappedValueAtLevel2.(arrayValue)

		// Append new elements to wrapped gchild array

		v := Uint64Value(i)

		err = wrappedArrayAtLevel2.Append(SomeValue{v})
		require.NoError(t, err)

		expectedWrappedArrayAtLevel2 = append(expectedWrappedArrayAtLevel2, someValue{v})

		expectedValues[0] = someValue{arrayValue{someValue{expectedWrappedArrayAtLevel2}}}

		require.Equal(t, uint64(i+1), wrappedArrayAtLevel2.Count())
		require.Equal(t, i+1, len(expectedWrappedArrayAtLevel2))

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel2WrappedChildArrayInlined(t, array, false)

	// Retrieve wrapped gchild array, and then remove elements from gchild array.
	// Wrapped gchild array is expected to be inlined at the end of loop.

	gchildArraySizeAfterRemoval := 2
	removeCount := gchildArraySize - gchildArraySizeAfterRemoval

	for i := 0; i < removeCount; i++ {
		// Get elementAtLevel1
		elementAtLevel1, err := array.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel1, isSomeValue := elementAtLevel1.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel1 := elementAsSomeValueAtLevel1.Value

		wrappedArrayAtLevel1, isArray := wrappedValueAtLevel1.(*Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel1 := expectedValues[0].(someValue).Value

		expectedWrappedArrayAtLevel1 := expectedWrappedValueAtLevel1.(arrayValue)

		// Get element at level 2

		elementAtLevel2, err := wrappedArrayAtLevel1.Get(0)
		require.NoError(t, err)

		// Test retrieved element type
		elementAsSomeValueAtLevel2, isSomeValue := elementAtLevel2.(SomeValue)
		require.True(t, isSomeValue)

		wrappedValueAtLevel2 := elementAsSomeValueAtLevel2.Value

		wrappedArrayAtLevel2, isArray := wrappedValueAtLevel2.(*Array)
		require.True(t, isArray)

		expectedWrappedValueAtLevel2 := expectedWrappedArrayAtLevel1[0].(someValue).Value

		expectedWrappedArrayAtLevel2 := expectedWrappedValueAtLevel2.(arrayValue)

		// Remove first element from wrapped gchild array

		existingStorable, err := wrappedArrayAtLevel2.Remove(0)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)

		// Verify removed value

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, expectedWrappedArrayAtLevel2[0], existingValue)

		expectedWrappedArrayAtLevel2 = expectedWrappedArrayAtLevel2[1:]

		expectedValues[0] = someValue{arrayValue{someValue{expectedWrappedArrayAtLevel2}}}

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	}

	testLevel2WrappedChildArrayInlined(t, array, true)

	testArrayMutableElementIndex(t, array)

	testArray(t, storage, typeInfo, address, array, expectedValues, true)
}

func TestArrayWrapperValueModifyNewArrayAtLevel1(t *testing.T) {

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

		// SomeValue([SomeValue(uint64)])
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

		// SomeValue([SomeValue([SomeValue(uint64)])])
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

	var expectedValues arrayValue

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	actualArraySize := 0

	t.Run("append and remove", func(t *testing.T) {

		// Append elements

		var appendCount int
		for appendCount < minWriteOperationSize {
			appendCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualArraySize += appendCount

		for i := 0; i < appendCount; i++ {
			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			err = array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, expected)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove at least half of elements

		var removeCount int
		minRemoveCount := int(array.Count()) / 2
		maxRemoveCount := int(array.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(array.Count()) + 1)
		}

		actualArraySize -= removeCount

		removeIndex := getRandomUniquePositiveNumbers(r, int(array.Count()), removeCount)

		sort.Sort(sort.Reverse(sort.IntSlice(removeIndex)))

		for _, index := range removeIndex {
			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("insert and remove", func(t *testing.T) {
		// Insert elements

		var insertCount int
		for insertCount < minWriteOperationSize {
			insertCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualArraySize += insertCount

		lowestInsertIndex := math.MaxInt

		for i := 0; i < insertCount; i++ {
			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			index := r.Intn(int(array.Count()))

			if index < lowestInsertIndex {
				lowestInsertIndex = index
			}

			err = array.Insert(uint64(index), v)
			require.NoError(t, err)

			newExpectedValue := make([]Value, len(expectedValues)+1)

			copy(newExpectedValue, expectedValues[:index])
			newExpectedValue[index] = expected
			copy(newExpectedValue[index+1:], expectedValues[index:])

			expectedValues = newExpectedValue
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including one previously inserted element)

		var removeCount int
		minRemoveCount := int(array.Count()) / 2
		maxRemoveCount := int(array.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(array.Count()) + 1)
		}

		actualArraySize -= removeCount

		// Remove previously inserted element first

		{
			index := lowestInsertIndex

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		// Remove more elements

		for i := 1; i < removeCount; i++ {
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("set and remove", func(t *testing.T) {
		// Set elements

		var setCount int
		if array.Count() <= 10 {
			setCount = int(array.Count())
		} else {
			for setCount < int(array.Count())/2 {
				setCount = r.Intn(int(array.Count()) + 1)
			}
		}

		setIndex := make([]int, 0, setCount)

		for i := 0; i < setCount; i++ {
			newValue := newElementFuncs[r.Intn(len(newElementFuncs))]
			v, expected := newValue(storage)

			index := r.Intn(int(array.Count()))

			testSetElementFromArray(t, storage, array, uint64(index), v, expectedValues[index])

			expectedValues[index] = expected

			setIndex = append(setIndex, index)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including some previously set elements)

		var removeCount int
		minRemoveCount := int(array.Count()) / 2
		maxRemoveCount := int(array.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(array.Count()))
		}

		actualArraySize -= removeCount

		// Remove some previously set elements first

		// Reverse sort and deduplicate set index
		sort.Sort(sort.Reverse(sort.IntSlice(setIndex)))

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
		if len(setIndex) < removeSetCount {
			removeSetCount = len(setIndex)
		}

		for _, index := range setIndex[:removeSetCount] {
			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		for i := 0; i < removeCount-removeSetCount; i++ {
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		for array.Count() > 0 {
			// Remove element at random index
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(0), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayWrapperValueModifyNewArrayAtLevel2(t *testing.T) {

	const (
		minWriteOperationSize = 124
		maxWriteOperationSize = 256
	)

	r := newRand(t)

	SetThreshold(256)
	defer SetThreshold(1024)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	// newValue creates value of type SomeValue([SomeValue(uint64)]).
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

	var expectedValues arrayValue

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	actualArraySize := 0

	t.Run("append and remove", func(t *testing.T) {

		// Append elements

		var appendCount int
		for appendCount < minWriteOperationSize {
			appendCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualArraySize += appendCount

		for i := 0; i < appendCount; i++ {
			v, expected := newValue(storage)

			err = array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, expected)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements

		var removeCount int
		minRemoveCount := int(array.Count()) / 2
		maxRemoveCount := int(array.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(array.Count()) + 1)
		}

		actualArraySize -= removeCount

		removeIndex := getRandomUniquePositiveNumbers(r, int(array.Count()), removeCount)

		sort.Sort(sort.Reverse(sort.IntSlice(removeIndex)))

		for _, index := range removeIndex {
			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("insert and remove", func(t *testing.T) {
		// Insert elements

		var insertCount int
		for insertCount < minWriteOperationSize {
			insertCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualArraySize += insertCount

		lowestInsertIndex := math.MaxInt

		for i := 0; i < insertCount; i++ {
			v, expected := newValue(storage)

			index := r.Intn(int(array.Count()))

			if index < lowestInsertIndex {
				lowestInsertIndex = index
			}

			err = array.Insert(uint64(index), v)
			require.NoError(t, err)

			newExpectedValue := make([]Value, len(expectedValues)+1)

			copy(newExpectedValue, expectedValues[:index])
			newExpectedValue[index] = expected
			copy(newExpectedValue[index+1:], expectedValues[index:])

			expectedValues = newExpectedValue
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including one previously inserted element)

		var removeCount int
		minRemoveCount := int(array.Count()) / 2
		maxRemoveCount := int(array.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(array.Count()) + 1)
		}

		actualArraySize -= removeCount

		// Remove previously inserted element first

		{
			index := lowestInsertIndex

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		// Remove more elements

		for i := 1; i < removeCount; i++ {
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		var setCount int
		if array.Count() <= 10 {
			setCount = int(array.Count())
		} else {
			for setCount < int(array.Count())/2 {
				setCount = r.Intn(int(array.Count()) + 1)
			}
		}

		setIndex := make([]int, 0, setCount)

		for i := 0; i < setCount; i++ {

			index := r.Intn(int(array.Count()))

			// Get element
			originalValue, err := array.Get(uint64(index))
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[index])
			require.NoError(t, err)

			expectedValues[index] = modifiedExpectedValue

			setIndex = append(setIndex, index)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including some previously set elements)

		var removeCount int
		minRemoveCount := int(array.Count()) / 2
		maxRemoveCount := int(array.Count()) / 4 * 3
		for removeCount < minRemoveCount || removeCount > maxRemoveCount {
			removeCount = r.Intn(int(array.Count()))
		}

		actualArraySize -= removeCount

		// Remove some previously set elements first

		// Reverse sort and deduplicate set index
		sort.Sort(sort.Reverse(sort.IntSlice(setIndex)))

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
		if len(setIndex) < removeSetCount {
			removeSetCount = len(setIndex)
		}

		for _, index := range setIndex[:removeSetCount] {
			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		for i := 0; i < removeCount-removeSetCount; i++ {
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		for array.Count() > 0 {
			// Remove element at random index
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(0), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayWrapperValueModifyNewArrayAtLevel3(t *testing.T) {

	const (
		minWriteOperationSize = 124
		maxWriteOperationSize = 256
	)

	r := newRand(t)

	SetThreshold(256)
	defer SetThreshold(1024)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	typeInfo := testTypeInfo{42}

	// newValue creates value of type SomeValue([SomeValue([SomeValue(uint64)])]))
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

	var expectedValues arrayValue

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	actualArraySize := 0

	t.Run("append and remove", func(t *testing.T) {

		// Append elements

		var appendCount int
		for appendCount < minWriteOperationSize {
			appendCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualArraySize += appendCount

		for i := 0; i < appendCount; i++ {
			v, expected := newValue(storage)

			err = array.Append(v)
			require.NoError(t, err)

			expectedValues = append(expectedValues, expected)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements

		var removeCount int
		for removeCount < int(array.Count())/2 {
			removeCount = r.Intn(int(array.Count()) + 1)
		}

		actualArraySize -= removeCount

		removeIndex := getRandomUniquePositiveNumbers(r, int(array.Count()), removeCount)

		sort.Sort(sort.Reverse(sort.IntSlice(removeIndex)))

		for _, index := range removeIndex {
			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("insert and remove", func(t *testing.T) {
		// Insert elements

		var insertCount int
		for insertCount < minWriteOperationSize {
			insertCount = r.Intn(maxWriteOperationSize + 1)
		}

		actualArraySize += insertCount

		lowestInsertIndex := math.MaxInt

		for i := 0; i < insertCount; i++ {
			v, expected := newValue(storage)

			index := r.Intn(int(array.Count()))

			if index < lowestInsertIndex {
				lowestInsertIndex = index
			}

			err = array.Insert(uint64(index), v)
			require.NoError(t, err)

			newExpectedValue := make([]Value, len(expectedValues)+1)

			copy(newExpectedValue, expectedValues[:index])
			newExpectedValue[index] = expected
			copy(newExpectedValue[index+1:], expectedValues[index:])

			expectedValues = newExpectedValue
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including one previously inserted element)

		var removeCount int
		for removeCount < int(array.Count())/2 {
			removeCount = r.Intn(int(array.Count()) + 1)
		}

		actualArraySize -= removeCount

		// Remove previously inserted element first

		{
			index := lowestInsertIndex

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		// Remove more elements

		for i := 1; i < removeCount; i++ {
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("modify retrieved nested container and remove", func(t *testing.T) {
		// Set elements

		var setCount int
		if array.Count() <= 10 {
			setCount = int(array.Count())
		} else {
			for setCount < int(array.Count())/2 {
				setCount = r.Intn(int(array.Count()) + 1)
			}
		}

		setIndex := make([]int, 0, setCount)

		for i := 0; i < setCount; i++ {

			index := r.Intn(int(array.Count()))

			// Get element
			originalValue, err := array.Get(uint64(index))
			require.NoError(t, err)
			require.NotNil(t, originalValue)

			_, isWrapperValue := originalValue.(SomeValue)
			require.True(t, isWrapperValue)

			// Modify retrieved element without setting back explicitly.
			_, modifiedExpectedValue, err := modifyValue(storage, originalValue, expectedValues[index])
			require.NoError(t, err)

			expectedValues[index] = modifiedExpectedValue

			setIndex = append(setIndex, index)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)

		// Remove some elements (including some previously set elements)

		var removeCount int
		for removeCount < int(array.Count())/2 {
			removeCount = r.Intn(int(array.Count()))
		}

		actualArraySize -= removeCount

		// Remove some previously set elements first

		// Reverse sort and deduplicate set index
		sort.Sort(sort.Reverse(sort.IntSlice(setIndex)))

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
		if len(setIndex) < removeSetCount {
			removeSetCount = len(setIndex)
		}

		for _, index := range setIndex[:removeSetCount] {
			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		for i := 0; i < removeCount-removeSetCount; i++ {
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(actualArraySize), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})

	t.Run("remove all", func(t *testing.T) {
		// Remove all elements

		for array.Count() > 0 {
			// Remove element at random index
			index := r.Intn(int(array.Count()))

			testRemoveElementFromArray(t, storage, array, uint64(index), expectedValues[index])

			expectedValues = append(expectedValues[:index], expectedValues[index+1:]...)
		}

		require.Equal(t, uint64(0), array.Count())

		testArrayMutableElementIndex(t, array)

		testArray(t, storage, typeInfo, address, array, expectedValues, true)
	})
}

func TestArrayWrapperValueModifyExistingArray(t *testing.T) {

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("modify level-1 wrapper array in [SomeValue([SomeValue(uint64)])]", func(t *testing.T) {
		const (
			arraySize      = 3
			childArraySize = 2
		)

		typeInfo := testTypeInfo{42}

		r := newRand(t)

		createStorage := func(arraySize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedValues []Value,
		) {
			storage := newTestPersistentStorage(t)

			createArrayOfSomeValueOfArrayOfSomeValueOfUint64 :=
				newArrayValueFunc(
					t,
					address,
					typeInfo,
					arraySize,
					newWrapperValueFunc(
						1,
						newArrayValueFunc(
							t,
							address,
							typeInfo,
							childArraySize,
							newWrapperValueFunc(
								1,
								newRandomUint64ValueFunc(r)))))

			v, expected := createArrayOfSomeValueOfArrayOfSomeValueOfUint64(storage)

			array := v.(*Array)
			expectedValues = expected.(arrayValue)

			testArray(t, storage, typeInfo, address, array, expectedValues, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, array.SlabID(), expectedValues
		}

		// Create a base storage with array in the format of
		// [SomeValue([SomeValue(uint64)])]
		baseStorage, rootSlabID, expectedValues := createStorage(arraySize)
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

		// Test retrieved element type and value

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

	t.Run("modify 2-level wrapper array in [SomeValue([SomeValue([SomeValue(uint64)])])]", func(t *testing.T) {
		const (
			arraySize       = 4
			childArraySize  = 3
			gchildArraySize = 2
		)

		typeInfo := testTypeInfo{42}

		createStorage := func(arraySize int) (
			_ BaseStorage,
			rootSlabID SlabID,
			expectedValues []Value,
		) {
			storage := newTestPersistentStorage(t)

			r := newRand(t)

			createArrayOfSomeValueOfArrayOfSomeValueOfArrayOfSomeValueOfUint64 :=
				newArrayValueFunc(
					t,
					address,
					typeInfo,
					arraySize,
					newWrapperValueFunc(
						1,
						newArrayValueFunc(
							t,
							address,
							typeInfo,
							childArraySize,
							newWrapperValueFunc(
								1,
								newArrayValueFunc(
									t,
									address,
									typeInfo,
									gchildArraySize,
									newWrapperValueFunc(
										1,
										newRandomUint64ValueFunc(r)))))))

			v, expected := createArrayOfSomeValueOfArrayOfSomeValueOfArrayOfSomeValueOfUint64(storage)

			array := v.(*Array)
			expectedValues = expected.(arrayValue)

			testArray(t, storage, typeInfo, address, array, expectedValues, true)

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(t, err)

			return storage.baseStorage, array.SlabID(), expectedValues
		}

		// Create a base storage with array in the format of
		// [SomeValue([SomeValue([SomeValue(uint64)])])]
		baseStorage, rootSlabID, expectedValues := createStorage(arraySize)
		require.Equal(t, arraySize, len(expectedValues))

		// Create a new storage with encoded array
		storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

		// Load existing array from storage
		array, err := NewArrayWithRootID(storage, rootSlabID)
		require.NoError(t, err)
		require.Equal(t, uint64(len(expectedValues)), array.Count())

		// Get and verify first element as SomeValue(array)

		expectedValue := expectedValues[0]

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

func testWrapperValueLevels(t *testing.T, expectedNestedLevels int, v Value) {
	nestedLevels := 0
	for {
		sw, ok := v.(SomeValue)
		if !ok {
			break
		}
		v = sw.Value
		nestedLevels++
	}
	require.Equal(t, expectedNestedLevels, nestedLevels)
}

func testArrayMutableElementIndex(t *testing.T, v Value) {
	v, _ = unwrapValue(v)

	array, ok := v.(*Array)
	if !ok {
		return
	}

	originalMutableIndex := make(map[ValueID]uint64)

	for vid, index := range array.mutableElementIndex {
		originalMutableIndex[vid] = index
	}

	for i := uint64(0); i < array.Count(); i++ {
		element, err := array.Get(i)
		require.NoError(t, err)

		element, _ = unwrapValue(element)

		switch element := element.(type) {
		case *Array:
			vid := element.ValueID()
			index, exists := originalMutableIndex[vid]
			require.True(t, exists)
			require.Equal(t, i, index)

			delete(originalMutableIndex, vid)

		case *OrderedMap:
			vid := element.ValueID()
			index, exists := originalMutableIndex[vid]
			require.True(t, exists)
			require.Equal(t, i, index)

			delete(originalMutableIndex, vid)
		}
	}

	require.Equal(t, 0, len(originalMutableIndex))
}

func testSetElementFromArray(t *testing.T, storage SlabStorage, array *Array, index uint64, newValue Value, expected Value) {
	existingStorable, err := array.Set(index, newValue)
	require.NoError(t, err)
	require.NotNil(t, existingStorable)

	var overwrittenSlabID SlabID

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := unwrapStorable(existingStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case ArraySlab, MapSlab:
		require.Fail(t, "overwritten storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingStorable)

	case SlabIDStorable:
		overwrittenSlabID = SlabID(wrappedStorable)

		// Verify SlabID has the same address
		require.Equal(t, array.Address(), overwrittenSlabID.Address())
	}

	// Verify overwritten value

	existingValue, err := existingStorable.StoredValue(storage)
	require.NoError(t, err)
	valueEqual(t, expected, existingValue)

	// Remove overwritten slabs from storage

	if overwrittenSlabID != SlabIDUndefined {
		err = storage.Remove(overwrittenSlabID)
		require.NoError(t, err)
	}
}

func testRemoveElementFromArray(t *testing.T, storage SlabStorage, array *Array, index uint64, expected Value) {
	existingStorable, err := array.Remove(index)
	require.NoError(t, err)
	require.NotNil(t, existingStorable)

	var removedSlabID SlabID

	// Verify wrapped storable doesn't contain inlined slab

	wrappedStorable := unwrapStorable(existingStorable)

	switch wrappedStorable := wrappedStorable.(type) {
	case ArraySlab, MapSlab:
		require.Fail(t, "removed storable shouldn't be (wrapped) ArraySlab or MapSlab: %s", existingStorable)

	case SlabIDStorable:
		removedSlabID = SlabID(wrappedStorable)

		// Verify SlabID has the same address
		require.Equal(t, array.Address(), removedSlabID.Address())
	}

	// Verify removed value

	existingValue, err := existingStorable.StoredValue(storage)
	require.NoError(t, err)
	valueEqual(t, expected, existingValue)

	// Remove slabs from storage
	if removedSlabID != SlabIDUndefined {
		err = storage.Remove(removedSlabID)
		require.NoError(t, err)
	}
}

func getRandomUniquePositiveNumbers(r *rand.Rand, nonInclusiveMax int, count int) []int {
	set := make(map[int]struct{})
	for len(set) < count {
		n := r.Intn(nonInclusiveMax)
		set[n] = struct{}{}
	}

	slice := make([]int, 0, count)
	for n := range set {
		slice = append(slice, n)
	}

	return slice
}
