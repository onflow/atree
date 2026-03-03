package atree_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/onflow/atree"
	testutils "github.com/onflow/atree/test_utils"

	"github.com/stretchr/testify/require"
)

func TestArrayCopy(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	newAddress := atree.Address{9, 10, 11, 12, 13, 14, 15, 16}

	t.Run("empty array", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array with [].

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		// Copy array.

		canCopy := array.CanCopy()
		require.True(t, canCopy)

		copiedArray, err := array.Copy(newAddress)
		require.NoError(t, err)

		// Modify original array.

		err = array.Append(testutils.NewUint64ValueFromInteger(0))
		require.NoError(t, err)

		expectedValues := testutils.ExpectedArrayValue{
			testutils.NewUint64ValueFromInteger(0),
		}

		// Modify copied array.

		err = copiedArray.Append(testutils.NewUint64ValueFromInteger(42))
		require.NoError(t, err)

		copiedArrayExpectedValues := testutils.ExpectedArrayValue{
			testutils.NewUint64ValueFromInteger(42),
		}

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 2)
		testArray(t, storage, typeInfo, newAddress, copiedArray, copiedArrayExpectedValues, false, 2)
	})

	t.Run("single slab array with simple values", func(t *testing.T) {
		const arrayCount = uint64(3)

		storage := newTestPersistentStorage(t)

		// Create an array with [0, 1, 2]

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := make(testutils.ExpectedArrayValue, arrayCount)
		for i := range expectedValues {
			v := testutils.NewUint64ValueFromInteger(i)
			expectedValues[i] = v

			err := array.Append(v)
			require.NoError(t, err)
		}

		// Copy array.

		canCopy := array.CanCopy()
		require.True(t, canCopy)

		copiedArray, err := array.Copy(newAddress)
		require.NoError(t, err)

		// Modify copied array.
		existingStorable, err := copiedArray.Set(0, testutils.NewUint64ValueFromInteger(42))
		require.NoError(t, err)
		require.Equal(t, testutils.NewUint64ValueFromInteger(0), existingStorable)

		copiedArrayExpectedValues := slices.Clone(expectedValues)
		copiedArrayExpectedValues[0] = testutils.NewUint64ValueFromInteger(42)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 2)
		testArray(t, storage, typeInfo, newAddress, copiedArray, copiedArrayExpectedValues, false, 2)
	})

	t.Run("single slab array with inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [[0]]

		nestedArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray.Append(testutils.NewUint64ValueFromInteger(0))
		require.NoError(t, err)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(nestedArray)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.ExpectedArrayValue{
				testutils.NewUint64ValueFromInteger(0),
			},
		}

		// Array with inlined container can't be copied.

		canCopy := array.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("single slab array with SlabIDStorable", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [SlabIDStorable]

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(testutils.NewStringValue(strings.Repeat("a", 1_000)))
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.NewStringValue(strings.Repeat("a", 1_000)),
		}

		// Array with reference to another slab can't be copied.

		canCopy := array.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("single slab array with wrapped simple values", func(t *testing.T) {
		const arrayCount = uint64(3)

		storage := newTestPersistentStorage(t)

		// Create an array [SomeValue(0), SomeValue(1), SomeValue(2)]

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := make(testutils.ExpectedArrayValue, arrayCount)
		for i := range expectedValues {
			v := testutils.NewUint64ValueFromInteger(i)
			wv := testutils.NewSomeValue(v)
			expectedValues[i] = testutils.NewExpectedWrapperValue(v)

			err := array.Append(wv)
			require.NoError(t, err)
		}

		// Copy array.

		canCopy := array.CanCopy()
		require.True(t, canCopy)

		copiedArray, err := array.Copy(newAddress)
		require.NoError(t, err)

		// Modify copied array.
		existingStorable, err := copiedArray.Set(0, testutils.NewSomeValue(testutils.NewUint64ValueFromInteger(42)))
		require.NoError(t, err)
		require.Equal(t, testutils.SomeStorable{Storable: testutils.NewUint64ValueFromInteger(0)}, existingStorable)

		copiedArrayExpectedValues := slices.Clone(expectedValues)
		copiedArrayExpectedValues[0] = testutils.NewExpectedWrapperValue(testutils.NewUint64ValueFromInteger(42))

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 2)
		testArray(t, storage, typeInfo, newAddress, copiedArray, copiedArrayExpectedValues, false, 2)
	})

	t.Run("single slab array with wrapped inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [SomeValue([0])]

		nestedArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray.Append(testutils.NewUint64ValueFromInteger(0))
		require.NoError(t, err)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(testutils.NewSomeValue(nestedArray))
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.NewExpectedWrapperValue(
				testutils.ExpectedArrayValue{
					testutils.NewUint64ValueFromInteger(0),
				},
			),
		}

		// Array with wrapped inlined container can not be copied.

		canCopy := array.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("single slab array with wrapped SlabIDStorable", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [SomeValue(SlabIDStorable)]

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(testutils.NewSomeValue(testutils.NewStringValue(strings.Repeat("a", 1_000))))
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.NewExpectedWrapperValue(
				testutils.NewStringValue(strings.Repeat("a", 1_000)),
			),
		}

		// Array with wrapped reference can't be copied.
		canCopy := array.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("inlined array with simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array with [[0]]

		nestedArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray.Append(testutils.NewUint64ValueFromInteger(0))
		require.NoError(t, err)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(nestedArray)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.ExpectedArrayValue{
				testutils.NewUint64ValueFromInteger(0),
			},
		}

		// Get nested (inlined) array
		retrievedInlinedArrayElement, err := array.Get(uint64(0))
		require.NoError(t, err)

		retrievedInlinedArray := retrievedInlinedArrayElement.(*atree.Array)
		require.True(t, atree.GetArrayRootSlab(retrievedInlinedArray).Inlined())

		// Inlined array with simple values can be copied.
		canCopy := retrievedInlinedArray.CanCopy()
		require.True(t, canCopy)

		copiedArray, err := retrievedInlinedArray.Copy(newAddress)
		require.NoError(t, err)

		// Modified copied array.
		existingStorable, err := copiedArray.Set(0, testutils.NewUint64ValueFromInteger(42))
		require.NoError(t, err)
		require.Equal(t, testutils.NewUint64ValueFromInteger(0), existingStorable)

		copiedArrayExpectedValues := testutils.ExpectedArrayValue{testutils.NewUint64ValueFromInteger(42)}

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 2)
		testArray(t, storage, typeInfo, newAddress, copiedArray, copiedArrayExpectedValues, false, 2)
	})

	t.Run("inlined array with inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [[[0]]]

		nestedArray1, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray1.Append(testutils.NewUint64ValueFromInteger(0))
		require.NoError(t, err)

		nestedArray2, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray2.Append(nestedArray1)
		require.NoError(t, err)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(nestedArray2)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.ExpectedArrayValue{
				testutils.ExpectedArrayValue{
					testutils.NewUint64ValueFromInteger(0),
				},
			},
		}

		// Get nested (inlined) array
		retrievedInlinedArrayElement, err := array.Get(uint64(0))
		require.NoError(t, err)

		retrievedInlinedArray := retrievedInlinedArrayElement.(*atree.Array)
		require.True(t, atree.GetArrayRootSlab(retrievedInlinedArray).Inlined())

		// Inlined array with another inlined container can't be copied.
		canCopy := retrievedInlinedArray.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("inlined array with SlabIDStorable", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [[SlabIDStorable]]

		nestedArray1, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray1.Append(testutils.NewStringValue(strings.Repeat("a", 1_000)))
		require.NoError(t, err)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(nestedArray1)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.ExpectedArrayValue{
				testutils.NewStringValue(strings.Repeat("a", 1_000)),
			},
		}

		// Get nested (inlined) array
		retrievedInlinedArrayElement, err := array.Get(uint64(0))
		require.NoError(t, err)

		retrievedInlinedArray := retrievedInlinedArrayElement.(*atree.Array)
		require.True(t, atree.GetArrayRootSlab(retrievedInlinedArray).Inlined())

		// Inlined array with reference to another slab can't be copied.
		canCopy := retrievedInlinedArray.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("inlined array with wrapped simple values", func(t *testing.T) {
		const arrayCount = uint64(3)

		storage := newTestPersistentStorage(t)

		// Create an array [[SomeValue(0), SomeValue(1), SomeValue(2)]]

		nestedArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		nestedArrayExpectedValues := make(testutils.ExpectedArrayValue, arrayCount)
		for i := range nestedArrayExpectedValues {
			v := testutils.NewUint64ValueFromInteger(i)
			wv := testutils.NewSomeValue(v)
			nestedArrayExpectedValues[i] = testutils.NewExpectedWrapperValue(v)

			err := nestedArray.Append(wv)
			require.NoError(t, err)
		}

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(nestedArray)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{nestedArrayExpectedValues}

		// Get nested (inlined) array
		retrievedInlinedArrayElement, err := array.Get(uint64(0))
		require.NoError(t, err)

		retrievedInlinedArray := retrievedInlinedArrayElement.(*atree.Array)
		require.True(t, atree.GetArrayRootSlab(retrievedInlinedArray).Inlined())

		// Inlined array with wrapped simple values can be copied.
		canCopy := retrievedInlinedArray.CanCopy()
		require.True(t, canCopy)

		copiedArray, err := retrievedInlinedArray.Copy(newAddress)
		require.NoError(t, err)

		// Modify copied array.
		existingStorable, err := copiedArray.Set(0, testutils.NewSomeValue(testutils.NewUint64ValueFromInteger(42)))
		require.NoError(t, err)
		require.Equal(t, testutils.SomeStorable{Storable: testutils.NewUint64ValueFromInteger(0)}, existingStorable)

		copiedArrayExpectedValues := slices.Clone(nestedArrayExpectedValues)
		copiedArrayExpectedValues[0] = testutils.NewExpectedWrapperValue(testutils.NewUint64ValueFromInteger(42))

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 2)
		testArray(t, storage, typeInfo, newAddress, copiedArray, copiedArrayExpectedValues, false, 2)
	})

	t.Run("inlined array with wrapped inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [[SomeValue([0])]]

		nestedArray1, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray1.Append(testutils.NewUint64ValueFromInteger(0))
		require.NoError(t, err)

		nestedArray2, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray2.Append(testutils.NewSomeValue(nestedArray1))
		require.NoError(t, err)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(nestedArray2)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.ExpectedArrayValue{
				testutils.NewExpectedWrapperValue(
					testutils.ExpectedArrayValue{
						testutils.NewUint64ValueFromInteger(0),
					},
				),
			},
		}

		// Get nested (inlined) array
		retrievedInlinedArrayElement, err := array.Get(uint64(0))
		require.NoError(t, err)

		retrievedInlinedArray := retrievedInlinedArrayElement.(*atree.Array)
		require.True(t, atree.GetArrayRootSlab(retrievedInlinedArray).Inlined())

		// Inlined array with wrapped inlined container can not be copied.
		canCopy := retrievedInlinedArray.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("inlined array with wrapped SlabIDStorable", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create an array [[SomeValue(SlabIDStorable)]]

		nestedArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nestedArray.Append(testutils.NewSomeValue(testutils.NewStringValue(strings.Repeat("a", 1_000))))
		require.NoError(t, err)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = array.Append(nestedArray)
		require.NoError(t, err)
		require.True(t, array.IsWithinSingleSlab())

		expectedValues := testutils.ExpectedArrayValue{
			testutils.ExpectedArrayValue{
				testutils.NewExpectedWrapperValue(
					testutils.NewStringValue(strings.Repeat("a", 1_000)),
				),
			},
		}

		// Get nested (inlined) array
		retrievedInlinedArrayElement, err := array.Get(uint64(0))
		require.NoError(t, err)

		retrievedInlinedArray := retrievedInlinedArrayElement.(*atree.Array)
		require.True(t, atree.GetArrayRootSlab(retrievedInlinedArray).Inlined())

		// Inlined array with wrapped reference can't be copied.
		canCopy := retrievedInlinedArray.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})

	t.Run("multi-slab array", func(t *testing.T) {
		const arrayCount = 4096

		storage := newTestPersistentStorage(t)

		// Create an array [0, 1, 2, ..., 4095]

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedValues := make(testutils.ExpectedArrayValue, arrayCount)
		for i := range expectedValues {
			v := testutils.NewUint64ValueFromInteger(i)
			expectedValues[i] = v

			err := array.Append(v)
			require.NoError(t, err)
		}

		require.False(t, array.IsWithinSingleSlab())

		// Multi-slab array can't be copied.

		canCopy := array.CanCopy()
		require.False(t, canCopy)

		// Test array and verify storage health.
		testArray(t, storage, typeInfo, address, array, expectedValues, false, 1)
	})
}
