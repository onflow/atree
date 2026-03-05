package atree_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/onflow/atree"
	testutils "github.com/onflow/atree/test_utils"

	"github.com/stretchr/testify/require"
)

func TestMapCopy(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	newAddress := atree.Address{9, 10, 11, 12, 13, 14, 15, 16}

	t.Run("empty map", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// Copy empty map.

		canCopy := m.CanCopyNonRefSimple()
		require.True(t, canCopy)

		copiedMap, err := m.CopyNonRefSimple(newAddress, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		// Test map and verify storage health.
		testEmptyMap(t, storage, typeInfo, address, m, 2)
		testEmptyMap(t, storage, typeInfo, newAddress, copiedMap, 2)

		// Modify original map.

		k := testutils.NewStringValue("a")
		v := testutils.Uint64Value(0)
		existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewUint64ValueFromInteger(0),
		}

		// Modify copied map.
		k = testutils.NewStringValue("b")
		v = testutils.Uint64Value(1)
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		copiedMapExpectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("b"): testutils.NewUint64ValueFromInteger(1),
		}

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, copiedMapExpectedValues, nil, false, 2)
	})

	t.Run("single slab map with simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": 0}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue("a")
		v := testutils.Uint64Value(0)
		existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.Uint64Value(0),
		}

		// Copy map.

		canCopy := m.CanCopyNonRefSimple()
		require.True(t, canCopy)

		copiedMap, err := m.CopyNonRefSimple(newAddress, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, expectedValues, nil, false, 2)

		// Modify copied map.

		v = testutils.Uint64Value(1)
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
		require.Equal(t, testutils.Uint64Value(0), existingStorable.(testutils.Uint64Value))

		k = testutils.NewStringValue("b")
		v = testutils.Uint64Value(2)
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		copiedMapExpectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewUint64ValueFromInteger(1),
			testutils.NewStringValue("b"): testutils.NewUint64ValueFromInteger(2),
		}

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, copiedMapExpectedValues, nil, false, 2)
	})

	t.Run("single slab map with collision simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)
		collisionDigesterBuilder := &mockDigesterBuilder{}

		// Create a map {"a": 0, "b": 1}

		m, err := atree.NewMap(storage, address, collisionDigesterBuilder, typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue("a")
		v := testutils.Uint64Value(0)

		digests := []atree.Digest{atree.Digest(0)}
		collisionDigesterBuilder.On("Digest", k).Return(mockDigester{digests})

		existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		k = testutils.NewStringValue("b")
		v = testutils.Uint64Value(1)

		collisionDigesterBuilder.On("Digest", k).Return(mockDigester{digests})

		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.Uint64Value(0),
			testutils.NewStringValue("b"): testutils.Uint64Value(1),
		}

		// Copy map.

		canCopy := m.CanCopyNonRefSimple()
		require.True(t, canCopy)

		copiedMap, err := m.CopyNonRefSimple(newAddress, collisionDigesterBuilder)
		require.NoError(t, err)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, expectedValues, nil, false, 2)

		// Modify copied map.

		v = testutils.Uint64Value(2)
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
		require.Equal(t, testutils.Uint64Value(1), existingStorable.(testutils.Uint64Value))

		k = testutils.NewStringValue("c")
		v = testutils.Uint64Value(3)
		collisionDigesterBuilder.On("Digest", k).Return(mockDigester{digests})
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		copiedMapExpectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewUint64ValueFromInteger(0),
			testutils.NewStringValue("b"): testutils.NewUint64ValueFromInteger(2),
			testutils.NewStringValue("c"): testutils.NewUint64ValueFromInteger(3),
		}

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, copiedMapExpectedValues, nil, false, 2)
	})

	t.Run("single slab map with inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": {b: 0}}

		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue("b")
		v := testutils.Uint64Value(0)
		existingStorable, err := nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k = testutils.NewStringValue("a")
		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, k, nestedMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.ExpectedMapValue{
				testutils.NewStringValue("b"): testutils.Uint64Value(0),
			},
		}

		// Map with inlined container can't be copied.

		canCopy := m.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("single slab map with SlabIDStorable as key", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {SlabIDStorable: 0}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue(strings.Repeat("a", 1_000))
		v := testutils.NewUint64ValueFromInteger(0)
		existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue(strings.Repeat("a", 1_000)): testutils.NewUint64ValueFromInteger(0),
		}

		// Map with SlabIDStorable can't be copied.

		canCopy := m.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("single slab map with SlabIDStorable as value", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": SlabIDStorable}
		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue("a")
		v := testutils.NewStringValue(strings.Repeat("a", 1_000))
		existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewStringValue(strings.Repeat("a", 1_000)),
		}

		// Map with SlabIDStorable can't be copied.

		canCopy := m.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("single slab map with wrapped simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": SomeValue(0)}
		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue("a")
		v := testutils.NewSomeValue(testutils.Uint64Value(0))
		existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewExpectedWrapperValue(testutils.Uint64Value(0)),
		}

		// Copy map.

		canCopy := m.CanCopyNonRefSimple()
		require.True(t, canCopy)

		copiedMap, err := m.CopyNonRefSimple(newAddress, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, expectedValues, nil, false, 2)

		// Modify copied map.

		v = testutils.NewSomeValue(testutils.Uint64Value(1))
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
		require.Equal(t, testutils.SomeStorable{Storable: testutils.Uint64Value(0)}, existingStorable.(testutils.SomeStorable))

		k = testutils.NewStringValue("b")
		v = testutils.NewSomeValue(testutils.Uint64Value(2))
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		copiedMapExpectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewExpectedWrapperValue(testutils.Uint64Value(1)),
			testutils.NewStringValue("b"): testutils.NewExpectedWrapperValue(testutils.Uint64Value(2)),
		}

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, copiedMapExpectedValues, nil, false, 2)
	})

	t.Run("single slab map with wrapped inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": SomeValue({b: 0})}
		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue("b")
		v := testutils.Uint64Value(0)
		existingStorable, err := nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k = testutils.NewStringValue("a")
		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, k, testutils.NewSomeValue(nestedMap))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewExpectedWrapperValue(testutils.ExpectedMapValue{
				testutils.NewStringValue("b"): testutils.Uint64Value(0),
			}),
		}

		// Map with wrapped inlined container can't be copied.

		canCopy := m.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("single slab map with wrapped SlabIDStorable as value", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": SomeValue(SlabIDStorable)}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := testutils.NewStringValue("a")
		v := testutils.NewSomeValue(testutils.NewStringValue(strings.Repeat("a", 1_000)))
		existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.NewExpectedWrapperValue(testutils.NewStringValue(strings.Repeat("a", 1_000))),
		}

		// Map with wrapped SlabIDStorable can't be copied.

		canCopy := m.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("inlined map with simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": {"b": 0}}

		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k1 := testutils.NewStringValue("b")
		v := testutils.Uint64Value(0)
		existingStorable, err := nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, k1, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k2 := testutils.NewStringValue("a")
		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, k2, nestedMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedNestedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("b"): testutils.Uint64Value(0),
		}

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): expectedNestedValues,
		}

		// Get nested (inlined) map

		retrievedInlinedMapElement, err := m.Get(testutils.CompareValue, testutils.GetHashInput, k2)
		require.NoError(t, err)

		retrievedInlinedMap := retrievedInlinedMapElement.(*atree.OrderedMap)
		require.True(t, atree.GetMapRootSlab(retrievedInlinedMap).Inlined())

		// Copy map.

		canCopy := retrievedInlinedMap.CanCopyNonRefSimple()
		require.True(t, canCopy)

		copiedMap, err := retrievedInlinedMap.CopyNonRefSimple(newAddress, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, expectedNestedValues, nil, false, 2)

		// Modify copied map.

		v = testutils.Uint64Value(1)
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k1, v)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
		require.Equal(t, testutils.Uint64Value(0), existingStorable.(testutils.Uint64Value))

		k := testutils.NewStringValue("c")
		v = testutils.Uint64Value(2)
		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		copiedMapExpectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("b"): testutils.NewUint64ValueFromInteger(1),
			testutils.NewStringValue("c"): testutils.NewUint64ValueFromInteger(2),
		}

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, copiedMapExpectedValues, nil, false, 2)
	})

	t.Run("inlined map with inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": {"b": {"c": 0}}}

		nestedMap2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err := nestedMap2.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("c"), testutils.NewUint64ValueFromInteger(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err = nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("b"), nestedMap2)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"), nestedMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.ExpectedMapValue{
				testutils.NewStringValue("b"): testutils.ExpectedMapValue{
					testutils.NewStringValue("c"): testutils.Uint64Value(0),
				},
			},
		}

		// Get nested (inlined) map

		retrievedInlinedMapElement, err := m.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"))
		require.NoError(t, err)

		retrievedInlinedMap := retrievedInlinedMapElement.(*atree.OrderedMap)
		require.True(t, atree.GetMapRootSlab(retrievedInlinedMap).Inlined())

		// Map with inlined container can't be copied.

		canCopy := retrievedInlinedMap.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("inlined map with SlabIDStorable", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": {"b": SlabIDStorable}}

		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err := nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("b"), testutils.NewStringValue(strings.Repeat("a", 1_000)))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"), nestedMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.ExpectedMapValue{
				testutils.NewStringValue("b"): testutils.NewStringValue(strings.Repeat("a", 1_000)),
			},
		}

		// Get nested (inlined) map

		retrievedInlinedMapElement, err := m.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"))
		require.NoError(t, err)

		retrievedInlinedMap := retrievedInlinedMapElement.(*atree.OrderedMap)
		require.True(t, atree.GetMapRootSlab(retrievedInlinedMap).Inlined())

		// Map with SlabIDStorable can't be copied.

		canCopy := retrievedInlinedMap.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("inlined map with wrapped simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": {"b": SomeValue(0)}}

		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err := nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("b"), testutils.NewSomeValue(testutils.NewUint64ValueFromInteger(0)))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"), nestedMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedNestedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("b"): testutils.NewExpectedWrapperValue(testutils.NewUint64ValueFromInteger(0)),
		}

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): expectedNestedValues,
		}

		// Get nested (inlined) map

		retrievedInlinedMapElement, err := m.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"))
		require.NoError(t, err)

		retrievedInlinedMap := retrievedInlinedMapElement.(*atree.OrderedMap)
		require.True(t, atree.GetMapRootSlab(retrievedInlinedMap).Inlined())

		// Copy map.

		canCopy := retrievedInlinedMap.CanCopyNonRefSimple()
		require.True(t, canCopy)

		copiedMap, err := retrievedInlinedMap.CopyNonRefSimple(newAddress, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, expectedNestedValues, nil, false, 2)

		// Modify copied map.

		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("b"), testutils.NewSomeValue(testutils.NewUint64ValueFromInteger(1)))
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
		require.Equal(t, testutils.SomeStorable{Storable: testutils.NewUint64ValueFromInteger(0)}, existingStorable.(testutils.SomeStorable))

		existingStorable, err = copiedMap.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("c"), testutils.NewSomeValue(testutils.NewUint64ValueFromInteger(2)))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		copiedMapExpectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("b"): testutils.NewExpectedWrapperValue(testutils.NewUint64ValueFromInteger(1)),
			testutils.NewStringValue("c"): testutils.NewExpectedWrapperValue(testutils.NewUint64ValueFromInteger(2)),
		}

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 2)
		testMap(t, storage, typeInfo, newAddress, copiedMap, copiedMapExpectedValues, nil, false, 2)
	})

	t.Run("inlined map with wrapped inlined container", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": {"b": SomeValue({})}}

		nestedMap2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err := nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("b"), testutils.NewSomeValue(nestedMap2))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"), nestedMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.ExpectedMapValue{
				testutils.NewStringValue("b"): testutils.NewExpectedWrapperValue(testutils.ExpectedMapValue{}),
			},
		}

		// Get nested (inlined) map

		retrievedInlinedMapElement, err := m.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"))
		require.NoError(t, err)

		retrievedInlinedMap := retrievedInlinedMapElement.(*atree.OrderedMap)
		require.True(t, atree.GetMapRootSlab(retrievedInlinedMap).Inlined())

		// Map with wrapped inlined container can't be copied.

		canCopy := retrievedInlinedMap.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("inlined map with wrapped SlabIDStorable", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create a map {"a": {"b": SomeValue(SlabIDStorable)}}

		nestedMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err := nestedMap.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("b"), testutils.NewSomeValue(testutils.NewStringValue(strings.Repeat("a", 1_000))))
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		existingStorable, err = m.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"), nestedMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues := testutils.ExpectedMapValue{
			testutils.NewStringValue("a"): testutils.ExpectedMapValue{
				testutils.NewStringValue("b"): testutils.NewExpectedWrapperValue(testutils.NewStringValue(strings.Repeat("a", 1_000))),
			},
		}

		// Get nested (inlined) map

		retrievedInlinedMapElement, err := m.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewStringValue("a"))
		require.NoError(t, err)

		retrievedInlinedMap := retrievedInlinedMapElement.(*atree.OrderedMap)
		require.True(t, atree.GetMapRootSlab(retrievedInlinedMap).Inlined())

		// Map with wrapped SlabIDStorable can't be copied.

		canCopy := retrievedInlinedMap.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})

	t.Run("multi-slab map", func(t *testing.T) {
		const mapCount = 1028

		storage := newTestPersistentStorage(t)

		// Create a map {"0": 0, "1": 1, ... "1027": 1027}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedValues := testutils.ExpectedMapValue{}
		for i := range mapCount {
			k := testutils.NewStringValue(strconv.Itoa(i))
			v := testutils.Uint64Value(i)
			existingStorable, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[k] = v
		}

		// Multi slab map can't be copied.

		canCopy := m.CanCopyNonRefSimple()
		require.False(t, canCopy)

		// Test map and verify storage health.
		testMap(t, storage, typeInfo, address, m, expectedValues, nil, false, 1)
	})
}
