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
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockDigesterBuilder struct {
	mock.Mock
}

var _ DigesterBuilder = &mockDigesterBuilder{}

type mockDigester struct {
	d []Digest
}

var _ Digester = &mockDigester{}

func (h *mockDigesterBuilder) SetSeed(_ uint64, _ uint64) {
}

func (h *mockDigesterBuilder) Digest(_ HashInputProvider, value Value) (Digester, error) {
	args := h.Called(value)
	return args.Get(0).(mockDigester), nil
}

func (d mockDigester) DigestPrefix(level uint) ([]Digest, error) {
	if level > uint(len(d.d)) {
		return nil, fmt.Errorf("digest level %d out of bounds", level)
	}
	return d.d[:level], nil
}

func (d mockDigester) Digest(level uint) (Digest, error) {
	if level >= uint(len(d.d)) {
		return 0, fmt.Errorf("digest level %d out of bounds", level)
	}
	return d.d[level], nil
}

func (d mockDigester) Levels() uint {
	return uint(len(d.d))
}

func (d mockDigester) Reset() {}

type errorDigesterBuilder struct {
	err error
}

var _ DigesterBuilder = &errorDigesterBuilder{}

func newErrorDigesterBuilder(err error) *errorDigesterBuilder {
	return &errorDigesterBuilder{err: err}
}

func (h *errorDigesterBuilder) SetSeed(_ uint64, _ uint64) {
}

func (h *errorDigesterBuilder) Digest(_ HashInputProvider, _ Value) (Digester, error) {
	return nil, h.err
}

func testEmptyMapV0(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	m *OrderedMap,
) {
	testMapV0(t, storage, typeInfo, address, m, nil, nil, false)
}

func testEmptyMap(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	m *OrderedMap,
) {
	testMap(t, storage, typeInfo, address, m, nil, nil, false)
}

func testMapV0(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	m *OrderedMap,
	keyValues map[Value]Value,
	sortedKeys []Value,
	hasNestedArrayMapElement bool,
) {
	_testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, hasNestedArrayMapElement, false)
}

func testMap(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	m *OrderedMap,
	keyValues mapValue,
	sortedKeys []Value,
	hasNestedArrayMapElement bool,
) {
	_testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, hasNestedArrayMapElement, true)
}

// _testMap verifies map elements and validates serialization and in-memory slab tree.
// It also verifies elements ordering if sortedKeys is not nil.
func _testMap(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	m *OrderedMap,
	expectedKeyValues map[Value]Value,
	sortedKeys []Value,
	hasNestedArrayMapElement bool,
	inlineEnabled bool,
) {
	require.True(t, typeInfoComparator(typeInfo, m.Type()))
	require.Equal(t, address, m.Address())
	require.Equal(t, uint64(len(expectedKeyValues)), m.Count())

	var err error

	// Verify map elements
	for k, expected := range expectedKeyValues {
		actual, err := m.Get(compare, hashInputProvider, k)
		require.NoError(t, err)

		valueEqual(t, expected, actual)
	}

	// Verify map elements ordering
	if len(sortedKeys) > 0 {
		require.Equal(t, len(expectedKeyValues), len(sortedKeys))

		i := 0
		err = m.IterateReadOnly(func(k, v Value) (bool, error) {
			expectedKey := sortedKeys[i]
			expectedValue := expectedKeyValues[expectedKey]

			valueEqual(t, expectedKey, k)
			valueEqual(t, expectedValue, v)

			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, len(expectedKeyValues), i)
	}

	// Verify in-memory slabs
	err = VerifyMap(m, address, typeInfo, typeInfoComparator, hashInputProvider, inlineEnabled)
	if err != nil {
		PrintMap(m)
	}
	require.NoError(t, err)

	// Verify slab serializations
	err = VerifyMapSerialization(
		m,
		storage.cborDecMode,
		storage.cborEncMode,
		storage.DecodeStorable,
		storage.DecodeTypeInfo,
		func(a, b Storable) bool {
			return reflect.DeepEqual(a, b)
		},
	)
	if err != nil {
		PrintMap(m)
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
	require.Equal(t, m.SlabID(), rootIDs[0])

	// Encode all non-nil slab
	encodedSlabs := make(map[SlabID][]byte)
	for id, slab := range storage.deltas {
		if slab != nil {
			b, err := EncodeSlab(slab, storage.cborEncMode)
			require.NoError(t, err)
			encodedSlabs[id] = b
		}
	}

	// Test decoded map from new storage to force slab decoding
	decodedMap, err := NewMapWithRootID(
		newTestPersistentStorageWithBaseStorageAndDeltas(t, storage.baseStorage, encodedSlabs),
		m.SlabID(),
		m.digesterBuilder)
	require.NoError(t, err)

	// Verify decoded map elements
	for k, expected := range expectedKeyValues {
		actual, err := decodedMap.Get(compare, hashInputProvider, k)
		require.NoError(t, err)

		valueEqual(t, expected, actual)
	}

	if !hasNestedArrayMapElement {
		// Need to call Commit before calling storage.Count() for PersistentSlabStorage.
		err = storage.Commit()
		require.NoError(t, err)

		stats, err := GetMapStats(m)
		require.NoError(t, err)
		require.Equal(t, stats.SlabCount(), uint64(storage.Count()))

		if len(expectedKeyValues) == 0 {
			// Verify slab count for empty map
			require.Equal(t, uint64(1), stats.DataSlabCount)
			require.Equal(t, uint64(0), stats.MetaDataSlabCount)
			require.Equal(t, uint64(0), stats.StorableSlabCount)
			require.Equal(t, uint64(0), stats.CollisionDataSlabCount)
		}
	}
}

type keysByDigest struct {
	keys            []Value
	digesterBuilder DigesterBuilder
}

func (d keysByDigest) Len() int { return len(d.keys) }

func (d keysByDigest) Swap(i, j int) { d.keys[i], d.keys[j] = d.keys[j], d.keys[i] }

func (d keysByDigest) Less(i, j int) bool {
	d1, err := d.digesterBuilder.Digest(hashInputProvider, d.keys[i])
	if err != nil {
		panic(err)
	}

	digest1, err := d1.DigestPrefix(d1.Levels())
	if err != nil {
		panic(err)
	}

	d2, err := d.digesterBuilder.Digest(hashInputProvider, d.keys[j])
	if err != nil {
		panic(err)
	}

	digest2, err := d2.DigestPrefix(d2.Levels())
	if err != nil {
		panic(err)
	}

	for z := 0; z < len(digest1); z++ {
		if digest1[z] != digest2[z] {
			return digest1[z] < digest2[z] // sort by hkey
		}
	}
	return i < j // sort by insertion order with hash collision
}

func TestMapSetAndGet(t *testing.T) {

	t.Run("unique keys", func(t *testing.T) {
		// Map tree will be 4 levels, with ~35 metadata slabs, and ~270 data slabs when
		// slab size is 256 bytes, number of map elements is 2048,
		// keys are strings of 16 bytes of random content, values are 0-2048,

		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			mapSize       = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		i := uint64(0)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))
			v := Uint64Value(i)
			keyValues[k] = v
			i++
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("replicate keys", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			mapSize       = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		i := uint64(0)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))
			v := Uint64Value(i)
			keyValues[k] = v
			i++
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Overwrite values
		for k, v := range keyValues {
			oldValue := v.(Uint64Value)
			newValue := Uint64Value(uint64(oldValue) + mapSize)

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, oldValue, existingValue)

			keyValues[k] = newValue
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("random key and value", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const (
			mapSize          = 2048
			keyStringMaxSize = 1024
		)

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		for len(keyValues) < mapSize {
			slen := r.Intn(keyStringMaxSize)
			k := NewStringValue(randStr(r, slen))
			v := randomValue(r, int(maxInlineMapElementSize))
			keyValues[k] = v
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("unique keys with hash collision", func(t *testing.T) {

		const (
			mapSize       = 1024
			keyStringSize = 16
		)

		SetThreshold(256)
		defer SetThreshold(1024)

		savedMaxCollisionLimitPerDigest := MaxCollisionLimitPerDigest
		MaxCollisionLimitPerDigest = uint32(math.Ceil(float64(mapSize) / 10))
		defer func() {
			MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
		}()

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[Value]Value, mapSize)
		i := uint64(0)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))
			v := Uint64Value(i)
			keyValues[k] = v
			i++

			digests := []Digest{
				Digest(i % 10),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("replicate keys with hash collision", func(t *testing.T) {
		const (
			mapSize       = 1024
			keyStringSize = 16
		)

		SetThreshold(256)
		defer SetThreshold(1024)

		savedMaxCollisionLimitPerDigest := MaxCollisionLimitPerDigest
		MaxCollisionLimitPerDigest = uint32(math.Ceil(float64(mapSize) / 10))
		defer func() {
			MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
		}()

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[Value]Value, mapSize)
		i := uint64(0)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))
			v := Uint64Value(i)
			keyValues[k] = v
			i++

			digests := []Digest{
				Digest(i % 10),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Overwrite values
		for k, v := range keyValues {
			oldValue := v.(Uint64Value)
			newValue := Uint64Value(uint64(oldValue) + mapSize)

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, oldValue, existingValue)

			keyValues[k] = newValue
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapGetKeyNotFound(t *testing.T) {
	t.Run("no collision", func(t *testing.T) {
		const mapSize = 1024

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			keyValues[k] = v
			storable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)

		k := NewStringValue(randStr(r, 1024))
		storable, err := m.Get(compare, hashInputProvider, k)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision", func(t *testing.T) {
		const mapSize = 256

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := &mockDigesterBuilder{}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			keyValues[k] = v

			digests := []Digest{Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)
		k := NewStringValue(randStr(r, 1024))

		digests := []Digest{Digest(0)}
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		storable, err := m.Get(compare, hashInputProvider, k)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision group", func(t *testing.T) {
		const mapSize = 256

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := &mockDigesterBuilder{}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			keyValues[k] = v

			digests := []Digest{Digest(i % 10)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)
		k := NewStringValue(randStr(r, 1024))

		digests := []Digest{Digest(0)}
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		storable, err := m.Get(compare, hashInputProvider, k)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapHas(t *testing.T) {

	t.Run("no error", func(t *testing.T) {
		const (
			mapSize       = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keys := make(map[Value]bool, mapSize*2)
		keysToInsert := make([]Value, 0, mapSize)
		keysToNotInsert := make([]Value, 0, mapSize)
		for len(keysToInsert) < mapSize || len(keysToNotInsert) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))
			if !keys[k] {
				keys[k] = true

				if len(keysToInsert) < mapSize {
					keysToInsert = append(keysToInsert, k)
				} else {
					keysToNotInsert = append(keysToNotInsert, k)
				}
			}
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for i, k := range keysToInsert {
			existingStorable, err := m.Set(compare, hashInputProvider, k, Uint64Value(i))
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for _, k := range keysToInsert {
			exist, err := m.Has(compare, hashInputProvider, k)
			require.NoError(t, err)
			require.True(t, exist)
		}

		for _, k := range keysToNotInsert {
			exist, err := m.Has(compare, hashInputProvider, k)
			require.NoError(t, err)
			require.False(t, exist)
		}
	})

	t.Run("error", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		testErr := errors.New("test")
		digesterBuilder := newErrorDigesterBuilder(testErr)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		exist, err := m.Has(compare, hashInputProvider, Uint64Value(0))
		// err is testErr wrapped in ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())
		require.False(t, exist)
	})
}

func testMapRemoveElement(t *testing.T, m *OrderedMap, k Value, expectedV Value) {

	removedKeyStorable, removedValueStorable, err := m.Remove(compare, hashInputProvider, k)
	require.NoError(t, err)

	removedKey, err := removedKeyStorable.StoredValue(m.Storage)
	require.NoError(t, err)
	valueEqual(t, k, removedKey)

	removedValue, err := removedValueStorable.StoredValue(m.Storage)
	require.NoError(t, err)
	valueEqual(t, expectedV, removedValue)

	if id, ok := removedKeyStorable.(SlabIDStorable); ok {
		err = m.Storage.Remove(SlabID(id))
		require.NoError(t, err)
	}

	if id, ok := removedValueStorable.(SlabIDStorable); ok {
		err = m.Storage.Remove(SlabID(id))
		require.NoError(t, err)
	}

	// Remove the same key for the second time.
	removedKeyStorable, removedValueStorable, err = m.Remove(compare, hashInputProvider, k)
	require.Equal(t, 1, errorCategorizationCount(err))
	var userError *UserError
	var keyNotFoundError *KeyNotFoundError
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &keyNotFoundError)
	require.ErrorAs(t, userError, &keyNotFoundError)
	require.Nil(t, removedKeyStorable)
	require.Nil(t, removedValueStorable)
}

func TestMapRemove(t *testing.T) {

	SetThreshold(512)
	defer SetThreshold(1024)

	const (
		mapSize              = 2048
		smallKeyStringSize   = 16
		smallValueStringSize = 16
		largeKeyStringSize   = 512
		largeValueStringSize = 512
	)

	r := newRand(t)

	smallKeyValues := make(map[Value]Value, mapSize)
	for len(smallKeyValues) < mapSize {
		k := NewStringValue(randStr(r, smallKeyStringSize))
		v := NewStringValue(randStr(r, smallValueStringSize))
		smallKeyValues[k] = v
	}

	largeKeyValues := make(map[Value]Value, mapSize)
	for len(largeKeyValues) < mapSize {
		k := NewStringValue(randStr(r, largeKeyStringSize))
		v := NewStringValue(randStr(r, largeValueStringSize))
		largeKeyValues[k] = v
	}

	testCases := []struct {
		name      string
		keyValues map[Value]Value
	}{
		{name: "small key and value", keyValues: smallKeyValues},
		{name: "large key and value", keyValues: largeKeyValues},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo := testTypeInfo{42}
			address := Address{1, 2, 3, 4, 5, 6, 7, 8}
			storage := newTestPersistentStorage(t)

			m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			// Insert elements
			for k, v := range tc.keyValues {
				existingStorable, err := m.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
			}

			testMap(t, storage, typeInfo, address, m, tc.keyValues, nil, false)

			count := len(tc.keyValues)

			// Remove all elements
			for k, v := range tc.keyValues {

				testMapRemoveElement(t, m, k, v)

				count--

				require.True(t, typeInfoComparator(typeInfo, m.Type()))
				require.Equal(t, address, m.Address())
				require.Equal(t, uint64(count), m.Count())
			}

			testEmptyMap(t, storage, typeInfo, address, m)
		})
	}

	t.Run("collision", func(t *testing.T) {
		// Test:
		// - data slab refers to an external slab containing elements with hash collision
		// - last collision element is inlined after all other collision elements are removed
		// - data slab overflows with inlined colllision element
		// - data slab splits

		SetThreshold(512)
		defer SetThreshold(1024)

		const (
			numOfElementsBeforeCollision = 54
			numOfElementsWithCollision   = 10
			numOfElementsAfterCollision  = 1
		)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		r := newRand(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		nextDigest := Digest(0)

		nonCollisionKeyValues := make(map[Value]Value)
		for i := 0; i < numOfElementsBeforeCollision; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			nonCollisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{nextDigest}})
			nextDigest++

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		collisionKeyValues := make(map[Value]Value)
		for len(collisionKeyValues) < numOfElementsWithCollision {
			k := NewStringValue(randStr(r, int(maxInlineMapKeySize)-2))
			v := NewStringValue(randStr(r, int(maxInlineMapKeySize)-2))
			collisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{nextDigest}})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		nextDigest++
		k := Uint64Value(nextDigest)
		v := Uint64Value(nextDigest)
		nonCollisionKeyValues[k] = v

		digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{nextDigest}})

		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		count := len(nonCollisionKeyValues) + len(collisionKeyValues)

		// Remove all collision elements
		for k, v := range collisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, typeInfoComparator(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, uint64(count), m.Count())
		}

		testMap(t, storage, typeInfo, address, m, nonCollisionKeyValues, nil, false)

		// Remove remaining elements
		for k, v := range nonCollisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, typeInfoComparator(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, uint64(count), m.Count())
		}

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("collision with data root", func(t *testing.T) {
		// Test:
		// - data slab refers to an external slab containing elements with hash collision
		// - last collision element is inlined after all other collision elements are removed
		// - data slab overflows with inlined colllision element
		// - data slab splits

		SetThreshold(512)
		defer SetThreshold(1024)

		const (
			numOfElementsWithCollision    = 10
			numOfElementsWithoutCollision = 35
		)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		r := newRand(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		collisionKeyValues := make(map[Value]Value)
		for len(collisionKeyValues) < numOfElementsWithCollision {
			k := NewStringValue(randStr(r, int(maxInlineMapKeySize)-2))
			v := NewStringValue(randStr(r, int(maxInlineMapKeySize)-2))
			collisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{0}})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		nonCollisionKeyValues := make(map[Value]Value)
		for i := 0; i < numOfElementsWithoutCollision; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			nonCollisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i) + 1}})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		count := len(nonCollisionKeyValues) + len(collisionKeyValues)

		// Remove all collision elements
		for k, v := range collisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, typeInfoComparator(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, uint64(count), m.Count())
		}

		testMap(t, storage, typeInfo, address, m, nonCollisionKeyValues, nil, false)

		// Remove remaining elements
		for k, v := range nonCollisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, typeInfoComparator(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, uint64(count), m.Count())
		}

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("no collision key not found", func(t *testing.T) {
		const mapSize = 1024

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			keyValues[k] = v
			storable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)

		k := NewStringValue(randStr(r, 1024))
		existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, k)
		require.Nil(t, existingKeyStorable)
		require.Nil(t, existingValueStorable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision key not found", func(t *testing.T) {
		const mapSize = 256

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := &mockDigesterBuilder{}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			keyValues[k] = v

			digests := []Digest{Digest(i % 10)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)
		k := NewStringValue(randStr(r, 1024))

		digests := []Digest{Digest(0)}
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, k)
		require.Nil(t, existingKeyStorable)
		require.Nil(t, existingValueStorable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestReadOnlyMapIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// Iterate key value pairs
		i := 0
		err = m.IterateReadOnly(func(k Value, v Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		// Iterate keys
		i = 0
		err = m.IterateReadOnlyKeys(func(k Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		// Iterate values
		i = 0
		err = m.IterateReadOnlyValues(func(v Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, mapValue{}, nil, false)
	})

	t.Run("no collision", func(t *testing.T) {
		const (
			mapSize       = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		i := uint64(0)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))
			if _, found := keyValues[k]; !found {
				keyValues[k] = Uint64Value(i)
				sortedKeys[i] = k
				i++
			}
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i = uint64(0)
		err = m.IterateReadOnly(func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		// Iterate keys
		i = uint64(0)
		err = m.IterateReadOnlyKeys(func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		// Iterate values
		i = uint64(0)
		err = m.IterateReadOnlyValues(func(v Value) (resume bool, err error) {
			k := sortedKeys[i]
			valueEqual(t, keyValues[k], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("collision", func(t *testing.T) {
		const (
			mapSize         = 1024
			keyStringSize   = 16
			valueStringSize = 16
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, 0, mapSize)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))

			if _, found := keyValues[k]; !found {
				v := NewStringValue(randStr(r, valueStringSize))
				sortedKeys = append(sortedKeys, k)
				keyValues[k] = v

				digests := []Digest{
					Digest(r.Intn(256)),
					Digest(r.Intn(256)),
					Digest(r.Intn(256)),
					Digest(r.Intn(256)),
				}
				digesterBuilder.On("Digest", k).Return(mockDigester{digests})

				existingStorable, err := m.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
			}
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.IterateReadOnly(func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		// Iterate keys
		i = uint64(0)
		err = m.IterateReadOnlyKeys(func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		// Iterate values
		i = uint64(0)
		err = m.IterateReadOnlyValues(func(v Value) (resume bool, err error) {
			valueEqual(t, keyValues[sortedKeys[i]], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})
}

func TestMutateElementFromReadOnlyMapIterator(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)
	digesterBuilder := newBasicDigesterBuilder()

	var mutationError *ReadOnlyIteratorElementMutationError

	t.Run("mutate inlined map key from IterateReadOnly", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// parent map {{}: 0}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, NewHashableMap(childMapKey), Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMapKey.Inlined())

		// Iterate elements and modify key
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := k.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(0), Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
				require.Equal(t, childMapKey.ValueID(), k.(mutableValueNotifier).ValueID())
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map value from IterateReadOnly", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Iterate elements and modify value
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(0), Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				require.Equal(t, childMap.ValueID(), v.(mutableValueNotifier).ValueID())
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map key from IterateReadOnlyKeys", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// parent map {{}: 0}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, NewHashableMap(childMapKey), Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMapKey.Inlined())

		// Iterate and modify key
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(0), Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v Value) {
				keyMutationCallbackCalled = true
				require.Equal(t, childMapKey.ValueID(), v.(mutableValueNotifier).ValueID())
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate inlined map value from IterateReadOnlyValues", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Iterate and modify value
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(1), Uint64Value(1))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				require.Equal(t, childMap.ValueID(), v.(mutableValueNotifier).ValueID())
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key from IterateReadOnly", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// Inserting elements into childMapKey so it can't be inlined
		const size = 20
		for i := 0; i < size; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			existingStorable, err := childMapKey.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// parent map {{...}: 0}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, NewHashableMap(childMapKey), Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.False(t, childMapKey.Inlined())

		// Iterate elements and modify key
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := k.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
				require.Equal(t, childMapKey.ValueID(), k.(mutableValueNotifier).ValueID())
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value from IterateReadOnly", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Inserting elements into childMap until it is no longer inlined
		for i := 0; childMap.Inlined(); i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify value
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				require.Equal(t, childMap.ValueID(), v.(mutableValueNotifier).ValueID())
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key from IterateReadOnlyKeys", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// Inserting elements into childMap so it can't be inlined.
		const size = 20
		for i := 0; i < size; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			existingStorable, err := childMapKey.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// parent map {{...}: 0}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, NewHashableMap(childMapKey), Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.False(t, childMapKey.Inlined())

		// Iterate and modify key
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v Value) {
				keyMutationCallbackCalled = true
				require.Equal(t, childMapKey.ValueID(), v.(mutableValueNotifier).ValueID())
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value from IterateReadOnlyValues", func(t *testing.T) {
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(compare, hashInputProvider, Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Inserting elements into childMap until it is no longer inlined
		for i := 0; childMap.Inlined(); i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify value
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				require.Equal(t, childMap.ValueID(), v.(mutableValueNotifier).ValueID())
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map key in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMapKey2 {}
		childMapKey2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {{}:0, {}:1} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMapKey1, childMapKey2} {
			k := NewHashableMap(m)
			v := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because Digest is called again on OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate element and modify key
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := k.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(0), Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
				vid := k.(mutableValueNotifier).ValueID()
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map value in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMap1, childMap2} {
			k := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify values
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(1), Uint64Value(1))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				vid := v.(mutableValueNotifier).ValueID()
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map key in collision from IterateReadOnlyKeys", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMapKey2 {}
		childMapKey2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {{}: 0, {}: 1} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMapKey1, childMapKey2} {
			k := NewHashableMap(m)
			v := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because Digest is called again on OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify keys
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(0), Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v Value) {
				keyMutationCallbackCalled = true
				vid := v.(mutableValueNotifier).ValueID()
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate inlined map value in collision from IterateReadOnlyValues", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMap1, childMap2} {
			k := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify values
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(compare, hashInputProvider, Uint64Value(0), Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				vid := v.(mutableValueNotifier).ValueID()
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		const size = 20
		for i := 0; i < size; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMapKey1.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// childMapKey2 {}
		childMapKey2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for i := 0; i < size; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMapKey2.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMapKey1, childMapKey2} {
			k := NewHashableMap(m)
			v := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because Digest is called again on OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify keys
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := k.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
				vid := k.(mutableValueNotifier).ValueID()
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			},
			func(v Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMap1, childMap2} {
			k := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := 0; childMap1.Inlined(); i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMap1.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := 0; childMap2.Inlined(); i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMap2.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify values
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k Value, v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(k Value) {
				keyMutationCallbackCalled = true
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				vid := v.(mutableValueNotifier).ValueID()
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key in collision from IterateReadOnlyKeys", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		size := 20
		for i := 0; i < size; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMapKey1.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// childMapKey2 {}
		childMapKey2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for i := 0; i < size; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMapKey2.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMapKey1, childMapKey2} {
			k := NewHashableMap(m)
			v := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because Digest is called again on OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify keys
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v Value) {
				keyMutationCallbackCalled = true
				vid := v.(mutableValueNotifier).ValueID()
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value in collision from IterateReadOnlyValues", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*OrderedMap{childMap1, childMap2} {
			k := Uint64Value(i)

			digests := []Digest{Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := 0; childMap1.Inlined(); i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMap1.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := 0; childMap2.Inlined(); i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := childMap2.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify values
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v Value) (resume bool, err error) {
				c, ok := v.(*OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(compare, hashInputProvider, Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v Value) {
				valueMutationCallbackCalled = true
				vid := v.(mutableValueNotifier).ValueID()
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})
}

func TestMutableMapIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// Iterate key value pairs
		i := 0
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, mapValue{}, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) * 2

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 25

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			sortedKeys[i] = k
			keyValues[k] = v
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) * 2

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 25

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 10

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		r := 'a'
		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision primitive values, 1 level", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) / 2
			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision primitive values, 4 levels", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) / 2
			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize = 15
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := Uint64Value(i)

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		const (
			mapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := Uint64Value(i)

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 1
			mutatedChildMapSize = 5
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 35
			childMapSize        = 1
			mutatedChildMapSize = 5
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 10
			mutatedChildMapSize = 1
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j >= mutatedChildMapSize; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision inlined container, 1 level", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := Uint64Value(0)
			cv := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			digests := []Digest{
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision inlined container, 4 levels", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := Uint64Value(0)
			cv := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			digests := []Digest{
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container", func(t *testing.T) {
		const (
			mapSize         = 15
			valueStringSize = 16
		)

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		i := uint64(0)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := NewStringValue(randStr(r, valueStringSize))

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)
			sortedKeys[i] = k

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i = uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			newChildMapKey := Uint64Value(1) // Previous key is 0
			newChildMapValue := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, newChildMapKey, newChildMapValue)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[newChildMapKey] = newChildMapValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Iterate and mutate child map (removing elements)
		i = uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			// Remove key 0
			ck := Uint64Value(0)

			existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, ck)
			require.NoError(t, err)
			require.NotNil(t, existingKeyStorable)
			require.NotNil(t, existingValueStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			delete(expectedChildMapValues, ck)

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 1
			mutatedChildMapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 5
			mutatedChildMapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 35
			mutatedChildMapSize = 1
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j > mutatedChildMapSize-1; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 35
			mutatedChildMapSize = 10
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.Iterate(compare, hashInputProvider, func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j > mutatedChildMapSize-1; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMutableMapIterateKeys(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		i := 0
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, mapValue{}, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(Uint64Value) * 2

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 25

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			sortedKeys[i] = k
			keyValues[k] = v
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(Uint64Value) * 2

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 25

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 10

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		r := 'a'
		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (bool, error) {
			valueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision primitive values, 1 level", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(Uint64Value) / 2
			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision primitive values, 4 levels", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(Uint64Value) / 2
			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize = 15
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := Uint64Value(i)

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		const (
			mapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := Uint64Value(i)

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 1
			mutatedChildMapSize = 5
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 35
			childMapSize        = 1
			mutatedChildMapSize = 5
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 10
			mutatedChildMapSize = 1
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j >= mutatedChildMapSize; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision inlined container, 1 level", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := Uint64Value(0)
			cv := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			digests := []Digest{
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision inlined container, 4 levels", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := Uint64Value(0)
			cv := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			digests := []Digest{
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container", func(t *testing.T) {
		const (
			mapSize         = 15
			valueStringSize = 16
		)

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		i := uint64(0)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := NewStringValue(randStr(r, valueStringSize))

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)
			sortedKeys[i] = k

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i = uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			newChildMapKey := Uint64Value(1) // Previous key is 0
			newChildMapValue := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, newChildMapKey, newChildMapValue)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[newChildMapKey] = newChildMapValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Iterate and mutate child map (removing elements)
		i = uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			// Remove key 0
			ck := Uint64Value(0)

			existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, ck)
			require.NoError(t, err)
			require.NotNil(t, existingKeyStorable)
			require.NotNil(t, existingValueStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			delete(expectedChildMapValues, ck)

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 1
			mutatedChildMapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 5
			mutatedChildMapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 35
			mutatedChildMapSize = 1
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j > mutatedChildMapSize-1; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 35
			mutatedChildMapSize = 10
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateKeys(compare, hashInputProvider, func(k Value) (resume bool, err error) {
			valueEqual(t, sortedKeys[i], k)

			v, err := m.Get(compare, hashInputProvider, k)
			require.NoError(t, err)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j > mutatedChildMapSize-1; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMutableMapIterateValues(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		i := 0
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, mapValue{}, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (bool, error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) * 2

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 25

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			sortedKeys[i] = k
			keyValues[k] = v
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (bool, error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) * 2

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 15

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (bool, error) {
			k := sortedKeys[i]

			valueEqual(t, sortedKeys[i], k)
			valueEqual(t, keyValues[k], v)

			newValue := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 25

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (bool, error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			newValue := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 10

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		r := 'a'
		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (bool, error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			newValue := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++
			r++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapSize, i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision primitive values, 1 level", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) / 2
			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision primitive values, 4 levels", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			newValue := v.(Uint64Value) / 2
			existingStorable, err := m.Set(compare, hashInputProvider, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize = 15
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := Uint64Value(i)

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		const (
			mapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := Uint64Value(i)

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 1
			mutatedChildMapSize = 5
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 35
			childMapSize        = 1
			mutatedChildMapSize = 5
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 10
			mutatedChildMapSize = 1
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j >= mutatedChildMapSize; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision inlined container, 1 level", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := Uint64Value(0)
			cv := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			digests := []Digest{
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision inlined container, 4 levels", func(t *testing.T) {
		const (
			mapSize = 1024
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := Uint64Value(0)
			cv := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			digests := []Digest{
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
				Digest(r.Intn(256)),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			childKey := Uint64Value(0)
			childNewValue := Uint64Value(i)

			existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container", func(t *testing.T) {
		const (
			mapSize         = 15
			valueStringSize = 16
		)

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		i := uint64(0)
		for i := 0; i < mapSize; i++ {
			ck := Uint64Value(0)
			cv := NewStringValue(randStr(r, valueStringSize))

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)
			sortedKeys[i] = k

			existingStorable, err = m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = mapValue{ck: cv}
		}
		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i = uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			newChildMapKey := Uint64Value(1) // Previous key is 0
			newChildMapValue := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, newChildMapKey, newChildMapValue)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			expectedChildMapValues[newChildMapKey] = newChildMapValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Iterate and mutate child map (removing elements)
		i = uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			// Remove key 0
			ck := Uint64Value(0)

			existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, ck)
			require.NoError(t, err)
			require.NotNil(t, existingKeyStorable)
			require.NotNil(t, existingValueStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			delete(expectedChildMapValues, ck)

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 1
			mutatedChildMapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 5
			mutatedChildMapSize = 35
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize; j < mutatedChildMapSize; j++ {
				childKey := Uint64Value(j)
				childValue := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 35
			mutatedChildMapSize = 1
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j > mutatedChildMapSize-1; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapSize             = 15
			childMapSize        = 35
			mutatedChildMapSize = 10
		)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := 0; i < mapSize; i++ {

			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(mapValue)
			for j := 0; j < childMapSize; j++ {
				ck := Uint64Value(j)
				cv := Uint64Value(j)

				existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := Uint64Value(i)

			existingStorable, err := m.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.True(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := uint64(0)
		err = m.IterateValues(compare, hashInputProvider, func(v Value) (resume bool, err error) {
			k := sortedKeys[i]

			valueEqual(t, keyValues[k], v)

			childMap, ok := v.(*OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(childMapSize), childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(mapValue)
			require.True(t, ok)

			for j := childMapSize - 1; j > mutatedChildMapSize-1; j-- {
				childKey := Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(compare, hashInputProvider, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, uint64(mutatedChildMapSize), childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)
		require.False(t, m.root.IsData())

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func testMapDeterministicHashCollision(t *testing.T, r *rand.Rand, maxDigestLevel int) {

	const (
		mapSize            = 1024
		keyStringMaxSize   = 1024
		valueStringMaxSize = 1024

		// mockDigestCount is the number of unique set of digests.
		// Each set has maxDigestLevel of digest.
		mockDigestCount = 8
	)

	uniqueFirstLevelDigests := make(map[Digest]bool, mockDigestCount)
	firstLevelDigests := make([]Digest, 0, mockDigestCount)
	for len(firstLevelDigests) < mockDigestCount {
		d := Digest(uint64(r.Intn(256)))
		if !uniqueFirstLevelDigests[d] {
			uniqueFirstLevelDigests[d] = true
			firstLevelDigests = append(firstLevelDigests, d)
		}
	}

	digestsGroup := make([][]Digest, mockDigestCount)
	for i := 0; i < mockDigestCount; i++ {
		digests := make([]Digest, maxDigestLevel)
		digests[0] = firstLevelDigests[i]
		for j := 1; j < maxDigestLevel; j++ {
			digests[j] = Digest(uint64(r.Intn(256)))
		}
		digestsGroup[i] = digests
	}

	digesterBuilder := &mockDigesterBuilder{}

	keyValues := make(map[Value]Value, mapSize)
	i := 0
	for len(keyValues) < mapSize {
		k := NewStringValue(randStr(r, r.Intn(keyStringMaxSize)))
		if _, found := keyValues[k]; !found {
			keyValues[k] = NewStringValue(randStr(r, r.Intn(valueStringMaxSize)))

			index := i % len(digestsGroup)
			digests := digestsGroup[index]
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			i++
		}
	}

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	stats, err := GetMapStats(m)
	require.NoError(t, err)
	require.Equal(t, uint64(mockDigestCount), stats.CollisionDataSlabCount)

	// Remove all elements
	for k, v := range keyValues {
		removedKeyStorable, removedValueStorable, err := m.Remove(compare, hashInputProvider, k)
		require.NoError(t, err)

		removedKey, err := removedKeyStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, k, removedKey)

		removedValue, err := removedValueStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, v, removedValue)

		if id, ok := removedKeyStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		if id, ok := removedValueStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}
	}

	testEmptyMap(t, storage, typeInfo, address, m)
}

func testMapRandomHashCollision(t *testing.T, r *rand.Rand, maxDigestLevel int) {

	const (
		mapSize            = 1024
		keyStringMaxSize   = 1024
		valueStringMaxSize = 1024
	)

	digesterBuilder := &mockDigesterBuilder{}

	keyValues := make(map[Value]Value, mapSize)
	for len(keyValues) < mapSize {
		k := NewStringValue(randStr(r, r.Intn(keyStringMaxSize)))

		if _, found := keyValues[k]; !found {
			keyValues[k] = NewStringValue(randStr(r, valueStringMaxSize))

			var digests []Digest
			for i := 0; i < maxDigestLevel; i++ {
				digests = append(digests, Digest(r.Intn(256)))
			}

			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}
	}

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	// Remove all elements
	for k, v := range keyValues {
		removedKeyStorable, removedValueStorable, err := m.Remove(compare, hashInputProvider, k)
		require.NoError(t, err)

		removedKey, err := removedKeyStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, k, removedKey)

		removedValue, err := removedValueStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, v, removedValue)

		if id, ok := removedKeyStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		if id, ok := removedValueStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}
	}

	testEmptyMap(t, storage, typeInfo, address, m)
}

func TestMapHashCollision(t *testing.T) {

	SetThreshold(512)
	defer SetThreshold(1024)

	const maxDigestLevel = 4

	r := newRand(t)

	for hashLevel := 1; hashLevel <= maxDigestLevel; hashLevel++ {
		name := fmt.Sprintf("deterministic max hash level %d", hashLevel)
		t.Run(name, func(t *testing.T) {
			testMapDeterministicHashCollision(t, r, hashLevel)
		})
	}

	for hashLevel := 1; hashLevel <= maxDigestLevel; hashLevel++ {
		name := fmt.Sprintf("random max hash level %d", hashLevel)
		t.Run(name, func(t *testing.T) {
			testMapRandomHashCollision(t, r, hashLevel)
		})
	}
}

func testMapSetRemoveRandomValues(
	t *testing.T,
	r *rand.Rand,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
) (*OrderedMap, map[Value]Value) {

	const (
		MapSetOp = iota
		MapRemoveOp
		MapMaxOp
	)

	const (
		opCount         = 4096
		digestMaxValue  = 256
		digestMaxLevels = 4
	)

	digesterBuilder := &mockDigesterBuilder{}

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	keyValues := make(map[Value]Value)
	var keys []Value
	for i := uint64(0); i < opCount; i++ {

		nextOp := r.Intn(MapMaxOp)

		if m.Count() == 0 {
			nextOp = MapSetOp
		}

		switch nextOp {

		case MapSetOp:

			k := randomValue(r, int(maxInlineMapElementSize))
			v := randomValue(r, int(maxInlineMapElementSize))

			var digests []Digest
			for i := 0; i < digestMaxLevels; i++ {
				digests = append(digests, Digest(r.Intn(digestMaxValue)))
			}

			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)

			if oldv, ok := keyValues[k]; ok {
				require.NotNil(t, existingStorable)

				existingValue, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)
				valueEqual(t, oldv, existingValue)

				if id, ok := existingStorable.(SlabIDStorable); ok {
					err = storage.Remove(SlabID(id))
					require.NoError(t, err)
				}
			} else {
				require.Nil(t, existingStorable)

				keys = append(keys, k)
			}

			keyValues[k] = v

		case MapRemoveOp:
			index := r.Intn(len(keys))
			k := keys[index]

			removedKeyStorable, removedValueStorable, err := m.Remove(compare, hashInputProvider, k)
			require.NoError(t, err)

			removedKey, err := removedKeyStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, k, removedKey)

			removedValue, err := removedValueStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, keyValues[k], removedValue)

			if id, ok := removedKeyStorable.(SlabIDStorable); ok {
				err := storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			if id, ok := removedValueStorable.(SlabIDStorable); ok {
				err := storage.Remove(SlabID(id))
				require.NoError(t, err)
			}

			delete(keyValues, k)
			copy(keys[index:], keys[index+1:])
			keys = keys[:len(keys)-1]
		}

		require.True(t, typeInfoComparator(typeInfo, m.Type()))
		require.Equal(t, address, m.Address())
		require.Equal(t, uint64(len(keys)), m.Count())
	}

	return m, keyValues
}

func TestMapSetRemoveRandomValues(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	storage := newTestPersistentStorage(t)
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, keyValues := testMapSetRemoveRandomValues(t, r, storage, typeInfo, address)

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
}

func TestMapDecodeV0(t *testing.T) {

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {

		mapSlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		slabData := map[SlabID][]byte{
			mapSlabID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 0 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		}

		// Decode data to new storage
		storage := newTestPersistentStorageWithData(t, slabData)

		// Test new map from storage
		decodedMap, err := NewMapWithRootID(storage, mapSlabID, NewDefaultDigesterBuilder())
		require.NoError(t, err)

		testEmptyMapV0(t, storage, typeInfo, address, decodedMap)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapSize = 1
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})
		}

		mapSlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		slabData := map[SlabID][]byte{

			mapSlabID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2a,
				// count: 1
				0x01,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// element: [uint64(0):uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
			},
		}

		// Decode data to new storage
		storage := newTestPersistentStorageWithData(t, slabData)

		// Test new map from storage
		decodedMap, err := NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("has pointer no collision", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		const mapSize = 8
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize-1; i++ {
			k := NewStringValue(strings.Repeat(string(r), 22))
			v := NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			r++
		}

		// Create nested array
		typeInfo2 := testTypeInfo{43}

		mapSlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		nestedSlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

		childArray, err := NewArray(storage, address, typeInfo2)
		childArray.root.SetSlabID(nestedSlabID)
		require.NoError(t, err)

		err = childArray.Append(Uint64Value(0))
		require.NoError(t, err)

		k := NewStringValue(strings.Repeat(string(r), 22))

		keyValues[k] = arrayValue{Uint64Value(0)}

		digests := []Digest{Digest(mapSize - 1), Digest((mapSize - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		slabData := map[SlabID][]byte{

			// metadata slab
			mapSlabID: {
				// extra data
				// version
				0x00,
				// flag: root + map meta
				0x89,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + meta
				0x89,
				// child header count
				0x00, 0x02,
				// child header 1 (slab id, first key, size)
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// NOTE: size is modified to pass validation test.
				// This shouldn't affect migration because metadata slab is recreated when elements are migrated.
				0x00, 0x00, 0x00, 0xf6,
				// child header 2
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// NOTE: size is modified to pass validation test.
				// This shouldn't affect migration because metadata slab is recreated when elements are migrated.
				0x00, 0x00, 0x00, 0xf2,
			},

			// data slab
			id2: {
				// version
				0x00,
				// flag: map data
				0x08,
				// next slab id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:dddddddddddddddddddddd]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
			},

			// data slab
			id3: {
				// version
				0x00,
				// flag: has pointer + map data
				0x48,
				// next slab id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
			},

			// array data slab
			nestedSlabID: {
				// version
				0x00,
				// flag: root + array data
				0x80,
				// extra data (CBOR encoded array of 1 elements)
				0x81,
				// type info
				0x18, 0x2b,

				// version
				0x00,
				// flag: root + array data
				0x80,
				// CBOR encoded array head (fixed size 3 byte)
				0x99, 0x00, 0x01,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
			},
		}

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, slabData)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 1 level", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapSize = 8
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{Digest(i % 4), Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			keyValues[k] = v
		}

		mapSlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		slabData := map[SlabID][]byte{

			// map metadata slab
			mapSlabID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 2 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,

				// inline collision group corresponding to hkey 0
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(0), uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
				// element: [uint64(4), uint64(8)]
				0x82, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x08,

				// inline collision group corresponding to hkey 1
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(1), uint64(2)]
				0x82, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02,
				// element: [uint64(5), uint64(10)]
				0x82, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x0a,

				// inline collision group corresponding to hkey 2
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(2), uint64(4)]
				0x82, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x04,
				// element: [uint64(6), uint64(12)]
				0x82, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x0c,

				// inline collision group corresponding to hkey 3
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(3), uint64(6)]
				0x82, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x06,
				// element: [uint64(7), uint64(14)]
				0x82, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x0e,
			},
		}

		// Decode data to new storage
		storage := newTestPersistentStorageWithData(t, slabData)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 2 levels", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapSize = 8
		keyValues := make(map[Value]Value)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{Digest(i % 4), Digest(i % 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			keyValues[k] = v
		}

		mapSlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		slabData := map[SlabID][]byte{

			// map metadata slab
			mapSlabID: {
				// extra data
				// version
				0x00,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + map data
				0x88,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,

				// inline collision group corresponding to hkey 0
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// inline collision group corresponding to hkey [0, 0]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(0), uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
				// element: [uint64(4), uint64(8)]
				0x82, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x08,

				// inline collision group corresponding to hkey 1
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 1 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// inline collision group corresponding to hkey [1, 1]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(1), uint64(2)]
				0x82, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02,
				// element: [uint64(5), uint64(10)]
				0x82, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x0a,

				// inline collision group corresponding to hkey 2
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 element)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// inline collision group corresponding to hkey [2, 0]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 element)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(2), uint64(4)]
				0x82, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x04,
				// element: [uint64(6), uint64(12)]
				0x82, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x0c,

				// inline collision group corresponding to hkey 3
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 1 element)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// inline collision group corresponding to hkey [3, 1]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 element)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// element: [uint64(3), uint64(6)]
				0x82, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x06,
				// element: [uint64(7), uint64(14)]
				0x82, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x0e,
			},
		}

		// Decode data to new storage
		storage := newTestPersistentStorageWithData(t, slabData)

		// Test new map from storage
		decodedMap, err := NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("external collision", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapSize = 20
		keyValues := make(map[Value]Value)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{Digest(i % 2), Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			keyValues[k] = v
		}

		mapSlabID := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		slabData := map[SlabID][]byte{

			// map data slab
			mapSlabID: {
				// extra data
				// version
				0x00,
				// flag: root + has pointer + map data
				0xc8,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 10
				0x14,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// version
				0x00,
				// flag: root + has pointer + map data
				0xc8,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,

				// external collision group corresponding to hkey 0
				// (tag number CBORTagExternalCollisionGroup)
				0xd8, 0xfe,
				// (tag content: slab id)
				0xd8, 0xff, 0x50,
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,

				// external collision group corresponding to hkey 1
				// (tag number CBORTagExternalCollisionGroup)
				0xd8, 0xfe,
				// (tag content: slab id)
				0xd8, 0xff, 0x50,
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			},

			// external collision group
			id2: {
				// version
				0x00,
				// flag: any size + collision group
				0x2b,
				// next slab id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 10)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 8
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 10
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
				// hkey: 12
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c,
				// hkey: 14
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0e,
				// hkey: 16
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
				// hkey: 18
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12,

				// elements (array of 10 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
				// element: [uint64(0), uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
				// element: [uint64(2), uint64(4)]
				0x82, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x04,
				// element: [uint64(4), uint64(8)]
				0x82, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x08,
				// element: [uint64(6), uint64(12)]
				0x82, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x0c,
				// element: [uint64(8), uint64(16)]
				0x82, 0xd8, 0xa4, 0x08, 0xd8, 0xa4, 0x10,
				// element: [uint64(10), uint64(20)]
				0x82, 0xd8, 0xa4, 0x0a, 0xd8, 0xa4, 0x14,
				// element: [uint64(12), uint64(24)]
				0x82, 0xd8, 0xa4, 0x0c, 0xd8, 0xa4, 0x18, 0x18,
				// element: [uint64(14), uint64(28)]
				0x82, 0xd8, 0xa4, 0x0e, 0xd8, 0xa4, 0x18, 0x1c,
				// element: [uint64(16), uint64(32)]
				0x82, 0xd8, 0xa4, 0x10, 0xd8, 0xa4, 0x18, 0x20,
				// element: [uint64(18), uint64(36)]
				0x82, 0xd8, 0xa4, 0x12, 0xd8, 0xa4, 0x18, 0x24,
			},

			// external collision group
			id3: {
				// version
				0x00,
				// flag: any size + collision group
				0x2b,
				// next slab id
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 10)
				0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
				// hkey: 9
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
				// hkey: 11
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,
				// hkey: 13
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d,
				// hkey: 15
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f,
				// hkey: 17
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11,
				// hkey: 19
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13,

				// elements (array of 10 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
				// element: [uint64(1), uint64(2)]
				0x82, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02,
				// element: [uint64(3), uint64(6)]
				0x82, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x06,
				// element: [uint64(5), uint64(10)]
				0x82, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x0a,
				// element: [uint64(7), uint64(14)]
				0x82, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x0e,
				// element: [uint64(9), uint64(18)]
				0x82, 0xd8, 0xa4, 0x09, 0xd8, 0xa4, 0x12,
				// element: [uint64(11), uint64(22))]
				0x82, 0xd8, 0xa4, 0x0b, 0xd8, 0xa4, 0x16,
				// element: [uint64(13), uint64(26)]
				0x82, 0xd8, 0xa4, 0x0d, 0xd8, 0xa4, 0x18, 0x1a,
				// element: [uint64(15), uint64(30)]
				0x82, 0xd8, 0xa4, 0x0f, 0xd8, 0xa4, 0x18, 0x1e,
				// element: [uint64(17), uint64(34)]
				0x82, 0xd8, 0xa4, 0x11, 0xd8, 0xa4, 0x18, 0x22,
				// element: [uint64(19), uint64(38)]
				0x82, 0xd8, 0xa4, 0x13, 0xd8, 0xa4, 0x18, 0x26,
			},
		}

		// Decode data to new storage
		storage := newTestPersistentStorageWithData(t, slabData)

		// Test new map from storage
		decodedMap, err := NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})
}

func TestMapEncodeDecode(t *testing.T) {

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {

		storage := newTestBasicStorage(t)

		// Create map
		m, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		expected := map[SlabID][]byte{
			id1: {
				// version
				0x10,
				// flag: root + map data
				0x88,

				// extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 0
				0x00,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x59, 0x00, 0x00,

				// elements (array of 0 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x00,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 1, len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, NewDefaultDigesterBuilder())
		require.NoError(t, err)

		testEmptyMap(t, storage2, typeInfo, address, decodedMap)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 1
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, uint64(mapSize), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			id1: {
				// version
				0x10,
				// flag: root + map data
				0x88,

				// extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 1
				0x01,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 1)
				0x59, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x01,
				// element: [uint64(0):uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("has inlined array", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 8
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize-1; i++ {
			k := NewStringValue(strings.Repeat(string(r), 22))
			v := NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child array
		typeInfo2 := testTypeInfo{43}

		childArray, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		err = childArray.Append(Uint64Value(0))
		require.NoError(t, err)

		k := NewStringValue(strings.Repeat(string(r), 22))
		v := childArray

		keyValues[k] = arrayValue{Uint64Value(0)}

		digests := []Digest{Digest(mapSize - 1), Digest((mapSize - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert nested array
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(mapSize), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// metadata slab
			id1: {
				// version
				0x10,
				// flag: root + map meta
				0x89,

				// extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// child shared address
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,

				// child header count
				0x00, 0x02,
				// child header 1 (slab id, first key, size)
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0xf6,
				// child header 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0xf3,
			},

			// data slab
			id2: {
				// version
				0x12,
				// flag: map data
				0x08,
				// next slab id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:dddddddddddddddddddddd]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
			},

			// data slab
			id3: {
				// version
				0x11,
				// flag: has inlined slab + map data
				0x08,

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

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xfa, 0x83, 0x18, 0x00, 0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x99, 0x00, 0x01, 0xd8, 0xa4, 0x0,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])
		require.Equal(t, expected[id3], stored[id3])

		// Verify slab size in header is correct.
		meta, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)
		require.Equal(t, 2, len(meta.childrenHeaders))
		require.Equal(t, uint32(len(stored[id2])), meta.childrenHeaders[0].size)

		const inlinedExtraDataSize = 8
		require.Equal(t, uint32(len(stored[id3])-inlinedExtraDataSize+SlabIDLength), meta.childrenHeaders[1].size)

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, inlined child map of same type", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize; i++ {

			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := Uint64Value(i)
			cv := Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = mapValue{ck: cv}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x81,
				0x18, 0x2b,
				// element 1: array of inlined extra data
				0x82,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element 0:
				0x82,
				// key: "a"
				0x61, 0x61,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: "b"
				0x61, 0x62,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xca, 0x96, 0x9f, 0xeb, 0x5f, 0x29, 0x4f, 0xb9,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: 1
				0xd8, 0xa4, 0x02,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, inlined child map of different type", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo1 := testTypeInfo{43}
		childMapTypeInfo2 := testTypeInfo{44}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize; i++ {

			var ti TypeInfo
			if i%2 == 0 {
				ti = childMapTypeInfo2
			} else {
				ti = childMapTypeInfo1
			}

			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), ti)
			require.NoError(t, err)

			ck := Uint64Value(i)
			cv := Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = mapValue{ck: cv}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x82,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2c,
				// count: 1
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// element 1
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2b,
				// count: 1
				0x01,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element 0:
				0x82,
				// key: "a"
				0x61, 0x61,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: "b"
				0x61, 0x62,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xca, 0x96, 0x9f, 0xeb, 0x5f, 0x29, 0x4f, 0xb9,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: 1
				0xd8, 0xa4, 0x02,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, multiple levels of inlined child map of same type", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize; i++ {
			// Create grand child map
			gchildMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			gck := Uint64Value(i)
			gcv := Uint64Value(i * 2)

			// Insert element to grand child map
			existingStorable, err := gchildMap.Set(compare, hashInputProvider, gck, gcv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := Uint64Value(i)

			// Insert grand child map to child map
			existingStorable, err = childMap.Set(compare, hashInputProvider, ck, gchildMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = mapValue{ck: mapValue{gck: gcv}}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x81,
				0x18, 0x2b,
				// element 1: array of inlined extra data
				0x84,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,
				// element 1
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// element 2
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0xeb, 0x0e, 0x1d, 0xca, 0x7a, 0x7e, 0xe1, 0x19,
				// element 3
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x8d, 0x99, 0xcc, 0x54, 0xc8, 0x6b, 0xab, 0x50,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,

				// element 0:
				0x82,
				// key: "a"
				0x61, 0x61,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xc0, 0xba, 0xe2, 0x41, 0xcf, 0xda, 0xb7, 0x84,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined grand child map (tag: CBORTagInlineMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x1,
				// inlined map slab index
				0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2,
				// inlined grand child map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x0,
				// value: 0
				0xd8, 0xa4, 0x0,

				// element 1:
				0x82,
				// key: "b"
				0x61, 0x62,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 2
				0x18, 0x02,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x3a, 0x2d, 0x24, 0x7c, 0xca, 0xdf, 0xa0, 0x58,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: inlined grand child map (tag: CBORTagInlineMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 3
				0x18, 0x3,
				// inlined map slab index
				0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4,
				// inlined grand child map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x68, 0x9f, 0x33, 0x33, 0x89, 0x0d, 0x89, 0xd1,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x1,
				// value: 2
				0xd8, 0xa4, 0x2,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, multiple levels of inlined child map of different type", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo1 := testTypeInfo{43}
		childMapTypeInfo2 := testTypeInfo{44}
		gchildMapTypeInfo1 := testTypeInfo{45}
		gchildMapTypeInfo2 := testTypeInfo{46}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize; i++ {
			var gti TypeInfo
			if i%2 == 0 {
				gti = gchildMapTypeInfo2
			} else {
				gti = gchildMapTypeInfo1
			}

			// Create grand child map
			gchildMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), gti)
			require.NoError(t, err)

			gck := Uint64Value(i)
			gcv := Uint64Value(i * 2)

			// Insert element to grand child map
			existingStorable, err := gchildMap.Set(compare, hashInputProvider, gck, gcv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			var cti TypeInfo
			if i%2 == 0 {
				cti = childMapTypeInfo2
			} else {
				cti = childMapTypeInfo1
			}

			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), cti)
			require.NoError(t, err)

			ck := Uint64Value(i)

			// Insert grand child map to child map
			existingStorable, err = childMap.Set(compare, hashInputProvider, ck, gchildMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = mapValue{ck: mapValue{gck: gcv}}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version 1, flag: has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of type info
				0x80,
				// element 1: array of extra data
				0x84,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2c,
				// count: 1
				0x01,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,

				// element 1
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2e,
				// count: 1
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,

				// element 2
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2b,
				// count: 1
				0x01,
				// seed
				0x1b, 0xeb, 0x0e, 0x1d, 0xca, 0x7a, 0x7e, 0xe1, 0x19,

				// element 3
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2d,
				// count: 1
				0x01,
				// seed
				0x1b, 0x8d, 0x99, 0xcc, 0x54, 0xc8, 0x6b, 0xab, 0x50,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,

				// element 0:
				0x82,
				// key: "a"
				0x61, 0x61,
				// value: inlined child map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined child map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xc0, 0xba, 0xe2, 0x41, 0xcf, 0xda, 0xb7, 0x84,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined grand child map (tag: CBORTagInlineMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x1,
				// inlined map slab index
				0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2,
				// inlined grand child map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: "b"
				0x61, 0x62,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 2
				0x18, 0x02,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x3a, 0x2d, 0x24, 0x7c, 0xca, 0xdf, 0xa0, 0x58,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: inlined grand child map (tag: CBORTagInlineMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 3
				0x18, 0x3,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// inlined grand child map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x68, 0x9f, 0x33, 0x33, 0x89, 0x0d, 0x89, 0xd1,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x1,
				// value: 2
				0xd8, 0xa4, 0x2,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root metadata slab, inlined child map of same type", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 8
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize; i++ {

			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := Uint64Value(i)
			cv := Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = mapValue{ck: cv}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 10}) // inlined maps index 2-9
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 11})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version
				0x10,
				// flag: root + map metadata
				0x89,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// child shared address
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,

				// child header count
				0x00, 0x02,
				// child header 1 (slab id, first key, size)
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0xda,
				// child header 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0xda,
			},
			id2: {
				// version, flag: has inlined slab, has next slab ID
				0x13,
				// flag: map data
				0x08,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x81,
				0x18, 0x2b,
				// element 1: array of inlined extra data
				0x84,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// element 1
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,
				// element 2
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x8d, 0x99, 0xcc, 0x54, 0xc8, 0x6b, 0xab, 0x50,
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0xeb, 0x0e, 0x1d, 0xca, 0x7a, 0x7e, 0xe1, 0x19,

				// next slab ID
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,

				// element 0:
				0x82,
				// key: "a"
				0x61, 0x61,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: "b"
				0x61, 0x62,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xca, 0x96, 0x9f, 0xeb, 0x5f, 0x29, 0x4f, 0xb9,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: 1
				0xd8, 0xa4, 0x02,

				// element 3:
				0x82,
				// key: "c"
				0x61, 0x63,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 2
				0x18, 0x02,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xc4, 0x85, 0xc1, 0xd1, 0xd5, 0xc0, 0x40, 0x96,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 2
				0xd8, 0xa4, 0x02,
				// value: 4
				0xd8, 0xa4, 0x04,

				// element 4:
				0x82,
				// key: "d"
				0x61, 0x64,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 3
				0x18, 0x03,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xc5, 0x75, 0x9c, 0xf7, 0x20, 0xc5, 0x65, 0xa1,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 3
				0xd8, 0xa4, 0x03,
				// value: 6
				0xd8, 0xa4, 0x06,
			},

			id3: {
				// version, flag: has inlined slab
				0x11,
				// flag: map data
				0x08,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x81,
				0x18, 0x2b,
				// element 1: array of inlined extra data
				0x84,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x4f, 0xca, 0x11, 0xbd, 0x8d, 0xcb, 0xfb, 0x64,
				// element 1
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0xdc, 0xe4, 0xe4, 0x6, 0xa9, 0x50, 0x40, 0xb9,
				// element 2
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x79, 0xb3, 0x45, 0x84, 0x9e, 0x66, 0xa5, 0xa4,
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0xdd, 0xbd, 0x43, 0x10, 0xbe, 0x2d, 0xa9, 0xfc,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,

				// element 0:
				0x82,
				// key: "e"
				0x61, 0x65,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x8e, 0x5e, 0x4f, 0xf6, 0xec, 0x2f, 0x2a, 0xcf,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 4
				0xd8, 0xa4, 0x04,
				// value: 8
				0xd8, 0xa4, 0x08,

				// element 1:
				0x82,
				// key: "f"
				0x61, 0x66,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x0d, 0x36, 0x1e, 0xfd, 0xbb, 0x5c, 0x05, 0xdf,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 5
				0xd8, 0xa4, 0x05,
				// value: 10
				0xd8, 0xa4, 0x0a,

				// element 3:
				0x82,
				// key: "g"
				0x61, 0x67,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 2
				0x18, 0x02,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x6d, 0x8e, 0x42, 0xa2, 0x00, 0xc6, 0x71, 0xf2,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 6
				0xd8, 0xa4, 0x06,
				// value: 12
				0xd8, 0xa4, 0x0c,

				// element 4:
				0x82,
				// key: "h"
				0x61, 0x68,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 3
				0x18, 0x03,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xbb, 0x06, 0x37, 0x6e, 0x3a, 0x78, 0xe8, 0x6c,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 7
				0xd8, 0xa4, 0x07,
				// value: 14
				0xd8, 0xa4, 0x0e,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])
		require.Equal(t, expected[id3], stored[id3])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root metadata slab, inlined child map of different type", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo1 := testTypeInfo{43}
		childMapTypeInfo2 := testTypeInfo{44}
		childMapTypeInfo3 := testTypeInfo{45}
		childMapTypeInfo4 := testTypeInfo{46}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 8
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize; i++ {

			var ti TypeInfo
			switch i % 4 {
			case 0:
				ti = childMapTypeInfo1
			case 1:
				ti = childMapTypeInfo2
			case 2:
				ti = childMapTypeInfo3
			case 3:
				ti = childMapTypeInfo4
			}

			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), ti)
			require.NoError(t, err)

			ck := Uint64Value(i)
			cv := Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = mapValue{ck: cv}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 10}) // inlined maps index 2-9
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 11})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version
				0x10,
				// flag: root + map metadata
				0x89,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// child shared address
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,

				// child header count
				0x00, 0x02,
				// child header 1 (slab id, first key, size)
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0xda,
				// child header 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0xda,
			},
			id2: {
				// version, flag: has inlined slab, has next slab ID
				0x13,
				// flag: map data
				0x08,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x84,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2b,
				// count: 1
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// element 1
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2c,
				// count: 1
				0x01,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,
				// element 2
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2d,
				// count: 1
				0x01,
				// seed
				0x1b, 0x8d, 0x99, 0xcc, 0x54, 0xc8, 0x6b, 0xab, 0x50,
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2e,
				// count: 1
				0x01,
				// seed
				0x1b, 0xeb, 0x0e, 0x1d, 0xca, 0x7a, 0x7e, 0xe1, 0x19,

				// next slab ID
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,

				// element 0:
				0x82,
				// key: "a"
				0x61, 0x61,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: "b"
				0x61, 0x62,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xca, 0x96, 0x9f, 0xeb, 0x5f, 0x29, 0x4f, 0xb9,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: 1
				0xd8, 0xa4, 0x02,

				// element 3:
				0x82,
				// key: "c"
				0x61, 0x63,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 2
				0x18, 0x02,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xc4, 0x85, 0xc1, 0xd1, 0xd5, 0xc0, 0x40, 0x96,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 2
				0xd8, 0xa4, 0x02,
				// value: 4
				0xd8, 0xa4, 0x04,

				// element 4:
				0x82,
				// key: "d"
				0x61, 0x64,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 3
				0x18, 0x03,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xc5, 0x75, 0x9c, 0xf7, 0x20, 0xc5, 0x65, 0xa1,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 3
				0xd8, 0xa4, 0x03,
				// value: 6
				0xd8, 0xa4, 0x06,
			},

			id3: {
				// version, flag: has inlined slab
				0x11,
				// flag: map data
				0x08,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x84,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2b,
				// count: 1
				0x01,
				// seed
				0x1b, 0x4f, 0xca, 0x11, 0xbd, 0x8d, 0xcb, 0xfb, 0x64,
				// element 1
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2c,
				// count: 1
				0x01,
				// seed
				0x1b, 0xdc, 0xe4, 0xe4, 0x6, 0xa9, 0x50, 0x40, 0xb9,
				// element 2
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2d,
				// count: 1
				0x01,
				// seed
				0x1b, 0x79, 0xb3, 0x45, 0x84, 0x9e, 0x66, 0xa5, 0xa4,
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2e,
				// count: 1
				0x01,
				// seed
				0x1b, 0xdd, 0xbd, 0x43, 0x10, 0xbe, 0x2d, 0xa9, 0xfc,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,

				// element 0:
				0x82,
				// key: "e"
				0x61, 0x65,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x8e, 0x5e, 0x4f, 0xf6, 0xec, 0x2f, 0x2a, 0xcf,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 4
				0xd8, 0xa4, 0x04,
				// value: 8
				0xd8, 0xa4, 0x08,

				// element 1:
				0x82,
				// key: "f"
				0x61, 0x66,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x0d, 0x36, 0x1e, 0xfd, 0xbb, 0x5c, 0x05, 0xdf,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 5
				0xd8, 0xa4, 0x05,
				// value: 10
				0xd8, 0xa4, 0x0a,

				// element 3:
				0x82,
				// key: "g"
				0x61, 0x67,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 2
				0x18, 0x02,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x6d, 0x8e, 0x42, 0xa2, 0x00, 0xc6, 0x71, 0xf2,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 6
				0xd8, 0xa4, 0x06,
				// value: 12
				0xd8, 0xa4, 0x0c,

				// element 4:
				0x82,
				// key: "h"
				0x61, 0x68,
				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 3
				0x18, 0x03,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0xbb, 0x06, 0x37, 0x6e, 0x3a, 0x78, 0xe8, 0x6c,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 7
				0xd8, 0xa4, 0x07,
				// value: 14
				0xd8, 0xa4, 0x0e,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])
		require.Equal(t, expected[id3], stored[id3])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 1 level", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 8
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{Digest(i % 4), Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
		}

		require.Equal(t, uint64(mapSize), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// map data slab
			id1: {
				// version
				0x10,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 2 elements)
				0x99, 0x00, 0x04,

				// inline collision group corresponding to hkey 0
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [uint64(0), uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
				// element: [uint64(4), uint64(8)]
				0x82, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x08,

				// inline collision group corresponding to hkey 1
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [uint64(1), uint64(2)]
				0x82, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02,
				// element: [uint64(5), uint64(10)]
				0x82, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x0a,

				// inline collision group corresponding to hkey 2
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [uint64(2), uint64(4)]
				0x82, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x04,
				// element: [uint64(6), uint64(12)]
				0x82, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x0c,

				// inline collision group corresponding to hkey 3
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [uint64(3), uint64(6)]
				0x82, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x06,
				// element: [uint64(7), uint64(14)]
				0x82, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x0e,
			},
		}

		stored, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 2 levels", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 8
		keyValues := make(map[Value]Value)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{Digest(i % 4), Digest(i % 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
		}

		require.Equal(t, uint64(mapSize), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// map data slab
			id1: {
				// version
				0x10,
				// flag: root + map data
				0x88,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2a,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				0x99, 0x00, 0x04,

				// inline collision group corresponding to hkey 0
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x59, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 elements)
				0x99, 0x00, 0x01,

				// inline collision group corresponding to hkey [0, 0]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [uint64(0), uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
				// element: [uint64(4), uint64(8)]
				0x82, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x08,

				// inline collision group corresponding to hkey 1
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x59, 0x00, 0x08,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 1 elements)
				0x99, 0x00, 0x01,

				// inline collision group corresponding to hkey [1, 1]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [uint64(1), uint64(2)]
				0x82, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02,
				// element: [uint64(5), uint64(10)]
				0x82, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x0a,

				// inline collision group corresponding to hkey 2
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x59, 0x00, 0x08,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// elements (array of 1 element)
				0x99, 0x00, 0x01,

				// inline collision group corresponding to hkey [2, 0]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 element)
				0x99, 0x00, 0x02,
				// element: [uint64(2), uint64(4)]
				0x82, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x04,
				// element: [uint64(6), uint64(12)]
				0x82, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x0c,

				// inline collision group corresponding to hkey 3
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 1)
				0x59, 0x00, 0x08,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 1 element)
				0x99, 0x00, 0x01,

				// inline collision group corresponding to hkey [3, 1]
				// (tag number CBORTagInlineCollisionGroup)
				0xd8, 0xfd,
				// (tag content: array of 3 elements)
				0x83,

				// level: 2
				0x02,

				// hkeys (empty byte string)
				0x40,

				// elements (array of 2 element)
				0x99, 0x00, 0x02,
				// element: [uint64(3), uint64(6)]
				0x82, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x06,
				// element: [uint64(7), uint64(14)]
				0x82, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x0e,
			},
		}

		stored, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("external collision", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 20
		keyValues := make(map[Value]Value)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)

			digests := []Digest{Digest(i % 2), Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
		}

		require.Equal(t, uint64(mapSize), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// map data slab
			id1: {
				// version
				0x10,
				// flag: root + has pointer + map data
				0xc8,
				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 10
				0x14,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				0x99, 0x00, 0x02,

				// external collision group corresponding to hkey 0
				// (tag number CBORTagExternalCollisionGroup)
				0xd8, 0xfe,
				// (tag content: slab id)
				0xd8, 0xff, 0x50,
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,

				// external collision group corresponding to hkey 1
				// (tag number CBORTagExternalCollisionGroup)
				0xd8, 0xfe,
				// (tag content: slab id)
				0xd8, 0xff, 0x50,
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			},

			// external collision group
			id2: {
				// version
				0x10,
				// flag: any size + collision group
				0x2b,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 10)
				0x59, 0x00, 0x50,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 8
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08,
				// hkey: 10
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
				// hkey: 12
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c,
				// hkey: 14
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0e,
				// hkey: 16
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
				// hkey: 18
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12,

				// elements (array of 10 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x0a,
				// element: [uint64(0), uint64(0)]
				0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
				// element: [uint64(2), uint64(4)]
				0x82, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x04,
				// element: [uint64(4), uint64(8)]
				0x82, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x08,
				// element: [uint64(6), uint64(12)]
				0x82, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x0c,
				// element: [uint64(8), uint64(16)]
				0x82, 0xd8, 0xa4, 0x08, 0xd8, 0xa4, 0x10,
				// element: [uint64(10), uint64(20)]
				0x82, 0xd8, 0xa4, 0x0a, 0xd8, 0xa4, 0x14,
				// element: [uint64(12), uint64(24)]
				0x82, 0xd8, 0xa4, 0x0c, 0xd8, 0xa4, 0x18, 0x18,
				// element: [uint64(14), uint64(28)]
				0x82, 0xd8, 0xa4, 0x0e, 0xd8, 0xa4, 0x18, 0x1c,
				// element: [uint64(16), uint64(32)]
				0x82, 0xd8, 0xa4, 0x10, 0xd8, 0xa4, 0x18, 0x20,
				// element: [uint64(18), uint64(36)]
				0x82, 0xd8, 0xa4, 0x12, 0xd8, 0xa4, 0x18, 0x24,
			},

			// external collision group
			id3: {
				// version
				0x10,
				// flag: any size + collision group
				0x2b,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 1
				0x01,

				// hkeys (byte string of length 8 * 10)
				0x59, 0x00, 0x50,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
				// hkey: 9
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
				// hkey: 11
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,
				// hkey: 13
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d,
				// hkey: 15
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f,
				// hkey: 17
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11,
				// hkey: 19
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13,

				// elements (array of 10 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x0a,
				// element: [uint64(1), uint64(2)]
				0x82, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02,
				// element: [uint64(3), uint64(6)]
				0x82, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x06,
				// element: [uint64(5), uint64(10)]
				0x82, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x0a,
				// element: [uint64(7), uint64(14)]
				0x82, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x0e,
				// element: [uint64(9), uint64(18)]
				0x82, 0xd8, 0xa4, 0x09, 0xd8, 0xa4, 0x12,
				// element: [uint64(11), uint64(22))]
				0x82, 0xd8, 0xa4, 0x0b, 0xd8, 0xa4, 0x16,
				// element: [uint64(13), uint64(26)]
				0x82, 0xd8, 0xa4, 0x0d, 0xd8, 0xa4, 0x18, 0x1a,
				// element: [uint64(15), uint64(30)]
				0x82, 0xd8, 0xa4, 0x0f, 0xd8, 0xa4, 0x18, 0x1e,
				// element: [uint64(17), uint64(34)]
				0x82, 0xd8, 0xa4, 0x11, 0xd8, 0xa4, 0x18, 0x22,
				// element: [uint64(19), uint64(38)]
				0x82, 0xd8, 0xa4, 0x13, 0xd8, 0xa4, 0x18, 0x26,
			},
		}

		stored, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])
		require.Equal(t, expected[id3], stored[id3])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to child map", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize-1; i++ {
			k := NewStringValue(strings.Repeat(string(r), 22))
			v := NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child map
		typeInfo2 := testTypeInfo{43}

		childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo2)
		require.NoError(t, err)

		expectedChildMapValues := mapValue{}
		for i := 0; i < 2; i++ {
			k := Uint64Value(i)
			v := NewStringValue(strings.Repeat("b", 22))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v
		}

		k := NewStringValue(strings.Repeat(string(r), 22))
		v := childMap
		keyValues[k] = expectedChildMapValues

		digests := []Digest{Digest(mapSize - 1), Digest((mapSize - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert child map
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(mapSize), m.Count())

		// root slab (data slab) ID
		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		// child map slab ID
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// data slab
			id1: {
				// version
				0x10,
				// flag: root + has pointer + map data
				0xc8,

				// extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,2)]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			},

			// map data slab
			id2: {
				// version
				0x10,
				// flag: root + map data
				0x88,

				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2b,
				// count
				0x02,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey
				0x4f, 0x6a, 0x3e, 0x93, 0xdd, 0xb1, 0xbe, 0x5,
				// hkey
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [1:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0xd8, 0xa4, 0x1,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [0:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0xd8, 0xa4, 0x0,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to grand child map", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize-1; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i * 2)
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Create child map
		childTypeInfo := testTypeInfo{43}

		childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childTypeInfo)
		require.NoError(t, err)

		// Create grand child map
		gchildTypeInfo := testTypeInfo{44}

		gchildMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), gchildTypeInfo)
		require.NoError(t, err)

		expectedGChildMapValues := mapValue{}
		r := 'a'
		for i := 0; i < 2; i++ {
			k := NewStringValue(strings.Repeat(string(r), 22))
			v := NewStringValue(strings.Repeat(string(r), 22))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			r++
		}

		// Insert grand child map to child map
		existingStorable, err := childMap.Set(compare, hashInputProvider, Uint64Value(0), gchildMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		k := Uint64Value(mapSize - 1)
		v := childMap
		keyValues[k] = mapValue{Uint64Value(0): expectedGChildMapValues}

		digests := []Digest{Digest(mapSize - 1), Digest((mapSize - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert child map
		existingStorable, err = m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(mapSize), m.Count())

		// root slab (data slab) ID
		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		// grand child map slab ID
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// data slab
			id1: {
				// version, flag: has inlined slab
				0x11,
				// flag: root + has pointer + map data
				0xc8,

				// extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x81,
				// element 0
				// inlined map extra data
				0xd8, 0xf8,
				0x83,
				0x18, 0x2b,
				// count: 1
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [0:0]
				0x82,
				0xd8, 0xa4, 0x0,
				0xd8, 0xa4, 0x0,
				// element: [1:inlined map]
				0x82,
				// key: 1
				0xd8, 0xa4, 0x1,

				// value: inlined map (tag: CBORTagInlinedMap)
				0xd8, 0xfb,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined map elements (array of 3 elements)
				0x83,
				// level 0
				0x00,
				// hkey bytes
				0x59, 0x00, 0x08,
				0x93, 0x26, 0xc4, 0xd9, 0xc6, 0xea, 0x1c, 0x45,
				// 1 element
				0x99, 0x00, 0x01,
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: SlabID{...3}
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			},

			// map data slab
			id2: {
				// version
				0x10,
				// flag: root + map data
				0x88,

				// extra data (CBOR encoded array of 3 elements)
				0x83,
				// type info
				0x18, 0x2c,
				// count
				0x02,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0xa,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey
				0x30, 0x43, 0xc5, 0x14, 0x8f, 0x52, 0x18, 0x43,
				// hkey
				0x98, 0x0f, 0x5c, 0xdb, 0x37, 0x71, 0x6c, 0x13,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to child array", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 8
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize-1; i++ {
			k := NewStringValue(strings.Repeat(string(r), 22))
			v := NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child array
		const childArraySize = 5

		typeInfo2 := testTypeInfo{43}

		childArray, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		expectedChildValues := make([]Value, childArraySize)
		for i := 0; i < childArraySize; i++ {
			v := NewStringValue(strings.Repeat("b", 22))
			err = childArray.Append(v)
			require.NoError(t, err)

			expectedChildValues[i] = v
		}

		k := NewStringValue(strings.Repeat(string(r), 22))
		v := childArray

		keyValues[k] = arrayValue(expectedChildValues)

		digests := []Digest{Digest(mapSize - 1), Digest((mapSize - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert nested array
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(mapSize), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		id4 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// metadata slab
			id1: {
				// version
				0x10,
				// flag: root + map meta
				0x89,

				// extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info: "map"
				0x18, 0x2A,
				// count: 8
				0x08,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// child shared address
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,

				// child header count
				0x00, 0x02,
				// child header 1 (slab id, first key, size)
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0xf6,
				// child header 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				0x00, 0xf2,
			},

			// data slab
			id2: {
				// version
				0x12,
				// flag: map data
				0x08,
				// next slab id
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:bbbbbbbbbbbbbbbbbbbbbb]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
				// element: [cccccccccccccccccccccc:cccccccccccccccccccccc]
				0x82,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				0x76, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63,
				// element: [dddddddddddddddddddddd:dddddddddddddddddddddd]
				0x82,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
				0x76, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64, 0x64,
			},

			// data slab
			id3: {
				// version
				0x10,
				// flag: has pointer + map data
				0x48,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 4
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// hkey: 5
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// hkey: 6
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
				// hkey: 7
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,
				// element: [eeeeeeeeeeeeeeeeeeeeee:eeeeeeeeeeeeeeeeeeeeee]
				0x82,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				0x76, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65,
				// element: [ffffffffffffffffffffff:ffffffffffffffffffffff]
				0x82,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				0x76, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
				// element: [gggggggggggggggggggggg:gggggggggggggggggggggg]
				0x82,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				0x76, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67, 0x67,
				// element: [hhhhhhhhhhhhhhhhhhhhhh:SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
				0x82,
				0x76, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68, 0x68,
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
			},

			// array data slab
			id4: {
				// version
				0x10,
				// flag: root + array data
				0x80,
				// extra data (CBOR encoded array of 1 elements)
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

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])
		require.Equal(t, expected[id3], stored[id3])
		require.Equal(t, expected[id4], stored[id4])

		// Verify slab size in header is correct.
		meta, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)
		require.Equal(t, 2, len(meta.childrenHeaders))
		require.Equal(t, uint32(len(stored[id2])), meta.childrenHeaders[0].size)
		require.Equal(t, uint32(len(stored[id3])+SlabIDLength), meta.childrenHeaders[1].size)

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to grand child array", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		r := 'a'
		for i := uint64(0); i < mapSize-1; i++ {
			k := NewStringValue(strings.Repeat(string(r), 22))
			v := NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []Digest{Digest(i), Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child array
		childTypeInfo := testTypeInfo{43}

		childArray, err := NewArray(storage, address, childTypeInfo)
		require.NoError(t, err)

		// Create grand child array
		const gchildArraySize = 5

		gchildTypeInfo := testTypeInfo{44}

		gchildArray, err := NewArray(storage, address, gchildTypeInfo)
		require.NoError(t, err)

		expectedGChildValues := make([]Value, gchildArraySize)
		for i := 0; i < gchildArraySize; i++ {
			v := NewStringValue(strings.Repeat("b", 22))
			err = gchildArray.Append(v)
			require.NoError(t, err)

			expectedGChildValues[i] = v
		}

		// Insert grand child array to child array
		err = childArray.Append(gchildArray)
		require.NoError(t, err)

		k := NewStringValue(strings.Repeat(string(r), 22))
		v := childArray

		keyValues[k] = arrayValue{arrayValue(expectedGChildValues)}

		digests := []Digest{Digest(mapSize - 1), Digest((mapSize - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert child array to parent map
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(mapSize), m.Count())

		// parent map root slab ID
		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		// grand child array root slab ID
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// data slab
			id1: {
				// version, flag: has inlined slab
				0x11,
				// flag: root + has pointer + map data
				0xc8,

				// extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x81,
				// element 0
				// inlined array extra data
				0xd8, 0xf7,
				0x81,
				0x18, 0x2b,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element: [aaaaaaaaaaaaaaaaaaaaaa:aaaaaaaaaaaaaaaaaaaaaa]
				0x82,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				0x76, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
				// element: [bbbbbbbbbbbbbbbbbbbbbb:inlined array]
				0x82,
				0x76, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,

				// value: inlined array (tag: CBORTagInlinedArray)
				0xd8, 0xfa,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined array elements (1 element)
				0x99, 0x00, 0x01,
				// SlabID{...3}
				0xd8, 0xff, 0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			},

			// grand array data slab
			id2: {
				// version
				0x10,
				// flag: root + array data
				0x80,
				// extra data (CBOR encoded array of 1 elements)
				0x81,
				// type info
				0x18, 0x2c,

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

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])
		require.Equal(t, expected[id2], stored[id2])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to storable slab", func(t *testing.T) {

		SetThreshold(256)
		defer SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		k := Uint64Value(0)
		v := Uint64Value(0)

		digests := []Digest{Digest(0), Digest(1)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(1), m.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})

		expectedNoPointer := []byte{

			// version
			0x10,
			// flag: root + map data
			0x88,
			// extra data (CBOR encoded array of 3 elements)
			0x83,
			// type info: "map"
			0x18, 0x2A,
			// count: 10
			0x01,
			// seed
			0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

			// the following encoded data is valid CBOR

			// elements (array of 3 elements)
			0x83,

			// level: 0
			0x00,

			// hkeys (byte string of length 8 * 1)
			0x59, 0x00, 0x08,
			// hkey: 0
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

			// elements (array of 1 elements)
			// each element is encoded as CBOR array of 2 elements (key, value)
			0x99, 0x00, 0x01,
			// element: [uint64(0), uint64(0)]
			0x82, 0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x00,
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 1, len(stored))
		require.Equal(t, expectedNoPointer, stored[id1])

		// Overwrite existing value with long string
		vs := NewStringValue(strings.Repeat("a", 128))
		existingStorable, err = m.Set(compare, hashInputProvider, k, vs)
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, v, existingValue)

		expectedHasPointer := []byte{

			// version
			0x10,
			// flag: root + pointer + map data
			0xc8,
			// extra data (CBOR encoded array of 3 elements)
			0x83,
			// type info: "map"
			0x18, 0x2A,
			// count: 10
			0x01,
			// seed
			0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

			// the following encoded data is valid CBOR

			// elements (array of 3 elements)
			0x83,

			// level: 0
			0x00,

			// hkeys (byte string of length 8 * 1)
			0x59, 0x00, 0x08,
			// hkey: 0
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

			// elements (array of 1 elements)
			// each element is encoded as CBOR array of 2 elements (key, value)
			0x99, 0x00, 0x01,
			// element: [uint64(0), slab id]
			0x82, 0xd8, 0xa4, 0x00,
			// (tag content: slab id)
			0xd8, 0xff, 0x50,
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
		}

		expectedStorableSlab := []byte{
			// version
			0x10,
			// flag: storable + no size limit
			0x3f,
			// "aaaa..."
			0x78, 0x80,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
			0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
		}

		stored, err = storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 2, len(stored))
		require.Equal(t, expectedHasPointer, stored[id1])
		require.Equal(t, expectedStorableSlab, stored[id2])
	})

	t.Run("same composite with one field", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testCompositeTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {

			// Create child map, composite with one field "uuid"
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := NewStringValue("uuid")
			cv := Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = mapValue{ck: cv}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x81,
				// element 0
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6, 0x18, 0x2b,
				// count
				0x01,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// composite digests
				0x48, 0x4c, 0x1f, 0x34, 0x74, 0x38, 0x15, 0x64, 0xe5,
				// composite keys ["uuid"]
				0x81, 0x64, 0x75, 0x75, 0x69, 0x64,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element 0:
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined composite elements (array of 1 elements)
				0x81,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: 2
				0xd8, 0xa4, 0x01,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined composite elements (array of 1 elements)
				0x81,
				// value: 1
				0xd8, 0xa4, 0x01,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with two fields (same order)", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testCompositeTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			expectedChildMapVaues := mapValue{}

			// Create child map, composite with one field "uuid"
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := NewStringValue("uuid")
			cv := Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapVaues[ck] = cv

			ck = NewStringValue("amount")
			cv = Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err = childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapVaues[ck] = cv

			k := Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapVaues
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x81,
				// element 0
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6, 0x18, 0x2b,
				// count: 2
				0x02,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// composite digests
				0x50, 0x3b, 0xef, 0x5b, 0xe2, 0x9b, 0x8d, 0xf9, 0x65, 0x4c, 0x1f, 0x34, 0x74, 0x38, 0x15, 0x64, 0xe5,
				// composite keys ["amount", "uuid"]
				0x82, 0x66, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x64, 0x75, 0x75, 0x69, 0x64,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// 0x99, 0x0, 0x2, 0x82, 0xd8, 0xa4, 0x0, 0xd8, 0xfc, 0x83, 0x18, 0x0, 0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x82, 0xd8, 0xa4, 0x0, 0xd8, 0xa4, 0x0, 0x82, 0xd8, 0xa4, 0x1, 0xd8, 0xfc, 0x83, 0x18, 0x0, 0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x82, 0xd8, 0xa4, 0x2, 0xd8, 0xa4, 0x1

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element 0:
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: 2
				0xd8, 0xa4, 0x01,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 2
				0xd8, 0xa4, 0x02,
				// value: 1
				0xd8, 0xa4, 0x01,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with two fields (different order)", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testCompositeTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		// fields are ordered differently because of different seed.
		for i := uint64(0); i < mapSize; i++ {
			expectedChildMapValues := mapValue{}

			// Create child map, composite with one field "uuid"
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := NewStringValue("uuid")
			cv := Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			ck = NewStringValue("a")
			cv = Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err = childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			k := Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x81,
				// element 0
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6, 0x18, 0x2b,
				// count: 2
				0x02,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// composite digests
				0x50,
				0x42, 0xa5, 0xa2, 0x7f, 0xb3, 0xc9, 0x0c, 0xa1,
				0x4c, 0x1f, 0x34, 0x74, 0x38, 0x15, 0x64, 0xe5,
				// composite keys ["a", "uuid"]
				0x82, 0x61, 0x61, 0x64, 0x75, 0x75, 0x69, 0x64,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// 0x99, 0x0, 0x2, 0x82, 0xd8, 0xa4, 0x0, 0xd8, 0xfc, 0x83, 0x18, 0x0, 0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x82, 0xd8, 0xa4, 0x0, 0xd8, 0xa4, 0x0, 0x82, 0xd8, 0xa4, 0x1, 0xd8, 0xfc, 0x83, 0x18, 0x0, 0x48, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x82, 0xd8, 0xa4, 0x2, 0xd8, 0xa4, 0x1

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element 0:
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: 2
				0xd8, 0xa4, 0x01,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 2
				0xd8, 0xa4, 0x02,
				// value: 1
				0xd8, 0xa4, 0x01,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with different fields", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testCompositeTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 3
		keyValues := make(map[Value]Value, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			expectedChildMapValues := mapValue{}

			// Create child map
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := NewStringValue("uuid")
			cv := Uint64Value(i)

			// Insert first element "uuid" to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			// Insert second element to child map (second element is different)
			switch i % 3 {
			case 0:
				ck = NewStringValue("a")
				cv = Uint64Value(i * 2)
				existingStorable, err = childMap.Set(compare, hashInputProvider, ck, cv)

			case 1:
				ck = NewStringValue("b")
				cv = Uint64Value(i * 2)
				existingStorable, err = childMap.Set(compare, hashInputProvider, ck, cv)

			case 2:
				ck = NewStringValue("c")
				cv = Uint64Value(i * 2)
				existingStorable, err = childMap.Set(compare, hashInputProvider, ck, cv)
			}

			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			k := Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 3
				0x03,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x81,
				0xd8, 0xf6, 0x18, 0x2b,
				// element 1: array of inlined extra data
				0x83,
				// element 0
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 2
				0x02,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// composite digests
				0x50,
				0x42, 0xa5, 0xa2, 0x7f, 0xb3, 0xc9, 0x0c, 0xa1,
				0x4c, 0x1f, 0x34, 0x74, 0x38, 0x15, 0x64, 0xe5,
				// composite keys ["a", "uuid"]
				0x82, 0x61, 0x61, 0x64, 0x75, 0x75, 0x69, 0x64,

				// element 1
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 2
				0x02,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0xa,
				// composite digests
				0x50,
				0x74, 0x0a, 0x02, 0xc1, 0x19, 0x6f, 0xb8, 0x9e,
				0x82, 0x41, 0xee, 0xef, 0xc7, 0xb3, 0x2f, 0x28,
				// composite keys ["uuid", "b"]
				0x82, 0x64, 0x75, 0x75, 0x69, 0x64, 0x61, 0x62,

				// element 2
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 2
				0x02,
				// seed
				0x1b, 0x8d, 0x99, 0xcc, 0x54, 0xc8, 0x6b, 0xab, 0x50,
				// composite digests
				0x50,
				0x5a, 0x98, 0x80, 0xf4, 0xa6, 0x52, 0x9e, 0x2d,
				0x6d, 0x8a, 0x0a, 0xe7, 0x19, 0xf1, 0xbb, 0x8b,
				// composite keys ["uuid", "c"]
				0x82, 0x64, 0x75, 0x75, 0x69, 0x64, 0x61, 0x63,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 3)
				0x59, 0x00, 0x18,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,

				// elements (array of 3 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x03,
				// element 0:
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 1
				0xd8, 0xa4, 0x01,
				// value: 2
				0xd8, 0xa4, 0x02,

				// element 2:
				0x82,
				// key: 2
				0xd8, 0xa4, 0x02,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 2
				0x18, 0x02,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 2
				0xd8, 0xa4, 0x02,
				// value: 4
				0xd8, 0xa4, 0x04,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with different number of fields", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo := testCompositeTypeInfo{43}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 2
		keyValues := make(map[Value]Value, mapSize)
		// fields are ordered differently because of different seed.
		for i := uint64(0); i < mapSize; i++ {
			expectedChildMapValues := mapValue{}

			// Create child map, composite with one field "uuid"
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := NewStringValue("uuid")
			cv := Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			if i == 0 {
				ck = NewStringValue("a")
				cv = Uint64Value(i * 2)

				// Insert element to child map
				existingStorable, err = childMap.Set(compare, hashInputProvider, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[ck] = cv
			}

			k := Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 2
				0x02,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x81,
				0xd8, 0xf6, 0x18, 0x2b,
				// element 1: array of inlined extra data
				0x82,
				// element 0
				// inlined map extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 2
				0x02,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// composite digests
				0x50,
				0x42, 0xa5, 0xa2, 0x7f, 0xb3, 0xc9, 0x0c, 0xa1,
				0x4c, 0x1f, 0x34, 0x74, 0x38, 0x15, 0x64, 0xe5,
				// composite keys ["a", "uuid"]
				0x82, 0x61, 0x61, 0x64, 0x75, 0x75, 0x69, 0x64,
				// element 0
				// inlined map extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6,
				0x00,
				// count: 1
				0x01,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,
				// composite digests
				0x48,
				0x74, 0x0a, 0x02, 0xc1, 0x19, 0x6f, 0xb8, 0x9e,
				// composite keys ["uuid"]
				0x81, 0x64, 0x75, 0x75, 0x69, 0x64,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 2)
				0x59, 0x00, 0x10,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,

				// elements (array of 2 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x02,
				// element 0:
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: 2
				0xd8, 0xa4, 0x01,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined composite elements (array of 2 elements)
				0x81,
				// value: 1
				0xd8, 0xa4, 0x01,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("different composite", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		childMapTypeInfo1 := testCompositeTypeInfo{43}
		childMapTypeInfo2 := testCompositeTypeInfo{44}

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapSize = 4
		keyValues := make(map[Value]Value, mapSize)
		// fields are ordered differently because of different seed.
		for i := uint64(0); i < mapSize; i++ {
			expectedChildMapValues := mapValue{}

			var ti TypeInfo
			if i%2 == 0 {
				ti = childMapTypeInfo1
			} else {
				ti = childMapTypeInfo2
			}

			// Create child map, composite with two field "uuid" and "a"
			childMap, err := NewMap(storage, address, NewDefaultDigesterBuilder(), ti)
			require.NoError(t, err)

			ck := NewStringValue("uuid")
			cv := Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			ck = NewStringValue("a")
			cv = Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err = childMap.Set(compare, hashInputProvider, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			k := Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())

		id1 := NewSlabID(address, SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{
			id1: {
				// version, has inlined slab
				0x11,
				// flag: root + map data
				0x88,

				// slab extra data
				// CBOR encoded array of 3 elements
				0x83,
				// type info
				0x18, 0x2a,
				// count: 4
				0x04,
				// seed
				0x1b, 0x52, 0xa8, 0x78, 0x3, 0x85, 0x2c, 0xaa, 0x49,

				// inlined extra data
				0x82,
				// element 0: array of inlined type info
				0x80,
				// element 1: array of inlined extra data
				0x82,
				// element 0
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6, 0x18, 0x2b,
				// count: 2
				0x02,
				// seed
				0x1b, 0xa9, 0x3a, 0x2d, 0x6f, 0x53, 0x49, 0xaa, 0xdd,
				// composite digests
				0x50,
				0x42, 0xa5, 0xa2, 0x7f, 0xb3, 0xc9, 0x0c, 0xa1,
				0x4c, 0x1f, 0x34, 0x74, 0x38, 0x15, 0x64, 0xe5,
				// composite keys ["a", "uuid"]
				0x82, 0x61, 0x61, 0x64, 0x75, 0x75, 0x69, 0x64,
				// element 1
				// inlined composite extra data
				0xd8, 0xf9,
				0x83,
				// map extra data
				0x83,
				// type info
				0xd8, 0xf6, 0x18, 0x2c,
				// count: 2
				0x02,
				// seed
				0x1b, 0x23, 0xd4, 0xf4, 0x3f, 0x19, 0xf8, 0x95, 0x0a,
				// composite digests
				0x50,
				0x74, 0x0a, 0x02, 0xc1, 0x19, 0x6f, 0xb8, 0x9e,
				0xea, 0x8e, 0x6f, 0x69, 0x81, 0x19, 0x68, 0x81,
				// composite keys ["uuid", "a"]
				0x82, 0x64, 0x75, 0x75, 0x69, 0x64, 0x61, 0x61,

				// the following encoded data is valid CBOR

				// elements (array of 3 elements)
				0x83,

				// level: 0
				0x00,

				// hkeys (byte string of length 8 * 4)
				0x59, 0x00, 0x20,
				// hkey: 0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// hkey: 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				// hkey: 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// hkey: 3
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,

				// elements (array of 4 elements)
				// each element is encoded as CBOR array of 2 elements (key, value)
				0x99, 0x00, 0x04,
				// element 0:
				0x82,
				// key: 0
				0xd8, 0xa4, 0x00,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 0
				0xd8, 0xa4, 0x00,
				// value: 0
				0xd8, 0xa4, 0x00,

				// element 1:
				0x82,
				// key: 1
				0xd8, 0xa4, 0x01,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 1
				0xd8, 0xa4, 0x01,
				// value: 2
				0xd8, 0xa4, 0x02,

				// element 2:
				0x82,
				// key: 2
				0xd8, 0xa4, 0x02,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 0
				0x18, 0x00,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 4
				0xd8, 0xa4, 0x04,
				// value: 2
				0xd8, 0xa4, 0x02,

				// element 3:
				0x82,
				// key: 3
				0xd8, 0xa4, 0x03,
				// value: inlined composite (tag: CBORTagInlinedCompactMap)
				0xd8, 0xfc,
				// array of 3 elements
				0x83,
				// extra data index 1
				0x18, 0x01,
				// inlined map slab index
				0x48, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
				// inlined composite elements (array of 2 elements)
				0x82,
				// value: 3
				0xd8, 0xa4, 0x03,
				// value: 6
				0xd8, 0xa4, 0x06,
			},
		}

		// Verify encoded data
		stored, err := storage.Encode()
		require.NoError(t, err)

		require.Equal(t, len(expected), len(stored))
		require.Equal(t, expected[id1], stored[id1])

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})
}

func TestMapEncodeDecodeRandomValues(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, keyValues := testMapSetRemoveRandomValues(t, r, storage, typeInfo, address)

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	// Create a new storage with encoded data from base storage
	storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

	// Create new map from new storage
	m2, err := NewMapWithRootID(storage2, m.SlabID(), m.digesterBuilder)
	require.NoError(t, err)

	testMap(t, storage2, typeInfo, address, m2, keyValues, nil, false)
}

func TestMapStoredValue(t *testing.T) {

	const mapSize = 4096

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	keyValues := make(map[Value]Value, mapSize)
	i := 0
	for len(keyValues) < mapSize {
		k := NewStringValue(randStr(r, 16))
		keyValues[k] = Uint64Value(i)
		i++
	}

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	rootID := m.SlabID()

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

			m2, ok := value.(*OrderedMap)
			require.True(t, ok)

			testMap(t, storage, typeInfo, address, m2, keyValues, nil, false)
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

func TestMapPopIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())

		i := uint64(0)
		err = m.PopIterate(func(k Storable, v Storable) {
			i++
		})
		require.NoError(t, err)
		require.Equal(t, uint64(0), i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("root-dataslab", func(t *testing.T) {
		const mapSize = 10

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			key, value := Uint64Value(i), Uint64Value(i*10)
			sortedKeys[i] = key
			keyValues[key] = value

			existingStorable, err := m.Set(compare, hashInputProvider, key, value)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, uint64(mapSize), m.Count())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())

		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := mapSize
		err = m.PopIterate(func(k, v Storable) {
			i--

			kv, err := k.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		const mapSize = 4096

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		i := 0
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, 16))
			if _, found := keyValues[k]; !found {
				sortedKeys[i] = k
				keyValues[k] = NewStringValue(randStr(r, 16))
				i++
			}
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		err = storage.Commit()
		require.NoError(t, err)

		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i = len(keyValues)
		err = m.PopIterate(func(k Storable, v Storable) {
			i--

			kv, err := k.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("collision", func(t *testing.T) {
		//MetaDataSlabCount:1 DataSlabCount:13 CollisionDataSlabCount:100

		const mapSize = 1024

		SetThreshold(512)
		defer SetThreshold(1024)

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := &mockDigesterBuilder{}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value, mapSize)
		sortedKeys := make([]Value, mapSize)
		i := 0
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, 16))

			if _, found := keyValues[k]; !found {

				sortedKeys[i] = k
				keyValues[k] = NewStringValue(randStr(r, 16))

				digests := []Digest{
					Digest(i % 100),
					Digest(i % 5),
				}

				digesterBuilder.On("Digest", k).Return(mockDigester{digests})

				existingStorable, err := m.Set(compare, hashInputProvider, k, keyValues[k])
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				i++
			}
		}

		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		err = storage.Commit()
		require.NoError(t, err)

		// Iterate key value pairs
		i = mapSize
		err = m.PopIterate(func(k Storable, v Storable) {
			i--

			kv, err := k.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})
}

func TestEmptyMap(t *testing.T) {

	t.Parallel()

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := NewMap(storage, address, NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	t.Run("get", func(t *testing.T) {
		s, err := m.Get(compare, hashInputProvider, Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)
		require.Nil(t, s)
	})

	t.Run("remove", func(t *testing.T) {
		existingMapKeyStorable, existingMapValueStorable, err := m.Remove(compare, hashInputProvider, Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)
		require.Nil(t, existingMapKeyStorable)
		require.Nil(t, existingMapValueStorable)
	})

	t.Run("readonly iterate", func(t *testing.T) {
		i := 0
		err := m.IterateReadOnly(func(k Value, v Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("iterate", func(t *testing.T) {
		i := 0
		err := m.Iterate(compare, hashInputProvider, func(k Value, v Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("count", func(t *testing.T) {
		count := m.Count()
		require.Equal(t, uint64(0), count)
	})

	t.Run("type", func(t *testing.T) {
		require.True(t, typeInfoComparator(typeInfo, m.Type()))
	})

	t.Run("address", func(t *testing.T) {
		require.Equal(t, address, m.Address())
	})

	// TestMapEncodeDecode/empty tests empty map encoding and decoding
}

func TestMapFromBatchData(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := testTypeInfo{42}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}

		// Create a map with new storage, new address, and original map's elements.
		copied, err := NewMapFromBatchData(
			storage,
			address,
			NewDefaultDigesterBuilder(),
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {
				return iter.Next()
			})
		require.NoError(t, err)
		require.NotEqual(t, copied.SlabID(), m.SlabID())

		testEmptyMap(t, storage, typeInfo, address, copied)
	})

	t.Run("root-dataslab", func(t *testing.T) {
		SetThreshold(1024)

		const mapSize = 10

		typeInfo := testTypeInfo{42}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			storable, err := m.Set(compare, hashInputProvider, Uint64Value(i), Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		require.Equal(t, uint64(mapSize), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []Value
		keyValues := make(map[Value]Value)

		storage := newTestPersistentStorage(t)
		digesterBuilder := NewDefaultDigesterBuilder()
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}

		// Create a map with new storage, new address, and original map's elements.
		copied, err := NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {

				k, v, err := iter.Next()

				// Save key value pair
				if k != nil {
					sortedKeys = append(sortedKeys, k)
					keyValues[k] = v
				}

				return k, v, err
			})

		require.NoError(t, err)
		require.NotEqual(t, copied.SlabID(), m.SlabID())

		testMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 4096

		typeInfo := testTypeInfo{42}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			storable, err := m.Set(compare, hashInputProvider, Uint64Value(i), Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		require.Equal(t, uint64(mapSize), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []Value
		keyValues := make(map[Value]Value)

		storage := newTestPersistentStorage(t)
		digesterBuilder := NewDefaultDigesterBuilder()
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}

		copied, err := NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {
				k, v, err := iter.Next()

				if k != nil {
					sortedKeys = append(sortedKeys, k)
					keyValues[k] = v
				}

				return k, v, err
			})

		require.NoError(t, err)
		require.NotEqual(t, m.SlabID(), copied.SlabID())

		testMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
	})

	t.Run("rebalance two data slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 10

		typeInfo := testTypeInfo{42}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			storable, err := m.Set(compare, hashInputProvider, Uint64Value(i), Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		k := NewStringValue(strings.Repeat("a", int(maxInlineMapElementSize-2)))
		v := NewStringValue(strings.Repeat("b", int(maxInlineMapElementSize-2)))
		storable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		require.Equal(t, uint64(mapSize+1), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []Value
		keyValues := make(map[Value]Value)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		digesterBuilder := NewDefaultDigesterBuilder()

		copied, err := NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {
				k, v, err := iter.Next()

				if k != nil {
					sortedKeys = append(sortedKeys, k)
					keyValues[k] = v
				}

				return k, v, err
			})

		require.NoError(t, err)
		require.NotEqual(t, m.SlabID(), copied.SlabID())

		testMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
	})

	t.Run("merge two data slabs", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 8

		typeInfo := testTypeInfo{42}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			storable, err := m.Set(compare, hashInputProvider, Uint64Value(i), Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		storable, err := m.Set(
			compare,
			hashInputProvider,
			NewStringValue(strings.Repeat("b", int(maxInlineMapElementSize-2))),
			NewStringValue(strings.Repeat("b", int(maxInlineMapElementSize-2))),
		)
		require.NoError(t, err)
		require.Nil(t, storable)

		require.Equal(t, uint64(mapSize+1), m.Count())
		require.Equal(t, typeInfo, m.Type())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []Value
		keyValues := make(map[Value]Value)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		digesterBuilder := NewDefaultDigesterBuilder()

		copied, err := NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {
				k, v, err := iter.Next()

				if k != nil {
					sortedKeys = append(sortedKeys, k)
					keyValues[k] = v
				}

				return k, v, err
			})

		require.NoError(t, err)
		require.NotEqual(t, m.SlabID(), copied.SlabID())

		testMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
	})

	t.Run("random", func(t *testing.T) {
		SetThreshold(256)
		defer SetThreshold(1024)

		const mapSize = 4096

		r := newRand(t)

		typeInfo := testTypeInfo{42}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for m.Count() < mapSize {
			k := randomValue(r, int(maxInlineMapElementSize))
			v := randomValue(r, int(maxInlineMapElementSize))

			_, err = m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(mapSize), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}
		digesterBuilder := NewDefaultDigesterBuilder()

		var sortedKeys []Value
		keyValues := make(map[Value]Value, mapSize)

		copied, err := NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {
				k, v, err := iter.Next()

				if k != nil {
					sortedKeys = append(sortedKeys, k)
					keyValues[k] = v
				}

				return k, v, err
			})

		require.NoError(t, err)
		require.NotEqual(t, m.SlabID(), copied.SlabID())

		testMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
	})

	t.Run("collision", func(t *testing.T) {

		const mapSize = 1024

		SetThreshold(512)
		defer SetThreshold(1024)

		savedMaxCollisionLimitPerDigest := MaxCollisionLimitPerDigest
		defer func() {
			MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
		}()
		MaxCollisionLimitPerDigest = mapSize / 2

		typeInfo := testTypeInfo{42}

		digesterBuilder := &mockDigesterBuilder{}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			digesterBuilder,
			typeInfo,
		)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {

			k, v := Uint64Value(i), Uint64Value(i*10)

			digests := make([]Digest, 2)
			if i%2 == 0 {
				digests[0] = 0
			} else {
				digests[0] = Digest(i % (mapSize / 2))
			}
			digests[1] = Digest(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		require.Equal(t, uint64(mapSize), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []Value
		keyValues := make(map[Value]Value)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}

		i := 0
		copied, err := NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {
				k, v, err := iter.Next()

				if k != nil {
					sortedKeys = append(sortedKeys, k)
					keyValues[k] = v
				}

				i++
				return k, v, err
			})

		require.NoError(t, err)
		require.NotEqual(t, m.SlabID(), copied.SlabID())

		testMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
	})

	t.Run("data slab too large", func(t *testing.T) {
		// Slab size must not exceed maxThreshold.
		// We cannot make this problem happen after Atree Issue #193
		// was fixed by PR #194 & PR #197. This test is to catch regressions.

		SetThreshold(256)
		defer SetThreshold(1024)

		r := newRand(t)

		maxStringSize := int(maxInlineMapKeySize - 2)

		typeInfo := testTypeInfo{42}

		digesterBuilder := &mockDigesterBuilder{}

		m, err := NewMap(
			newTestPersistentStorage(t),
			Address{1, 2, 3, 4, 5, 6, 7, 8},
			digesterBuilder,
			typeInfo,
		)
		require.NoError(t, err)

		k := NewStringValue(randStr(r, maxStringSize))
		v := NewStringValue(randStr(r, maxStringSize))
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{3881892766069237908}})

		storable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		k = NewStringValue(randStr(r, maxStringSize))
		v = NewStringValue(randStr(r, maxStringSize))
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{3882976639190041664}})

		storable, err = m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		k = NewStringValue("zFKUYYNfIfJCCakcDuIEHj")
		v = NewStringValue("EZbaCxxjDtMnbRlXJMgfHnZ")
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{3883321011075439822}})

		storable, err = m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []Value
		keyValues := make(map[Value]Value)

		storage := newTestPersistentStorage(t)
		address := Address{2, 3, 4, 5, 6, 7, 8, 9}

		copied, err := NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			compare,
			hashInputProvider,
			m.Seed(),
			func() (Value, Value, error) {
				k, v, err := iter.Next()

				if k != nil {
					sortedKeys = append(sortedKeys, k)
					keyValues[k] = v
				}

				return k, v, err
			})

		require.NoError(t, err)
		require.NotEqual(t, m.SlabID(), copied.SlabID())

		testMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
	})
}

func TestMapNestedStorables(t *testing.T) {

	t.Run("SomeValue", func(t *testing.T) {

		const mapSize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value)
		for i := uint64(0); i < mapSize; i++ {

			ks := strings.Repeat("a", int(i))
			k := SomeValue{Value: NewStringValue(ks)}

			vs := strings.Repeat("b", int(i))
			v := SomeValue{Value: NewStringValue(vs)}

			keyValues[k] = v

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, true)
	})

	t.Run("Array", func(t *testing.T) {

		const mapSize = 4096

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[Value]Value)
		for i := uint64(0); i < mapSize; i++ {

			// Create a child array with one element
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			vs := strings.Repeat("b", int(i))
			v := SomeValue{Value: NewStringValue(vs)}

			err = childArray.Append(v)
			require.NoError(t, err)

			// Insert nested array into map
			ks := strings.Repeat("a", int(i))
			k := SomeValue{Value: NewStringValue(ks)}

			keyValues[k] = arrayValue{v}

			existingStorable, err := m.Set(compare, hashInputProvider, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, true)
	})
}

func TestMapMaxInlineElement(t *testing.T) {
	t.Parallel()

	r := newRand(t)
	maxStringSize := int(maxInlineMapKeySize - 2)
	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	keyValues := make(map[Value]Value)
	for len(keyValues) < 2 {
		// String length is maxInlineMapKeySize - 2 to account for string encoding overhead.
		k := NewStringValue(randStr(r, maxStringSize))
		v := NewStringValue(randStr(r, maxStringSize))
		keyValues[k] = v

		_, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
	}

	require.True(t, m.root.IsData())

	// Size of root data slab with two elements (key+value pairs) of
	// max inlined size is target slab size minus
	// slab id size (next slab id is omitted in root slab)
	require.Equal(t, targetThreshold-SlabIDLength, uint64(m.root.Header().size))

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
}

func TestMapString(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("small", func(t *testing.T) {
		const mapSize = 3

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := `[0:0 1:1 2:2]`
		require.Equal(t, want, m.String())
	})

	t.Run("large", func(t *testing.T) {
		const mapSize = 30

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := `[0:0 1:1 2:2 3:3 4:4 5:5 6:6 7:7 8:8 9:9 10:10 11:11 12:12 13:13 14:14 15:15 16:16 17:17 18:18 19:19 20:20 21:21 22:22 23:23 24:24 25:25 26:26 27:27 28:28 29:29]`
		require.Equal(t, want, m.String())
	})
}

func TestMapSlabDump(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("small", func(t *testing.T) {
		const mapSize = 3

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:55 firstkey:0 elements: [0:0:0 1:1:1 2:2:2]",
		}
		dumps, err := DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("large", func(t *testing.T) {
		const mapSize = 30

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i)}})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapMetaDataSlab id:0x102030405060708.1 size:48 firstKey:0 children: [{id:0x102030405060708.2 size:221 firstKey:0} {id:0x102030405060708.3 size:293 firstKey:13}]",
			"level 2, MapDataSlab id:0x102030405060708.2 size:221 firstkey:0 elements: [0:0:0 1:1:1 2:2:2 3:3:3 4:4:4 5:5:5 6:6:6 7:7:7 8:8:8 9:9:9 10:10:10 11:11:11 12:12:12]",
			"level 2, MapDataSlab id:0x102030405060708.3 size:293 firstkey:13 elements: [13:13:13 14:14:14 15:15:15 16:16:16 17:17:17 18:18:18 19:19:19 20:20:20 21:21:21 22:22:22 23:23:23 24:24:24 25:25:25 26:26:26 27:27:27 28:28:28 29:29:29]",
		}
		dumps, err := DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("inline collision", func(t *testing.T) {
		const mapSize = 30

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i % 10)}})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapMetaDataSlab id:0x102030405060708.1 size:48 firstKey:0 children: [{id:0x102030405060708.2 size:213 firstKey:0} {id:0x102030405060708.3 size:221 firstKey:5}]",
			"level 2, MapDataSlab id:0x102030405060708.2 size:213 firstkey:0 elements: [0:inline[:0:0 :10:10 :20:20] 1:inline[:1:1 :11:11 :21:21] 2:inline[:2:2 :12:12 :22:22] 3:inline[:3:3 :13:13 :23:23] 4:inline[:4:4 :14:14 :24:24]]",
			"level 2, MapDataSlab id:0x102030405060708.3 size:221 firstkey:5 elements: [5:inline[:5:5 :15:15 :25:25] 6:inline[:6:6 :16:16 :26:26] 7:inline[:7:7 :17:17 :27:27] 8:inline[:8:8 :18:18 :28:28] 9:inline[:9:9 :19:19 :29:29]]",
		}
		dumps, err := DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("external collision", func(t *testing.T) {
		const mapSize = 30

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(i % 2)}})

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:68 firstkey:0 elements: [0:external(0x102030405060708.2) 1:external(0x102030405060708.3)]",
			"collision: MapDataSlab id:0x102030405060708.2 size:135 firstkey:0 elements: [:0:0 :2:2 :4:4 :6:6 :8:8 :10:10 :12:12 :14:14 :16:16 :18:18 :20:20 :22:22 :24:24 :26:26 :28:28]",
			"collision: MapDataSlab id:0x102030405060708.3 size:135 firstkey:0 elements: [:1:1 :3:3 :5:5 :7:7 :9:9 :11:11 :13:13 :15:15 :17:17 :19:19 :21:21 :23:23 :25:25 :27:27 :29:29]",
		}
		dumps, err := DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("key overflow", func(t *testing.T) {

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		k := NewStringValue(strings.Repeat("a", int(maxInlineMapKeySize)))
		v := NewStringValue(strings.Repeat("b", int(maxInlineMapKeySize)))
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(0)}})

		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:93 firstkey:0 elements: [0:SlabIDStorable({[1 2 3 4 5 6 7 8] [0 0 0 0 0 0 0 2]}):bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb]",
			"StorableSlab id:0x102030405060708.2 storable:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}
		dumps, err := DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("value overflow", func(t *testing.T) {

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		k := NewStringValue(strings.Repeat("a", int(maxInlineMapKeySize-2)))
		v := NewStringValue(strings.Repeat("b", int(maxInlineMapElementSize)))
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []Digest{Digest(0)}})

		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:91 firstkey:0 elements: [0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:SlabIDStorable({[1 2 3 4 5 6 7 8] [0 0 0 0 0 0 0 2]})]",
			"StorableSlab id:0x102030405060708.2 storable:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}
		dumps, err := DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})
}

func TestMaxCollisionLimitPerDigest(t *testing.T) {
	savedMaxCollisionLimitPerDigest := MaxCollisionLimitPerDigest
	defer func() {
		MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
	}()

	t.Run("collision limit 0", func(t *testing.T) {
		const mapSize = 1024

		SetThreshold(256)
		defer SetThreshold(1024)

		// Set noncryptographic hash collision limit as 0,
		// meaning no collision is allowed at first level.
		MaxCollisionLimitPerDigest = uint32(0)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			keyValues[k] = v

			digests := []Digest{Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// Insert elements within collision limits
		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Insert elements exceeding collision limits
		collisionKeyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(mapSize + i)
			v := Uint64Value(mapSize + i)
			collisionKeyValues[k] = v

			digests := []Digest{Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.Equal(t, 1, errorCategorizationCount(err))
			var fatalError *FatalError
			var collisionLimitError *CollisionLimitError
			require.ErrorAs(t, err, &fatalError)
			require.ErrorAs(t, err, &collisionLimitError)
			require.ErrorAs(t, fatalError, &collisionLimitError)
			require.Nil(t, existingStorable)
		}

		// Verify that no new elements exceeding collision limit inserted
		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Update elements within collision limits
		for k := range keyValues {
			v := Uint64Value(0)
			keyValues[k] = v
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision limit > 0", func(t *testing.T) {
		const mapSize = 1024

		SetThreshold(256)
		defer SetThreshold(1024)

		// Set noncryptographic hash collision limit as 7,
		// meaning at most 8 elements in collision group per digest at first level.
		MaxCollisionLimitPerDigest = uint32(7)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(i)
			v := Uint64Value(i)
			keyValues[k] = v

			digests := []Digest{Digest(i % 128)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// Insert elements within collision limits
		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Insert elements exceeding collision limits
		collisionKeyValues := make(map[Value]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			k := Uint64Value(mapSize + i)
			v := Uint64Value(mapSize + i)
			collisionKeyValues[k] = v

			digests := []Digest{Digest(i % 128)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.Equal(t, 1, errorCategorizationCount(err))
			var fatalError *FatalError
			var collisionLimitError *CollisionLimitError
			require.ErrorAs(t, err, &fatalError)
			require.ErrorAs(t, err, &collisionLimitError)
			require.ErrorAs(t, fatalError, &collisionLimitError)
			require.Nil(t, existingStorable)
		}

		// Verify that no new elements exceeding collision limit inserted
		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Update elements within collision limits
		for k := range keyValues {
			v := Uint64Value(0)
			keyValues[k] = v
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapLoadedValueIterator(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parent map: 1 root data slab
		require.Equal(t, 1, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, nil)
	})

	t.Run("root data slab with simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithSimpleValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map: 1 root data slab
		require.Equal(t, 1, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map: 1 root data slab
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values in collision group", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 2), Digest(i)} },
		)

		// parent map: 1 root data slab
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values in external collision group", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision group, 4 elements in the group.
		const mapSize = 12
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 4), Digest(i)} },
		)

		// parent map: 1 root data slab, 3 external collision group
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+3+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map: 1 root data slab
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from front to back.
		for i := 0; i < len(values); i++ {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[i+1:]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with long string keys, unload key from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithLongStringKey(t, storage, address, typeInfo, mapSize)

		// parent map: 1 root data slab
		// long string keys: 1 storable slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload external key from front to back.
		for i := 0; i < len(values); i++ {
			k := values[i][0]

			s, ok := k.(StringValue)
			require.True(t, ok)

			// Find storage id for StringValue s.
			var keyID SlabID
			for id, slab := range storage.deltas {
				if sslab, ok := slab.(*StorableSlab); ok {
					if other, ok := sslab.storable.(StringValue); ok {
						if s.str == other.str {
							keyID = id
							break
						}
					}
				}
			}

			require.NoError(t, keyID.Valid())

			err := storage.Remove(keyID)
			require.NoError(t, err)

			expectedValues := values[i+1:]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in collision group, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 2), Digest(i)} },
		)

		// parent map: 1 root data slab
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from front to back.
		for i := 0; i < len(values); i++ {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[i+1:]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 4), Digest(i)} },
		)

		// parent map: 1 root data slab, 3 external collision group
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+3+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from front to back
		for i := 0; i < len(values); i++ {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[i+1:]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload external slab from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 4), Digest(i)} },
		)

		// parent map: 1 root data slab, 3 external collision group
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+3+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload external collision group slab from front to back

		var externalCollisionSlabIDs []SlabID
		for id, slab := range storage.deltas {
			if dataSlab, ok := slab.(*MapDataSlab); ok {
				if dataSlab.collisionGroup {
					externalCollisionSlabIDs = append(externalCollisionSlabIDs, id)
				}
			}
		}
		require.Equal(t, 3, len(externalCollisionSlabIDs))

		sort.Slice(externalCollisionSlabIDs, func(i, j int) bool {
			a := externalCollisionSlabIDs[i]
			b := externalCollisionSlabIDs[j]
			if a.address == b.address {
				return a.IndexAsUint64() < b.IndexAsUint64()
			}
			return a.AddressAsUint64() < b.AddressAsUint64()
		})

		for i, id := range externalCollisionSlabIDs {
			err := storage.Remove(id)
			require.NoError(t, err)

			expectedValues := values[i*4+4:]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values, unload composite value from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map: 1 root data slab
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from back to front.
		for i := len(values) - 1; i >= 0; i-- {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[:i]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with long string key, unload key from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithLongStringKey(t, storage, address, typeInfo, mapSize)

		// parent map: 1 root data slab
		// long string keys: 1 storable slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from front to back.
		for i := len(values) - 1; i >= 0; i-- {
			k := values[i][0]

			s, ok := k.(StringValue)
			require.True(t, ok)

			// Find storage id for StringValue s.
			var keyID SlabID
			for id, slab := range storage.deltas {
				if sslab, ok := slab.(*StorableSlab); ok {
					if other, ok := sslab.storable.(StringValue); ok {
						if s.str == other.str {
							keyID = id
							break
						}
					}
				}
			}

			require.NoError(t, keyID.Valid())

			err := storage.Remove(keyID)
			require.NoError(t, err)

			expectedValues := values[:i]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in collision group, unload value from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 2), Digest(i)} },
		)

		// parent map: 1 root data slab
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[:i]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload value from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 4), Digest(i)} },
		)

		// parent map: 1 root data slab, 3 external collision group
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+3+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[:i]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload external slab from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 4), Digest(i)} },
		)

		// parent map: 1 root data slab, 3 external collision group
		// composite elements: 1 root data slab for each
		require.Equal(t, 1+3+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload external slabs from back to front
		var externalCollisionSlabIDs []SlabID
		for id, slab := range storage.deltas {
			if dataSlab, ok := slab.(*MapDataSlab); ok {
				if dataSlab.collisionGroup {
					externalCollisionSlabIDs = append(externalCollisionSlabIDs, id)
				}
			}
		}
		require.Equal(t, 3, len(externalCollisionSlabIDs))

		sort.Slice(externalCollisionSlabIDs, func(i, j int) bool {
			a := externalCollisionSlabIDs[i]
			b := externalCollisionSlabIDs[j]
			if a.address == b.address {
				return a.IndexAsUint64() < b.IndexAsUint64()
			}
			return a.AddressAsUint64() < b.AddressAsUint64()
		})

		for i := len(externalCollisionSlabIDs) - 1; i >= 0; i-- {
			err := storage.Remove(externalCollisionSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[:i*4]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload value in the middle
		unloadValueIndex := 1

		err := storage.Remove(childSlabIDs[unloadValueIndex])
		require.NoError(t, err)

		copy(values[unloadValueIndex:], values[unloadValueIndex+1:])
		values = values[:len(values)-1]

		testMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with long string key, unload key in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithLongStringKey(t, storage, address, typeInfo, mapSize)

		// parent map: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload key in the middle.
		unloadValueIndex := 1

		k := values[unloadValueIndex][0]

		s, ok := k.(StringValue)
		require.True(t, ok)

		// Find storage id for StringValue s.
		var keyID SlabID
		for id, slab := range storage.deltas {
			if sslab, ok := slab.(*StorableSlab); ok {
				if other, ok := sslab.storable.(StringValue); ok {
					if s.str == other.str {
						keyID = id
						break
					}
				}
			}
		}

		require.NoError(t, keyID.Valid())

		err := storage.Remove(keyID)
		require.NoError(t, err)

		copy(values[unloadValueIndex:], values[unloadValueIndex+1:])
		values = values[:len(values)-1]

		testMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values in collision group, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 2), Digest(i)} },
		)

		// parent map: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element in the middle
		for _, unloadValueIndex := range []int{1, 3, 5} {
			err := storage.Remove(childSlabIDs[unloadValueIndex])
			require.NoError(t, err)
		}

		expectedValues := [][2]Value{
			values[0],
			values[2],
			values[4],
		}
		testMapLoadedElements(t, m, expectedValues)
	})

	t.Run("root data slab with composite values in external collision group, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 4), Digest(i)} },
		)

		// parent map: 1 root data slab, 3 external collision group
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+3+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite value in the middle.
		for _, unloadValueIndex := range []int{1, 3, 5, 7, 9, 11} {
			err := storage.Remove(childSlabIDs[unloadValueIndex])
			require.NoError(t, err)
		}

		expectedValues := [][2]Value{
			values[0],
			values[2],
			values[4],
			values[6],
			values[8],
			values[10],
		}
		testMapLoadedElements(t, m, expectedValues)
	})

	t.Run("root data slab with composite values in external collision group, unload external slab in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i / 4), Digest(i)} },
		)

		// parent map: 1 root data slab, 3 external collision group
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+3+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload external slabs in the middle.
		var externalCollisionSlabIDs []SlabID
		for id, slab := range storage.deltas {
			if dataSlab, ok := slab.(*MapDataSlab); ok {
				if dataSlab.collisionGroup {
					externalCollisionSlabIDs = append(externalCollisionSlabIDs, id)
				}
			}
		}
		require.Equal(t, 3, len(externalCollisionSlabIDs))

		sort.Slice(externalCollisionSlabIDs, func(i, j int) bool {
			a := externalCollisionSlabIDs[i]
			b := externalCollisionSlabIDs[j]
			if a.address == b.address {
				return a.IndexAsUint64() < b.IndexAsUint64()
			}
			return a.AddressAsUint64() < b.AddressAsUint64()
		})

		id := externalCollisionSlabIDs[1]
		err := storage.Remove(id)
		require.NoError(t, err)

		copy(values[4:], values[8:])
		values = values[:8]

		testMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values, unload composite elements during iteration", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		i := 0
		err := m.IterateReadOnlyLoadedValues(func(k Value, v Value) (bool, error) {
			// At this point, iterator returned first element (v).

			// Remove all other nested composite elements (except first element) from storage.
			for _, slabID := range childSlabIDs[1:] {
				err := storage.Remove(slabID)
				require.NoError(t, err)
			}

			require.Equal(t, 0, i)
			valueEqual(t, values[0][0], k)
			valueEqual(t, values[0][1], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 1, i) // Only first element is iterated because other elements are remove during iteration.
	})

	t.Run("root data slab with simple and composite values, unloading composite value", func(t *testing.T) {
		const mapSize = 3

		// Create a map with nested composite value at specified index
		for childArrayIndex := 0; childArrayIndex < mapSize; childArrayIndex++ {
			storage := newTestPersistentStorage(t)

			m, values, childSlabID := createMapWithSimpleAndChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapSize,
				childArrayIndex,
				func(i int) []Digest { return []Digest{Digest(i)} },
			)

			// parent map: 1 root data slab
			// composite element: 1 root data slab
			require.Equal(t, 2, len(storage.deltas))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, values)

			// Unload composite value
			err := storage.Remove(childSlabID)
			require.NoError(t, err)

			copy(values[childArrayIndex:], values[childArrayIndex+1:])
			values = values[:len(values)-1]

			testMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab with simple values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values := createMapWithSimpleValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		require.Equal(t, 4, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)
	})

	t.Run("root metadata slab with composite values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		// composite values: 1 root data slab for each
		require.Equal(t, 4+mapSize, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)
	})

	t.Run("root metadata slab with composite values, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		// composite values : 1 root data slab for each
		require.Equal(t, 4+mapSize, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from front to back
		for i := 0; i < len(values); i++ {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[i+1:]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root metadata slab with composite values, unload values from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		// composite values: 1 root data slab for each
		require.Equal(t, 4+mapSize, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			expectedValues := values[:i]
			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root metadata slab with composite values, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		// composite values: 1 root data slab for each
		require.Equal(t, 4+mapSize, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		// Unload composite element in the middle
		for _, index := range []int{4, 14} {
			err := storage.Remove(childSlabIDs[index])
			require.NoError(t, err)

			copy(values[index:], values[index+1:])
			values = values[:len(values)-1]

			copy(childSlabIDs[index:], childSlabIDs[index+1:])
			childSlabIDs = childSlabIDs[:len(childSlabIDs)-1]

			testMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab with simple and composite values, unload composite value", func(t *testing.T) {
		const mapSize = 20

		// Create a map with nested composite value at specified index
		for childArrayIndex := 0; childArrayIndex < mapSize; childArrayIndex++ {
			storage := newTestPersistentStorage(t)

			m, values, childSlabID := createMapWithSimpleAndChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapSize,
				childArrayIndex,
				func(i int) []Digest { return []Digest{Digest(i)} },
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			// composite values: 1 root data slab for each
			require.Equal(t, 5, len(storage.deltas))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, values)

			err := storage.Remove(childSlabID)
			require.NoError(t, err)

			copy(values[childArrayIndex:], values[childArrayIndex+1:])
			values = values[:len(values)-1]

			testMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab, unload data slab from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20

		m, values := createMapWithSimpleValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		require.Equal(t, 4, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		rootMetaDataSlab, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)

		// Unload data slabs from front to back
		for i := 0; i < len(rootMetaDataSlab.childrenHeaders); i++ {

			childHeader := rootMetaDataSlab.childrenHeaders[i]

			// Get data slab element count before unload it from storage.
			// Element count isn't in the header.
			mapDataSlab, ok := storage.deltas[childHeader.slabID].(*MapDataSlab)
			require.True(t, ok)

			count := mapDataSlab.elements.Count()

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			values = values[count:]

			testMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab, unload data slab from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20

		m, values := createMapWithSimpleValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		require.Equal(t, 4, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		rootMetaDataSlab, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)

		// Unload data slabs from back to front
		for i := len(rootMetaDataSlab.childrenHeaders) - 1; i >= 0; i-- {

			childHeader := rootMetaDataSlab.childrenHeaders[i]

			// Get data slab element count before unload it from storage
			// Element count isn't in the header.
			mapDataSlab, ok := storage.deltas[childHeader.slabID].(*MapDataSlab)
			require.True(t, ok)

			count := mapDataSlab.elements.Count()

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			values = values[:len(values)-int(count)]

			testMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab, unload data slab in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20

		m, values := createMapWithSimpleValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (2 levels): 1 root metadata slab, 3 data slabs
		require.Equal(t, 4, len(storage.deltas))
		require.Equal(t, 1, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, values)

		rootMetaDataSlab, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)

		require.True(t, len(rootMetaDataSlab.childrenHeaders) > 2)

		index := 1
		childHeader := rootMetaDataSlab.childrenHeaders[index]

		// Get element count from previous data slab
		mapDataSlab, ok := storage.deltas[rootMetaDataSlab.childrenHeaders[0].slabID].(*MapDataSlab)
		require.True(t, ok)

		countAtIndex0 := mapDataSlab.elements.Count()

		// Get element count from slab to be unloaded
		mapDataSlab, ok = storage.deltas[rootMetaDataSlab.childrenHeaders[index].slabID].(*MapDataSlab)
		require.True(t, ok)

		countAtIndex1 := mapDataSlab.elements.Count()

		err := storage.Remove(childHeader.slabID)
		require.NoError(t, err)

		copy(values[countAtIndex0:], values[countAtIndex0+countAtIndex1:])
		values = values[:m.Count()-uint64(countAtIndex1)]

		testMapLoadedElements(t, m, values)
	})

	t.Run("root metadata slab, unload non-root metadata slab from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 200

		m, values := createMapWithSimpleValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (3 levels): 1 root metadata slab, 3 child metadata slabs, n data slabs
		require.Equal(t, 4, getMapMetaDataSlabCount(storage))

		rootMetaDataSlab, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)

		// Unload non-root metadata slabs from front to back.
		for i := 0; i < len(rootMetaDataSlab.childrenHeaders); i++ {

			childHeader := rootMetaDataSlab.childrenHeaders[i]

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			// Use firstKey to deduce number of elements in slab.
			var expectedValues [][2]Value
			if i < len(rootMetaDataSlab.childrenHeaders)-1 {
				nextChildHeader := rootMetaDataSlab.childrenHeaders[i+1]
				expectedValues = values[int(nextChildHeader.firstKey):]
			}

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root metadata slab, unload non-root metadata slab from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 200

		m, values := createMapWithSimpleValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (3 levels): 1 root metadata slab, 3 child metadata slabs, n data slabs
		require.Equal(t, 4, getMapMetaDataSlabCount(storage))

		rootMetaDataSlab, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)

		// Unload non-root metadata slabs from back to front.
		for i := len(rootMetaDataSlab.childrenHeaders) - 1; i >= 0; i-- {

			childHeader := rootMetaDataSlab.childrenHeaders[i]

			err := storage.Remove(childHeader.slabID)
			require.NoError(t, err)

			// Use firstKey to deduce number of elements in slabs.
			values = values[:childHeader.firstKey]

			testMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab with composite values, unload composite value at random index", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const mapSize = 500
		m, values, childSlabIDs := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
		// nested composite elements: 1 root data slab for each
		require.True(t, len(storage.deltas) > 1+mapSize)
		require.True(t, getMapMetaDataSlabCount(storage) > 1)

		testMapLoadedElements(t, m, values)

		r := newRand(t)

		// Unload composite element in random position
		for len(values) > 0 {

			i := r.Intn(len(values))

			err := storage.Remove(childSlabIDs[i])
			require.NoError(t, err)

			copy(values[i:], values[i+1:])
			values = values[:len(values)-1]

			copy(childSlabIDs[i:], childSlabIDs[i+1:])
			childSlabIDs = childSlabIDs[:len(childSlabIDs)-1]

			testMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab with composite values, unload random data slab", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const mapSize = 500
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
		// composite values: 1 root data slab for each
		require.True(t, len(storage.deltas) > 1+mapSize)
		require.True(t, getMapMetaDataSlabCount(storage) > 1)

		testMapLoadedElements(t, m, values)

		rootMetaDataSlab, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)

		type slabInfo struct {
			id         SlabID
			startIndex int
			count      int
		}

		var dataSlabInfos []*slabInfo
		for _, mheader := range rootMetaDataSlab.childrenHeaders {

			nonRootMetaDataSlab, ok := storage.deltas[mheader.slabID].(*MapMetaDataSlab)
			require.True(t, ok)

			for i := 0; i < len(nonRootMetaDataSlab.childrenHeaders); i++ {
				h := nonRootMetaDataSlab.childrenHeaders[i]

				if len(dataSlabInfos) > 0 {
					// Update previous slabInfo.count
					dataSlabInfos[len(dataSlabInfos)-1].count = int(h.firstKey) - dataSlabInfos[len(dataSlabInfos)-1].startIndex
				}

				dataSlabInfos = append(dataSlabInfos, &slabInfo{id: h.slabID, startIndex: int(h.firstKey)})
			}
		}

		r := newRand(t)

		for len(dataSlabInfos) > 0 {
			index := r.Intn(len(dataSlabInfos))

			slabToBeRemoved := dataSlabInfos[index]

			// Update startIndex for all subsequence data slabs
			for i := index + 1; i < len(dataSlabInfos); i++ {
				dataSlabInfos[i].startIndex -= slabToBeRemoved.count
			}

			err := storage.Remove(slabToBeRemoved.id)
			require.NoError(t, err)

			if index == len(dataSlabInfos)-1 {
				values = values[:slabToBeRemoved.startIndex]
			} else {
				copy(values[slabToBeRemoved.startIndex:], values[slabToBeRemoved.startIndex+slabToBeRemoved.count:])
				values = values[:len(values)-slabToBeRemoved.count]
			}

			copy(dataSlabInfos[index:], dataSlabInfos[index+1:])
			dataSlabInfos = dataSlabInfos[:len(dataSlabInfos)-1]

			testMapLoadedElements(t, m, values)
		}

		require.Equal(t, 0, len(values))
	})

	t.Run("root metadata slab with composite values, unload random slab", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const mapSize = 500
		m, values, _ := createMapWithChildArrayValues(
			t,
			storage,
			address,
			typeInfo,
			mapSize,
			func(i int) []Digest { return []Digest{Digest(i)} },
		)

		// parent map (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
		// composite values: 1 root data slab for each
		require.True(t, len(storage.deltas) > 1+mapSize)
		require.True(t, getMapMetaDataSlabCount(storage) > 1)

		testMapLoadedElements(t, m, values)

		type slabInfo struct {
			id         SlabID
			startIndex int
			count      int
			children   []*slabInfo
		}

		rootMetaDataSlab, ok := m.root.(*MapMetaDataSlab)
		require.True(t, ok)

		metadataSlabInfos := make([]*slabInfo, len(rootMetaDataSlab.childrenHeaders))
		for i, mheader := range rootMetaDataSlab.childrenHeaders {

			if i > 0 {
				prevMetaDataSlabInfo := metadataSlabInfos[i-1]
				prevDataSlabInfo := prevMetaDataSlabInfo.children[len(prevMetaDataSlabInfo.children)-1]

				// Update previous metadata slab count
				prevMetaDataSlabInfo.count = int(mheader.firstKey) - prevMetaDataSlabInfo.startIndex

				// Update previous data slab count
				prevDataSlabInfo.count = int(mheader.firstKey) - prevDataSlabInfo.startIndex
			}

			metadataSlabInfo := &slabInfo{
				id:         mheader.slabID,
				startIndex: int(mheader.firstKey),
			}

			nonRootMetadataSlab, ok := storage.deltas[mheader.slabID].(*MapMetaDataSlab)
			require.True(t, ok)

			children := make([]*slabInfo, len(nonRootMetadataSlab.childrenHeaders))
			for i, h := range nonRootMetadataSlab.childrenHeaders {
				children[i] = &slabInfo{
					id:         h.slabID,
					startIndex: int(h.firstKey),
				}
				if i > 0 {
					children[i-1].count = int(h.firstKey) - children[i-1].startIndex
				}
			}

			metadataSlabInfo.children = children
			metadataSlabInfos[i] = metadataSlabInfo
		}

		const (
			metadataSlabType int = iota
			dataSlabType
			maxSlabType
		)

		r := newRand(t)

		for len(metadataSlabInfos) > 0 {

			var slabInfoToBeRemoved *slabInfo
			var isLastSlab bool

			switch r.Intn(maxSlabType) {

			case metadataSlabType:

				metadataSlabIndex := r.Intn(len(metadataSlabInfos))

				isLastSlab = metadataSlabIndex == len(metadataSlabInfos)-1

				slabInfoToBeRemoved = metadataSlabInfos[metadataSlabIndex]

				count := slabInfoToBeRemoved.count

				// Update startIndex for subsequence metadata slabs
				for i := metadataSlabIndex + 1; i < len(metadataSlabInfos); i++ {
					metadataSlabInfos[i].startIndex -= count

					for j := 0; j < len(metadataSlabInfos[i].children); j++ {
						metadataSlabInfos[i].children[j].startIndex -= count
					}
				}

				copy(metadataSlabInfos[metadataSlabIndex:], metadataSlabInfos[metadataSlabIndex+1:])
				metadataSlabInfos = metadataSlabInfos[:len(metadataSlabInfos)-1]

			case dataSlabType:

				metadataSlabIndex := r.Intn(len(metadataSlabInfos))

				metadataSlabInfo := metadataSlabInfos[metadataSlabIndex]

				dataSlabIndex := r.Intn(len(metadataSlabInfo.children))

				isLastSlab = (metadataSlabIndex == len(metadataSlabInfos)-1) &&
					(dataSlabIndex == len(metadataSlabInfo.children)-1)

				slabInfoToBeRemoved = metadataSlabInfo.children[dataSlabIndex]

				count := slabInfoToBeRemoved.count

				// Update startIndex for all subsequence data slabs in this metadata slab info
				for i := dataSlabIndex + 1; i < len(metadataSlabInfo.children); i++ {
					metadataSlabInfo.children[i].startIndex -= count
				}

				copy(metadataSlabInfo.children[dataSlabIndex:], metadataSlabInfo.children[dataSlabIndex+1:])
				metadataSlabInfo.children = metadataSlabInfo.children[:len(metadataSlabInfo.children)-1]

				metadataSlabInfo.count -= count

				// Update startIndex for all subsequence metadata slabs.
				for i := metadataSlabIndex + 1; i < len(metadataSlabInfos); i++ {
					metadataSlabInfos[i].startIndex -= count

					for j := 0; j < len(metadataSlabInfos[i].children); j++ {
						metadataSlabInfos[i].children[j].startIndex -= count
					}
				}

				if len(metadataSlabInfo.children) == 0 {
					copy(metadataSlabInfos[metadataSlabIndex:], metadataSlabInfos[metadataSlabIndex+1:])
					metadataSlabInfos = metadataSlabInfos[:len(metadataSlabInfos)-1]
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

			testMapLoadedElements(t, m, values)
		}

		require.Equal(t, 0, len(values))
	})
}

func createMapWithLongStringKey(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	size int,
) (*OrderedMap, [][2]Value) {

	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map.
	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	expectedValues := make([][2]Value, size)
	r := 'a'
	for i := 0; i < size; i++ {
		s := strings.Repeat(string(r), int(maxInlineMapElementSize))

		k := NewStringValue(s)
		v := Uint64Value(i)

		expectedValues[i] = [2]Value{k, v}

		digests := []Digest{Digest(i)}
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		r++
	}

	return m, expectedValues
}

func createMapWithSimpleValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	size int,
	newDigests func(i int) []Digest,
) (*OrderedMap, [][2]Value) {

	digesterBuilder := &mockDigesterBuilder{}

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	expectedValues := make([][2]Value, size)
	r := rune('a')
	for i := 0; i < size; i++ {
		k := Uint64Value(i)
		v := NewStringValue(strings.Repeat(string(r), 20))

		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		expectedValues[i] = [2]Value{k, v}

		existingStorable, err := m.Set(compare, hashInputProvider, expectedValues[i][0], expectedValues[i][1])
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	return m, expectedValues
}

func createMapWithChildArrayValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	size int,
	newDigests func(i int) []Digest,
) (*OrderedMap, [][2]Value, []SlabID) {
	const childArraySize = 50

	// Use mockDigesterBuilder to guarantee element order.
	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map
	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	slabIDs := make([]SlabID, size)
	expectedValues := make([][2]Value, size)
	for i := 0; i < size; i++ {
		// Create child array
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedChildValues := make([]Value, childArraySize)
		for j := 0; j < childArraySize; j++ {
			v := Uint64Value(j)

			err = childArray.Append(v)
			require.NoError(t, err)

			expectedChildValues[j] = v
		}

		k := Uint64Value(i)
		v := childArray

		expectedValues[i] = [2]Value{k, arrayValue(expectedChildValues)}
		slabIDs[i] = childArray.SlabID()

		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		// Set child array to parent
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	return m, expectedValues, slabIDs
}

func createMapWithSimpleAndChildArrayValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	size int,
	compositeValueIndex int,
	newDigests func(i int) []Digest,
) (*OrderedMap, [][2]Value, SlabID) {
	const childArraySize = 50

	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map
	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	var slabID SlabID
	values := make([][2]Value, size)
	r := 'a'
	for i := 0; i < size; i++ {

		k := Uint64Value(i)

		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		if compositeValueIndex == i {
			// Create child array with one element
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			expectedChildValues := make([]Value, childArraySize)
			for j := 0; j < childArraySize; j++ {
				v := Uint64Value(j)
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedChildValues[j] = v
			}

			values[i] = [2]Value{k, arrayValue(expectedChildValues)}

			existingStorable, err := m.Set(compare, hashInputProvider, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			slabID = childArray.SlabID()

		} else {
			v := NewStringValue(strings.Repeat(string(r), 18))
			values[i] = [2]Value{k, v}

			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}
	}

	return m, values, slabID
}

func testMapLoadedElements(t *testing.T, m *OrderedMap, expectedValues [][2]Value) {
	i := 0
	err := m.IterateReadOnlyLoadedValues(func(k Value, v Value) (bool, error) {
		require.True(t, i < len(expectedValues))
		valueEqual(t, expectedValues[i][0], k)
		valueEqual(t, expectedValues[i][1], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expectedValues), i)
}

func getMapMetaDataSlabCount(storage *PersistentSlabStorage) int {
	var counter int
	for _, slab := range storage.deltas {
		if _, ok := slab.(*MapMetaDataSlab); ok {
			counter++
		}
	}
	return counter
}

func TestMaxInlineMapValueSize(t *testing.T) {

	t.Run("small key", func(t *testing.T) {
		// Value has larger max inline size when key is less than max map key size.

		SetThreshold(256)
		defer SetThreshold(1024)

		mapSize := 2
		keyStringSize := 16                               // Key size is less than max map key size.
		valueStringSize := maxInlineMapElementSize/2 + 10 // Value size is more than half of max map element size.

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, int(valueStringSize)))
			keyValues[k] = v
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Both key and value are stored in map slab.
		require.Equal(t, 1, len(storage.deltas))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("max size key", func(t *testing.T) {
		// Value max size is about half of max map element size when key is exactly max map key size.

		SetThreshold(256)
		defer SetThreshold(1024)

		mapSize := 1
		keyStringSize := maxInlineMapKeySize - 2         // Key size is exactly max map key size (2 bytes is string encoding overhead).
		valueStringSize := maxInlineMapElementSize/2 + 2 // Value size is more than half of max map element size (add 2 bytes to make it more than half).

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, int(keyStringSize)))
			v := NewStringValue(randStr(r, int(valueStringSize)))
			keyValues[k] = v
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Key is stored in map slab, while value is stored separately in storable slab.
		require.Equal(t, 2, len(storage.deltas))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("large key", func(t *testing.T) {
		// Value has larger max inline size when key is more than max map key size because
		// when key size exceeds max map key size, it is stored in a separate storable slab,
		// and SlabIDStorable is stored as key in the map, which is 19 bytes.

		SetThreshold(256)
		defer SetThreshold(1024)

		mapSize := 1
		keyStringSize := maxInlineMapKeySize + 10         // key size is more than max map key size
		valueStringSize := maxInlineMapElementSize/2 + 10 // value size is more than half of max map element size

		r := newRand(t)

		keyValues := make(map[Value]Value, mapSize)
		for len(keyValues) < mapSize {
			k := NewStringValue(randStr(r, int(keyStringSize)))
			v := NewStringValue(randStr(r, int(valueStringSize)))
			keyValues[k] = v
		}

		typeInfo := testTypeInfo{42}
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Key is stored in separate storable slabs, while value is stored in map slab.
		require.Equal(t, 2, len(storage.deltas))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapID(t *testing.T) {
	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	sid := m.SlabID()
	id := m.ValueID()

	require.Equal(t, sid.address[:], id[:SlabAddressLength])
	require.Equal(t, sid.index[:], id[SlabAddressLength:])
}

func TestSlabSizeWhenResettingMutableStorableInMap(t *testing.T) {
	const (
		mapSize             = 3
		keyStringSize       = 16
		initialStorableSize = 1
		mutatedStorableSize = 5
	)

	keyValues := make(map[Value]*testMutableValue, mapSize)
	for i := 0; i < mapSize; i++ {
		k := Uint64Value(i)
		v := newTestMutableValue(initialStorableSize)
		keyValues[k] = v
	}

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	require.True(t, m.root.IsData())

	expectedElementSize := singleElementPrefixSize + digestSize + Uint64Value(0).ByteSize() + initialStorableSize
	expectedMapRootDataSlabSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize + expectedElementSize*mapSize
	require.Equal(t, expectedMapRootDataSlabSize, m.root.ByteSize())

	err = VerifyMap(m, address, typeInfo, typeInfoComparator, hashInputProvider, true)
	require.NoError(t, err)

	// Reset mutable values after changing its storable size
	for k, v := range keyValues {
		v.updateStorableSize(mutatedStorableSize)

		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)
	}

	require.True(t, m.root.IsData())

	expectedElementSize = singleElementPrefixSize + digestSize + Uint64Value(0).ByteSize() + mutatedStorableSize
	expectedMapRootDataSlabSize = mapRootDataSlabPrefixSize + hkeyElementsPrefixSize + expectedElementSize*mapSize
	require.Equal(t, expectedMapRootDataSlabSize, m.root.ByteSize())

	err = VerifyMap(m, address, typeInfo, typeInfoComparator, hashInputProvider, true)
	require.NoError(t, err)
}

func TestChildMapInlinabilityInParentMap(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	const expectedEmptyInlinedMapSize = uint32(inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize) // 22

	t.Run("parent is root data slab, with one child map", func(t *testing.T) {
		const (
			mapSize         = 1
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentMap, expectedKeyValues := createMapWithEmptyChildMap(t, storage, address, typeInfo, mapSize, func() Value {
			return NewStringValue(randStr(r, keyStringSize))
		})

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)

		// Appending 3 elements to child map so that inlined child map reaches max inlined size as map element.
		for i := 0; i < 3; i++ {
			for childKey, child := range children {
				childMap := child.m
				valueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
				require.True(t, ok)

				k := NewStringValue(randStr(r, keyStringSize))
				v := NewStringValue(randStr(r, valueStringSize))

				existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
				require.Equal(t, uint64(i+1), childMap.Count())

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childMap.ValueID())        // Value ID is unchanged
				require.Equal(t, 1, getStoredDeltas(storage))

				// Test inlined child slab size
				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

				// Test parent slab size
				expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedInlinedMapSize
				expectedParentSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) +
					expectedParentElementSize*mapSize
				require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		i := 0
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			require.False(t, childMap.Inlined())
			require.Equal(t, 1+1+i, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.

			i++

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childMap.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childMap.ValueID())       // Value ID is unchanged

			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedStandaloneSlabSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedStandaloneSlabSize, childMap.root.ByteSize())

			expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + SlabIDStorable(expectedSlabID).ByteSize()
			expectedParentSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) +
				expectedParentElementSize*mapSize
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Remove elements from child map which triggers standalone map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			keys := make([]Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID())
				require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged
				require.Equal(t, 1, getStoredDeltas(storage))

				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

				expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedInlinedMapSize
				expectedParentSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) +
					expectedParentElementSize*mapSize
				require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.
	})

	t.Run("parent is root data slab, with two child maps", func(t *testing.T) {
		const (
			mapSize         = 2
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentMap, expectedKeyValues := createMapWithEmptyChildMap(t, storage, address, typeInfo, mapSize, func() Value {
			return NewStringValue(randStr(r, keyStringSize))
		})

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)

		expectedParentSize := parentMap.root.ByteSize()

		// Appending 3 elements to child map so that inlined child map reaches max inlined size as map element.
		for i := 0; i < 3; i++ {
			for childKey, child := range children {
				childMap := child.m
				valueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
				require.True(t, ok)

				k := NewStringValue(randStr(r, keyStringSize))
				v := NewStringValue(randStr(r, valueStringSize))

				existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
				require.Equal(t, uint64(i+1), childMap.Count())

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childMap.ValueID())        // Value ID is unchanged
				require.Equal(t, 1, getStoredDeltas(storage))

				// Test inlined child slab size
				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

				// Test parent slab size
				expectedParentSize += expectedChildElementSize
				require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		i := 0
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			require.False(t, childMap.Inlined())
			require.Equal(t, 1+1+i, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.

			i++

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childMap.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childMap.ValueID())       // Value ID is unchanged

			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedStandaloneSlabSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedStandaloneSlabSize, childMap.root.ByteSize())

			// Subtract inlined child map size from expected parent size
			expectedParentSize -= uint32(inlinedMapDataSlabPrefixSize+hkeyElementsPrefixSize) +
				expectedChildElementSize*uint32(childMap.Count()-1)
			// Add slab id storable size to expected parent size
			expectedParentSize += SlabIDStorable(expectedSlabID).ByteSize()
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.Equal(t, 1+mapSize, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.

		// Remove one element from each child map which triggers standalone map slab becomes inlined slab again.
		i = 0
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			var aKey Value
			for k := range expectedChildMapValues {
				aKey = k
				break
			}

			existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedChildMapValues, aKey)

			require.Equal(t, 1+mapSize-1-i, getStoredDeltas(storage))

			i++

			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID())
			require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

			// Subtract slab id storable size from expected parent size
			expectedParentSize -= SlabIDStorable(SlabID{}).ByteSize()
			// Add expected inlined child map to expected parent size
			expectedParentSize += expectedInlinedMapSize
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Remove remaining elements from each inlined child map.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			keys := make([]Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				require.Equal(t, 1, getStoredDeltas(storage))

				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID())
				require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

				expectedParentSize -= expectedChildElementSize
				require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.
	})

	t.Run("parent is root metadata slab, with four child maps", func(t *testing.T) {
		const (
			mapSize         = 4
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentMap, expectedKeyValues := createMapWithEmptyChildMap(t, storage, address, typeInfo, mapSize, func() Value {
			return NewStringValue(randStr(r, keyStringSize))
		})

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)

		// Appending 3 elements to child map so that inlined child map reaches max inlined size as map element.
		for i := 0; i < 3; i++ {
			for childKey, child := range children {
				childMap := child.m
				valueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
				require.True(t, ok)

				k := NewStringValue(randStr(r, keyStringSize))
				v := NewStringValue(randStr(r, valueStringSize))

				existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
				require.Equal(t, uint64(i+1), childMap.Count())

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childMap.ValueID())        // Value ID is unchanged

				// Test inlined child slab size
				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		// Parent array has 1 meta data slab and 2 data slabs.
		// All child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, parentMap.root.IsData())

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			require.False(t, childMap.Inlined())

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childMap.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childMap.ValueID())       // Value ID is unchanged

			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedStandaloneSlabSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedStandaloneSlabSize, childMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Parent map has one root data slab.
		// Each child maps has one root data slab.
		require.Equal(t, 1+mapSize, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.
		require.True(t, parentMap.root.IsData())

		// Remove one element from each child map which triggers standalone map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			var aKey Value
			for k := range expectedChildMapValues {
				aKey = k
				break
			}

			existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedChildMapValues, aKey)

			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID())
			require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Parent map has one metadata slab + 2 data slabs.
		require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because child map is inlined again.
		require.False(t, parentMap.root.IsData())

		// Remove remaining elements from each inlined child map.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			keys := make([]Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID())
				require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedInlinedMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedInlinedMapSize, childMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())
		for _, child := range children {
			require.Equal(t, uint64(0), child.m.Count())
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		// Test parent map slab size
		expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedEmptyInlinedMapSize
		expectedParentSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) + // standalone map data slab with 0 element
			expectedParentElementSize*uint32(mapSize)
		require.Equal(t, expectedParentSize, parentMap.root.ByteSize())
	})
}

func TestNestedThreeLevelChildMapInlinabilityInParentMap(t *testing.T) {

	SetThreshold(256)
	defer SetThreshold(1024)

	t.Run("parent is root data slab, one child map, one grand child map, changes to grand child map triggers child map slab to become standalone slab", func(t *testing.T) {
		const (
			mapSize         = 1
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() Value {
			return NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with an inlined child map, with an inlined grand child map
		parentMap, expectedKeyValues := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapSize, getKeyFunc)

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapSize, len(children))

		expectedParentSize := parentMap.root.ByteSize()

		// Inserting 1 elements to grand child map so that inlined grand child map reaches max inlined size as map element.
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())       // Value ID is unchanged

			// Only parent map slab is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			// Test parent slab size
			expectedParentSize += expectedGrandChildElementSize
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Add one more element to grand child map which triggers inlined child map slab (NOT grand child map slab) becomes standalone slab
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is NOT inlined
			require.False(t, childMap.Inlined())
			require.Equal(t, valueIDToSlabID(cValueID), childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())                 // Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 2, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test standalone child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildMapSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			// Test parent slab size
			expectedParentSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
				singleElementPrefixSize + digestSize + encodedKeySize + SlabIDStorable(SlabID{}).ByteSize()
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 2, getStoredDeltas(storage)) // There is 2 stored slab because child map is not inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove elements from grand child map which triggers standalone child map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			gchildKeys := make([]Value, 0, len(expectedGChildMapValues))
			for k := range expectedGChildMapValues {
				gchildKeys = append(gchildKeys, k)
			}

			for _, k := range gchildKeys {
				existingMapKey, existingMapValueStorable, err := gchildMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKey)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedGChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				// Grand child map is inlined
				require.True(t, gchildMap.Inlined())
				require.Equal(t, SlabIDUndefined, gchildMap.SlabID())
				require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedGrandChildElementSize*uint32(gchildMap.Count())
				require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

				// Test inlined child slab size
				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
				expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

				// Test parent child slab size
				expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedChildMapSize
				expectedParentMapSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedParentElementSize*uint32(parentMap.Count())
				require.Equal(t, expectedParentMapSize, parentMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			require.Equal(t, uint64(0), gchildMap.Count())
			require.Equal(t, uint64(1), childMap.Count())
		}

		require.Equal(t, uint64(1), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("parent is root data slab, one child map, one grand child map, changes to grand child map triggers grand child array slab to become standalone slab", func(t *testing.T) {
		const (
			mapSize              = 1
			keyStringSize        = 9
			valueStringSize      = 4
			largeValueStringSize = 40
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()
		encodedLargeValueSize := NewStringValue(strings.Repeat("a", largeValueStringSize)).ByteSize()
		slabIDStorableSize := SlabIDStorable(SlabID{}).ByteSize()

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() Value {
			return NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with an inlined child map, with an inlined grand child map
		parentMap, expectedKeyValues := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapSize, getKeyFunc)

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapSize, len(children))

		expectedParentSize := parentMap.root.ByteSize()

		// Inserting 1 elements to grand child map so that inlined grand child map reaches max inlined size as map element.
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())       // Value ID is unchanged

			// Only parent map slab is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			// Test parent slab size
			expectedParentSize += expectedGrandChildElementSize
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		gchildLargeElementKeys := make(map[Value]Value) // key: child map key, value: gchild map key
		// Add one large element to grand child map which triggers inlined grand child map slab (NOT child map slab) becomes standalone slab
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, largeValueStringSize))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			gchildLargeElementKeys[childKey] = k

			// Grand child map is NOT inlined
			require.False(t, gchildMap.Inlined())
			require.Equal(t, valueIDToSlabID(gValueID), gchildMap.SlabID()) // Slab ID is valid for not inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())                 // Value ID is unchanged

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())       // Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 2, getStoredDeltas(storage))

			// Test standalone grand child slab size
			expectedGrandChildElement1Size := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildElement2Size := singleElementPrefixSize + digestSize + encodedKeySize + encodedLargeValueSize
			expectedGrandChildMapSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElement1Size + expectedGrandChildElement2Size
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + slabIDStorableSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize + expectedChildElementSize
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			// Test parent slab size
			expectedParentSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
				singleElementPrefixSize + digestSize + encodedKeySize + expectedChildMapSize
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 2, getStoredDeltas(storage)) // There is 2 stored slab because child map is not inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove elements from grand child map which triggers standalone child map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			// Get all grand child map keys with large element key first
			keys := make([]Value, 0, len(expectedGChildMapValues))
			keys = append(keys, gchildLargeElementKeys[childKey])
			for k := range expectedGChildMapValues {
				if k != gchildLargeElementKeys[childKey] {
					keys = append(keys, k)
				}
			}

			// Remove all elements (large element first) to trigger grand child map being inlined again.
			for _, k := range keys {

				existingMapKeyStorable, existingMapValueStorable, err := gchildMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedGChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				// Grand child map is inlined
				require.True(t, gchildMap.Inlined())
				require.Equal(t, SlabIDUndefined, gchildMap.SlabID())
				require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedGrandChildElementSize*uint32(gchildMap.Count())
				require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

				// Test inlined child slab size
				expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
				expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

				// Test parent child slab size
				expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedChildMapSize
				expectedParentMapSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedParentElementSize*uint32(parentMap.Count())
				require.Equal(t, expectedParentMapSize, parentMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			require.Equal(t, uint64(0), gchildMap.Count())
			require.Equal(t, uint64(1), childMap.Count())
		}

		require.Equal(t, uint64(1), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("parent is root data slab, two child map, one grand child map each, changes to child map triggers child map slab to become standalone slab", func(t *testing.T) {
		const (
			mapSize         = 2
			keyStringSize   = 4
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()
		slabIDStorableSize := SlabIDStorable(SlabID{}).ByteSize()

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() Value {
			return NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with inlined child map, containing inlined grand child map
		parentMap, expectedKeyValues := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapSize, getKeyFunc)

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapSize, len(children))

		expectedParentSize := parentMap.root.ByteSize()

		// Insert 1 elements to grand child map (both child map and grand child map are still inlined).
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())       // Value ID is unchanged

			// Only parent map slab is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			// Test parent slab size
			expectedParentSize += expectedGrandChildElementSize
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		expectedParentSize = parentMap.root.ByteSize()

		// Add 1 element to each child map so child map reaches its max size
		for childKey, child := range children {

			childMap := child.m
			cValueID := child.valueID

			var gchild *mapInfo
			for _, gv := range child.children {
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())       // Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildElementSize := digestSize + singleElementPrefixSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize := digestSize + singleElementPrefixSize + encodedKeySize + encodedValueSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize + (digestSize + singleElementPrefixSize + encodedKeySize + expectedGrandChildMapSize)
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			// Test parent slab size
			expectedParentSize += digestSize + singleElementPrefixSize + encodedKeySize + encodedValueSize
			require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Add 1 more element to each child map so child map reaches its max size
		i := 0
		for childKey, child := range children {

			childMap := child.m
			cValueID := child.valueID

			var gchild *mapInfo
			for _, gv := range child.children {
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is NOT inlined
			require.False(t, childMap.Inlined())
			require.Equal(t, valueIDToSlabID(cValueID), childMap.SlabID()) // Slab ID is the same as value ID for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())                 // Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, (1 + i + 1), getStoredDeltas(storage))

			i++

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test standalone child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedChildMapSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*2 + (digestSize + singleElementPrefixSize + encodedKeySize + expectedGrandChildMapSize)
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1+mapSize, getStoredDeltas(storage)) // There is 1+mapSize stored slab because all child maps are standalone.

		// Test parent slab size
		expectedParentSize = mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
			(singleElementPrefixSize+digestSize+encodedKeySize+slabIDStorableSize)*mapSize
		require.Equal(t, expectedParentSize, parentMap.root.ByteSize())

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		expectedParentMapSize := parentMap.root.ByteSize()

		// Remove one element from child map which triggers standalone child map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			var aKey Value
			for k := range expectedChildMapValues {
				if k != gchildKey {
					aKey = k
					break
				}
			}

			// Remove one element
			existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedChildMapValues, aKey)

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID())
			require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID())
			require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize1 := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildElementSize2 := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize1 + expectedChildElementSize2*uint32(childMap.Count()-1)
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			// Test parent child slab size
			expectedParentMapSize = expectedParentMapSize - slabIDStorableSize + expectedChildMapSize
			require.Equal(t, expectedParentMapSize, parentMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// remove remaining elements from child map, except for grand child map
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			keys := make([]Value, 0, len(expectedChildMapValues)-1)
			for k := range expectedChildMapValues {
				if k != gchildKey {
					keys = append(keys, k)
				}
			}

			// Remove all elements, except grand child map
			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				// Grand child map is inlined
				require.True(t, gchildMap.Inlined())
				require.Equal(t, SlabIDUndefined, gchildMap.SlabID())
				require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedGrandChildElementSize*uint32(gchildMap.Count())
				require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

				// Test inlined child slab size
				expectedChildElementSize1 := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
				expectedChildElementSize2 := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
				expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize1 + expectedChildElementSize2*uint32(childMap.Count()-1)
				require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

				// Test parent child slab size
				expectedParentMapSize -= digestSize + singleElementPrefixSize + encodedKeySize + encodedValueSize
				require.Equal(t, expectedParentMapSize, parentMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			require.Equal(t, uint64(1), gchildMap.Count())
			require.Equal(t, uint64(1), childMap.Count())
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("parent is root metadata slab, with four child maps, each child map has grand child maps", func(t *testing.T) {
		const (
			mapSize         = 4
			keyStringSize   = 4
			valueStringSize = 8
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()
		slabIDStorableSize := SlabIDStorable(SlabID{}).ByteSize()

		r := newRand(t)

		typeInfo := testTypeInfo{42}
		storage := newTestPersistentStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() Value {
			return NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with inlined child map, containing inlined grand child map
		parentMap, expectedKeyValues := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapSize, getKeyFunc)

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapSize, len(children))

		// Insert 1 element to grand child map
		// Both child map and grand child map are still inlined, but parent map's root slab is metadata slab.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())       // Value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.False(t, parentMap.Inlined())
		require.False(t, parentMap.root.IsData())
		// There is 3 stored slab: parent metadata slab with 2 data slabs (all child and grand child maps are inlined)
		require.Equal(t, 3, getStoredDeltas(storage))

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Insert 1 element to grand child map
		// - grand child maps are inlined
		// - child maps are standalone
		// - parent map's root slab is data slab.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())       // Value ID is unchanged

			// Child map is NOT inlined
			require.False(t, childMap.Inlined())
			require.Equal(t, valueIDToSlabID(cValueID), childMap.SlabID()) // Slab ID is same as value ID
			require.Equal(t, cValueID, childMap.ValueID())                 // Value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test standalone child slab size
			expectedChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildMapSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize*uint32(childMap.Count())
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.False(t, parentMap.Inlined())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1+mapSize, getStoredDeltas(storage))

		// Test parent slab size
		expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + slabIDStorableSize
		expectedParentMapSize := mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
			expectedParentElementSize*uint32(parentMap.Count())
		require.Equal(t, expectedParentMapSize, parentMap.root.ByteSize())

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove one element from grand child map to trigger child map inlined again.
		// - grand child maps are inlined
		// - child maps are inlined
		// - parent map root slab is metadata slab
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(mapValue)
			require.True(t, ok)

			var aKey Value
			for k := range expectedGChildMapValues {
				aKey = k
				break
			}

			// Remove one element from grand child map
			existingMapKeyStorable, existingMapValueStorable, err := gchildMap.Remove(compare, hashInputProvider, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedGChildMapValues, aKey)

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, SlabIDUndefined, childMap.SlabID())
			require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, SlabIDUndefined, gchildMap.SlabID())
			require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildElementSize := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedGrandChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedGrandChildElementSize*uint32(gchildMap.Count())
			require.Equal(t, expectedGrandChildMapSize, gchildMap.root.ByteSize())

			// Test inlined child slab size
			expectedChildElementSize1 := singleElementPrefixSize + digestSize + encodedKeySize + expectedGrandChildMapSize
			expectedChildElementSize2 := singleElementPrefixSize + digestSize + encodedKeySize + encodedValueSize
			expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
				expectedChildElementSize1 + expectedChildElementSize2*uint32(childMap.Count()-1)
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.False(t, parentMap.root.IsData())
		require.Equal(t, 3, getStoredDeltas(storage))

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove all grand child element to trigger
		// - child maps are inlined
		// - parent map root slab is data slab
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
			require.True(t, ok)

			keys := make([]Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			// Remove grand children
			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				// Grand child map is returned as SlabIDStorable, even if it was stored inlined in the parent.
				id, ok := existingMapValueStorable.(SlabIDStorable)
				require.True(t, ok)

				v, err := id.StoredValue(storage)
				require.NoError(t, err)

				gchildMap, ok := v.(*OrderedMap)
				require.True(t, ok)

				expectedGChildMapValues, ok := expectedChildMapValues[k].(mapValue)
				require.True(t, ok)

				valueEqual(t, expectedGChildMapValues, gchildMap)

				err = storage.Remove(SlabID(id))
				require.NoError(t, err)

				delete(expectedChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			expectedChildMapSize := uint32(inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize)
			require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

			require.Equal(t, uint64(0), childMap.Count())
		}

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.True(t, parentMap.root.IsData())
		require.Equal(t, 1, getStoredDeltas(storage))

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		expectedChildMapSize := uint32(inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize)
		expectedParentMapSize = mapRootDataSlabPrefixSize + hkeyElementsPrefixSize +
			(digestSize+singleElementPrefixSize+encodedKeySize+expectedChildMapSize)*uint32(mapSize)
		require.Equal(t, expectedParentMapSize, parentMap.root.ByteSize())
	})
}

func TestChildMapWhenParentMapIsModified(t *testing.T) {
	const (
		mapSize                     = 2
		keyStringSize               = 4
		valueStringSize             = 4
		expectedEmptyInlinedMapSize = uint32(inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize) // 22
	)

	// encoded key size is the same for all string keys of the same length.
	encodedKeySize := NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()

	r := newRand(t)

	typeInfo := testTypeInfo{42}
	storage := newTestPersistentStorage(t)
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	parentMapDigesterBuilder := &mockDigesterBuilder{}
	parentDigest := 1

	// Create parent map with mock digests
	parentMap, err := NewMap(storage, address, parentMapDigesterBuilder, typeInfo)
	require.NoError(t, err)

	expectedKeyValues := make(map[Value]Value)

	// Insert 2 child map with digest values of 1 and 3.
	for i := 0; i < mapSize; i++ {
		// Create child map
		childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := NewStringValue(randStr(r, keyStringSize))

		digests := []Digest{
			Digest(parentDigest),
		}
		parentMapDigesterBuilder.On("Digest", k).Return(mockDigester{digests})
		parentDigest += 2

		// Insert child map to parent map
		existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedKeyValues[k] = mapValue{}

		require.True(t, childMap.Inlined())
		testInlinedMapIDs(t, address, childMap)

		// Test child map slab size
		require.Equal(t, expectedEmptyInlinedMapSize, childMap.root.ByteSize())

		// Test parent map slab size
		expectedParentElementSize := singleElementPrefixSize + digestSize + encodedKeySize + expectedEmptyInlinedMapSize
		expectedParentSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) + // standalone map data slab with 0 element
			expectedParentElementSize*uint32(i+1)
		require.Equal(t, expectedParentSize, parentMap.root.ByteSize())
	}

	require.Equal(t, uint64(mapSize), parentMap.Count())
	require.True(t, parentMap.root.IsData())
	require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

	testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

	children := getInlinedChildMapsFromParentMap(t, address, parentMap)
	require.Equal(t, mapSize, len(children))

	var keysForNonChildMaps []Value

	t.Run("insert elements in parent map", func(t *testing.T) {

		newDigests := []Digest{
			0, // insert value at digest 0, so all child map physical positions are moved by +1
			2, // insert value at digest 2, so second child map physical positions are moved by +1
			4, // insert value at digest 4, so no child map physical positions are moved
		}

		for _, digest := range newDigests {

			k := NewStringValue(randStr(r, keyStringSize))
			v := NewStringValue(randStr(r, valueStringSize))

			digests := []Digest{digest}
			parentMapDigesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedKeyValues[k] = v
			keysForNonChildMaps = append(keysForNonChildMaps, k)

			i := 0
			for childKey, child := range children {
				childMap := child.m
				childValueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
				require.True(t, ok)

				k := NewStringValue(randStr(r, keyStringSize))
				v := Uint64Value(i)

				i++

				existingStorable, err = childMap.Set(compare, hashInputProvider, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, childValueID, childMap.ValueID())   // Value ID is unchanged

				// Test inlined child slab size
				expectedChildElementSize := singleElementPrefixSize + digestSize + k.ByteSize() + v.ByteSize()
				expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
					expectedChildElementSize*uint32(childMap.Count())
				require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		t.Run("remove elements from parent map", func(t *testing.T) {
			// Remove element at digest 0, so all child map physical position are moved by -1.
			// Remove element at digest 2, so only second child map physical position is moved by -1
			// Remove element at digest 4, so no child map physical position is moved by -1

			for _, k := range keysForNonChildMaps {

				existingMapKeyStorable, existingMapValueStorable, err := parentMap.Remove(compare, hashInputProvider, k)
				require.NoError(t, err)
				require.NotNil(t, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedKeyValues, k)

				i := 0
				for childKey, child := range children {
					childMap := child.m
					childValueID := child.valueID

					expectedChildMapValues, ok := expectedKeyValues[childKey].(mapValue)
					require.True(t, ok)

					k := NewStringValue(randStr(r, keyStringSize))
					v := Uint64Value(i)

					i++

					existingStorable, err := childMap.Set(compare, hashInputProvider, k, v)
					require.NoError(t, err)
					require.Nil(t, existingStorable)

					expectedChildMapValues[k] = v

					require.True(t, childMap.Inlined())
					require.Equal(t, SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
					require.Equal(t, childValueID, childMap.ValueID())   // Value ID is unchanged

					// Test inlined child slab size
					expectedChildElementSize := singleElementPrefixSize + digestSize + k.ByteSize() + v.ByteSize()
					expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
						expectedChildElementSize*uint32(childMap.Count())
					require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

					testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
				}
			}
		})
	})
}

func createMapWithEmptyChildMap(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	mapSize int,
	getKey func() Value,
) (*OrderedMap, map[Value]Value) {

	const expectedEmptyInlinedMapSize = uint32(inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize) // 22

	// Create parent map
	parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	expectedKeyValues := make(map[Value]Value)

	for i := 0; i < mapSize; i++ {
		// Create child map
		childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := getKey()

		ks, err := k.Storable(storage, address, maxInlineMapElementSize)
		require.NoError(t, err)

		// Insert child map to parent map
		existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedKeyValues[k] = mapValue{}

		require.True(t, childMap.Inlined())
		testInlinedMapIDs(t, address, childMap)

		// Test child map slab size
		require.Equal(t, expectedEmptyInlinedMapSize, childMap.root.ByteSize())

		// Test parent map slab size
		expectedParentElementSize := singleElementPrefixSize + digestSize + ks.ByteSize() + expectedEmptyInlinedMapSize
		expectedParentSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) + // standalone map data slab with 0 element
			expectedParentElementSize*uint32(i+1)
		require.Equal(t, expectedParentSize, parentMap.root.ByteSize())
	}

	return parentMap, expectedKeyValues
}

func createMapWithEmpty2LevelChildMap(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	mapSize int,
	getKey func() Value,
) (*OrderedMap, map[Value]Value) {

	const expectedEmptyInlinedMapSize = uint32(inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize) // 22

	// Create parent map
	parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	expectedKeyValues := make(map[Value]Value)

	for i := 0; i < mapSize; i++ {
		// Create child map
		childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// Create grand child map
		gchildMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := getKey()

		ks, err := k.Storable(storage, address, maxInlineMapElementSize)
		require.NoError(t, err)

		// Insert grand child map to child map
		existingStorable, err := childMap.Set(compare, hashInputProvider, k, gchildMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, gchildMap.Inlined())
		testInlinedMapIDs(t, address, gchildMap)

		// Insert child map to parent map
		existingStorable, err = parentMap.Set(compare, hashInputProvider, k, childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedKeyValues[k] = mapValue{k: mapValue{}}

		require.True(t, childMap.Inlined())
		testInlinedMapIDs(t, address, childMap)

		// Test grand child map slab size
		require.Equal(t, expectedEmptyInlinedMapSize, gchildMap.root.ByteSize())

		// Test child map slab size
		expectedChildElementSize := singleElementPrefixSize + digestSize + ks.ByteSize() + expectedEmptyInlinedMapSize
		expectedChildMapSize := inlinedMapDataSlabPrefixSize + hkeyElementsPrefixSize +
			expectedChildElementSize*uint32(childMap.Count())
		require.Equal(t, expectedChildMapSize, childMap.root.ByteSize())

		// Test parent map slab size
		expectedParentElementSize := singleElementPrefixSize + digestSize + ks.ByteSize() + expectedChildMapSize
		expectedParentSize := uint32(mapRootDataSlabPrefixSize+hkeyElementsPrefixSize) + // standalone map data slab with 0 element
			expectedParentElementSize*uint32(i+1)
		require.Equal(t, expectedParentSize, parentMap.root.ByteSize())
	}

	testNotInlinedMapIDs(t, address, parentMap)

	return parentMap, expectedKeyValues
}

type mapInfo struct {
	m        *OrderedMap
	valueID  ValueID
	children map[Value]*mapInfo
}

func getInlinedChildMapsFromParentMap(t *testing.T, address Address, parentMap *OrderedMap) map[Value]*mapInfo {

	children := make(map[Value]*mapInfo)

	err := parentMap.IterateReadOnlyKeys(func(k Value) (bool, error) {
		if k == nil {
			return false, nil
		}

		e, err := parentMap.Get(compare, hashInputProvider, k)
		require.NoError(t, err)

		childMap, ok := e.(*OrderedMap)
		if !ok {
			return true, nil
		}

		if childMap.Inlined() {
			testInlinedMapIDs(t, address, childMap)
		} else {
			testNotInlinedMapIDs(t, address, childMap)
		}

		children[k] = &mapInfo{
			m:        childMap,
			valueID:  childMap.ValueID(),
			children: getInlinedChildMapsFromParentMap(t, address, childMap),
		}

		return true, nil
	})
	require.NoError(t, err)

	return children
}

func TestMapSetReturnedValue(t *testing.T) {
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

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

			expectedKeyValues[k] = expectedChildValues
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite existing child array value
		for k := range expectedKeyValues {
			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)

			expectedKeyValues[k] = Uint64Value(0)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			// Insert one element to child array
			v := NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedKeyValues[k] = arrayValue{v}
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite existing child array value
		for k := range expectedKeyValues {
			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			expectedKeyValues[k] = Uint64Value(0)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(mapValue)
			expectedKeyValues[k] = expectedChildValues

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

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite existing child map value
		for k := range expectedKeyValues {
			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			expectedKeyValues[k] = Uint64Value(0)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(mapValue)
			expectedKeyValues[k] = expectedChildValues

			// Insert into child map until child map is not inlined
			v := NewStringValue(strings.Repeat("a", 10))

			existingStorable, err = childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite existing child map value
		for k := range expectedKeyValues {
			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			expectedKeyValues[k] = Uint64Value(0)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})
}

func TestMapRemoveReturnedValue(t *testing.T) {
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

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

			expectedKeyValues[k] = expectedChildValues
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child array value
		for k := range expectedKeyValues {
			keyStorable, valueStorable, err := parentMap.Remove(compare, hashInputProvider, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)

			delete(expectedKeyValues, k)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child array
			childArray, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			// Insert one element to child array
			v := NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedKeyValues[k] = arrayValue{v}
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child array value
		for k := range expectedKeyValues {
			keyStorable, valueStorable, err := parentMap.Remove(compare, hashInputProvider, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			delete(expectedKeyValues, k)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(mapValue)
			expectedKeyValues[k] = expectedChildValues

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

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child map value
		for k := range expectedKeyValues {
			keyStorable, valueStorable, err := parentMap.Remove(compare, hashInputProvider, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			delete(expectedKeyValues, k)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const mapSize = 2

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[Value]Value)

		for i := 0; i < mapSize; i++ {
			// Create child map
			childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := Uint64Value(i)

			existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(mapValue)
			expectedKeyValues[k] = expectedChildValues

			// Insert into child map until child map is not inlined
			v := NewStringValue(strings.Repeat("a", 10))

			existingStorable, err = childMap.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child map value
		for k := range expectedKeyValues {
			keyStorable, valueStorable, err := parentMap.Remove(compare, hashInputProvider, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			valueEqual(t, expectedKeyValues[k], child)

			delete(expectedKeyValues, k)

			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})
}

func TestMapWithOutdatedCallback(t *testing.T) {
	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("overwritten child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(mapValue)

		// Create child array
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		k := Uint64Value(0)

		// Insert child array to parent map
		existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childArray)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		v := NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedKeyValues[k] = arrayValue{v}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite child array value from parent
		valueStorable, err := parentMap.Set(compare, hashInputProvider, k, Uint64Value(0))
		require.NoError(t, err)

		id, ok := valueStorable.(SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		valueEqual(t, expectedKeyValues[k], child)

		expectedKeyValues[k] = Uint64Value(0)

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.NotNil(t, childArray.parentUpdater)

		// modify overwritten child array
		err = childArray.Append(Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.Nil(t, childArray.parentUpdater)

		// No-op on parent
		valueEqual(t, expectedKeyValues, parentMap)
	})

	t.Run("removed child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(mapValue)

		// Create child array
		childArray, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		k := Uint64Value(0)

		// Insert child array to parent map
		existingStorable, err := parentMap.Set(compare, hashInputProvider, k, childArray)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		v := NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedKeyValues[k] = arrayValue{v}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child array value from parent
		keyStorable, valueStorable, err := parentMap.Remove(compare, hashInputProvider, k)
		require.NoError(t, err)
		require.Equal(t, keyStorable, k)

		id, ok := valueStorable.(SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		valueEqual(t, expectedKeyValues[k], child)

		delete(expectedKeyValues, k)

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.NotNil(t, childArray.parentUpdater)

		// modify removed child array
		err = childArray.Append(Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.Nil(t, childArray.parentUpdater)

		// No-op on parent
		valueEqual(t, expectedKeyValues, parentMap)
	})
}

func TestMapSetType(t *testing.T) {
	typeInfo := testTypeInfo{42}
	newTypeInfo := testTypeInfo{43}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())
		require.Equal(t, typeInfo, m.Type())
		require.True(t, m.root.IsData())

		seed := m.root.ExtraData().Seed

		err = m.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())
		require.Equal(t, newTypeInfo, m.Type())
		require.Equal(t, seed, m.root.ExtraData().Seed)

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingMapSetType(t, m.SlabID(), storage.baseStorage, newTypeInfo, m.Count(), seed)
	})

	t.Run("data slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		mapSize := 10
		for i := 0; i < mapSize; i++ {
			v := Uint64Value(i)
			existingStorable, err := m.Set(compare, hashInputProvider, v, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.Equal(t, typeInfo, m.Type())
		require.True(t, m.root.IsData())

		seed := m.root.ExtraData().Seed

		err = m.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, m.Type())
		require.Equal(t, uint64(mapSize), m.Count())
		require.Equal(t, seed, m.root.ExtraData().Seed)

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingMapSetType(t, m.SlabID(), storage.baseStorage, newTypeInfo, m.Count(), seed)
	})

	t.Run("metadata slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		mapSize := 10_000
		for i := 0; i < mapSize; i++ {
			v := Uint64Value(i)
			existingStorable, err := m.Set(compare, hashInputProvider, v, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, uint64(mapSize), m.Count())
		require.Equal(t, typeInfo, m.Type())
		require.False(t, m.root.IsData())

		seed := m.root.ExtraData().Seed

		err = m.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, m.Type())
		require.Equal(t, uint64(mapSize), m.Count())
		require.Equal(t, seed, m.root.ExtraData().Seed)

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingMapSetType(t, m.SlabID(), storage.baseStorage, newTypeInfo, m.Count(), seed)
	})

	t.Run("inlined in parent container root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMapSeed := childMap.root.ExtraData().Seed

		existingStorable, err := parentMap.Set(compare, hashInputProvider, Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(1), parentMap.Count())
		require.Equal(t, typeInfo, parentMap.Type())
		require.True(t, parentMap.root.IsData())
		require.False(t, parentMap.Inlined())

		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, typeInfo, childMap.Type())
		require.True(t, childMap.root.IsData())
		require.True(t, childMap.Inlined())

		err = childMap.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childMap.Type())
		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, childMapSeed, childMap.root.ExtraData().Seed)

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedMapSetType(
			t,
			parentMap.SlabID(),
			Uint64Value(0),
			storage.baseStorage,
			newTypeInfo,
			childMap.Count(),
			childMapSeed,
		)
	})

	t.Run("inlined in parent container non-root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMap, err := NewMap(storage, address, newBasicDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMapSeed := childMap.root.ExtraData().Seed

		mapSize := 10_000
		for i := 0; i < mapSize-1; i++ {
			v := Uint64Value(i)
			existingStorable, err := parentMap.Set(compare, hashInputProvider, v, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		existingStorable, err := parentMap.Set(compare, hashInputProvider, Uint64Value(mapSize-1), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(mapSize), parentMap.Count())
		require.Equal(t, typeInfo, parentMap.Type())
		require.False(t, parentMap.root.IsData())
		require.False(t, parentMap.Inlined())

		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, typeInfo, childMap.Type())
		require.True(t, childMap.root.IsData())
		require.True(t, childMap.Inlined())

		err = childMap.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childMap.Type())
		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, childMapSeed, childMap.root.ExtraData().Seed)

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedMapSetType(
			t,
			parentMap.SlabID(),
			Uint64Value(mapSize-1),
			storage.baseStorage,
			newTypeInfo,
			childMap.Count(),
			childMapSeed,
		)
	})
}

func testExistingMapSetType(
	t *testing.T,
	id SlabID,
	baseStorage BaseStorage,
	expectedTypeInfo testTypeInfo,
	expectedCount uint64,
	expectedSeed uint64,
) {
	newTypeInfo := testTypeInfo{value: expectedTypeInfo.value + 1}

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing map by ID
	m, err := NewMapWithRootID(storage, id, newBasicDigesterBuilder())
	require.NoError(t, err)
	require.Equal(t, expectedCount, m.Count())
	require.Equal(t, expectedTypeInfo, m.Type())
	require.Equal(t, expectedSeed, m.root.ExtraData().Seed)

	// Modify type info of existing map
	err = m.SetType(newTypeInfo)
	require.NoError(t, err)
	require.Equal(t, expectedCount, m.Count())
	require.Equal(t, newTypeInfo, m.Type())
	require.Equal(t, expectedSeed, m.root.ExtraData().Seed)

	// Commit data in storage
	err = storage.FastCommit(runtime.NumCPU())
	require.NoError(t, err)

	// Create storage from existing data
	storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

	// Load existing map again from storage
	m2, err := NewMapWithRootID(storage2, id, newBasicDigesterBuilder())
	require.NoError(t, err)
	require.Equal(t, expectedCount, m2.Count())
	require.Equal(t, newTypeInfo, m2.Type())
	require.Equal(t, expectedSeed, m2.root.ExtraData().Seed)
}

func testExistingInlinedMapSetType(
	t *testing.T,
	parentID SlabID,
	inlinedChildKey Value,
	baseStorage BaseStorage,
	expectedTypeInfo testTypeInfo,
	expectedCount uint64,
	expectedSeed uint64,
) {
	newTypeInfo := testTypeInfo{value: expectedTypeInfo.value + 1}

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing map by ID
	parentMap, err := NewMapWithRootID(storage, parentID, newBasicDigesterBuilder())
	require.NoError(t, err)

	element, err := parentMap.Get(compare, hashInputProvider, inlinedChildKey)
	require.NoError(t, err)

	childMap, ok := element.(*OrderedMap)
	require.True(t, ok)

	require.Equal(t, expectedCount, childMap.Count())
	require.Equal(t, expectedTypeInfo, childMap.Type())
	require.Equal(t, expectedSeed, childMap.root.ExtraData().Seed)

	// Modify type info of existing map
	err = childMap.SetType(newTypeInfo)
	require.NoError(t, err)
	require.Equal(t, expectedCount, childMap.Count())
	require.Equal(t, newTypeInfo, childMap.Type())
	require.Equal(t, expectedSeed, childMap.root.ExtraData().Seed)

	// Commit data in storage
	err = storage.FastCommit(runtime.NumCPU())
	require.NoError(t, err)

	// Create storage from existing data
	storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

	// Load existing map again from storage
	parentMap2, err := NewMapWithRootID(storage2, parentID, newBasicDigesterBuilder())
	require.NoError(t, err)

	element2, err := parentMap2.Get(compare, hashInputProvider, inlinedChildKey)
	require.NoError(t, err)

	childMap2, ok := element2.(*OrderedMap)
	require.True(t, ok)

	require.Equal(t, expectedCount, childMap2.Count())
	require.Equal(t, newTypeInfo, childMap2.Type())
	require.Equal(t, expectedSeed, childMap.root.ExtraData().Seed)
}
