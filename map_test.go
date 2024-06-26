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

func verifyEmptyMap(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	m *OrderedMap,
) {
	verifyMap(t, storage, typeInfo, address, m, nil, nil, false)
}

// verifyMap verifies map elements and validates serialization and in-memory slab tree.
// It also verifies elements ordering if sortedKeys is not nil.
func verifyMap(
	t *testing.T,
	storage *PersistentSlabStorage,
	typeInfo TypeInfo,
	address Address,
	m *OrderedMap,
	keyValues map[Value]Value,
	sortedKeys []Value,
	hasNestedArrayMapElement bool,
) {
	require.True(t, typeInfoComparator(typeInfo, m.Type()))
	require.Equal(t, address, m.Address())
	require.Equal(t, uint64(len(keyValues)), m.Count())

	var err error

	// Verify map elements
	for k, v := range keyValues {
		e, err := m.Get(compare, hashInputProvider, k)
		require.NoError(t, err)

		valueEqual(t, typeInfoComparator, v, e)
	}

	// Verify map elements ordering
	if len(sortedKeys) > 0 {
		require.Equal(t, len(keyValues), len(sortedKeys))

		i := 0
		err = m.Iterate(func(k, v Value) (bool, error) {
			expectedKey := sortedKeys[i]
			expectedValue := keyValues[expectedKey]

			valueEqual(t, typeInfoComparator, expectedKey, k)
			valueEqual(t, typeInfoComparator, expectedValue, v)

			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, len(keyValues), i)
	}

	// Verify in-memory slabs
	err = ValidMap(m, typeInfo, typeInfoComparator, hashInputProvider)
	if err != nil {
		PrintMap(m)
	}
	require.NoError(t, err)

	// Verify slab serializations
	err = ValidMapSerialization(
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

	if !hasNestedArrayMapElement {
		// Need to call Commit before calling storage.Count() for PersistentSlabStorage.
		err = storage.Commit()
		require.NoError(t, err)

		stats, err := GetMapStats(m)
		require.NoError(t, err)
		require.Equal(t, stats.SlabCount(), uint64(storage.Count()))

		if len(keyValues) == 0 {
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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
			valueEqual(t, typeInfoComparator, oldValue, existingValue)

			keyValues[k] = newValue
		}

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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
			valueEqual(t, typeInfoComparator, oldValue, existingValue)

			keyValues[k] = newValue
		}

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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
	valueEqual(t, typeInfoComparator, k, removedKey)

	removedValue, err := removedValueStorable.StoredValue(m.Storage)
	require.NoError(t, err)
	valueEqual(t, typeInfoComparator, expectedV, removedValue)

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

			verifyMap(t, storage, typeInfo, address, m, tc.keyValues, nil, false)

			count := len(tc.keyValues)

			// Remove all elements
			for k, v := range tc.keyValues {

				testMapRemoveElement(t, m, k, v)

				count--

				require.True(t, typeInfoComparator(typeInfo, m.Type()))
				require.Equal(t, address, m.Address())
				require.Equal(t, uint64(count), m.Count())
			}

			verifyEmptyMap(t, storage, typeInfo, address, m)
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

		verifyMap(t, storage, typeInfo, address, m, nonCollisionKeyValues, nil, false)

		// Remove remaining elements
		for k, v := range nonCollisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, typeInfoComparator(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, uint64(count), m.Count())
		}

		verifyEmptyMap(t, storage, typeInfo, address, m)
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

		verifyMap(t, storage, typeInfo, address, m, nonCollisionKeyValues, nil, false)

		// Remove remaining elements
		for k, v := range nonCollisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, typeInfoComparator(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, uint64(count), m.Count())
		}

		verifyEmptyMap(t, storage, typeInfo, address, m)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapIterate(t *testing.T) {

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
		err = m.Iterate(func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, typeInfoComparator, sortedKeys[i], k)
			valueEqual(t, typeInfoComparator, keyValues[k], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		// Iterate keys
		i = uint64(0)
		err = m.IterateKeys(func(k Value) (resume bool, err error) {
			valueEqual(t, typeInfoComparator, sortedKeys[i], k)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		// Iterate values
		i = uint64(0)
		err = m.IterateValues(func(v Value) (resume bool, err error) {
			k := sortedKeys[i]
			valueEqual(t, typeInfoComparator, keyValues[k], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapSize), i)

		verifyMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
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

		t.Log("created map of unique key value pairs")

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		t.Log("sorted keys by digests")

		// Iterate key value pairs
		i := uint64(0)
		err = m.Iterate(func(k Value, v Value) (resume bool, err error) {
			valueEqual(t, typeInfoComparator, sortedKeys[i], k)
			valueEqual(t, typeInfoComparator, keyValues[k], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		t.Log("iterated key value pairs")

		// Iterate keys
		i = uint64(0)
		err = m.IterateKeys(func(k Value) (resume bool, err error) {
			valueEqual(t, typeInfoComparator, sortedKeys[i], k)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		t.Log("iterated keys")

		// Iterate values
		i = uint64(0)
		err = m.IterateValues(func(v Value) (resume bool, err error) {
			valueEqual(t, typeInfoComparator, keyValues[sortedKeys[i]], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		t.Log("iterated values")

		verifyMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
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

	verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	stats, err := GetMapStats(m)
	require.NoError(t, err)
	require.Equal(t, uint64(mockDigestCount), stats.CollisionDataSlabCount)

	// Remove all elements
	for k, v := range keyValues {
		removedKeyStorable, removedValueStorable, err := m.Remove(compare, hashInputProvider, k)
		require.NoError(t, err)

		removedKey, err := removedKeyStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, typeInfoComparator, k, removedKey)

		removedValue, err := removedValueStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, typeInfoComparator, v, removedValue)

		if id, ok := removedKeyStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		if id, ok := removedValueStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}
	}

	verifyEmptyMap(t, storage, typeInfo, address, m)
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

	verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	// Remove all elements
	for k, v := range keyValues {
		removedKeyStorable, removedValueStorable, err := m.Remove(compare, hashInputProvider, k)
		require.NoError(t, err)

		removedKey, err := removedKeyStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, typeInfoComparator, k, removedKey)

		removedValue, err := removedValueStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, typeInfoComparator, v, removedValue)

		if id, ok := removedKeyStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}

		if id, ok := removedValueStorable.(SlabIDStorable); ok {
			err = storage.Remove(SlabID(id))
			require.NoError(t, err)
		}
	}

	verifyEmptyMap(t, storage, typeInfo, address, m)
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
				valueEqual(t, typeInfoComparator, oldv, existingValue)

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
			valueEqual(t, typeInfoComparator, k, removedKey)

			removedValue, err := removedValueStorable.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, keyValues[k], removedValue)

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

	verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
}

func TestMapDecodeV0(t *testing.T) {

	typeInfo := testTypeInfo{42}
	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {

		mapSlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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

		verifyEmptyMap(t, storage, typeInfo, address, decodedMap)
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

		mapSlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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

		verifyMap(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
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

		mapSlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		nestedSlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 4}}

		nested, err := NewArray(storage, address, typeInfo2)
		nested.root.SetSlabID(nestedSlabID)
		require.NoError(t, err)

		err = nested.Append(Uint64Value(0))
		require.NoError(t, err)

		k := NewStringValue(strings.Repeat(string(r), 22))
		v := nested
		keyValues[k] = v

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

		verifyMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
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

		mapSlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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

		verifyMap(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
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

		mapSlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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

		verifyMap(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
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

		mapSlabID := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 3}}

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

		verifyMap(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
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

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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

		verifyEmptyMap(t, storage2, typeInfo, address, decodedMap)
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

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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

		verifyMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("has pointer no collision", func(t *testing.T) {

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

		// Create nested array
		typeInfo2 := testTypeInfo{43}

		nested, err := NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		err = nested.Append(Uint64Value(0))
		require.NoError(t, err)

		k := NewStringValue(strings.Repeat(string(r), 22))
		v := nested
		keyValues[k] = v

		digests := []Digest{Digest(mapSize - 1), Digest((mapSize - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert nested array
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(mapSize), m.Count())

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 3}}
		id4 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 4}}

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
				0x99, 0x00, 0x01,
				// CBOR encoded array elements
				0xd8, 0xa4, 0x00,
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
		// Need to add slabIDSize to encoded data slab here because empty slab ID is omitted during encoding.
		require.Equal(t, uint32(len(stored[id3])+slabIDSize), meta.childrenHeaders[1].size)

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		verifyMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
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

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

		// Expected serialized slab data with slab id
		expected := map[SlabID][]byte{

			// map metadata slab
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

		verifyMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
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

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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

		verifyMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
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

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}
		id2 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 2}}
		id3 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 3}}

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

		verifyMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer", func(t *testing.T) {
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

		id1 := SlabID{address: address, index: SlabIndex{0, 0, 0, 0, 0, 0, 0, 1}}

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
		vs := NewStringValue(strings.Repeat("a", 512))
		existingStorable, err = m.Set(compare, hashInputProvider, k, vs)
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		valueEqual(t, typeInfoComparator, v, existingValue)

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

		stored, err = storage.Encode()
		require.NoError(t, err)
		require.Equal(t, 2, len(stored))
		require.Equal(t, expectedHasPointer, stored[id1])
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

	verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	// Create a new storage with encoded data from base storage
	storage2 := newTestPersistentStorageWithBaseStorage(t, storage.baseStorage)

	// Create new map from new storage
	m2, err := NewMapWithRootID(storage2, m.SlabID(), m.digesterBuilder)
	require.NoError(t, err)

	verifyMap(t, storage2, typeInfo, address, m2, keyValues, nil, false)
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

			verifyMap(t, storage, typeInfo, address, m2, keyValues, nil, false)
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
		digesterBuilder := newBasicDigesterBuilder()

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
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

		verifyEmptyMap(t, storage, typeInfo, address, m)
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
			valueEqual(t, typeInfoComparator, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		verifyEmptyMap(t, storage, typeInfo, address, m)
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
			valueEqual(t, typeInfoComparator, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		verifyEmptyMap(t, storage, typeInfo, address, m)
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
			valueEqual(t, typeInfoComparator, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			valueEqual(t, typeInfoComparator, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		verifyEmptyMap(t, storage, typeInfo, address, m)
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
		existingKey, existingValue, err := m.Remove(compare, hashInputProvider, Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *UserError
		var keyNotFoundError *KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)
		require.Nil(t, existingKey)
		require.Nil(t, existingValue)
	})

	t.Run("iterate", func(t *testing.T) {
		i := 0
		err := m.Iterate(func(k Value, v Value) (bool, error) {
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

		iter, err := m.Iterator()
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

		verifyEmptyMap(t, storage, typeInfo, address, copied)
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

		iter, err := m.Iterator()
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

		verifyMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
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

		iter, err := m.Iterator()
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

		verifyMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
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

		iter, err := m.Iterator()
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

		verifyMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
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

		iter, err := m.Iterator()
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

		verifyMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
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

		iter, err := m.Iterator()
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

		verifyMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
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

		iter, err := m.Iterator()
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

		verifyMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
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

		iter, err := m.Iterator()
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

		verifyMap(t, storage, typeInfo, address, copied, keyValues, sortedKeys, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, true)
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

			// Create a nested array with one element
			array, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			vs := strings.Repeat("b", int(i))
			v := SomeValue{Value: NewStringValue(vs)}

			err = array.Append(v)
			require.NoError(t, err)

			// Insert nested array into map
			ks := strings.Repeat("a", int(i))
			k := SomeValue{Value: NewStringValue(ks)}

			keyValues[k] = array

			existingStorable, err := m.Set(compare, hashInputProvider, k, array)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, true)
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
	require.Equal(t, targetThreshold-slabIDSize, uint64(m.root.Header().size))

	verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)

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
		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Update elements within collision limits
		for k := range keyValues {
			v := Uint64Value(0)
			keyValues[k] = v
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
		}

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)

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
		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Update elements within collision limits
		for k := range keyValues {
			v := Uint64Value(0)
			keyValues[k] = v
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
		}

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMapLoadedElements(t, m, nil)
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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values in collision group", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values in external collision group", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision group, 4 elements in the group.
		const mapSize = 12
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from front to back.
		for i := 0; i < len(values); i++ {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[i+1:]
			verifyMapLoadedElements(t, m, expectedValues)
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

		verifyMapLoadedElements(t, m, values)

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
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in collision group, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from front to back.
		for i := 0; i < len(values); i++ {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[i+1:]
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from front to back
		for i := 0; i < len(values); i++ {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[i+1:]
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload external slab from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

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
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values, unload composite value from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from back to front.
		for i := len(values) - 1; i >= 0; i-- {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[:i]
			verifyMapLoadedElements(t, m, expectedValues)
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

		verifyMapLoadedElements(t, m, values)

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
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in collision group, unload value from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[:i]
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload value from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[:i]
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values in external collision group, unload external slab from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

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
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root data slab with composite values, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload value in the middle
		unloadValueIndex := 1

		v := values[unloadValueIndex][1]

		nestedArray, ok := v.(*Array)
		require.True(t, ok)

		err := storage.Remove(nestedArray.SlabID())
		require.NoError(t, err)

		copy(values[unloadValueIndex:], values[unloadValueIndex+1:])
		values = values[:len(values)-1]

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with long string key, unload key in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithLongStringKey(t, storage, address, typeInfo, mapSize)

		// parent map: 1 root data slab
		// nested composite elements: 1 root data slab for each
		require.Equal(t, 1+mapSize, len(storage.deltas))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		verifyMapLoadedElements(t, m, values)

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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values in collision group, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 collision groups, 2 elements in each group.
		const mapSize = 6
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element in the middle
		for _, unloadValueIndex := range []int{1, 3, 5} {
			v := values[unloadValueIndex][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)
		}

		expectedValues := [][2]Value{
			values[0],
			values[2],
			values[4],
		}
		verifyMapLoadedElements(t, m, expectedValues)
	})

	t.Run("root data slab with composite values in external collision group, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite value in the middle.
		for _, unloadValueIndex := range []int{1, 3, 5, 7, 9, 11} {
			v := values[unloadValueIndex][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
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
		verifyMapLoadedElements(t, m, expectedValues)
	})

	t.Run("root data slab with composite values in external collision group, unload external slab in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		// Create parent map with 3 external collision groups, 4 elements in the group.
		const mapSize = 12
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root data slab with composite values, unload composite elements during iteration", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 3
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		i := 0
		err := m.IterateLoadedValues(func(k Value, v Value) (bool, error) {
			// At this point, iterator returned first element (v).

			// Remove all other nested composite elements (except first element) from storage.
			for _, element := range values[1:] {
				value := element[1]
				nestedArray, ok := value.(*Array)
				require.True(t, ok)

				err := storage.Remove(nestedArray.SlabID())
				require.NoError(t, err)
			}

			require.Equal(t, 0, i)
			valueEqual(t, typeInfoComparator, values[0][0], k)
			valueEqual(t, typeInfoComparator, values[0][1], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 1, i) // Only first element is iterated because other elements are remove during iteration.
	})

	t.Run("root data slab with simple and composite values, unloading composite value", func(t *testing.T) {
		const mapSize = 3

		// Create a map with nested composite value at specified index
		for nestedCompositeIndex := 0; nestedCompositeIndex < mapSize; nestedCompositeIndex++ {
			storage := newTestPersistentStorage(t)

			m, values := createMapWithSimpleAndCompositeValues(
				t,
				storage,
				address,
				typeInfo,
				mapSize,
				nestedCompositeIndex,
				func(i int) []Digest { return []Digest{Digest(i)} },
			)

			// parent map: 1 root data slab
			// composite element: 1 root data slab
			require.Equal(t, 2, len(storage.deltas))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			verifyMapLoadedElements(t, m, values)

			// Unload composite value
			v := values[nestedCompositeIndex][1].(*Array)

			err := storage.Remove(v.SlabID())
			require.NoError(t, err)

			copy(values[nestedCompositeIndex:], values[nestedCompositeIndex+1:])
			values = values[:len(values)-1]

			verifyMapLoadedElements(t, m, values)
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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root metadata slab with composite values", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)
	})

	t.Run("root metadata slab with composite values, unload value from front to back", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from front to back
		for i := 0; i < len(values); i++ {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[i+1:]
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root metadata slab with composite values, unload values from back to front", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element from back to front
		for i := len(values) - 1; i >= 0; i-- {
			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			expectedValues := values[:i]
			verifyMapLoadedElements(t, m, expectedValues)
		}
	})

	t.Run("root metadata slab with composite values, unload value in the middle", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		const mapSize = 20
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		// Unload composite element in the middle
		for _, index := range []int{4, 14} {

			v := values[index][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			copy(values[index:], values[index+1:])
			values = values[:len(values)-1]

			verifyMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab with simple and composite values, unload composite value", func(t *testing.T) {
		const mapSize = 20

		// Create a map with nested composite value at specified index
		for nestedCompositeIndex := 0; nestedCompositeIndex < mapSize; nestedCompositeIndex++ {
			storage := newTestPersistentStorage(t)

			m, values := createMapWithSimpleAndCompositeValues(
				t,
				storage,
				address,
				typeInfo,
				mapSize,
				nestedCompositeIndex,
				func(i int) []Digest { return []Digest{Digest(i)} },
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			// composite values: 1 root data slab for each
			require.Equal(t, 5, len(storage.deltas))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			verifyMapLoadedElements(t, m, values)

			v := values[nestedCompositeIndex][1].(*Array)

			err := storage.Remove(v.SlabID())
			require.NoError(t, err)

			copy(values[nestedCompositeIndex:], values[nestedCompositeIndex+1:])
			values = values[:len(values)-1]

			verifyMapLoadedElements(t, m, values)
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

		verifyMapLoadedElements(t, m, values)

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

			verifyMapLoadedElements(t, m, values)
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

		verifyMapLoadedElements(t, m, values)

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

			verifyMapLoadedElements(t, m, values)
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

		verifyMapLoadedElements(t, m, values)

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

		verifyMapLoadedElements(t, m, values)
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

			verifyMapLoadedElements(t, m, expectedValues)
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

			verifyMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab with composite values, unload composite value at random index", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const mapSize = 500
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

		r := newRand(t)

		// Unload composite element in random position
		for len(values) > 0 {

			i := r.Intn(len(values))

			v := values[i][1]

			nestedArray, ok := v.(*Array)
			require.True(t, ok)

			err := storage.Remove(nestedArray.SlabID())
			require.NoError(t, err)

			copy(values[i:], values[i+1:])
			values = values[:len(values)-1]

			verifyMapLoadedElements(t, m, values)
		}
	})

	t.Run("root metadata slab with composite values, unload random data slab", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const mapSize = 500
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

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

			verifyMapLoadedElements(t, m, values)
		}

		require.Equal(t, 0, len(values))
	})

	t.Run("root metadata slab with composite values, unload random slab", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		const mapSize = 500
		m, values := createMapWithCompositeValues(
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

		verifyMapLoadedElements(t, m, values)

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

			verifyMapLoadedElements(t, m, values)
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

func createMapWithCompositeValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	size int,
	newDigests func(i int) []Digest,
) (*OrderedMap, [][2]Value) {

	// Use mockDigesterBuilder to guarantee element order.
	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map
	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	expectedValues := make([][2]Value, size)
	for i := 0; i < size; i++ {
		// Create nested array
		nested, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		err = nested.Append(Uint64Value(i))
		require.NoError(t, err)

		k := Uint64Value(i)
		v := nested

		expectedValues[i] = [2]Value{k, v}

		//digests := []Digest{Digest(i)}
		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		// Set nested array to parent
		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	return m, expectedValues
}

func createMapWithSimpleAndCompositeValues(
	t *testing.T,
	storage SlabStorage,
	address Address,
	typeInfo TypeInfo,
	size int,
	compositeValueIndex int,
	newDigests func(i int) []Digest,
) (*OrderedMap, [][2]Value) {

	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map
	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	values := make([][2]Value, size)
	r := 'a'
	for i := 0; i < size; i++ {

		k := Uint64Value(i)

		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		if compositeValueIndex == i {
			// Create nested array with one element
			a, err := NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			err = a.Append(Uint64Value(i))
			require.NoError(t, err)

			values[i] = [2]Value{k, a}
		} else {
			values[i] = [2]Value{k, NewStringValue(strings.Repeat(string(r), 18))}
		}

		existingStorable, err := m.Set(compare, hashInputProvider, values[i][0], values[i][1])
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	return m, values
}

func verifyMapLoadedElements(t *testing.T, m *OrderedMap, expectedValues [][2]Value) {
	i := 0
	err := m.IterateLoadedValues(func(k Value, v Value) (bool, error) {
		require.True(t, i < len(expectedValues))
		valueEqual(t, typeInfoComparator, expectedValues[i][0], k)
		valueEqual(t, typeInfoComparator, expectedValues[i][1], v)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

		verifyMap(t, storage, typeInfo, address, m, keyValues, nil, false)
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

	require.Equal(t, sid.address[:], id[:8])
	require.Equal(t, sid.index[:], id[8:])
}

func TestSlabSizeWhenResettingMutableStorableInMap(t *testing.T) {
	const (
		mapSize             = 3
		keyStringSize       = 16
		initialStorableSize = 1
		mutatedStorableSize = 5
	)

	keyValues := make(map[Value]*mutableValue, mapSize)
	for i := 0; i < mapSize; i++ {
		k := Uint64Value(i)
		v := newMutableValue(initialStorableSize)
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

	err = ValidMap(m, typeInfo, typeInfoComparator, hashInputProvider)
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

	err = ValidMap(m, typeInfo, typeInfoComparator, hashInputProvider)
	require.NoError(t, err)
}
