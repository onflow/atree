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

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
)

type mockDigesterBuilder struct {
	mock.Mock
}

var _ atree.DigesterBuilder = &mockDigesterBuilder{}

type mockDigester struct {
	d []atree.Digest
}

var _ atree.Digester = &mockDigester{}

func (h *mockDigesterBuilder) SetSeed(_ uint64, _ uint64) {
}

func (h *mockDigesterBuilder) Digest(_ atree.HashInputProvider, value atree.Value) (atree.Digester, error) {
	args := h.Called(value)
	return args.Get(0).(mockDigester), nil
}

func (d mockDigester) DigestPrefix(level uint) ([]atree.Digest, error) {
	if level > uint(len(d.d)) {
		return nil, fmt.Errorf("digest level %d out of bounds", level)
	}
	return d.d[:level], nil
}

func (d mockDigester) Digest(level uint) (atree.Digest, error) {
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

var _ atree.DigesterBuilder = &errorDigesterBuilder{}

func newErrorDigesterBuilder(err error) *errorDigesterBuilder {
	return &errorDigesterBuilder{err: err}
}

func (h *errorDigesterBuilder) SetSeed(_ uint64, _ uint64) {
}

func (h *errorDigesterBuilder) Digest(_ atree.HashInputProvider, _ atree.Value) (atree.Digester, error) {
	return nil, h.err
}

func testEmptyMapV0(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	m *atree.OrderedMap,
) {
	testMapV0(t, storage, typeInfo, address, m, nil, nil, false)
}

func testEmptyMap(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	m *atree.OrderedMap,
) {
	testMap(t, storage, typeInfo, address, m, nil, nil, false)
}

func testMapV0(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	m *atree.OrderedMap,
	expectedValues test_utils.ExpectedMapValue,
	sortedKeys []atree.Value,
	hasNestedArrayMapElement bool,
) {
	_testMap(t, storage, typeInfo, address, m, expectedValues, sortedKeys, hasNestedArrayMapElement, false)
}

func testMap(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	m *atree.OrderedMap,
	expectedValues test_utils.ExpectedMapValue,
	sortedKeys []atree.Value,
	hasNestedArrayMapElement bool,
) {
	_testMap(t, storage, typeInfo, address, m, expectedValues, sortedKeys, hasNestedArrayMapElement, true)
}

// _testMap verifies map elements and validates serialization and in-memory slab tree.
// It also verifies elements ordering if sortedKeys is not nil.
func _testMap(
	t *testing.T,
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
	m *atree.OrderedMap,
	expectedValues test_utils.ExpectedMapValue,
	sortedKeys []atree.Value,
	hasNestedArrayMapElement bool,
	inlineEnabled bool,
) {
	require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
	require.Equal(t, address, m.Address())
	require.Equal(t, uint64(len(expectedValues)), m.Count())

	var err error

	// Verify map elements
	for k, expected := range expectedValues {
		actual, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.NoError(t, err)

		testValueEqual(t, expected, actual)
	}

	// Verify map elements ordering
	if len(sortedKeys) > 0 {
		require.Equal(t, len(expectedValues), len(sortedKeys))

		i := 0
		err = m.IterateReadOnly(func(k, v atree.Value) (bool, error) {
			expectedKey := sortedKeys[i]
			expectedValue := expectedValues[expectedKey]

			testValueEqual(t, expectedKey, k)
			testValueEqual(t, expectedValue, v)

			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, len(expectedValues), i)
	}

	// Verify in-memory slabs
	err = atree.VerifyMap(m, address, typeInfo, test_utils.CompareTypeInfo, test_utils.GetHashInput, inlineEnabled)
	if err != nil {
		atree.PrintMap(m)
	}
	require.NoError(t, err)

	// Verify slab serializations
	err = atree.VerifyMapSerialization(
		m,
		atree.GetCBORDecMode(storage),
		atree.GetCBOREncMode(storage),
		storage.DecodeStorable,
		storage.DecodeTypeInfo,
		func(a, b atree.Storable) bool {
			return reflect.DeepEqual(a, b)
		},
	)
	if err != nil {
		atree.PrintMap(m)
	}
	require.NoError(t, err)

	// Check storage slab tree
	rootIDSet, err := atree.CheckStorageHealth(storage, 1)
	require.NoError(t, err)

	rootIDs := make([]atree.SlabID, 0, len(rootIDSet))
	for id := range rootIDSet {
		rootIDs = append(rootIDs, id)
	}
	require.Equal(t, 1, len(rootIDs))
	require.Equal(t, m.SlabID(), rootIDs[0])

	// Encode all non-nil slab
	encodedSlabs := make(map[atree.SlabID][]byte)
	deltas := atree.GetDeltas(storage)
	for id, slab := range deltas {
		if slab != nil {
			b, err := atree.EncodeSlab(slab, atree.GetCBOREncMode(storage))
			require.NoError(t, err)
			encodedSlabs[id] = b
		}
	}

	// Test decoded map from new storage to force slab decoding
	decodedMap, err := atree.NewMapWithRootID(
		newTestPersistentStorageWithBaseStorageAndDeltas(t, atree.GetBaseStorage(storage), encodedSlabs),
		m.SlabID(),
		atree.GetMapDigesterBuilder(m))
	require.NoError(t, err)

	// Verify decoded map elements
	for k, expected := range expectedValues {
		actual, err := decodedMap.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.NoError(t, err)

		testValueEqual(t, expected, actual)
	}

	if !hasNestedArrayMapElement {
		// Need to call Commit before calling storage.Count() for atree.PersistentSlabStorage.
		err = storage.Commit()
		require.NoError(t, err)

		stats, err := atree.GetMapStats(m)
		require.NoError(t, err)
		require.Equal(t, stats.SlabCount(), uint64(storage.Count())) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		if len(expectedValues) == 0 {
			// Verify slab count for empty map
			require.Equal(t, uint64(1), stats.DataSlabCount)
			require.Equal(t, uint64(0), stats.MetaDataSlabCount)
			require.Equal(t, uint64(0), stats.StorableSlabCount)
			require.Equal(t, uint64(0), stats.CollisionDataSlabCount)
		}
	}
}

type keysByDigest struct {
	keys            []atree.Value
	digesterBuilder atree.DigesterBuilder
}

func (d keysByDigest) Len() int { return len(d.keys) }

func (d keysByDigest) Swap(i, j int) { d.keys[i], d.keys[j] = d.keys[j], d.keys[i] }

func (d keysByDigest) Less(i, j int) bool {
	d1, err := d.digesterBuilder.Digest(test_utils.GetHashInput, d.keys[i])
	if err != nil {
		panic(err)
	}

	digest1, err := d1.DigestPrefix(d1.Levels())
	if err != nil {
		panic(err)
	}

	d2, err := d.digesterBuilder.Digest(test_utils.GetHashInput, d.keys[j])
	if err != nil {
		panic(err)
	}

	digest2, err := d2.DigestPrefix(d2.Levels())
	if err != nil {
		panic(err)
	}

	for z := range digest1 {
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

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			mapCount      = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		i := uint64(0)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.Uint64Value(i)
			keyValues[k] = v
			i++
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("replicate keys", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			mapCount      = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		i := uint64(0)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.Uint64Value(i)
			keyValues[k] = v
			i++
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Overwrite values
		for k, v := range keyValues {
			oldValue := v.(test_utils.Uint64Value)
			newValue := test_utils.Uint64Value(uint64(oldValue) + mapCount)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, oldValue, existingValue)

			keyValues[k] = newValue
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("random key and value", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const (
			mapCount         = 2048
			keyStringMaxSize = 1024
		)

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for len(keyValues) < mapCount {
			slen := r.Intn(keyStringMaxSize)
			k := test_utils.NewStringValue(randStr(r, slen))
			v := randomValue(r, atree.MaxInlineMapElementSize())
			keyValues[k] = v
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("unique keys with hash collision", func(t *testing.T) {

		const (
			mapCount      = 1024
			keyStringSize = 16
		)

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		savedMaxCollisionLimitPerDigest := atree.MaxCollisionLimitPerDigest
		atree.MaxCollisionLimitPerDigest = uint32(math.Ceil(float64(mapCount) / 10))
		defer func() {
			atree.MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
		}()

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		i := uint64(0)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.Uint64Value(i)
			keyValues[k] = v
			i++

			digests := []atree.Digest{
				atree.Digest(i % 10),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("replicate keys with hash collision", func(t *testing.T) {
		const (
			mapCount      = 1024
			keyStringSize = 16
		)

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		savedMaxCollisionLimitPerDigest := atree.MaxCollisionLimitPerDigest
		atree.MaxCollisionLimitPerDigest = uint32(math.Ceil(float64(mapCount) / 10))
		defer func() {
			atree.MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
		}()

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		i := uint64(0)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.Uint64Value(i)
			keyValues[k] = v
			i++

			digests := []atree.Digest{
				atree.Digest(i % 10),
			}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Overwrite values
		for k, v := range keyValues {
			oldValue := v.(test_utils.Uint64Value)
			newValue := test_utils.Uint64Value(uint64(oldValue) + mapCount)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, oldValue, existingValue)

			keyValues[k] = newValue
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapGetKeyNotFound(t *testing.T) {
	t.Run("no collision", func(t *testing.T) {
		const mapCount = uint64(1024)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			keyValues[k] = v
			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)

		k := test_utils.NewStringValue(randStr(r, 1024))
		storable, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var keyNotFoundError *atree.KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision", func(t *testing.T) {
		const mapCount = uint64(256)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := &mockDigesterBuilder{}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)
		k := test_utils.NewStringValue(randStr(r, 1024))

		digests := []atree.Digest{atree.Digest(0)}
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		storable, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var keyNotFoundError *atree.KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision group", func(t *testing.T) {
		const mapCount = uint64(256)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := &mockDigesterBuilder{}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i % 10)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)
		k := test_utils.NewStringValue(randStr(r, 1024))

		digests := []atree.Digest{atree.Digest(0)}
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		storable, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.Nil(t, storable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var keyNotFoundError *atree.KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapHas(t *testing.T) {

	t.Run("no error", func(t *testing.T) {
		const (
			mapCount      = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keys := make(map[atree.Value]bool, mapCount*2)
		keysToInsert := make([]atree.Value, 0, mapCount)
		keysToNotInsert := make([]atree.Value, 0, mapCount)
		for len(keysToInsert) < mapCount || len(keysToNotInsert) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			if !keys[k] {
				keys[k] = true

				if len(keysToInsert) < mapCount {
					keysToInsert = append(keysToInsert, k)
				} else {
					keysToNotInsert = append(keysToNotInsert, k)
				}
			}
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for i, k := range keysToInsert {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.Uint64Value(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for _, k := range keysToInsert {
			exist, err := m.Has(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)
			require.True(t, exist)
		}

		for _, k := range keysToNotInsert {
			exist, err := m.Has(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)
			require.False(t, exist)
		}
	})

	t.Run("error", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		testErr := errors.New("test")
		digesterBuilder := newErrorDigesterBuilder(testErr)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		exist, err := m.Has(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		// err is testErr wrapped in atree.ExternalError.
		require.Equal(t, 1, errorCategorizationCount(err))
		var externalError *atree.ExternalError
		require.ErrorAs(t, err, &externalError)
		require.Equal(t, testErr, externalError.Unwrap())
		require.False(t, exist)
	})
}

func testMapRemoveElement(t *testing.T, m *atree.OrderedMap, k atree.Value, expectedValue atree.Value) {

	removedKeyStorable, removedValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
	require.NoError(t, err)

	removedKey, err := removedKeyStorable.StoredValue(m.Storage)
	require.NoError(t, err)
	testValueEqual(t, k, removedKey)

	removedValue, err := removedValueStorable.StoredValue(m.Storage)
	require.NoError(t, err)
	testValueEqual(t, expectedValue, removedValue)

	if id, ok := removedKeyStorable.(atree.SlabIDStorable); ok {
		err = m.Storage.Remove(atree.SlabID(id))
		require.NoError(t, err)
	}

	if id, ok := removedValueStorable.(atree.SlabIDStorable); ok {
		err = m.Storage.Remove(atree.SlabID(id))
		require.NoError(t, err)
	}

	// Remove the same key for the second time.
	removedKeyStorable, removedValueStorable, err = m.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
	require.Equal(t, 1, errorCategorizationCount(err))
	var userError *atree.UserError
	var keyNotFoundError *atree.KeyNotFoundError
	require.ErrorAs(t, err, &userError)
	require.ErrorAs(t, err, &keyNotFoundError)
	require.ErrorAs(t, userError, &keyNotFoundError)
	require.Nil(t, removedKeyStorable)
	require.Nil(t, removedValueStorable)
}

func TestMapRemove(t *testing.T) {

	atree.SetThreshold(512)
	defer atree.SetThreshold(1024)

	const (
		mapCount             = 2048
		smallKeyStringSize   = 16
		smallValueStringSize = 16
		largeKeyStringSize   = 512
		largeValueStringSize = 512
	)

	r := newRand(t)

	smallKeyValues := make(map[atree.Value]atree.Value, mapCount)
	for len(smallKeyValues) < mapCount {
		k := test_utils.NewStringValue(randStr(r, smallKeyStringSize))
		v := test_utils.NewStringValue(randStr(r, smallValueStringSize))
		smallKeyValues[k] = v
	}

	largeKeyValues := make(map[atree.Value]atree.Value, mapCount)
	for len(largeKeyValues) < mapCount {
		k := test_utils.NewStringValue(randStr(r, largeKeyStringSize))
		v := test_utils.NewStringValue(randStr(r, largeValueStringSize))
		largeKeyValues[k] = v
	}

	testCases := []struct {
		name      string
		keyValues map[atree.Value]atree.Value
	}{
		{name: "small key and value", keyValues: smallKeyValues},
		{name: "large key and value", keyValues: largeKeyValues},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo := test_utils.NewSimpleTypeInfo(42)
			address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
			storage := newTestPersistentStorage(t)

			m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			// Insert elements
			for k, v := range tc.keyValues {
				existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
			}

			testMap(t, storage, typeInfo, address, m, tc.keyValues, nil, false)

			count := uint64(len(tc.keyValues))

			// Remove all elements
			for k, v := range tc.keyValues {

				testMapRemoveElement(t, m, k, v)

				count--

				require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
				require.Equal(t, address, m.Address())
				require.Equal(t, count, m.Count())
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

		atree.SetThreshold(512)
		defer atree.SetThreshold(1024)

		const (
			numOfElementsBeforeCollision = uint64(54)
			numOfElementsWithCollision   = uint64(10)
			numOfElementsAfterCollision  = uint64(1)
		)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		r := newRand(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		nextDigest := atree.Digest(0)

		nonCollisionKeyValues := make(map[atree.Value]atree.Value)
		for i := range numOfElementsBeforeCollision {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			nonCollisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{nextDigest}})
			nextDigest++

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		collisionKeyValues := make(map[atree.Value]atree.Value)
		for uint64(len(collisionKeyValues)) < numOfElementsWithCollision {
			k := test_utils.NewStringValue(randStr(r, int(atree.MaxInlineMapKeySize())-2)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			v := test_utils.NewStringValue(randStr(r, int(atree.MaxInlineMapKeySize())-2)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			collisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{nextDigest}})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		nextDigest++
		k := test_utils.Uint64Value(nextDigest)
		v := test_utils.Uint64Value(nextDigest)
		nonCollisionKeyValues[k] = v

		digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{nextDigest}})

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		count := uint64(len(nonCollisionKeyValues) + len(collisionKeyValues)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		// Remove all collision elements
		for k, v := range collisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, count, m.Count())
		}

		testMap(t, storage, typeInfo, address, m, nonCollisionKeyValues, nil, false)

		// Remove remaining elements
		for k, v := range nonCollisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, count, m.Count())
		}

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("collision with data root", func(t *testing.T) {
		// Test:
		// - data slab refers to an external slab containing elements with hash collision
		// - last collision element is inlined after all other collision elements are removed
		// - data slab overflows with inlined colllision element
		// - data slab splits

		atree.SetThreshold(512)
		defer atree.SetThreshold(1024)

		const (
			numOfElementsWithCollision    = uint64(10)
			numOfElementsWithoutCollision = uint64(35)
		)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		r := newRand(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		collisionKeyValues := make(map[atree.Value]atree.Value)
		for uint64(len(collisionKeyValues)) < numOfElementsWithCollision {
			k := test_utils.NewStringValue(randStr(r, int(atree.MaxInlineMapKeySize())-2)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			v := test_utils.NewStringValue(randStr(r, int(atree.MaxInlineMapKeySize())-2)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			collisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{0}})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		nonCollisionKeyValues := make(map[atree.Value]atree.Value)
		for i := range numOfElementsWithoutCollision {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			nonCollisionKeyValues[k] = v

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i) + 1}})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		count := uint64(len(nonCollisionKeyValues) + len(collisionKeyValues)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		// Remove all collision elements
		for k, v := range collisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, count, m.Count())
		}

		testMap(t, storage, typeInfo, address, m, nonCollisionKeyValues, nil, false)

		// Remove remaining elements
		for k, v := range nonCollisionKeyValues {

			testMapRemoveElement(t, m, k, v)

			count--

			require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
			require.Equal(t, address, m.Address())
			require.Equal(t, count, m.Count())
		}

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("no collision key not found", func(t *testing.T) {
		const mapCount = uint64(1024)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			keyValues[k] = v
			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)

		k := test_utils.NewStringValue(randStr(r, 1024))
		existingKeyStorable, existingValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.Nil(t, existingKeyStorable)
		require.Nil(t, existingValueStorable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var keyNotFoundError *atree.KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision key not found", func(t *testing.T) {
		const mapCount = uint64(256)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := &mockDigesterBuilder{}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i % 10)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		r := newRand(t)
		k := test_utils.NewStringValue(randStr(r, 1024))

		digests := []atree.Digest{atree.Digest(0)}
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		existingKeyStorable, existingValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.Nil(t, existingKeyStorable)
		require.Nil(t, existingValueStorable)
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var keyNotFoundError *atree.KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestReadOnlyMapIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// Iterate key value pairs
		i := 0
		err = m.IterateReadOnly(func(atree.Value, atree.Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		// Iterate keys
		i = 0
		err = m.IterateReadOnlyKeys(func(atree.Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		// Iterate values
		i = 0
		err = m.IterateReadOnlyValues(func(atree.Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, test_utils.ExpectedMapValue{}, nil, false)
	})

	t.Run("no collision", func(t *testing.T) {
		const (
			mapCount      = 2048
			keyStringSize = 16
		)

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		i := uint64(0)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			if _, found := keyValues[k]; !found {
				keyValues[k] = test_utils.Uint64Value(i)
				sortedKeys[i] = k
				i++
			}
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i = uint64(0)
		err = m.IterateReadOnly(func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapCount), i)

		// Iterate keys
		i = uint64(0)
		err = m.IterateReadOnlyKeys(func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapCount), i)

		// Iterate values
		i = uint64(0)
		err = m.IterateReadOnlyValues(func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]
			testValueEqual(t, keyValues[k], v)
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, uint64(mapCount), i)

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("collision", func(t *testing.T) {
		const (
			mapCount        = 1024
			keyStringSize   = 16
			valueStringSize = 16
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 4
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, 0, mapCount)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))

			if _, found := keyValues[k]; !found {
				v := test_utils.NewStringValue(randStr(r, valueStringSize))
				sortedKeys = append(sortedKeys, k)
				keyValues[k] = v

				digests := newRandomDigests(r, digestLevels)
				digesterBuilder.On("Digest", k).Return(mockDigester{digests})

				existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
			}
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := uint64(0)
		err = m.IterateReadOnly(func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapCount))

		// Iterate keys
		i = uint64(0)
		err = m.IterateReadOnlyKeys(func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapCount))

		// Iterate values
		i = uint64(0)
		err = m.IterateReadOnlyValues(func(v atree.Value) (resume bool, err error) {
			testValueEqual(t, keyValues[sortedKeys[i]], v)
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapCount))

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})
}

func TestMutateElementFromReadOnlyMapIterator(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)
	digesterBuilder := atree.NewDefaultDigesterBuilder()

	var mutationError *atree.ReadOnlyIteratorElementMutationError

	t.Run("mutate inlined map key from IterateReadOnly", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// parent map {{}: 0}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.NewHashableMap(childMapKey), test_utils.Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMapKey.Inlined())

		// Iterate elements and modify key
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k atree.Value, _ atree.Value) (resume bool, err error) {
				c, ok := k.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(k atree.Value) {
				keyMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(k)
				require.NoError(t, err)
				require.Equal(t, childMapKey.ValueID(), valueID)
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map value from IterateReadOnly", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Iterate elements and modify value
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(_ atree.Value, v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(atree.Value) {
				keyMutationCallbackCalled = true
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.Equal(t, childMap.ValueID(), valueID)
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map key from IterateReadOnlyKeys", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// parent map {{}: 0}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.NewHashableMap(childMapKey), test_utils.Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMapKey.Inlined())

		// Iterate and modify key
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v atree.Value) {
				keyMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.Equal(t, childMapKey.ValueID(), valueID)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate inlined map value from IterateReadOnlyValues", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Iterate and modify value
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(1), test_utils.Uint64Value(1))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.Equal(t, childMap.ValueID(), valueID)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key from IterateReadOnly", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// Inserting elements into childMapKey so it can't be inlined
		const mapCount = uint64(20)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			existingStorable, err := childMapKey.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// parent map {{...}: 0}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.NewHashableMap(childMapKey), test_utils.Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.False(t, childMapKey.Inlined())

		// Iterate elements and modify key
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k atree.Value, _ atree.Value) (resume bool, err error) {
				c, ok := k.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(k atree.Value) {
				keyMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(k)
				require.NoError(t, err)
				require.Equal(t, childMapKey.ValueID(), valueID)
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value from IterateReadOnly", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Inserting elements into childMap until it is no longer inlined
		for i := uint64(0); childMap.Inlined(); i++ {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify value
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(_ atree.Value, v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(atree.Value) {
				keyMutationCallbackCalled = true
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.Equal(t, childMap.ValueID(), valueID)
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key from IterateReadOnlyKeys", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map key {}
		childMapKey, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMapKey.Inlined())

		// Inserting elements into childMap so it can't be inlined.
		const mapCount = uint64(20)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			existingStorable, err := childMapKey.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// parent map {{...}: 0}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.NewHashableMap(childMapKey), test_utils.Uint64Value(0))
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.False(t, childMapKey.Inlined())

		// Iterate and modify key
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v atree.Value) {
				keyMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.Equal(t, childMapKey.ValueID(), valueID)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value from IterateReadOnlyValues", func(t *testing.T) {
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// child map {}
		childMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)
		require.False(t, childMap.Inlined())

		// parent map {0: {}}
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
		require.True(t, childMap.Inlined())

		// Inserting elements into childMap until it is no longer inlined
		for i := uint64(0); childMap.Inlined(); i++ {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify value
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				valueID, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.Equal(t, childMap.ValueID(), valueID)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map key in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMapKey2 {}
		childMapKey2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {{}:0, {}:1} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMapKey1, childMapKey2} {
			k := test_utils.NewHashableMap(m)
			v := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because atree.Digest is called again on atree.OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate element and modify key
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k atree.Value, _ atree.Value) (resume bool, err error) {
				c, ok := k.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(k atree.Value) {
				keyMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(k)
				require.NoError(t, err)
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map value in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMap1, childMap2} {
			k := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify values
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(_ atree.Value, v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(1), test_utils.Uint64Value(1))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(atree.Value) {
				keyMutationCallbackCalled = true
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate inlined map key in collision from IterateReadOnlyKeys", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMapKey2 {}
		childMapKey2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {{}: 0, {}: 1} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMapKey1, childMapKey2} {
			k := test_utils.NewHashableMap(m)
			v := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because atree.Digest is called again on atree.OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify keys
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v atree.Value) {
				keyMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate inlined map value in collision from IterateReadOnlyValues", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMap1, childMap2} {
			k := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify values
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.True(t, c.Inlined())

				existingStorable, err := c.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingStorable)

				return true, err
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(20)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMapKey1.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// childMapKey2 {}
		childMapKey2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMapKey2.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMapKey1, childMapKey2} {
			k := test_utils.NewHashableMap(m)
			v := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because atree.Digest is called again on atree.OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify keys
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(k atree.Value, _ atree.Value) (resume bool, err error) {
				c, ok := k.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(k atree.Value) {
				keyMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(k)
				require.NoError(t, err)
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			},
			func(atree.Value) {
				valueMutationCallbackCalled = true
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
		require.False(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value in collision from IterateReadOnly", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMap1, childMap2} {
			k := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := uint64(0); childMap1.Inlined(); i++ {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMap1.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := uint64(0); childMap2.Inlined(); i++ {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMap2.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate elements and modify values
		var keyMutationCallbackCalled, valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyWithMutationCallback(
			func(_ atree.Value, v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(atree.Value) {
				keyMutationCallbackCalled = true
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.False(t, keyMutationCallbackCalled)
		require.True(t, valueMutationCallbackCalled)
	})

	t.Run("mutate not inlined map key in collision from IterateReadOnlyKeys", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMapKey1 {}
		childMapKey1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(20)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMapKey1.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// childMapKey2 {}
		childMapKey2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMapKey2.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMapKey1, childMapKey2} {
			k := test_utils.NewHashableMap(m)
			v := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
			// This is needed because atree.Digest is called again on atree.OrderedMap when inserting collision element.
			digesterBuilder.On("Digest", m).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify keys
		var keyMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyKeysWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v atree.Value) {
				keyMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.True(t, childMapKey1.ValueID() == vid || childMapKey2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, keyMutationCallbackCalled)
	})

	t.Run("mutate not inlined map value in collision from IterateReadOnlyValues", func(t *testing.T) {
		digesterBuilder := &mockDigesterBuilder{}

		// childMap1 {}
		childMap1, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// childMap2 {}
		childMap2, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parentMap {0: {}, 1:{}} with all elements in the same collision group
		for i, m := range []*atree.OrderedMap{childMap1, childMap2} {
			k := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			digests := []atree.Digest{atree.Digest(0)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, m)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := uint64(0); childMap1.Inlined(); i++ {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMap1.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		for i := uint64(0); childMap2.Inlined(); i++ {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := childMap2.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Iterate and modify values
		var valueMutationCallbackCalled bool
		err = parentMap.IterateReadOnlyValuesWithMutationCallback(
			func(v atree.Value) (resume bool, err error) {
				c, ok := v.(*atree.OrderedMap)
				require.True(t, ok)
				require.False(t, c.Inlined())

				existingKeyStorable, existingValueStorable, err := c.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
				require.ErrorAs(t, err, &mutationError)
				require.Nil(t, existingKeyStorable)
				require.Nil(t, existingValueStorable)

				return true, err
			},
			func(v atree.Value) {
				valueMutationCallbackCalled = true
				vid, err := atree.GetMutableValueNotifierValueID(v)
				require.NoError(t, err)
				require.True(t, childMap1.ValueID() == vid || childMap2.ValueID() == vid)
			})

		require.ErrorAs(t, err, &mutationError)
		require.True(t, valueMutationCallbackCalled)
	})
}

func TestMutableMapIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// Iterate key value pairs
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(atree.Value, atree.Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, test_utils.ExpectedMapValue{}, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(15)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) * 2

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(25)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			sortedKeys[i] = k
			keyValues[k] = v
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) * 2

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(15)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(25)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(10)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		r := 'a'
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision primitive values, 1 level", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 1
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) / 2
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision primitive values, 4 levels", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 4
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) / 2
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount = uint64(15)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		const (
			mapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(5)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(35)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(5)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(10)
			mutatedChildMapCount = uint64(1)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j >= mutatedChildMapCount; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision inlined container, 1 level", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 1
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision inlined container, 4 levels", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 4
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container", func(t *testing.T) {
		const (
			mapCount        = uint64(15)
			valueStringSize = 16
		)

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.NewStringValue(randStr(r, valueStringSize))

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)
			sortedKeys[i] = k

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			newChildMapKey := test_utils.Uint64Value(1) // Previous key is 0
			newChildMapValue := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, newChildMapKey, newChildMapValue)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[newChildMapKey] = newChildMapValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Iterate and mutate child map (removing elements)
		i = 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			// Remove key 0
			ck := test_utils.Uint64Value(0)

			existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, ck)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(5)
			mutatedChildMapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(35)
			mutatedChildMapCount = uint64(1)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j > mutatedChildMapCount-1; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(35)
			mutatedChildMapCount = uint64(10)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value, v atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j > mutatedChildMapCount-1; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMutableMapIterateKeys(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(atree.Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, test_utils.ExpectedMapValue{}, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(15)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(test_utils.Uint64Value) * 2

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(25)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			sortedKeys[i] = k
			keyValues[k] = v
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(test_utils.Uint64Value) * 2

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(15)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(25)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(10)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		r := 'a'
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (bool, error) {
			testValueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision primitive values, 1 level", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 1
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(test_utils.Uint64Value) / 2
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision primitive values, 4 levels", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 4
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v := keyValues[k]
			newValue := v.(test_utils.Uint64Value) / 2
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount = uint64(15)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		const (
			mapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(5)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(35)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(5)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(10)
			mutatedChildMapCount = uint64(1)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j >= mutatedChildMapCount; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision inlined container, 1 level", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 1
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision inlined container, 4 levels", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 4
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container", func(t *testing.T) {
		const (
			mapCount        = uint64(15)
			valueStringSize = 16
		)

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.NewStringValue(randStr(r, valueStringSize))

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)
			sortedKeys[i] = k

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			newChildMapKey := test_utils.Uint64Value(1) // Previous key is 0
			newChildMapValue := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, newChildMapKey, newChildMapValue)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[newChildMapKey] = newChildMapValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Iterate and mutate child map (removing elements)
		i = 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			// Remove key 0
			ck := test_utils.Uint64Value(0)

			existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, ck)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(5)
			mutatedChildMapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(35)
			mutatedChildMapCount = uint64(1)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j > mutatedChildMapCount-1; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(35)
			mutatedChildMapCount = uint64(10)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(k atree.Value) (resume bool, err error) {
			testValueEqual(t, sortedKeys[i], k)

			v, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j > mutatedChildMapCount-1; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMutableMapIterateValues(t *testing.T) {

	t.Run("empty", func(t *testing.T) {

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(atree.Value) (resume bool, err error) {
			i++
			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testMap(t, storage, typeInfo, address, m, test_utils.ExpectedMapValue{}, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(15)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (bool, error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) * 2

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, no slab operation", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(25)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			sortedKeys[i] = k
			keyValues[k] = v
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (bool, error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) * 2

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, v, existingValue)

			keyValues[k] = newValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is data slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(15)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (bool, error) {
			k := sortedKeys[i]

			testValueEqual(t, sortedKeys[i], k)
			testValueEqual(t, keyValues[k], v)

			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, split slab", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(25)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		r := 'a'
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (bool, error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			newValue := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate primitive values, root is metadata slab, merge slabs", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(10)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		r := 'a'
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.NewStringValue(strings.Repeat(string(r), 25))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
			keyValues[k] = v
			sortedKeys[i] = k
		}
		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (bool, error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			newValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision primitive values, 1 level", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 1
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) / 2
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision primitive values, 4 levels", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 4
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			newValue := v.(test_utils.Uint64Value) / 2
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, newValue)
			require.NoError(t, err)

			existingValue, err := existingStorable.StoredValue(storage)
			require.NoError(t, err)
			require.Equal(t, keyValues[k], existingValue)

			i++
			keyValues[k] = newValue

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount = uint64(15)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, no slab operation", func(t *testing.T) {
		const (
			mapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (updating elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[childKey] = childNewValue

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(5)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(35)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(5)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(10)
			mutatedChildMapCount = uint64(1)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j >= mutatedChildMapCount; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("mutate collision inlined container, 1 level", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 1
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate collision inlined container, 4 levels", func(t *testing.T) {
		const (
			mapCount = uint64(1024)
		)

		r := newRand(t)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const digestLevels = 4
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(0)
			cv := test_utils.Uint64Value(i)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			digests := newRandomDigests(r, digestLevels)
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
			sortedKeys[i] = k
		}

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			childKey := test_utils.Uint64Value(0)
			childNewValue := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childNewValue)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues[childKey] = childNewValue
			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, sortedKeys, false)
	})

	t.Run("mutate inlined container", func(t *testing.T) {
		const (
			mapCount        = uint64(15)
			valueStringSize = 16
		)

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			ck := test_utils.Uint64Value(0)
			cv := test_utils.NewStringValue(randStr(r, valueStringSize))

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)
			sortedKeys[i] = k

			existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}
		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(1), childMap.Count())
			require.True(t, childMap.Inlined())

			newChildMapKey := test_utils.Uint64Value(1) // Previous key is 0
			newChildMapValue := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, newChildMapKey, newChildMapValue)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedChildMapValues[newChildMapKey] = newChildMapValue

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Iterate and mutate child map (removing elements)
		i = 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, uint64(2), childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			// Remove key 0
			ck := test_utils.Uint64Value(0)

			existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, ck)
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
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(1)
			mutatedChildMapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("uninline inlined container, root is metadata slab, merge slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(5)
			mutatedChildMapCount = uint64(35)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount; j < mutatedChildMapCount; j++ {
				childKey := test_utils.Uint64Value(j)
				childValue := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, childKey, childValue)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[childKey] = childValue
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, no slab operation", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(35)
			mutatedChildMapCount = uint64(1)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j > mutatedChildMapCount-1; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("inline uninlined container, root is data slab, split slab", func(t *testing.T) {
		const (
			mapCount             = uint64(15)
			childMapCount        = uint64(35)
			mutatedChildMapCount = uint64(10)
		)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {

			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			childMapValues := make(test_utils.ExpectedMapValue)
			for j := range childMapCount {
				ck := test_utils.Uint64Value(j)
				cv := test_utils.Uint64Value(j)

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				childMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			keyValues[k] = childMapValues
			sortedKeys[i] = k
		}

		require.Equal(t, mapCount, m.Count())
		require.True(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Sort keys by digest
		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate and mutate child map (inserting elements)
		i := 0
		err = m.IterateValues(test_utils.CompareValue, test_utils.GetHashInput, func(v atree.Value) (resume bool, err error) {
			k := sortedKeys[i]

			testValueEqual(t, keyValues[k], v)

			childMap, ok := v.(*atree.OrderedMap)
			require.True(t, ok)
			require.Equal(t, childMapCount, childMap.Count())
			require.False(t, childMap.Inlined())

			expectedChildMapValues, ok := keyValues[k].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			for j := childMapCount - 1; j > mutatedChildMapCount-1; j-- {
				childKey := test_utils.Uint64Value(j)

				existingKeyStorable, existingValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, childKey)
				require.NoError(t, err)
				require.NotNil(t, existingKeyStorable)
				require.NotNil(t, existingValueStorable)

				delete(expectedChildMapValues, childKey)
			}

			require.Equal(t, mutatedChildMapCount, childMap.Count())
			require.True(t, childMap.Inlined())

			i++

			return true, nil
		})

		require.NoError(t, err)
		require.Equal(t, mapCount, uint64(i)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.False(t, IsMapRootDataSlab(m))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func testMapDeterministicHashCollision(t *testing.T, r *rand.Rand, maxDigestLevel int) {

	const (
		mapCount           = 1024
		keyStringMaxSize   = 1024
		valueStringMaxSize = 1024

		// mockDigestCount is the number of unique set of digests.
		// Each set has maxDigestLevel of digest.
		mockDigestCount = 8
	)

	uniqueFirstLevelDigests := make(map[atree.Digest]bool, mockDigestCount)
	firstLevelDigests := make([]atree.Digest, 0, mockDigestCount)
	for len(firstLevelDigests) < mockDigestCount {
		d := newRandomDigest(r)
		if !uniqueFirstLevelDigests[d] {
			uniqueFirstLevelDigests[d] = true
			firstLevelDigests = append(firstLevelDigests, d)
		}
	}

	digestsGroup := make([][]atree.Digest, mockDigestCount)
	for i := range digestsGroup {
		digests := make([]atree.Digest, maxDigestLevel)
		digests[0] = firstLevelDigests[i]
		for j := 1; j < maxDigestLevel; j++ {
			digests[j] = newRandomDigest(r)
		}
		digestsGroup[i] = digests
	}

	digesterBuilder := &mockDigesterBuilder{}

	keyValues := make(map[atree.Value]atree.Value, mapCount)
	i := 0
	for len(keyValues) < mapCount {
		k := test_utils.NewStringValue(randStr(r, r.Intn(keyStringMaxSize)))
		if _, found := keyValues[k]; !found {
			keyValues[k] = test_utils.NewStringValue(randStr(r, r.Intn(valueStringMaxSize)))

			index := i % len(digestsGroup)
			digests := digestsGroup[index]
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			i++
		}
	}

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	stats, err := atree.GetMapStats(m)
	require.NoError(t, err)
	require.Equal(t, uint64(mockDigestCount), stats.CollisionDataSlabCount)

	// Remove all elements
	for k, v := range keyValues {
		removedKeyStorable, removedValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.NoError(t, err)

		removedKey, err := removedKeyStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, k, removedKey)

		removedValue, err := removedValueStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, v, removedValue)

		if id, ok := removedKeyStorable.(atree.SlabIDStorable); ok {
			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		if id, ok := removedValueStorable.(atree.SlabIDStorable); ok {
			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}
	}

	testEmptyMap(t, storage, typeInfo, address, m)
}

func testMapRandomHashCollision(t *testing.T, r *rand.Rand, maxDigestLevel int) {

	const (
		mapCount           = 1024
		keyStringMaxSize   = 1024
		valueStringMaxSize = 1024
	)

	digesterBuilder := &mockDigesterBuilder{}

	keyValues := make(map[atree.Value]atree.Value, mapCount)
	for len(keyValues) < mapCount {
		k := test_utils.NewStringValue(randStr(r, r.Intn(keyStringMaxSize)))

		if _, found := keyValues[k]; !found {
			keyValues[k] = test_utils.NewStringValue(randStr(r, valueStringMaxSize))

			digests := make([]atree.Digest, maxDigestLevel)
			for i := range digests {
				digests[i] = newRandomDigest(r)
			}

			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}
	}

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	// Remove all elements
	for k, v := range keyValues {
		removedKeyStorable, removedValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.NoError(t, err)

		removedKey, err := removedKeyStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, k, removedKey)

		removedValue, err := removedValueStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, v, removedValue)

		if id, ok := removedKeyStorable.(atree.SlabIDStorable); ok {
			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		if id, ok := removedValueStorable.(atree.SlabIDStorable); ok {
			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}
	}

	testEmptyMap(t, storage, typeInfo, address, m)
}

func TestMapHashCollision(t *testing.T) {

	atree.SetThreshold(512)
	defer atree.SetThreshold(1024)

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
	storage *atree.PersistentSlabStorage,
	typeInfo atree.TypeInfo,
	address atree.Address,
) (*atree.OrderedMap, map[atree.Value]atree.Value) {

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

	m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	keyValues := make(map[atree.Value]atree.Value)
	var keys []atree.Value
	for range opCount {

		nextOp := r.Intn(MapMaxOp)

		if m.Count() == 0 {
			nextOp = MapSetOp
		}

		switch nextOp {

		case MapSetOp:

			k := randomValue(r, atree.MaxInlineMapElementSize())
			v := randomValue(r, atree.MaxInlineMapElementSize())

			digests := make([]atree.Digest, digestMaxLevels)
			for i := range digests {
				digests[i] = atree.Digest(r.Intn(digestMaxValue)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			}

			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)

			if oldv, ok := keyValues[k]; ok {
				require.NotNil(t, existingStorable)

				existingValue, err := existingStorable.StoredValue(storage)
				require.NoError(t, err)
				testValueEqual(t, oldv, existingValue)

				if id, ok := existingStorable.(atree.SlabIDStorable); ok {
					err = storage.Remove(atree.SlabID(id))
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

			removedKeyStorable, removedValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)

			removedKey, err := removedKeyStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, k, removedKey)

			removedValue, err := removedValueStorable.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, keyValues[k], removedValue)

			if id, ok := removedKeyStorable.(atree.SlabIDStorable); ok {
				err := storage.Remove(atree.SlabID(id))
				require.NoError(t, err)
			}

			if id, ok := removedValueStorable.(atree.SlabIDStorable); ok {
				err := storage.Remove(atree.SlabID(id))
				require.NoError(t, err)
			}

			delete(keyValues, k)
			copy(keys[index:], keys[index+1:])
			keys = keys[:len(keys)-1]
		}

		require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
		require.Equal(t, address, m.Address())
		require.Equal(t, uint64(len(keys)), m.Count())
	}

	return m, keyValues
}

func TestMapSetRemoveRandomValues(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	storage := newTestPersistentStorage(t)
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, keyValues := testMapSetRemoveRandomValues(t, r, storage, typeInfo, address)

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
}

func TestMapDecodeV0(t *testing.T) {

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {

		mapSlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		slabData := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage, mapSlabID, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		testEmptyMapV0(t, storage, typeInfo, address, decodedMap)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapCount = uint64(1)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})
		}

		mapSlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		slabData := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("has pointer no collision", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount - 1 {
			k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			v := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			r++
		}

		mapSlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		nestedSlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

		k := test_utils.NewStringValue(strings.Repeat(string(r), 22))

		keyValues[k] = test_utils.ExpectedArrayValue{test_utils.Uint64Value(0)}

		digests := []atree.Digest{atree.Digest(mapCount - 1), atree.Digest((mapCount - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		slabData := map[atree.SlabID][]byte{

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
				// element: [hhhhhhhhhhhhhhhhhhhhhh:atree.SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
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
		decodedMap, err := atree.NewMapWithRootID(storage2, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 1 level", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := []atree.Digest{atree.Digest(i % 4), atree.Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			keyValues[k] = v
		}

		mapSlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		slabData := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 2 levels", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := []atree.Digest{atree.Digest(i % 4), atree.Digest(i % 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			keyValues[k] = v
		}

		mapSlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		slabData := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("external collision", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		digesterBuilder := &mockDigesterBuilder{}

		const mapCount = uint64(20)
		keyValues := make(map[atree.Value]atree.Value)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := []atree.Digest{atree.Digest(i % 2), atree.Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			keyValues[k] = v
		}

		mapSlabID := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		slabData := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage, mapSlabID, digesterBuilder)
		require.NoError(t, err)

		testMapV0(t, storage, typeInfo, address, decodedMap, keyValues, nil, false)
	})
}

func TestMapEncodeDecode(t *testing.T) {

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {

		storage := newTestBasicStorage(t)

		// Create map
		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		testEmptyMap(t, storage2, typeInfo, address, decodedMap)
	})

	t.Run("dataslab as root", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(1)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, mapCount, m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("has inlined array", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount - 1 {
			k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			v := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child array
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)

		childArray, err := atree.NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		err = childArray.Append(test_utils.Uint64Value(0))
		require.NoError(t, err)

		k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
		v := childArray

		keyValues[k] = test_utils.ExpectedArrayValue{test_utils.Uint64Value(0)}

		digests := []atree.Digest{atree.Digest(mapCount - 1), atree.Digest((mapCount - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert nested array
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, mapCount, m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
				// element: [hhhhhhhhhhhhhhhhhhhhhh:atree.SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
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
		meta, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
		require.True(t, ok)

		childSlabIDs, childSizes, _ := atree.GetMapMetaDataSlabChildInfo(meta)

		require.Equal(t, 2, len(childSlabIDs))
		require.Equal(t, uint32(len(stored[id2])), childSizes[0]) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		const inlinedExtraDataSize = 8
		require.Equal(t, uint32(len(stored[id3])-inlinedExtraDataSize+atree.SlabIDLength), childSizes[1]) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, inlined child map of same type", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewSimpleTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount {

			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(i)
			cv := test_utils.Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, inlined child map of different type", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo1 := test_utils.NewSimpleTypeInfo(43)
		childMapTypeInfo2 := test_utils.NewSimpleTypeInfo(44)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount {

			var ti atree.TypeInfo
			if i%2 == 0 {
				ti = childMapTypeInfo2
			} else {
				ti = childMapTypeInfo1
			}

			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), ti)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(i)
			cv := test_utils.Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, multiple levels of inlined child map of same type", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewSimpleTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount {
			// Create grand child map
			gchildMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			gck := test_utils.Uint64Value(i)
			gcv := test_utils.Uint64Value(i * 2)

			// Insert element to grand child map
			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, gck, gcv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(i)

			// Insert grand child map to child map
			existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, gchildMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = test_utils.ExpectedMapValue{ck: test_utils.ExpectedMapValue{gck: gcv}}
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root data slab, multiple levels of inlined child map of different type", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo1 := test_utils.NewSimpleTypeInfo(43)
		childMapTypeInfo2 := test_utils.NewSimpleTypeInfo(44)
		gchildMapTypeInfo1 := test_utils.NewSimpleTypeInfo(45)
		gchildMapTypeInfo2 := test_utils.NewSimpleTypeInfo(46)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount {
			var gti atree.TypeInfo
			if i%2 == 0 {
				gti = gchildMapTypeInfo2
			} else {
				gti = gchildMapTypeInfo1
			}

			// Create grand child map
			gchildMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), gti)
			require.NoError(t, err)

			gck := test_utils.Uint64Value(i)
			gcv := test_utils.Uint64Value(i * 2)

			// Insert element to grand child map
			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, gck, gcv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			var cti atree.TypeInfo
			if i%2 == 0 {
				cti = childMapTypeInfo2
			} else {
				cti = childMapTypeInfo1
			}

			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), cti)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(i)

			// Insert grand child map to child map
			existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, gchildMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = test_utils.ExpectedMapValue{ck: test_utils.ExpectedMapValue{gck: gcv}}
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root metadata slab, inlined child map of same type", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewSimpleTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount {

			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(i)
			cv := test_utils.Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 10}) // inlined maps index 2-9
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 11})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("root metadata slab, inlined child map of different type", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo1 := test_utils.NewSimpleTypeInfo(43)
		childMapTypeInfo2 := test_utils.NewSimpleTypeInfo(44)
		childMapTypeInfo3 := test_utils.NewSimpleTypeInfo(45)
		childMapTypeInfo4 := test_utils.NewSimpleTypeInfo(46)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount {

			var ti atree.TypeInfo
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
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), ti)
			require.NoError(t, err)

			ck := test_utils.Uint64Value(i)
			cv := test_utils.Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.NewStringValue(string(r))
			r++

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 10}) // inlined maps index 2-9
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 11})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 1 level", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := []atree.Digest{atree.Digest(i % 4), atree.Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
		}

		require.Equal(t, mapCount, m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("inline collision 2 levels", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := []atree.Digest{atree.Digest(i % 4), atree.Digest(i % 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
		}

		require.Equal(t, mapCount, m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("external collision", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(20)
		keyValues := make(map[atree.Value]atree.Value)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)

			digests := []atree.Digest{atree.Digest(i % 2), atree.Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = v
		}

		require.Equal(t, mapCount, m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to child map", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount - 1 {
			k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			v := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child map
		typeInfo2 := test_utils.NewSimpleTypeInfo(43)

		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo2)
		require.NoError(t, err)

		expectedChildMapValues := test_utils.ExpectedMapValue{}
		for i := range uint64(2) {
			k := test_utils.Uint64Value(i)
			v := test_utils.NewStringValue(strings.Repeat("b", 22))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v
		}

		k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
		v := childMap
		keyValues[k] = expectedChildMapValues

		digests := []atree.Digest{atree.Digest(mapCount - 1), atree.Digest((mapCount - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert child map
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, mapCount, m.Count())

		// root slab (data slab) ID
		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		// child map slab ID
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
				// element: [bbbbbbbbbbbbbbbbbbbbbb:atree.SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,2)]
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to grand child map", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount - 1 {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i * 2)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Create child map
		childTypeInfo := test_utils.NewSimpleTypeInfo(43)

		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childTypeInfo)
		require.NoError(t, err)

		// Create grand child map
		gchildTypeInfo := test_utils.NewSimpleTypeInfo(44)

		gchildMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), gchildTypeInfo)
		require.NoError(t, err)

		expectedGChildMapValues := test_utils.ExpectedMapValue{}
		r := 'a'
		for range 2 {
			k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			v := test_utils.NewStringValue(strings.Repeat(string(r), 22))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			r++
		}

		// Insert grand child map to child map
		existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), gchildMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		k := test_utils.Uint64Value(mapCount - 1)
		v := childMap
		keyValues[k] = test_utils.ExpectedMapValue{test_utils.Uint64Value(0): expectedGChildMapValues}

		digests := []atree.Digest{atree.Digest(mapCount - 1), atree.Digest((mapCount - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert child map
		existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, mapCount, m.Count())

		// root slab (data slab) ID
		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		// grand child map slab ID
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
				// value: atree.SlabID{...3}
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to child array", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(8)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount - 1 {
			k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			v := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child array
		const childArrayCount = 5

		typeInfo2 := test_utils.NewSimpleTypeInfo(43)

		childArray, err := atree.NewArray(storage, address, typeInfo2)
		require.NoError(t, err)

		expectedChildValues := make([]atree.Value, childArrayCount)
		for i := range expectedChildValues {
			v := test_utils.NewStringValue(strings.Repeat("b", 22))
			err = childArray.Append(v)
			require.NoError(t, err)

			expectedChildValues[i] = v
		}

		k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
		v := childArray

		keyValues[k] = test_utils.ExpectedArrayValue(expectedChildValues)

		digests := []atree.Digest{atree.Digest(mapCount - 1), atree.Digest((mapCount - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert nested array
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, mapCount, m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})
		id3 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})
		id4 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 4})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
				// element: [hhhhhhhhhhhhhhhhhhhhhh:atree.SlabID(1,2,3,4,5,6,7,8,0,0,0,0,0,0,0,4)]
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
		meta, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
		require.True(t, ok)

		childSlabIDs, childSizes, _ := atree.GetMapMetaDataSlabChildInfo(meta)

		require.Equal(t, 2, len(childSlabIDs))
		require.Equal(t, uint32(len(stored[id2])), childSizes[0])                    //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		require.Equal(t, uint32(len(stored[id3])+atree.SlabIDLength), childSizes[1]) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		// Decode data to new storage
		storage2 := newTestPersistentStorageWithData(t, stored)

		// Test new map from storage2
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to grand child array", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		r := 'a'
		for i := range mapCount - 1 {
			k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			v := test_utils.NewStringValue(strings.Repeat(string(r), 22))
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i), atree.Digest(i * 2)}
			digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			r++
		}

		// Create child array
		childTypeInfo := test_utils.NewSimpleTypeInfo(43)

		childArray, err := atree.NewArray(storage, address, childTypeInfo)
		require.NoError(t, err)

		// Create grand child array
		const gchildArrayCount = 5

		gchildTypeInfo := test_utils.NewSimpleTypeInfo(44)

		gchildArray, err := atree.NewArray(storage, address, gchildTypeInfo)
		require.NoError(t, err)

		expectedGChildValues := make([]atree.Value, gchildArrayCount)
		for i := range expectedGChildValues {
			v := test_utils.NewStringValue(strings.Repeat("b", 22))
			err = gchildArray.Append(v)
			require.NoError(t, err)

			expectedGChildValues[i] = v
		}

		// Insert grand child array to child array
		err = childArray.Append(gchildArray)
		require.NoError(t, err)

		k := test_utils.NewStringValue(strings.Repeat(string(r), 22))
		v := childArray

		keyValues[k] = test_utils.ExpectedArrayValue{test_utils.ExpectedArrayValue(expectedGChildValues)}

		digests := []atree.Digest{atree.Digest(mapCount - 1), atree.Digest((mapCount - 1) * 2)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		// Insert child array to parent map
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, mapCount, m.Count())

		// parent map root slab ID
		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		// grand child array root slab ID
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 3})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{

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
				// atree.SlabID{...3}
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("pointer to storable slab", func(t *testing.T) {

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		k := test_utils.Uint64Value(0)
		v := test_utils.Uint64Value(0)

		digests := []atree.Digest{atree.Digest(0), atree.Digest(1)}
		digesterBuilder.On("Digest", k).Return(mockDigester{d: digests})

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(1), m.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})
		id2 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 2})

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
		vs := test_utils.NewStringValue(strings.Repeat("a", 128))
		existingStorable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, vs)
		require.NoError(t, err)

		existingValue, err := existingStorable.StoredValue(storage)
		require.NoError(t, err)
		testValueEqual(t, v, existingValue)

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
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewCompositeTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {

			// Create child map, composite with one field "uuid"
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.NewStringValue("uuid")
			cv := test_utils.Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			k := test_utils.Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = test_utils.ExpectedMapValue{ck: cv}
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with two fields (same order)", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewCompositeTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			expectedChildMapVaues := test_utils.ExpectedMapValue{}

			// Create child map, composite with one field "uuid"
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.NewStringValue("uuid")
			cv := test_utils.Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapVaues[ck] = cv

			ck = test_utils.NewStringValue("amount")
			cv = test_utils.Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapVaues[ck] = cv

			k := test_utils.Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapVaues
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with two fields (different order)", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewCompositeTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		// fields are ordered differently because of different seed.
		for i := range mapCount {
			expectedChildMapValues := test_utils.ExpectedMapValue{}

			// Create child map, composite with one field "uuid"
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.NewStringValue("uuid")
			cv := test_utils.Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			ck = test_utils.NewStringValue("a")
			cv = test_utils.Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			k := test_utils.Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with different fields", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewCompositeTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(3)
		keyValues := make(map[atree.Value]atree.Value, mapCount)

		for i := range mapCount {
			expectedChildMapValues := test_utils.ExpectedMapValue{}

			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.NewStringValue("uuid")
			cv := test_utils.Uint64Value(i)

			// Insert first element "uuid" to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			// Insert second element to child map (second element is different)
			switch i % 3 {
			case 0:
				ck = test_utils.NewStringValue("a")
				cv = test_utils.Uint64Value(i * 2)
				existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)

			case 1:
				ck = test_utils.NewStringValue("b")
				cv = test_utils.Uint64Value(i * 2)
				existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)

			case 2:
				ck = test_utils.NewStringValue("c")
				cv = test_utils.Uint64Value(i * 2)
				existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			}

			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			k := test_utils.Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("same composite with different number of fields", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo := test_utils.NewCompositeTypeInfo(43)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(2)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		// fields are ordered differently because of different seed.
		for i := range mapCount {
			expectedChildMapValues := test_utils.ExpectedMapValue{}

			// Create child map, composite with one field "uuid"
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), childMapTypeInfo)
			require.NoError(t, err)

			ck := test_utils.NewStringValue("uuid")
			cv := test_utils.Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			if i == 0 {
				ck = test_utils.NewStringValue("a")
				cv = test_utils.Uint64Value(i * 2)

				// Insert element to child map
				existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[ck] = cv
			}

			k := test_utils.Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})

	t.Run("different composite", func(t *testing.T) {
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		childMapTypeInfo1 := test_utils.NewCompositeTypeInfo(43)
		childMapTypeInfo2 := test_utils.NewCompositeTypeInfo(44)

		// Create and populate map in memory
		storage := newTestBasicStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		// Create map
		parentMap, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(4)
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		// fields are ordered differently because of different seed.
		for i := range mapCount {
			expectedChildMapValues := test_utils.ExpectedMapValue{}

			var ti atree.TypeInfo
			if i%2 == 0 {
				ti = childMapTypeInfo1
			} else {
				ti = childMapTypeInfo2
			}

			// Create child map, composite with two field "uuid" and "a"
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), ti)
			require.NoError(t, err)

			ck := test_utils.NewStringValue("uuid")
			cv := test_utils.Uint64Value(i)

			// Insert element to child map
			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			ck = test_utils.NewStringValue("a")
			cv = test_utils.Uint64Value(i * 2)

			// Insert element to child map
			existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, ck, cv)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[ck] = cv

			k := test_utils.Uint64Value(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			// Insert child map to parent map
			existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			keyValues[k] = expectedChildMapValues
		}

		require.Equal(t, mapCount, parentMap.Count())

		id1 := atree.NewSlabID(address, atree.SlabIndex{0, 0, 0, 0, 0, 0, 0, 1})

		// Expected serialized slab data with slab id
		expected := map[atree.SlabID][]byte{
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
		decodedMap, err := atree.NewMapWithRootID(storage2, id1, digesterBuilder)
		require.NoError(t, err)

		testMap(t, storage2, typeInfo, address, decodedMap, keyValues, nil, false)
	})
}

func TestMapEncodeDecodeRandomValues(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, keyValues := testMapSetRemoveRandomValues(t, r, storage, typeInfo, address)

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

	// Create a new storage with encoded data from base storage
	storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

	// Create new map from new storage
	m2, err := atree.NewMapWithRootID(storage2, m.SlabID(), atree.GetMapDigesterBuilder(m))
	require.NoError(t, err)

	testMap(t, storage2, typeInfo, address, m2, keyValues, nil, false)
}

func TestMapStoredValue(t *testing.T) {

	const mapCount = 4096

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	keyValues := make(map[atree.Value]atree.Value, mapCount)
	i := uint64(0)
	for len(keyValues) < mapCount {
		k := test_utils.NewStringValue(randStr(r, 16))
		keyValues[k] = test_utils.Uint64Value(i)
		i++
	}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	rootID := m.SlabID()

	slabIterator, err := storage.SlabIterator()
	require.NoError(t, err)

	for {
		id, slab := slabIterator()

		if id == atree.SlabIDUndefined {
			break
		}

		value, err := slab.StoredValue(storage)

		if id == rootID {
			require.NoError(t, err)

			m2, ok := value.(*atree.OrderedMap)
			require.True(t, ok)

			testMap(t, storage, typeInfo, address, m2, keyValues, nil, false)
		} else {
			require.Equal(t, 1, errorCategorizationCount(err))
			var fatalError *atree.FatalError
			var notValueError *atree.NotValueError
			require.ErrorAs(t, err, &fatalError)
			require.ErrorAs(t, err, &notValueError)
			require.ErrorAs(t, fatalError, &notValueError)
			require.Nil(t, value)
		}
	}
}

func TestMapPopIterate(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())

		i := 0
		err = m.PopIterate(func(atree.Storable, atree.Storable) {
			i++
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("root-dataslab", func(t *testing.T) {
		const mapCount = uint64(10)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		for i := range mapCount {
			key, value := test_utils.Uint64Value(i), test_utils.Uint64Value(i*10)
			sortedKeys[i] = key
			keyValues[key] = value

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, key, value)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, mapCount, m.Count())

		err = storage.Commit()
		require.NoError(t, err)

		require.Equal(t, 1, storage.Count())

		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		i := mapCount
		err = m.PopIterate(func(k, v atree.Storable) {
			i--

			kv, err := k.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, uint64(0), i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("root-metaslab", func(t *testing.T) {
		const mapCount = 4096

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		i := 0
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, 16))
			if _, found := keyValues[k]; !found {
				sortedKeys[i] = k
				keyValues[k] = test_utils.NewStringValue(randStr(r, 16))
				i++
			}
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		err = storage.Commit()
		require.NoError(t, err)

		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		// Iterate key value pairs
		i = len(keyValues)
		err = m.PopIterate(func(k atree.Storable, v atree.Storable) {
			i--

			kv, err := k.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})

	t.Run("collision", func(t *testing.T) {
		//MetaDataSlabCount:1 DataSlabCount:13 CollisionDataSlabCount:100

		const mapCount = 1024

		atree.SetThreshold(512)
		defer atree.SetThreshold(1024)

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		digesterBuilder := &mockDigesterBuilder{}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		sortedKeys := make([]atree.Value, mapCount)
		i := 0
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, 16))

			if _, found := keyValues[k]; !found {

				sortedKeys[i] = k
				keyValues[k] = test_utils.NewStringValue(randStr(r, 16))

				digests := []atree.Digest{
					atree.Digest(i % 100), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
					atree.Digest(i % 5),   //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				}

				digesterBuilder.On("Digest", k).Return(mockDigester{digests})

				existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, keyValues[k])
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				i++
			}
		}

		sort.Stable(keysByDigest{sortedKeys, digesterBuilder})

		err = storage.Commit()
		require.NoError(t, err)

		// Iterate key value pairs
		i = mapCount
		err = m.PopIterate(func(k atree.Storable, v atree.Storable) {
			i--

			kv, err := k.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, sortedKeys[i], kv)

			vv, err := v.StoredValue(storage)
			require.NoError(t, err)
			testValueEqual(t, keyValues[sortedKeys[i]], vv)
		})

		require.NoError(t, err)
		require.Equal(t, 0, i)

		testEmptyMap(t, storage, typeInfo, address, m)
	})
}

func TestEmptyMap(t *testing.T) {

	t.Parallel()

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	t.Run("get", func(t *testing.T) {
		s, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var keyNotFoundError *atree.KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)
		require.Nil(t, s)
	})

	t.Run("remove", func(t *testing.T) {
		existingMapKeyStorable, existingMapValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0))
		require.Equal(t, 1, errorCategorizationCount(err))
		var userError *atree.UserError
		var keyNotFoundError *atree.KeyNotFoundError
		require.ErrorAs(t, err, &userError)
		require.ErrorAs(t, err, &keyNotFoundError)
		require.ErrorAs(t, userError, &keyNotFoundError)
		require.Nil(t, existingMapKeyStorable)
		require.Nil(t, existingMapValueStorable)
	})

	t.Run("readonly iterate", func(t *testing.T) {
		i := 0
		err := m.IterateReadOnly(func(atree.Value, atree.Value) (bool, error) {
			i++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, i)
	})

	t.Run("iterate", func(t *testing.T) {
		i := 0
		err := m.Iterate(test_utils.CompareValue, test_utils.GetHashInput, func(atree.Value, atree.Value) (bool, error) {
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
		require.True(t, test_utils.CompareTypeInfo(typeInfo, m.Type()))
	})

	t.Run("address", func(t *testing.T) {
		require.Equal(t, address, m.Address())
	})

	// TestMapEncodeDecode/empty tests empty map encoding and decoding
}

func TestMapFromBatchData(t *testing.T) {

	t.Run("empty", func(t *testing.T) {
		typeInfo := test_utils.NewSimpleTypeInfo(42)

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			atree.NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}

		// Create a map with new storage, new address, and original map's elements.
		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			atree.NewDefaultDigesterBuilder(),
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {
				return iter.Next()
			})
		require.NoError(t, err)
		require.NotEqual(t, copied.SlabID(), m.SlabID())

		testEmptyMap(t, storage, typeInfo, address, copied)
	})

	t.Run("root-dataslab", func(t *testing.T) {
		atree.SetThreshold(1024)

		const mapCount = uint64(10)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			atree.NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := range mapCount {
			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(i), test_utils.Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		require.Equal(t, mapCount, m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []atree.Value
		keyValues := make(map[atree.Value]atree.Value)

		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}

		// Create a map with new storage, new address, and original map's elements.
		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {

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
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(4096)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			atree.NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := range mapCount {
			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(i), test_utils.Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		require.Equal(t, mapCount, m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []atree.Value
		keyValues := make(map[atree.Value]atree.Value)

		storage := newTestPersistentStorage(t)
		digesterBuilder := atree.NewDefaultDigesterBuilder()
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}

		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {
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
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(10)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			atree.NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := range mapCount {
			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(i), test_utils.Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		k := test_utils.NewStringValue(strings.Repeat("a", int(atree.MaxInlineMapElementSize()-2))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		v := test_utils.NewStringValue(strings.Repeat("b", int(atree.MaxInlineMapElementSize()-2))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		require.Equal(t, mapCount+1, m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []atree.Value
		keyValues := make(map[atree.Value]atree.Value)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {
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
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = uint64(8)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			atree.NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for i := range mapCount {
			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(i), test_utils.Uint64Value(i*10))
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		storable, err := m.Set(
			test_utils.CompareValue,
			test_utils.GetHashInput,
			test_utils.NewStringValue(strings.Repeat("b", int(atree.MaxInlineMapElementSize()-2))), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			test_utils.NewStringValue(strings.Repeat("b", int(atree.MaxInlineMapElementSize()-2))), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		)
		require.NoError(t, err)
		require.Nil(t, storable)

		require.Equal(t, mapCount+1, m.Count())
		require.Equal(t, typeInfo, m.Type())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []atree.Value
		keyValues := make(map[atree.Value]atree.Value)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {
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
		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		const mapCount = 4096

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			atree.NewDefaultDigesterBuilder(),
			typeInfo,
		)
		require.NoError(t, err)

		for m.Count() < mapCount {
			k := randomValue(r, atree.MaxInlineMapElementSize())
			v := randomValue(r, atree.MaxInlineMapElementSize())

			_, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(mapCount), m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}
		digesterBuilder := atree.NewDefaultDigesterBuilder()

		var sortedKeys []atree.Value
		keyValues := make(map[atree.Value]atree.Value, mapCount)

		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {
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

		const (
			mapCount                   = uint64(1024)
			maxCollisionLimitPerDigest = uint32(1024 / 2)
		)

		atree.SetThreshold(512)
		defer atree.SetThreshold(1024)

		savedMaxCollisionLimitPerDigest := atree.MaxCollisionLimitPerDigest
		defer func() {
			atree.MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
		}()
		atree.MaxCollisionLimitPerDigest = maxCollisionLimitPerDigest

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		digesterBuilder := &mockDigesterBuilder{}

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			digesterBuilder,
			typeInfo,
		)
		require.NoError(t, err)

		for i := range mapCount {

			k, v := test_utils.Uint64Value(i), test_utils.Uint64Value(i*10)

			digests := make([]atree.Digest, 2)
			if i%2 == 0 {
				digests[0] = 0
			} else {
				digests[0] = atree.Digest(i % (mapCount / 2))
			}
			digests[1] = atree.Digest(i)

			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, storable)
		}

		require.Equal(t, mapCount, m.Count())

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []atree.Value
		keyValues := make(map[atree.Value]atree.Value)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}

		i := 0
		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {
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

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		r := newRand(t)

		maxStringSize := int(atree.MaxInlineMapKeySize() - 2) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		typeInfo := test_utils.NewSimpleTypeInfo(42)

		digesterBuilder := &mockDigesterBuilder{}

		m, err := atree.NewMap(
			newTestPersistentStorage(t),
			atree.Address{1, 2, 3, 4, 5, 6, 7, 8},
			digesterBuilder,
			typeInfo,
		)
		require.NoError(t, err)

		k := test_utils.NewStringValue(randStr(r, maxStringSize))
		v := test_utils.NewStringValue(randStr(r, maxStringSize))
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{3881892766069237908}})

		storable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		k = test_utils.NewStringValue(randStr(r, maxStringSize))
		v = test_utils.NewStringValue(randStr(r, maxStringSize))
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{3882976639190041664}})

		storable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		k = test_utils.NewStringValue("zFKUYYNfIfJCCakcDuIEHj")
		v = test_utils.NewStringValue("EZbaCxxjDtMnbRlXJMgfHnZ")
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{3883321011075439822}})

		storable, err = m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, storable)

		iter, err := m.ReadOnlyIterator()
		require.NoError(t, err)

		var sortedKeys []atree.Value
		keyValues := make(map[atree.Value]atree.Value)

		storage := newTestPersistentStorage(t)
		address := atree.Address{2, 3, 4, 5, 6, 7, 8, 9}

		copied, err := atree.NewMapFromBatchData(
			storage,
			address,
			digesterBuilder,
			m.Type(),
			test_utils.CompareValue,
			test_utils.GetHashInput,
			m.Seed(),
			func() (atree.Value, atree.Value, error) {
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

	t.Run("test_utils.SomeValue", func(t *testing.T) {

		const mapCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value)
		for i := range mapCount {

			ks := strings.Repeat("a", i)
			k := test_utils.NewSomeValue(test_utils.NewStringValue(ks))

			vs := strings.Repeat("b", i)
			v := test_utils.NewSomeValue(test_utils.NewStringValue(vs))

			keyValues[k] = test_utils.NewExpectedWrapperValue(test_utils.NewStringValue(vs))

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, true)
	})

	t.Run("atree.Array", func(t *testing.T) {

		const mapCount = 4096

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		keyValues := make(map[atree.Value]atree.Value)
		for i := range mapCount {

			// Create a child array with one element
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			vs := strings.Repeat("b", i)
			v := test_utils.NewSomeValue(test_utils.NewStringValue(vs))

			err = childArray.Append(v)
			require.NoError(t, err)

			// Insert nested array into map
			ks := strings.Repeat("a", i)
			k := test_utils.NewSomeValue(test_utils.NewStringValue(ks))

			keyValues[k] = test_utils.ExpectedArrayValue{test_utils.NewExpectedWrapperValue(test_utils.NewStringValue(vs))}

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, true)
	})
}

func TestMapMaxInlineElement(t *testing.T) {
	t.Parallel()

	r := newRand(t)
	maxStringSize := int(atree.MaxInlineMapKeySize() - 2) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	keyValues := make(map[atree.Value]atree.Value)
	for len(keyValues) < 2 {
		// String length is maxInlineMapKeySize - 2 to account for string encoding overhead.
		k := test_utils.NewStringValue(randStr(r, maxStringSize))
		v := test_utils.NewStringValue(randStr(r, maxStringSize))
		keyValues[k] = v

		_, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
	}

	require.True(t, IsMapRootDataSlab(m))

	// Size of root data slab with two elements (key+value pairs) of
	// max inlined size is target slab size minus
	// slab id size (next slab id is omitted in root slab)
	require.Equal(t, atree.TargetSlabSize()-atree.SlabIDLength, uint64(GetMapRootSlabByteSize(m)))

	testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
}

func TestMapString(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("small", func(t *testing.T) {
		const mapCount = uint64(3)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := `[0:0 1:1 2:2]`
		require.Equal(t, want, m.String())
	})

	t.Run("large", func(t *testing.T) {
		const mapCount = uint64(30)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := `[0:0 1:1 2:2 3:3 4:4 5:5 6:6 7:7 8:8 9:9 10:10 11:11 12:12 13:13 14:14 15:15 16:16 17:17 18:18 19:19 20:20 21:21 22:22 23:23 24:24 25:25 26:26 27:27 28:28 29:29]`
		require.Equal(t, want, m.String())
	})
}

func TestMapSlabDump(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("small", func(t *testing.T) {
		const mapCount = uint64(3)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:55 firstkey:0 elements: [0:0:0 1:1:1 2:2:2]",
		}
		dumps, err := atree.DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("large", func(t *testing.T) {
		const mapCount = uint64(30)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i)}})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapMetaDataSlab id:0x102030405060708.1 size:48 firstKey:0 children: [{id:0x102030405060708.2 size:221 firstKey:0} {id:0x102030405060708.3 size:293 firstKey:13}]",
			"level 2, MapDataSlab id:0x102030405060708.2 size:221 firstkey:0 elements: [0:0:0 1:1:1 2:2:2 3:3:3 4:4:4 5:5:5 6:6:6 7:7:7 8:8:8 9:9:9 10:10:10 11:11:11 12:12:12]",
			"level 2, MapDataSlab id:0x102030405060708.3 size:293 firstkey:13 elements: [13:13:13 14:14:14 15:15:15 16:16:16 17:17:17 18:18:18 19:19:19 20:20:20 21:21:21 22:22:22 23:23:23 24:24:24 25:25:25 26:26:26 27:27:27 28:28:28 29:29:29]",
		}
		dumps, err := atree.DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("inline collision", func(t *testing.T) {
		const mapCount = uint64(30)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i % 10)}})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapMetaDataSlab id:0x102030405060708.1 size:48 firstKey:0 children: [{id:0x102030405060708.2 size:213 firstKey:0} {id:0x102030405060708.3 size:221 firstKey:5}]",
			"level 2, MapDataSlab id:0x102030405060708.2 size:213 firstkey:0 elements: [0:inline[:0:0 :10:10 :20:20] 1:inline[:1:1 :11:11 :21:21] 2:inline[:2:2 :12:12 :22:22] 3:inline[:3:3 :13:13 :23:23] 4:inline[:4:4 :14:14 :24:24]]",
			"level 2, MapDataSlab id:0x102030405060708.3 size:221 firstkey:5 elements: [5:inline[:5:5 :15:15 :25:25] 6:inline[:6:6 :16:16 :26:26] 7:inline[:7:7 :17:17 :27:27] 8:inline[:8:8 :18:18 :28:28] 9:inline[:9:9 :19:19 :29:29]]",
		}
		dumps, err := atree.DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("external collision", func(t *testing.T) {
		const mapCount = uint64(30)

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(i % 2)}})

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:68 firstkey:0 elements: [0:external(0x102030405060708.2) 1:external(0x102030405060708.3)]",
			"collision: MapDataSlab id:0x102030405060708.2 size:135 firstkey:0 elements: [:0:0 :2:2 :4:4 :6:6 :8:8 :10:10 :12:12 :14:14 :16:16 :18:18 :20:20 :22:22 :24:24 :26:26 :28:28]",
			"collision: MapDataSlab id:0x102030405060708.3 size:135 firstkey:0 elements: [:1:1 :3:3 :5:5 :7:7 :9:9 :11:11 :13:13 :15:15 :17:17 :19:19 :21:21 :23:23 :25:25 :27:27 :29:29]",
		}
		dumps, err := atree.DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("key overflow", func(t *testing.T) {

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		k := test_utils.NewStringValue(strings.Repeat("a", int(atree.MaxInlineMapKeySize()))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		v := test_utils.NewStringValue(strings.Repeat("b", int(atree.MaxInlineMapKeySize()))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(0)}})

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:93 firstkey:0 elements: [0:SlabIDStorable({[1 2 3 4 5 6 7 8] [0 0 0 0 0 0 0 2]}):bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb]",
			"StorableSlab id:0x102030405060708.2 storable:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}
		dumps, err := atree.DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})

	t.Run("value overflow", func(t *testing.T) {

		digesterBuilder := &mockDigesterBuilder{}
		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		k := test_utils.NewStringValue(strings.Repeat("a", int(atree.MaxInlineMapKeySize()-2)))   //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		v := test_utils.NewStringValue(strings.Repeat("b", int(atree.MaxInlineMapElementSize()))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		digesterBuilder.On("Digest", k).Return(mockDigester{d: []atree.Digest{atree.Digest(0)}})

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		want := []string{
			"level 1, MapDataSlab id:0x102030405060708.1 size:91 firstkey:0 elements: [0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:SlabIDStorable({[1 2 3 4 5 6 7 8] [0 0 0 0 0 0 0 2]})]",
			"StorableSlab id:0x102030405060708.2 storable:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}
		dumps, err := atree.DumpMapSlabs(m)
		require.NoError(t, err)
		require.Equal(t, want, dumps)
	})
}

func TestMaxCollisionLimitPerDigest(t *testing.T) {
	savedMaxCollisionLimitPerDigest := atree.MaxCollisionLimitPerDigest
	defer func() {
		atree.MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
	}()

	t.Run("collision limit 0", func(t *testing.T) {
		const mapCount = uint64(1024)

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Set noncryptographic hash collision limit as 0,
		// meaning no collision is allowed at first level.
		atree.MaxCollisionLimitPerDigest = uint32(0)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// Insert elements within collision limits
		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Insert elements exceeding collision limits
		collisionKeyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(mapCount + i)
			v := test_utils.Uint64Value(mapCount + i)
			collisionKeyValues[k] = v

			digests := []atree.Digest{atree.Digest(i)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.Equal(t, 1, errorCategorizationCount(err))
			var fatalError *atree.FatalError
			var collisionLimitError *atree.CollisionLimitError
			require.ErrorAs(t, err, &fatalError)
			require.ErrorAs(t, err, &collisionLimitError)
			require.ErrorAs(t, fatalError, &collisionLimitError)
			require.Nil(t, existingStorable)
		}

		// Verify that no new elements exceeding collision limit inserted
		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Update elements within collision limits
		for k := range keyValues {
			v := test_utils.Uint64Value(0)
			keyValues[k] = v
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("collision limit > 0", func(t *testing.T) {
		const mapCount = uint64(1024)

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		// Set noncryptographic hash collision limit as 7,
		// meaning at most 8 elements in collision group per digest at first level.
		atree.MaxCollisionLimitPerDigest = uint32(7)

		digesterBuilder := &mockDigesterBuilder{}
		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(i)
			v := test_utils.Uint64Value(i)
			keyValues[k] = v

			digests := []atree.Digest{atree.Digest(i % 128)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// Insert elements within collision limits
		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Insert elements exceeding collision limits
		collisionKeyValues := make(map[atree.Value]atree.Value, mapCount)
		for i := range mapCount {
			k := test_utils.Uint64Value(mapCount + i)
			v := test_utils.Uint64Value(mapCount + i)
			collisionKeyValues[k] = v

			digests := []atree.Digest{atree.Digest(i % 128)}
			digesterBuilder.On("Digest", k).Return(mockDigester{digests})
		}

		for k, v := range collisionKeyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.Equal(t, 1, errorCategorizationCount(err))
			var fatalError *atree.FatalError
			var collisionLimitError *atree.CollisionLimitError
			require.ErrorAs(t, err, &fatalError)
			require.ErrorAs(t, err, &collisionLimitError)
			require.ErrorAs(t, fatalError, &collisionLimitError)
			require.Nil(t, existingStorable)
		}

		// Verify that no new elements exceeding collision limit inserted
		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)

		// Update elements within collision limits
		for k := range keyValues {
			v := test_utils.Uint64Value(0)
			keyValues[k] = v
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.NotNil(t, existingStorable)
		}

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapLoadedValueIterator(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	runTest := func(name string, f func(useWrapperValue bool) func(*testing.T)) {
		for _, useWrapperValue := range []bool{false, true} {
			if useWrapperValue {
				name += ", use wrapper value"
			}

			t.Run(name, f(useWrapperValue))
		}
	}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		digesterBuilder := &mockDigesterBuilder{}

		m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		// parent map: 1 root data slab
		require.Equal(t, 1, GetDeltasCount(storage))
		require.Equal(t, 0, getMapMetaDataSlabCount(storage))

		testMapLoadedElements(t, m, nil)
	})

	runTest("root data slab with simple values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues := createMapWithSimpleValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			require.Equal(t, 1, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root data slab with composite values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root data slab with composite values in collision group", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 collision groups, 2 elements in each group.
			const mapCount = 6
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 2), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root data slab with composite values in external collision group", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 external collision group, 4 elements in the group.
			const mapCount = 12
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 4), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab, 3 external collision group
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+3+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root data slab with composite values, unload value from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from front to back.
			for i, slabID := range childSlabIDs {
				err := storage.Remove(slabID)
				require.NoError(t, err)

				testMapLoadedElements(t, m, expectedValues[i+1:])
			}
		}
	})

	runTest("root data slab with long string keys, unload key from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues := createMapWithLongStringKey(t, storage, address, typeInfo, mapCount, useWrapperValue)

			// parent map: 1 root data slab
			// long string keys: 1 storable slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload external key from front to back.
			for i := range expectedValues {
				k := expectedValues[i][0]

				s, ok := k.(test_utils.StringValue)
				require.True(t, ok)

				// Find storage id for StringValue s.
				var keyID atree.SlabID
				deltas := atree.GetDeltas(storage)
				for id, slab := range deltas {
					if sslab, ok := slab.(*atree.StorableSlab); ok {
						if other, ok := sslab.ChildStorables()[0].(test_utils.StringValue); ok {
							if s.Equal(other) {
								keyID = id
								break
							}
						}
					}
				}

				require.NoError(t, keyID.Valid())

				err := storage.Remove(keyID)
				require.NoError(t, err)

				testMapLoadedElements(t, m, expectedValues[i+1:])
			}
		}
	})

	runTest("root data slab with composite values in collision group, unload value from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 collision groups, 2 elements in each group.
			const mapCount = 6
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 2), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from front to back.
			for i, childSlabID := range childSlabIDs {
				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				testMapLoadedElements(t, m, expectedValues[i+1:])
			}
		}
	})

	runTest("root data slab with composite values in external collision group, unload value from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 external collision groups, 4 elements in the group.
			const mapCount = 12
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 4), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab, 3 external collision group
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+3+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from front to back
			for i, childSlabID := range childSlabIDs {
				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				testMapLoadedElements(t, m, expectedValues[i+1:])
			}
		}
	})

	runTest("root data slab with composite values in external collision group, unload external slab from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 external collision groups, 4 elements in the group.
			const mapCount = 12
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 4), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab, 3 external collision group
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+3+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload external collision group slab from front to back

			var externalCollisionSlabIDs []atree.SlabID
			deltas := atree.GetDeltas(storage)
			for id, slab := range deltas {
				if dataSlab, ok := slab.(*atree.MapDataSlab); ok {
					if atree.IsMapDataSlabCollisionGroup(dataSlab) {
						externalCollisionSlabIDs = append(externalCollisionSlabIDs, id)
					}
				}
			}
			require.Equal(t, 3, len(externalCollisionSlabIDs))

			sort.Slice(externalCollisionSlabIDs, func(i, j int) bool {
				a := externalCollisionSlabIDs[i]
				b := externalCollisionSlabIDs[j]
				if a.Address() == b.Address() {
					return a.IndexAsUint64() < b.IndexAsUint64()
				}
				return a.AddressAsUint64() < b.AddressAsUint64()
			})

			for i, id := range externalCollisionSlabIDs {
				err := storage.Remove(id)
				require.NoError(t, err)

				expectedValues := expectedValues[i*4+4:]
				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root data slab with composite values, unload composite value from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from back to front.
			for i := len(expectedValues) - 1; i >= 0; i-- {
				err := storage.Remove(childSlabIDs[i])
				require.NoError(t, err)

				expectedValues := expectedValues[:i]
				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root data slab with long string key, unload key from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues := createMapWithLongStringKey(t, storage, address, typeInfo, mapCount, useWrapperValue)

			// parent map: 1 root data slab
			// long string keys: 1 storable slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from front to back.
			for i := len(expectedValues) - 1; i >= 0; i-- {
				k := expectedValues[i][0]

				s, ok := k.(test_utils.StringValue)
				require.True(t, ok)

				// Find storage id for StringValue s.
				var keyID atree.SlabID
				deltas := atree.GetDeltas(storage)
				for id, slab := range deltas {
					if sslab, ok := slab.(*atree.StorableSlab); ok {
						if other, ok := sslab.ChildStorables()[0].(test_utils.StringValue); ok {
							if s.Equal(other) {
								keyID = id
								break
							}
						}
					}
				}

				require.NoError(t, keyID.Valid())

				err := storage.Remove(keyID)
				require.NoError(t, err)

				expectedValues := expectedValues[:i]
				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root data slab with composite values in collision group, unload value from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 collision groups, 2 elements in each group.
			const mapCount = 6
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 2), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from back to front
			for i := len(expectedValues) - 1; i >= 0; i-- {
				err := storage.Remove(childSlabIDs[i])
				require.NoError(t, err)

				expectedValues := expectedValues[:i]
				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root data slab with composite values in external collision group, unload value from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 external collision groups, 4 elements in the group.
			const mapCount = 12
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 4), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab, 3 external collision group
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+3+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from back to front
			for i := len(expectedValues) - 1; i >= 0; i-- {
				err := storage.Remove(childSlabIDs[i])
				require.NoError(t, err)

				expectedValues := expectedValues[:i]
				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root data slab with composite values in external collision group, unload external slab from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 external collision groups, 4 elements in the group.
			const mapCount = 12
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 4), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab, 3 external collision group
			// composite elements: 1 root data slab for each
			require.Equal(t, 1+3+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload external slabs from back to front
			var externalCollisionSlabIDs []atree.SlabID
			deltas := atree.GetDeltas(storage)
			for id, slab := range deltas {
				if dataSlab, ok := slab.(*atree.MapDataSlab); ok {
					if atree.IsMapDataSlabCollisionGroup(dataSlab) {
						externalCollisionSlabIDs = append(externalCollisionSlabIDs, id)
					}
				}
			}
			require.Equal(t, 3, len(externalCollisionSlabIDs))

			sort.Slice(externalCollisionSlabIDs, func(i, j int) bool {
				a := externalCollisionSlabIDs[i]
				b := externalCollisionSlabIDs[j]
				if a.Address() == b.Address() {
					return a.IndexAsUint64() < b.IndexAsUint64()
				}
				return a.AddressAsUint64() < b.AddressAsUint64()
			})

			for i := len(externalCollisionSlabIDs) - 1; i >= 0; i-- {
				err := storage.Remove(externalCollisionSlabIDs[i])
				require.NoError(t, err)

				expectedValues := expectedValues[:i*4]
				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root data slab with composite values, unload value in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload value in the middle
			unloadValueIndex := 1

			err := storage.Remove(childSlabIDs[unloadValueIndex])
			require.NoError(t, err)

			copy(expectedValues[unloadValueIndex:], expectedValues[unloadValueIndex+1:])
			expectedValues = expectedValues[:len(expectedValues)-1]

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root data slab with long string key, unload key in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues := createMapWithLongStringKey(t, storage, address, typeInfo, mapCount, useWrapperValue)

			// parent map: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload key in the middle.
			unloadValueIndex := 1

			k := expectedValues[unloadValueIndex][0]

			s, ok := k.(test_utils.StringValue)
			require.True(t, ok)

			// Find storage id for StringValue s.
			var keyID atree.SlabID
			deltas := atree.GetDeltas(storage)
			for id, slab := range deltas {
				if sslab, ok := slab.(*atree.StorableSlab); ok {
					if other, ok := sslab.ChildStorables()[0].(test_utils.StringValue); ok {
						if s.Equal(other) {
							keyID = id
							break
						}
					}
				}
			}

			require.NoError(t, keyID.Valid())

			err := storage.Remove(keyID)
			require.NoError(t, err)

			copy(expectedValues[unloadValueIndex:], expectedValues[unloadValueIndex+1:])
			expectedValues = expectedValues[:len(expectedValues)-1]

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root data slab with composite values in collision group, unload value in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 collision groups, 2 elements in each group.
			const mapCount = 6
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 2), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element in the middle
			for _, unloadValueIndex := range []int{1, 3, 5} {
				err := storage.Remove(childSlabIDs[unloadValueIndex])
				require.NoError(t, err)
			}

			testMapLoadedElements(
				t,
				m,
				[][2]atree.Value{
					expectedValues[0],
					expectedValues[2],
					expectedValues[4],
				})
		}
	})

	runTest("root data slab with composite values in external collision group, unload value in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 external collision groups, 4 elements in the group.
			const mapCount = 12
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 4), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab, 3 external collision group
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+3+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite value in the middle.
			for _, unloadValueIndex := range []int{1, 3, 5, 7, 9, 11} {
				err := storage.Remove(childSlabIDs[unloadValueIndex])
				require.NoError(t, err)
			}

			testMapLoadedElements(
				t,
				m,
				[][2]atree.Value{
					expectedValues[0],
					expectedValues[2],
					expectedValues[4],
					expectedValues[6],
					expectedValues[8],
					expectedValues[10],
				})
		}
	})

	runTest("root data slab with composite values in external collision group, unload external slab in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			// Create parent map with 3 external collision groups, 4 elements in the group.
			const mapCount = 12
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i / 4), atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab, 3 external collision group
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+3+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload external slabs in the middle.
			var externalCollisionSlabIDs []atree.SlabID
			deltas := atree.GetDeltas(storage)
			for id, slab := range deltas {
				if dataSlab, ok := slab.(*atree.MapDataSlab); ok {
					if atree.IsMapDataSlabCollisionGroup(dataSlab) {
						externalCollisionSlabIDs = append(externalCollisionSlabIDs, id)
					}
				}
			}
			require.Equal(t, 3, len(externalCollisionSlabIDs))

			sort.Slice(externalCollisionSlabIDs, func(i, j int) bool {
				a := externalCollisionSlabIDs[i]
				b := externalCollisionSlabIDs[j]
				if a.Address() == b.Address() {
					return a.IndexAsUint64() < b.IndexAsUint64()
				}
				return a.AddressAsUint64() < b.AddressAsUint64()
			})

			id := externalCollisionSlabIDs[1]
			err := storage.Remove(id)
			require.NoError(t, err)

			copy(expectedValues[4:], expectedValues[8:])
			expectedValues = expectedValues[:8]

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root data slab with composite values, unload composite elements during iteration", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 3
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map: 1 root data slab
			// nested composite elements: 1 root data slab for each
			require.Equal(t, 1+mapCount, GetDeltasCount(storage))
			require.Equal(t, 0, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			i := 0
			err := m.IterateReadOnlyLoadedValues(func(k atree.Value, v atree.Value) (bool, error) {
				// At this point, iterator returned first element (v).

				// Remove all other nested composite elements (except first element) from storage.
				for _, slabID := range childSlabIDs[1:] {
					err := storage.Remove(slabID)
					require.NoError(t, err)
				}

				require.Equal(t, 0, i)
				testValueEqual(t, expectedValues[0][0], k)
				testValueEqual(t, expectedValues[0][1], v)
				i++
				return true, nil
			})

			require.NoError(t, err)
			require.Equal(t, 1, i) // Only first element is iterated because other elements are remove during iteration.
		}
	})

	runTest("root data slab with simple and composite values, unloading composite value", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			const mapCount = 3

			// Create a map with nested composite value at specified index
			for childArrayIndex := range mapCount {
				storage := newTestPersistentStorage(t)

				m, expectedValues, childSlabID := createMapWithSimpleAndChildArrayValues(
					t,
					storage,
					address,
					typeInfo,
					mapCount,
					childArrayIndex,
					func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
					useWrapperValue,
				)

				// parent map: 1 root data slab
				// composite element: 1 root data slab
				require.Equal(t, 2, GetDeltasCount(storage))
				require.Equal(t, 0, getMapMetaDataSlabCount(storage))

				testMapLoadedElements(t, m, expectedValues)

				// Unload composite value
				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				copy(expectedValues[childArrayIndex:], expectedValues[childArrayIndex+1:])
				expectedValues = expectedValues[:len(expectedValues)-1]

				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab with simple values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20
			m, expectedValues := createMapWithSimpleValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root metadata slab with composite values", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			// composite values: 1 root data slab for each
			require.Equal(t, 4+mapCount, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root metadata slab with composite values, unload value from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			// composite values : 1 root data slab for each
			require.Equal(t, 4+mapCount, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from front to back
			for i, childSlabID := range childSlabIDs {
				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				testMapLoadedElements(t, m, expectedValues[i+1:])
			}
		}
	})

	runTest("root metadata slab with composite values, unload values from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			// composite values: 1 root data slab for each
			require.Equal(t, 4+mapCount, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element from back to front
			for i := len(expectedValues) - 1; i >= 0; i-- {
				err := storage.Remove(childSlabIDs[i])
				require.NoError(t, err)

				expectedValues := expectedValues[:i]
				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab with composite values, unload value in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			// composite values: 1 root data slab for each
			require.Equal(t, 4+mapCount, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			// Unload composite element in the middle
			for _, index := range []int{4, 14} {
				err := storage.Remove(childSlabIDs[index])
				require.NoError(t, err)

				copy(expectedValues[index:], expectedValues[index+1:])
				expectedValues = expectedValues[:len(expectedValues)-1]

				copy(childSlabIDs[index:], childSlabIDs[index+1:])
				childSlabIDs = childSlabIDs[:len(childSlabIDs)-1]

				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab with simple and composite values, unload composite value", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			const mapCount = 20

			// Create a map with nested composite value at specified index
			for childArrayIndex := range mapCount {
				storage := newTestPersistentStorage(t)

				m, expectedValues, childSlabID := createMapWithSimpleAndChildArrayValues(
					t,
					storage,
					address,
					typeInfo,
					mapCount,
					childArrayIndex,
					func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
					useWrapperValue,
				)

				// parent map (2 levels): 1 root metadata slab, 3 data slabs
				// composite values: 1 root data slab for each
				require.Equal(t, 5, GetDeltasCount(storage))
				require.Equal(t, 1, getMapMetaDataSlabCount(storage))

				testMapLoadedElements(t, m, expectedValues)

				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				copy(expectedValues[childArrayIndex:], expectedValues[childArrayIndex+1:])
				expectedValues = expectedValues[:len(expectedValues)-1]

				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab, unload data slab from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20

			m, expectedValues := createMapWithSimpleValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			rootMetaDataSlab, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _, _ := atree.GetMapMetaDataSlabChildInfo(rootMetaDataSlab)

			// Unload data slabs from front to back
			for _, slabID := range childSlabIDs {

				// Get data slab element count before unload it from storage.
				// Element count isn't in the header.
				mapDataSlab, ok := atree.GetDeltas(storage)[slabID].(*atree.MapDataSlab)
				require.True(t, ok)

				count := atree.GetMapDataSlabElementCount(mapDataSlab)

				err := storage.Remove(slabID)
				require.NoError(t, err)

				expectedValues = expectedValues[count:]

				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab, unload data slab from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20

			m, expectedValues := createMapWithSimpleValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			rootMetaDataSlab, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _, _ := atree.GetMapMetaDataSlabChildInfo(rootMetaDataSlab)

			// Unload data slabs from back to front
			for i := len(childSlabIDs) - 1; i >= 0; i-- {

				slabID := childSlabIDs[i]

				// Get data slab element count before unload it from storage
				// Element count isn't in the header.
				mapDataSlab, ok := atree.GetDeltas(storage)[slabID].(*atree.MapDataSlab)
				require.True(t, ok)

				count := atree.GetMapDataSlabElementCount(mapDataSlab)

				err := storage.Remove(slabID)
				require.NoError(t, err)

				expectedValues = expectedValues[:len(expectedValues)-int(count)]

				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab, unload data slab in the middle", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 20

			m, expectedValues := createMapWithSimpleValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (2 levels): 1 root metadata slab, 3 data slabs
			require.Equal(t, 4, GetDeltasCount(storage))
			require.Equal(t, 1, getMapMetaDataSlabCount(storage))

			testMapLoadedElements(t, m, expectedValues)

			rootMetaDataSlab, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _, _ := atree.GetMapMetaDataSlabChildInfo(rootMetaDataSlab)

			require.True(t, len(childSlabIDs) > 2)

			const prevIndex = 0
			const index = prevIndex + 1

			prevSlabID := childSlabIDs[prevIndex]
			slabID := childSlabIDs[index]

			// Get element count from previous data slab
			mapDataSlab, ok := atree.GetDeltas(storage)[prevSlabID].(*atree.MapDataSlab)
			require.True(t, ok)

			countAtIndex0 := atree.GetMapDataSlabElementCount(mapDataSlab)

			// Get element count from slab to be unloaded
			mapDataSlab, ok = atree.GetDeltas(storage)[slabID].(*atree.MapDataSlab)
			require.True(t, ok)

			countAtIndex1 := atree.GetMapDataSlabElementCount(mapDataSlab)

			err := storage.Remove(slabID)
			require.NoError(t, err)

			copy(expectedValues[countAtIndex0:], expectedValues[countAtIndex0+countAtIndex1:])
			expectedValues = expectedValues[:m.Count()-uint64(countAtIndex1)]

			testMapLoadedElements(t, m, expectedValues)
		}
	})

	runTest("root metadata slab, unload non-root metadata slab from front to back", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 200

			m, expectedValues := createMapWithSimpleValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (3 levels): 1 root metadata slab, 3 child metadata slabs, n data slabs
			require.Equal(t, 4, getMapMetaDataSlabCount(storage))

			rootMetaDataSlab, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _, childFirstKeys := atree.GetMapMetaDataSlabChildInfo(rootMetaDataSlab)

			// Unload non-root metadata slabs from front to back.
			for i, childSlabID := range childSlabIDs {
				err := storage.Remove(childSlabID)
				require.NoError(t, err)

				// Use firstKey to deduce number of elements in slab.
				var loadedExpectedValues [][2]atree.Value
				if i < len(childSlabIDs)-1 {
					nextFirstKey := childFirstKeys[i+1]
					loadedExpectedValues = expectedValues[int(nextFirstKey):] //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				}

				testMapLoadedElements(t, m, loadedExpectedValues)
			}
		}
	})

	runTest("root metadata slab, unload non-root metadata slab from back to front", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {
			storage := newTestPersistentStorage(t)

			const mapCount = 200

			m, expectedValues := createMapWithSimpleValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (3 levels): 1 root metadata slab, 3 child metadata slabs, n data slabs
			require.Equal(t, 4, getMapMetaDataSlabCount(storage))

			rootMetaDataSlab, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _, childFirstKeys := atree.GetMapMetaDataSlabChildInfo(rootMetaDataSlab)

			// Unload non-root metadata slabs from back to front.
			for i := len(childSlabIDs) - 1; i >= 0; i-- {
				slabID := childSlabIDs[i]
				firstKey := childFirstKeys[i]

				err := storage.Remove(slabID)
				require.NoError(t, err)

				// Use firstKey to deduce number of elements in slabs.
				expectedValues = expectedValues[:firstKey]

				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab with composite values, unload composite value at random index", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {

			storage := newTestPersistentStorage(t)

			const mapCount = 500
			m, expectedValues, childSlabIDs := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// nested composite elements: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+mapCount)
			require.True(t, getMapMetaDataSlabCount(storage) > 1)

			testMapLoadedElements(t, m, expectedValues)

			r := newRand(t)

			// Unload composite element in random position
			for len(expectedValues) > 0 {

				i := r.Intn(len(expectedValues))

				err := storage.Remove(childSlabIDs[i])
				require.NoError(t, err)

				copy(expectedValues[i:], expectedValues[i+1:])
				expectedValues = expectedValues[:len(expectedValues)-1]

				copy(childSlabIDs[i:], childSlabIDs[i+1:])
				childSlabIDs = childSlabIDs[:len(childSlabIDs)-1]

				testMapLoadedElements(t, m, expectedValues)
			}
		}
	})

	runTest("root metadata slab with composite values, unload random data slab", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {

			storage := newTestPersistentStorage(t)

			const mapCount = 500
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// composite values: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+mapCount)
			require.True(t, getMapMetaDataSlabCount(storage) > 1)

			testMapLoadedElements(t, m, expectedValues)

			rootMetaDataSlab, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _, _ := atree.GetMapMetaDataSlabChildInfo(rootMetaDataSlab)

			type slabInfo struct {
				id         atree.SlabID
				startIndex int
				count      int
			}

			var dataSlabInfos []*slabInfo
			for _, slabID := range childSlabIDs {

				nonRootMetaDataSlab, ok := atree.GetDeltas(storage)[slabID].(*atree.MapMetaDataSlab)
				require.True(t, ok)

				nonrootSlabIDs, _, nonrootFirstKeys := atree.GetMapMetaDataSlabChildInfo(nonRootMetaDataSlab)

				for i := range nonrootSlabIDs {

					slabID := nonrootSlabIDs[i]
					firstKey := nonrootFirstKeys[i]

					if len(dataSlabInfos) > 0 {
						// Update previous slabInfo.count
						dataSlabInfos[len(dataSlabInfos)-1].count = int(firstKey) - dataSlabInfos[len(dataSlabInfos)-1].startIndex //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
					}

					dataSlabInfos = append(dataSlabInfos, &slabInfo{id: slabID, startIndex: int(firstKey)}) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
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
					expectedValues = expectedValues[:slabToBeRemoved.startIndex]
				} else {
					copy(expectedValues[slabToBeRemoved.startIndex:], expectedValues[slabToBeRemoved.startIndex+slabToBeRemoved.count:])
					expectedValues = expectedValues[:len(expectedValues)-slabToBeRemoved.count]
				}

				copy(dataSlabInfos[index:], dataSlabInfos[index+1:])
				dataSlabInfos = dataSlabInfos[:len(dataSlabInfos)-1]

				testMapLoadedElements(t, m, expectedValues)
			}

			require.Equal(t, 0, len(expectedValues))
		}
	})

	runTest("root metadata slab with composite values, unload random slab", func(useWrapperValue bool) func(t *testing.T) {
		return func(t *testing.T) {

			storage := newTestPersistentStorage(t)

			const mapCount = 500
			m, expectedValues, _ := createMapWithChildArrayValues(
				t,
				storage,
				address,
				typeInfo,
				mapCount,
				func(i int) []atree.Digest { return []atree.Digest{atree.Digest(i)} }, //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				useWrapperValue,
			)

			// parent map (3 levels): 1 root metadata slab, n non-root metadata slabs, n data slabs
			// composite values: 1 root data slab for each
			require.True(t, GetDeltasCount(storage) > 1+mapCount)
			require.True(t, getMapMetaDataSlabCount(storage) > 1)

			testMapLoadedElements(t, m, expectedValues)

			type slabInfo struct {
				id         atree.SlabID
				startIndex int
				count      int
				children   []*slabInfo
			}

			rootMetaDataSlab, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
			require.True(t, ok)

			childSlabIDs, _, childFirstKeys := atree.GetMapMetaDataSlabChildInfo(rootMetaDataSlab)

			metadataSlabInfos := make([]*slabInfo, len(childSlabIDs))
			for i, slabID := range childSlabIDs {

				firstKey := childFirstKeys[i]

				if i > 0 {
					prevMetaDataSlabInfo := metadataSlabInfos[i-1]
					prevDataSlabInfo := prevMetaDataSlabInfo.children[len(prevMetaDataSlabInfo.children)-1]

					// Update previous metadata slab count
					prevMetaDataSlabInfo.count = int(firstKey) - prevMetaDataSlabInfo.startIndex //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

					// Update previous data slab count
					prevDataSlabInfo.count = int(firstKey) - prevDataSlabInfo.startIndex //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				}

				metadataSlabInfo := &slabInfo{
					id:         slabID,
					startIndex: int(firstKey), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				}

				nonRootMetadataSlab, ok := atree.GetDeltas(storage)[slabID].(*atree.MapMetaDataSlab)
				require.True(t, ok)

				nonrootSlabIDs, _, nonrootFirstKeys := atree.GetMapMetaDataSlabChildInfo(nonRootMetadataSlab)

				children := make([]*slabInfo, len(nonrootSlabIDs))
				for i, slabID := range nonrootSlabIDs {
					firstKey := nonrootFirstKeys[i]

					children[i] = &slabInfo{
						id:         slabID,
						startIndex: int(firstKey), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
					}
					if i > 0 {
						children[i-1].count = int(firstKey) - children[i-1].startIndex //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
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
					for _, slabInfo := range metadataSlabInfos[metadataSlabIndex+1:] {
						slabInfo.startIndex -= count

						for _, childSlabInfo := range slabInfo.children {
							childSlabInfo.startIndex -= count
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
					for _, childSlabInfo := range metadataSlabInfo.children[dataSlabIndex+1:] {
						childSlabInfo.startIndex -= count
					}

					copy(metadataSlabInfo.children[dataSlabIndex:], metadataSlabInfo.children[dataSlabIndex+1:])
					metadataSlabInfo.children = metadataSlabInfo.children[:len(metadataSlabInfo.children)-1]

					metadataSlabInfo.count -= count

					// Update startIndex for all subsequence metadata slabs.
					for _, slabInfo := range metadataSlabInfos[metadataSlabIndex+1:] {
						slabInfo.startIndex -= count

						for _, childSlabInfo := range slabInfo.children {
							childSlabInfo.startIndex -= count
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
					expectedValues = expectedValues[:slabInfoToBeRemoved.startIndex]
				} else {
					copy(expectedValues[slabInfoToBeRemoved.startIndex:], expectedValues[slabInfoToBeRemoved.startIndex+slabInfoToBeRemoved.count:])
					expectedValues = expectedValues[:len(expectedValues)-slabInfoToBeRemoved.count]
				}

				testMapLoadedElements(t, m, expectedValues)
			}

			require.Equal(t, 0, len(expectedValues))
		}
	})
}

func createMapWithLongStringKey(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	count int,
	useWrapperValue bool,
) (*atree.OrderedMap, [][2]atree.Value) {

	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map.
	m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	expectedValues := make([][2]atree.Value, count)
	r := 'a'
	for i := range expectedValues {
		s := strings.Repeat(string(r), int(atree.MaxInlineMapElementSize())) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		k := test_utils.NewStringValue(s)
		v := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		digests := []atree.Digest{atree.Digest(i)} //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		if useWrapperValue {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.NewSomeValue(v))
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[i] = [2]atree.Value{k, test_utils.NewExpectedWrapperValue(v)}
		} else {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[i] = [2]atree.Value{k, v}
		}

		r++
	}

	return m, expectedValues
}

func createMapWithSimpleValues(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	count int,
	newDigests func(i int) []atree.Digest,
	useWrapperValue bool,
) (*atree.OrderedMap, [][2]atree.Value) {

	digesterBuilder := &mockDigesterBuilder{}

	m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	expectedValues := make([][2]atree.Value, count)
	r := rune('a')
	for i := range expectedValues {
		k := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		v := test_utils.NewStringValue(strings.Repeat(string(r), 20))

		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		if useWrapperValue {
			expectedValues[i] = [2]atree.Value{k, test_utils.NewExpectedWrapperValue(v)}

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.NewSomeValue(v))
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		} else {
			expectedValues[i] = [2]atree.Value{k, v}

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}
	}

	return m, expectedValues
}

func createMapWithChildArrayValues(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	count int,
	newDigests func(i int) []atree.Digest,
	useWrapperValue bool,
) (*atree.OrderedMap, [][2]atree.Value, []atree.SlabID) {
	const childArrayCount = 50

	// Use mockDigesterBuilder to guarantee element order.
	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map
	m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	slabIDs := make([]atree.SlabID, count)
	expectedValues := make([][2]atree.Value, count)
	for i := range expectedValues {
		// Create child array
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		expectedChildValues := make([]atree.Value, childArrayCount)
		for j := range expectedChildValues {
			v := test_utils.Uint64Value(j) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

			err = childArray.Append(v)
			require.NoError(t, err)

			expectedChildValues[j] = v
		}

		k := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		v := childArray

		slabIDs[i] = childArray.SlabID()

		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		// Set child array to parent
		if useWrapperValue {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.NewSomeValue(v))
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[i] = [2]atree.Value{k, test_utils.NewExpectedWrapperValue(test_utils.ExpectedArrayValue(expectedChildValues))}
		} else {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedValues[i] = [2]atree.Value{k, test_utils.ExpectedArrayValue(expectedChildValues)}
		}
	}

	return m, expectedValues, slabIDs
}

func createMapWithSimpleAndChildArrayValues(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	count int,
	compositeValueIndex int,
	newDigests func(i int) []atree.Digest,
	useWrapperValue bool,
) (*atree.OrderedMap, [][2]atree.Value, atree.SlabID) {
	const childArrayCount = 50

	digesterBuilder := &mockDigesterBuilder{}

	// Create parent map
	m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	var slabID atree.SlabID
	expectedValues := make([][2]atree.Value, count)
	r := 'a'
	for i := range expectedValues {

		k := test_utils.Uint64Value(i) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests

		digests := newDigests(i)
		digesterBuilder.On("Digest", k).Return(mockDigester{digests})

		if compositeValueIndex == i {
			// Create child array with one element
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			expectedChildValues := make([]atree.Value, childArrayCount)
			for j := range expectedChildValues {
				v := test_utils.Uint64Value(j) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				err = childArray.Append(v)
				require.NoError(t, err)

				expectedChildValues[j] = v
			}

			if useWrapperValue {
				existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.NewSomeValue(childArray))
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedValues[i] = [2]atree.Value{k, test_utils.NewExpectedWrapperValue(test_utils.ExpectedArrayValue(expectedChildValues))}
			} else {
				existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedValues[i] = [2]atree.Value{k, test_utils.ExpectedArrayValue(expectedChildValues)}
			}

			slabID = childArray.SlabID()

		} else {
			v := test_utils.NewStringValue(strings.Repeat(string(r), 18))
			expectedValues[i] = [2]atree.Value{k, v}

			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}
	}

	return m, expectedValues, slabID
}

func testMapLoadedElements(t *testing.T, m *atree.OrderedMap, expectedValues [][2]atree.Value) {
	i := 0
	err := m.IterateReadOnlyLoadedValues(func(k atree.Value, v atree.Value) (bool, error) {
		require.True(t, i < len(expectedValues))
		testValueEqual(t, expectedValues[i][0], k)
		testValueEqual(t, expectedValues[i][1], v)
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(expectedValues), i)
}

func getMapMetaDataSlabCount(storage *atree.PersistentSlabStorage) int {
	var counter int
	deltas := atree.GetDeltas(storage)
	for _, slab := range deltas {
		if _, ok := slab.(*atree.MapMetaDataSlab); ok {
			counter++
		}
	}
	return counter
}

func TestMaxInlineMapValueSize(t *testing.T) {

	t.Run("small key", func(t *testing.T) {
		// atree.Value has larger max inline size when key is less than max map key size.

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		mapCount := 2
		keyStringSize := 16                                       // Key size is less than max map key size.
		valueStringSize := atree.MaxInlineMapElementSize()/2 + 10 // atree.Value size is more than half of max map element size.

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, int(valueStringSize))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			keyValues[k] = v
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Both key and value are stored in map slab.
		require.Equal(t, 1, GetDeltasCount(storage))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("max size key", func(t *testing.T) {
		// atree.Value max size is about half of max map element size when key is exactly max map key size.

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		mapCount := 1
		keyStringSize := atree.MaxInlineMapKeySize() - 2         // Key size is exactly max map key size (2 bytes is string encoding overhead).
		valueStringSize := atree.MaxInlineMapElementSize()/2 + 2 // atree.Value size is more than half of max map element size (add 2 bytes to make it more than half).

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, int(keyStringSize)))   //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			v := test_utils.NewStringValue(randStr(r, int(valueStringSize))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			keyValues[k] = v
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Key is stored in map slab, while value is stored separately in storable slab.
		require.Equal(t, 2, GetDeltasCount(storage))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})

	t.Run("large key", func(t *testing.T) {
		// atree.Value has larger max inline size when key is more than max map key size because
		// when key size exceeds max map key size, it is stored in a separate storable slab,
		// and atree.SlabIDStorable is stored as key in the map, which is 19 bytes.

		atree.SetThreshold(256)
		defer atree.SetThreshold(1024)

		mapCount := 1
		keyStringSize := atree.MaxInlineMapKeySize() + 10         // key size is more than max map key size
		valueStringSize := atree.MaxInlineMapElementSize()/2 + 10 // value size is more than half of max map element size

		r := newRand(t)

		keyValues := make(map[atree.Value]atree.Value, mapCount)
		for len(keyValues) < mapCount {
			k := test_utils.NewStringValue(randStr(r, int(keyStringSize)))   //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			v := test_utils.NewStringValue(randStr(r, int(valueStringSize))) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			keyValues[k] = v
		}

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		for k, v := range keyValues {
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		// Key is stored in separate storable slabs, while value is stored in map slab.
		require.Equal(t, 2, GetDeltasCount(storage))

		testMap(t, storage, typeInfo, address, m, keyValues, nil, false)
	})
}

func TestMapID(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	slabID := m.SlabID()
	valueID := m.ValueID()

	testEqualValueIDAndSlabID(t, slabID, valueID)
}

func TestSlabSizeWhenResettingMutableStorableInMap(t *testing.T) {
	const (
		mapCount            = uint64(3)
		keyStringSize       = 16
		initialStorableSize = 1
		mutatedStorableSize = 5
	)

	keyValues := make(map[atree.Value]*test_utils.MutableValue, mapCount)
	elementByteSizes := make([][2]uint32, mapCount)
	for i := range mapCount {
		k := test_utils.Uint64Value(i)
		v := test_utils.NewMutableValue(initialStorableSize)
		keyValues[k] = v
		elementByteSizes[i] = [2]uint32{k.ByteSize(), initialStorableSize}
	}

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
	storage := newTestPersistentStorage(t)

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	for k, v := range keyValues {
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, existingStorable)
	}

	require.True(t, IsMapRootDataSlab(m))

	expectedMapRootDataSlabSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
	require.Equal(t, expectedMapRootDataSlabSize, GetMapRootSlabByteSize(m))

	err = atree.VerifyMap(m, address, typeInfo, test_utils.CompareTypeInfo, test_utils.GetHashInput, true)
	require.NoError(t, err)

	// Reset mutable values after changing its storable size
	elementByteSizes = elementByteSizes[:0]
	for k, v := range keyValues {
		v.UpdateStorableSize(mutatedStorableSize)

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
		require.NoError(t, err)
		require.NotNil(t, existingStorable)

		ks, err := k.Storable(storage, address, atree.MaxInlineMapKeySize())
		require.NoError(t, err)
		elementByteSizes = append(elementByteSizes, [2]uint32{ks.ByteSize(), mutatedStorableSize})
	}

	require.True(t, IsMapRootDataSlab(m))

	expectedMapRootDataSlabSize = atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
	require.Equal(t, expectedMapRootDataSlabSize, GetMapRootSlabByteSize(m))

	err = atree.VerifyMap(m, address, typeInfo, test_utils.CompareTypeInfo, test_utils.GetHashInput, true)
	require.NoError(t, err)
}

func TestChildMapInlinabilityInParentMap(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("parent is root data slab, with one child map", func(t *testing.T) {
		const (
			mapCount        = 1
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := test_utils.NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentMap, expectedKeyValues, elementByteSizesByKey := createMapWithEmptyChildMap(t, storage, address, typeInfo, mapCount, func() atree.Value {
			return test_utils.NewStringValue(randStr(r, keyStringSize))
		})

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)

		// Appending 3 elements to child map so that inlined child map reaches max inlined size as map element.
		for i := range uint64(3) {
			for childKey, child := range children {
				childMap := child.m
				valueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
				require.True(t, ok)

				k := test_utils.NewStringValue(randStr(r, keyStringSize))
				v := test_utils.NewStringValue(randStr(r, valueStringSize))

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
				require.Equal(t, i+1, childMap.Count())

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childMap.ValueID())              // atree.Value ID is unchanged
				require.Equal(t, 1, getStoredDeltas(storage))

				// Test inlined child slab size
				expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(childMap.Count())) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

				// Test parent slab size
				elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], expectedInlinedMapSize}

				elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
				for _, v := range elementByteSizesByKey {
					elementByteSizes = append(elementByteSizes, v)
				}
				expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
				require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		i := 0
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			require.False(t, childMap.Inlined())
			require.Equal(t, 1+1+i, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.

			i++

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childMap.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childMap.ValueID())       // atree.Value ID is unchanged

			expectedStandaloneSlabSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedStandaloneSlabSize, GetMapRootSlabByteSize(childMap))

			elementByteSizesByKey[childKey] = [2]uint32{
				elementByteSizesByKey[childKey][0],
				slabIDStorableByteSize,
			}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}
			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Remove elements from child map which triggers standalone map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			keys := make([]atree.Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
				require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged
				require.Equal(t, 1, getStoredDeltas(storage))

				expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(childMap.Count())) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

				elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], expectedInlinedMapSize}

				elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
				for _, v := range elementByteSizesByKey {
					elementByteSizes = append(elementByteSizes, v)
				}
				expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
				require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.
	})

	t.Run("parent is root data slab, with two child maps", func(t *testing.T) {
		const (
			mapCount        = 2
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := test_utils.NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentMap, expectedKeyValues, elementByteSizesByKey := createMapWithEmptyChildMap(t, storage, address, typeInfo, mapCount, func() atree.Value {
			return test_utils.NewStringValue(randStr(r, keyStringSize))
		})

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)

		// Appending 3 elements to child map so that inlined child map reaches max inlined size as map element.
		for i := range uint64(3) {
			for childKey, child := range children {
				childMap := child.m
				valueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
				require.True(t, ok)

				k := test_utils.NewStringValue(randStr(r, keyStringSize))
				v := test_utils.NewStringValue(randStr(r, valueStringSize))

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
				require.Equal(t, i+1, childMap.Count())

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childMap.ValueID())              // atree.Value ID is unchanged
				require.Equal(t, 1, getStoredDeltas(storage))

				// Test inlined child slab size
				expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

				// Test parent slab size
				elementByteSizesByKey[childKey] = [2]uint32{
					elementByteSizesByKey[childKey][0],
					expectedInlinedMapSize}

				elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
				for _, v := range elementByteSizesByKey {
					elementByteSizes = append(elementByteSizes, v)
				}
				expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
				require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		i := 0
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			require.False(t, childMap.Inlined())
			require.Equal(t, 1+1+i, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.

			i++

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childMap.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childMap.ValueID())       // atree.Value ID is unchanged

			expectedStandaloneSlabSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedStandaloneSlabSize, GetMapRootSlabByteSize(childMap))

			elementByteSizesByKey[childKey] = [2]uint32{
				elementByteSizesByKey[childKey][0],
				slabIDStorableByteSize,
			}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}
			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.Equal(t, 1+mapCount, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.

		// Remove one element from each child map which triggers standalone map slab becomes inlined slab again.
		i = 0
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			var aKey atree.Value
			for k := range expectedChildMapValues {
				aKey = k
				break
			}

			existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedChildMapValues, aKey)

			require.Equal(t, 1+mapCount-1-i, getStoredDeltas(storage))

			i++

			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
			require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

			expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

			elementByteSizesByKey[childKey] = [2]uint32{
				elementByteSizesByKey[childKey][0],
				expectedInlinedMapSize,
			}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}
			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Remove remaining elements from each inlined child map.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			keys := make([]atree.Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				require.Equal(t, 1, getStoredDeltas(storage))

				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
				require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

				expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

				elementByteSizesByKey[childKey] = [2]uint32{
					elementByteSizesByKey[childKey][0],
					expectedInlinedMapSize,
				}

				elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
				for _, v := range elementByteSizesByKey {
					elementByteSizes = append(elementByteSizes, v)
				}
				expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
				require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.
	})

	t.Run("parent is root metadata slab, with four child maps", func(t *testing.T) {
		const (
			mapCount        = 4
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := test_utils.NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		parentMap, expectedKeyValues, _ := createMapWithEmptyChildMap(t, storage, address, typeInfo, mapCount, func() atree.Value {
			return test_utils.NewStringValue(randStr(r, keyStringSize))
		})

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)

		// Appending 3 elements to child map so that inlined child map reaches max inlined size as map element.
		for i := range uint64(3) {
			for childKey, child := range children {
				childMap := child.m
				valueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
				require.True(t, ok)

				k := test_utils.NewStringValue(randStr(r, keyStringSize))
				v := test_utils.NewStringValue(randStr(r, valueStringSize))

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)
				require.Equal(t, i+1, childMap.Count())

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, valueID, childMap.ValueID())              // atree.Value ID is unchanged

				// Test inlined child slab size
				expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		// Parent array has 1 meta data slab and 2 data slabs.
		// All child arrays are inlined.
		require.Equal(t, 3, getStoredDeltas(storage))
		require.False(t, IsMapRootDataSlab(parentMap))

		// Add one more element to child array which triggers inlined child array slab becomes standalone slab
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			require.False(t, childMap.Inlined())

			expectedSlabID := valueIDToSlabID(valueID)
			require.Equal(t, expectedSlabID, childMap.SlabID()) // Storage ID is the same bytewise as value ID.
			require.Equal(t, valueID, childMap.ValueID())       // atree.Value ID is unchanged

			expectedStandaloneSlabSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedStandaloneSlabSize, GetMapRootSlabByteSize(childMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Parent map has one root data slab.
		// Each child maps has one root data slab.
		require.Equal(t, 1+mapCount, getStoredDeltas(storage)) // There are >1 stored slab because child map is no longer inlined.
		require.True(t, IsMapRootDataSlab(parentMap))

		// Remove one element from each child map which triggers standalone map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			var aKey atree.Value
			for k := range expectedChildMapValues {
				aKey = k
				break
			}

			existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedChildMapValues, aKey)

			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
			require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

			expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		// Parent map has one metadata slab + 2 data slabs.
		require.Equal(t, 3, getStoredDeltas(storage)) // There are 3 stored slab because child map is inlined again.
		require.False(t, IsMapRootDataSlab(parentMap))

		// Remove remaining elements from each inlined child map.
		for childKey, child := range children {
			childMap := child.m
			valueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			keys := make([]atree.Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
				require.Equal(t, valueID, childMap.ValueID()) // value ID is unchanged

				expectedInlinedMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedInlinedMapSize, GetMapRootSlabByteSize(childMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		require.Equal(t, uint64(mapCount), parentMap.Count())
		for _, child := range children {
			require.Equal(t, uint64(0), child.m.Count())
		}

		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		// Test parent map slab size
		expectedParentSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
			encodedKeySize,
			emptyInlinedMapByteSize,
			mapCount,
		)
		require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))
	})
}

func TestNestedThreeLevelChildMapInlinabilityInParentMap(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	t.Run("parent is root data slab, one child map, one grand child map, changes to grand child map triggers child map slab to become standalone slab", func(t *testing.T) {
		const (
			mapCount        = 1
			keyStringSize   = 9
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := test_utils.NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() atree.Value {
			return test_utils.NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with an inlined child map, with an inlined grand child map
		parentMap, expectedKeyValues, elementByteSizesByKey := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapCount, getKeyFunc)

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapCount, len(children))

		// Inserting 1 elements to grand child map so that inlined grand child map reaches max inlined size as map element.
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())             // atree.Value ID is unchanged

			// Only parent map slab is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				expectedGrandChildMapSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent slab size
			elementByteSizesByKey[childKey] = [2]uint32{
				elementByteSizesByKey[childKey][0],
				expectedChildMapSize,
			}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}

			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Add one more element to grand child map which triggers inlined child map slab (NOT grand child map slab) becomes standalone slab
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is NOT inlined
			require.False(t, childMap.Inlined())
			require.Equal(t, valueIDToSlabID(cValueID), childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())                 // atree.Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 2, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test standalone child slab size
			expectedChildMapSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				expectedGrandChildMapSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent slab size
			expectedParentSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				slabIDStorableByteSize,
				int(parentMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 2, getStoredDeltas(storage)) // There is 2 stored slab because child map is not inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove elements from grand child map which triggers standalone child map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			gchildKeys := make([]atree.Value, 0, len(expectedGChildMapValues))
			for k := range expectedGChildMapValues {
				gchildKeys = append(gchildKeys, k)
			}

			for _, k := range gchildKeys {
				existingMapKey, existingMapValueStorable, err := gchildMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKey)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedGChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				// Grand child map is inlined
				require.True(t, gchildMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID())
				require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

				// Test inlined child slab size
				expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					expectedGrandChildMapSize,
					int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

				// Test parent child slab size
				expectedParentMapSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					expectedChildMapSize,
					int(parentMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedParentMapSize, GetMapRootSlabByteSize(parentMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			require.Equal(t, uint64(0), gchildMap.Count())
			require.Equal(t, uint64(1), childMap.Count())
		}

		require.Equal(t, uint64(1), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("parent is root data slab, one child map, one grand child map, changes to grand child map triggers grand child array slab to become standalone slab", func(t *testing.T) {
		const (
			mapCount             = 1
			keyStringSize        = 9
			valueStringSize      = 4
			largeValueStringSize = 40
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := test_utils.NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()
		encodedLargeValueSize := test_utils.NewStringValue(strings.Repeat("a", largeValueStringSize)).ByteSize()

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() atree.Value {
			return test_utils.NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with an inlined child map, with an inlined grand child map
		parentMap, expectedKeyValues, elementByteSizesByKey := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapCount, getKeyFunc)

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapCount, len(children))

		// Inserting 1 elements to grand child map so that inlined grand child map reaches max inlined size as map element.
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())             // atree.Value ID is unchanged

			// Only parent map slab is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				expectedGrandChildMapSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent slab size
			elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], expectedChildMapSize}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}

			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		gchildLargeElementKeys := make(map[atree.Value]atree.Value) // key: child map key, value: gchild map key
		// Add one large element to grand child map which triggers inlined grand child map slab (NOT child map slab) becomes standalone slab
		for childKey, child := range children {
			require.Equal(t, 1, len(child.children))

			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, largeValueStringSize))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			gchildLargeElementKeys[childKey] = k

			// Grand child map is NOT inlined
			require.False(t, gchildMap.Inlined())
			require.Equal(t, valueIDToSlabID(gValueID), gchildMap.SlabID()) // Slab ID is valid for not inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())                 // atree.Value ID is unchanged

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())             // atree.Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 2, getStoredDeltas(storage))

			// Test standalone grand child slab size
			expectedGrandChildMapSize := atree.ComputeMapRootDataSlabByteSize(
				[][2]uint32{
					{encodedKeySize, encodedValueSize},
					{encodedKeySize, encodedLargeValueSize},
				},
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSize([][2]uint32{{encodedKeySize, slabIDStorableByteSize}})
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent slab size
			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(
				[][2]uint32{
					{encodedKeySize, expectedChildMapSize},
				},
			)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 2, getStoredDeltas(storage)) // There is 2 stored slab because child map is not inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove elements from grand child map which triggers standalone child map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			// Get all grand child map keys with large element key first
			keys := make([]atree.Value, 0, len(expectedGChildMapValues))
			keys = append(keys, gchildLargeElementKeys[childKey])
			for k := range expectedGChildMapValues {
				if k != gchildLargeElementKeys[childKey] {
					keys = append(keys, k)
				}
			}

			// Remove all elements (large element first) to trigger grand child map being inlined again.
			for _, k := range keys {

				existingMapKeyStorable, existingMapValueStorable, err := gchildMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedGChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				// Grand child map is inlined
				require.True(t, gchildMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID())
				require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

				// Test inlined child slab size
				expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					expectedGrandChildMapSize,
					int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

				// Test parent child slab size
				expectedParentMapSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
					encodedKeySize, expectedChildMapSize,
					int(parentMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedParentMapSize, GetMapRootSlabByteSize(parentMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			require.Equal(t, uint64(0), gchildMap.Count())
			require.Equal(t, uint64(1), childMap.Count())
		}

		require.Equal(t, uint64(1), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("parent is root data slab, two child map, one grand child map each, changes to child map triggers child map slab to become standalone slab", func(t *testing.T) {
		const (
			mapCount        = 2
			keyStringSize   = 4
			valueStringSize = 4
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := test_utils.NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()
		slabIDStorableSize := atree.SlabIDStorable(atree.SlabID{}).ByteSize()

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() atree.Value {
			return test_utils.NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with inlined child map, containing inlined grand child map
		parentMap, expectedKeyValues, elementByteSizesByKey := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapCount, getKeyFunc)

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapCount, len(children))

		// Insert 1 elements to grand child map (both child map and grand child map are still inlined).
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}

			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())             // atree.Value ID is unchanged

			// Only parent map slab is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				expectedGrandChildMapSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent slab size
			elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], expectedChildMapSize}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}

			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

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

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())             // atree.Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, 1, getStoredDeltas(storage))

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSize(
				[][2]uint32{
					{encodedKeySize, encodedValueSize},
					{encodedKeySize, expectedGrandChildMapSize},
				},
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent slab size
			elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], expectedChildMapSize}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}

			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, IsMapRootDataSlab(parentMap))
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

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildMapValues[k] = v

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is NOT inlined
			require.False(t, childMap.Inlined())
			require.Equal(t, valueIDToSlabID(cValueID), childMap.SlabID()) // Slab ID is the same as value ID for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())                 // atree.Value ID is unchanged

			// Parent map is standalone
			require.False(t, parentMap.Inlined())
			require.Equal(t, (1 + i + 1), getStoredDeltas(storage))

			i++

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test standalone child slab size
			expectedChildMapSize := atree.ComputeMapRootDataSlabByteSize(
				[][2]uint32{
					{encodedKeySize, encodedValueSize},
					{encodedKeySize, encodedValueSize},
					{encodedKeySize, expectedGrandChildMapSize},
				},
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent slab size
			elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], slabIDStorableSize}

			elementByteSizes := make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}

			expectedParentSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1+mapCount, getStoredDeltas(storage)) // There is 1+mapCount stored slab because all child maps are standalone.

		// Test parent slab size
		expectedParentSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
			encodedKeySize,
			slabIDStorableSize,
			mapCount,
		)
		require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove one element from child map which triggers standalone child map slab becomes inlined slab again.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			var aKey atree.Value
			for k := range expectedChildMapValues {
				if k != gchildKey {
					aKey = k
					break
				}
			}

			// Remove one element
			existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedChildMapValues, aKey)

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
			require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID())
			require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			elementByteSizes := make([][2]uint32, int(childMap.Count())) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			elementByteSizes[0] = [2]uint32{encodedKeySize, expectedGrandChildMapSize}
			for i := 1; i < len(elementByteSizes); i++ {
				elementByteSizes[i] = [2]uint32{encodedKeySize, encodedValueSize}
			}
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSize(elementByteSizes)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			// Test parent child slab size
			elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], expectedChildMapSize}

			elementByteSizes = make([][2]uint32, 0, len(elementByteSizesByKey))
			for _, v := range elementByteSizesByKey {
				elementByteSizes = append(elementByteSizes, v)
			}

			expectedParentMapSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
			require.Equal(t, expectedParentMapSize, GetMapRootSlabByteSize(parentMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// remove remaining elements from child map, except for grand child map
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			keys := make([]atree.Value, 0, len(expectedChildMapValues)-1)
			for k := range expectedChildMapValues {
				if k != gchildKey {
					keys = append(keys, k)
				}
			}

			// Remove all elements, except grand child map
			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				// Grand child map is inlined
				require.True(t, gchildMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID())
				require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

				// Test inlined grand child slab size
				expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					encodedKeySize,
					encodedValueSize,
					int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

				// Test inlined child slab size
				elementByteSizes := make([][2]uint32, int(childMap.Count())) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				elementByteSizes[0] = [2]uint32{encodedKeySize, expectedGrandChildMapSize}
				for i := 1; i < len(elementByteSizes); i++ {
					elementByteSizes[i] = [2]uint32{encodedKeySize, encodedValueSize}
				}
				expectedChildMapSize := atree.ComputeInlinedMapSlabByteSize(elementByteSizes)
				require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

				// Test parent child slab size
				elementByteSizesByKey[childKey] = [2]uint32{elementByteSizesByKey[childKey][0], expectedChildMapSize}

				elementByteSizes = make([][2]uint32, 0, len(elementByteSizesByKey))
				for _, v := range elementByteSizesByKey {
					elementByteSizes = append(elementByteSizes, v)
				}

				expectedParentMapSize := atree.ComputeMapRootDataSlabByteSize(elementByteSizes)
				require.Equal(t, expectedParentMapSize, GetMapRootSlabByteSize(parentMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			require.Equal(t, uint64(1), gchildMap.Count())
			require.Equal(t, uint64(1), childMap.Count())
		}

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map and grand child map are inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("parent is root metadata slab, with four child maps, each child map has grand child maps", func(t *testing.T) {
		const (
			mapCount        = 4
			keyStringSize   = 4
			valueStringSize = 8
		)

		// encoded key size is the same for all string keys of the same length.
		encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()
		encodedValueSize := test_utils.NewStringValue(strings.Repeat("a", valueStringSize)).ByteSize()
		slabIDStorableSize := atree.SlabIDStorable(atree.SlabID{}).ByteSize()

		r := newRand(t)

		typeInfo := test_utils.NewSimpleTypeInfo(42)
		storage := newTestPersistentStorage(t)
		address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

		getKeyFunc := func() atree.Value {
			return test_utils.NewStringValue(randStr(r, keyStringSize))
		}

		// Create a parent map, with inlined child map, containing inlined grand child map
		parentMap, expectedKeyValues, _ := createMapWithEmpty2LevelChildMap(t, storage, address, typeInfo, mapCount, getKeyFunc)

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		children := getInlinedChildMapsFromParentMap(t, address, parentMap)
		require.Equal(t, mapCount, len(children))

		// Insert 1 element to grand child map
		// Both child map and grand child map are still inlined, but parent map's root slab is metadata slab.
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is still inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, cValueID, childMap.ValueID())             // atree.Value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				expectedGrandChildMapSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.False(t, parentMap.Inlined())
		require.False(t, IsMapRootDataSlab(parentMap))
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

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			existingStorable, err := gchildMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedGChildMapValues[k] = v

			// Grand child map is still inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID()) // Slab ID is undefined for inlined slab
			require.Equal(t, gValueID, gchildMap.ValueID())             // atree.Value ID is unchanged

			// Child map is NOT inlined
			require.False(t, childMap.Inlined())
			require.Equal(t, valueIDToSlabID(cValueID), childMap.SlabID()) // Slab ID is same as value ID
			require.Equal(t, cValueID, childMap.ValueID())                 // atree.Value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test standalone child slab size
			expectedChildMapSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				expectedGrandChildMapSize,
				int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.False(t, parentMap.Inlined())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1+mapCount, getStoredDeltas(storage))

		// Test parent slab size
		expectedParentMapSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
			encodedKeySize,
			slabIDStorableSize,
			int(parentMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		)
		require.Equal(t, expectedParentMapSize, GetMapRootSlabByteSize(parentMap))

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove one element from grand child map to trigger child map inlined again.
		// - grand child maps are inlined
		// - child maps are inlined
		// - parent map root slab is metadata slab
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			var gchildKey atree.Value
			var gchild *mapInfo
			for gk, gv := range child.children {
				gchildKey = gk
				gchild = gv
				break
			}
			gchildMap := gchild.m
			gValueID := gchild.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			expectedGChildMapValues, ok := expectedChildMapValues[gchildKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			var aKey atree.Value
			for k := range expectedGChildMapValues {
				aKey = k
				break
			}

			// Remove one element from grand child map
			existingMapKeyStorable, existingMapValueStorable, err := gchildMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, aKey)
			require.NoError(t, err)
			require.Equal(t, aKey, existingMapKeyStorable)
			require.NotNil(t, existingMapValueStorable)

			delete(expectedGChildMapValues, aKey)

			// Child map is inlined
			require.True(t, childMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
			require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

			// Grand child map is inlined
			require.True(t, gchildMap.Inlined())
			require.Equal(t, atree.SlabIDUndefined, gchildMap.SlabID())
			require.Equal(t, gValueID, gchildMap.ValueID()) // value ID is unchanged

			// Test inlined grand child slab size
			expectedGrandChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
				encodedKeySize,
				encodedValueSize,
				int(gchildMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			)
			require.Equal(t, expectedGrandChildMapSize, GetMapRootSlabByteSize(gchildMap))

			// Test inlined child slab size
			elementByteSizes := make([][2]uint32, int(childMap.Count())) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
			elementByteSizes[0] = [2]uint32{encodedKeySize, expectedGrandChildMapSize}
			for i := 1; i < len(elementByteSizes); i++ {
				elementByteSizes[i] = [2]uint32{encodedKeySize, encodedValueSize}
			}
			expectedChildMapSize := atree.ComputeInlinedMapSlabByteSize(elementByteSizes)
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
		}

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.False(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 3, getStoredDeltas(storage))

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove all grand child element to trigger
		// - child maps are inlined
		// - parent map root slab is data slab
		for childKey, child := range children {
			childMap := child.m
			cValueID := child.valueID

			expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
			require.True(t, ok)

			keys := make([]atree.Value, 0, len(expectedChildMapValues))
			for k := range expectedChildMapValues {
				keys = append(keys, k)
			}

			// Remove grand children
			for _, k := range keys {
				existingMapKeyStorable, existingMapValueStorable, err := childMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.Equal(t, k, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				// Grand child map is returned as atree.SlabIDStorable, even if it was stored inlined in the parent.
				id, ok := existingMapValueStorable.(atree.SlabIDStorable)
				require.True(t, ok)

				v, err := id.StoredValue(storage)
				require.NoError(t, err)

				gchildMap, ok := v.(*atree.OrderedMap)
				require.True(t, ok)

				expectedGChildMapValues, ok := expectedChildMapValues[k].(test_utils.ExpectedMapValue)
				require.True(t, ok)

				testValueEqual(t, expectedGChildMapValues, gchildMap)

				err = storage.Remove(atree.SlabID(id))
				require.NoError(t, err)

				delete(expectedChildMapValues, k)

				// Child map is inlined
				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID())
				require.Equal(t, cValueID, childMap.ValueID()) // value ID is unchanged

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}

			expectedChildMapSize := emptyInlinedMapByteSize
			require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

			require.Equal(t, uint64(0), childMap.Count())
		}

		require.Equal(t, uint64(mapCount), parentMap.Count())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.Equal(t, 1, getStoredDeltas(storage))

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		expectedChildMapSize := emptyInlinedMapByteSize
		expectedParentMapSize = atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
			encodedKeySize,
			expectedChildMapSize,
			mapCount,
		)
		require.Equal(t, expectedParentMapSize, GetMapRootSlabByteSize(parentMap))
	})
}

func TestChildMapWhenParentMapIsModified(t *testing.T) {
	const (
		mapCount        = 2
		keyStringSize   = 4
		valueStringSize = 4
	)

	// encoded key size is the same for all string keys of the same length.
	encodedKeySize := test_utils.NewStringValue(strings.Repeat("a", keyStringSize)).ByteSize()

	r := newRand(t)

	typeInfo := test_utils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	parentMapDigesterBuilder := &mockDigesterBuilder{}
	parentDigest := uint64(1)

	// Create parent map with mock digests
	parentMap, err := atree.NewMap(storage, address, parentMapDigesterBuilder, typeInfo)
	require.NoError(t, err)

	expectedKeyValues := make(map[atree.Value]atree.Value)

	// Insert 2 child map with digest values of 1 and 3.
	for i := range mapCount {
		// Create child map
		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := test_utils.NewStringValue(randStr(r, keyStringSize))

		digests := []atree.Digest{
			atree.Digest(parentDigest),
		}
		parentMapDigesterBuilder.On("Digest", k).Return(mockDigester{digests})
		parentDigest += 2

		// Insert child map to parent map
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedKeyValues[k] = test_utils.ExpectedMapValue{}

		require.True(t, childMap.Inlined())
		testInlinedMapIDs(t, address, childMap)

		// Test child map slab size
		require.Equal(t, emptyInlinedMapByteSize, GetMapRootSlabByteSize(childMap))

		// Test parent map slab size
		expectedParentSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
			encodedKeySize,
			emptyInlinedMapByteSize,
			i+1,
		)
		require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))
	}

	require.Equal(t, uint64(mapCount), parentMap.Count())
	require.True(t, IsMapRootDataSlab(parentMap))
	require.Equal(t, 1, getStoredDeltas(storage)) // There is only 1 stored slab because child map is inlined.

	testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

	children := getInlinedChildMapsFromParentMap(t, address, parentMap)
	require.Equal(t, mapCount, len(children))

	var keysForNonChildMaps []atree.Value

	t.Run("insert elements in parent map", func(t *testing.T) {

		newDigests := []atree.Digest{
			0, // insert value at digest 0, so all child map physical positions are moved by +1
			2, // insert value at digest 2, so second child map physical positions are moved by +1
			4, // insert value at digest 4, so no child map physical positions are moved
		}

		for _, digest := range newDigests {

			k := test_utils.NewStringValue(randStr(r, keyStringSize))
			v := test_utils.NewStringValue(randStr(r, valueStringSize))

			digests := []atree.Digest{digest}
			parentMapDigesterBuilder.On("Digest", k).Return(mockDigester{digests})

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedKeyValues[k] = v
			keysForNonChildMaps = append(keysForNonChildMaps, k)

			i := uint64(0)
			for childKey, child := range children {
				childMap := child.m
				childValueID := child.valueID

				expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
				require.True(t, ok)

				k := test_utils.NewStringValue(randStr(r, keyStringSize))
				v := test_utils.Uint64Value(i)

				i++

				existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
				require.NoError(t, err)
				require.Nil(t, existingStorable)

				expectedChildMapValues[k] = v

				require.True(t, childMap.Inlined())
				require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
				require.Equal(t, childValueID, childMap.ValueID())         // atree.Value ID is unchanged

				// Test inlined child slab size
				expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
					k.ByteSize(),
					v.ByteSize(),
					int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
				)
				require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

				testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
			}
		}

		t.Run("remove elements from parent map", func(t *testing.T) {
			// Remove element at digest 0, so all child map physical position are moved by -1.
			// Remove element at digest 2, so only second child map physical position is moved by -1
			// Remove element at digest 4, so no child map physical position is moved by -1

			for _, k := range keysForNonChildMaps {

				existingMapKeyStorable, existingMapValueStorable, err := parentMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
				require.NoError(t, err)
				require.NotNil(t, existingMapKeyStorable)
				require.NotNil(t, existingMapValueStorable)

				delete(expectedKeyValues, k)

				i := uint64(0)
				for childKey, child := range children {
					childMap := child.m
					childValueID := child.valueID

					expectedChildMapValues, ok := expectedKeyValues[childKey].(test_utils.ExpectedMapValue)
					require.True(t, ok)

					k := test_utils.NewStringValue(randStr(r, keyStringSize))
					v := test_utils.Uint64Value(i)

					i++

					existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
					require.NoError(t, err)
					require.Nil(t, existingStorable)

					expectedChildMapValues[k] = v

					require.True(t, childMap.Inlined())
					require.Equal(t, atree.SlabIDUndefined, childMap.SlabID()) // Slab ID is undefined for inlined slab
					require.Equal(t, childValueID, childMap.ValueID())         // atree.Value ID is unchanged

					// Test inlined child slab size
					expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
						k.ByteSize(),
						v.ByteSize(),
						int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
					)
					require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

					testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
				}
			}
		})
	})
}

func createMapWithEmptyChildMap(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	mapCount int,
	getKey func() atree.Value,
) (*atree.OrderedMap, map[atree.Value]atree.Value, map[atree.Value][2]uint32) {

	// Create parent map
	parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	expectedValues := make(map[atree.Value]atree.Value)

	elementByteSizesByKey := make(map[atree.Value][2]uint32)

	for i := range mapCount {
		// Create child map
		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := getKey()

		ks, err := k.Storable(storage, address, atree.MaxInlineMapElementSize())
		require.NoError(t, err)

		// Insert child map to parent map
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues[k] = test_utils.ExpectedMapValue{}

		require.True(t, childMap.Inlined())
		testInlinedMapIDs(t, address, childMap)

		// Test child map slab size
		require.Equal(t, emptyInlinedMapByteSize, GetMapRootSlabByteSize(childMap))

		// Test parent map slab size
		expectedParentSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
			ks.ByteSize(),
			emptyInlinedMapByteSize,
			i+1,
		)
		require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

		elementByteSizesByKey[k] = [2]uint32{ks.ByteSize(), emptyInlinedMapByteSize}
	}

	return parentMap, expectedValues, elementByteSizesByKey
}

func createMapWithEmpty2LevelChildMap(
	t *testing.T,
	storage atree.SlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	mapCount int,
	getKey func() atree.Value,
) (*atree.OrderedMap, map[atree.Value]atree.Value, map[atree.Value][2]uint32) {

	// Create parent map
	parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	expectedValues := make(map[atree.Value]atree.Value)

	elementByteSizesByKey := make(map[atree.Value][2]uint32)

	for i := range mapCount {
		// Create child map
		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		// Create grand child map
		gchildMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		k := getKey()

		ks, err := k.Storable(storage, address, atree.MaxInlineMapElementSize())
		require.NoError(t, err)

		// Insert grand child map to child map
		existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, gchildMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.True(t, gchildMap.Inlined())
		testInlinedMapIDs(t, address, gchildMap)

		// Insert child map to parent map
		existingStorable, err = parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		expectedValues[k] = test_utils.ExpectedMapValue{k: test_utils.ExpectedMapValue{}}

		require.True(t, childMap.Inlined())
		testInlinedMapIDs(t, address, childMap)

		// Test grand child map slab size
		require.Equal(t, emptyInlinedMapByteSize, GetMapRootSlabByteSize(gchildMap))

		// Test child map slab size
		expectedChildMapSize := atree.ComputeInlinedMapSlabByteSizeWithFixSizedElement(
			ks.ByteSize(),
			emptyInlinedMapByteSize,
			int(childMap.Count()), //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
		)
		require.Equal(t, expectedChildMapSize, GetMapRootSlabByteSize(childMap))

		// Test parent map slab size
		expectedParentSize := atree.ComputeMapRootDataSlabByteSizeWithFixSizedElement(
			ks.ByteSize(),
			expectedChildMapSize,
			i+1,
		)
		require.Equal(t, expectedParentSize, GetMapRootSlabByteSize(parentMap))

		elementByteSizesByKey[k] = [2]uint32{ks.ByteSize(), expectedChildMapSize}
	}

	testNotInlinedMapIDs(t, address, parentMap)

	return parentMap, expectedValues, elementByteSizesByKey
}

type mapInfo struct {
	m        *atree.OrderedMap
	valueID  atree.ValueID
	children map[atree.Value]*mapInfo
}

func getInlinedChildMapsFromParentMap(t *testing.T, address atree.Address, parentMap *atree.OrderedMap) map[atree.Value]*mapInfo {

	children := make(map[atree.Value]*mapInfo)

	err := parentMap.IterateReadOnlyKeys(func(k atree.Value) (bool, error) {
		if k == nil {
			return false, nil
		}

		e, err := parentMap.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.NoError(t, err)

		childMap, ok := e.(*atree.OrderedMap)
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
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			var expectedChildValues test_utils.ExpectedArrayValue
			for {
				v := test_utils.NewStringValue(strings.Repeat("a", 10))

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
			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)

			expectedKeyValues[k] = test_utils.Uint64Value(0)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			// Insert one element to child array
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedKeyValues[k] = test_utils.ExpectedArrayValue{v}
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite existing child array value
		for k := range expectedKeyValues {
			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			expectedKeyValues[k] = test_utils.Uint64Value(0)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedKeyValues[k] = expectedChildValues

			// Insert into child map until child map is not inlined

			for j := uint64(0); ; j++ {
				k := test_utils.Uint64Value(j)
				v := test_utils.NewStringValue(strings.Repeat("a", 10))

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
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
			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			expectedKeyValues[k] = test_utils.Uint64Value(0)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedKeyValues[k] = expectedChildValues

			// Insert into child map until child map is not inlined
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite existing child map value
		for k := range expectedKeyValues {
			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.Uint64Value(0))
			require.NoError(t, err)
			require.NotNil(t, existingStorable)

			id, ok := existingStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			expectedKeyValues[k] = test_utils.Uint64Value(0)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})
}

func TestMapRemoveReturnedValue(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("child array is not inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			var expectedChildValues test_utils.ExpectedArrayValue
			for {
				v := test_utils.NewStringValue(strings.Repeat("a", 10))

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
			keyStorable, valueStorable, err := parentMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)

			delete(expectedKeyValues, k)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child array is inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child array
			childArray, err := atree.NewArray(storage, address, typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			// Insert one element to child array
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			err = childArray.Append(v)
			require.NoError(t, err)
			require.True(t, childArray.Inlined())

			expectedKeyValues[k] = test_utils.ExpectedArrayValue{v}
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child array value
		for k := range expectedKeyValues {
			keyStorable, valueStorable, err := parentMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			delete(expectedKeyValues, k)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is not inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedKeyValues[k] = expectedChildValues

			// Insert into child map until child map is not inlined

			for j := uint64(0); ; j++ {
				k := test_utils.Uint64Value(j)
				v := test_utils.NewStringValue(strings.Repeat("a", 10))

				existingStorable, err := childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
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
			keyStorable, valueStorable, err := parentMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			delete(expectedKeyValues, k)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})

	t.Run("child map is inlined", func(t *testing.T) {
		const mapCount = uint64(2)

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(map[atree.Value]atree.Value)

		for i := range mapCount {
			// Create child map
			childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
			require.NoError(t, err)

			k := test_utils.Uint64Value(i)

			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childMap)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues := make(test_utils.ExpectedMapValue)
			expectedKeyValues[k] = expectedChildValues

			// Insert into child map until child map is not inlined
			v := test_utils.NewStringValue(strings.Repeat("a", 10))

			existingStorable, err = childMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)

			expectedChildValues[k] = v
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child map value
		for k := range expectedKeyValues {
			keyStorable, valueStorable, err := parentMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
			require.NoError(t, err)
			require.Equal(t, keyStorable, k)

			id, ok := valueStorable.(atree.SlabIDStorable)
			require.True(t, ok)

			child, err := id.StoredValue(storage)
			require.NoError(t, err)

			testValueEqual(t, expectedKeyValues[k], child)

			delete(expectedKeyValues, k)

			err = storage.Remove(atree.SlabID(id))
			require.NoError(t, err)
		}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)
	})
}

func TestMapWithOutdatedCallback(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("overwritten child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(test_utils.ExpectedMapValue)

		// Create child array
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		k := test_utils.Uint64Value(0)

		// Insert child array to parent map
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		v := test_utils.NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedKeyValues[k] = test_utils.ExpectedArrayValue{v}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Overwrite child array value from parent
		valueStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, test_utils.Uint64Value(0))
		require.NoError(t, err)

		id, ok := valueStorable.(atree.SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		testValueEqual(t, expectedKeyValues[k], child)

		expectedKeyValues[k] = test_utils.Uint64Value(0)

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.True(t, atree.ArrayHasParentUpdater(childArray))

		// modify overwritten child array
		err = childArray.Append(test_utils.Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.False(t, atree.ArrayHasParentUpdater(childArray))

		// No-op on parent
		testValueEqual(t, expectedKeyValues, parentMap)
	})

	t.Run("removed child array", func(t *testing.T) {

		storage := newTestPersistentStorage(t)

		// Create parent map
		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		expectedKeyValues := make(test_utils.ExpectedMapValue)

		// Create child array
		childArray, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		k := test_utils.Uint64Value(0)

		// Insert child array to parent map
		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, k, childArray)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		v := test_utils.NewStringValue(strings.Repeat("a", 10))

		err = childArray.Append(v)
		require.NoError(t, err)

		expectedKeyValues[k] = test_utils.ExpectedArrayValue{v}

		testMap(t, storage, typeInfo, address, parentMap, expectedKeyValues, nil, true)

		// Remove child array value from parent
		keyStorable, valueStorable, err := parentMap.Remove(test_utils.CompareValue, test_utils.GetHashInput, k)
		require.NoError(t, err)
		require.Equal(t, keyStorable, k)

		id, ok := valueStorable.(atree.SlabIDStorable)
		require.True(t, ok)

		child, err := id.StoredValue(storage)
		require.NoError(t, err)

		testValueEqual(t, expectedKeyValues[k], child)

		delete(expectedKeyValues, k)

		// childArray.parentUpdater isn't nil before callback is invoked.
		require.True(t, atree.ArrayHasParentUpdater(childArray))

		// modify removed child array
		err = childArray.Append(test_utils.Uint64Value(0))
		require.NoError(t, err)

		// childArray.parentUpdater is nil after callback is invoked.
		require.False(t, atree.ArrayHasParentUpdater(childArray))

		// No-op on parent
		testValueEqual(t, expectedKeyValues, parentMap)
	})
}

func TestMapSetType(t *testing.T) {
	typeInfo := test_utils.NewSimpleTypeInfo(42)
	newTypeInfo := test_utils.NewSimpleTypeInfo(43)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("empty", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())
		require.Equal(t, typeInfo, m.Type())
		require.True(t, IsMapRootDataSlab(m))

		seed := m.Seed()

		err = m.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Count())
		require.Equal(t, newTypeInfo, m.Type())
		require.Equal(t, seed, m.Seed())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingMapSetType(t, m.SlabID(), atree.GetBaseStorage(storage), newTypeInfo, m.Count(), seed)
	})

	t.Run("data slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(10)
		for i := range mapCount {
			v := test_utils.Uint64Value(i)
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, v, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, mapCount, m.Count())
		require.Equal(t, typeInfo, m.Type())
		require.True(t, IsMapRootDataSlab(m))

		seed := m.Seed()

		err = m.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, m.Type())
		require.Equal(t, mapCount, m.Count())
		require.Equal(t, seed, m.Seed())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingMapSetType(t, m.SlabID(), atree.GetBaseStorage(storage), newTypeInfo, m.Count(), seed)
	})

	t.Run("metadata slab root", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		const mapCount = uint64(10_000)
		for i := range mapCount {
			v := test_utils.Uint64Value(i)
			existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, v, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		require.Equal(t, mapCount, m.Count())
		require.Equal(t, typeInfo, m.Type())
		require.False(t, IsMapRootDataSlab(m))

		seed := m.Seed()

		err = m.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, m.Type())
		require.Equal(t, mapCount, m.Count())
		require.Equal(t, seed, m.Seed())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingMapSetType(t, m.SlabID(), atree.GetBaseStorage(storage), newTypeInfo, m.Count(), seed)
	})

	t.Run("inlined in parent container root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMapSeed := childMap.Seed()

		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(0), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, uint64(1), parentMap.Count())
		require.Equal(t, typeInfo, parentMap.Type())
		require.True(t, IsMapRootDataSlab(parentMap))
		require.False(t, parentMap.Inlined())

		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, typeInfo, childMap.Type())
		require.True(t, IsMapRootDataSlab(childMap))
		require.True(t, childMap.Inlined())

		err = childMap.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childMap.Type())
		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, childMapSeed, childMap.Seed())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedMapSetType(
			t,
			parentMap.SlabID(),
			test_utils.Uint64Value(0),
			atree.GetBaseStorage(storage),
			newTypeInfo,
			childMap.Count(),
			childMapSeed,
		)
	})

	t.Run("inlined in parent container non-root data slab", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		parentMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMap, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		childMapSeed := childMap.Seed()

		const mapCount = uint64(10_000)
		for i := range mapCount - 1 {
			v := test_utils.Uint64Value(i)
			existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, v, v)
			require.NoError(t, err)
			require.Nil(t, existingStorable)
		}

		existingStorable, err := parentMap.Set(test_utils.CompareValue, test_utils.GetHashInput, test_utils.Uint64Value(mapCount-1), childMap)
		require.NoError(t, err)
		require.Nil(t, existingStorable)

		require.Equal(t, mapCount, parentMap.Count())
		require.Equal(t, typeInfo, parentMap.Type())
		require.False(t, IsMapRootDataSlab(parentMap))
		require.False(t, parentMap.Inlined())

		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, typeInfo, childMap.Type())
		require.True(t, IsMapRootDataSlab(childMap))
		require.True(t, childMap.Inlined())

		err = childMap.SetType(newTypeInfo)
		require.NoError(t, err)
		require.Equal(t, newTypeInfo, childMap.Type())
		require.Equal(t, uint64(0), childMap.Count())
		require.Equal(t, childMapSeed, childMap.Seed())

		// Commit modified slabs in storage
		err = storage.FastCommit(runtime.NumCPU())
		require.NoError(t, err)

		testExistingInlinedMapSetType(
			t,
			parentMap.SlabID(),
			test_utils.Uint64Value(mapCount-1),
			atree.GetBaseStorage(storage),
			newTypeInfo,
			childMap.Count(),
			childMapSeed,
		)
	})
}

func testExistingMapSetType(
	t *testing.T,
	id atree.SlabID,
	baseStorage atree.BaseStorage,
	expectedTypeInfo test_utils.SimpleTypeInfo,
	expectedCount uint64,
	expectedSeed uint64,
) {
	newTypeInfo := test_utils.NewSimpleTypeInfo(expectedTypeInfo.Value() + 1)

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing map by ID
	m, err := atree.NewMapWithRootID(storage, id, atree.NewDefaultDigesterBuilder())
	require.NoError(t, err)
	require.Equal(t, expectedCount, m.Count())
	require.Equal(t, expectedTypeInfo, m.Type())
	require.Equal(t, expectedSeed, m.Seed())

	// Modify type info of existing map
	err = m.SetType(newTypeInfo)
	require.NoError(t, err)
	require.Equal(t, expectedCount, m.Count())
	require.Equal(t, newTypeInfo, m.Type())
	require.Equal(t, expectedSeed, m.Seed())

	// Commit data in storage
	err = storage.FastCommit(runtime.NumCPU())
	require.NoError(t, err)

	// Create storage from existing data
	storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

	// Load existing map again from storage
	m2, err := atree.NewMapWithRootID(storage2, id, atree.NewDefaultDigesterBuilder())
	require.NoError(t, err)
	require.Equal(t, expectedCount, m2.Count())
	require.Equal(t, newTypeInfo, m2.Type())
	require.Equal(t, expectedSeed, m2.Seed())
}

func testExistingInlinedMapSetType(
	t *testing.T,
	parentID atree.SlabID,
	inlinedChildKey atree.Value,
	baseStorage atree.BaseStorage,
	expectedTypeInfo test_utils.SimpleTypeInfo,
	expectedCount uint64,
	expectedSeed uint64,
) {
	newTypeInfo := test_utils.NewSimpleTypeInfo(expectedTypeInfo.Value() + 1)

	// Create storage from existing data
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)

	// Load existing map by ID
	parentMap, err := atree.NewMapWithRootID(storage, parentID, atree.NewDefaultDigesterBuilder())
	require.NoError(t, err)

	element, err := parentMap.Get(test_utils.CompareValue, test_utils.GetHashInput, inlinedChildKey)
	require.NoError(t, err)

	childMap, ok := element.(*atree.OrderedMap)
	require.True(t, ok)

	require.Equal(t, expectedCount, childMap.Count())
	require.Equal(t, expectedTypeInfo, childMap.Type())
	require.Equal(t, expectedSeed, childMap.Seed())

	// Modify type info of existing map
	err = childMap.SetType(newTypeInfo)
	require.NoError(t, err)
	require.Equal(t, expectedCount, childMap.Count())
	require.Equal(t, newTypeInfo, childMap.Type())
	require.Equal(t, expectedSeed, childMap.Seed())

	// Commit data in storage
	err = storage.FastCommit(runtime.NumCPU())
	require.NoError(t, err)

	// Create storage from existing data
	storage2 := newTestPersistentStorageWithBaseStorage(t, atree.GetBaseStorage(storage))

	// Load existing map again from storage
	parentMap2, err := atree.NewMapWithRootID(storage2, parentID, atree.NewDefaultDigesterBuilder())
	require.NoError(t, err)

	element2, err := parentMap2.Get(test_utils.CompareValue, test_utils.GetHashInput, inlinedChildKey)
	require.NoError(t, err)

	childMap2, ok := element2.(*atree.OrderedMap)
	require.True(t, ok)

	require.Equal(t, expectedCount, childMap2.Count())
	require.Equal(t, newTypeInfo, childMap2.Type())
	require.Equal(t, expectedSeed, childMap.Seed())
}

func newRandomDigests(r *rand.Rand, level int) []atree.Digest {
	digest := make([]atree.Digest, level)
	for i := range digest {
		digest[i] = newRandomDigest(r)
	}
	return digest
}

func newRandomDigest(r *rand.Rand) atree.Digest {
	return atree.Digest(r.Intn(256)) //nolint:gosec // integer overflow conversions (e.g. uint64 -> int (G115), etc.) are OK for tests
}
