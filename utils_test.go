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
	"flag"
	"math/rand"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"

	"github.com/onflow/atree/test_utils"
)

var (
	runes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
)

var seed = flag.Int64("seed", 0, "seed for pseudo-random source")

func newRand(tb testing.TB) *rand.Rand {
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	// Benchmarks always log, so only log for tests which
	// will only log with -v flag or on error.
	if t, ok := tb.(*testing.T); ok {
		t.Logf("seed: %d\n", *seed)
	}

	return rand.New(rand.NewSource(*seed))
}

// randStr returns random UTF-8 string of given length.
func randStr(r *rand.Rand, length int) string {
	b := make([]rune, length)
	for i := 0; i < length; i++ {
		b[i] = runes[r.Intn(len(runes))]
	}
	return string(b)
}

func randomValue(r *rand.Rand, maxInlineSize int) atree.Value {
	switch r.Intn(6) {

	case 0:
		return test_utils.Uint8Value(r.Intn(255))

	case 1:
		return test_utils.Uint16Value(r.Intn(6535))

	case 2:
		return test_utils.Uint32Value(r.Intn(4294967295))

	case 3:
		return test_utils.Uint64Value(r.Intn(1844674407370955161))

	case 4: // small string (inlinable)
		slen := r.Intn(maxInlineSize)
		return test_utils.NewStringValue(randStr(r, slen))

	case 5: // large string (external)
		slen := r.Intn(1024) + maxInlineSize
		return test_utils.NewStringValue(randStr(r, slen))

	default:
		panic(atree.NewUnreachableError())
	}
}

// ExpectedArrayValue

type ExpectedArrayValue []atree.Value

var _ atree.Value = &ExpectedArrayValue{}

func (v ExpectedArrayValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

// ExpectedMapValue

type ExpectedMapValue map[atree.Value]atree.Value

var _ atree.Value = &ExpectedMapValue{}

func (v ExpectedMapValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

// ExpectedWrapperValue

type ExpectedWrapperValue struct {
	Value atree.Value
}

var _ atree.Value = &ExpectedWrapperValue{}

func (v ExpectedWrapperValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

func testValueEqual(t *testing.T, expected atree.Value, actual atree.Value) {
	switch expected := expected.(type) {
	case ExpectedArrayValue:
		actual, ok := actual.(*atree.Array)
		require.True(t, ok)

		testArrayEqual(t, expected, actual)

	case *atree.Array:
		require.FailNow(t, "expected value shouldn't be *atree.Array")

	case ExpectedMapValue:
		actual, ok := actual.(*atree.OrderedMap)
		require.True(t, ok)

		testMapEqual(t, expected, actual)

	case *atree.OrderedMap:
		require.FailNow(t, "expected value shouldn't be *atree.OrderedMap")

	case ExpectedWrapperValue:
		actual, ok := actual.(test_utils.SomeValue)
		require.True(t, ok)

		testValueEqual(t, expected.Value, actual.Value)

	case test_utils.SomeValue:
		require.FailNow(t, "expected value shouldn't be test_utils.SomeValue")

	default:
		require.Equal(t, expected, actual)
	}
}

func testArrayEqual(t *testing.T, expected ExpectedArrayValue, actual *atree.Array) {
	require.Equal(t, uint64(len(expected)), actual.Count())

	iterator, err := actual.ReadOnlyIterator()
	require.NoError(t, err)

	i := 0
	for {
		actualValue, err := iterator.Next()
		require.NoError(t, err)

		if actualValue == nil {
			break
		}

		testValueEqual(t, expected[i], actualValue)
		i++
	}
	require.Equal(t, len(expected), i)
}

func testMapEqual(t *testing.T, expected ExpectedMapValue, actual *atree.OrderedMap) {
	require.Equal(t, uint64(len(expected)), actual.Count())

	iterator, err := actual.ReadOnlyIterator()
	require.NoError(t, err)

	i := 0
	for {
		actualKey, actualValue, err := iterator.Next()
		require.NoError(t, err)

		if actualKey == nil {
			break
		}

		expectedValue, exist := expected[actualKey]
		require.True(t, exist)

		testValueEqual(t, expectedValue, actualValue)
		i++
	}
	require.Equal(t, len(expected), i)
}

func GetDeltasCount(storage *atree.PersistentSlabStorage) int {
	return len(atree.GetDeltas(storage))
}

func GetCacheCount(storage *atree.PersistentSlabStorage) int {
	return len(atree.GetCache(storage))
}

func IsArrayRootDataSlab(array *atree.Array) bool {
	return atree.GetArrayRootSlab(array).IsData()
}

func GetArrayRootSlabByteSize(array *atree.Array) uint32 {
	return atree.GetArrayRootSlab(array).ByteSize()
}

func IsMapRootDataSlab(m *atree.OrderedMap) bool {
	return atree.GetMapRootSlab(m).IsData()
}

func GetMapRootSlabByteSize(m *atree.OrderedMap) uint32 {
	return atree.GetMapRootSlab(m).ByteSize()
}

var (
	slabIDStorableByteSize    = atree.SlabIDStorable{}.ByteSize()
	emptyInlinedArrayByteSize = atree.ComputeInlinedArraySlabByteSize(nil)
	emptyInlinedMapByteSize   = atree.ComputeInlinedMapSlabByteSize(nil)
)

// Storage test util functions

func newTestPersistentStorage(t testing.TB) *atree.PersistentSlabStorage {
	baseStorage := test_utils.NewInMemBaseStorage()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return atree.NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		test_utils.DecodeStorable,
		test_utils.DecodeTypeInfo,
	)
}

func newTestPersistentStorageWithData(t testing.TB, data map[atree.SlabID][]byte) *atree.PersistentSlabStorage {
	baseStorage := test_utils.NewInMemBaseStorageFromMap(data)
	return newTestPersistentStorageWithBaseStorage(t, baseStorage)
}

func newTestPersistentStorageWithBaseStorage(t testing.TB, baseStorage atree.BaseStorage) *atree.PersistentSlabStorage {

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return atree.NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		test_utils.DecodeStorable,
		test_utils.DecodeTypeInfo,
	)
}

func newTestPersistentStorageWithBaseStorageAndDeltas(t testing.TB, baseStorage atree.BaseStorage, data map[atree.SlabID][]byte) *atree.PersistentSlabStorage {
	for id, b := range data {
		err := baseStorage.Store(id, b)
		require.NoError(t, err)
	}
	return newTestPersistentStorageWithBaseStorage(t, baseStorage)
}

func newTestBasicStorage(t testing.TB) *atree.BasicSlabStorage {
	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return atree.NewBasicSlabStorage(
		encMode,
		decMode,
		test_utils.DecodeStorable,
		test_utils.DecodeTypeInfo,
	)
}

// SlabID and ValueID test util functions

func NewSlabIDFromRawAddressAndIndex(rawAddress, rawIndex []byte) atree.SlabID {
	var address atree.Address
	copy(address[:], rawAddress)

	var index atree.SlabIndex
	copy(index[:], rawIndex)

	return atree.NewSlabID(address, index)
}

func valueIDToSlabID(vid atree.ValueID) atree.SlabID {
	return NewSlabIDFromRawAddressAndIndex(
		vid[:atree.SlabAddressLength],
		vid[atree.SlabAddressLength:],
	)
}

func testInlinedMapIDs(t *testing.T, address atree.Address, m *atree.OrderedMap) {
	testInlinedSlabIDAndValueID(t, address, m.SlabID(), m.ValueID())
}

func testNotInlinedMapIDs(t *testing.T, address atree.Address, m *atree.OrderedMap) {
	testNotInlinedSlabIDAndValueID(t, address, m.SlabID(), m.ValueID())
}

func testInlinedSlabIDAndValueID(t *testing.T, expectedAddress atree.Address, slabID atree.SlabID, valueID atree.ValueID) {
	require.Equal(t, atree.SlabIDUndefined, slabID)

	require.Equal(t, expectedAddress[:], valueID[:atree.SlabAddressLength])
	require.NotEqual(t, atree.SlabIndexUndefined[:], valueID[atree.SlabAddressLength:])
}

func testNotInlinedSlabIDAndValueID(t *testing.T, expectedAddress atree.Address, slabID atree.SlabID, valueID atree.ValueID) {
	require.Equal(t, expectedAddress, slabID.Address())
	require.NotEqual(t, atree.SlabIndexUndefined, slabID.Index())

	testEqualValueIDAndSlabID(t, slabID, valueID)
}

func testEqualValueIDAndSlabID(t *testing.T, slabID atree.SlabID, valueID atree.ValueID) {
	sidAddress := slabID.Address()
	sidIndex := slabID.Index()

	require.Equal(t, sidAddress[:], valueID[:atree.SlabAddressLength])
	require.Equal(t, sidIndex[:], valueID[atree.SlabAddressLength:])
}
