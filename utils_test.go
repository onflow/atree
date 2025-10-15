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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"

	testutils "github.com/onflow/atree/test_utils"
)

const (
	uint8Type int = iota
	uint16Type
	uint32Type
	uint64Type
	smallStringType
	largeStringType
	maxSimpleValueType
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
	for i := range b {
		b[i] = runes[r.Intn(len(runes))]
	}
	return string(b)
}

func randomValue(r *rand.Rand, maxInlineSize uint32) atree.Value {
	switch r.Intn(maxSimpleValueType) {

	case uint8Type:
		return testutils.Uint8Value(r.Intn(255))

	case uint16Type:
		return testutils.Uint16Value(r.Intn(6535))

	case uint32Type:
		return testutils.Uint32Value(r.Intn(4294967295))

	case uint64Type:
		return testutils.Uint64Value(r.Intn(1844674407370955161))

	case smallStringType: // small string (inlinable)
		slen := r.Intn(int(maxInlineSize))
		return testutils.NewStringValue(randStr(r, slen))

	case largeStringType: // large string (external)
		slen := r.Intn(1024) + int(maxInlineSize)
		return testutils.NewStringValue(randStr(r, slen))

	default:
		panic(atree.NewUnreachableError())
	}
}

func testValueEqual(t *testing.T, expected atree.Value, actual atree.Value) {
	equal, err := testutils.ValueEqual(expected, actual)
	require.NoError(t, err)
	require.True(t, equal)
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
	baseStorage := testutils.NewInMemBaseStorage()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return atree.NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		testutils.DecodeStorable,
		testutils.DecodeTypeInfo,
	)
}

func newTestPersistentStorageWithData(t testing.TB, data map[atree.SlabID][]byte) *atree.PersistentSlabStorage {
	baseStorage := testutils.NewInMemBaseStorageFromMap(data)
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
		testutils.DecodeStorable,
		testutils.DecodeTypeInfo,
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
		testutils.DecodeStorable,
		testutils.DecodeTypeInfo,
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

func getRandomArrayIndex(r *rand.Rand, array *atree.Array) uint64 {
	return uint64(r.Intn(int(array.Count())))
}

func getRandomArrayIndexes(r *rand.Rand, array *atree.Array, count int) []uint64 {
	set := make(map[uint64]struct{})
	for len(set) < count {
		n := getRandomArrayIndex(r, array)
		set[n] = struct{}{}
	}

	slice := make([]uint64, 0, count)
	for n := range set {
		slice = append(slice, n)
	}

	return slice
}

// getRandomUint64InRange returns a number in the range of [min, max)
func getRandomUint64InRange(r *rand.Rand, minNum uint64, maxNum uint64) uint64 {
	if minNum >= maxNum {
		panic(fmt.Sprintf("min %d >= max %d", minNum, maxNum))
	}
	// since minNum < maxNum, maxNum - minNum >= 1
	return minNum + uint64(r.Intn(int(maxNum-minNum)))
}

type uint64Slice []uint64

func (x uint64Slice) Len() int           { return len(x) }
func (x uint64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x uint64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
