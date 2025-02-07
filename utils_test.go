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
		return Uint8Value(r.Intn(255))

	case 1:
		return Uint16Value(r.Intn(6535))

	case 2:
		return Uint32Value(r.Intn(4294967295))

	case 3:
		return Uint64Value(r.Intn(1844674407370955161))

	case 4: // small string (inlinable)
		slen := r.Intn(maxInlineSize)
		return NewStringValue(randStr(r, slen))

	case 5: // large string (external)
		slen := r.Intn(1024) + maxInlineSize
		return NewStringValue(randStr(r, slen))

	default:
		panic(atree.NewUnreachableError())
	}
}

type testTypeInfo struct {
	value uint64
}

var _ atree.TypeInfo = testTypeInfo{}

func (i testTypeInfo) Copy() atree.TypeInfo {
	return i
}

func (i testTypeInfo) IsComposite() bool {
	return false
}

func (i testTypeInfo) Identifier() string {
	return fmt.Sprintf("uint64(%d)", i)
}

func (i testTypeInfo) Encode(enc *cbor.StreamEncoder) error {
	return enc.EncodeUint64(i.value)
}

func (i testTypeInfo) Equal(other atree.TypeInfo) bool {
	otherTestTypeInfo, ok := other.(testTypeInfo)
	return ok && i.value == otherTestTypeInfo.value
}

const testCompositeTypeInfoTagNum = 246

type testCompositeTypeInfo struct {
	value uint64
}

var _ atree.TypeInfo = testCompositeTypeInfo{}

func (i testCompositeTypeInfo) Copy() atree.TypeInfo {
	return i
}

func (i testCompositeTypeInfo) IsComposite() bool {
	return true
}

func (i testCompositeTypeInfo) Identifier() string {
	return fmt.Sprintf("composite(%d)", i)
}

func (i testCompositeTypeInfo) Encode(enc *cbor.StreamEncoder) error {
	err := enc.EncodeTagHead(testCompositeTypeInfoTagNum)
	if err != nil {
		return err
	}
	return enc.EncodeUint64(i.value)
}

func (i testCompositeTypeInfo) Equal(other atree.TypeInfo) bool {
	otherTestTypeInfo, ok := other.(testCompositeTypeInfo)
	return ok && i.value == otherTestTypeInfo.value
}

func typeInfoComparator(a, b atree.TypeInfo) bool {
	switch a := a.(type) {
	case testTypeInfo:
		return a.Equal(b)

	case testCompositeTypeInfo:
		return a.Equal(b)

	default:
		return false
	}
}

func newTestPersistentStorage(t testing.TB) *atree.PersistentSlabStorage {
	baseStorage := NewInMemBaseStorage()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return atree.NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		decodeStorable,
		decodeTypeInfo,
	)
}

func newTestPersistentStorageWithData(t testing.TB, data map[atree.SlabID][]byte) *atree.PersistentSlabStorage {
	baseStorage := NewInMemBaseStorage()
	baseStorage.segments = data
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
		decodeStorable,
		decodeTypeInfo,
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
		decodeStorable,
		decodeTypeInfo,
	)
}

type InMemBaseStorage struct {
	segments         map[atree.SlabID][]byte
	slabIndex        map[atree.Address]atree.SlabIndex
	bytesRetrieved   int
	bytesStored      int
	segmentsReturned map[atree.SlabID]struct{}
	segmentsUpdated  map[atree.SlabID]struct{}
	segmentsTouched  map[atree.SlabID]struct{}
}

var _ atree.BaseStorage = &InMemBaseStorage{}

func NewInMemBaseStorage() *InMemBaseStorage {
	return NewInMemBaseStorageFromMap(
		make(map[atree.SlabID][]byte),
	)
}

func NewInMemBaseStorageFromMap(segments map[atree.SlabID][]byte) *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:         segments,
		slabIndex:        make(map[atree.Address]atree.SlabIndex),
		segmentsReturned: make(map[atree.SlabID]struct{}),
		segmentsUpdated:  make(map[atree.SlabID]struct{}),
		segmentsTouched:  make(map[atree.SlabID]struct{}),
	}
}

func (s *InMemBaseStorage) Retrieve(id atree.SlabID) ([]byte, bool, error) {
	seg, ok := s.segments[id]
	s.bytesRetrieved += len(seg)
	s.segmentsReturned[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return seg, ok, nil
}

func (s *InMemBaseStorage) Store(id atree.SlabID, data []byte) error {
	s.segments[id] = data
	s.bytesStored += len(data)
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return nil
}

func (s *InMemBaseStorage) Remove(id atree.SlabID) error {
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	delete(s.segments, id)
	return nil
}

func (s *InMemBaseStorage) GenerateSlabID(address atree.Address) (atree.SlabID, error) {
	index := s.slabIndex[address]
	nextIndex := index.Next()

	s.slabIndex[address] = nextIndex
	return atree.NewSlabID(address, nextIndex), nil
}

func (s *InMemBaseStorage) SegmentCounts() int {
	return len(s.segments)
}

func (s *InMemBaseStorage) Size() int {
	total := 0
	for _, seg := range s.segments {
		total += len(seg)
	}
	return total
}

func (s *InMemBaseStorage) BytesRetrieved() int {
	return s.bytesRetrieved
}

func (s *InMemBaseStorage) BytesStored() int {
	return s.bytesStored
}

func (s *InMemBaseStorage) SegmentsReturned() int {
	return len(s.segmentsReturned)
}

func (s *InMemBaseStorage) SegmentsUpdated() int {
	return len(s.segmentsUpdated)
}

func (s *InMemBaseStorage) SegmentsTouched() int {
	return len(s.segmentsTouched)
}

func (s *InMemBaseStorage) ResetReporter() {
	s.bytesStored = 0
	s.bytesRetrieved = 0
	s.segmentsReturned = make(map[atree.SlabID]struct{})
	s.segmentsUpdated = make(map[atree.SlabID]struct{})
	s.segmentsTouched = make(map[atree.SlabID]struct{})
}

func valueEqual(t *testing.T, expected atree.Value, actual atree.Value) {
	switch expected := expected.(type) {
	case arrayValue:
		actual, ok := actual.(*atree.Array)
		require.True(t, ok)

		arrayEqual(t, expected, actual)

	case *atree.Array:
		require.FailNow(t, "expected value shouldn't be *atree.Array")

	case mapValue:
		actual, ok := actual.(*atree.OrderedMap)
		require.True(t, ok)

		mapEqual(t, expected, actual)

	case *atree.OrderedMap:
		require.FailNow(t, "expected value shouldn't be *atree.OrderedMap")

	case someValue:
		actual, ok := actual.(SomeValue)
		require.True(t, ok)

		valueEqual(t, expected.Value, actual.Value)

	case SomeValue:
		require.FailNow(t, "expected value shouldn't be SomeValue")

	default:
		require.Equal(t, expected, actual)
	}
}

func arrayEqual(t *testing.T, expected arrayValue, actual *atree.Array) {
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

		valueEqual(t, expected[i], actualValue)
		i++
	}
	require.Equal(t, len(expected), i)
}

func mapEqual(t *testing.T, expected mapValue, actual *atree.OrderedMap) {
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

		valueEqual(t, expectedValue, actualValue)
		i++
	}
	require.Equal(t, len(expected), i)
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

type arrayValue []atree.Value

var _ atree.Value = &arrayValue{}

func (v arrayValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

type mapValue map[atree.Value]atree.Value

var _ atree.Value = &mapValue{}

func (v mapValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

type someValue struct {
	Value atree.Value
}

var _ atree.Value = &someValue{}

func (v someValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

func GetDeltasCount(storage *atree.PersistentSlabStorage) int {
	return len(atree.GetDeltas(storage))
}

func GetCacheCount(storage *atree.PersistentSlabStorage) int {
	return len(atree.GetCache(storage))
}

func NewSlabIDFromRawAddressAndIndex(rawAddress, rawIndex []byte) atree.SlabID {
	var address atree.Address
	copy(address[:], rawAddress)

	var index atree.SlabIndex
	copy(index[:], rawIndex)

	return atree.NewSlabID(address, index)
}

func testEqualValueIDAndSlabID(t *testing.T, slabID atree.SlabID, valueID atree.ValueID) {
	sidAddress := slabID.Address()
	sidIndex := slabID.Index()

	require.Equal(t, sidAddress[:], valueID[:atree.SlabAddressLength])
	require.Equal(t, sidIndex[:], valueID[atree.SlabAddressLength:])
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
