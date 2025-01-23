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
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
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

func randomValue(r *rand.Rand, maxInlineSize int) Value {
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
		panic(NewUnreachableError())
	}
}

type testTypeInfo struct {
	value uint64
}

var _ TypeInfo = testTypeInfo{}

func (i testTypeInfo) Copy() TypeInfo {
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

func (i testTypeInfo) Equal(other TypeInfo) bool {
	otherTestTypeInfo, ok := other.(testTypeInfo)
	return ok && i.value == otherTestTypeInfo.value
}

const testCompositeTypeInfoTagNum = 246

type testCompositeTypeInfo struct {
	value uint64
}

var _ TypeInfo = testCompositeTypeInfo{}

func (i testCompositeTypeInfo) Copy() TypeInfo {
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

func (i testCompositeTypeInfo) Equal(other TypeInfo) bool {
	otherTestTypeInfo, ok := other.(testCompositeTypeInfo)
	return ok && i.value == otherTestTypeInfo.value
}

func typeInfoComparator(a, b TypeInfo) bool {
	switch a := a.(type) {
	case testTypeInfo:
		return a.Equal(b)

	case testCompositeTypeInfo:
		return a.Equal(b)

	default:
		return false
	}
}

func newTestPersistentStorage(t testing.TB) *PersistentSlabStorage {
	baseStorage := NewInMemBaseStorage()

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		decodeStorable,
		decodeTypeInfo,
	)
}

func newTestPersistentStorageWithData(t testing.TB, data map[SlabID][]byte) *PersistentSlabStorage {
	baseStorage := NewInMemBaseStorage()
	baseStorage.segments = data
	return newTestPersistentStorageWithBaseStorage(t, baseStorage)
}

func newTestPersistentStorageWithBaseStorage(t testing.TB, baseStorage BaseStorage) *PersistentSlabStorage {

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		decodeStorable,
		decodeTypeInfo,
	)
}

func newTestPersistentStorageWithBaseStorageAndDeltas(t testing.TB, baseStorage BaseStorage, data map[SlabID][]byte) *PersistentSlabStorage {
	storage := newTestPersistentStorageWithBaseStorage(t, baseStorage)
	for id, b := range data {
		err := storage.baseStorage.Store(id, b)
		require.NoError(t, err)
	}
	return storage
}

func newTestBasicStorage(t testing.TB) *BasicSlabStorage {
	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	return NewBasicSlabStorage(
		encMode,
		decMode,
		decodeStorable,
		decodeTypeInfo,
	)
}

type InMemBaseStorage struct {
	segments         map[SlabID][]byte
	slabIndex        map[Address]SlabIndex
	bytesRetrieved   int
	bytesStored      int
	segmentsReturned map[SlabID]struct{}
	segmentsUpdated  map[SlabID]struct{}
	segmentsTouched  map[SlabID]struct{}
}

var _ BaseStorage = &InMemBaseStorage{}

func NewInMemBaseStorage() *InMemBaseStorage {
	return NewInMemBaseStorageFromMap(
		make(map[SlabID][]byte),
	)
}

func NewInMemBaseStorageFromMap(segments map[SlabID][]byte) *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:         segments,
		slabIndex:        make(map[Address]SlabIndex),
		segmentsReturned: make(map[SlabID]struct{}),
		segmentsUpdated:  make(map[SlabID]struct{}),
		segmentsTouched:  make(map[SlabID]struct{}),
	}
}

func (s *InMemBaseStorage) Retrieve(id SlabID) ([]byte, bool, error) {
	seg, ok := s.segments[id]
	s.bytesRetrieved += len(seg)
	s.segmentsReturned[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return seg, ok, nil
}

func (s *InMemBaseStorage) Store(id SlabID, data []byte) error {
	s.segments[id] = data
	s.bytesStored += len(data)
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return nil
}

func (s *InMemBaseStorage) Remove(id SlabID) error {
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	delete(s.segments, id)
	return nil
}

func (s *InMemBaseStorage) GenerateSlabID(address Address) (SlabID, error) {
	index := s.slabIndex[address]
	nextIndex := index.Next()

	s.slabIndex[address] = nextIndex
	return NewSlabID(address, nextIndex), nil
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
	s.segmentsReturned = make(map[SlabID]struct{})
	s.segmentsUpdated = make(map[SlabID]struct{})
	s.segmentsTouched = make(map[SlabID]struct{})
}

func valueEqual(t *testing.T, expected Value, actual Value) {
	switch expected := expected.(type) {
	case arrayValue:
		actual, ok := actual.(*Array)
		require.True(t, ok)

		arrayEqual(t, expected, actual)

	case *Array:
		require.FailNow(t, "expected value shouldn't be *Array")

	case mapValue:
		actual, ok := actual.(*OrderedMap)
		require.True(t, ok)

		mapEqual(t, expected, actual)

	case *OrderedMap:
		require.FailNow(t, "expected value shouldn't be *OrderedMap")

	default:
		require.Equal(t, expected, actual)
	}
}

func arrayEqual(t *testing.T, expected arrayValue, actual *Array) {
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

func mapEqual(t *testing.T, expected mapValue, actual *OrderedMap) {
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

func valueIDToSlabID(vid ValueID) SlabID {
	var id SlabID
	copy(id.address[:], vid[:SlabAddressLength])
	copy(id.index[:], vid[SlabAddressLength:])
	return id
}

func testInlinedMapIDs(t *testing.T, address Address, m *OrderedMap) {
	testInlinedSlabIDAndValueID(t, address, m.SlabID(), m.ValueID())
}

func testNotInlinedMapIDs(t *testing.T, address Address, m *OrderedMap) {
	testNotInlinedSlabIDAndValueID(t, address, m.SlabID(), m.ValueID())
}

func testInlinedSlabIDAndValueID(t *testing.T, expectedAddress Address, slabID SlabID, valueID ValueID) {
	require.Equal(t, SlabIDUndefined, slabID)

	require.Equal(t, expectedAddress[:], valueID[:SlabAddressLength])
	require.NotEqual(t, SlabIndexUndefined[:], valueID[SlabAddressLength:])
}

func testNotInlinedSlabIDAndValueID(t *testing.T, expectedAddress Address, slabID SlabID, valueID ValueID) {
	require.Equal(t, expectedAddress, slabID.address)
	require.NotEqual(t, SlabIndexUndefined, slabID.index)

	require.Equal(t, slabID.address[:], valueID[:SlabAddressLength])
	require.Equal(t, slabID.index[:], valueID[SlabAddressLength:])
}

type arrayValue []Value

var _ Value = &arrayValue{}

func (v arrayValue) Storable(SlabStorage, Address, uint64) (Storable, error) {
	panic("not reachable")
}

type mapValue map[Value]Value

var _ Value = &mapValue{}

func (v mapValue) Storable(SlabStorage, Address, uint64) (Storable, error) {
	panic("not reachable")
}
