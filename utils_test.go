/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2021 Dapper Labs, Inc.
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

func (i testTypeInfo) Encode(enc *cbor.StreamEncoder) error {
	return enc.EncodeUint64(i.value)
}

func (i testTypeInfo) Equal(other TypeInfo) bool {
	otherTestTypeInfo, ok := other.(testTypeInfo)
	return ok && i.value == otherTestTypeInfo.value
}

func typeInfoComparator(a, b TypeInfo) bool {
	x, ok := a.(testTypeInfo)
	if !ok {
		return false
	}
	y, ok := b.(testTypeInfo)
	return ok && x.value == y.value
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

func valueEqual(t *testing.T, tic TypeInfoComparator, a Value, b Value) {
	switch a.(type) {
	case *Array:
		arrayEqual(t, tic, a, b)
	case *OrderedMap:
		mapEqual(t, tic, a, b)
	default:
		require.Equal(t, a, b)
	}
}

func arrayEqual(t *testing.T, tic TypeInfoComparator, a Value, b Value) {
	array1, ok := a.(*Array)
	require.True(t, ok)

	array2, ok := b.(*Array)
	require.True(t, ok)

	require.True(t, tic(array1.Type(), array2.Type()))
	require.Equal(t, array1.Address(), array2.Address())
	require.Equal(t, array1.Count(), array2.Count())
	require.Equal(t, array1.SlabID(), array2.SlabID())

	iterator1, err := array1.Iterator()
	require.NoError(t, err)

	iterator2, err := array2.Iterator()
	require.NoError(t, err)

	for {
		value1, err := iterator1.Next()
		require.NoError(t, err)

		value2, err := iterator2.Next()
		require.NoError(t, err)

		valueEqual(t, tic, value1, value2)

		if value1 == nil || value2 == nil {
			break
		}
	}
}

func mapEqual(t *testing.T, tic TypeInfoComparator, a Value, b Value) {
	m1, ok := a.(*OrderedMap)
	require.True(t, ok)

	m2, ok := b.(*OrderedMap)
	require.True(t, ok)

	require.True(t, tic(m1.Type(), m2.Type()))
	require.Equal(t, m1.Address(), m2.Address())
	require.Equal(t, m1.Count(), m2.Count())
	require.Equal(t, m1.SlabID(), m2.SlabID())

	iterator1, err := m1.Iterator()
	require.NoError(t, err)

	iterator2, err := m2.Iterator()
	require.NoError(t, err)

	for {
		key1, value1, err := iterator1.Next()
		require.NoError(t, err)

		key2, value2, err := iterator2.Next()
		require.NoError(t, err)

		valueEqual(t, tic, key1, key2)
		valueEqual(t, tic, value1, value2)

		if key1 == nil || key2 == nil {
			break
		}
	}
}
