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

func newTestPersistentStorageWithData(t testing.TB, data map[StorageID][]byte) *PersistentSlabStorage {
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
	require.Equal(t, array1.StorageID(), array2.StorageID())

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
	require.Equal(t, m1.StorageID(), m2.StorageID())

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

func encodeSlabs(cborEncMode cbor.EncMode, slabs map[StorageID]Slab) (map[StorageID][]byte, error) {
	m := make(map[StorageID][]byte)
	for id, slab := range slabs {
		b, err := Encode(slab, cborEncMode)
		if err != nil {
			return nil, err
		}
		m[id] = b
	}
	return m, nil
}
