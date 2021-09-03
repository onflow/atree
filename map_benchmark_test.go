/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

// GENERAL COMMENT:
// running this test with
//   go test -run=none -bench=Map  -benchmem
// will track the heap allocations for the Benchmarks

type Map interface {
	Get(key ComparableValue) (Storable, error)
	Set(key ComparableValue, value Value) (Storable, error)
	Remove(key ComparableValue) (Storable, Storable, error)

	Count() uint64
	StorageID() StorageID
	Print()
}

var _ Map = &OrderedMap{}
var _ Map = &BasicDict{}

type MapFactory interface {
	NewMap(storage SlabStorage, address Address) (Map, error)
	NewMapWithRootID(storage SlabStorage, id StorageID) (Map, error)
}

type OrderedMapFactory struct {
	digesterBuilder DigesterBuilder
	typeInfo        cbor.RawMessage
}

var _ MapFactory = &OrderedMapFactory{}

func NewOrderedMapFactory(digesterBuilder DigesterBuilder, typeInfo cbor.RawMessage) MapFactory {
	return &OrderedMapFactory{digesterBuilder: digesterBuilder, typeInfo: typeInfo}
}

func (m *OrderedMapFactory) NewMap(storage SlabStorage, address Address) (Map, error) {
	return NewMap(storage, address, m.digesterBuilder, m.typeInfo)
}

func (m *OrderedMapFactory) NewMapWithRootID(storage SlabStorage, id StorageID) (Map, error) {
	return NewMapWithRootID(storage, id, m.digesterBuilder)
}

type BasicDictFactory struct {
}

var _ MapFactory = &BasicDictFactory{}

func NewBasicDictFactory() MapFactory {
	return &BasicDictFactory{}
}

func (m *BasicDictFactory) NewMap(storage SlabStorage, address Address) (Map, error) {
	return NewBasicDict(storage, address), nil
}

func (m *BasicDictFactory) NewMapWithRootID(storage SlabStorage, id StorageID) (Map, error) {
	return NewBasicDictWithRootID(storage, id)
}

const mapOpCount = 100

func BenchmarkXSMapUint64Key(b *testing.B) {
	benchmarkMap(b, 100, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomUint64MapKey)
}

func BenchmarkXSBasicMapUint64Key(b *testing.B) {
	benchmarkMap(b, 100, mapOpCount, NewBasicDictFactory(), RandomUint64MapKey)
}

func BenchmarkSMapUint64Key(b *testing.B) {
	benchmarkMap(b, 1000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomUint64MapKey)
}

func BenchmarkSBasicMapUint64Key(b *testing.B) {
	benchmarkMap(b, 1000, mapOpCount, NewBasicDictFactory(), RandomUint64MapKey)
}

func BenchmarkMMapUint64Key(b *testing.B) {
	benchmarkMap(b, 10_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomUint64MapKey)
}

func BenchmarkMBasicMapUint64Key(b *testing.B) {
	benchmarkMap(b, 10_000, mapOpCount, NewBasicDictFactory(), RandomUint64MapKey)
}

func BenchmarkLMapUint64Key(b *testing.B) {
	benchmarkMap(b, 100_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomUint64MapKey)
}

func BenchmarkLBasicMapUint64Key(b *testing.B) {
	benchmarkMap(b, 100_000, mapOpCount, NewBasicDictFactory(), RandomUint64MapKey)
}

func BenchmarkXLMapUint64Key(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXLMapUint64Key in short mode.")
	}
	benchmarkMap(b, 1_000_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomUint64MapKey)
}

func BenchmarkXLBaiscMapUint64Key(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXLBaiscMapUint64Key in short mode.")
	}
	benchmarkMap(b, 1_000_000, mapOpCount, NewBasicDictFactory(), RandomUint64MapKey)
}

func BenchmarkXXLMapUint64Key(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXLMapUint64Key in short mode.")
	}
	benchmarkMap(b, 10_000_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomUint64MapKey)
}

func BenchmarkXXLBasicMapUint64Key(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXLBasicMapUint64Key in short mode.")
	}
	benchmarkMap(b, 10_000_000, mapOpCount, NewBasicDictFactory(), RandomUint64MapKey)
}

func BenchmarkXXXLMapUint64Key(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXXLMapUint64Key in short mode.")
	}
	benchmarkMap(b, 100_000_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomUint64MapKey)
}

func BenchmarkXXXLBasicMapUint64Key(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXXLBasicMapUint64Key in short mode.")
	}
	benchmarkMap(b, 100_000_000, mapOpCount, NewBasicDictFactory(), RandomUint64MapKey)
}

func BenchmarkXSMap(b *testing.B) {
	benchmarkMap(b, 100, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomMapKey)
}

func BenchmarkXSBasicMap(b *testing.B) {
	benchmarkMap(b, 100, mapOpCount, NewBasicDictFactory(), RandomMapKey)
}

func BenchmarkSMap(b *testing.B) {
	benchmarkMap(b, 1000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomMapKey)
}

func BenchmarkSBasicMap(b *testing.B) {
	benchmarkMap(b, 1000, mapOpCount, NewBasicDictFactory(), RandomMapKey)
}

func BenchmarkMMap(b *testing.B) {
	benchmarkMap(b, 10_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomMapKey)
}

func BenchmarkMBasicMap(b *testing.B) {
	benchmarkMap(b, 10_000, mapOpCount, NewBasicDictFactory(), RandomMapKey)
}

func BenchmarkLMap(b *testing.B) {
	benchmarkMap(b, 100_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomMapKey)
}

func BenchmarkLBasicMap(b *testing.B) {
	benchmarkMap(b, 100_000, mapOpCount, NewBasicDictFactory(), RandomMapKey)
}

func BenchmarkXLMap(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXLMap in short mode.")
	}
	benchmarkMap(b, 1_000_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomMapKey)
}

func BenchmarkXLBaiscMap(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXLBaiscMap in short mode.")
	}
	benchmarkMap(b, 1_000_000, mapOpCount, NewBasicDictFactory(), RandomMapKey)
}

func BenchmarkXXLMap(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXLMap in short mode.")
	}
	benchmarkMap(b, 10_000_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomMapKey)
}

func BenchmarkXXLBasicMap(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXLBasicMap in short mode.")
	}
	benchmarkMap(b, 10_000_000, mapOpCount, NewBasicDictFactory(), RandomMapKey)
}

func BenchmarkXXXLMap(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXXLMap in short mode.")
	}
	benchmarkMap(b, 100_000_000, mapOpCount, NewOrderedMapFactory(newBasicDigesterBuilder(), cbor.RawMessage{0x18, 0x2A}), RandomMapKey)
}

func BenchmarkXXXLBasicMap(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXXLBasicMap in short mode.")
	}
	benchmarkMap(b, 100_000_000, mapOpCount, NewBasicDictFactory(), RandomMapKey)
}

type randomKeyFunc func() ComparableValue

func RandomUint64MapKey() ComparableValue {
	return Uint64Value(rand.Intn(1844674407370955161))
}

func RandomMapKey() ComparableValue {
	switch rand.Intn(5) {
	case 0:
		return Uint8Value(rand.Intn(255))
	case 1:
		return Uint16Value(rand.Intn(6535))
	case 2:
		return Uint32Value(rand.Intn(4294967295))
	case 3:
		return Uint64Value(rand.Intn(1844674407370955161))
	case 4:
		return NewStringValue(randStr(rand.Intn(512)))
	default:
		return Uint8Value(rand.Intn(255))
	}
}

func RandomMapValue() Value {
	switch rand.Intn(5) {
	case 0:
		return Uint8Value(rand.Intn(255))
	case 1:
		return Uint16Value(rand.Intn(6535))
	case 2:
		return Uint32Value(rand.Intn(4294967295))
	case 3:
		return Uint64Value(rand.Intn(1844674407370955161))
	case 4:
		return NewStringValue(randStr(rand.Intn(512)))
	default:
		return Uint8Value(rand.Intn(255))
	}
}

// benchmarkMap benchmarks the performance of the atree map
func benchmarkMap(b *testing.B, initialMapSize, numberOfElements int, mapFactory MapFactory, randomKey randomKeyFunc) {

	rand.Seed(time.Now().UnixNano())

	storage := newTestPersistentStorage(b)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := mapFactory.NewMap(storage, address)

	require.NoError(b, err)

	var start time.Time
	var totalRawDataSize uint32
	var totalSetTime time.Duration
	var totalRemoveTime time.Duration
	var totalLookupTime time.Duration

	keys := make([]ComparableValue, 0, initialMapSize+numberOfElements)

	// setup
	for m.Count() < uint64(initialMapSize) {

		k := randomKey()

		v := RandomMapValue()

		existingStorable, err := m.Set(k, v)
		require.NoError(b, err)

		if existingStorable == nil {
			keys = append(keys, k)

			storable, err := k.Storable(storage, address, math.MaxUint64)
			require.NoError(b, err)
			totalRawDataSize += storable.ByteSize()
		}

		storable, err := v.Storable(storage, address, math.MaxUint64)
		require.NoError(b, err)
		totalRawDataSize += storable.ByteSize()
	}

	require.Equal(b, uint64(initialMapSize), m.Count())

	require.NoError(b, storage.Commit())
	b.ResetTimer()

	mapID := m.StorageID()

	// set
	storage.DropCache()
	start = time.Now()
	m, err = mapFactory.NewMapWithRootID(storage, mapID)
	require.NoError(b, err)

	for i := 0; i < numberOfElements; i++ {

		k := randomKey()
		v := RandomMapValue()

		existingStorable, err := m.Set(k, v)
		require.NoError(b, err)

		if existingStorable == nil {

			keys = append(keys, k)

			storable, err := k.Storable(storage, address, math.MaxUint64)
			require.NoError(b, err)
			totalRawDataSize += storable.ByteSize()
		}

		storable, err := v.Storable(storage, address, math.MaxUint64)
		require.NoError(b, err)
		totalRawDataSize += storable.ByteSize()
	}
	require.NoError(b, storage.Commit())
	totalSetTime = time.Since(start)

	// remove
	storage.DropCache()
	start = time.Now()

	m, err = mapFactory.NewMapWithRootID(storage, mapID)
	require.NoError(b, err)

	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(len(keys))
		key := keys[ind]

		keyStorable, vStorable, err := m.Remove(key)
		require.NoError(b, err)

		totalRawDataSize -= keyStorable.ByteSize()

		totalRawDataSize -= vStorable.ByteSize()

		copy(keys[ind:], keys[ind+1:])
		keys = keys[:len(keys)-1]
	}
	require.NoError(b, storage.Commit())
	totalRemoveTime = time.Since(start)

	// lookup
	storage.DropCache()
	start = time.Now()

	m, err = mapFactory.NewMapWithRootID(storage, mapID)
	require.NoError(b, err)

	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(len(keys))
		_, err := m.Get(keys[ind])
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalLookupTime = time.Since(start)

	// random lookup
	storage.baseStorage.ResetReporter()
	storage.DropCache()

	m, err = mapFactory.NewMapWithRootID(storage, mapID)
	require.NoError(b, err)

	ind := rand.Intn(len(keys))
	_, err = m.Get(keys[ind])
	require.NoError(b, err)

	storageOverheadRatio := float64(storage.baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(storage.baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(storage.baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(storage.baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
	b.ReportMetric(float64(storage.baseStorage.BytesRetrieved()), "storage_bytes_loaded_for_lookup")
	// b.ReportMetric(float64(m.Count()), "number_of_elements")
	b.ReportMetric(float64(int(totalSetTime)), fmt.Sprintf("set_%d_time_(ns)", numberOfElements))
	b.ReportMetric(float64(int(totalRemoveTime)), fmt.Sprintf("remove_%d_time_(ns)", numberOfElements))
	b.ReportMetric(float64(int(totalLookupTime)), fmt.Sprintf("lookup_%d_time_(ns)", numberOfElements))
}

func BenchmarkLMapMemoryImpactWithUint64Key(b *testing.B) {
	benchmarkMapLongTermImpactOnMemory(b, 10_000, 1000_000, newBasicDigesterBuilder(), RandomUint64MapKey)
}

func BenchmarkLMapMemoryImpact(b *testing.B) {
	benchmarkMapLongTermImpactOnMemory(b, 10_000, 1000_000, newBasicDigesterBuilder(), RandomMapKey)
}

func benchmarkMapLongTermImpactOnMemory(b *testing.B, initialMapSize, numberOfOps int, digesterBuilder DigesterBuilder, randomKey randomKeyFunc) {

	rand.Seed(time.Now().UnixNano())

	storage := newTestPersistentStorage(b)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)

	require.NoError(b, err)

	var totalRawDataSize uint32

	var keys []ComparableValue

	// setup
	for m.Count() < uint64(initialMapSize) {

		k := randomKey()

		v := RandomMapValue()

		existingStorable, err := m.Set(k, v)
		require.NoError(b, err)

		if existingStorable == nil {
			keys = append(keys, k)

			storable, err := k.Storable(storage, m.Address(), math.MaxUint64)
			require.NoError(b, err)
			totalRawDataSize += storable.ByteSize()
		}

		storable, err := v.Storable(storage, m.Address(), math.MaxUint64)
		require.NoError(b, err)
		totalRawDataSize += storable.ByteSize()

	}
	require.NoError(b, storage.Commit())
	b.ResetTimer()

	for i := 0; i < numberOfOps; i++ {
		action := rand.Intn(2)
		if len(keys) == 0 {
			action = 1
		}
		// select opt
		switch action {
		case 0: // remove
			ind := rand.Intn(len(keys))
			keyStorable, vStorable, err := m.Remove(keys[ind])
			require.NoError(b, err)

			totalRawDataSize -= keyStorable.ByteSize()

			totalRawDataSize -= vStorable.ByteSize()

			copy(keys[ind:], keys[ind+1:])
			keys = keys[:len(keys)-1]
		case 1: // set
			k := randomKey()
			v := RandomMapValue()

			storable, err := k.Storable(storage, m.Address(), math.MaxUint64)
			require.NoError(b, err)
			totalRawDataSize += storable.ByteSize()

			storable, err = v.Storable(storage, m.Address(), math.MaxUint64)
			require.NoError(b, err)
			totalRawDataSize += storable.ByteSize()

			_, err = m.Set(k, v)
			require.NoError(b, err)
		}
	}

	require.NoError(b, storage.Commit())

	storageOverheadRatio := float64(storage.baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(storage.baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(storage.baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(storage.baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
}
