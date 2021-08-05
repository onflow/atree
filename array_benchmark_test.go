/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// GENERAL COMMENT:
// running this test with
//   go test -bench=.  -benchmem
// will track the heap allocations for the Benchmarks

const opCount = 100

func BenchmarkXSArray(b *testing.B) { benchmarkArray(b, 100, opCount) }

func BenchmarkSArray(b *testing.B) { benchmarkArray(b, 1000, opCount) }

func BenchmarkMArray(b *testing.B) { benchmarkArray(b, 10_000, opCount) }

func BenchmarkLArray(b *testing.B) { benchmarkArray(b, 100_000, opCount) }

func BenchmarkXLArray(b *testing.B) { benchmarkArray(b, 1_000_000, opCount) }

func BenchmarkXXLArray(b *testing.B) { benchmarkArray(b, 10_000_000, opCount) }

func BenchmarkXXXLArray(b *testing.B) { benchmarkArray(b, 100_000_000, opCount) }

// TODO add nested arrays as class 5
func RandomValue() Value {
	switch rand.Intn(4) {
	case 0:
		return Uint8Value(rand.Intn(255))
	case 1:
		return Uint16Value(rand.Intn(6535))
	case 2:
		return Uint32Value(rand.Intn(4294967295))
	case 3:
		return Uint64Value(rand.Intn(1844674407370955161))
	default:
		return Uint8Value(rand.Intn(255))
	}
}

// BenchmarkArray benchmarks the performance of the atree array
func benchmarkArray(b *testing.B, initialArraySize, numberOfElements int) {

	rand.Seed(time.Now().UnixNano())

	storage := newTestPersistentStorage(b)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const typeInfo = "[AnyType]"

	array, err := NewArray(storage, address, typeInfo)

	require.NoError(b, err)

	// array := NewBasicArray(storage)

	var start time.Time
	var totalRawDataSize uint32
	var totalAppendTime time.Duration
	var totalRemoveTime time.Duration
	var totalInsertTime time.Duration
	var totalLookupTime time.Duration

	// setup
	for i := 0; i < initialArraySize; i++ {
		v := RandomValue()
		storable, err := v.Storable(storage, array.Address())
		require.NoError(b, err)
		totalRawDataSize += storable.ByteSize()
		err = array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	b.ResetTimer()

	arrayID := array.StorageID()

	// append
	storage.DropCache()
	start = time.Now()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err := NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)
	for i := 0; i < numberOfElements; i++ {
		v := RandomValue()
		storable, err := v.Storable(storage, array.Address())
		totalRawDataSize += storable.ByteSize()
		err = array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalAppendTime = time.Since(start)

	// remove
	storage.DropCache()
	start = time.Now()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		s, err := array.Remove(uint64(ind))
		require.NoError(b, err)
		storable, err := s.Storable(storage, array.Address())
		require.NoError(b, err)
		totalRawDataSize -= storable.ByteSize()
	}
	require.NoError(b, storage.Commit())
	totalRemoveTime = time.Since(start)

	// insert
	storage.DropCache()
	start = time.Now()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		v := RandomValue()
		storable, err := v.Storable(storage, array.Address())
		totalRawDataSize += storable.ByteSize()
		err = array.Insert(uint64(ind), v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalInsertTime = time.Since(start)

	// lookup
	storage.DropCache()
	start = time.Now()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		_, err := array.Get(uint64(ind))
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalLookupTime = time.Since(start)

	// random lookup
	storage.baseStorage.ResetReporter()
	storage.DropCache()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	ind := rand.Intn(int(array.Count()))
	_, err = array.Get(uint64(ind))
	require.NoError(b, err)
	storageOverheadRatio := float64(storage.baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(storage.baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(storage.baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(storage.baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
	b.ReportMetric(float64(storage.baseStorage.BytesRetrieved()), "storage_bytes_loaded_for_lookup")
	// b.ReportMetric(float64(array.Count()), "number_of_elements")
	b.ReportMetric(float64(int(totalAppendTime)), "append_100_time_(ns)")
	b.ReportMetric(float64(int(totalRemoveTime)), "remove_100_time_(ns)")
	b.ReportMetric(float64(int(totalInsertTime)), "insert_100_time_(ns)")
	b.ReportMetric(float64(int(totalLookupTime)), "lookup_100_time_(ns)")
}

func BenchmarkLArrayMemoryImpact(b *testing.B) { benchmarkLongTermImpactOnMemory(b, 10_000, 1000_000) }

// BenchmarkArray benchmarks the performance of the atree array
func benchmarkLongTermImpactOnMemory(b *testing.B, initialArraySize, numberOfOps int) {

	rand.Seed(time.Now().UnixNano())

	storage := newTestPersistentStorage(b)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	const typeInfo = "[AnyType]"

	array, err := NewArray(storage, address, typeInfo)

	require.NoError(b, err)

	var totalRawDataSize uint32

	// setup
	for i := 0; i < initialArraySize; i++ {
		v := RandomValue()
		storable, err := v.Storable(storage, array.Address())
		totalRawDataSize += storable.ByteSize()
		err = array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	b.ResetTimer()

	for i := 0; i < numberOfOps; i++ {
		ind := rand.Intn(int(array.Count()))
		// select opt
		switch rand.Intn(2) {
		case 0: // remove
			v, err := array.Remove(uint64(ind))
			require.NoError(b, err)
			storable, err := v.Storable(storage, array.Address())
			require.NoError(b, err)
			totalRawDataSize -= storable.ByteSize()
		case 1: // insert
			v := RandomValue()
			storable, err := v.Storable(storage, array.Address())
			totalRawDataSize += storable.ByteSize()
			err = array.Insert(uint64(ind), v)
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
