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
func BenchmarkXSBasicArray(b *testing.B) { benchmarkBasicArray(b, 10, 100) }

func BenchmarkSBasicArray(b *testing.B) { benchmarkBasicArray(b, 1000, 100) }

func BenchmarkMBasicArray(b *testing.B) { benchmarkBasicArray(b, 10_000, 100) }

func BenchmarkLBasicArray(b *testing.B) { benchmarkBasicArray(b, 100_000, 100) }

func BenchmarkXLBasicArray(b *testing.B) { benchmarkBasicArray(b, 1_000_000, 100) }

func BenchmarkXXLBasicArray(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXLArray in short mode")
	}
	benchmarkBasicArray(b, 10_000_000, 100)
}

func BenchmarkXXXLBasicArray(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkXXXLArray in short mode")
	}
	benchmarkBasicArray(b, 100_000_000, 100)
}

// BenchmarkBasicArray benchmarks the performance of basic array
func benchmarkBasicArray(b *testing.B, initialArraySize, numberOfElements int) {

	seed := time.Now().UnixNano()
	//fmt.Printf("benchmarkBasicArray seed: 0x%x\n", seed)
	rand.Seed(seed)

	storage := newTestPersistentStorage(b)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array := NewBasicArray(storage, address)

	// TODO capture arrayID here ?

	var start time.Time
	var totalRawDataSize uint32
	var totalAppendTime time.Duration
	var totalRemoveTime time.Duration
	var totalInsertTime time.Duration
	var totalLookupTime time.Duration

	// setup
	for i := 0; i < initialArraySize; i++ {
		v := RandomValue()
		totalRawDataSize += v.Storable(storage, array.Address()).ByteSize()
		err := array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	b.ResetTimer()

	arrayID := array.StorageID()

	// append
	storage.DropCache()
	array, err := NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		v := RandomValue()
		totalRawDataSize += v.Storable(storage, array.Address()).ByteSize()
		err := array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalAppendTime = time.Since(start)

	// remove
	storage.DropCache()
	array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		s, err := array.Remove(uint64(ind))
		require.NoError(b, err)
		totalRawDataSize -= s.Storable(storage, array.Address()).ByteSize()
	}
	require.NoError(b, storage.Commit())
	totalRemoveTime = time.Since(start)

	// insert
	storage.DropCache()
	array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		v := RandomValue()
		totalRawDataSize += v.Storable(storage, array.Address()).ByteSize()
		err := array.Insert(uint64(ind), v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalInsertTime = time.Since(start)

	// lookup
	storage.DropCache()
	array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
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
	array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	ind := rand.Intn(int(array.Count()))
	_, err = array.Get(uint64(ind))
	require.NoError(b, err)
	storageOverheadRatio := float64(storage.baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(storage.baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(storage.baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(storage.baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(float64(storage.baseStorage.BytesRetrieved()), "storage_bytes_loaded_for_lookup")
	b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
	b.ReportMetric(float64(array.Count()), "number_of_elements")
	b.ReportMetric(float64(int(totalAppendTime)), "append_100_time_(ns)")
	b.ReportMetric(float64(int(totalRemoveTime)), "remove_100_time_(ns)")
	b.ReportMetric(float64(int(totalInsertTime)), "insert_100_time_(ns)")
	b.ReportMetric(float64(int(totalLookupTime)), "lookup_100_time_(ns)")
	// b.ReportMetric(float64(storage.BytesRetrieved()), "bytes_retrieved")
	// b.ReportMetric(float64(storage.BytesStored()), "bytes_stored")
	// b.ReportMetric(float64(storage.SegmentTouched()), "segments_touched"
}
