package main

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
func BenchmarkXSArray(b *testing.B) { benchmarkArray(b, 10, 100) }

func BenchmarkSArray(b *testing.B) { benchmarkArray(b, 1000, 100) }

func BenchmarkMArray(b *testing.B) { benchmarkArray(b, 10_000, 100) }

func BenchmarkLArray(b *testing.B) { benchmarkArray(b, 100_000, 100) }

func BenchmarkXLArray(b *testing.B) { benchmarkArray(b, 1_000_000, 100) }

func BenchmarkXXLArray(b *testing.B) { benchmarkArray(b, 10_000_000, 100) }

func BenchmarkXXXLArray(b *testing.B) { benchmarkArray(b, 100_000_000, 100) }

// TODO add nested arrays as class 5
func RandomValue() Storable {
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

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage, WithNoAutoCommit())

	array := NewArray(storage)
	// array := NewBasicArray(storage)

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
		totalRawDataSize += v.ByteSize()
		err := array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	b.ResetTimer()

	arrayID := array.StorageID()

	// append
	storage.DropCache()
	array, err := NewArrayWithRootID(storage, arrayID)
	// array, err := NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		v := RandomValue()
		totalRawDataSize += v.ByteSize()
		err := array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalAppendTime = time.Since(start)

	// remove
	storage.DropCache()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		s, err := array.Remove(uint64(ind))
		require.NoError(b, err)
		totalRawDataSize -= s.ByteSize()
	}
	require.NoError(b, storage.Commit())
	totalRemoveTime = time.Since(start)

	// insert
	storage.DropCache()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		v := RandomValue()
		totalRawDataSize += v.ByteSize()
		err := array.Insert(uint64(ind), v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalInsertTime = time.Since(start)

	// lookup
	storage.DropCache()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
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
	baseStorage.ResetReporter()
	storage.DropCache()
	array, err = NewArrayWithRootID(storage, arrayID)
	// array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	ind := rand.Intn(int(array.Count()))
	_, err = array.Get(uint64(ind))
	require.NoError(b, err)
	storageOverheadRatio := float64(baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(float64(baseStorage.BytesRetrieved()), "storage_bytes_loaded_for_lookup")
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
