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

func BenchmarkXXLArray(b *testing.B) { benchmarkArray(b, 10_000_000, 10) }

func BenchmarkXXXLArray(b *testing.B) { benchmarkArray(b, 100_000_000, 5) }

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

	// storage := NewBasicSlabStorage()
	storage := NewPersistentSlabStorage(baseStorage, WithNoAutoCommit())

	// array := NewArray(storage)
	array := NewBasicArray(storage)

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

	b.ResetTimer()
	// append
	for i := 0; i < numberOfElements; i++ {
		v := RandomValue()
		totalRawDataSize += v.ByteSize()
		start = time.Now()
		err := array.Append(v)
		totalAppendTime += time.Since(start)
		require.NoError(b, err)
	}

	// remove
	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		start = time.Now()
		s, err := array.Remove(uint64(ind))
		totalRemoveTime += time.Since(start)
		require.NoError(b, err)
		totalRawDataSize -= s.ByteSize()
	}

	// insert
	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		v := RandomValue()
		totalRawDataSize += v.ByteSize()
		start = time.Now()
		err := array.Insert(uint64(ind), v)
		totalInsertTime += time.Since(start)
		require.NoError(b, err)
	}

	// lookup
	for i := 0; i < numberOfElements; i++ {
		ind := rand.Intn(int(array.Count()))
		start = time.Now()
		_, err := array.Get(uint64(ind))
		totalLookupTime += time.Since(start)
		require.NoError(b, err)
	}

	// arrayID := array.dataSlabStorageID

	arrayID := array.root.header.id

	// random lookup
	baseStorage.ResetReporter()
	err := storage.Commit()
	require.NoError(b, err)
	storage.DropCache()

	newArray, err := NewBasicArrayWithRootID(storage, arrayID)

	// newArray, err := NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)
	ind := rand.Intn(int(newArray.Count()))
	_, err = newArray.Get(uint64(ind))
	require.NoError(b, err)
	storageOverheadRatio := float64(baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(float64(baseStorage.BytesRetrieved()), "storage_bytes_loaded_for_lookup")
	b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
	b.ReportMetric(float64(int(totalAppendTime)/numberOfElements), "avg_append_time_(ns)")
	b.ReportMetric(float64(int(totalRemoveTime)/numberOfElements), "avg_remove_time_(ns)")
	b.ReportMetric(float64(int(totalInsertTime)/numberOfElements), "avg_insert_time_(ns)")
	b.ReportMetric(float64(int(totalLookupTime)/numberOfElements), "avg_lookup_time_(ns)")
	// b.ReportMetric(float64(storage.BytesRetrieved()), "bytes_retrieved")
	// b.ReportMetric(float64(storage.BytesStored()), "bytes_stored")
	// b.ReportMetric(float64(storage.SegmentTouched()), "segments_touched"
}
