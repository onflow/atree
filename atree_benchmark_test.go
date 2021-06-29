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
func BenchmarkSmallArray(b *testing.B) { benchmarkArray(b, 10, 100) }

func BenchmarkMidArray(b *testing.B) { benchmarkArray(b, 1000, 100) }

func BenchmarkLargeArray(b *testing.B) { benchmarkArray(b, 100_000, 100) }

func BenchmarkHugeArray(b *testing.B) { benchmarkArray(b, 10_000_000, 100) }

func BenchmarkSuperHugeArray(b *testing.B) { benchmarkArray(b, 100_000_000, 100) }

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
	storage := NewBasicSlabStorage()
	array := NewArray(storage)

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
		totalRawDataSize -= s.ByteSize()
		require.NoError(b, err)
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

	// storageOverheadRatio := float64(storage.Size()) / float64(basicArraySize)
	// b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
	b.ReportMetric(float64(int(totalAppendTime)/numberOfElements), "avg_append_time_(ns)")
	b.ReportMetric(float64(int(totalRemoveTime)/numberOfElements), "avg_remove_time_(ns)")
	b.ReportMetric(float64(int(totalInsertTime)/numberOfElements), "avg_insert_time_(ns)")
	b.ReportMetric(float64(int(totalLookupTime)/numberOfElements), "avg_lookup_time_(ns)")
	// b.ReportMetric(float64(storage.BytesRetrieved()), "bytes_retrieved")
	// b.ReportMetric(float64(storage.BytesStored()), "bytes_stored")
	// b.ReportMetric(float64(storage.SegmentTouched()), "segments_touched"
}
