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

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := NewBasicArray(storage, address)
	require.NoError(b, err)

	// TODO capture arrayID here ?

	var start time.Time
	var totalRawDataSize uint32
	var totalAppendTime time.Duration
	var totalRemoveTime time.Duration
	var totalInsertTime time.Duration
	var totalLookupTime time.Duration

	// setup
	for i := 0; i < initialArraySize; i++ {
		v := RandomValue(r)
		storable, err := v.Storable(storage, array.Address(), MaxInlineArrayElementSize)
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
	array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		v := RandomValue(r)
		storable, err := v.Storable(storage, array.Address(), MaxInlineArrayElementSize)
		require.NoError(b, err)
		totalRawDataSize += storable.ByteSize()
		err = array.Append(v)
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
		ind := r.Intn(int(array.Count()))
		s, err := array.Remove(uint64(ind))
		require.NoError(b, err)
		storable, err := s.Storable(storage, array.Address(), MaxInlineArrayElementSize)
		require.NoError(b, err)
		totalRawDataSize -= storable.ByteSize()
	}
	require.NoError(b, storage.Commit())
	totalRemoveTime = time.Since(start)

	// insert
	storage.DropCache()
	array, err = NewBasicArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	start = time.Now()
	for i := 0; i < numberOfElements; i++ {
		ind := r.Intn(int(array.Count()))
		v := RandomValue(r)
		storable, err := v.Storable(storage, array.Address(), MaxInlineArrayElementSize)
		require.NoError(b, err)
		totalRawDataSize += storable.ByteSize()
		err = array.Insert(uint64(ind), v)
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
		ind := r.Intn(int(array.Count()))
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

	ind := r.Intn(int(array.Count()))
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
