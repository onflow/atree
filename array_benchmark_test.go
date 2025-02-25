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

package atree_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
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

// BenchmarkArray benchmarks the performance of the atree array
func benchmarkArray(b *testing.B, initialArrayCount, numberOfElements int) {

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	typeInfo := test_utils.NewSimpleTypeInfo(42)

	array, err := atree.NewArray(storage, address, typeInfo)

	require.NoError(b, err)

	var start time.Time
	var totalRawDataSize uint32
	var totalAppendTime time.Duration
	var totalRemoveTime time.Duration
	var totalInsertTime time.Duration
	var totalLookupTime time.Duration

	// setup
	for range initialArrayCount {
		v := randomValue(r, int(atree.MaxInlineArrayElementSize()))
		storable, err := v.Storable(storage, array.Address(), atree.MaxInlineArrayElementSize())
		require.NoError(b, err)
		totalRawDataSize += storable.ByteSize()
		err = array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	b.ResetTimer()

	arrayID := array.SlabID()

	// append
	storage.DropCache()
	start = time.Now()
	array, err = atree.NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)
	for range numberOfElements {
		v := randomValue(r, int(atree.MaxInlineArrayElementSize()))

		storable, err := v.Storable(storage, array.Address(), atree.MaxInlineArrayElementSize())
		require.NoError(b, err)

		totalRawDataSize += storable.ByteSize()

		err = array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalAppendTime = time.Since(start)

	// remove
	storage.DropCache()
	start = time.Now()
	array, err = atree.NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	for range numberOfElements {
		ind := r.Intn(int(array.Count()))
		storable, err := array.Remove(uint64(ind))
		require.NoError(b, err)
		totalRawDataSize -= storable.ByteSize()
	}
	require.NoError(b, storage.Commit())
	totalRemoveTime = time.Since(start)

	// insert
	storage.DropCache()
	start = time.Now()
	array, err = atree.NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	for range numberOfElements {
		ind := r.Intn(int(array.Count()))
		v := randomValue(r, int(atree.MaxInlineArrayElementSize()))

		storable, err := v.Storable(storage, array.Address(), atree.MaxInlineArrayElementSize())
		require.NoError(b, err)

		totalRawDataSize += storable.ByteSize()

		err = array.Insert(uint64(ind), v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalInsertTime = time.Since(start)

	// lookup
	storage.DropCache()
	start = time.Now()
	array, err = atree.NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	for range numberOfElements {
		ind := r.Intn(int(array.Count()))
		_, err := array.Get(uint64(ind))
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	totalLookupTime = time.Since(start)

	baseStorage := atree.GetBaseStorage(storage)

	// random lookup
	baseStorage.ResetReporter()
	storage.DropCache()
	array, err = atree.NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	ind := r.Intn(int(array.Count()))
	_, err = array.Get(uint64(ind))
	require.NoError(b, err)

	storageOverheadRatio := float64(baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
	b.ReportMetric(float64(baseStorage.BytesRetrieved()), "storage_bytes_loaded_for_lookup")
	// b.ReportMetric(float64(array.Count()), "number_of_elements")
	b.ReportMetric(float64(int(totalAppendTime)), "append_100_time_(ns)")
	b.ReportMetric(float64(int(totalRemoveTime)), "remove_100_time_(ns)")
	b.ReportMetric(float64(int(totalInsertTime)), "insert_100_time_(ns)")
	b.ReportMetric(float64(int(totalLookupTime)), "lookup_100_time_(ns)")
}

func BenchmarkLArrayMemoryImpact(b *testing.B) { benchmarkLongTermImpactOnMemory(b, 10_000, 1000_000) }

// BenchmarkArray benchmarks the performance of the atree array
func benchmarkLongTermImpactOnMemory(b *testing.B, initialArrayCount, numberOfOps int) {

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	typeInfo := test_utils.NewSimpleTypeInfo(42)

	array, err := atree.NewArray(storage, address, typeInfo)

	require.NoError(b, err)

	var totalRawDataSize uint32

	// setup
	for range initialArrayCount {
		v := randomValue(r, int(atree.MaxInlineArrayElementSize()))

		storable, err := v.Storable(storage, array.Address(), atree.MaxInlineArrayElementSize())
		require.NoError(b, err)

		totalRawDataSize += storable.ByteSize()

		err = array.Append(v)
		require.NoError(b, err)
	}
	require.NoError(b, storage.Commit())
	b.ResetTimer()

	for range numberOfOps {
		ind := r.Intn(int(array.Count()))
		// select opt
		switch r.Intn(2) {
		case 0: // remove
			storable, err := array.Remove(uint64(ind))
			require.NoError(b, err)
			totalRawDataSize -= storable.ByteSize()
		case 1: // insert
			v := randomValue(r, int(atree.MaxInlineArrayElementSize()))

			storable, err := v.Storable(storage, array.Address(), atree.MaxInlineArrayElementSize())
			require.NoError(b, err)

			totalRawDataSize += storable.ByteSize()

			err = array.Insert(uint64(ind), v)
			require.NoError(b, err)
		}
	}
	require.NoError(b, storage.Commit())

	baseStorage := atree.GetBaseStorage(storage)

	storageOverheadRatio := float64(baseStorage.Size()) / float64(totalRawDataSize)
	b.ReportMetric(float64(baseStorage.SegmentsTouched()), "segments_touched")
	b.ReportMetric(float64(baseStorage.SegmentCounts()), "segments_total")
	b.ReportMetric(float64(totalRawDataSize), "storage_raw_data_size")
	b.ReportMetric(float64(baseStorage.Size()), "storage_stored_data_size")
	b.ReportMetric(storageOverheadRatio, "storage_overhead_ratio")
}
