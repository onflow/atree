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

package atree

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

var noop Storable

func BenchmarkArrayGet100x(b *testing.B) {
	benchmarks := []struct {
		name             string
		initialArraySize int
		numberOfOps      int
		long             bool
	}{
		{"10", 10, 100, false},
		{"1000", 1000, 100, false},
		{"10000", 10_000, 100, false},
		{"100000", 100_000, 100, false},
		{"1000000", 1_000_000, 100, false},
		{"10000000", 10_000_000, 100, true},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if bm.long && testing.Short() {
				b.Skipf("Skipping %s in short mode", bm.name)
			}
			benchmarkArrayGet(b, bm.initialArraySize, bm.numberOfOps)
		})
	}
}

func BenchmarkArrayInsert100x(b *testing.B) {
	benchmarks := []struct {
		name             string
		initialArraySize int
		numberOfOps      int
		long             bool
	}{
		{"10", 10, 100, false},
		{"1000", 1000, 100, false},
		{"10000", 10_000, 100, false},
		{"100000", 100_000, 100, false},
		{"1000000", 1_000_000, 100, true},
		{"10000000", 10_000_000, 100, true},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if bm.long && testing.Short() {
				b.Skipf("Skipping %s in short mode", bm.name)
			}
			benchmarkArrayInsert(b, bm.initialArraySize, bm.numberOfOps)
		})
	}
}

func BenchmarkArrayRemove100x(b *testing.B) {
	benchmarks := []struct {
		name             string
		initialArraySize int
		numberOfOps      int
		long             bool
	}{
		{"100", 100, 100, false},
		{"1000", 1000, 100, false},
		{"10000", 10_000, 100, false},
		{"100000", 100_000, 100, false},
		{"1000000", 1_000_000, 100, true},
		{"10000000", 10_000_000, 100, true},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if bm.long && testing.Short() {
				b.Skipf("Skipping %s in short mode", bm.name)
			}
			benchmarkArrayRemove(b, bm.initialArraySize, bm.numberOfOps)
		})
	}
}

// BenchmarkArrayRemoveAll benchmarks removing all elements in a loop.
func BenchmarkArrayRemoveAll(b *testing.B) {
	benchmarks := []struct {
		name             string
		initialArraySize int
		long             bool
	}{
		{"100", 100, false},
		{"1000", 1000, false},
		{"10000", 10_000, false},
		{"100000", 100_000, false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if bm.long && testing.Short() {
				b.Skipf("Skipping %s in short mode", bm.name)
			}
			benchmarkArrayRemoveAll(b, bm.initialArraySize)
		})
	}
}

// BenchmarkArrayPopIterate benchmarks removing all elements using PopIterate.
func BenchmarkArrayPopIterate(b *testing.B) {
	benchmarks := []struct {
		name             string
		initialArraySize int
		long             bool
	}{
		{"100", 100, false},
		{"1000", 1000, false},
		{"10000", 10_000, false},
		{"100000", 100_000, false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if bm.long && testing.Short() {
				b.Skipf("Skipping %s in short mode", bm.name)
			}
			benchmarkArrayPopIterate(b, bm.initialArraySize)
		})
	}
}

func BenchmarkNewArrayFromAppend(b *testing.B) {
	benchmarks := []struct {
		name             string
		initialArraySize int
		long             bool
	}{
		{"100", 100, false},
		{"1000", 1000, false},
		{"10000", 10_000, false},
		{"100000", 100_000, false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if bm.long && testing.Short() {
				b.Skipf("Skipping %s in short mode", bm.name)
			}
			benchmarkNewArrayFromAppend(b, bm.initialArraySize)
		})
	}
}

func BenchmarkNewArrayFromBatchData(b *testing.B) {
	benchmarks := []struct {
		name             string
		initialArraySize int
		long             bool
	}{
		{"100", 100, false},
		{"1000", 1000, false},
		{"10000", 10_000, false},
		{"100000", 100_000, false},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			if bm.long && testing.Short() {
				b.Skipf("Skipping %s in short mode", bm.name)
			}
			benchmarkNewArrayFromBatchData(b, bm.initialArraySize)
		})
	}
}

func setupArray(b *testing.B, r *rand.Rand, storage *PersistentSlabStorage, initialArraySize int) *Array {

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	typeInfo := testTypeInfo{42}

	array, err := NewArray(storage, address, typeInfo)
	require.NoError(b, err)

	for i := 0; i < initialArraySize; i++ {
		v := RandomValue(r)
		err := array.Append(v)
		require.NoError(b, err)
	}

	err = storage.Commit()
	require.NoError(b, err)

	arrayID := array.StorageID()

	storage.DropCache()

	newArray, err := NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	return newArray
}

func benchmarkArrayGet(b *testing.B, initialArraySize, numberOfOps int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	array := setupArray(b, r, storage, initialArraySize)

	var storable Storable

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		for i := 0; i < numberOfOps; i++ {
			index := r.Intn(int(array.Count()))
			storable, _ = array.Get(uint64(index))
		}
	}

	noop = storable
}

func benchmarkArrayInsert(b *testing.B, initialArraySize, numberOfOps int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	for i := 0; i < b.N; i++ {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArraySize)

		b.StartTimer()

		for i := 0; i < numberOfOps; i++ {
			index := r.Intn(int(array.Count()))
			v := RandomValue(r)
			_ = array.Insert(uint64(index), v)
		}
	}
}

func benchmarkArrayRemove(b *testing.B, initialArraySize, numberOfOps int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	for i := 0; i < b.N; i++ {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArraySize)

		b.StartTimer()

		for i := 0; i < numberOfOps; i++ {
			index := r.Intn(int(array.Count()))
			_, _ = array.Remove(uint64(index))
		}
	}
}

func benchmarkArrayRemoveAll(b *testing.B, initialArraySize int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	var storable Storable

	for i := 0; i < b.N; i++ {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArraySize)

		b.StartTimer()

		for i := initialArraySize - 1; i >= 0; i-- {
			storable, _ = array.Remove(uint64(i))
		}
	}

	noop = storable
}

func benchmarkArrayPopIterate(b *testing.B, initialArraySize int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	var storable Storable

	for i := 0; i < b.N; i++ {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArraySize)

		b.StartTimer()

		err := array.PopIterate(func(s Storable) {
			storable = s
		})
		if err != nil {
			b.Errorf(err.Error())
		}
	}

	noop = storable
}

func benchmarkNewArrayFromAppend(b *testing.B, initialArraySize int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	array := setupArray(b, r, storage, initialArraySize)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		copied, _ := NewArray(storage, array.Address(), array.Type())

		_ = array.Iterate(func(value Value) (bool, error) {
			_ = copied.Append(value)
			return true, nil
		})

		if copied.Count() != array.Count() {
			b.Errorf("Copied array has %d elements, want %d", copied.Count(), array.Count())
		}
	}
}

func benchmarkNewArrayFromBatchData(b *testing.B, initialArraySize int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	array := setupArray(b, r, storage, initialArraySize)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		iter, err := array.Iterator()
		require.NoError(b, err)

		copied, _ := NewArrayFromBatchData(storage, array.Address(), array.Type(), func() (Value, error) {
			return iter.Next()
		})

		if copied.Count() != array.Count() {
			b.Errorf("Copied array has %d elements, want %d", copied.Count(), array.Count())
		}
	}
}
