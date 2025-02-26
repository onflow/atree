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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
)

var noopValue atree.Value
var noopStorable atree.Storable

func BenchmarkArrayGet100x(b *testing.B) {
	benchmarks := []struct {
		name              string
		initialArrayCount int
		numberOfOps       int
		long              bool
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
			benchmarkArrayGet(b, bm.initialArrayCount, bm.numberOfOps)
		})
	}
}

func BenchmarkArrayInsert100x(b *testing.B) {
	benchmarks := []struct {
		name              string
		initialArrayCount int
		numberOfOps       int
		long              bool
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
			benchmarkArrayInsert(b, bm.initialArrayCount, bm.numberOfOps)
		})
	}
}

func BenchmarkArrayRemove100x(b *testing.B) {
	benchmarks := []struct {
		name              string
		initialArrayCount int
		numberOfOps       int
		long              bool
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
			benchmarkArrayRemove(b, bm.initialArrayCount, bm.numberOfOps)
		})
	}
}

// BenchmarkArrayRemoveAll benchmarks removing all elements in a loop.
func BenchmarkArrayRemoveAll(b *testing.B) {
	benchmarks := []struct {
		name              string
		initialArrayCount int
		long              bool
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
			benchmarkArrayRemoveAll(b, bm.initialArrayCount)
		})
	}
}

// BenchmarkArrayPopIterate benchmarks removing all elements using PopIterate.
func BenchmarkArrayPopIterate(b *testing.B) {
	benchmarks := []struct {
		name              string
		initialArrayCount int
		long              bool
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
			benchmarkArrayPopIterate(b, bm.initialArrayCount)
		})
	}
}

func BenchmarkNewArrayFromAppend(b *testing.B) {
	benchmarks := []struct {
		name              string
		initialArrayCount int
		long              bool
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
			benchmarkNewArrayFromAppend(b, bm.initialArrayCount)
		})
	}
}

func BenchmarkNewArrayFromBatchData(b *testing.B) {
	benchmarks := []struct {
		name              string
		initialArrayCount int
		long              bool
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
			benchmarkNewArrayFromBatchData(b, bm.initialArrayCount)
		})
	}
}

func setupArray(b *testing.B, r *rand.Rand, storage *atree.PersistentSlabStorage, initialArrayCount int) *atree.Array {

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	typeInfo := test_utils.NewSimpleTypeInfo(42)

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(b, err)

	for range initialArrayCount {
		v := randomValue(r, atree.MaxInlineArrayElementSize())
		err := array.Append(v)
		require.NoError(b, err)
	}

	err = storage.Commit()
	require.NoError(b, err)

	arrayID := array.SlabID()

	storage.DropCache()

	newArray, err := atree.NewArrayWithRootID(storage, arrayID)
	require.NoError(b, err)

	return newArray
}

func benchmarkArrayGet(b *testing.B, initialArrayCount, numberOfOps int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	array := setupArray(b, r, storage, initialArrayCount)

	var value atree.Value

	b.StartTimer()

	for range b.N {
		for range numberOfOps {
			index := getRandomArrayIndex(r, array)
			value, _ = array.Get(index)
		}
	}

	noopValue = value
}

func benchmarkArrayInsert(b *testing.B, initialArrayCount, numberOfOps int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	for range b.N {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArrayCount)

		b.StartTimer()

		for range numberOfOps {
			index := getRandomArrayIndex(r, array)
			v := randomValue(r, atree.MaxInlineArrayElementSize())
			_ = array.Insert(index, v)
		}
	}
}

func benchmarkArrayRemove(b *testing.B, initialArrayCount, numberOfOps int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	for range b.N {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArrayCount)

		b.StartTimer()

		for range numberOfOps {
			index := getRandomArrayIndex(r, array)
			_, _ = array.Remove(index)
		}
	}
}

func benchmarkArrayRemoveAll(b *testing.B, initialArrayCount int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	var storable atree.Storable

	for range b.N {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArrayCount)

		b.StartTimer()

		for i := initialArrayCount - 1; i >= 0; i-- {
			storable, _ = array.Remove(uint64(i))
		}
	}

	noopStorable = storable
}

func benchmarkArrayPopIterate(b *testing.B, initialArrayCount int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	var storable atree.Storable

	for range b.N {

		b.StopTimer()

		array := setupArray(b, r, storage, initialArrayCount)

		b.StartTimer()

		err := array.PopIterate(func(s atree.Storable) {
			storable = s
		})
		if err != nil {
			b.Error(err.Error())
		}
	}

	noopStorable = storable
}

func benchmarkNewArrayFromAppend(b *testing.B, initialArrayCount int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	array := setupArray(b, r, storage, initialArrayCount)

	b.StartTimer()

	for range b.N {
		copied, _ := atree.NewArray(storage, array.Address(), array.Type())

		_ = array.IterateReadOnly(func(value atree.Value) (bool, error) {
			_ = copied.Append(value)
			return true, nil
		})

		if copied.Count() != array.Count() {
			b.Errorf("Copied array has %d elements, want %d", copied.Count(), array.Count())
		}
	}
}

func benchmarkNewArrayFromBatchData(b *testing.B, initialArrayCount int) {

	b.StopTimer()

	r := newRand(b)

	storage := newTestPersistentStorage(b)

	array := setupArray(b, r, storage, initialArrayCount)

	b.StartTimer()

	for range b.N {
		iter, err := array.ReadOnlyIterator()
		require.NoError(b, err)

		copied, _ := atree.NewArrayFromBatchData(storage, array.Address(), array.Type(), func() (atree.Value, error) {
			return iter.Next()
		})

		if copied.Count() != array.Count() {
			b.Errorf("Copied array has %d elements, want %d", copied.Count(), array.Count())
		}
	}
}
