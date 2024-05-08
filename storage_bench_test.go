/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2024 Dapper Labs, Inc.
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
	"encoding/binary"
	"math/rand"
	"runtime"
	"strconv"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

func benchmarkFastCommit(b *testing.B, seed int64, numberOfSlabs int) {
	r := rand.New(rand.NewSource(seed))

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(b, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(b, err)

	slabs := make([]Slab, numberOfSlabs)
	for i := 0; i < numberOfSlabs; i++ {
		addr := generateRandomAddress(r)

		var index SlabIndex
		binary.BigEndian.PutUint64(index[:], uint64(i))

		id := SlabID{addr, index}

		slabs[i] = generateLargeSlab(id)
	}

	b.Run(strconv.Itoa(numberOfSlabs), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()

			baseStorage := NewInMemBaseStorage()
			storage := NewPersistentSlabStorage(baseStorage, encMode, decMode, nil, nil)

			for _, slab := range slabs {
				err = storage.Store(slab.SlabID(), slab)
				require.NoError(b, err)
			}

			b.StartTimer()

			err := storage.FastCommit(runtime.NumCPU())
			require.NoError(b, err)
		}
	})
}

func benchmarkNondeterministicFastCommit(b *testing.B, seed int64, numberOfSlabs int) {
	r := rand.New(rand.NewSource(seed))

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(b, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(b, err)

	slabs := make([]Slab, numberOfSlabs)
	for i := 0; i < numberOfSlabs; i++ {
		addr := generateRandomAddress(r)

		var index SlabIndex
		binary.BigEndian.PutUint64(index[:], uint64(i))

		id := SlabID{addr, index}

		slabs[i] = generateLargeSlab(id)
	}

	b.Run(strconv.Itoa(numberOfSlabs), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()

			baseStorage := NewInMemBaseStorage()
			storage := NewPersistentSlabStorage(baseStorage, encMode, decMode, nil, nil)

			for _, slab := range slabs {
				err = storage.Store(slab.SlabID(), slab)
				require.NoError(b, err)
			}

			b.StartTimer()

			err := storage.NonderterministicFastCommit(runtime.NumCPU())
			require.NoError(b, err)
		}
	})
}

func BenchmarkStorageFastCommit(b *testing.B) {
	fixedSeed := int64(1234567) // intentionally use fixed constant rather than time, etc.

	benchmarkFastCommit(b, fixedSeed, 10)
	benchmarkFastCommit(b, fixedSeed, 100)
	benchmarkFastCommit(b, fixedSeed, 1_000)
	benchmarkFastCommit(b, fixedSeed, 10_000)
	benchmarkFastCommit(b, fixedSeed, 100_000)
	benchmarkFastCommit(b, fixedSeed, 1_000_000)
}

func BenchmarkStorageNondeterministicFastCommit(b *testing.B) {
	fixedSeed := int64(1234567) // intentionally use fixed constant rather than time, etc.

	benchmarkNondeterministicFastCommit(b, fixedSeed, 10)
	benchmarkNondeterministicFastCommit(b, fixedSeed, 100)
	benchmarkNondeterministicFastCommit(b, fixedSeed, 1_000)
	benchmarkNondeterministicFastCommit(b, fixedSeed, 10_000)
	benchmarkNondeterministicFastCommit(b, fixedSeed, 100_000)
	benchmarkNondeterministicFastCommit(b, fixedSeed, 1_000_000)
}
