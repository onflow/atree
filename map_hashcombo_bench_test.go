/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"errors"
	"fmt"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockDigesterBuilder struct {
	mock.Mock

	digesterBuilder DigesterBuilder
}

var _ DigesterBuilder = &mockDigesterBuilder{}

type mockDigester struct {
	digester Digester

	// digests is used for mocking hash collision
	digests []Digest
}

var _ Digester = &mockDigester{}

func (b *mockDigesterBuilder) SetSeed(k0 uint64, k1 uint64) {
	b.digesterBuilder.SetSeed(k0, k1)
}

func (b *mockDigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	digester, err := b.digesterBuilder.Digest(hashable)
	if err != nil {
		return nil, err
	}

	args := b.Called(hashable)

	digests := args.Get(0).([]Digest)

	return &mockDigester{digester: digester, digests: digests}, nil
}

func (g *mockDigester) DigestPrefix(level int) ([]Digest, error) {
	// Call wrapped digester
	_, err := g.digester.DigestPrefix(level)
	if err != nil {
		return nil, err
	}

	if level > len(g.digests) {
		return nil, errors.New("digest level out of bounds")
	}
	return g.digests[:level], nil
}

func (g *mockDigester) Digest(level int) (Digest, error) {
	// Call wrapped digester
	_, err := g.digester.Digest(level)
	if err != nil {
		return Digest(0), err
	}

	if level >= len(g.digests) {
		return Digest(0), errors.New("digest level out of bounds")
	}
	return g.digests[level], nil
}

func (g *mockDigester) Levels() int {
	return g.digester.Levels()
}

func uniqueKeyValues(size int) ([]ComparableValue, []Value) {

	keys := make([]ComparableValue, 0, size)
	values := make([]Value, 0, size)

	uniqueKeys := make(map[ComparableValue]bool)
	for len(uniqueKeys) < size {
		//k := RandomMapKey()
		k := RandomUint64MapKey()
		if !uniqueKeys[k] {
			uniqueKeys[k] = true

			keys = append(keys, k)
			values = append(values, RandomMapValue())
		}
	}

	return keys, values
}

func setupMapWithCollision(
	storage *PersistentSlabStorage,
	digesterBuilder *mockDigesterBuilder,
	keys []ComparableValue,
	values []Value,
	collisionLevel int,
	totalLevel int,
	numOfCollisionGroups int,
) (*OrderedMap, error) {

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	if err != nil {
		return nil, err
	}

	for i, k := range keys {
		// Set up mock digests
		var digests []Digest

		// Append colliding digests
		for j := 0; j < collisionLevel; j++ {
			collisionDigest := Digest(i % numOfCollisionGroups)
			digests = append(digests, collisionDigest)
		}

		// Append not colliding digests
		for j := collisionLevel; j < totalLevel; j++ {
			digests = append(digests, Digest(i))
		}

		digesterBuilder.On("Digest", k).Return(digests)

		existingStorable, err := m.Set(k, values[i])
		if err != nil {
			return nil, err
		}
		if existingStorable != nil {
			return nil, fmt.Errorf("found duplicate keys %s", k)
		}
	}

	mapID := m.StorageID()

	err = storage.Commit()
	if err != nil {
		return nil, err
	}

	storage.DropCache()

	return NewMapWithRootID(storage, mapID, digesterBuilder)
}

func BenchmarkMapHashCollisionGet(b *testing.B) {

	type collisionSetting struct {
		collisionGroup int
		collisionLevel int
	}

	benchmarks := []struct {
		name            string
		initialMapSize  int
		digesterBuilder DigesterBuilder
		maxDigestLevel  int
	}{
		// wyhash v3 + SipHash + BLAKE3
		{"wyv3_10elems", 10, newWYV3DigesterBuilder(), 5},
		{"wyv3_100elems", 100, newWYV3DigesterBuilder(), 5},
		{"wyv3_1000elems", 1000, newWYV3DigesterBuilder(), 5},
		//{"wyv3_10000elems", 10_000, newWYV3DigesterBuilder(), 5},
		//{"wyv3_100000elems", 100_000, newWYV3DigesterBuilder(), 5},

		// XXH128 + BLAKE3
		{"XXH128_10elems", 10, newXXH128DigesterBuilder(), 4},
		{"XXH128_100elems", 100, newXXH128DigesterBuilder(), 4},
		{"XXH128_1000elems", 1000, newXXH128DigesterBuilder(), 4},
		//{"XXH128_10000elems", 10_000, newXXH128DigesterBuilder(), 4},
		//{"XXH128_100000elems", 100_000, newXXH128DigesterBuilder(), 4},

		// SipHash + BLAKE3
		{"Sip128_10elems", 10, newBasicDigesterBuilder(), 4},
		{"Sip128_100elems", 100, newBasicDigesterBuilder(), 4},
		{"Sip128_1000elems", 1000, newBasicDigesterBuilder(), 4},
		//{"Sip128_10000elems", 10_000, newBasicDigesterBuilder(), 4},
		//{"Sip128_100000elems", 100_000, newBasicDigesterBuilder(), 4},

		// XXH128 with prefix + BLAKE3
		{"XXH128p_10elems", 10, newXXH128pDigesterBuilder(), 4},
		{"XXH128p_100elems", 100, newXXH128pDigesterBuilder(), 4},
		{"XXH128p_1000elems", 1000, newXXH128pDigesterBuilder(), 4},
		//{"XXH128p_10000elems", 10_000, newXXH128pDigesterBuilder(), 4},
		//{"XXH128p_100000elems", 100_000, newXXH128pDigesterBuilder(), 4},

		// XXH128 hasher with prefix + BLAKE3
		{"XXH128p2_10elems", 10, newXXH128pDigesterBuilder2(), 4},
		{"XXH128p2_100elems", 100, newXXH128pDigesterBuilder2(), 4},
		{"XXH128p2_1000elems", 1000, newXXH128pDigesterBuilder2(), 4},
		//{"XXH128p2_10000elems", 10_000, newXXH128pDigesterBuilder2(), 4},
		//{"XXH128p2_100000elems", 100_000, newXXH128pDigesterBuilder2(), 4},
	}

	// Keys and values can be reused.
	keys, values := uniqueKeyValues(100_000)

	// Benchmark different hash combos with:
	// - no collision to max collision level
	// - 1 to len(keys) / 2 collision groups
	for _, bm := range benchmarks {

		collisionSettings := []collisionSetting{{0, 0}}

		collisionGroups := []int{1, bm.initialMapSize / 2}

		for collisionLevel := 1; collisionLevel <= bm.maxDigestLevel; collisionLevel++ {
			for _, collisionGroup := range collisionGroups {
				collisionSettings = append(collisionSettings,
					collisionSetting{collisionLevel: collisionLevel, collisionGroup: collisionGroup})
			}
		}

		for _, setting := range collisionSettings {

			var name string
			if setting.collisionLevel == 0 {
				name = fmt.Sprintf("%s_no_collision", bm.name)
			} else {
				name = fmt.Sprintf("%s_collision_level_%d_collision_group_%d", bm.name, setting.collisionLevel, setting.collisionGroup)
			}

			b.Run(name, func(b *testing.B) {
				b.StopTimer()

				digesterBuilder := &mockDigesterBuilder{digesterBuilder: bm.digesterBuilder}

				storage := newTestPersistentStorage(b)

				m, err := setupMapWithCollision(
					storage,
					digesterBuilder,
					keys[:bm.initialMapSize],
					values[:bm.initialMapSize],
					setting.collisionLevel,
					bm.maxDigestLevel,
					setting.collisionGroup,
				)
				require.NoError(b, err)

				count := m.Count()

				var storable Storable

				b.StartTimer()

				index := 0
				for i := 0; i < b.N; i++ {
					if index >= int(count) {
						index = 0
					}

					storable, _ = m.Get(keys[index])

					index++
				}

				noop = storable
			})
		}
	}
}

func BenchmarkMapHashCollisionSet100NewElem(b *testing.B) {

	type collisionSetting struct {
		collisionGroup int
		collisionLevel int
	}

	benchmarks := []struct {
		name            string
		initialMapSize  int
		digesterBuilder DigesterBuilder
		maxDigestLevel  int
	}{
		// wyhash v3 + SipHash + BLAKE3
		{"wyv3_10elems", 10, newWYV3DigesterBuilder(), 5},
		{"wyv3_100elems", 100, newWYV3DigesterBuilder(), 5},
		{"wyv3_1000elems", 1000, newWYV3DigesterBuilder(), 5},
		//{"wyv3_10000elems", 10_000, newWYV3DigesterBuilder(), 5},
		//{"wyv3_100000elems", 100_000, newWYV3DigesterBuilder(), 5},

		// XXH128 + BLAKE3
		{"XXH128_10elems", 10, newXXH128DigesterBuilder(), 4},
		{"XXH128_100elems", 100, newXXH128DigesterBuilder(), 4},
		{"XXH128_1000elems", 1000, newXXH128DigesterBuilder(), 4},
		//{"XXH128_10000elems", 10_000, newXXH128DigesterBuilder(), 4},
		//{"XXH128_100000elems", 100_000, newXXH128DigesterBuilder(), 4},

		// SipHash + BLAKE3
		{"Sip128_10elems", 10, newBasicDigesterBuilder(), 4},
		{"Sip128_100elems", 100, newBasicDigesterBuilder(), 4},
		{"Sip128_1000elems", 1000, newBasicDigesterBuilder(), 4},
		//{"Sip128_10000elems", 10_000, newBasicDigesterBuilder(), 4},
		//{"Sip128_100000elems", 100_000, newBasicDigesterBuilder(), 4},

		// XXH128 with prefix + BLAKE3
		{"XXH128p_10elems", 10, newXXH128pDigesterBuilder(), 4},
		{"XXH128p_100elems", 100, newXXH128pDigesterBuilder(), 4},
		{"XXH128p_1000elems", 1000, newXXH128pDigesterBuilder(), 4},
		//{"XXH128p_10000elems", 10_000, newXXH128pDigesterBuilder(), 4},
		//{"XXH128p_100000elems", 100_000, newXXH128pDigesterBuilder(), 4},

		// XXH128 hasher with prefix + BLAKE3
		{"XXH128p2_10elems", 10, newXXH128pDigesterBuilder2(), 4},
		{"XXH128p2_100elems", 100, newXXH128pDigesterBuilder2(), 4},
		{"XXH128p2_1000elems", 1000, newXXH128pDigesterBuilder2(), 4},
		//{"XXH128p2_10000elems", 10_000, newXXH128pDigesterBuilder2(), 4},
		//{"XXH128p2_100000elems", 100_000, newXXH128pDigesterBuilder2(), 4},
	}

	const numberOfInsertion = 100

	// Keys and values can be reused.
	keys, values := uniqueKeyValues(100_000)

	// Benchmark different hash combos with:
	// - no collision to max collision level
	// - 1 to len(keys) / 2 collision groups
	for _, bm := range benchmarks {

		if bm.initialMapSize+numberOfInsertion >= len(keys) {
			b.Fatalf("not enough unique keys: initial map size %d, number of insertion %d", bm.initialMapSize, numberOfInsertion)
		}

		collisionSettings := []collisionSetting{{0, 0}}

		collisionGroups := []int{1, numberOfInsertion / 2}

		for collisionLevel := 1; collisionLevel <= bm.maxDigestLevel; collisionLevel++ {
			for _, collisionGroup := range collisionGroups {
				collisionSettings = append(collisionSettings,
					collisionSetting{collisionLevel: collisionLevel, collisionGroup: collisionGroup})
			}
		}

		for _, setting := range collisionSettings {

			var name string
			if setting.collisionLevel == 0 {
				name = fmt.Sprintf("%s_no_collision", bm.name)
			} else {
				name = fmt.Sprintf("%s_collision_level_%d_collision_group_%d", bm.name, setting.collisionLevel, setting.collisionGroup)
			}

			b.Run(name, func(b *testing.B) {

				for i := 0; i < b.N; i++ {

					b.StopTimer()

					digesterBuilder := &mockDigesterBuilder{digesterBuilder: bm.digesterBuilder}

					storage := newTestPersistentStorage(b)

					// new map without collision
					m, err := setupMapWithCollision(
						storage,
						digesterBuilder,
						keys[:bm.initialMapSize],
						values[:bm.initialMapSize],
						0,
						bm.maxDigestLevel,
						0,
					)
					require.NoError(b, err)

					count := m.Count()

					// create and register mock digests for new elements
					for i := 0; i < numberOfInsertion; i++ {
						// Set up mock digests
						var digests []Digest

						// Append colliding digests
						for j := 0; j < setting.collisionLevel; j++ {
							collisionDigest := Digest(i % setting.collisionGroup)
							digests = append(digests, collisionDigest)
						}

						// Append not colliding digests
						for j := setting.collisionLevel; j < bm.maxDigestLevel; j++ {
							digests = append(digests, Digest(int(count)+i))
						}

						digesterBuilder.On("Digest", keys[int(count)+i]).Return(digests)
					}

					var storable Storable

					b.StartTimer()

					for j := 0; j < numberOfInsertion; j++ {
						storable, _ = m.Set(keys[int(count)+j], values[int(count)+j])
					}

					noop = storable

					require.Equal(b, uint64(bm.initialMapSize+numberOfInsertion), m.Count())
				}
			})
		}
	}
}

func BenchmarkMapHashCollisionRemove100Elem(b *testing.B) {

	type collisionSetting struct {
		collisionGroup int
		collisionLevel int
	}

	benchmarks := []struct {
		name            string
		initialMapSize  int
		digesterBuilder DigesterBuilder
		maxDigestLevel  int
	}{
		// wyhash v3 + SipHash + BLAKE3
		//{"wyv3_10elems", 10, newWYV3DigesterBuilder(), 5},
		{"wyv3_100elems", 100, newWYV3DigesterBuilder(), 5},
		{"wyv3_1000elems", 1000, newWYV3DigesterBuilder(), 5},
		//{"wyv3_10000elems", 10_000, newWYV3DigesterBuilder(), 5},
		//{"wyv3_100000elems", 100_000, newWYV3DigesterBuilder(), 5},

		// XXH128 + BLAKE3
		//{"XXH128_10elems", 10, newXXH128DigesterBuilder(), 4},
		{"XXH128_100elems", 100, newXXH128DigesterBuilder(), 4},
		{"XXH128_1000elems", 1000, newXXH128DigesterBuilder(), 4},
		//{"XXH128_10000elems", 10_000, newXXH128DigesterBuilder(), 4},
		//{"XXH128_100000elems", 100_000, newXXH128DigesterBuilder(), 4},

		// SipHash + BLAKE3
		//{"Sip128_10elems", 10, newBasicDigesterBuilder(), 4},
		{"Sip128_100elems", 100, newBasicDigesterBuilder(), 4},
		{"Sip128_1000elems", 1000, newBasicDigesterBuilder(), 4},
		//{"Sip128_10000elems", 10_000, newBasicDigesterBuilder(), 4},
		//{"Sip128_100000elems", 100_000, newBasicDigesterBuilder(), 4},

		// XXH128 with prefix + BLAKE3
		//{"XXH128p_10elems", 10, newXXH128pDigesterBuilder(), 4},
		{"XXH128p_100elems", 100, newXXH128pDigesterBuilder(), 4},
		{"XXH128p_1000elems", 1000, newXXH128pDigesterBuilder(), 4},
		//{"XXH128p_10000elems", 10_000, newXXH128pDigesterBuilder(), 4},
		//{"XXH128p_100000elems", 100_000, newXXH128pDigesterBuilder(), 4},

		// XXH128 hasher with prefix + BLAKE3
		//{"XXH128p2_10elems", 10, newXXH128pDigesterBuilder2(), 4},
		{"XXH128p2_100elems", 100, newXXH128pDigesterBuilder2(), 4},
		{"XXH128p2_1000elems", 1000, newXXH128pDigesterBuilder2(), 4},
		//{"XXH128p2_10000elems", 10_000, newXXH128pDigesterBuilder2(), 4},
		//{"XXH128p2_100000elems", 100_000, newXXH128pDigesterBuilder2(), 4},
	}

	const numberOfRemoval = 100

	// Keys and values can be reused.
	keys, values := uniqueKeyValues(100_000)

	// Benchmark different hash combos with:
	// - no collision to max collision level
	// - 1 to len(keys) / 2 collision groups
	for _, bm := range benchmarks {

		collisionSettings := []collisionSetting{{0, 0}}

		collisionGroups := []int{1, bm.initialMapSize / 2}

		for collisionLevel := 1; collisionLevel <= bm.maxDigestLevel; collisionLevel++ {
			for _, collisionGroup := range collisionGroups {
				collisionSettings = append(collisionSettings,
					collisionSetting{collisionLevel: collisionLevel, collisionGroup: collisionGroup})
			}
		}

		for _, setting := range collisionSettings {

			var name string
			if setting.collisionLevel == 0 {
				name = fmt.Sprintf("%s_no_collision", bm.name)
			} else {
				name = fmt.Sprintf("%s_collision_level_%d_collision_group_%d", bm.name, setting.collisionLevel, setting.collisionGroup)
			}

			b.Run(name, func(b *testing.B) {

				for i := 0; i < b.N; i++ {

					b.StopTimer()

					digesterBuilder := &mockDigesterBuilder{digesterBuilder: bm.digesterBuilder}

					storage := newTestPersistentStorage(b)

					m, err := setupMapWithCollision(
						storage,
						digesterBuilder,
						keys[:bm.initialMapSize],
						values[:bm.initialMapSize],
						setting.collisionLevel,
						bm.maxDigestLevel,
						setting.collisionGroup,
					)
					require.NoError(b, err)

					count := m.Count()

					var storable Storable

					b.StartTimer()

					for j := 0; j < numberOfRemoval && j < int(count); j++ {
						storable, _, _ = m.Remove(keys[j])
					}

					noop = storable
				}
			})
		}
	}
}
