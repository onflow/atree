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
	collisionLevel int,
	totalLevel int,
	keys []ComparableValue,
	values []Value) (*OrderedMap, error) {

	const numOfElemsInCollisionGroup = 2

	numOfCollisionGroups := len(keys) / numOfElemsInCollisionGroup

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
		collisionDigest := Digest(i % numOfCollisionGroups)
		for j := 0; j < collisionLevel; j++ {
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

func BenchmarkMapHashComboCollision(b *testing.B) {
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

	for _, bm := range benchmarks {

		for collisionLevel := 0; collisionLevel <= bm.maxDigestLevel; collisionLevel++ {

			var name string
			if collisionLevel == 0 {
				name = fmt.Sprintf("%s_no_collision", bm.name)
			} else {
				name = fmt.Sprintf("%s_collision_level_%d", bm.name, collisionLevel)
			}

			b.Run(name, func(b *testing.B) {
				b.StopTimer()

				digesterBuilder := &mockDigesterBuilder{digesterBuilder: bm.digesterBuilder}

				storage := newTestPersistentStorage(b)

				m, err := setupMapWithCollision(
					storage,
					digesterBuilder,
					collisionLevel,
					bm.maxDigestLevel,
					keys[:bm.initialMapSize],
					values[:bm.initialMapSize])
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
