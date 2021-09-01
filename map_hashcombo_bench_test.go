/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dchest/siphash"
	"github.com/fxamacker/cbor/v2"
	orisanoV3 "github.com/orisano/wyhash/v3"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"
)

// xxh128DigesterBuilder creates Digester with XXH128 and BLAKE3.
type xxh128DigesterBuilder struct {
}

var _ DigesterBuilder = &xxh128DigesterBuilder{}

// xxh128DigesterBuilder creates Digester with XXH128 with 8 bytes prefix and BLAKE3.
type xxh128pDigesterBuilder struct {
	k0 uint64
	k1 uint64
}

var _ DigesterBuilder = &xxh128pDigesterBuilder{}

// wyv3DigesterBuilder creates Digester with wyhash v3 and BLAKE3.
type wyv3DigesterBuilder struct {
	k0 uint64
	k1 uint64
}

var _ DigesterBuilder = &wyv3DigesterBuilder{}

// HASH ALGO ALT: Replace SipHash128 with XXH128 for super fast first two levels:
//
//    XXH128 -- first 64 bit of digest (with prefix if prefix isn't empty)
//    XXH128 -- next 64 bit of digest
//    BLAKE3 -- first 64 bit of digest, without key
//    BLAKE3 -- next 64 bit of digest, without key
//    no more hashing, linear search all collisions
type xxh128Digester struct {
	xxh128Hash [2]uint64
	blake3Hash [4]uint64
	prefix     []byte
	msg        []byte
}

// HASH ALGO ALT: Use wyhash v3 before SipHash 128 for super fast first first level:
//
//    wyhash v3 -- first 64 bit of digest
//    SipHash -- first 64 bit of digest
//    SipHash -- next 64 bit of digest
//    BLAKE3 -- first 64 bit of digest, without key
//    BLAKE3 -- next 64 bit of digest, without key
//    no more hashing, linear search all collisions
type wyv3Digester struct {
	k0         uint64
	k1         uint64
	wyv3Hash   uint64
	sipHash    [2]uint64
	blake3Hash [4]uint64
	msg        []byte
}

var (
	emptyXXH128Hash [2]uint64
)

func newXXH128DigesterBuilder() *xxh128DigesterBuilder {
	return &xxh128DigesterBuilder{}
}

func (bdb *xxh128DigesterBuilder) SetSeed(_ uint64, _ uint64) {
	// noop
}

func (bdb *xxh128DigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	return &xxh128Digester{msg: msg}, nil
}

func newXXH128pDigesterBuilder() *xxh128pDigesterBuilder {
	return &xxh128pDigesterBuilder{}
}

func (bdb *xxh128pDigesterBuilder) SetSeed(k0 uint64, k1 uint64) {
	bdb.k0 = k0
	bdb.k1 = k1
}

func (bdb *xxh128pDigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	if bdb.k0 == 0 {
		return nil, NewHashError(errors.New("k0 is uninitialized"))
	}

	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, bdb.k0)

	return &xxh128Digester{prefix: prefix, msg: msg}, nil
}

func (bd *xxh128Digester) DigestPrefix(level int) ([]Digest, error) {
	if level > bd.Levels() {
		return nil, errors.New("digest level out of bounds")
	}
	var prefix []Digest
	for i := 0; i < level; i++ {
		d, err := bd.Digest(i)
		if err != nil {
			return nil, err
		}
		prefix = append(prefix, d)
	}
	return prefix, nil
}

func (bd *xxh128Digester) Digest(level int) (Digest, error) {
	if level >= bd.Levels() {
		return 0, errors.New("digest level out of bounds")
	}

	switch level {
	case 0, 1:
		{
			if bd.xxh128Hash == emptyXXH128Hash {
				var uint128 xxh3.Uint128

				if len(bd.prefix) == 0 {
					uint128 = xxh3.Hash128(bd.msg)
				} else {
					b := make([]byte, len(bd.prefix)+len(bd.msg))
					n := copy(b, bd.prefix)
					copy(b[n:], bd.msg)

					uint128 = xxh3.Hash128(b)
				}

				bd.xxh128Hash[0], bd.xxh128Hash[1] = uint128.Lo, uint128.Hi
			}
			return Digest(bd.xxh128Hash[level]), nil
		}
	case 2, 3:
		{
			if bd.blake3Hash == emptyBlake3Hash {
				sum := blake3.Sum256(bd.msg)
				bd.blake3Hash[0] = binary.BigEndian.Uint64(sum[:])
				bd.blake3Hash[1] = binary.BigEndian.Uint64(sum[8:])
				bd.blake3Hash[2] = binary.BigEndian.Uint64(sum[16:])
				bd.blake3Hash[3] = binary.BigEndian.Uint64(sum[24:])
			}
			return Digest(bd.blake3Hash[level-2]), nil
		}
	default: // list mode
		return 0, nil
	}
}

func (bd *xxh128Digester) Levels() int {
	return 4
}

func newWYV3DigesterBuilder() *wyv3DigesterBuilder {
	return &wyv3DigesterBuilder{}
}

func (bdb *wyv3DigesterBuilder) SetSeed(k0 uint64, k1 uint64) {
	bdb.k0 = k0
	bdb.k1 = k1
}

func (bdb *wyv3DigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	if bdb.k0 == 0 {
		return nil, NewHashError(errors.New("k0 is uninitialized"))
	}

	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	return &wyv3Digester{k0: bdb.k0, k1: bdb.k1, msg: msg}, nil
}

func (bd *wyv3Digester) DigestPrefix(level int) ([]Digest, error) {
	if level > bd.Levels() {
		return nil, errors.New("digest level out of bounds")
	}
	var prefix []Digest
	for i := 0; i < level; i++ {
		d, err := bd.Digest(i)
		if err != nil {
			return nil, err
		}
		prefix = append(prefix, d)
	}
	return prefix, nil
}

func (bd *wyv3Digester) Digest(level int) (Digest, error) {
	if level >= bd.Levels() {
		return 0, errors.New("digest level out of bounds")
	}

	switch level {
	case 0:
		if bd.wyv3Hash == 0 {
			bd.wyv3Hash = orisanoV3.Sum64(bd.k0, bd.msg)
		}
		return Digest(bd.wyv3Hash), nil
	case 1, 2:
		{
			if bd.sipHash == emptySipHash {
				bd.sipHash[0], bd.sipHash[1] = siphash.Hash128(bd.k0, bd.k1, bd.msg)
			}
			return Digest(bd.sipHash[level]), nil
		}
	case 3, 4:
		{
			if bd.blake3Hash == emptyBlake3Hash {
				sum := blake3.Sum256(bd.msg)
				bd.blake3Hash[0] = binary.BigEndian.Uint64(sum[:])
				bd.blake3Hash[1] = binary.BigEndian.Uint64(sum[8:])
				bd.blake3Hash[2] = binary.BigEndian.Uint64(sum[16:])
				bd.blake3Hash[3] = binary.BigEndian.Uint64(sum[24:])
			}
			return Digest(bd.blake3Hash[level-2]), nil
		}
	default: // list mode
		return 0, nil
	}
}

func (bd *wyv3Digester) Levels() int {
	return 5
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

func setupMap(storage *PersistentSlabStorage, digesterBuilder DigesterBuilder, keys []ComparableValue, values []Value) (*OrderedMap, error) {

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	typeInfo := cbor.RawMessage{0x18, 0x2A} // unsigned(42)

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	if err != nil {
		return nil, err
	}

	for i, k := range keys {
		err = m.Set(k, values[i])
		if err != nil {
			return nil, err
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

func BenchmarkMapHashCombo(b *testing.B) {
	benchmarks := []struct {
		name            string
		initialMapSize  int
		digesterBuilder DigesterBuilder
	}{
		// wyhash v3 + SipHash + BLAKE3
		{"wyv3_10elems", 10, newWYV3DigesterBuilder()},
		{"wyv3_1000elems", 1000, newWYV3DigesterBuilder()},
		{"wyv3_10000elems", 10_000, newWYV3DigesterBuilder()},
		{"wyv3_100000elems", 100_000, newWYV3DigesterBuilder()},

		// XXH128 + BLAKE3
		{"XXH128_10elems", 10, newXXH128DigesterBuilder()},
		{"XXH128_1000elems", 1000, newXXH128DigesterBuilder()},
		{"XXH128_10000elems", 10_000, newXXH128DigesterBuilder()},
		{"XXH128_100000elems", 100_000, newXXH128DigesterBuilder()},

		// SipHash + BLAKE3
		{"Sip128_10elems", 10, newBasicDigesterBuilder()},
		{"Sip128_1000elems", 1000, newBasicDigesterBuilder()},
		{"Sip128_10000elems", 10_000, newBasicDigesterBuilder()},
		{"Sip128_100000elems", 100_000, newBasicDigesterBuilder()},

		// XXH128 with prefix + BLAKE3
		{"XXH128p_10elems", 10, newXXH128pDigesterBuilder()},
		{"XXH128p_1000elems", 1000, newXXH128pDigesterBuilder()},
		{"XXH128p_10000elems", 10_000, newXXH128pDigesterBuilder()},
		{"XXH128p_100000elems", 100_000, newXXH128pDigesterBuilder()},
	}

	// Keys and values can be reused.
	keys, values := uniqueKeyValues(100_000)

	for _, bm := range benchmarks {

		b.Run(bm.name, func(b *testing.B) {
			b.StopTimer()

			storage := newTestPersistentStorage(b)

			m, err := setupMap(storage, bm.digesterBuilder, keys[:bm.initialMapSize], values[:bm.initialMapSize])
			require.NoError(b, err)

			count := m.Count()

			var storable Storable

			b.StartTimer()

			index := 0
			for i := 0; i < b.N; i++ {
				if index >= int(count) {
					index = 0
				}

				v, _ := m.Get(keys[index])

				storable, _ = v.Storable(storage, m.Address(), maxInlineMapElementSize)

				index++
			}

			noop = storable
		})
	}
}
