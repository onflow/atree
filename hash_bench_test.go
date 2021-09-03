/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dchest/siphash"
	orisanoV3 "github.com/orisano/wyhash/v3"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"
	"golang.org/x/crypto/sha3"
)

type hashFunc func([]byte) (uint64, uint64)

func xxh128(msg []byte) (uint64, uint64) {
	uint128 := xxh3.Hash128(msg)
	return uint128.Lo, uint128.Hi
}

func xxh128WithPrefix(msg []byte) (uint64, uint64) {
	const k0 = uint64(0x9E3779B97F4A7C15)

	b := make([]byte, 8+len(msg))
	binary.BigEndian.PutUint64(b, k0)
	copy(b[8:], msg)

	uint128 := xxh3.Hash128(msg)
	return uint128.Lo, uint128.Hi
}

func xxh128Hasher(msg []byte) (uint64, uint64) {
	hasher := xxh3.New()

	_, err := hasher.Write(msg)
	if err != nil {
		panic(fmt.Sprintf("hasher.Write failed: %s", err))
	}

	uint128 := hasher.Sum128()
	return uint128.Lo, uint128.Hi
}

func xxh128HasherWithPrefix(msg []byte) (uint64, uint64) {
	const k0 = uint64(0x9E3779B97F4A7C15)

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], k0)

	hasher := xxh3.New()

	_, err := hasher.Write(b[:])
	if err != nil {
		panic(fmt.Sprintf("hasher.Write failed: %s", err))
	}

	_, err = hasher.Write(msg)
	if err != nil {
		panic(fmt.Sprintf("hasher.Write failed: %s", err))
	}

	uint128 := hasher.Sum128()
	return uint128.Lo, uint128.Hi
}

func siphash64(msg []byte) (uint64, uint64) {
	const k0 = uint64(0x9E3779B97F4A7C15)
	const k1 = uint64(0x1BD11BDAA9FC1A22)
	return siphash.Hash(k0, k1, msg), 0
}

func siphash128(msg []byte) (uint64, uint64) {
	const k0 = uint64(0x9E3779B97F4A7C15)
	const k1 = uint64(0x1BD11BDAA9FC1A22)
	return siphash.Hash128(k0, k1, msg)
}

func wyhashv3(msg []byte) (uint64, uint64) {
	const k0 = uint64(0x9E3779B97F4A7C15)
	return orisanoV3.Sum64(k0, msg), 0
}

func blake3_128(msg []byte) (uint64, uint64) {
	sum := blake3.Sum256(msg)
	return binary.BigEndian.Uint64(sum[:]), binary.BigEndian.Uint64(sum[8:])
}

func sha3_128(msg []byte) (uint64, uint64) {
	sum := sha3.Sum256(msg)
	return binary.BigEndian.Uint64(sum[:]), binary.BigEndian.Uint64(sum[8:])
}

var lo uint64
var hi uint64

func BenchmarkHash(b *testing.B) {
	benchmarks := []struct {
		name     string
		v        Uint64Value
		hashFunc hashFunc
	}{
		{"WYv3_3bytes", Uint64Value(0), wyhashv3},
		{"WYv3_4bytes", Uint64Value(24), wyhashv3},
		{"WYv3_5bytes", Uint64Value(1000), wyhashv3},
		{"WYv3_7bytes", Uint64Value(1000000), wyhashv3},
		{"WYv3_11bytes", Uint64Value(1000000000000), wyhashv3},

		{"XXH128_3bytes", Uint64Value(0), xxh128},
		{"XXH128_4bytes", Uint64Value(24), xxh128},
		{"XXH128_5bytes", Uint64Value(1000), xxh128},
		{"XXH128_7bytes", Uint64Value(1000000), xxh128},
		{"XXH128_11bytes", Uint64Value(1000000000000), xxh128},

		{"XXH128P_3bytes", Uint64Value(0), xxh128WithPrefix},
		{"XXH128P_4bytes", Uint64Value(24), xxh128WithPrefix},
		{"XXH128P_5bytes", Uint64Value(1000), xxh128WithPrefix},
		{"XXH128P_7bytes", Uint64Value(1000000), xxh128WithPrefix},
		{"XXH128P_11bytes", Uint64Value(1000000000000), xxh128WithPrefix},

		{"XXH128Hasher_3bytes", Uint64Value(0), xxh128Hasher},
		{"XXH128Hasher_4bytes", Uint64Value(24), xxh128Hasher},
		{"XXH128Hasher_5bytes", Uint64Value(1000), xxh128Hasher},
		{"XXH128Hasher_7bytes", Uint64Value(1000000), xxh128Hasher},
		{"XXH128Hasher_11bytes", Uint64Value(1000000000000), xxh128Hasher},

		{"XXH128PHasher_3bytes", Uint64Value(0), xxh128HasherWithPrefix},
		{"XXH128PHasher_4bytes", Uint64Value(24), xxh128HasherWithPrefix},
		{"XXH128PHasher_5bytes", Uint64Value(1000), xxh128HasherWithPrefix},
		{"XXH128PHasher_7bytes", Uint64Value(1000000), xxh128HasherWithPrefix},
		{"XXH128PHasher_11bytes", Uint64Value(1000000000000), xxh128HasherWithPrefix},

		{"Sip64_3bytes", Uint64Value(0), siphash64},
		{"Sip64_4bytes", Uint64Value(24), siphash64},
		{"Sip64_5bytes", Uint64Value(1000), siphash64},
		{"Sip64_7bytes", Uint64Value(1000000), siphash64},
		{"Sip64_11bytes", Uint64Value(1000000000000), siphash64},

		{"Sip128_3bytes", Uint64Value(0), siphash128},
		{"Sip128_4bytes", Uint64Value(24), siphash128},
		{"Sip128_5bytes", Uint64Value(1000), siphash128},
		{"Sip128_7bytes", Uint64Value(1000000), siphash128},
		{"Sip128_11bytes", Uint64Value(1000000000000), siphash128},

		{"BLAKE3_3bytes", Uint64Value(0), blake3_128},
		{"BLAKE3_4bytes", Uint64Value(24), blake3_128},
		{"BLAKE3_5bytes", Uint64Value(1000), blake3_128},
		{"BLAKE3_7bytes", Uint64Value(1000000), blake3_128},
		{"BLAKE3_11bytes", Uint64Value(1000000000000), blake3_128},

		{"SHA3_3bytes", Uint64Value(0), sha3_128},
		{"SHA3_4bytes", Uint64Value(24), sha3_128},
		{"SHA3_5bytes", Uint64Value(1000), sha3_128},
		{"SHA3_7bytes", Uint64Value(1000000), sha3_128},
		{"SHA3_11bytes", Uint64Value(1000000000000), sha3_128},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			hashcode, err := bm.v.HashCode()
			require.NoError(b, err)

			for i := 0; i < b.N; i++ {
				lo, hi = bm.hashFunc(hashcode)
			}
		})
	}
}
