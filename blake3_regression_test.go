/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright Flow Foundation
 * Copyright 2021 Faye Amacker
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
 *
 * ------------------------------------------------------------------------
 *
 * This file is a modified subset of circlehash64_test.go copied from
 *
 *     https://github.com/fxamacker/circlehash
 *
 * Expected digest values and some names of functions and variables were
 * modified to check BLAKE3 instead of CircleHash64. Seeds were removed.
 * Test data size was increased from 16KiB to 64KiB to be able to
 * verify code paths in BLAKE3 that might be optimized for larger
 * input sizes.
 */

package atree_test

import (
	"bytes"
	"crypto/sha512"
	"hash"
	"testing"

	"github.com/stretchr/testify/require"
	blake3zeebo "github.com/zeebo/blake3"
	blake3luke "lukechampine.com/blake3"
)

// Compatibility tests check 131072 BLAKE3 digests produced by hashing
// input sizes of various lengths (0-64KiB bytes) and then hashing the
// equivalent "concatenated" BLAKE3 digests with SHA-512.
//
// Additionally, each 131072 BLAKE3 digest is compared between
// github.com/zeebo/blake3 and lukechampine.com/blake3 libraries.
//
// Tests use SHA-512 digest to represent many BLAKE3 digests because SHA-512 is
// included in Go and is available in many languages. This approach eliminates
// the need to store and separately compare 131072 BLAKE3 digests.
//
// Tests for input sizes greater than 128 bytes can help BLAKE3 implementations
// that rely on input size to determine which optimized code path to execute.

var countBLAKE3 uint64 // count calls to Hash256 (doesn't double count zeebo & luke)

func TestBLAKE3Vectors(t *testing.T) {

	// Official BLAKE3 test vector checks 35 digests using
	// 35 sizes of input from the same repeating pattern
	inputs := makeBLAKE3InputData(102400)

	sizes := []int{
		0, 1, 2, 3, 4, 5, 6, 7,
		8, 63, 64, 65, 127, 128, 129, 1023,
		1024, 1025, 2048, 2049, 3072, 3073, 4096, 4097,
		5120, 5121, 6144, 6145, 7168, 7169, 8192, 8193,
		16384, 31744, 102400,
	}

	// Use SHA-512 to hash 35 BLAKE3 digest results
	// so we only have to compare one hardcoded value.
	h := sha512.New()
	want := decodeHexOrPanic("b785cc13e1ed42b2c31096c91aacf155d2898bcf2fbcfd3a02b481612423a4372a6367bd5da5ce9e1edadef81d44d77363060a4c4b6af436e4b4c189f6f72b3e")

	for _, n := range sizes {
		digest := countedAndComparedBLAKE3(t, inputs[:n])
		_, err := h.Write(digest[:])
		require.NoError(t, err)
	}

	got := h.Sum(nil)
	if !bytes.Equal(got, want) {
		t.Errorf("got 0x%064x; want 0x%064x", got, want)
	}
}

func makeBLAKE3InputData(length int) []byte {
	b := make([]byte, length)
	for i := 0; i < len(b); i++ {
		b[i] = byte(i % 251)
	}
	return b
}

// TestBLAKE3Regression checks 131072 BLAKE3 digests
// for expected values using input sizes up to
// 64KiB.  This test is designed to detect
// malicious hash implementations when this test
// is used with other tests for size 0 and some
// sizes > 64KiB.
func TestBLAKE3Regression(t *testing.T) {

	// Create 64 KiB of test data from SHA-512 using the simplest
	// form of SHA-512 feedback loop (nothing-up-my-sleeve).
	data := nonUniformBytes64KiB()

	// Verify BLAKE3 digests produced from hashing portions of
	// data. Input sizes vary from 1 to 64KiB bytes by varying
	// starting pos and ending pos.
	// We use 64KiB because BLAKE3 implementations can have
	// special optimizations for large data sizes and we
	// want to verify digests produced by all their code paths.

	testCases := []struct {
		name                     string
		wantSHA512VaringStartPos []byte
		wantSHA512VaringEndPos   []byte
	}{
		{
			"nokey",
			decodeHexOrPanic("8030991ad495219d9fdd346fab027d1f453887a3d157fa4bfcd67a4b213a6817817817f43779ddd2b274a243d8a942728141b72d8bcde9d49fdfc5d9a823983f"),
			decodeHexOrPanic("a8a7c00fce5a6adc774e8bf5ff45b40f382954c932288d0d79d589755b094f8db6fa16780e2ca9a1434b56a0716a25fb7eecb545f1c6f7599b08214fd1b59a8a"),
		},
		// If Atree begins to use keyed BLAKE3, add testCases here for keyed hashes
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			h := sha512.New()

			// test 65536 BLAKE3 digests by varying start pos of data
			checksumVaryingStartPosNoSeed(t, h, data)
			got := h.Sum(nil)
			if !bytes.Equal(got, tc.wantSHA512VaringStartPos) {
				t.Errorf("checksumVaryingStartPos(nonuniform16KiB) = 0x%0128x; want 0x%0128x",
					got,
					tc.wantSHA512VaringStartPos)
			}

			h.Reset()

			// test another 65536 BLAKE3 digests by varying end pos of data
			checksumVaryingEndPosNoSeed(t, h, data)
			got = h.Sum(nil)
			if !bytes.Equal(got, tc.wantSHA512VaringEndPos) {
				t.Errorf("checksumVaryingEndPos(nonuniform16KiB) = 0x%0128x; want 0x%0128x",
					got,
					tc.wantSHA512VaringEndPos)
			}
		})
	}
}

// checksumVaryingStartPosNoSeed updates cryptoHash512 with
// concatenated BLAKE3 digests.
func checksumVaryingStartPosNoSeed(t *testing.T, cryptoHash512 hash.Hash, data []byte) {

	// vary the starting position and keep the ending position
	for i := uint64(0); i < uint64(len(data)); i++ {

		digest := countedAndComparedBLAKE3(t, data[i:])

		// Feed digest into SHA-512, SHA3-512, etc.
		cryptoHash512.Write(digest[:])
	}
}

// checksumVaryingEndPosNoSeed updates cryptoHash512 with
// concatenated BLAKE3 digests.
func checksumVaryingEndPosNoSeed(t *testing.T, cryptoHash512 hash.Hash, data []byte) {

	// keep the starting position at zero and increment the length
	for i := uint64(1); i <= uint64(len(data)); i++ {
		digest := countedAndComparedBLAKE3(t, data[:i])

		// Feed digest into SHA-512, SHA3-512, etc.
		cryptoHash512.Write(digest[:])
	}
}

// nonUniformBytes64KiB returns 64KiB bytes of non-uniform bytes
// produced from SHA-512 in a feedback loop. SHA-512 is used instead
// of SHAKE-256 XOF or a stream cipher because SHA-512 is bundled with
// Go and is available in most languages. One reason a simple PRNG
// isn't used here is because different implementions in different
// programming languages are sometimes incompatible due to errors
// (like SplitMix64). SHA-512 will be compatible everywhere.
// For BLAKE3, we should use at least 64KiB because implementations
// might use optimized paths for various large input sizes.
func nonUniformBytes64KiB() []byte {
	b := make([]byte, 0, 1024*64)

	// Each input to SHA-512 is 64 bytes. First 64-byte input is zeros.
	// The next input to SHA-512 is the 64-byte output of SHA-512.
	// Each output of SHA-512 is appended to the returned byte slice.
	d := make([]byte, 64)
	for i := 0; i < 1024; i++ {
		a := sha512.Sum512(d)
		d = a[:]
		b = append(b, d...)
	}

	return b
}

func countedAndComparedBLAKE3(t *testing.T, data []byte) [32]byte {
	digest := blake3zeebo.Sum256(data)
	digest2 := blake3luke.Sum256(data)
	if digest != digest2 {
		t.Errorf("BLAKE3zeebo 0x%x != BLAKE3luke 0x%x", digest, digest2)
	}

	countBLAKE3++
	return digest
}
