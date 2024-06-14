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
 * This subset verifies nearly 200,000 CircleHash64 digests.  The full
 * CircleHash64 compatibility tests verify nearly 600,000 digests.
 */

package atree

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"testing"

	"github.com/fxamacker/circlehash"
	"github.com/stretchr/testify/require"
)

// CircleHash64 uses CircleHash64f as default hash. Expected SHA-512 checksums are
// from the C++ and Go CircleHash reference implementations by Faye Amacker.
// SHA-512 is used because it's included in Go and available in many languages.
//
// Compatibility tests check CircleHash64 digests produced by hashing
// input sizes of various lengths (0-16384 bytes).  Tests for input sizes greater
// than 128 bytes can help future implementations that rely on input size to
// determine which optimized code path to execute.

const (
	// nums are nothing-up-my-sleeve numbers
	numsAllZeros = uint64(0x0000000000000000)
	numsAll55s   = uint64(0x5555555555555555) // alternating 1 and 0 bit
	numsAllAAs   = uint64(0xAAAAAAAAAAAAAAAA) // alternating 0 and 1 bit
	numsAllFFs   = uint64(0xFFFFFFFFFFFFFFFF)

	numsGoldenRatio    = uint64(0x9E3779B97F4A7C15) // https://en.wikipedia.org/wiki/Golden_ratio
	numsGoldenRatioInv = numsGoldenRatio ^ numsAllFFs
)

var countCircleHash64f uint64 // count calls to Hash64 (doesn't include calls to HashString64)

// TestCircleHash64Regression is renamed from TestCircleHash64NonUniformBitPatternInputs.
func TestCircleHash64Regression(t *testing.T) {

	// Create 16 KiB of test data from SHA-512 using the simplest
	// form of SHA-512 feedback loop (nothing-up-my-sleeve).
	data := nonUniformBytes16KiB()

	// Verify CircleHash64 digests produced from hashing portions of
	// data using different seed values. Input sizes vary from
	// 1 to 16384 bytes by varying starting pos and ending pos.

	testCases := []struct {
		name                     string
		seed                     uint64
		wantSHA512VaringStartPos []byte
		wantSHA512VaringEndPos   []byte
	}{
		{
			"seed 00s",
			numsAllZeros,
			decodeHexOrPanic("8fc041d09087f9f3108ed86422ee6562f4eaf1ad0b1d83ede3f69b14f3798b8a5c80518ea7041f0803882ced33bce34351c5415469957e40ddd806d618742a71"),
			decodeHexOrPanic("80348e245dec5e09c424411c4dfa9fbbad1cbf68495707e3579bec8c7e7e010f6ff441b6b3987e4da28be39ccd355ae545ca0329284fa39a0d630b941e83355a"),
		},

		{
			"seed 55s",
			numsAll55s,
			decodeHexOrPanic("714733e2f758328f07556e849cc96b371dba28ed8c6c934f6591a7e4ea90a02dc93bb858639ed62b3aacc26932efe3a47aa4e5b713a8f1c2a5375988fb3fcf05"),
			decodeHexOrPanic("90ac86e7e8ce973ad402d1db741c7ee320b330ffbbe391c9b20eb9ce07385c66df3f40efd0865ee18894b559cde70f38ec7b01319b2ef2f3f61c64cc8abeca12"),
		},

		{
			"seed AAs",
			numsAllAAs,
			decodeHexOrPanic("710f68717bf5144e703e10236d9d2cda2b7e8e503aacf4168a088a1be51d3ffe83cf19908e238be15883f6cd25a2c7c71e715173e19fc73f5707ad7626c3b944"),
			decodeHexOrPanic("042037e4dfaf0072c5048c8043fa6ac1f197f8b3c2140d97ccfe9abd69f7ef6fb6739e0728c40bff272dbd6a0c82f7f04f95a0ca64cdfe73c080b691bd58214e"),
		},

		{
			"seed FFs",
			numsAllFFs,
			decodeHexOrPanic("e275ac0f2df55036ac844f7cbf6375fbad8b4c7fbac98296e5d0fbfbdb294534c5a45058883220572bff8145c3e2f191950f0cad2841c9bd50babe3b907469c4"),
			decodeHexOrPanic("12864d73da4f64ef97b988b400566f9b89ebaeee87629208ac7029a6cc6a57759025f83efd0480b1675fb4b06d128439c03ac300ce0c1fbd35dfaa9a91e233ac"),
		},

		{
			"seed GR",
			numsGoldenRatio,
			decodeHexOrPanic("a19151170f5a8e92a98416fff407f35317d458cd8f47a3d28b9a2ddcb277d6d0ff895a1b06f6aa5f25c67b71c74d9f6705ffbfe27edd1237ee990395f61842f2"),
			decodeHexOrPanic("b853718a24f4b46e0e3d1b4cf497637af09b5aa061496707b1839f824b9b4f4294113976765a72b9dfd916d8a56cc434a7f12116cff8406c8b3ffd8a8acd80d3"),
		},

		{
			"seed GRI",
			numsGoldenRatioInv,
			decodeHexOrPanic("e18b76bb467bbbab91deeb42307964fd92db5ac6bf5718da12ba391c3a89c6f0f7c6379dcf6c7676eb1bbc8c8d240919b154086bfba65fc4d0e468b67e474195"),
			decodeHexOrPanic("626cbb08e12d6988bc7d8f75e9571961d4e46240e5ef682562f7010d8916a7b104b988f6749b67f59f5e7cb4147017842d78ce17b7c9443813b92c0e198b62e2"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			h := sha512.New()

			checksumVaryingStartPos(t, h, tc.seed, data)
			got := h.Sum(nil)
			if !bytes.Equal(got, tc.wantSHA512VaringStartPos) {
				t.Errorf("checksumVaryingStartPos(nonuniform16KiB) = 0x%0128x; want 0x%0128x",
					got,
					tc.wantSHA512VaringStartPos)
			}

			h.Reset()

			checksumVaryingEndPos(t, h, tc.seed, data)
			got = h.Sum(nil)
			if !bytes.Equal(got, tc.wantSHA512VaringEndPos) {
				t.Errorf("checksumVaryingEndPos(nonuniform16KiB) = 0x%0128x; want 0x%0128x",
					got,
					tc.wantSHA512VaringEndPos)
			}
		})
	}

	require.Equal(t, uint64(196608), countCircleHash64f) // Update comments if this line changes
}

// checksumVaryingStartPos updates cryptoHash512 with
// concatenated CircleHash64 digests. E.g. passing in data containing
// 128 bytes will use 128 CircleHash64 digests ending at
// the last byte and incrementing the start position of data.
func checksumVaryingStartPos(t *testing.T, cryptoHash512 hash.Hash, seed uint64, data []byte) {

	// vary the starting position and keep the ending position
	for i := uint64(0); i < uint64(len(data)); i++ {

		digest := countedCircleHash64(t, data[i:], seed)

		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, digest)

		// Feed CircleHash64 result into SHA-512, SHA3-512, etc.
		cryptoHash512.Write(b)
	}
}

// checksumVaryingEndPos updates cryptoHash512 with
// concatenated CircleHash64 digests. E.g. passing in data containing
// 128 bytes will use 128 CircleHash64 digests always starting at
// the first byte and incrementing the length of input size.
func checksumVaryingEndPos(t *testing.T, cryptoHash512 hash.Hash, seed uint64, data []byte) {

	// keep the starting position at zero and increment the length
	for i := uint64(1); i <= uint64(len(data)); i++ {
		digest := countedCircleHash64(t, data[0:i], seed)

		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, digest)

		// Feed CircleHash64 result into SHA-512, SHA3-512, etc.
		cryptoHash512.Write(b)
	}
}

// nonUniformBytes16Kib returns 16384 bytes of non-uniform bytes
// produced from SHA-512 in a feedback loop. SHA-512 is used instead
// of SHAKE-256 XOF or a stream cipher because SHA-512 is bundled with
// Go and is available in most languages. One reason a simple PRNG
// isn't used here is because different implementions in different
// programming languages are sometimes incompatible due to errors
// (like SplitMix64). SHA-512 will be compatible everywhere.
// SHA-512 of the returned 16384-byte slice is:
// 412895cdfdf6fd60181cd709b6aed89cce63ede8402531185c969de50eb04ae3
// 5d042d7b2758d02f97c6b13b1a397e2fbeca7ceb07c606f3602bed97984f99c6
func nonUniformBytes16KiB() []byte {
	b := make([]byte, 0, 256*64) // length=0, capacity=16384

	// Each input to SHA-512 is 64 bytes. First 64-byte input is zeros.
	// The next input to SHA-512 is the 64-byte output of SHA-512.
	// Each output of SHA-512 is appended to the returned byte slice.
	d := make([]byte, 64)
	for i := 0; i < 256; i++ {
		a := sha512.Sum512(d)
		d = a[:]
		b = append(b, d...)
	}

	return b
}

// countedCircleHash64 calls Hash64 and increments countCircleHash64.
func countedCircleHash64(t *testing.T, data []byte, seed uint64) uint64 {
	digest := circlehash.Hash64(data, seed)
	digest2 := circlehash.Hash64String(string(data), seed)
	if digest != digest2 {
		t.Errorf("Hash64() 0x%x != Hash64String() 0x%x", digest, digest2)
	}

	if len(data) == 16 {
		a := binary.LittleEndian.Uint64(data)
		b := binary.LittleEndian.Uint64(data[8:])
		digest3 := circlehash.Hash64Uint64x2(a, b, seed)
		if digest != digest3 {
			t.Errorf("Hash64() 0x%x != Hash64Uint64x2() 0x%x", digest, digest3)
		}
	}

	countCircleHash64f++
	return digest
}

func decodeHexOrPanic(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(fmt.Sprintf("bad hex string: %s", err))
	}
	return b
}
