/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

const (
	cborArrayElementsTargetSize = 512*3 - 20 // max slab size minus data slab fix-sized prefix
)

func BenchmarkEncodeCBORArrayUint8(b *testing.B) {
	values := getUint8Values()
	benchmarkEncodeCBORArray(b, values)
}

func BenchmarkEncodeCBORArrayUint64(b *testing.B) {
	values := getUint64Values()
	benchmarkEncodeCBORArray(b, values)
}

func BenchmarkEncodeCBORArrayMixedTypes(b *testing.B) {
	values := getMixTypedValues()
	benchmarkEncodeCBORArray(b, values)
}

func BenchmarkDecodeCBORArrayUint8(b *testing.B) {
	values := getUint8Values()
	encMode, err := cbor.CanonicalEncOptions().EncMode()
	require.NoError(b, err)
	data := encodeStorables(b, values, encMode)
	benchmarkDecodeCBORArray(b, data)
}

func BenchmarkDecodeCBORArrayUint64(b *testing.B) {
	values := getUint64Values()
	encMode, err := cbor.CanonicalEncOptions().EncMode()
	require.NoError(b, err)
	data := encodeStorables(b, values, encMode)
	benchmarkDecodeCBORArray(b, data)
}

func BenchmarkDecodeCBORArrayMixedTypes(b *testing.B) {
	values := getMixTypedValues()
	encMode, err := cbor.CanonicalEncOptions().EncMode()
	require.NoError(b, err)
	data := encodeStorables(b, values, encMode)
	benchmarkDecodeCBORArray(b, data)
}

func benchmarkEncodeCBORArray(b *testing.B, values []Storable) {

	b.Logf("Encoding array of %d elements", len(values))

	encMode, err := cbor.CanonicalEncOptions().EncMode()
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		encodeStorables(b, values, encMode)
	}
}

func benchmarkDecodeCBORArray(b *testing.B, data []byte) {

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cborDec := cbor.NewByteStreamDecoder(data)

		elemCount, _ := cborDec.DecodeArrayHead()

		elements := make([]Storable, elemCount)
		for i := 0; i < int(elemCount); i++ {
			storable, _ := decodeStorable(cborDec, StorageIDUndefined)
			elements[i] = storable
		}
	}
}

func getUint8Values() []Storable {
	var values []Storable
	size := 0
	for {
		v := Uint8Value(0)
		if size+int(v.ByteSize()) > cborArrayElementsTargetSize {
			break
		}
		size += int(v.ByteSize())
		values = append(values, v)
	}
	return values
}

func getUint64Values() []Storable {
	var values []Storable
	size := 0
	for {
		v := Uint64Value(math.MaxUint64)
		if size+int(v.ByteSize()) > cborArrayElementsTargetSize {
			break
		}
		size += int(v.ByteSize())
		values = append(values, v)
	}
	return values
}

func getMixTypedValues() []Storable {
	var values []Storable
	size := 0
	for {
		var v Storable
		// TODO: Use Ramtin's RandomValue in benchmark after it is merged.
		switch rand.Intn(4) {
		case 0:
			v = Uint8Value(rand.Intn(math.MaxUint8))
		case 1:
			v = Uint16Value(rand.Intn(math.MaxUint16))
		case 2:
			v = Uint32Value(rand.Intn(math.MaxUint32))
		case 3:
			v = Uint64Value(rand.Intn(1844674407370955161))
		default:
			panic(fmt.Sprintf("missing case for %d", v))
		}

		if size+int(v.ByteSize()) > cborArrayElementsTargetSize {
			break
		}
		size += int(v.ByteSize())
		values = append(values, v)
	}
	return values
}

func encodeStorables(t testing.TB, values []Storable, encMode cbor.EncMode) []byte {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, encMode)

	enc.Scratch[0] = 0x80 | 27
	binary.BigEndian.PutUint64(enc.Scratch[1:], uint64(len(values)))
	_, err := enc.Write(enc.Scratch[:9])
	require.NoError(t, err)

	for _, v := range values {
		err = v.Encode(enc)
		require.NoError(t, err)
	}

	err = enc.CBOR.Flush()
	require.NoError(t, err)

	return buf.Bytes()
}
