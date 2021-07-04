/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
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
	data := encode(values)
	benchmarkDecodeCBORArray(b, data)
}

func BenchmarkDecodeCBORArrayUint64(b *testing.B) {
	values := getUint64Values()
	data := encode(values)
	benchmarkDecodeCBORArray(b, data)
}

func BenchmarkDecodeCBORArrayMixedTypes(b *testing.B) {
	values := getMixTypedValues()
	data := encode(values)
	benchmarkDecodeCBORArray(b, data)
}

func benchmarkEncodeCBORArray(b *testing.B, values []Storable) {

	b.Logf("Encoding array of %d elements", len(values))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		enc := newEncoder(&buf)

		enc.scratch[0] = 0x80 | 27
		binary.BigEndian.PutUint64(enc.scratch[1:], uint64(len(values)))
		enc.Write(enc.scratch[:9])

		for _, v := range values {
			v.Encode(enc)
		}
		enc.cbor.Flush()
	}
}

func benchmarkDecodeCBORArray(b *testing.B, data []byte) {

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cborDec := NewByteStreamDecoder(data)

		elemCount, _ := cborDec.DecodeArrayHead()

		elements := make([]Storable, elemCount)
		for i := 0; i < int(elemCount); i++ {
			storable, _ := decodeStorable(cborDec)
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
		}

		if size+int(v.ByteSize()) > cborArrayElementsTargetSize {
			break
		}
		size += int(v.ByteSize())
		values = append(values, v)
	}
	return values
}

func encode(values []Storable) []byte {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	enc.scratch[0] = 0x80 | 27
	binary.BigEndian.PutUint64(enc.scratch[1:], uint64(len(values)))
	enc.Write(enc.scratch[:9])

	for _, v := range values {
		v.Encode(enc)
	}
	enc.cbor.Flush()
	return buf.Bytes()
}
