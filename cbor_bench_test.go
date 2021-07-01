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

func BenchmarkCBORArrayUint8(b *testing.B) {
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
	benchmarkCBORArray(b, values)
}

func BenchmarkCBORArrayUint64(b *testing.B) {
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
	benchmarkCBORArray(b, values)
}

func BenchmarkCBORArrayMixedTypes(b *testing.B) {
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
	benchmarkCBORArray(b, values)
}

func benchmarkCBORArray(b *testing.B, values []Storable) {

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
