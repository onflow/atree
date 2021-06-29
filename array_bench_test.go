/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"flag"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

var elementCount = flag.Int("elemcount", 100000, "number of elements in atree")
var slabTargetSize = flag.Int("slabsize", 1024, "target slab size")

// perm returns a slice of n Storables (Uint64Value) in the range of [0, MaxUint64].
func perm(n int) []Storable {
	values := make([]Storable, n)
	for i := 0; i < n; i++ {
		v := rand.Uint64()
		values[i] = Uint64Value(v)
	}
	return values
}

var noop Storable

func BenchmarkGet(b *testing.B) {

	b.StopTimer()

	setThreshold(uint64(*slabTargetSize))

	// rvalues are random numbers to populate atree.
	rvalues := perm(*elementCount)

	// rindices are random indices for Get function.
	rindices := rand.Perm(*elementCount)

	// Create and populate an array of elemCount elements.
	array := NewArray(NewBasicSlabStorage())
	for _, v := range rvalues {
		array.Append(v)
	}
	require.Equal(b, len(rvalues), int(array.Count()))

	var storable Storable

	b.StartTimer()
	i := 0
	for i < b.N {
		for _, idx := range rindices {
			v, _ := array.Get(uint64(idx))
			storable = v

			i++
			if i >= b.N {
				return
			}
		}
	}

	noop = storable
}

func BenchmarkInsert(b *testing.B) {

	b.StopTimer()

	setThreshold(uint64(*slabTargetSize))

	// rvalues are random numbers to populate atree.
	rvalues := perm(*elementCount)

	storage := NewBasicSlabStorage()

	b.Run("insert-first", func(b *testing.B) {
		array := NewArray(storage)

		b.StartTimer()
		i := 0
		for i < b.N {
			for _, v := range rvalues {
				array.Insert(0, v)

				i++
				if i >= b.N {
					return
				}
			}
		}
		b.StopTimer()
	})

	b.Run("insert-last", func(b *testing.B) {
		array := NewArray(storage)

		b.StartTimer()
		i := 0
		for i < b.N {
			for idx, v := range rvalues {
				array.Insert(uint64(idx), v)

				i++
				if i >= b.N {
					return
				}
			}
		}
		b.StopTimer()
	})

	b.Run("insert-random", func(b *testing.B) {
		array := NewArray(storage)

		rindice := make([]int, *elementCount)
		for i := 0; i < len(rindice); i++ {
			if i == 0 {
				rindice[i] = 0
			} else {
				rindice[i] = rand.Intn(i)
			}
		}

		b.StartTimer()
		i := 0
		for i < b.N {
			for idx, v := range rvalues {
				array.Insert(uint64(rindice[idx]), v)

				i++
				if i >= b.N {
					return
				}
			}
		}
		b.StopTimer()
	})
}

func BenchmarkSet(b *testing.B) {
	// TODO
}

func BenchmarkRemove(b *testing.B) {
	// TODO
}

func BenchmarkRemoveInsert(b *testing.B) {
	// TODO
}
