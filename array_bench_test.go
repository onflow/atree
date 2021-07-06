/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package main

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// var slabTargetSize = flag.Int("slabsize", 1024, "target slab size")

var noop Storable

func BenchmarkGetXSArray(b *testing.B) { benchmarkGet(b, 10, 100) }

func BenchmarkGetSArray(b *testing.B) { benchmarkGet(b, 1000, 100) }

func BenchmarkGetMArray(b *testing.B) { benchmarkGet(b, 10_000, 100) }

func BenchmarkGetLArray(b *testing.B) { benchmarkGet(b, 100_000, 100) }

func BenchmarkGetXLArray(b *testing.B) { benchmarkGet(b, 1_000_000, 100) }

func BenchmarkGetXXLArray(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkGetXXLArray in short mode")
	}
	benchmarkGet(b, 10_000_000, 100)
}

func BenchmarkInsertXSArray(b *testing.B) { benchmarkInsert(b, 10, 100) }

func BenchmarkInsertSArray(b *testing.B) { benchmarkInsert(b, 1000, 100) }

func BenchmarkInsertMArray(b *testing.B) { benchmarkInsert(b, 10_000, 100) }

func BenchmarkInsertLArray(b *testing.B) { benchmarkInsert(b, 100_000, 100) }

func BenchmarkInsertXLArray(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkInsertXLArray in short mode")
	}
	benchmarkInsert(b, 1_000_000, 100)
}

func BenchmarkInsertXXLArray(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkInsertXXLArray in short mode")
	}
	benchmarkInsert(b, 10_000_000, 100)
}

func BenchmarkRemoveXSArray(b *testing.B) { benchmarkRemove(b, 10, 10) }

func BenchmarkRemoveSArray(b *testing.B) { benchmarkRemove(b, 1000, 100) }

func BenchmarkRemoveMArray(b *testing.B) { benchmarkRemove(b, 10_000, 100) }

func BenchmarkRemoveLArray(b *testing.B) { benchmarkRemove(b, 100_000, 100) }

func BenchmarkRemoveXLArray(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkRemoveXLArray in short mode")
	}
	benchmarkRemove(b, 1_000_000, 100)
}

func BenchmarkRemoveXXLArray(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping BenchmarkRemoveXXLArray in short mode")
	}
	benchmarkRemove(b, 10_000_000, 100)
}

// XXXLArray takes too long to run.
// func BenchmarkLookupXXXLArray(b *testing.B) { benchmarkLookup(b, 100_000_000, 100) }

func setupArray(storage *PersistentSlabStorage, initialArraySize int) (*Array, error) {

	array := NewArray(storage)

	for i := 0; i < initialArraySize; i++ {
		v := RandomValue()
		err := array.Append(v)
		if err != nil {
			return nil, err
		}
	}

	err := storage.Commit()
	if err != nil {
		return nil, err
	}

	arrayID := array.StorageID()

	storage.DropCache()

	newArray, err := NewArrayWithRootID(storage, arrayID)
	if err != nil {
		return nil, err
	}

	return newArray, nil
}

func benchmarkGet(b *testing.B, initialArraySize, numberOfElements int) {

	b.StopTimer()

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage, WithNoAutoCommit())

	array, err := setupArray(storage, initialArraySize)
	require.NoError(b, err)

	var storable Storable

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		for i := 0; i < numberOfElements; i++ {
			index := rand.Intn(int(array.Count()))
			v, _ := array.Get(uint64(index))
			storable = v
		}
	}

	noop = storable
}

func benchmarkInsert(b *testing.B, initialArraySize, numberOfElements int) {

	b.StopTimer()

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage, WithNoAutoCommit())

	for i := 0; i < b.N; i++ {

		b.StopTimer()

		array, err := setupArray(storage, initialArraySize)
		require.NoError(b, err)

		b.StartTimer()

		for i := 0; i < numberOfElements; i++ {
			index := rand.Intn(int(array.Count()))
			v := RandomValue()
			array.Insert(uint64(index), v)
		}
	}
}

func benchmarkRemove(b *testing.B, initialArraySize, numberOfElements int) {

	b.StopTimer()

	baseStorage := NewInMemBaseStorage()

	storage := NewPersistentSlabStorage(baseStorage, WithNoAutoCommit())

	for i := 0; i < b.N; i++ {

		b.StopTimer()

		array, err := setupArray(storage, initialArraySize)
		require.NoError(b, err)

		b.StartTimer()

		for i := 0; i < numberOfElements; i++ {
			index := rand.Intn(int(array.Count()))
			array.Remove(uint64(index))
		}
	}
}
