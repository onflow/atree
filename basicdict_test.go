/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"math"
	"math/rand"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"
)

func TestBasicDictSetAndGet(t *testing.T) {

	const dictSize = 1024 * 16

	storage := newTestBasicStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	dict := NewBasicDict(storage, address)

	// Set dictionary key+values
	for i := uint64(0); i < dictSize; i++ {
		k := Uint64Value(i)
		v := Uint64Value(i + 1)
		err := dict.Set(k, v)
		require.NoError(t, err)
	}

	require.Equal(t, uint64(dictSize), dict.Count())

	// Get dictionary values
	for i := uint64(0); i < dictSize; i++ {
		k := Uint64Value(i)
		v, err := dict.Get(k)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i+1), v)
	}

	// Set new values
	for i := uint64(0); i < dictSize; i++ {
		k := Uint64Value(i)
		v := Uint64Value(i * 10)
		err := dict.Set(k, v)
		require.NoError(t, err)
	}

	require.Equal(t, uint64(dictSize), dict.Count())

	// Get new values
	for i := uint64(0); i < dictSize; i++ {
		k := Uint64Value(i)
		v, err := dict.Get(k)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i*10), v)
	}

	// Get non-existent keys
	for i := uint64(dictSize); i < dictSize*2; i++ {
		k := Uint64Value(i)
		_, err := dict.Get(k)
		require.Error(t, err)
	}

	require.Equal(t, 1, storage.Count())
}

func TestBasicDictRemove(t *testing.T) {

	const dictSize = 1024 * 16

	storage := newTestBasicStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	dict := NewBasicDict(storage, address)

	// Set dictionary key+values
	for i := uint64(0); i < dictSize; i++ {
		k := Uint64Value(i)
		v := Uint64Value(i + 1)
		err := dict.Set(k, v)
		require.NoError(t, err)
	}

	require.Equal(t, uint64(dictSize), dict.Count())

	// Remove every other elements
	for i := uint64(0); i < dictSize; i += 2 {
		k := Uint64Value(i)

		v, err := dict.Remove(k)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i+1), v)
	}

	require.Equal(t, uint64(dictSize/2), dict.Count())

	// Get dictionary remaining values
	for i := uint64(1); i < dictSize; i += 2 {
		k := Uint64Value(i)
		v, err := dict.Get(k)
		require.NoError(t, err)
		require.Equal(t, Uint64Value(i+1), v)
	}

	require.Equal(t, 1, storage.Count())
}

func TestBasicDictRandomSetRemoveMixedTypes(t *testing.T) {

	const (
		SetAction = iota
		RemoveAction
		MaxAction
	)

	const (
		Uint8Type = iota
		Uint16Type
		Uint32Type
		Uint64Type
		StringType
		MaxType
	)

	const actionCount = 2 * 1024

	const stringMaxSize = 1024

	storage := newTestInMemoryStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	dict := NewBasicDict(storage, address)

	keyValues := make(map[string]Value)
	var keys []Value

	for i := uint64(0); i < actionCount; i++ {

		action := rand.Intn(MaxAction)

		if len(keys) == 0 {
			action = SetAction
		}

		switch action {

		case SetAction:

			var k Value

			switch rand.Intn(MaxType) {
			case Uint8Type:
				n := rand.Intn(math.MaxUint8 + 1)
				k = Uint8Value(n)
			case Uint16Type:
				n := rand.Intn(math.MaxUint16 + 1)
				k = Uint16Value(n)
			case Uint32Type:
				k = Uint32Value(rand.Uint32())
			case Uint64Type:
				k = Uint64Value(rand.Uint64())
			case StringType:
				k = NewStringValue(randStr(rand.Intn(stringMaxSize)))
			}

			var v Value

			switch rand.Intn(MaxType) {
			case Uint8Type:
				n := rand.Intn(math.MaxUint8 + 1)
				v = Uint8Value(n)
			case Uint16Type:
				n := rand.Intn(math.MaxUint16 + 1)
				v = Uint16Value(n)
			case Uint32Type:
				v = Uint32Value(rand.Uint32())
			case Uint64Type:
				v = Uint64Value(rand.Uint64())
			case StringType:
				v = NewStringValue(randStr(rand.Intn(stringMaxSize)))
			}

			keyString := dictionaryKey(k)

			if _, ok := keyValues[keyString]; !ok {
				keys = append(keys, k)
			}

			keyValues[keyString] = v

			err := dict.Set(k, v)
			require.NoError(t, err)

		case RemoveAction:
			ki := rand.Intn(len(keys))
			k := keys[ki]

			keyString := dictionaryKey(k)

			v, err := dict.Remove(k)
			require.NoError(t, err)
			require.Equal(t, keyValues[keyString], v)

			delete(keyValues, keyString)

			copy(keys[ki:], keys[ki+1:])
			keys = keys[:len(keys)-1]
		}

		require.Equal(t, uint64(len(keys)), dict.Count())
	}

	for _, k := range keys {

		keyString := dictionaryKey(k)

		e, err := dict.Get(k)
		require.NoError(t, err)
		require.Equal(t, keyValues[keyString], e)
	}

	require.Equal(t, 1, storage.Count())
}

func TestBasicDictDecodeEncodeRandomData(t *testing.T) {

	const (
		SetAction = iota
		RemoveAction
		MaxAction
	)

	const (
		Uint8Type = iota
		Uint16Type
		Uint32Type
		Uint64Type
		StringType
		MaxType
	)

	const actionCount = 2 * 1024

	const stringMaxSize = 1024

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	storage := NewBasicSlabStorage(encMode, decMode)
	storage.DecodeStorable = decodeStorable

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	dict := NewBasicDict(storage, address)

	keyValues := make(map[string]Value)
	var keys []Value

	for i := uint64(0); i < actionCount; i++ {

		action := rand.Intn(MaxAction)

		if len(keys) == 0 {
			action = SetAction
		}

		switch action {

		case SetAction:

			var k Value

			switch rand.Intn(MaxType) {
			case Uint8Type:
				n := rand.Intn(math.MaxUint8 + 1)
				k = Uint8Value(n)
			case Uint16Type:
				n := rand.Intn(math.MaxUint16 + 1)
				k = Uint16Value(n)
			case Uint32Type:
				k = Uint32Value(rand.Uint32())
			case Uint64Type:
				k = Uint64Value(rand.Uint64())
			case StringType:
				k = NewStringValue(randStr(rand.Intn(stringMaxSize)))
			}

			var v Value

			switch rand.Intn(MaxType) {
			case Uint8Type:
				n := rand.Intn(math.MaxUint8 + 1)
				v = Uint8Value(n)
			case Uint16Type:
				n := rand.Intn(math.MaxUint16 + 1)
				v = Uint16Value(n)
			case Uint32Type:
				v = Uint32Value(rand.Uint32())
			case Uint64Type:
				v = Uint64Value(rand.Uint64())
			case StringType:
				v = NewStringValue(randStr(rand.Intn(stringMaxSize)))
			}

			keyString := dictionaryKey(k)

			if _, ok := keyValues[keyString]; !ok {
				keys = append(keys, k)
			}

			keyValues[keyString] = v

			err := dict.Set(k, v)
			require.NoError(t, err)

		case RemoveAction:
			ki := rand.Intn(len(keys))
			k := keys[ki]

			keyString := dictionaryKey(k)

			v, err := dict.Remove(k)
			require.NoError(t, err)
			require.Equal(t, keyValues[keyString], v)

			delete(keyValues, keyString)

			copy(keys[ki:], keys[ki+1:])
			keys = keys[:len(keys)-1]
		}

		require.Equal(t, uint64(len(keys)), dict.Count())
	}

	rootID := dict.StorageID()

	// Encode slabs
	m1, err := storage.Encode()
	require.NoError(t, err)

	// Decode data to new storage

	storage2 := NewBasicSlabStorage(encMode, decMode)
	storage2.DecodeStorable = decodeStorable

	err = storage2.Load(m1)
	require.NoError(t, err)

	// Create new array from new storage
	dict2, err := NewBasicDictWithRootID(storage2, rootID)
	require.NoError(t, err)

	for _, k := range keys {
		keyString := dictionaryKey(k)

		e, err := dict2.Get(k)
		require.NoError(t, err)
		require.Equal(t, keyValues[keyString], e)
	}
}
