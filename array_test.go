package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendAndGet(t *testing.T) {
	storage := NewBasicStorage()

	array := NewArray(storage)

	n := uint64(256 * 256)
	for i := uint64(0); i < n; i++ {
		err := array.Append(i)
		require.NoError(t, err)
	}

	for i := uint64(0); i < n; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)
		require.Equal(t, i, e)
	}
}

func TestSplit(t *testing.T) {
	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	storage := NewBasicStorage()

	array := NewArray(storage)

	n := uint64(50)
	for i := uint64(0); i < n; i++ {
		err := array.Append(i)
		require.NoError(t, err)
	}

	// root (internal) node
	require.NotNil(t, array.root)
	require.False(t, array.root.IsLeaf())
	require.Equal(t, uint32(50), array.root.header.count)
	require.Equal(t, uint32(16*3), array.root.header.size) // 3 headers
	require.Equal(t, 3, len(array.root.orderedHeaders))    // 3 headers

	for _, h := range array.root.orderedHeaders {
		id := h.id
		slab, found, err := storage.Retrieve(id)
		require.NoError(t, err)
		require.True(t, found)
		require.False(t, slab.IsLeaf())
	}
}

func TestIterate(t *testing.T) {
	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	storage := NewBasicStorage()

	array := NewArray(storage)

	n := uint64(256 * 256)
	for i := uint64(0); i < n; i++ {
		err := array.Append(i)
		require.NoError(t, err)
	}

	i := uint64(0)
	err := array.Iterate(func(v uint64) {
		require.Equal(t, i, v)
		i++
	})
	require.NoError(t, err)
	require.Equal(t, i, n)
}
