package main

import (
	"math/rand"
	"testing"
	"time"

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

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)
}

func TestInsertAndGet(t *testing.T) {
	t.Parallel()

	t.Run("insert-first", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		storage := NewBasicStorage()

		array := NewArray(storage)

		n := uint64(256 * 256)
		for i := uint64(0); i < n; i++ {
			err := array.Insert(0, n-i-1)
			require.NoError(t, err)
		}

		for i := uint64(0); i < n; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, i, e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})

	t.Run("insert-last", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		storage := NewBasicStorage()

		array := NewArray(storage)

		n := uint64(256 * 256)
		for i := uint64(0); i < n; i++ {
			err := array.Insert(i, i)
			require.NoError(t, err)
		}

		for i := uint64(0); i < n; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, i, e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})

	t.Run("insert", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		storage := NewBasicStorage()

		array := NewArray(storage)

		n := uint64(256 * 256)
		for i := uint64(0); i < n; i += 2 {
			err := array.Append(i)
			require.NoError(t, err)
		}

		for i := uint64(1); i < n; i += 2 {
			err := array.Insert(i, i)
			require.NoError(t, err)
		}

		for i := uint64(0); i < n; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, i, e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})
}

func TestRemove(t *testing.T) {
	t.Parallel()

	t.Run("remove-first", func(t *testing.T) {
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

		require.Equal(t, n, array.Count())

		for i := uint64(0); i < n; i++ {
			v, err := array.Remove(0)
			require.NoError(t, err)
			require.Equal(t, i, v)
			require.Equal(t, n-i-1, array.Count())

			if i%8 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())
	})

	t.Run("remove-last", func(t *testing.T) {
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

		require.Equal(t, n, array.Count())

		for i := int(n) - 1; i >= 0; i-- {
			v, err := array.Remove(uint64(i))
			require.NoError(t, err)
			require.Equal(t, uint64(i), v)
			require.Equal(t, uint64(i), array.Count())

			if i%8 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())
	})

	t.Run("remove", func(t *testing.T) {
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

		require.Equal(t, n, array.Count())

		// Remove every other elements
		for i := uint64(0); i < array.Count(); i++ {
			expected, err := array.Get(i)
			require.NoError(t, err)

			v, err := array.Remove(i)
			require.NoError(t, err)
			require.Equal(t, expected, v)

			if i%8 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		for i, j := uint64(0), uint64(1); i < array.Count(); i, j = i+1, j+2 {
			v, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, j, v)
		}
	})
}

func TestSplit(t *testing.T) {
	t.Run("leaf node as root", func(t *testing.T) {
		storage := NewBasicStorage()

		array := NewArray(storage)

		n := uint64(50)
		for i := uint64(0); i < n; i++ {
			err := array.Append(i)
			require.NoError(t, err)
		}

		// leaf node as root
		require.NotNil(t, array.root)
		require.True(t, array.root.IsLeaf())
		require.Equal(t, uint32(50), array.root.Header().count)
		require.Equal(t, uint32(8*50), array.root.Header().size)
	})
	t.Run("internal node as root", func(t *testing.T) {
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

		// meta node as root
		require.NotNil(t, array.root)
		require.False(t, array.root.IsLeaf())
		require.Equal(t, uint32(50), array.root.Header().count)
		require.Equal(t, uint32(16*3), array.root.Header().size) // 3 headers

		root := array.root.(*ArrayMetaDataSlab)
		for _, h := range root.orderedHeaders {
			id := h.id
			slab, found, err := storage.Retrieve(id)
			require.NoError(t, err)
			require.True(t, found)
			require.False(t, slab.IsLeaf())
		}
	})
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

func TestConstRootStorageID(t *testing.T) {
	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	storage := NewBasicStorage()

	array := NewArray(storage)
	err := array.Append(0)
	require.NoError(t, err)

	savedRootID := array.StorageID()
	require.NotEqual(t, StorageIDUndefined, savedRootID)

	n := uint64(256 * 256)
	for i := uint64(1); i < n; i++ {
		err := array.Append(i)
		require.NoError(t, err)
	}

	rootID := array.StorageID()
	require.Equal(t, savedRootID, rootID)

	for i := uint64(0); i < n; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)
		require.Equal(t, i, e)
	}
}

func TestSetRandomValue(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	t.Parallel()

	t.Run("insert-first", func(t *testing.T) {

		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		storage := NewBasicStorage()

		array := NewArray(storage)

		n := uint64(256 * 256)
		values := make([]uint64, n)

		for i := uint64(0); i < n; i++ {
			v := rand.Uint64()
			values[n-i-1] = v

			err := array.Insert(0, v)
			require.NoError(t, err)
		}

		for i := uint64(0); i < n; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, values[i], e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})

	t.Run("insert-last", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		storage := NewBasicStorage()

		array := NewArray(storage)

		n := uint64(256 * 256)
		values := make([]uint64, n)

		for i := uint64(0); i < n; i++ {
			v := rand.Uint64()
			values[i] = v

			err := array.Insert(i, v)
			require.NoError(t, err)
		}

		for i := uint64(0); i < n; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, values[i], e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})

	t.Run("insert-random", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		storage := NewBasicStorage()

		array := NewArray(storage)

		n := uint64(256 * 256)
		values := make([]uint64, n)

		for i := uint64(0); i < n; i++ {
			k := rand.Intn(int(i) + 1)
			v := rand.Uint64()

			copy(values[k+1:], values[k:])
			values[k] = v

			err := array.Insert(uint64(k), v)
			require.NoError(t, err)
		}

		for k, v := range values {
			e, err := array.Get(uint64(k))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})

}

func TestRemoveRandomElement(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	storage := NewBasicStorage()

	array := NewArray(storage)

	n := uint64(256 * 256)
	values := make([]uint64, n)

	// Insert n random values into array
	for i := uint64(0); i < n; i++ {
		v := rand.Uint64()
		values[i] = v

		err := array.Insert(i, v)
		require.NoError(t, err)
	}

	require.Equal(t, n, array.Count())

	// Remove n elements at random index
	for i := uint64(0); i < n; i++ {
		k := rand.Intn(int(array.Count()))

		v, err := array.Remove(uint64(k))
		require.NoError(t, err)
		require.Equal(t, values[k], v)

		copy(values[k:], values[k+1:])
		values = values[:len(values)-1]
	}

	require.Equal(t, uint64(0), array.Count())
	require.Equal(t, uint64(0), uint64(len(values)))

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)
}
