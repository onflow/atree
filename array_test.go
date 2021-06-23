package main

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Seed only once and print seed for easier debugging.
func init() {
	seed := time.Now().UnixNano()
	fmt.Printf("seed: %d\n", seed)
	rand.Seed(seed)
}

func TestAppendAndGet(t *testing.T) {

	const arraySize = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i, uint64(v))
	}

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
}

func TestSetAndGet(t *testing.T) {

	const arraySize = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		err := array.Set(i, Uint64Value(i*10))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i*10, uint64(v))
	}

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
}

func TestInsertAndGet(t *testing.T) {
	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Insert(0, Uint64Value(arraySize-i-1))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(v))
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Insert(i, Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(v))
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

	t.Run("insert", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i += 2 {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(1); i < arraySize; i += 2 {
			err := array.Insert(i, Uint64Value(i))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(v))
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})
}

func TestRemove(t *testing.T) {
	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("remove-first", func(t *testing.T) {
		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		for i := uint64(0); i < arraySize; i++ {
			v, err := array.Remove(0)
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, i, uint64(e))

			require.Equal(t, arraySize-i-1, array.Count())

			if i%8 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

	t.Run("remove-last", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		for i := arraySize - 1; i >= 0; i-- {
			v, err := array.Remove(uint64(i))
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, uint64(i), uint64(e))

			require.Equal(t, uint64(i), array.Count())

			if i%8 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		require.Equal(t, uint64(0), array.Count())

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

	t.Run("remove", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		// Remove every other elements
		for i := uint64(0); i < array.Count(); i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, err := array.Remove(i)
			require.NoError(t, err)

			require.Equal(t, e, v)

			if i%8 == 0 {
				verified, err := array.valid()
				require.NoError(t, err)
				require.True(t, verified)
			}
		}

		for i, j := uint64(0), uint64(1); i < array.Count(); i, j = i+1, j+2 {
			v, err := array.Get(i)
			require.NoError(t, err)

			e, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, j, uint64(e))
		}

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})
}

func TestSplit(t *testing.T) {
	t.Run("leaf node as root", func(t *testing.T) {
		const arraySize = 50

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		// leaf node as root
		require.NotNil(t, array.root)
		require.True(t, array.root.IsLeaf())
		require.Equal(t, uint32(50), array.root.Header().count)
		require.Equal(t, uint32(8*50), array.root.Header().size)

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

	t.Run("internal node as root", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		const arraySize = 50

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
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
			node, ok := slab.(ArrayNode)
			require.True(t, ok)
			require.False(t, node.IsLeaf())
		}

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})
}

func TestIterate(t *testing.T) {
	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	for i := uint64(0); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	i := uint64(0)
	err := array.Iterate(func(v Storable) {
		e, ok := v.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i, uint64(e))
		i++
	})
	require.NoError(t, err)
	require.Equal(t, i, uint64(arraySize))
}

func TestConstRootStorageID(t *testing.T) {
	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)
	err := array.Append(Uint64Value(0))
	require.NoError(t, err)

	savedRootID := array.root.Header().id
	require.NotEqual(t, StorageIDUndefined, savedRootID)

	for i := uint64(1); i < arraySize; i++ {
		err := array.Append(Uint64Value(i))
		require.NoError(t, err)
	}

	rootID := array.root.Header().id
	require.Equal(t, savedRootID, rootID)

	for i := uint64(0); i < arraySize; i++ {
		e, err := array.Get(i)
		require.NoError(t, err)

		v, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, i, uint64(v))
	}
}

func TestSetRandomValue(t *testing.T) {

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	values := make([]uint64, arraySize)

	for i := uint64(0); i < arraySize; i++ {
		k := rand.Intn(int(i) + 1)
		v := rand.Uint64()

		copy(values[k+1:], values[k:])
		values[k] = v

		err := array.Insert(uint64(k), Uint64Value(v))
		require.NoError(t, err)
	}

	for i := uint64(0); i < arraySize; i++ {
		k := rand.Intn(arraySize)
		v := rand.Uint64()

		values[k] = v

		err := array.Set(uint64(k), Uint64Value(v))
		require.NoError(t, err)
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, v, uint64(ev))
	}

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
}

func TestInsertRandomValue(t *testing.T) {

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("insert-first", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		values := make([]uint64, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			v := rand.Uint64()
			values[arraySize-i-1] = v

			err := array.Insert(0, Uint64Value(v))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, values[i], uint64(v))
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

	t.Run("insert-last", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		values := make([]uint64, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			v := rand.Uint64()
			values[i] = v

			err := array.Insert(i, Uint64Value(v))
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)

			v, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, values[i], uint64(v))
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

	t.Run("insert-random", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		values := make([]uint64, arraySize)

		for i := uint64(0); i < arraySize; i++ {
			k := rand.Intn(int(i) + 1)
			v := rand.Uint64()

			copy(values[k+1:], values[k:])
			values[k] = v

			err := array.Insert(uint64(k), Uint64Value(v))
			require.NoError(t, err)
		}

		for k, v := range values {
			e, err := array.Get(uint64(k))
			require.NoError(t, err)

			ev, ok := e.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, v, uint64(ev))
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)

		stats, _ := array.Stats()
		require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
	})

}

func TestRemoveRandomElement(t *testing.T) {

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	values := make([]uint64, arraySize)

	// Insert n random values into array
	for i := uint64(0); i < arraySize; i++ {
		v := rand.Uint64()
		values[i] = v

		err := array.Insert(i, Uint64Value(v))
		require.NoError(t, err)
	}

	require.Equal(t, uint64(arraySize), array.Count())

	// Remove n elements at random index
	for i := uint64(0); i < arraySize; i++ {
		k := rand.Intn(int(array.Count()))

		v, err := array.Remove(uint64(k))
		require.NoError(t, err)

		ev, ok := v.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, values[k], uint64(ev))

		copy(values[k:], values[k+1:])
		values = values[:len(values)-1]
	}

	require.Equal(t, uint64(0), array.Count())
	require.Equal(t, uint64(0), uint64(len(values)))

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
}

func TestRandomAppendSetInsertRemove(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	const actionCount = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	values := make([]uint64, 0, actionCount)

	for i := uint64(0); i < actionCount; i++ {

		nextAction := rand.Intn(MaxAction)

		switch nextAction {

		case AppendAction:
			v := rand.Uint64()

			values = append(values, v)

			err := array.Append(Uint64Value(v))
			require.NoError(t, err)

		case SetAction:
			if array.Count() == 0 {
				continue
			}
			k := rand.Intn(int(array.Count()))
			v := rand.Uint64()

			values[k] = v

			err := array.Set(uint64(k), Uint64Value(v))
			require.NoError(t, err)

		case InsertAction:
			k := rand.Intn(int(array.Count() + 1))
			v := rand.Uint64()

			if k == int(array.Count()) {
				values = append(values, v)
			} else {
				values = append(values, 0)
				copy(values[k+1:], values[k:])
				values[k] = v
			}

			err := array.Insert(uint64(k), Uint64Value(v))
			require.NoError(t, err)

		case RemoveAction:
			if array.Count() > 0 {
				k := rand.Intn(int(array.Count()))

				v, err := array.Remove(uint64(k))
				require.NoError(t, err)

				ev, ok := v.(Uint64Value)
				require.True(t, ok)
				require.Equal(t, values[k], uint64(ev))

				copy(values[k:], values[k+1:])
				values = values[:len(values)-1]
			}
		}

		require.Equal(t, array.Count(), uint64(len(values)))
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint64Value)
		require.True(t, ok)
		require.Equal(t, v, uint64(ev))
	}

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
}

func TestRandomAppendSetInsertRemoveUint8(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	const actionCount = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	values := make([]uint8, 0, actionCount)

	for i := uint64(0); i < actionCount; i++ {

		nextAction := rand.Intn(MaxAction)

		switch nextAction {

		case AppendAction:
			v := rand.Intn(math.MaxUint8 + 1)

			values = append(values, uint8(v))

			err := array.Append(Uint8Value(v))
			require.NoError(t, err)

		case SetAction:
			if array.Count() == 0 {
				continue
			}
			k := rand.Intn(int(array.Count()))
			v := rand.Intn(math.MaxUint8 + 1)

			values[k] = uint8(v)

			err := array.Set(uint64(k), Uint8Value(v))
			require.NoError(t, err)

		case InsertAction:
			k := rand.Intn(int(array.Count() + 1))
			v := rand.Intn(math.MaxUint8 + 1)

			if k == int(array.Count()) {
				values = append(values, uint8(v))
			} else {
				values = append(values, 0)
				copy(values[k+1:], values[k:])
				values[k] = uint8(v)
			}

			err := array.Insert(uint64(k), Uint8Value(v))
			require.NoError(t, err)

		case RemoveAction:
			if array.Count() > 0 {
				k := rand.Intn(int(array.Count()))

				v, err := array.Remove(uint64(k))
				require.NoError(t, err)

				ev, ok := v.(Uint8Value)
				require.True(t, ok)
				require.Equal(t, values[k], uint8(ev))

				copy(values[k:], values[k+1:])
				values = values[:len(values)-1]
			}
		}

		require.Equal(t, array.Count(), uint64(len(values)))
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)

		ev, ok := e.(Uint8Value)
		require.True(t, ok)
		require.Equal(t, v, uint8(ev))
	}

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
}

func TestRandomAppendSetInsertRemoveMixedTypes(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	const (
		Uint8Type = iota
		Uint16Type
		Uint32Type
		Uint64Type
		MaxType
	)

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	const actionCount = 256 * 256

	storage := NewBasicSlabStorage()

	array := NewArray(storage)

	values := make([]Storable, 0, actionCount)

	for i := uint64(0); i < actionCount; i++ {

		var v Storable

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
		}

		switch rand.Intn(MaxAction) {

		case AppendAction:
			values = append(values, v)
			err := array.Append(v)
			require.NoError(t, err)

		case SetAction:
			if array.Count() == 0 {
				continue
			}
			k := rand.Intn(int(array.Count()))

			values[k] = v

			err := array.Set(uint64(k), v)
			require.NoError(t, err)

		case InsertAction:
			k := rand.Intn(int(array.Count() + 1))

			if k == int(array.Count()) {
				values = append(values, v)
			} else {
				values = append(values, nil)
				copy(values[k+1:], values[k:])
				values[k] = v
			}

			err := array.Insert(uint64(k), v)
			require.NoError(t, err)

		case RemoveAction:
			if array.Count() > 0 {
				k := rand.Intn(int(array.Count()))

				v, err := array.Remove(uint64(k))
				require.NoError(t, err)

				require.Equal(t, values[k], v)

				copy(values[k:], values[k+1:])
				values = values[:len(values)-1]
			}
		}

		require.Equal(t, array.Count(), uint64(len(values)))

		verified, err := array.valid()
		if !verified {
			array.Print()
		}
		require.NoError(t, err)
		require.True(t, verified)
	}

	for k, v := range values {
		e, err := array.Get(uint64(k))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := array.Stats()
	require.Equal(t, stats.LeafNodeCount+stats.InternalNodeCount, uint64(array.storage.Count()))
}

func TestNestedArray(t *testing.T) {

	setThreshold(50)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("inline", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		nestedArrays := make([]Storable, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested := NewArray(storage)
			nested.Append(Uint64Value(i * 2))
			nested.Append(Uint64Value(i*2 + 1))
			require.True(t, nested.root.IsLeaf())

			nestedArrays[i] = nested.root
		}

		array := NewArray(storage)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})
	t.Run("not-inline", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		nestedArrays := make([]Storable, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested := NewArray(storage)
			for i := uint64(0); i < 50; i++ {
				err := nested.Append(Uint64Value(i))
				require.NoError(t, err)
			}
			require.False(t, nested.root.IsLeaf())

			nestedArrays[i] = nested.root
		}

		array := NewArray(storage)
		for _, a := range nestedArrays {
			err := array.Append(a)
			require.NoError(t, err)
		}

		for i := uint64(0); i < arraySize; i++ {
			e, err := array.Get(i)
			require.NoError(t, err)
			require.Equal(t, nestedArrays[i], e)
		}

		verified, err := array.valid()
		require.NoError(t, err)
		require.True(t, verified)
	})

}
