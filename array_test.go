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

func resetStorageID() {
	storageIDCounter = 0
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
}

func TestInsertAndGet(t *testing.T) {
	setThreshold(60)
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
	})
}

func TestRemove(t *testing.T) {
	setThreshold(60)
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
	})
}

func TestIterate(t *testing.T) {
	setThreshold(60)
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
	setThreshold(60)
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

	setThreshold(60)
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
}

func TestInsertRandomValue(t *testing.T) {

	setThreshold(60)
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
	})

}

func TestRemoveRandomElement(t *testing.T) {

	setThreshold(60)
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
}

func TestRandomAppendSetInsertRemove(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	setThreshold(60)
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
}

func TestRandomAppendSetInsertRemoveUint8(t *testing.T) {

	const (
		AppendAction = iota
		SetAction
		InsertAction
		RemoveAction
		MaxAction
	)

	setThreshold(60)
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

	setThreshold(60)
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
}

func TestNestedArray(t *testing.T) {

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	t.Run("inline", func(t *testing.T) {

		const arraySize = 256 * 256

		storage := NewBasicSlabStorage()

		nestedArrays := make([]Storable, arraySize)
		for i := uint64(0); i < arraySize; i++ {
			nested := NewArray(storage)

			err := nested.Append(Uint64Value(i * 2))
			require.NoError(t, err)

			err = nested.Append(Uint64Value(i*2 + 1))
			require.NoError(t, err)
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

func TestEncode(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		storage := NewBasicSlabStorage()

		array := NewArray(storage)

		b, err := array.Encode()
		require.NoError(t, err)
		require.Equal(t, 0, len(b))
	})

	t.Run("data root", func(t *testing.T) {
		const arraySize = 5

		// Reset storage id for consistent encoded data
		resetStorageID()

		storage := NewBasicSlabStorage()
		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			array.Append(Uint64Value(i))
		}

		require.Equal(t, uint64(arraySize), array.Count())

		expected := []byte{
			// root
			// node flag (leaf node | array data)
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array size (fixed size 3 byte)
			0x99, 0x00, 0x05,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x04,
		}

		b, err := array.Encode()
		require.NoError(t, err)
		require.Equal(t, expected, b)
	})

	t.Run("metadata root", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		const arraySize = 20

		// Reset storage id for consistent encoded data
		resetStorageID()

		storage := NewBasicSlabStorage()
		array := NewArray(storage)

		for i := uint64(0); i < arraySize; i++ {
			err := array.Append(Uint64Value(i))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(arraySize), array.Count())

		/*
			level 1, meta ({id:1 size:37 count:20}) headers: [{id:2 size:50 count:10} {id:3 size:50 count:10} ]
			level 2, leaf ({id:2 size:50 count:10}): [0 1 2 ... 7 8 9]
			level 2, leaf ({id:3 size:50 count:10}): [10 11 12 ... 17 18 19]
		*/
		expected := []byte{
			// root (meta data slab)
			// node flag
			0x01,
			// child header count
			0x00, 0x00, 0x00, 0x02,
			// child header 1 (storage id, count, size)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,
			// child header 2
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,

			// data slab
			// node flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x08, 0xd8, 0xa4, 0x09,

			// data slab
			// node flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x0a, 0xd8, 0xa4, 0x0b, 0xd8, 0xa4, 0x0c, 0xd8, 0xa4, 0x0d, 0xd8, 0xa4, 0x0e, 0xd8, 0xa4, 0x0f, 0xd8, 0xa4, 0x10, 0xd8, 0xa4, 0x11, 0xd8, 0xa4, 0x12, 0xd8, 0xa4, 0x13,
		}
		b, err := array.Encode()
		require.NoError(t, err)
		require.Equal(t, expected, b)
	})
}

func TestDecodeEncode(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		storage := NewBasicSlabStorage()

		decoded, err := NewArrayFromData(storage, StorageIDUndefined, []byte{})
		require.NoError(t, err)
		require.Equal(t, nil, decoded.root)
		require.Equal(t, StorageIDUndefined, decoded.dataSlabStorageID)

		// Encode decoded array and compare encoded bytes
		encodedData, err := decoded.Encode()
		require.NoError(t, err)
		require.Equal(t, []byte(nil), encodedData)
	})

	t.Run("data root", func(t *testing.T) {
		storage := NewBasicSlabStorage()

		data := []byte{
			// root
			// node flag (leaf node | array data)
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x05,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x04,
		}
		decoded, err := NewArrayFromData(storage, StorageID(1), data)
		require.NoError(t, err)
		require.Equal(t, uint64(5), decoded.Count())
		require.NotNil(t, decoded.root)
		require.True(t, decoded.root.IsLeaf())

		expected := []Storable{Uint64Value(0), Uint64Value(1), Uint64Value(2), Uint64Value(3), Uint64Value(4)}

		leaf := decoded.root.(*ArrayDataSlab)
		require.Equal(t, expected, leaf.elements)

		// Encode decoded array and compare encoded bytes
		encodedData, err := decoded.Encode()
		require.NoError(t, err)
		require.Equal(t, data, encodedData)
	})

	t.Run("metadata root", func(t *testing.T) {
		setThreshold(50)
		defer func() {
			setThreshold(1024)
		}()

		storage := NewBasicSlabStorage()

		// level 1, meta ({id:1 size:37 count:20}) headers: [{id:2 size:50 count:10} {id:3 size:50 count:10} ]
		// level 2, leaf ({id:2 size:50 count:10}): [0 1 2 ... 7 8 9]
		// level 2, leaf ({id:3 size:50 count:10}): [10 11 12 ... 17 18 19]

		data := []byte{
			// root
			// node flag (internal)
			0x01,
			// child header count
			0x00, 0x00, 0x00, 0x02,
			// child header 1 (storage id, count, size)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,
			// child header 2
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x32,

			// leaf 1
			// node flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x00, 0xd8, 0xa4, 0x01, 0xd8, 0xa4, 0x02, 0xd8, 0xa4, 0x03, 0xd8, 0xa4, 0x04, 0xd8, 0xa4, 0x05, 0xd8, 0xa4, 0x06, 0xd8, 0xa4, 0x07, 0xd8, 0xa4, 0x08, 0xd8, 0xa4, 0x09,

			// leaf 2
			// node flag
			0x06,
			// prev storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
			// next storage id
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// CBOR encoded array head (fixed size 3 byte)
			0x99, 0x00, 0x0a,
			// CBOR encoded array elements
			0xd8, 0xa4, 0x0a, 0xd8, 0xa4, 0x0b, 0xd8, 0xa4, 0x0c, 0xd8, 0xa4, 0x0d, 0xd8, 0xa4, 0x0e, 0xd8, 0xa4, 0x0f, 0xd8, 0xa4, 0x10, 0xd8, 0xa4, 0x11, 0xd8, 0xa4, 0x12, 0xd8, 0xa4, 0x13,
		}

		decoded, err := NewArrayFromData(storage, StorageID(1), data)
		require.NoError(t, err)
		require.Equal(t, uint64(20), decoded.Count())
		require.NotNil(t, decoded.root)
		require.False(t, decoded.root.IsLeaf())

		// Encode decoded array and compare encoded bytes
		encodedData, err := decoded.Encode()
		require.NoError(t, err)
		require.Equal(t, data, encodedData)
	})
}

func TestDecodeEncodeRandomData(t *testing.T) {
	const (
		Uint8Type = iota
		Uint16Type
		Uint32Type
		Uint64Type
		MaxType
	)

	setThreshold(60)
	defer func() {
		setThreshold(1024)
	}()

	const arraySize = 256 * 256

	array := NewArray(NewBasicSlabStorage())

	for i := uint64(0); i < arraySize; i++ {

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

		err := array.Append(v)
		require.NoError(t, err)
	}

	verified, err := array.valid()
	require.NoError(t, err)
	require.True(t, verified)

	// Encode array with random data of mixed types
	b1, err := array.Encode()
	require.NoError(t, err)

	// Decode data to new array
	decodedArray, err := NewArrayFromData(NewBasicSlabStorage(), array.root.Header().id, b1)
	require.NoError(t, err)

	// Encode decoded array
	b2, err := decodedArray.Encode()
	require.NoError(t, err)
	require.Equal(t, b1, b2)
}
