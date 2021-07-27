package atree

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockHasher struct {
	mock.Mock
}

func (h *mockHasher) Hash(hashable Hashable) ([]uint64, error) {
	args := h.Called(hashable)
	return args.Get(0).([]uint64), nil
}

func (h *mockHasher) DigestSize() int {
	args := h.Called()
	return args.Get(0).(int)
}

var (
	secretkey = [16]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
	}

	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func randStr(n int) string {
	r := make([]rune, n)
	for i := range r {
		r[i] = letters[rand.Intn(len(letters))]
	}
	return string(r)
}

func TestMapSetAndGet(t *testing.T) {

	t.Run("unique keys", func(t *testing.T) {

		const mapSize = 64 * 1024

		storage := NewBasicSlabStorage()

		uniqueKeyValues := make(map[MapKey]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				k := NewStringValue(storage, s)
				if _, kExist := uniqueKeyValues[k]; !kExist {
					uniqueKeyValues[k] = Uint64Value(i)
					break
				}
			}
		}

		m, err := NewMap(storage, &sipHash128{secretkey})
		require.NoError(t, err)

		for k, v := range uniqueKeyValues {
			err := m.Set(k, v)
			require.NoError(t, err)
		}

		verified, err := m.valid()
		if !verified {
			m.Print()
		}
		require.NoError(t, err)
		require.True(t, verified)

		for k, v := range uniqueKeyValues {
			e, err := m.Get(k)
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	})

	t.Run("replicate keys", func(t *testing.T) {

		const mapSize = 64 * 1024

		storage := NewBasicSlabStorage()

		uniqueKeyValues := make(map[MapKey]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				k := NewStringValue(storage, s)
				if _, kExist := uniqueKeyValues[k]; !kExist {
					uniqueKeyValues[k] = Uint64Value(i)
					break
				}
			}
		}

		m, err := NewMap(storage, &sipHash128{secretkey})
		require.NoError(t, err)

		for k, v := range uniqueKeyValues {
			err := m.Set(k, v)
			require.NoError(t, err)
		}

		verified, err := m.valid()
		require.NoError(t, err)
		require.True(t, verified)

		// Overwrite previously inserted values
		for k := range uniqueKeyValues {
			v, _ := uniqueKeyValues[k].(Uint64Value)
			v = Uint64Value(uint64(v) + mapSize)
			uniqueKeyValues[k] = v

			err := m.Set(k, v)
			require.NoError(t, err)
		}

		for k, v := range uniqueKeyValues {
			e, err := m.Get(k)
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	})

	// Test random string with random length as key and random uint as value
	t.Run("random key and value", func(t *testing.T) {

		const mapSize = 64 * 1024
		const maxKeyLength = 224

		uniqueKeyValues := make(map[string]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				slen := rand.Intn(maxKeyLength + 1)
				s := randStr(slen)

				if _, kExist := uniqueKeyValues[s]; !kExist {
					uniqueKeyValues[s] = RandomValue()
					break
				}
			}
		}

		storage := NewBasicSlabStorage()

		m, err := NewMap(storage, &sipHash128{secretkey})
		require.NoError(t, err)

		for k, v := range uniqueKeyValues {
			err := m.Set(NewStringValue(storage, k), v)
			require.NoError(t, err)
		}

		verified, err := m.valid()
		require.NoError(t, err)
		require.True(t, verified)

		for k, v := range uniqueKeyValues {
			e, err := m.Get(NewStringValue(storage, k))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	})
}

func TestMapHas(t *testing.T) {

	const mapSize = 64 * 1024

	storage := NewBasicSlabStorage()

	// Only first half of unique keys are inserted into the map.
	uniqueKeyValues := make(map[MapKey]Uint64Value, mapSize*2)
	uniqueKeys := make([]MapKey, mapSize*2)
	for i := uint64(0); i < mapSize*2; i++ {
		for {
			s := randStr(16)
			if _, kExist := uniqueKeyValues[NewStringValue(storage, s)]; !kExist {
				k := NewStringValue(storage, s)
				uniqueKeyValues[k] = Uint64Value(i)
				uniqueKeys[i] = k
				break
			}
		}
	}

	m, err := NewMap(storage, &sipHash128{secretkey})
	require.NoError(t, err)

	for _, k := range uniqueKeys[:mapSize] {
		err := m.Set(k, uniqueKeyValues[k])
		require.NoError(t, err)
	}

	verified, err := m.valid()
	if !verified {
		m.Print()
	}
	require.NoError(t, err)
	require.True(t, verified)

	for i, k := range uniqueKeys {
		exist, err := m.Has(k)
		require.NoError(t, err)

		if i < mapSize {
			require.Equal(t, true, exist)
		} else {
			require.Equal(t, false, exist)
		}
	}
}

func TestMapIterate(t *testing.T) {
	t.Run("no collision", func(t *testing.T) {
		const mapSize = 64 * 1024

		storage := NewBasicSlabStorage()

		hasher := &sipHash128{secretkey}

		uniqueKeyValues := make(map[string]uint64, mapSize)

		sortedKeys := make([]*StringValue, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if _, kExist := uniqueKeyValues[s]; !kExist {
					sortedKeys[i] = NewStringValue(storage, s)
					uniqueKeyValues[s] = i
					break
				}
			}
		}

		// Sort keys by hashed value
		sort.SliceStable(sortedKeys, func(i, j int) bool {
			digest1, err := hasher.Hash(sortedKeys[i])
			require.NoError(t, err)

			digest2, err := hasher.Hash(sortedKeys[j])
			require.NoError(t, err)

			for z := 0; z < len(digest1); z++ {
				if digest1[z] != digest2[z] {
					return digest1[z] < digest2[z] // sort by hkey
				}
			}
			return i < j // sort by insertion order with hash collision
		})

		m, err := NewMap(storage, hasher)
		require.NoError(t, err)

		for k, v := range uniqueKeyValues {
			err := m.Set(NewStringValue(storage, k), Uint64Value(v))
			require.NoError(t, err)
		}

		i := uint64(0)
		err = m.Iterate(func(k Value, v Value) (resume bool, err error) {
			ks, ok := k.(*StringValue)
			require.True(t, ok)
			require.Equal(t, sortedKeys[i].String(), ks.String())

			vi, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, uniqueKeyValues[ks.String()], uint64(vi))

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))
	})

	t.Run("collision", func(t *testing.T) {
		const mapSize = 1024

		hasher := &mockHasher{}
		hasher.On("DigestSize", mock.Anything).Return(16)

		storage := NewBasicSlabStorage()

		uniqueKeyValues := make(map[MapKey]Value, mapSize)

		sortedKeys := make([]MapKey, mapSize)

		keys := make([]MapKey, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				k := NewStringValue(storage, s)

				if _, kExist := uniqueKeyValues[k]; !kExist {
					v := NewStringValue(storage, randStr(16))

					sortedKeys[i] = k
					keys[i] = k
					uniqueKeyValues[k] = v

					digest1 := rand.Intn(256)
					digest2 := rand.Intn(256)

					hasher.On("Hash", k).Return([]uint64{uint64(digest1), uint64(digest2)})
					break
				}
			}
		}

		// Sort keys by hashed value
		sort.SliceStable(sortedKeys, func(i, j int) bool {
			digest1, err := hasher.Hash(sortedKeys[i])
			require.NoError(t, err)

			digest2, err := hasher.Hash(sortedKeys[j])
			require.NoError(t, err)

			for z := 0; z < len(digest1); z++ {
				if digest1[z] != digest2[z] {
					return digest1[z] < digest2[z] // sort by hkey
				}
			}
			return i < j // sort by insertion order with hash collision
		})

		m, err := NewMap(storage, hasher)
		require.NoError(t, err)

		for _, k := range keys {
			v, ok := uniqueKeyValues[k]
			require.True(t, ok)

			err := m.Set(k, v)
			require.NoError(t, err)
		}

		i := uint64(0)
		err = m.Iterate(func(k Value, v Value) (resume bool, err error) {
			require.Equal(t, sortedKeys[i], k)

			mk, ok := k.(MapKey)
			require.True(t, ok)
			require.Equal(t, uniqueKeyValues[mk], v)

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))
	})
}

func TestMapHashCollision(t *testing.T) {
	const mapSize = 2 * 1024

	hasher := &mockHasher{}
	hasher.On("DigestSize", mock.Anything).Return(16)

	storage := NewBasicSlabStorage()

	uniqueKeyValues := make(map[MapKey]Value, mapSize)
	for i := uint64(0); i < mapSize; i++ {
		for {
			s := randStr(16)
			k := NewStringValue(storage, s)
			if _, kExist := uniqueKeyValues[k]; !kExist {
				v := NewStringValue(storage, randStr(16))
				uniqueKeyValues[k] = v

				digest1 := rand.Intn(256)
				digest2 := rand.Intn(256)

				hasher.On("Hash", k).Return([]uint64{uint64(digest1), uint64(digest2)})

				break
			}
		}
	}

	m, err := NewMap(storage, hasher)
	require.NoError(t, err)

	for k, v := range uniqueKeyValues {
		err := m.Set(k, v)
		require.NoError(t, err)
	}

	verified, err := m.valid()
	if !verified {
		m.Print()
	}
	require.NoError(t, err)
	require.True(t, verified)

	for k, v := range uniqueKeyValues {
		e, err := m.Get(k)
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	stats, _ := m.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
}
