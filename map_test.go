package main

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/aead/siphash"
	"github.com/stretchr/testify/require"
)

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

		uniqueKeyValues := make(map[Key]Uint64Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if _, kExist := uniqueKeyValues[NewStringValue(s)]; !kExist {
					uniqueKeyValues[NewStringValue(s)] = Uint64Value(i)
					break
				}
			}
		}

		storage := NewBasicSlabStorage()

		m := NewMap(storage, secretkey)

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
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(m.storage.Count()))
	})

	t.Run("replicate keys", func(t *testing.T) {

		const mapSize = 64 * 1024

		uniqueKeyValues := make(map[Key]uint64, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if _, kExist := uniqueKeyValues[NewStringValue(s)]; !kExist {
					uniqueKeyValues[NewStringValue(s)] = i
					break
				}
			}
		}

		storage := NewBasicSlabStorage()

		m := NewMap(storage, secretkey)

		for k, v := range uniqueKeyValues {
			err := m.Set(k, Uint64Value(v))
			require.NoError(t, err)
		}

		verified, err := m.valid()
		require.NoError(t, err)
		require.True(t, verified)

		// Overwrite previously inserted values
		for k := range uniqueKeyValues {
			v := uniqueKeyValues[k] + mapSize
			uniqueKeyValues[k] = v

			err := m.Set(k, Uint64Value(v))
			require.NoError(t, err)
		}

		for k, v := range uniqueKeyValues {
			e, err := m.Get(k)
			require.NoError(t, err)
			require.Equal(t, Uint64Value(v), e)
		}

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(m.storage.Count()))
	})

	// Test random string with random length as key and random uint as value
	t.Run("random key and value", func(t *testing.T) {

		const mapSize = 64 * 1024
		const maxKeyLength = 224

		uniqueKeyValues := make(map[string]Storable, mapSize)
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

		m := NewMap(storage, secretkey)

		for k, v := range uniqueKeyValues {
			err := m.Set(NewStringValue(k), v)
			require.NoError(t, err)
		}

		verified, err := m.valid()
		require.NoError(t, err)
		require.True(t, verified)

		for k, v := range uniqueKeyValues {
			e, err := m.Get(NewStringValue(k))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount, uint64(m.storage.Count()))
	})
}

func TestMapIterate(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		const mapSize = 64 * 1024

		uniqueKeyValues := make(map[string]uint64, mapSize)

		sortedKeys := make([]string, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if _, kExist := uniqueKeyValues[s]; !kExist {
					sortedKeys[i] = s
					uniqueKeyValues[s] = i
					break
				}
			}
		}

		// Sort keys by hashed value
		sort.Slice(sortedKeys, func(i, j int) bool {
			ihkey := siphash.Sum64([]byte(sortedKeys[i]), &secretkey)
			jhkey := siphash.Sum64([]byte(sortedKeys[j]), &secretkey)
			return ihkey < jhkey
		})

		storage := NewBasicSlabStorage()

		m := NewMap(storage, secretkey)

		for k, v := range uniqueKeyValues {
			err := m.Set(NewStringValue(k), Uint64Value(v))
			require.NoError(t, err)
		}

		i := uint64(0)
		err := m.Iterate(func(k Key, v Storable) {
			ks, ok := k.(*StringValue)
			require.True(t, ok)
			require.Equal(t, sortedKeys[i], ks.String())

			vi, ok := v.(Uint64Value)
			require.True(t, ok)
			require.Equal(t, uniqueKeyValues[ks.String()], uint64(vi))

			i++
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))
	})
}
