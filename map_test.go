package atree

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockDigesterBuilder struct {
	mock.Mock
}

var _ DigesterBuilder = &mockDigesterBuilder{}

type mockDigester struct {
	d []Digest
}

var _ Digester = &mockDigester{}

func (h *mockDigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	args := h.Called(hashable)
	return args.Get(0).(mockDigester), nil
}

func (d mockDigester) DigestPrefix(level int) ([]Digest, error) {
	if level > len(d.d) {
		return nil, fmt.Errorf("digest level %d out of bounds", level)
	}
	return d.d[:level], nil
}

func (d mockDigester) Digest(level int) (Digest, error) {
	if level >= len(d.d) {
		return 0, fmt.Errorf("digest level %d out of bounds", level)
	}
	return d.d[level], nil
}

func (d mockDigester) Levels() int {
	return len(d.d)
}

var (
	secretkey = [16]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
	}
)

func newTestInMemoryStorage(t testing.TB) SlabStorage {

	encMode, err := cbor.EncOptions{}.EncMode()
	require.NoError(t, err)

	decMode, err := cbor.DecOptions{}.DecMode()
	require.NoError(t, err)

	storage := NewBasicSlabStorage(encMode, decMode)
	storage.DecodeStorable = decodeStorable

	return storage
}

// TODO: use newTestPersistentStorage after serialization is implemented.
func TestMapSetAndGet(t *testing.T) {

	t.Run("unique keys", func(t *testing.T) {

		const mapSize = 64 * 1024

		const typeInfo = "map[String]Uint64"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		storage := newTestInMemoryStorage(t)

		uniqueKeys := make(map[string]bool, mapSize)
		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if !uniqueKeys[s] {
					uniqueKeys[s] = true

					k := NewStringValue(s)
					uniqueKeyValues[k] = Uint64Value(i)
					break
				}
			}
		}

		m, err := NewMap(storage, address, newBasicDigesterBuilder(secretkey), typeInfo)
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
			strv := k.(StringValue)
			require.NotNil(t, strv)

			e, err := m.Get(NewStringValue(strv.str))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		require.Equal(t, typeInfo, m.Type())
		require.Equal(t, uint64(len(uniqueKeyValues)), m.Count())

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	})

	t.Run("replicate keys", func(t *testing.T) {

		const mapSize = 64 * 1024

		const typeInfo = "map[String]Uint64"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		storage := newTestInMemoryStorage(t)

		uniqueKeys := make(map[string]bool, mapSize)
		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if !uniqueKeys[s] {
					uniqueKeys[s] = true

					k := NewStringValue(s)
					uniqueKeyValues[k] = Uint64Value(i)
					break
				}
			}
		}

		m, err := NewMap(storage, address, newBasicDigesterBuilder(secretkey), typeInfo)
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
			strv := k.(StringValue)
			require.NotNil(t, strv)

			e, err := m.Get(NewStringValue(strv.str))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		require.Equal(t, typeInfo, m.Type())
		require.Equal(t, uint64(len(uniqueKeyValues)), m.Count())

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	})

	// Test random string with random length as key and random uint as value
	t.Run("random key and value", func(t *testing.T) {

		const mapSize = 64 * 1024
		const maxKeyLength = 224

		const typeInfo = "map[String]AnyType"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		uniqueKeys := make(map[string]bool, mapSize)
		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				slen := rand.Intn(maxKeyLength + 1)
				s := randStr(slen)

				if !uniqueKeys[s] {
					uniqueKeys[s] = true

					k := NewStringValue(s)
					uniqueKeyValues[k] = RandomValue()
					break
				}
			}
		}

		storage := newTestInMemoryStorage(t)

		m, err := NewMap(storage, address, newBasicDigesterBuilder(secretkey), typeInfo)
		require.NoError(t, err)

		for k, v := range uniqueKeyValues {
			err := m.Set(k, v)
			require.NoError(t, err)
		}

		verified, err := m.valid()
		require.NoError(t, err)
		require.True(t, verified)

		for k, v := range uniqueKeyValues {
			strv := k.(StringValue)
			require.NotNil(t, strv)

			e, err := m.Get(NewStringValue(strv.str))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		require.Equal(t, typeInfo, m.Type())
		require.Equal(t, uint64(len(uniqueKeyValues)), m.Count())

		stats, _ := m.Stats()
		require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	})
}

func TestMapHas(t *testing.T) {

	const mapSize = 64 * 1024

	const typeInfo = "map[String]Uint64"

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	storage := newTestInMemoryStorage(t)

	uniqueKeys := make(map[string]bool, mapSize*2)
	var keysToInsert []string
	var keysToNotInsert []string
	for i := uint64(0); i < mapSize*2; i++ {
		for {
			s := randStr(16)
			if !uniqueKeys[s] {
				uniqueKeys[s] = true

				if i%2 == 0 {
					keysToInsert = append(keysToInsert, s)
				} else {
					keysToNotInsert = append(keysToNotInsert, s)
				}

				break
			}
		}
	}

	m, err := NewMap(storage, address, newBasicDigesterBuilder(secretkey), typeInfo)
	require.NoError(t, err)

	for i, k := range keysToInsert {
		err := m.Set(NewStringValue(k), Uint64Value(i))
		require.NoError(t, err)
	}

	verified, err := m.valid()
	if !verified {
		m.Print()
	}
	require.NoError(t, err)
	require.True(t, verified)

	for _, k := range keysToInsert {
		exist, err := m.Has(NewStringValue(k))
		require.NoError(t, err)
		require.Equal(t, true, exist)
	}

	for _, k := range keysToNotInsert {
		exist, err := m.Has(NewStringValue(k))
		require.NoError(t, err)
		require.Equal(t, false, exist)
	}

	require.Equal(t, typeInfo, m.Type())
	require.Equal(t, uint64(mapSize), m.Count())
}

func TestMapRemove(t *testing.T) {

	SetThreshold(512)
	defer func() {
		SetThreshold(1024)
	}()

	t.Run("small key and value", func(t *testing.T) {

		const mapSize = 2 * 1024

		const keyStringMaxSize = 16

		const valueStringMaxSize = 16

		const typeInfo = "map[String]Uint64"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		storage := newTestInMemoryStorage(t)

		uniqueKeys := make(map[string]bool, mapSize)
		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(keyStringMaxSize)
				if !uniqueKeys[s] {
					uniqueKeys[s] = true

					k := NewStringValue(s)
					uniqueKeyValues[k] = NewStringValue(randStr(valueStringMaxSize))
					break
				}
			}
		}

		m, err := NewMap(storage, address, newBasicDigesterBuilder(secretkey), typeInfo)
		require.NoError(t, err)

		// Insert elements
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

		// Get elements
		for k, v := range uniqueKeyValues {
			strv := k.(StringValue)
			require.NotNil(t, strv)

			e, err := m.Get(NewStringValue(strv.str))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		count := len(uniqueKeyValues)

		// Remove all elements
		for k, v := range uniqueKeyValues {
			strv := k.(StringValue)
			require.NotNil(t, strv)

			removedKey, removedValue, err := m.Remove(NewStringValue(strv.str))
			require.NoError(t, err)
			require.Equal(t, k, removedKey)
			require.Equal(t, v, removedValue)

			removedKey, removedValue, err = m.Remove(NewStringValue(strv.str))
			require.Error(t, err)
			require.Nil(t, removedKey)
			require.Nil(t, removedValue)

			count--

			require.Equal(t, uint64(count), m.Count())

			require.Equal(t, typeInfo, m.Type())
		}

		stats, _ := m.Stats()
		require.Equal(t, uint64(1), stats.DataSlabCount)
		require.Equal(t, uint64(0), stats.MetaDataSlabCount)
		require.Equal(t, uint64(0), stats.CollisionDataSlabCount)

		require.Equal(t, int(1), m.storage.Count())
	})

	t.Run("large key and value", func(t *testing.T) {
		const mapSize = 2 * 1024

		const keyStringMaxSize = 512

		const valueStringMaxSize = 512

		const typeInfo = "map[String]Uint64"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		storage := newTestInMemoryStorage(t)

		uniqueKeys := make(map[string]bool, mapSize)
		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(keyStringMaxSize)
				if !uniqueKeys[s] {
					uniqueKeys[s] = true

					k := NewStringValue(s)
					uniqueKeyValues[k] = NewStringValue(randStr(valueStringMaxSize))
					break
				}
			}
		}

		m, err := NewMap(storage, address, newBasicDigesterBuilder(secretkey), typeInfo)
		require.NoError(t, err)

		// Insert elements
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

		// Get elements
		for k, v := range uniqueKeyValues {
			strv := k.(StringValue)
			require.NotNil(t, strv)

			e, err := m.Get(NewStringValue(strv.str))
			require.NoError(t, err)
			require.Equal(t, v, e)
		}

		count := len(uniqueKeyValues)

		// Remove all elements
		for k, v := range uniqueKeyValues {
			strv := k.(StringValue)
			require.NotNil(t, strv)

			removedKey, removedValue, err := m.Remove(NewStringValue(strv.str))
			require.NoError(t, err)
			require.Equal(t, k, removedKey)
			require.Equal(t, v, removedValue)

			removedKey, removedValue, err = m.Remove(NewStringValue(strv.str))
			require.Error(t, err)
			require.Nil(t, removedKey)
			require.Nil(t, removedValue)

			count--

			require.Equal(t, uint64(count), m.Count())

			require.Equal(t, typeInfo, m.Type())
		}

		stats, _ := m.Stats()
		require.Equal(t, uint64(1), stats.DataSlabCount)
		require.Equal(t, uint64(0), stats.MetaDataSlabCount)
		require.Equal(t, uint64(0), stats.CollisionDataSlabCount)
	})
}

func TestMapIterate(t *testing.T) {
	t.Run("no collision", func(t *testing.T) {
		const mapSize = 64 * 1024

		const typeInfo = "map[String]Uint64"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		storage := newTestInMemoryStorage(t)

		digesterBuilder := newBasicDigesterBuilder(secretkey)

		uniqueKeyValues := make(map[string]uint64, mapSize)

		sortedKeys := make([]StringValue, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if _, exist := uniqueKeyValues[s]; !exist {
					sortedKeys[i] = NewStringValue(s)
					uniqueKeyValues[s] = i
					break
				}
			}
		}

		// Sort keys by hashed value
		sort.SliceStable(sortedKeys, func(i, j int) bool {
			d1, err := digesterBuilder.Digest(sortedKeys[i])
			require.NoError(t, err)

			digest1, err := d1.DigestPrefix(d1.Levels())
			require.NoError(t, err)

			d2, err := digesterBuilder.Digest(sortedKeys[j])
			require.NoError(t, err)

			digest2, err := d2.DigestPrefix(d2.Levels())
			require.NoError(t, err)

			for z := 0; z < len(digest1); z++ {
				if digest1[z] != digest2[z] {
					return digest1[z] < digest2[z] // sort by hkey
				}
			}
			return i < j // sort by insertion order with hash collision
		})

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
		require.NoError(t, err)

		for k, v := range uniqueKeyValues {
			err := m.Set(NewStringValue(k), Uint64Value(v))
			require.NoError(t, err)
		}

		i := uint64(0)
		err = m.Iterate(func(k Value, v Value) (resume bool, err error) {
			ks, ok := k.(StringValue)
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

		require.Equal(t, typeInfo, m.Type())
	})

	t.Run("collision", func(t *testing.T) {
		const mapSize = 1024

		const typeInfo = "map[String]String"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		digesterBuilder := &mockDigesterBuilder{}

		storage := newTestInMemoryStorage(t)

		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)

		uniqueKeys := make(map[string]bool, mapSize)

		sortedKeys := make([]ComparableValue, mapSize)

		keys := make([]ComparableValue, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)

				if !uniqueKeys[s] {
					uniqueKeys[s] = true

					k := NewStringValue(s)
					v := NewStringValue(randStr(16))

					sortedKeys[i] = k
					keys[i] = k
					uniqueKeyValues[k] = v

					digests := []Digest{
						Digest(rand.Intn(256)),
						Digest(rand.Intn(256)),
						Digest(rand.Intn(256)),
						Digest(rand.Intn(256)),
					}

					digesterBuilder.On("Digest", k).Return(mockDigester{digests})
					break
				}
			}
		}

		// Sort keys by hashed value
		sort.SliceStable(sortedKeys, func(i, j int) bool {

			d1, err := digesterBuilder.Digest(sortedKeys[i])
			require.NoError(t, err)

			digest1, err := d1.DigestPrefix(d1.Levels())
			require.NoError(t, err)

			d2, err := digesterBuilder.Digest(sortedKeys[j])
			require.NoError(t, err)

			digest2, err := d2.DigestPrefix(d2.Levels())
			require.NoError(t, err)

			for z := 0; z < len(digest1); z++ {
				if digest1[z] != digest2[z] {
					return digest1[z] < digest2[z] // sort by hkey
				}
			}
			return i < j // sort by insertion order with hash collision
		})

		m, err := NewMap(storage, address, digesterBuilder, typeInfo)
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

			mk, ok := k.(ComparableValue)
			require.True(t, ok)
			require.Equal(t, uniqueKeyValues[mk], v)

			i++

			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, i, uint64(mapSize))

		require.Equal(t, typeInfo, m.Type())
	})
}

func testMapDeterministicHashCollision(t *testing.T, maxHashLevel int) {

	const mapSize = 2 * 1024

	// mockDigestCount is the number of unique set of digests.
	// Each set has maxHashLevel of digest.
	const mockDigestCount = 8

	const typeInfo = "map[String]String"

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	digesterBuilder := &mockDigesterBuilder{}

	// Generate mockDigestCount*maxHashLevel number of unique digest
	digests := make([]Digest, 0, mockDigestCount*maxHashLevel)
	uniqueDigest := make(map[Digest]bool)
	for len(uniqueDigest) < mockDigestCount*maxHashLevel {
		d := Digest(uint64(rand.Intn(256)))
		if !uniqueDigest[d] {
			uniqueDigest[d] = true
			digests = append(digests, d)
		}
	}

	storage := newTestInMemoryStorage(t)

	uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
	uniqueKeys := make(map[string]bool)
	for i := uint64(0); i < mapSize; i++ {
		for {
			s := randStr(16)
			if !uniqueKeys[s] {
				uniqueKeys[s] = true

				k := NewStringValue(s)
				uniqueKeyValues[k] = NewStringValue(randStr(16))

				index := (i % mockDigestCount)
				startIndex := int(index) * maxHashLevel
				endIndex := int(index)*maxHashLevel + maxHashLevel

				digests := digests[startIndex:endIndex]

				digesterBuilder.On("Digest", k).Return(mockDigester{digests})

				break
			}
		}
	}

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	for k, v := range uniqueKeyValues {
		err := m.Set(k, v)
		require.NoError(t, err)
	}

	verified, err := m.valid()
	require.NoError(t, err)
	require.True(t, verified)

	for k, v := range uniqueKeyValues {
		strv := k.(StringValue)
		require.NotNil(t, strv)

		e, err := m.Get(NewStringValue(strv.str))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	require.Equal(t, typeInfo, m.Type())

	stats, _ := m.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	require.Equal(t, uint64(mockDigestCount), stats.CollisionDataSlabCount)

	for k, v := range uniqueKeyValues {
		strv := k.(StringValue)
		require.NotNil(t, strv)

		removedKey, removedValue, err := m.Remove(NewStringValue(strv.str))
		require.NoError(t, err)
		require.Equal(t, k, removedKey)
		require.Equal(t, v, removedValue)
	}

	require.Equal(t, uint64(0), m.Count())

	require.Equal(t, typeInfo, m.Type())

	require.Equal(t, uint64(1), uint64(m.storage.Count()))

	stats, _ = m.Stats()
	require.Equal(t, uint64(1), stats.DataSlabCount)
	require.Equal(t, uint64(0), stats.MetaDataSlabCount)
	require.Equal(t, uint64(0), stats.CollisionDataSlabCount)
}

func testMapRandomHashCollision(t *testing.T, maxHashLevel int) {

	const mapSize = 2 * 1024

	const typeInfo = "map[String]String"

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	digesterBuilder := &mockDigesterBuilder{}

	storage := newTestInMemoryStorage(t)

	uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
	uniqueKeys := make(map[string]bool)
	for i := uint64(0); i < mapSize; i++ {
		for {
			s := randStr(rand.Intn(16))

			if !uniqueKeys[s] {
				uniqueKeys[s] = true

				k := NewStringValue(s)
				uniqueKeyValues[k] = NewStringValue(randStr(16))

				var digests []Digest
				for i := 0; i < maxHashLevel; i++ {
					digests = append(digests, Digest(rand.Intn(256)))
				}

				digesterBuilder.On("Digest", k).Return(mockDigester{digests})

				break
			}
		}
	}

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	for k, v := range uniqueKeyValues {
		err := m.Set(k, v)
		require.NoError(t, err)
	}

	verified, err := m.valid()
	require.NoError(t, err)
	require.True(t, verified)

	for k, v := range uniqueKeyValues {
		strv := k.(StringValue)
		require.NotNil(t, strv)

		e, err := m.Get(NewStringValue(strv.str))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	require.Equal(t, typeInfo, m.Type())

	stats, _ := m.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))

	for k, v := range uniqueKeyValues {
		strv := k.(StringValue)
		require.NotNil(t, strv)

		removedKey, removedValue, err := m.Remove(NewStringValue(strv.str))
		require.NoError(t, err)
		require.Equal(t, k, removedKey)
		require.Equal(t, v, removedValue)
	}

	require.Equal(t, uint64(0), m.Count())

	require.Equal(t, typeInfo, m.Type())

	require.Equal(t, uint64(1), uint64(m.storage.Count()))

	stats, _ = m.Stats()
	require.Equal(t, uint64(1), stats.DataSlabCount)
	require.Equal(t, uint64(0), stats.MetaDataSlabCount)
	require.Equal(t, uint64(0), stats.CollisionDataSlabCount)
}

func TestMapHashCollision(t *testing.T) {

	SetThreshold(512)
	defer func() {
		SetThreshold(1024)
	}()

	const maxHashLevel = 4

	for hashLevel := 1; hashLevel <= maxHashLevel; hashLevel++ {
		name := fmt.Sprintf("deterministic max hash level %d", hashLevel)
		t.Run(name, func(t *testing.T) {
			testMapDeterministicHashCollision(t, hashLevel)
		})
	}

	for hashLevel := 1; hashLevel <= maxHashLevel; hashLevel++ {
		name := fmt.Sprintf("random max hash level %d", hashLevel)
		t.Run(name, func(t *testing.T) {
			testMapRandomHashCollision(t, hashLevel)
		})
	}
}

func TestMapLargeElement(t *testing.T) {

	SetThreshold(512)
	defer func() {
		SetThreshold(1024)
	}()

	const typeInfo = "map[string]string"

	const mapSize = 2 * 1024

	const keySize = 512
	const valueSize = 512

	strs := make(map[string]string, mapSize)
	for i := uint64(0); i < mapSize; i++ {
		k := randStr(keySize)
		v := randStr(valueSize)
		strs[k] = v
	}

	storage := newTestInMemoryStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := NewMap(storage, address, newBasicDigesterBuilder(secretkey), typeInfo)
	require.NoError(t, err)

	for k, v := range strs {
		err := m.Set(NewStringValue(k), NewStringValue(v))
		require.NoError(t, err)
	}

	for k, v := range strs {
		e, err := m.Get(NewStringValue(k))
		require.NoError(t, err)

		sv, ok := e.(StringValue)
		require.True(t, ok)
		require.Equal(t, v, sv.str)
	}

	require.Equal(t, typeInfo, m.Type())
	require.Equal(t, uint64(mapSize), m.Count())

	verified, err := m.valid()
	require.NoError(t, err)
	require.True(t, verified)

	stats, _ := m.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+mapSize*2, uint64(m.storage.Count()))
}

func TestMapRandomSetRemoveMixedTypes(t *testing.T) {

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

	SetThreshold(256)
	defer func() {
		SetThreshold(1024)
	}()

	const actionCount = 2 * 1024

	const digestMaxValue = 256

	const digestMaxLevels = 4

	const stringMaxSize = 512

	const typeInfo = "[AnyType]"

	storage := newTestInMemoryStorage(t)

	address := Address{1, 2, 3, 4, 5, 6, 7, 8}

	digesterBuilder := &mockDigesterBuilder{}

	m, err := NewMap(storage, address, digesterBuilder, typeInfo)
	require.NoError(t, err)

	keyValues := make(map[ComparableValue]Value)
	var keys []ComparableValue

	for i := uint64(0); i < actionCount; i++ {

		switch rand.Intn(MaxAction) {

		case SetAction:

			var k ComparableValue

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

			keyValues[k] = v

			var digests []Digest
			for i := 0; i < digestMaxLevels; i++ {
				digests = append(digests, Digest(rand.Intn(digestMaxValue)))
			}

			digesterBuilder.On("Digest", k).Return(mockDigester{digests})

			oldCount := m.Count()

			err := m.Set(k, v)
			require.NoError(t, err)

			newCount := m.Count()

			if newCount > oldCount {
				keys = append(keys, k)
			}

		case RemoveAction:
			if len(keys) > 0 {
				ki := rand.Intn(len(keys))
				k := keys[ki]

				removedKey, removedValue, err := m.Remove(k)
				require.NoError(t, err)
				require.Equal(t, k, removedKey)
				require.Equal(t, keyValues[k], removedValue)

				delete(keyValues, k)
				copy(keys[ki:], keys[ki+1:])
				keys = keys[:len(keys)-1]
			}
		}

		require.Equal(t, m.Count(), uint64(len(keys)))
		require.Equal(t, typeInfo, m.Type())
	}

	for k, v := range keyValues {
		e, err := m.Get(k)
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	verified, err := m.valid()
	require.NoError(t, err)
	require.True(t, verified)
}
