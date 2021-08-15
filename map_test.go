package atree

import (
	"fmt"
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

		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				k := NewStringValue(s)
				if _, kExist := uniqueKeyValues[k]; !kExist {
					uniqueKeyValues[k] = Uint64Value(i)
					break
				}
			}
		}

		m, err := NewMap(storage, address, NewBasicDigesterBuilder(secretkey), typeInfo)
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

		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)
		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				k := NewStringValue(s)
				if _, kExist := uniqueKeyValues[k]; !kExist {
					uniqueKeyValues[k] = Uint64Value(i)
					break
				}
			}
		}

		m, err := NewMap(storage, address, NewBasicDigesterBuilder(secretkey), typeInfo)
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

		storage := newTestInMemoryStorage(t)

		m, err := NewMap(storage, address, NewBasicDigesterBuilder(secretkey), typeInfo)
		require.NoError(t, err)

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

	// Only first half of unique keys are inserted into the map.
	uniqueKeyValues := make(map[ComparableValue]Uint64Value, mapSize*2)
	uniqueKeys := make([]ComparableValue, mapSize*2)
	for i := uint64(0); i < mapSize*2; i++ {
		for {
			s := randStr(16)
			if _, kExist := uniqueKeyValues[NewStringValue(s)]; !kExist {
				k := NewStringValue(s)
				uniqueKeyValues[k] = Uint64Value(i)
				uniqueKeys[i] = k
				break
			}
		}
	}

	m, err := NewMap(storage, address, NewBasicDigesterBuilder(secretkey), typeInfo)
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

	require.Equal(t, typeInfo, m.Type())
	require.Equal(t, uint64(mapSize), m.Count())
}

func TestMapIterate(t *testing.T) {
	t.Run("no collision", func(t *testing.T) {
		const mapSize = 64 * 1024

		const typeInfo = "map[String]Uint64"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		storage := newTestInMemoryStorage(t)

		digesterBuilder := NewBasicDigesterBuilder(secretkey)

		uniqueKeyValues := make(map[string]uint64, mapSize)

		sortedKeys := make([]*StringValue, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				if _, kExist := uniqueKeyValues[s]; !kExist {
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

		require.Equal(t, typeInfo, m.Type())
	})

	t.Run("collision", func(t *testing.T) {
		const mapSize = 1024

		const typeInfo = "map[String]String"

		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		digesterBuilder := &mockDigesterBuilder{}

		storage := newTestInMemoryStorage(t)

		uniqueKeyValues := make(map[ComparableValue]Value, mapSize)

		sortedKeys := make([]ComparableValue, mapSize)

		keys := make([]ComparableValue, mapSize)

		for i := uint64(0); i < mapSize; i++ {
			for {
				s := randStr(16)
				k := NewStringValue(s)

				if _, kExist := uniqueKeyValues[k]; !kExist {
					v := NewStringValue(randStr(16))

					sortedKeys[i] = k
					keys[i] = k
					uniqueKeyValues[k] = v

					digest1 := Digest(rand.Intn(256))
					digest2 := Digest(rand.Intn(256))

					digesterBuilder.On("Digest", k).Return(mockDigester{[]Digest{digest1, digest2}})
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
		strv := k.(*StringValue)
		require.NotNil(t, strv)

		e, err := m.Get(NewStringValue(strv.str))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	require.Equal(t, typeInfo, m.Type())

	stats, _ := m.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
	require.Equal(t, uint64(mockDigestCount), stats.CollisionDataSlabCount)
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
		strv := k.(*StringValue)
		require.NotNil(t, strv)

		e, err := m.Get(NewStringValue(strv.str))
		require.NoError(t, err)
		require.Equal(t, v, e)
	}

	require.Equal(t, typeInfo, m.Type())

	stats, _ := m.Stats()
	require.Equal(t, stats.DataSlabCount+stats.MetaDataSlabCount+stats.CollisionDataSlabCount, uint64(m.storage.Count()))
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

	m, err := NewMap(storage, address, NewBasicDigesterBuilder(secretkey), typeInfo)
	require.NoError(t, err)

	for k, v := range strs {
		err := m.Set(NewStringValue(k), NewStringValue(v))
		require.NoError(t, err)
	}

	for k, v := range strs {
		e, err := m.Get(NewStringValue(k))
		require.NoError(t, err)

		sv, ok := e.(*StringValue)
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
