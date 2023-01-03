/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2021 Dapper Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/onflow/atree"
)

const (
	maxNestedLevels    = 5
	maxNestedArraySize = 50
	maxNestedMapSize   = 50
)

const (
	uint8Type int = iota
	uint16Type
	uint32Type
	uint64Type
	smallStringType
	largeStringType
	arrayType
	mapType
	maxValueType
)

var (
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	r *rand.Rand
)

func newRand(seed int64) *rand.Rand {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	fmt.Printf("rand seed 0x%x\n", seed)
	return rand.New(rand.NewSource(seed))
}

func randStr(n int) string {
	runes := make([]rune, n)
	for i := range runes {
		runes[i] = letters[r.Intn(len(letters))]
	}
	return string(runes)
}

func generateValue(storage *atree.PersistentSlabStorage, address atree.Address, valueType int, nestedLevels int) (atree.Value, error) {
	switch valueType {
	case uint8Type:
		return Uint8Value(r.Intn(255)), nil
	case uint16Type:
		return Uint16Value(r.Intn(6535)), nil
	case uint32Type:
		return Uint32Value(r.Intn(4294967295)), nil
	case uint64Type:
		return Uint64Value(r.Intn(1844674407370955161)), nil
	case smallStringType:
		slen := r.Intn(125)
		return NewStringValue(randStr(slen)), nil
	case largeStringType:
		slen := r.Intn(125) + 1024
		return NewStringValue(randStr(slen)), nil
	case arrayType:
		length := r.Intn(maxNestedArraySize)
		return newArray(storage, address, length, nestedLevels)
	case mapType:
		length := r.Intn(maxNestedMapSize)
		return newMap(storage, address, length, nestedLevels)
	default:
		return Uint8Value(r.Intn(255)), nil
	}
}

func randomKey() (atree.Value, error) {
	t := r.Intn(largeStringType + 1)
	return generateValue(nil, atree.Address{}, t, 0)
}

func randomValue(storage *atree.PersistentSlabStorage, address atree.Address, nestedLevels int) (atree.Value, error) {
	var t int
	if nestedLevels <= 0 {
		t = r.Intn(largeStringType + 1)
	} else {
		t = r.Intn(maxValueType)
	}
	return generateValue(storage, address, t, nestedLevels)
}

func copyValue(storage *atree.PersistentSlabStorage, address atree.Address, value atree.Value) (atree.Value, error) {
	switch v := value.(type) {
	case Uint8Value:
		return Uint8Value(uint8(v)), nil
	case Uint16Value:
		return Uint16Value(uint16(v)), nil
	case Uint32Value:
		return Uint32Value(uint32(v)), nil
	case Uint64Value:
		return Uint64Value(uint64(v)), nil
	case StringValue:
		return NewStringValue(v.str), nil
	case *atree.Array:
		return copyArray(storage, address, v)
	case *atree.OrderedMap:
		return copyMap(storage, address, v)
	default:
		return nil, fmt.Errorf("failed to copy value: value type %T isn't supported", v)
	}
}

func copyArray(storage *atree.PersistentSlabStorage, address atree.Address, array *atree.Array) (*atree.Array, error) {
	iterator, err := array.Iterator()
	if err != nil {
		return nil, err
	}
	return atree.NewArrayFromBatchData(storage, address, array.Type(), func() (atree.Value, error) {
		v, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		return copyValue(storage, address, v)
	})
}

func copyMap(storage *atree.PersistentSlabStorage, address atree.Address, m *atree.OrderedMap) (*atree.OrderedMap, error) {
	iterator, err := m.Iterator()
	if err != nil {
		return nil, err
	}
	return atree.NewMapFromBatchData(
		storage,
		address,
		atree.NewDefaultDigesterBuilder(),
		m.Type(),
		compare,
		hashInputProvider,
		m.Seed(),
		func() (atree.Value, atree.Value, error) {
			k, v, err := iterator.Next()
			if err != nil {
				return nil, nil, err
			}
			if k == nil {
				return nil, nil, nil
			}
			copiedKey, err := copyValue(storage, address, k)
			if err != nil {
				return nil, nil, err
			}
			copiedValue, err := copyValue(storage, address, v)
			if err != nil {
				return nil, nil, err
			}
			return copiedKey, copiedValue, nil
		})
}

func removeValue(storage *atree.PersistentSlabStorage, value atree.Value) error {
	switch v := value.(type) {
	case *atree.Array:
		return removeStorable(storage, atree.StorageIDStorable(v.StorageID()))
	case *atree.OrderedMap:
		return removeStorable(storage, atree.StorageIDStorable(v.StorageID()))
	}
	return nil
}

func removeStorable(storage *atree.PersistentSlabStorage, storable atree.Storable) error {
	sid, ok := storable.(atree.StorageIDStorable)
	if !ok {
		return nil
	}

	id := atree.StorageID(sid)

	value, err := storable.StoredValue(storage)
	if err != nil {
		return err
	}

	switch v := value.(type) {
	case StringValue:
		return storage.Remove(id)
	case *atree.Array:
		err := v.PopIterate(func(storable atree.Storable) {
			_ = removeStorable(storage, storable)
		})
		if err != nil {
			return err
		}
		return storage.Remove(id)

	case *atree.OrderedMap:
		err := v.PopIterate(func(keyStorable atree.Storable, valueStorable atree.Storable) {
			_ = removeStorable(storage, keyStorable)
			_ = removeStorable(storage, valueStorable)
		})
		if err != nil {
			return err
		}
		return storage.Remove(id)

	default:
		return fmt.Errorf("failed to remove storable: storable type %T isn't supported", v)
	}
}

func valueEqual(a atree.Value, b atree.Value) error {
	switch a.(type) {
	case *atree.Array:
		return arrayEqual(a, b)
	case *atree.OrderedMap:
		return mapEqual(a, b)
	default:
		if !reflect.DeepEqual(a, b) {
			return fmt.Errorf("value %s (%T) != value %s (%T)", a, a, b, b)
		}
	}
	return nil
}

func arrayEqual(a atree.Value, b atree.Value) error {
	array1, ok := a.(*atree.Array)
	if !ok {
		return fmt.Errorf("value %s type is %T, want *atree.Array", a, a)
	}

	array2, ok := b.(*atree.Array)
	if !ok {
		return fmt.Errorf("value %s type is %T, want *atree.Array", b, b)
	}

	if array1.Count() != array2.Count() {
		return fmt.Errorf("array %s count %d != array %s count %d", array1, array1.Count(), array2, array2.Count())
	}

	iterator1, err := array1.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get array1 iterator: %w", err)
	}

	iterator2, err := array2.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get array2 iterator: %w", err)
	}

	for {
		value1, err := iterator1.Next()
		if err != nil {
			return fmt.Errorf("iterator1.Next() error: %w", err)
		}

		value2, err := iterator2.Next()
		if err != nil {
			return fmt.Errorf("iterator2.Next() error: %w", err)
		}

		err = valueEqual(value1, value2)
		if err != nil {
			return fmt.Errorf("array elements are different: %w", err)
		}

		if value1 == nil || value2 == nil {
			break
		}
	}

	return nil
}

func mapEqual(a atree.Value, b atree.Value) error {
	m1, ok := a.(*atree.OrderedMap)
	if !ok {
		return fmt.Errorf("value %s type is %T, want *atree.OrderedMap", a, a)
	}

	m2, ok := b.(*atree.OrderedMap)
	if !ok {
		return fmt.Errorf("value %s type is %T, want *atree.OrderedMap", b, b)
	}

	if m1.Count() != m2.Count() {
		return fmt.Errorf("map %s count %d != map %s count %d", m1, m1.Count(), m2, m2.Count())
	}

	iterator1, err := m1.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get m1 iterator: %w", err)
	}

	iterator2, err := m2.Iterator()
	if err != nil {
		return fmt.Errorf("failed to get m2 iterator: %w", err)
	}

	for {
		key1, value1, err := iterator1.Next()
		if err != nil {
			return fmt.Errorf("iterator1.Next() error: %w", err)
		}

		key2, value2, err := iterator2.Next()
		if err != nil {
			return fmt.Errorf("iterator2.Next() error: %w", err)
		}

		err = valueEqual(key1, key2)
		if err != nil {
			return fmt.Errorf("map keys are different: %w", err)
		}

		err = valueEqual(value1, value2)
		if err != nil {
			return fmt.Errorf("map values are different: %w", err)
		}

		if key1 == nil || key2 == nil {
			break
		}
	}

	return nil
}

// newArray creates atree.Array with random elements of specified size and nested level
func newArray(storage *atree.PersistentSlabStorage, address atree.Address, length int, nestedLevel int) (*atree.Array, error) {
	typeInfo := testTypeInfo{value: 123}

	array, err := atree.NewArray(storage, address, typeInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create new array: %w", err)
	}

	values := make([]atree.Value, length)

	for i := 0; i < length; i++ {
		value, err := randomValue(storage, address, nestedLevel-1)
		if err != nil {
			return nil, err
		}
		copedValue, err := copyValue(storage, atree.Address{}, value)
		if err != nil {
			return nil, err
		}
		values[i] = copedValue
		err = array.Append(value)
		if err != nil {
			return nil, err
		}
	}

	err = checkArrayDataLoss(array, values)
	if err != nil {
		return nil, err
	}

	for _, v := range values {
		err := removeValue(storage, v)
		if err != nil {
			return nil, err
		}
	}

	return array, nil
}

// newMap creates atree.OrderedMap with random elements of specified size and nested level
func newMap(storage *atree.PersistentSlabStorage, address atree.Address, length int, nestedLevel int) (*atree.OrderedMap, error) {
	typeInfo := testTypeInfo{value: 123}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create new map: %w", err)
	}

	elements := make(map[atree.Value]atree.Value, length)

	for i := 0; i < length; i++ {
		k, err := randomKey()
		if err != nil {
			return nil, err
		}

		copiedKey, err := copyValue(storage, atree.Address{}, k)
		if err != nil {
			return nil, err
		}

		v, err := randomValue(storage, address, nestedLevel-1)
		if err != nil {
			return nil, err
		}

		copiedValue, err := copyValue(storage, atree.Address{}, v)
		if err != nil {
			return nil, err
		}

		elements[copiedKey] = copiedValue

		existingStorable, err := m.Set(compare, hashInputProvider, k, v)
		if err != nil {
			return nil, err
		}

		if existingStorable != nil {
			// Delete overwritten element
			err = removeStorable(storage, existingStorable)
			if err != nil {
				return nil, fmt.Errorf("failed to remove storable element %s: %w", existingStorable, err)
			}
		}
	}

	err = checkMapDataLoss(m, elements)
	if err != nil {
		return nil, err
	}

	for k, v := range elements {
		err := removeValue(storage, k)
		if err != nil {
			return nil, err
		}
		err = removeValue(storage, v)
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

type InMemBaseStorage struct {
	segments       map[atree.StorageID][]byte
	storageIndex   map[atree.Address]atree.StorageIndex
	bytesRetrieved int
	bytesStored    int
}

var _ atree.BaseStorage = &InMemBaseStorage{}

func NewInMemBaseStorage() *InMemBaseStorage {
	return NewInMemBaseStorageFromMap(
		make(map[atree.StorageID][]byte),
	)
}

func NewInMemBaseStorageFromMap(segments map[atree.StorageID][]byte) *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:     segments,
		storageIndex: make(map[atree.Address]atree.StorageIndex),
	}
}

func (s *InMemBaseStorage) Retrieve(id atree.StorageID) ([]byte, bool, error) {
	seg, ok := s.segments[id]
	s.bytesRetrieved += len(seg)
	return seg, ok, nil
}

func (s *InMemBaseStorage) Store(id atree.StorageID, data []byte) error {
	s.segments[id] = data
	s.bytesStored += len(data)
	return nil
}

func (s *InMemBaseStorage) Remove(id atree.StorageID) error {
	delete(s.segments, id)
	return nil
}

func (s *InMemBaseStorage) GenerateStorageID(address atree.Address) (atree.StorageID, error) {
	index := s.storageIndex[address]
	nextIndex := index.Next()

	s.storageIndex[address] = nextIndex
	return atree.NewStorageID(address, nextIndex), nil
}

func (s *InMemBaseStorage) SegmentCounts() int {
	return len(s.segments)
}

func (s *InMemBaseStorage) Size() int {
	total := 0
	for _, seg := range s.segments {
		total += len(seg)
	}
	return total
}

func (s *InMemBaseStorage) BytesRetrieved() int {
	return s.bytesRetrieved
}

func (s *InMemBaseStorage) BytesStored() int {
	return s.bytesStored
}

func (s *InMemBaseStorage) SegmentsReturned() int {
	// not needed
	return 0
}

func (s *InMemBaseStorage) SegmentsUpdated() int {
	// not needed
	return 0
}

func (s *InMemBaseStorage) SegmentsTouched() int {
	// not needed
	return 0
}

func (s *InMemBaseStorage) ResetReporter() {
	// not needed
}
