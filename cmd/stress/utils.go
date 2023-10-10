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
	"math"
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
	maxSimpleValueType
)

const (
	arrayType int = iota
	mapType
	compositeType
	maxContainerValueType
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

func generateSimpleValue(
	valueType int,
) (expected atree.Value, actual atree.Value, err error) {
	switch valueType {
	case uint8Type:
		v := Uint8Value(r.Intn(math.MaxUint8)) // 255
		return v, v, nil

	case uint16Type:
		v := Uint16Value(r.Intn(math.MaxUint16)) // 65535
		return v, v, nil

	case uint32Type:
		v := Uint32Value(r.Intn(math.MaxUint32)) // 4294967295
		return v, v, nil

	case uint64Type:
		v := Uint64Value(r.Intn(math.MaxInt)) // 9_223_372_036_854_775_807
		return v, v, nil

	case smallStringType:
		slen := r.Intn(125)
		v := NewStringValue(randStr(slen))
		return v, v, nil

	case largeStringType:
		slen := r.Intn(125) + 1024/2
		v := NewStringValue(randStr(slen))
		return v, v, nil

	default:
		return nil, nil, fmt.Errorf("unexpected randome simple value type %d", valueType)
	}
}

func generateContainerValue(
	valueType int,
	storage atree.SlabStorage,
	address atree.Address,
	nestedLevels int,
) (expected atree.Value, actual atree.Value, err error) {
	switch valueType {
	case arrayType:
		length := r.Intn(maxNestedArraySize)
		return newArray(storage, address, length, nestedLevels)

	case mapType:
		length := r.Intn(maxNestedMapSize)
		return newMap(storage, address, length, nestedLevels)

	case compositeType:
		return newComposite(storage, address, nestedLevels)

	default:
		return nil, nil, fmt.Errorf("unexpected randome container value type %d", valueType)
	}
}

func randomKey() (atree.Value, atree.Value, error) {
	t := r.Intn(maxSimpleValueType)
	return generateSimpleValue(t)
}

func randomValue(
	storage atree.SlabStorage,
	address atree.Address,
	nestedLevels int,
) (expected atree.Value, actual atree.Value, err error) {
	if nestedLevels <= 0 {
		t := r.Intn(maxSimpleValueType)
		return generateSimpleValue(t)
	}

	t := r.Intn(maxContainerValueType)
	return generateContainerValue(t, storage, address, nestedLevels)
}

func removeStorable(storage atree.SlabStorage, storable atree.Storable) error {

	value, err := storable.StoredValue(storage)
	if err != nil {
		return err
	}

	switch v := value.(type) {
	case *atree.Array:
		err := v.PopIterate(func(storable atree.Storable) {
			_ = removeStorable(storage, storable)
		})
		if err != nil {
			return err
		}

	case *atree.OrderedMap:
		err := v.PopIterate(func(keyStorable atree.Storable, valueStorable atree.Storable) {
			_ = removeStorable(storage, keyStorable)
			_ = removeStorable(storage, valueStorable)
		})
		if err != nil {
			return err
		}
	}

	if sid, ok := storable.(atree.SlabIDStorable); ok {
		return storage.Remove(atree.SlabID(sid))
	}

	return nil
}

func valueEqual(expected atree.Value, actual atree.Value) error {
	switch expected := expected.(type) {
	case arrayValue:
		actual, ok := actual.(*atree.Array)
		if !ok {
			return fmt.Errorf("failed to convert actual value to *Array, got %T", actual)
		}

		return arrayEqual(expected, actual)

	case *atree.Array:
		return fmt.Errorf("expected value shouldn't be *Array")

	case mapValue:
		actual, ok := actual.(*atree.OrderedMap)
		if !ok {
			return fmt.Errorf("failed to convert actual value to *OrderedMap, got %T", actual)
		}

		return mapEqual(expected, actual)

	case *atree.OrderedMap:
		return fmt.Errorf("expected value shouldn't be *OrderedMap")

	default:
		if !reflect.DeepEqual(expected, actual) {
			return fmt.Errorf("expected value %v (%T) != actual value %v (%T)", expected, expected, actual, actual)
		}
	}

	return nil
}

func arrayEqual(expected arrayValue, actual *atree.Array) error {
	if uint64(len(expected)) != actual.Count() {
		return fmt.Errorf("array count %d != expected count %d", actual.Count(), len(expected))
	}

	iterator, err := actual.ReadOnlyIterator()
	if err != nil {
		return fmt.Errorf("failed to get array iterator: %w", err)
	}

	i := 0
	for {
		actualValue, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("iterator.Next() error: %w", err)
		}

		if actualValue == nil {
			break
		}

		if i >= len(expected) {
			return fmt.Errorf("more elements from array iterator than expected")
		}

		err = valueEqual(expected[i], actualValue)
		if err != nil {
			return fmt.Errorf("array elements are different: %w", err)
		}

		i++
	}

	if i != len(expected) {
		return fmt.Errorf("got %d iterated array elements, expect %d values", i, len(expected))
	}

	return nil
}

func mapEqual(expected mapValue, actual *atree.OrderedMap) error {
	if uint64(len(expected)) != actual.Count() {
		return fmt.Errorf("map count %d != expected count %d", actual.Count(), len(expected))
	}

	iterator, err := actual.ReadOnlyIterator()
	if err != nil {
		return fmt.Errorf("failed to get map iterator: %w", err)
	}

	i := 0
	for {
		actualKey, actualValue, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("iterator.Next() error: %w", err)
		}

		if actualKey == nil {
			break
		}

		expectedValue, exist := expected[actualKey]
		if !exist {
			return fmt.Errorf("failed to find key %v in expected values", actualKey)
		}

		err = valueEqual(expectedValue, actualValue)
		if err != nil {
			return fmt.Errorf("map values are different: %w", err)
		}

		i++
	}

	if i != len(expected) {
		return fmt.Errorf("got %d iterated map elements, expect %d values", i, len(expected))
	}

	return nil
}

// newArray creates atree.Array with random elements of specified size and nested level
func newArray(
	storage atree.SlabStorage,
	address atree.Address,
	length int,
	nestedLevel int,
) (arrayValue, *atree.Array, error) {

	typeInfo := newArrayTypeInfo()

	array, err := atree.NewArray(storage, address, typeInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new array: %w", err)
	}

	expectedValues := make(arrayValue, length)

	for i := 0; i < length; i++ {
		expectedValue, value, err := randomValue(storage, address, nestedLevel-1)
		if err != nil {
			return nil, nil, err
		}

		expectedValues[i] = expectedValue

		err = array.Append(value)
		if err != nil {
			return nil, nil, err
		}
	}

	err = checkArrayDataLoss(expectedValues, array)
	if err != nil {
		return nil, nil, err
	}

	return expectedValues, array, nil
}

// newMap creates atree.OrderedMap with random elements of specified size and nested level
func newMap(
	storage atree.SlabStorage,
	address atree.Address,
	length int,
	nestedLevel int,
) (mapValue, *atree.OrderedMap, error) {

	typeInfo := newMapTypeInfo()

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new map: %w", err)
	}

	expectedValues := make(mapValue, length)

	for m.Count() < uint64(length) {
		expectedKey, key, err := randomKey()
		if err != nil {
			return nil, nil, err
		}

		expectedValue, value, err := randomValue(storage, address, nestedLevel-1)
		if err != nil {
			return nil, nil, err
		}

		expectedValues[expectedKey] = expectedValue

		existingStorable, err := m.Set(compare, hashInputProvider, key, value)
		if err != nil {
			return nil, nil, err
		}

		if existingStorable != nil {
			// Delete overwritten element
			err = removeStorable(storage, existingStorable)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to remove storable element %s: %w", existingStorable, err)
			}
		}
	}

	err = checkMapDataLoss(expectedValues, m)
	if err != nil {
		return nil, nil, err
	}

	return expectedValues, m, nil
}

// newComposite creates atree.OrderedMap with elements of random composite type and nested level
func newComposite(
	storage atree.SlabStorage,
	address atree.Address,
	nestedLevel int,
) (mapValue, *atree.OrderedMap, error) {

	compositeType := newCompositeTypeInfo()

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), compositeType)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new map: %w", err)
	}

	expectedValues := make(mapValue)

	for _, name := range compositeType.getFieldNames() {

		expectedKey, key := NewStringValue(name), NewStringValue(name)

		expectedValue, value, err := randomValue(storage, address, nestedLevel-1)
		if err != nil {
			return nil, nil, err
		}

		expectedValues[expectedKey] = expectedValue

		existingStorable, err := m.Set(compare, hashInputProvider, key, value)
		if err != nil {
			return nil, nil, err
		}
		if existingStorable != nil {
			return nil, nil, fmt.Errorf("failed to create new map of composite type: found duplicate field name %s", name)
		}
	}

	err = checkMapDataLoss(expectedValues, m)
	if err != nil {
		return nil, nil, err
	}

	return expectedValues, m, nil
}

type InMemBaseStorage struct {
	segments       map[atree.SlabID][]byte
	storageIndex   map[atree.Address]atree.SlabIndex
	bytesRetrieved int
	bytesStored    int
}

var _ atree.BaseStorage = &InMemBaseStorage{}

func NewInMemBaseStorage() *InMemBaseStorage {
	return NewInMemBaseStorageFromMap(
		make(map[atree.SlabID][]byte),
	)
}

func NewInMemBaseStorageFromMap(segments map[atree.SlabID][]byte) *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:     segments,
		storageIndex: make(map[atree.Address]atree.SlabIndex),
	}
}

func (s *InMemBaseStorage) Retrieve(id atree.SlabID) ([]byte, bool, error) {
	seg, ok := s.segments[id]
	s.bytesRetrieved += len(seg)
	return seg, ok, nil
}

func (s *InMemBaseStorage) Store(id atree.SlabID, data []byte) error {
	s.segments[id] = data
	s.bytesStored += len(data)
	return nil
}

func (s *InMemBaseStorage) Remove(id atree.SlabID) error {
	delete(s.segments, id)
	return nil
}

func (s *InMemBaseStorage) GenerateSlabID(address atree.Address) (atree.SlabID, error) {
	index := s.storageIndex[address]
	nextIndex := index.Next()

	s.storageIndex[address] = nextIndex
	return atree.NewSlabID(address, nextIndex), nil
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

// arrayValue is an atree.Value that represents an array of atree.Value.
// It's used to test elements of atree.Array.
type arrayValue []atree.Value

var _ atree.Value = &arrayValue{}

func (v arrayValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

// mapValue is an atree.Value that represents a map of atree.Value.
// It's used to test elements of atree.OrderedMap.
type mapValue map[atree.Value]atree.Value

var _ atree.Value = &mapValue{}

func (v mapValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic("not reachable")
}

var typeInfoComparator = func(a atree.TypeInfo, b atree.TypeInfo) bool {
	return a.ID() == b.ID()
}
