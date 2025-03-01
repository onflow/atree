/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright Flow Foundation
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
	"time"

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
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
		v := test_utils.Uint8Value(r.Intn(math.MaxUint8)) // 255
		return v, v, nil

	case uint16Type:
		v := test_utils.Uint16Value(r.Intn(math.MaxUint16)) // 65535
		return v, v, nil

	case uint32Type:
		v := test_utils.Uint32Value(r.Intn(math.MaxUint32)) // 4294967295
		return v, v, nil

	case uint64Type:
		v := test_utils.Uint64Value(r.Intn(math.MaxInt)) // 9_223_372_036_854_775_807
		return v, v, nil

	case smallStringType:
		slen := r.Intn(125)
		v := test_utils.NewStringValue(randStr(slen))
		return v, v, nil

	case largeStringType:
		slen := r.Intn(125) + 1024/2
		v := test_utils.NewStringValue(randStr(slen))
		return v, v, nil

	default:
		return nil, nil, fmt.Errorf("unexpected random simple value type %d", valueType)
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
		return nil, nil, fmt.Errorf("unexpected random container value type %d", valueType)
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

		expected, actual, err = generateSimpleValue(t)
		if err != nil {
			return nil, nil, err
		}
	} else {
		t := r.Intn(maxContainerValueType)

		expected, actual, err = generateContainerValue(t, storage, address, nestedLevels)
		if err != nil {
			return nil, nil, err
		}
	}

	expected, actual = randomWrapperValue(expected, actual)
	return expected, actual, nil
}

func randomWrapperValue(expected atree.Value, actual atree.Value) (atree.Value, atree.Value) {
	const (
		noWrapperValue = iota
		useWrapperValue
		maxWrapperValueChoice
	)

	if flagAlwaysUseWrapperValue || r.Intn(maxWrapperValueChoice) == useWrapperValue {
		return test_utils.NewExpectedWrapperValue(expected), test_utils.NewSomeValue(actual)
	}

	return expected, actual
}

func removeStorable(storage atree.SlabStorage, storable atree.Storable) error {

	value, err := storable.StoredValue(storage)
	if err != nil {
		return err
	}

	unwrappedValue, _ := unwrapValue(value)

	switch v := unwrappedValue.(type) {
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

	if sid, ok := unwrapStorable(storable).(atree.SlabIDStorable); ok {
		return storage.Remove(atree.SlabID(sid))
	}

	return nil
}

// newArray creates atree.Array with random elements of specified size and nested level
func newArray(
	storage atree.SlabStorage,
	address atree.Address,
	length int,
	nestedLevel int,
) (test_utils.ExpectedArrayValue, *atree.Array, error) {

	typeInfo := newArrayTypeInfo()

	array, err := atree.NewArray(storage, address, typeInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new array: %w", err)
	}

	expectedValues := make(test_utils.ExpectedArrayValue, length)

	for i := range expectedValues {
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
) (test_utils.ExpectedMapValue, *atree.OrderedMap, error) {

	typeInfo := newMapTypeInfo()

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new map: %w", err)
	}

	expectedValues := make(test_utils.ExpectedMapValue, length)

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

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, key, value)
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
) (test_utils.ExpectedMapValue, *atree.OrderedMap, error) {

	compositeType := newCompositeTypeInfo()

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), compositeType)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new map: %w", err)
	}

	expectedValues := make(test_utils.ExpectedMapValue)

	for _, name := range compositeType.getFieldNames() {

		expectedKey, key := test_utils.NewStringValue(name), test_utils.NewStringValue(name)

		expectedValue, value, err := randomValue(storage, address, nestedLevel-1)
		if err != nil {
			return nil, nil, err
		}

		expectedValues[expectedKey] = expectedValue

		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, key, value)
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

func unwrapValue(v atree.Value) (atree.Value, uint64) {
	switch v := v.(type) {
	case atree.WrapperValue:
		return v.UnwrapAtreeValue()
	default:
		return v, 0
	}
}

func unwrapStorable(s atree.Storable) atree.Storable {
	switch s := s.(type) {
	case atree.WrapperStorable:
		return s.UnwrapAtreeStorable()
	default:
		return s
	}
}
