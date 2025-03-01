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

package test_utils

import (
	"fmt"
	"reflect"

	"github.com/onflow/atree"
)

// ExpectedArrayValue

type ExpectedArrayValue []atree.Value

var _ atree.Value = &ExpectedArrayValue{}

func (v ExpectedArrayValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic(atree.NewUnreachableError())
}

// ExpectedMapValue

type ExpectedMapValue map[atree.Value]atree.Value

var _ atree.Value = &ExpectedMapValue{}

func (v ExpectedMapValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic(atree.NewUnreachableError())
}

// ExpectedWrapperValue

type ExpectedWrapperValue struct {
	Value atree.Value
}

var _ atree.Value = &ExpectedWrapperValue{}

func NewExpectedWrapperValue(value atree.Value) ExpectedWrapperValue {
	return ExpectedWrapperValue{value}
}

func (v ExpectedWrapperValue) Storable(atree.SlabStorage, atree.Address, uint64) (atree.Storable, error) {
	panic(atree.NewUnreachableError())
}

func ValueEqual(expected atree.Value, actual atree.Value) (bool, error) {
	switch expected := expected.(type) {
	case ExpectedArrayValue:
		actual, ok := actual.(*atree.Array)
		if !ok {
			return false, nil
		}
		return ArrayEqual(expected, actual)

	case *atree.Array:
		return false, fmt.Errorf("ValueEqual failed: expected value shouldn't be *atree.Array")

	case ExpectedMapValue:
		actual, ok := actual.(*atree.OrderedMap)
		if !ok {
			return false, nil
		}
		return MapEqual(expected, actual)

	case *atree.OrderedMap:
		return false, fmt.Errorf("ValueEqual failed: expected value shouldn't be *atree.OrderedMap")

	case ExpectedWrapperValue:
		actual, ok := actual.(SomeValue)
		if !ok {
			return false, nil
		}
		return ValueEqual(expected.Value, actual.Value)

	case SomeValue:
		return false, fmt.Errorf("ValueEqual failed: expected value shouldn't be SomeValue")

	default:
		return reflect.DeepEqual(expected, actual), nil
	}
}

func ArrayEqual(expected ExpectedArrayValue, actual *atree.Array) (bool, error) {
	if uint64(len(expected)) != actual.Count() {
		return false, nil
	}

	iterator, err := actual.ReadOnlyIterator()
	if err != nil {
		return false, err
	}

	i := 0
	for {
		actualValue, err := iterator.Next()
		if err != nil {
			return false, err
		}

		if actualValue == nil {
			break
		}

		if i >= len(expected) {
			return false, nil
		}

		equal, err := ValueEqual(expected[i], actualValue)
		if !equal || err != nil {
			return equal, err
		}

		i++
	}

	if len(expected) != i {
		return false, fmt.Errorf("ArrayEqual failed: iterated %d time, expected %d elements", i, len(expected))
	}

	return true, nil
}

func MapEqual(expected ExpectedMapValue, actual *atree.OrderedMap) (bool, error) {
	if uint64(len(expected)) != actual.Count() {
		return false, nil
	}

	iterator, err := actual.ReadOnlyIterator()
	if err != nil {
		return false, err
	}

	i := 0
	for {
		actualKey, actualValue, err := iterator.Next()
		if err != nil {
			return false, err
		}

		if actualKey == nil {
			break
		}

		expectedValue, exist := expected[actualKey]
		if !exist {
			return false, nil
		}

		equal, err := ValueEqual(expectedValue, actualValue)
		if !equal || err != nil {
			return equal, err
		}

		i++
	}

	if len(expected) != i {
		return false, fmt.Errorf("MapEqual failed: iterated %d time, expected %d elements", i, len(expected))
	}

	return true, nil
}
