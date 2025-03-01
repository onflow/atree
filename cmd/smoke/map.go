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
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/onflow/atree"
	"github.com/onflow/atree/test_utils"
)

type mapOpType int

const (
	mapSetOp1 mapOpType = iota
	mapSetOp2
	mapSetOp3
	mapRemoveOp
	mapMutateChildContainerAfterGet
	mapMutateChildContainerAfterSet
	maxMapOp
)

const mapNoOp mapOpType = -1

type mapStatus struct {
	lock sync.RWMutex

	startTime time.Time

	count uint64 // number of elements in map

	setOps                          uint64
	removeOps                       uint64
	mutateChildContainerAfterGetOps uint64
	mutateChildContainerAfterSetOps uint64
}

var _ Status = &mapStatus{}

func newMapStatus() *mapStatus {
	return &mapStatus{startTime: time.Now()}
}

func (status *mapStatus) String() string {
	status.lock.RLock()
	defer status.lock.RUnlock()

	duration := time.Since(status.startTime)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return fmt.Sprintf("duration %s, heapAlloc %d MiB, %d elements, %d sets, %d removes, %d Get mutations, %d Set mutations",
		duration.Truncate(time.Second).String(),
		m.Alloc/1024/1024,
		status.count,
		status.setOps,
		status.removeOps,
		status.mutateChildContainerAfterGetOps,
		status.mutateChildContainerAfterSetOps,
	)
}

func (status *mapStatus) incOp(op mapOpType, newTotalCount uint64) {
	status.lock.Lock()
	defer status.lock.Unlock()

	switch op {
	case mapSetOp1, mapSetOp2, mapSetOp3:
		status.setOps++

	case mapRemoveOp:
		status.removeOps++

	case mapMutateChildContainerAfterGet:
		status.mutateChildContainerAfterGetOps++

	case mapMutateChildContainerAfterSet:
		status.mutateChildContainerAfterSetOps++
	}

	status.count = newTotalCount
}

func (status *mapStatus) Write() {
	writeStatus(status.String())
}

func testMap(
	storage *atree.PersistentSlabStorage,
	address atree.Address,
	status *mapStatus,
) {
	typeInfo := newMapTypeInfo()

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create new map: %s", err)
		return
	}

	// expectedValues contains generated keys and values. It is used to check data loss.
	expectedValues := make(test_utils.ExpectedMapValue, flagMaxLength)

	reduceHeapAllocs := false

	opCountForStorageHealthCheck := uint64(0)

	var ms runtime.MemStats

	count := uint64(0)

	for {
		runtime.ReadMemStats(&ms)
		allocMiB := ms.Alloc / 1024 / 1024

		if !reduceHeapAllocs && allocMiB > flagMaxHeapAllocMiB {
			fmt.Printf("\nHeapAlloc is %d MiB, removing elements to reduce allocs...\n", allocMiB)
			reduceHeapAllocs = true
		} else if reduceHeapAllocs && allocMiB < flagMinHeapAllocMiB {
			fmt.Printf("\nHeapAlloc is %d MiB, resuming random operation...\n", allocMiB)
			reduceHeapAllocs = false
		}

		if reduceHeapAllocs && m.Count() == 0 {
			fmt.Printf("\nHeapAlloc is %d MiB while map is empty, dropping read/write cache to free mem\n", allocMiB)
			reduceHeapAllocs = false

			// Commit slabs to storage and drop read and write to reduce mem
			err = storage.FastCommit(runtime.NumCPU())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to commit to storage: %s", err)
				return
			}

			storage.DropDeltas()
			storage.DropCache()

			expectedValues = make(map[atree.Value]atree.Value, flagMaxLength)

			// Load root slab from storage and cache it in read cache
			rootID := m.SlabID()
			m, err = atree.NewMapWithRootID(storage, rootID, atree.NewDefaultDigesterBuilder())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create map from root id %s: %s", rootID, err)
				return
			}

			runtime.GC()

			// Check if map is using > MaxHeapAlloc while empty.
			runtime.ReadMemStats(&ms)
			allocMiB = ms.Alloc / 1024 / 1024
			fmt.Printf("\nHeapAlloc is %d MiB after cleanup and forced gc\n", allocMiB)

			// Prevent infinite loop that doesn't do useful work.
			if allocMiB > flagMaxHeapAllocMiB {
				// This shouldn't happen unless there's a memory leak.
				fmt.Fprintf(
					os.Stderr,
					"Exiting because allocMiB %d > maxMapHeapAlloMiB %d with empty map\n",
					allocMiB,
					flagMaxHeapAllocMiB)
				return
			}
		}

		var forceRemove bool
		if m.Count() == flagMaxLength || reduceHeapAllocs {
			forceRemove = true
		}

		var prevOp mapOpType
		expectedValues, prevOp, err = modifyMap(expectedValues, m, maxNestedLevels, forceRemove)
		if err != nil {
			fmt.Fprint(os.Stderr, err.Error())
			return
		}

		opCountForStorageHealthCheck++

		count++

		// Update status
		status.incOp(prevOp, m.Count())

		// Check map elements against elements after every op
		err = checkMapDataLoss(expectedValues, m)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}

		if opCountForStorageHealthCheck >= flagMinOpsForStorageHealthCheck {
			opCountForStorageHealthCheck = 0

			if !checkStorageHealth(storage, m.SlabID()) {
				return
			}

			// Commit slabs to storage so slabs are encoded and then decoded at next op.
			err = storage.FastCommit(runtime.NumCPU())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to commit to storage: %s", err)
				return
			}

			// Drop cache after commit to force slab decoding at next op.
			storage.DropCache()
		}

		if flagIterationCount > 0 && flagIterationCount == count {
			break
		}
	}
}

func nextMapOp(
	expectedValues test_utils.ExpectedMapValue,
	m *atree.OrderedMap,
	nestedLevels int,
	forceRemove bool,
) (mapOpType, error) {

	isComposite := m.Type().IsComposite()

	if isComposite && m.Count() == 0 {
		// No op for empty composite values.
		return mapNoOp, nil
	}

	if forceRemove && !isComposite {
		if m.Count() == 0 {
			return 0, fmt.Errorf("failed to force remove map elements because map has no elements")
		}
		return mapRemoveOp, nil
	}

	if m.Count() == 0 {
		return mapSetOp1, nil
	}

	for {
		nextOp := mapOpType(r.Intn(int(maxMapOp)))

		switch nextOp {
		case mapRemoveOp:
			if !isComposite {
				return nextOp, nil
			}

			// Can't remove fields in map of composite type.
			// Try another map operations.

		case mapMutateChildContainerAfterSet:
			if nestedLevels-1 > 0 {
				return nextOp, nil
			}

			// New child container can't be created because next nestedLevels is 0.
			// Try another map operation.

		case mapMutateChildContainerAfterGet:
			if hasChildContainerInMap(expectedValues) {
				return nextOp, nil
			}

			// Map doesn't have child container, try another map operation.

		default:
			return nextOp, nil
		}
	}
}

func getMapKeys(m *atree.OrderedMap) ([]atree.Value, error) {
	keys := make([]atree.Value, 0, m.Count())
	err := m.IterateKeys(test_utils.CompareValue, test_utils.GetHashInput, func(key atree.Value) (resume bool, err error) {
		keys = append(keys, key)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func modifyMap(
	expectedValues test_utils.ExpectedMapValue,
	m *atree.OrderedMap,
	nestedLevels int,
	forceRemove bool,
) (test_utils.ExpectedMapValue, mapOpType, error) {

	storage := m.Storage
	address := m.Address()

	nextOp, err := nextMapOp(expectedValues, m, nestedLevels, forceRemove)
	if err != nil {
		return nil, 0, err
	}

	if nextOp == mapNoOp {
		return expectedValues, nextOp, nil
	}

	switch nextOp {
	case mapSetOp1, mapSetOp2, mapSetOp3, mapMutateChildContainerAfterSet:

		var nextNestedLevels int

		switch nextOp {
		case mapSetOp1, mapSetOp2, mapSetOp3:
			nextNestedLevels = r.Intn(nestedLevels)
		case mapMutateChildContainerAfterSet:
			nextNestedLevels = nestedLevels - 1
		default:
			panic(atree.NewUnreachableError())
		}

		var expectedKey, key atree.Value
		var err error

		if m.Type().IsComposite() {
			// Update existing field, instead of creating new field.
			keys, err := getMapKeys(m)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to iterate composite keys: %s", err)
			}
			if len(keys) == 0 {
				// No op for empty composite values.
				return expectedValues, mapNoOp, nil
			}

			key = keys[r.Intn(len(keys))]
			expectedKey = key

		} else {
			expectedKey, key, err = randomKey()
			if err != nil {
				return nil, 0, fmt.Errorf("failed to generate random key %s: %s", key, err)
			}
		}

		expectedChildValue, child, err := randomValue(storage, address, nextNestedLevels)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to generate random value %s: %s", child, err)
		}

		oldExpectedValue := expectedValues[expectedKey]

		// Update expectedValues
		expectedValues[expectedKey] = expectedChildValue

		// Update map
		existingStorable, err := m.Set(test_utils.CompareValue, test_utils.GetHashInput, key, child)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to set %s at index %d: %s", child, key, err)
		}

		// Compare old value from map with old value from elements
		if (oldExpectedValue == nil) != (existingStorable == nil) {
			return nil, 0, fmt.Errorf("set returned storable %s != expected %s", existingStorable, oldExpectedValue)
		}

		if existingStorable != nil {

			existingValue, err := existingStorable.StoredValue(storage)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to convert %s to value: %s", existingStorable, err)
			}

			equal, err := test_utils.ValueEqual(oldExpectedValue, existingValue)
			if err != nil {
				return nil, 0, fmt.Errorf("Set() returned wrong existing value %s, want %s", existingValue, oldExpectedValue)
			}
			if !equal {
				return nil, 0, fmt.Errorf("overwritten map element isn't as expected: %s, %s", oldExpectedValue, existingValue)
			}

			// Delete removed element from storage
			err = removeStorable(storage, existingStorable)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to remove map storable element %s: %s", existingStorable, err)
			}
		}

		if nextOp == mapMutateChildContainerAfterSet {
			expectedValues[expectedKey], err = modifyContainer(expectedValues[expectedKey], child, nextNestedLevels)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to modify child container at key %s: %w", expectedKey, err)
			}
		}

	case mapRemoveOp:
		if m.Type().IsComposite() {
			panic(atree.NewUnreachableError())
		}

		// Use for-range on Go map to get random key
		var key atree.Value
		for k := range expectedValues {
			key = k
			break
		}

		oldExpectedValue := expectedValues[key]

		// Update expectedValues
		delete(expectedValues, key)

		// Update map
		existingKeyStorable, existingValueStorable, err := m.Remove(test_utils.CompareValue, test_utils.GetHashInput, key)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to remove element with key %s: %s", key, err)
		}

		// Compare removed key from map with removed key from elements
		existingKeyValue, err := existingKeyStorable.StoredValue(storage)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to convert %s to value: %s", existingKeyStorable, err)
		}

		equal, err := test_utils.ValueEqual(key, existingKeyValue)
		if err != nil {
			return nil, 0, fmt.Errorf("Remove() returned wrong existing key %s, want %s", existingKeyStorable, key)
		}
		if !equal {
			return nil, 0, fmt.Errorf("removed map key isn't as expected: %s, %s", key, existingKeyValue)
		}

		// Compare removed value from map with removed value from elements
		existingValue, err := existingValueStorable.StoredValue(storage)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to convert %s to value: %s", existingValueStorable, err)
		}

		equal, err = test_utils.ValueEqual(oldExpectedValue, existingValue)
		if err != nil {
			return nil, 0, fmt.Errorf("Remove() returned wrong existing value %s, want %s", existingValueStorable, oldExpectedValue)
		}
		if !equal {
			return nil, 0, fmt.Errorf("removed map element isn't as expected: %s, %s", oldExpectedValue, existingValue)
		}

		// Delete removed element from storage
		err = removeStorable(storage, existingKeyStorable)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to remove key %s: %s", existingKeyStorable, err)
		}

		err = removeStorable(storage, existingValueStorable)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to remove value %s: %s", existingValueStorable, err)
		}

	case mapMutateChildContainerAfterGet:
		key, found := getRandomChildContainerKeyInMap(expectedValues)
		if !found {
			// mapMutateChildContainerAfterGet op can't be performed because there isn't any child container in this map.
			// Try another map operation.
			return modifyMap(expectedValues, m, nestedLevels, forceRemove)
		}

		child, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, key)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to get element from map at key %s: %s", key, err)
		}

		expectedValues[key], err = modifyContainer(expectedValues[key], child, nestedLevels-1)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to modify child container at key %s: %w", key, err)
		}
	}

	return expectedValues, nextOp, nil
}

func hasChildContainerInMap(expectedValues test_utils.ExpectedMapValue) bool {
	for _, v := range expectedValues {
		v, _ = unwrapValue(v)
		switch v.(type) {
		case test_utils.ExpectedArrayValue, test_utils.ExpectedMapValue:
			return true
		}
	}
	return false
}

func getRandomChildContainerKeyInMap(expectedValues test_utils.ExpectedMapValue) (key atree.Value, found bool) {
	keys := make([]atree.Value, 0, len(expectedValues))
	for k, v := range expectedValues {
		v, _ = unwrapValue(v)
		switch v.(type) {
		case test_utils.ExpectedArrayValue, test_utils.ExpectedMapValue:
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, false
	}
	return keys[r.Intn(len(keys))], true
}

func checkMapDataLoss(expectedValues test_utils.ExpectedMapValue, m *atree.OrderedMap) error {

	// Check map has the same number of elements as elements
	if m.Count() != uint64(len(expectedValues)) {
		return fmt.Errorf("Count() %d != len(values) %d", m.Count(), len(expectedValues))
	}

	// Check every element
	for k, v := range expectedValues {
		convertedValue, err := m.Get(test_utils.CompareValue, test_utils.GetHashInput, k)
		if err != nil {
			return fmt.Errorf("failed to get element with key %s: %w", k, err)
		}
		equal, err := test_utils.ValueEqual(v, convertedValue)
		if err != nil {
			return fmt.Errorf("failed to compare %s and %s: %w", v, convertedValue, err)
		}
		if !equal {
			return fmt.Errorf("map element isn't as expected: %s, %s", convertedValue, v)
		}
	}

	if flagCheckSlabEnabled {
		err := checkMapSlab(m)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkMapSlab(m *atree.OrderedMap) error {
	err := atree.VerifyMap(m, m.Address(), m.Type(), compareTypeInfo, test_utils.GetHashInput, true)
	if err != nil {
		return err
	}

	return atree.VerifyMapSerialization(
		m,
		cborDecMode,
		cborEncMode,
		test_utils.DecodeStorable,
		decodeTypeInfo,
		func(a, b atree.Storable) bool {
			return reflect.DeepEqual(a, b)
		},
	)
}
