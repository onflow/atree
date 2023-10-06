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
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/onflow/atree"
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

func (status *mapStatus) incOp(op mapOpType, count uint64) {
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

	status.count = count
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
	expectedValues := make(mapValue, flagMaxLength)

	reduceHeapAllocs := false

	opCount := uint64(0)

	var ms runtime.MemStats

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

		opCount++

		// Update status
		status.incOp(prevOp, m.Count())

		// Check map elements against elements after every op
		err = checkMapDataLoss(expectedValues, m)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}

		if opCount >= 100 {
			opCount = 0
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
	}
}

func nextMapOp(
	expectedValues mapValue,
	m *atree.OrderedMap,
	nestedLevels int,
	forceRemove bool,
) (mapOpType, error) {

	if forceRemove {
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

func modifyMap(
	expectedValues mapValue,
	m *atree.OrderedMap,
	nestedLevels int,
	forceRemove bool,
) (mapValue, mapOpType, error) {

	storage := m.Storage
	address := m.Address()

	nextOp, err := nextMapOp(expectedValues, m, nestedLevels, forceRemove)
	if err != nil {
		return nil, 0, err
	}

	switch nextOp {
	case mapSetOp1, mapSetOp2, mapSetOp3, mapMutateChildContainerAfterSet:

		var nextNestedLevels int

		if nextOp == mapMutateChildContainerAfterSet {
			nextNestedLevels = nestedLevels - 1
		} else { // mapSetOp1, mapSetOp2, mapSetOp3
			nextNestedLevels = r.Intn(nestedLevels)
		}

		expectedKey, key, err := randomKey()
		if err != nil {
			return nil, 0, fmt.Errorf("failed to generate random key %s: %s", key, err)
		}

		expectedChildValue, child, err := randomValue(storage, address, nextNestedLevels)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to generate random value %s: %s", child, err)
		}

		oldExpectedValue := expectedValues[expectedKey]

		// Update expectedValues
		expectedValues[expectedKey] = expectedChildValue

		// Update map
		existingStorable, err := m.Set(compare, hashInputProvider, key, child)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to set %s at index %d: %s", child, key, err)
		}

		// Compare old value from map with old value from elements
		if (oldExpectedValue == nil) != (existingStorable == nil) {
			return nil, 0, fmt.Errorf("Set returned storable %s != expected %s", existingStorable, oldExpectedValue)
		}

		if existingStorable != nil {

			existingValue, err := existingStorable.StoredValue(storage)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to convert %s to value: %s", existingStorable, err)
			}

			err = valueEqual(oldExpectedValue, existingValue)
			if err != nil {
				return nil, 0, fmt.Errorf("Set() returned wrong existing value %s, want %s", existingValue, oldExpectedValue)
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
		existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, key)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to remove element with key %s: %s", key, err)
		}

		// Compare removed key from map with removed key from elements
		existingKeyValue, err := existingKeyStorable.StoredValue(storage)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to convert %s to value: %s", existingKeyStorable, err)
		}

		err = valueEqual(key, existingKeyValue)
		if err != nil {
			return nil, 0, fmt.Errorf("Remove() returned wrong existing key %s, want %s", existingKeyStorable, key)
		}

		// Compare removed value from map with removed value from elements
		existingValue, err := existingValueStorable.StoredValue(storage)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to convert %s to value: %s", existingValueStorable, err)
		}

		err = valueEqual(oldExpectedValue, existingValue)
		if err != nil {
			return nil, 0, fmt.Errorf("Remove() returned wrong existing value %s, want %s", existingValueStorable, oldExpectedValue)
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

		child, err := m.Get(compare, hashInputProvider, key)
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

func hasChildContainerInMap(expectedValues mapValue) bool {
	for _, v := range expectedValues {
		switch v.(type) {
		case arrayValue, mapValue:
			return true
		}
	}
	return false
}

func getRandomChildContainerKeyInMap(expectedValues mapValue) (key atree.Value, found bool) {
	keys := make([]atree.Value, 0, len(expectedValues))
	for k, v := range expectedValues {
		switch v.(type) {
		case arrayValue, mapValue:
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, false
	}
	return keys[r.Intn(len(keys))], true
}

func checkMapDataLoss(expectedValues mapValue, m *atree.OrderedMap) error {

	// Check map has the same number of elements as elements
	if m.Count() != uint64(len(expectedValues)) {
		return fmt.Errorf("Count() %d != len(values) %d", m.Count(), len(expectedValues))
	}

	// Check every element
	for k, v := range expectedValues {
		convertedValue, err := m.Get(compare, hashInputProvider, k)
		if err != nil {
			return fmt.Errorf("failed to get element with key %s: %w", k, err)
		}
		err = valueEqual(v, convertedValue)
		if err != nil {
			return fmt.Errorf("failed to compare %s and %s: %w", v, convertedValue, err)
		}
	}

	if flagCheckSlabEnabled {
		typeInfoComparator := func(a atree.TypeInfo, b atree.TypeInfo) bool {
			return a.ID() == b.ID()
		}

		err := atree.VerifyMap(m, m.Address(), m.Type(), typeInfoComparator, hashInputProvider, true)
		if err != nil {
			return err
		}

		err = atree.VerifyMapSerialization(
			m,
			cborDecMode,
			cborEncMode,
			decodeStorable,
			decodeTypeInfo,
			func(a, b atree.Storable) bool {
				return reflect.DeepEqual(a, b)
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}
