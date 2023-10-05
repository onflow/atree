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

const (
	mapSetOp1 = iota
	mapSetOp2
	mapSetOp3
	mapRemoveOp
	maxMapOp
)

type mapStatus struct {
	lock sync.RWMutex

	startTime time.Time

	count uint64 // number of elements in map

	setOps    uint64
	removeOps uint64
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

	return fmt.Sprintf("duration %s, heapAlloc %d MiB, %d elements, %d sets, %d removes",
		duration.Truncate(time.Second).String(),
		m.Alloc/1024/1024,
		status.count,
		status.setOps,
		status.removeOps,
	)
}

func (status *mapStatus) incSet(newValue bool) {
	status.lock.Lock()
	defer status.lock.Unlock()

	status.setOps++

	if newValue {
		status.count++
	}
}

func (status *mapStatus) incRemove() {
	status.lock.Lock()
	defer status.lock.Unlock()

	status.removeOps++
	status.count--
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

	// keys contains generated keys.  It is used to select random keys for removal.
	keys := make([]atree.Value, 0, flagMaxLength)

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

		nextOp := r.Intn(maxMapOp)

		if m.Count() == flagMaxLength || reduceHeapAllocs {
			nextOp = mapRemoveOp
		}

		switch nextOp {

		case mapSetOp1, mapSetOp2, mapSetOp3:
			opCount++

			expectedKey, key, err := randomKey()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate random key %s: %s", key, err)
				return
			}

			nestedLevels := r.Intn(maxNestedLevels)
			expectedValue, value, err := randomValue(storage, address, nestedLevels)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate random value %s: %s", value, err)
				return
			}

			oldExpectedValue, keyExist := expectedValues[expectedKey]

			// Update keys
			if !keyExist {
				keys = append(keys, expectedKey)
			}

			// Update expectedValues
			expectedValues[expectedKey] = expectedValue

			// Update map
			existingStorable, err := m.Set(compare, hashInputProvider, key, value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to set %s at index %d: %s", value, key, err)
				return
			}

			// Compare old value from map with old value from elements
			if (oldExpectedValue == nil) && (existingStorable != nil) {
				fmt.Fprintf(os.Stderr, "Set returned storable %s, want nil", existingStorable)
				return
			}

			if (oldExpectedValue != nil) && (existingStorable == nil) {
				fmt.Fprintf(os.Stderr, "Set returned nil, want %s", oldExpectedValue)
				return
			}

			if existingStorable != nil {

				existingValue, err := existingStorable.StoredValue(storage)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingStorable, err)
					return
				}

				err = valueEqual(oldExpectedValue, existingValue)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Set() returned wrong existing value %s, want %s", existingValue, oldExpectedValue)
					return
				}

				err = removeStorable(storage, existingStorable)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to remove map storable element %s: %s", existingStorable, err)
					return
				}
			}

			// Update status
			status.incSet(oldExpectedValue == nil)

		case mapRemoveOp:
			if m.Count() == 0 {
				continue
			}

			opCount++

			index := r.Intn(len(keys))
			key := keys[index]

			oldExpectedValue := expectedValues[key]

			// Update unexpectedValues
			delete(expectedValues, key)

			// Update keys
			copy(keys[index:], keys[index+1:])
			keys[len(keys)-1] = nil
			keys = keys[:len(keys)-1]

			// Update map
			existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, key)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove element with key %s: %s", key, err)
				return
			}

			// Compare removed key from map with removed key from elements
			existingKeyValue, err := existingKeyStorable.StoredValue(storage)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingKeyStorable, err)
				return
			}

			err = valueEqual(key, existingKeyValue)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Remove() returned wrong existing key %s, want %s", existingKeyStorable, key)
				return
			}

			// Compare removed value from map with removed value from elements
			existingValue, err := existingValueStorable.StoredValue(storage)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingValueStorable, err)
				return
			}

			err = valueEqual(oldExpectedValue, existingValue)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Remove() returned wrong existing value %s, want %s", existingValueStorable, oldExpectedValue)
				return
			}

			err = removeStorable(storage, existingKeyStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove key %s: %s", existingKeyStorable, err)
				return
			}

			err = removeStorable(storage, existingValueStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove value %s: %s", existingValueStorable, err)
				return
			}

			// Update status
			status.incRemove()
		}

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
