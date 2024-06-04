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
	"runtime"
	"sync"
	"time"

	"github.com/onflow/atree"
)

const (
	arrayAppendOp = iota
	arrayInsertOp
	arraySetOp
	arrayRemoveOp
	maxArrayOp
)

type arrayStatus struct {
	lock sync.RWMutex

	startTime time.Time

	count uint64 // number of elements in array

	appendOps uint64
	insertOps uint64
	setOps    uint64
	removeOps uint64
}

var _ Status = &arrayStatus{}

func newArrayStatus() *arrayStatus {
	return &arrayStatus{startTime: time.Now()}
}

func (status *arrayStatus) String() string {
	status.lock.RLock()
	defer status.lock.RUnlock()

	duration := time.Since(status.startTime)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return fmt.Sprintf("duration %s, heapAlloc %d MiB, %d elements, %d appends, %d sets, %d inserts, %d removes",
		duration.Truncate(time.Second).String(),
		m.Alloc/1024/1024,
		status.count,
		status.appendOps,
		status.setOps,
		status.insertOps,
		status.removeOps,
	)
}

func (status *arrayStatus) incAppend() {
	status.lock.Lock()
	defer status.lock.Unlock()

	status.appendOps++
	status.count++
}

func (status *arrayStatus) incSet() {
	status.lock.Lock()
	defer status.lock.Unlock()

	status.setOps++
}

func (status *arrayStatus) incInsert() {
	status.lock.Lock()
	defer status.lock.Unlock()

	status.insertOps++
	status.count++
}

func (status *arrayStatus) incRemove() {
	status.lock.Lock()
	defer status.lock.Unlock()

	status.removeOps++
	status.count--
}

func (status *arrayStatus) Write() {
	writeStatus(status.String())
}

func testArray(
	storage *atree.PersistentSlabStorage,
	address atree.Address,
	typeInfo atree.TypeInfo,
	maxLength uint64,
	status *arrayStatus,
	minHeapAllocMiB uint64,
	maxHeapAllocMiB uint64,
) {

	// Create new array
	array, err := atree.NewArray(storage, address, typeInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create new array: %s", err)
		return
	}

	// values contains array elements in the same order.  It is used to check data loss.
	values := make([]atree.Value, 0, maxLength)

	reduceHeapAllocs := false

	opCount := uint64(0)

	var m runtime.MemStats

	for {
		runtime.ReadMemStats(&m)
		allocMiB := m.Alloc / 1024 / 1024

		if !reduceHeapAllocs && allocMiB > maxHeapAllocMiB {
			fmt.Printf("\nHeapAlloc is %d MiB, removing elements to reduce allocs...\n", allocMiB)
			reduceHeapAllocs = true
		} else if reduceHeapAllocs && allocMiB < minHeapAllocMiB {
			fmt.Printf("\nHeapAlloc is %d MiB, resuming random operation...\n", allocMiB)
			reduceHeapAllocs = false
		}

		if reduceHeapAllocs && array.Count() == 0 {
			fmt.Printf("\nHeapAlloc is %d MiB while array is empty, drop read/write cache to free mem\n", allocMiB)
			reduceHeapAllocs = false

			// Commit slabs to storage and drop read and write to reduce mem
			err = storage.FastCommit(runtime.NumCPU())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to commit to storage: %s", err)
				return
			}

			storage.DropDeltas()
			storage.DropCache()

			// Load root slab from storage and cache it in read cache
			rootID := array.SlabID()
			array, err = atree.NewArrayWithRootID(storage, rootID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create array from root id %s: %s", rootID, err)
				return
			}

			runtime.GC()

			// Check if map is using > MaxHeapAlloc while empty.
			runtime.ReadMemStats(&m)
			allocMiB = m.Alloc / 1024 / 1024
			fmt.Printf("\nHeapAlloc is %d MiB after cleanup and forced gc\n", allocMiB)

			// Prevent infinite loop that doesn't do useful work.
			if allocMiB > maxHeapAllocMiB {
				// This shouldn't happen unless there's a memory leak.
				fmt.Fprintf(
					os.Stderr,
					"Exiting because allocMiB %d > maxMapHeapAlloMiB %d with empty map\n",
					allocMiB,
					maxHeapAllocMiB)
				return
			}
		}

		nextOp := r.Intn(maxArrayOp)

		if array.Count() == maxLength || reduceHeapAllocs {
			nextOp = arrayRemoveOp
		}

		switch nextOp {

		case arrayAppendOp:
			opCount++

			nestedLevels := r.Intn(maxNestedLevels)
			v, err := randomValue(storage, address, nestedLevels)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate random value %s: %s", v, err)
				return
			}

			copiedValue, err := copyValue(storage, atree.Address{}, v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to copy random value %s: %s", v, err)
				return
			}

			// Append to values
			values = append(values, copiedValue)

			// Append to array
			err = array.Append(v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to append %s: %s", v, err)
				return
			}

			// Update status
			status.incAppend()

		case arraySetOp:
			opCount++

			if array.Count() == 0 {
				continue
			}

			k := r.Intn(int(array.Count()))

			nestedLevels := r.Intn(maxNestedLevels)
			v, err := randomValue(storage, address, nestedLevels)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate random value %s: %s", v, err)
				return
			}

			copiedValue, err := copyValue(storage, atree.Address{}, v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to copy random value %s: %s", v, err)
				return
			}

			oldV := values[k]

			// Update values
			values[k] = copiedValue

			// Update array
			existingStorable, err := array.Set(uint64(k), v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to set %s at index %d: %s", v, k, err)
				return
			}

			// Compare overwritten value from array with overwritten value from values
			existingValue, err := existingStorable.StoredValue(storage)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingStorable, err)
				return
			}

			err = valueEqual(oldV, existingValue)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to compare %s and %s: %s", existingValue, oldV, err)
				return
			}

			// Delete overwritten element from storage
			err = removeStorable(storage, existingStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove storable %s: %s", existingStorable, err)
				return
			}

			err = removeValue(storage, oldV)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove copied overwritten value %s: %s", oldV, err)
				return
			}

			// Update status
			status.incSet()

		case arrayInsertOp:
			opCount++

			k := r.Intn(int(array.Count() + 1))

			nestedLevels := r.Intn(maxNestedLevels)
			v, err := randomValue(storage, address, nestedLevels)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate random value %s: %s", v, err)
				return
			}

			copiedValue, err := copyValue(storage, atree.Address{}, v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to copy random value %s: %s", v, err)
				return
			}

			// Update values
			if k == int(array.Count()) {
				values = append(values, copiedValue)
			} else {
				values = append(values, nil)
				copy(values[k+1:], values[k:])
				values[k] = copiedValue
			}

			// Update array
			err = array.Insert(uint64(k), v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to insert %s into index %d: %s", v, k, err)
				return
			}

			// Update status
			status.incInsert()

		case arrayRemoveOp:
			if array.Count() == 0 {
				continue
			}

			opCount++

			k := r.Intn(int(array.Count()))

			oldV := values[k]

			// Update values
			copy(values[k:], values[k+1:])
			values[len(values)-1] = nil
			values = values[:len(values)-1]

			// Update array
			existingStorable, err := array.Remove(uint64(k))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove element at index %d: %s", k, err)
				return
			}

			// Compare removed value from array with removed value from values
			existingValue, err := existingStorable.StoredValue(storage)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingStorable, err)
				return
			}

			err = valueEqual(oldV, existingValue)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to compare %s and %s: %s", existingValue, oldV, err)
				return
			}

			// Delete removed element from storage
			err = removeStorable(storage, existingStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove element %s: %s", existingStorable, err)
				return
			}

			err = removeValue(storage, oldV)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove copied removed value %s: %s", oldV, err)
				return
			}

			// Update status
			status.incRemove()
		}

		// Check array elements against values after every op
		err = checkArrayDataLoss(array, values)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}

		if opCount >= 100 {
			opCount = 0
			rootIDs, err := atree.CheckStorageHealth(storage, -1)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
			ids := make([]atree.SlabID, 0, len(rootIDs))
			for id := range rootIDs {
				// filter out root ids with empty address
				if !id.HasTempAddress() {
					ids = append(ids, id)
				}
			}
			if len(ids) != 1 || ids[0] != array.SlabID() {
				fmt.Fprintf(os.Stderr, "root slab ids %v in storage, want %s\n", ids, array.SlabID())
				return
			}
		}
	}
}

func checkArrayDataLoss(array *atree.Array, values []atree.Value) error {

	// Check array has the same number of elements as values
	if array.Count() != uint64(len(values)) {
		return fmt.Errorf("Count() %d != len(values) %d", array.Count(), len(values))
	}

	// Check every element
	for i, v := range values {
		convertedValue, err := array.Get(uint64(i))
		if err != nil {
			return fmt.Errorf("failed to get element at %d: %w", i, err)
		}
		err = valueEqual(v, convertedValue)
		if err != nil {
			return fmt.Errorf("failed to compare %s and %s: %w", v, convertedValue, err)
		}
	}

	return nil
}
