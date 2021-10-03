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
	"os"
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

	return fmt.Sprintf("duration %s, %d elements, %d appends, %d sets, %d inserts, %d removes",
		duration.Truncate(time.Second).String(),
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

func testArray(storage *atree.PersistentSlabStorage, address atree.Address, typeInfo atree.TypeInfo, maxLength uint64, status *arrayStatus) {

	// Create new array
	array, err := atree.NewArray(storage, address, typeInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create new array: %s", err)
		return
	}

	// values contains array elements in the same order.  It is used to check data loss.
	values := make([]atree.Value, 0, maxLength)

	for {
		nextOp := rand.Intn(maxArrayOp)

		if array.Count() == maxLength {
			nextOp = arrayRemoveOp
		}

		switch nextOp {

		case arrayAppendOp:
			v := randomValue()

			// Append to values
			values = append(values, v)

			// Append to array
			err := array.Append(v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to append %s: %s", v, err)
				return
			}

			// Update status
			status.incAppend()

		case arraySetOp:
			if array.Count() == 0 {
				continue
			}

			k := rand.Intn(int(array.Count()))
			v := randomValue()

			oldV := values[k]

			// Update values
			values[k] = v

			// Update array
			existingStorable, err := array.Set(uint64(k), v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to set %s at index %d: %s", v, k, err)
				return
			}

			// Compare overwritten storable from array with overwritten value from values
			equal, err := compare(array.Storage, oldV, existingStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to compare %s and %s: %s", existingStorable, oldV, err)
				return
			}
			if !equal {
				fmt.Fprintf(os.Stderr, "Set() returned wrong existing value %s, want %s", existingStorable, oldV)
				return
			}

			// Delete overwritten element from storage
			if sid, ok := existingStorable.(atree.StorageIDStorable); ok {
				id := atree.StorageID(sid)
				err = storage.Remove(id)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to remove referenced slab %d: %s", id, err)
					return
				}
			}

			// Update status
			status.incSet()

		case arrayInsertOp:
			k := rand.Intn(int(array.Count() + 1))
			v := randomValue()

			// Update values
			if k == int(array.Count()) {
				values = append(values, v)
			} else {
				values = append(values, nil)
				copy(values[k+1:], values[k:])
				values[k] = v
			}

			// Update array
			err := array.Insert(uint64(k), v)
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

			k := rand.Intn(int(array.Count()))

			oldV := values[k]

			// Update values
			copy(values[k:], values[k+1:])
			values = values[:len(values)-1]

			// Update array
			existingStorable, err := array.Remove(uint64(k))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove element at index %d: %s", k, err)
				return
			}

			// Compare removed value from array with removed value from values
			equal, err := compare(array.Storage, oldV, existingStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to compare %s and %s: %s", existingStorable, oldV, err)
				return
			}
			if !equal {
				fmt.Fprintf(os.Stderr, "Remove() returned wrong existing value %s, want %s", existingStorable, oldV)
				return
			}

			// Delete removed element from storage
			if sid, ok := existingStorable.(atree.StorageIDStorable); ok {
				id := atree.StorageID(sid)
				err = storage.Remove(id)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to remove referenced slab %d: %s", id, err)
					return
				}
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

	}
}

func checkArrayDataLoss(array *atree.Array, values []atree.Value) error {

	// Check array has the same number of elements as values
	if array.Count() != uint64(len(values)) {
		return fmt.Errorf("Count() %d != len(values) %d", array.Count(), len(values))
	}

	// Check every element
	for i, v := range values {
		storable, err := array.Get(uint64(i))
		if err != nil {
			return fmt.Errorf("failed to get element at %d: %s", i, err)
		}
		equal, err := compare(array.Storage, v, storable)
		if err != nil {
			return fmt.Errorf("failed to compare %s and %s: %s", v, storable, err)
		}
		if !equal {
			return fmt.Errorf("Get(%d) returns %s, want %s", i, storable, v)
		}
	}

	return nil
}
