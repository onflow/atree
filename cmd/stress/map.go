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
	mapSetOp = iota
	mapRemoveOp
	maxMapOp
)

type mapStatus struct {
	lock sync.RWMutex

	startTime time.Time

	count uint64 // number of elements in array

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

	return fmt.Sprintf("duration %s, %d elements, %d sets, %d removes",
		duration.Truncate(time.Second).String(),
		status.count,
		status.setOps,
		status.removeOps,
	)
}

func (status *mapStatus) incSet(new bool) {
	status.lock.Lock()
	defer status.lock.Unlock()

	status.setOps++

	if new {
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

func testMap(storage *atree.PersistentSlabStorage, address atree.Address, typeInfo atree.TypeInfo, maxLength uint64, status *mapStatus) {

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create new map: %s", err)
		return
	}

	// elements contains generated keys and values. It is used to check data loss.
	elements := make(map[atree.Value]atree.Value, maxLength)

	// keys contains generated keys.  It is used to select random keys for removal.
	keys := make([]atree.Value, 0, maxLength)

	for {
		nextOp := rand.Intn(maxMapOp)

		if m.Count() == maxLength {
			nextOp = mapRemoveOp
		}

		switch nextOp {

		case mapSetOp:
			k := randomValue()
			v := randomValue()

			oldV := elements[k]

			// Update keys
			if oldV == nil {
				keys = append(keys, k)
			}

			// Update elements
			elements[k] = v

			// Update map
			existingStorable, err := m.Set(compare, hashInputProvider, k, v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to set %s at index %d: %s", v, k, err)
				return
			}

			// Compare old value from map with old value from elements
			if (oldV == nil) && (existingStorable != nil) {
				fmt.Fprintf(os.Stderr, "Set returned storable %s, want nil", existingStorable)
				return
			}

			if (oldV != nil) && (existingStorable == nil) {
				fmt.Fprintf(os.Stderr, "Set returned nil, want %s", oldV)
				return
			}

			if existingStorable != nil {
				equal, err := compare(m.Storage, oldV, existingStorable)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to compare %s and %s: %s", existingStorable, oldV, err)
					return
				}
				if !equal {
					fmt.Fprintf(os.Stderr, "Set() returned wrong existing value %s, want %s", existingStorable, oldV)
					return
				}

				// Delete overwritten element
				if sid, ok := existingStorable.(atree.StorageIDStorable); ok {
					id := atree.StorageID(sid)
					err = storage.Remove(id)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to remove referenced slab %d: %s", id, err)
						return
					}
				}
			}

			// Update status
			status.incSet(oldV == nil)

		case mapRemoveOp:
			if m.Count() == 0 {
				continue
			}

			index := rand.Intn(len(keys))
			k := keys[index]

			oldV := elements[k]

			// Update elements
			delete(elements, k)

			// Update keys
			copy(keys[index:], keys[index+1:])
			keys = keys[:len(keys)-1]

			// Update map
			existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, k)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove element with key %s: %s", k, err)
				return
			}

			// Compare removed key from map with removed key from elements
			equal, err := compare(m.Storage, k, existingKeyStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to compare %s and %s: %s", existingKeyStorable, k, err)
				return
			}
			if !equal {
				fmt.Fprintf(os.Stderr, "Remove() returned wrong existing key %s, want %s", existingKeyStorable, k)
				return
			}

			// Compare removed value from map with removed value from elements
			equal, err = compare(m.Storage, oldV, existingValueStorable)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to compare %s and %s: %s", existingValueStorable, oldV, err)
				return
			}
			if !equal {
				fmt.Fprintf(os.Stderr, "Remove() returned wrong existing value %s, want %s", existingValueStorable, oldV)
				return
			}

			// Delete removed key
			if sid, ok := existingKeyStorable.(atree.StorageIDStorable); ok {
				id := atree.StorageID(sid)
				err = storage.Remove(id)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to remove referenced slab %d: %s", id, err)
					return
				}
			}

			// Delete removed value
			if sid, ok := existingValueStorable.(atree.StorageIDStorable); ok {
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

		// Check map elements against elements after every op
		err = checkMapDataLoss(m, elements)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}

	}
}

func checkMapDataLoss(m *atree.OrderedMap, elements map[atree.Value]atree.Value) error {

	// Check map has the same number of elements as elements
	if m.Count() != uint64(len(elements)) {
		return fmt.Errorf("Count() %d != len(values) %d", m.Count(), len(elements))
	}

	// Check every element
	for k, v := range elements {
		storable, err := m.Get(compare, hashInputProvider, k)
		if err != nil {
			return fmt.Errorf("failed to get element with key %s: %s", k, err)
		}
		equal, err := compare(m.Storage, v, storable)
		if err != nil {
			return fmt.Errorf("failed to compare %s and %s: %s", v, storable, err)
		}
		if !equal {
			return fmt.Errorf("Get(%s) returns %s, want %s", k, storable, v)
		}
	}

	return nil
}
