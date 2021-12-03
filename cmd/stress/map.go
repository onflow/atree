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
			k, err := randomKey()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate random key %s: %s", k, err)
				return
			}

			nestedLevels := rand.Intn(maxNestedLevels)
			v, err := randomValue(storage, address, nestedLevels)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate random value %s: %s", v, err)
				return
			}

			copiedKey, err := copyValue(storage, atree.Address{}, k)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to copy random key %s: %s", k, err)
				return
			}

			copiedValue, err := copyValue(storage, atree.Address{}, v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to copy random value %s: %s", k, err)
				return
			}

			oldV := elements[copiedKey]

			// Update keys
			if oldV == nil {
				keys = append(keys, copiedKey)
			}

			// Update elements
			elements[copiedKey] = copiedValue

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

				existingValue, err := existingStorable.StoredValue(storage)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingStorable, err)
					return
				}

				err = valueEqual(oldV, existingValue)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Set() returned wrong existing value %s, want %s", existingValue, oldV)
					return
				}

				err = removeStorable(storage, existingStorable)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to remove map storable element %s: %s", existingStorable, err)
					return
				}

				err = removeValue(storage, oldV)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to remove copied overwritten value %s: %s", existingValue, err)
					return
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
			keys[len(keys)-1] = nil
			keys = keys[:len(keys)-1]

			// Update map
			existingKeyStorable, existingValueStorable, err := m.Remove(compare, hashInputProvider, k)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove element with key %s: %s", k, err)
				return
			}

			// Compare removed key from map with removed key from elements
			existingKeyValue, err := existingKeyStorable.StoredValue(storage)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingKeyStorable, err)
				return
			}

			err = valueEqual(k, existingKeyValue)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Remove() returned wrong existing key %s, want %s", existingKeyStorable, k)
				return
			}

			// Compare removed value from map with removed value from elements
			existingValue, err := existingValueStorable.StoredValue(storage)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to convert %s to value: %s", existingValueStorable, err)
				return
			}

			err = valueEqual(oldV, existingValue)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Remove() returned wrong existing value %s, want %s", existingValueStorable, oldV)
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

			err = removeValue(storage, k)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove copied key %s: %s", k, err)
				return
			}

			err = removeValue(storage, oldV)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove copied value %s: %s", existingValue, err)
				return
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
			return fmt.Errorf("failed to get element with key %s: %w", k, err)
		}
		convertedValue, err := storable.StoredValue(m.Storage)
		if err != nil {
			return fmt.Errorf("failed to convert storable to value with key %s: %w", k, err)
		}
		err = valueEqual(v, convertedValue)
		if err != nil {
			return fmt.Errorf("failed to compare %s and %s: %w", v, convertedValue, err)
		}
	}

	return nil
}
