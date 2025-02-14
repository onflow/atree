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

package atree

import (
	"fmt"
)

type ArrayIterator interface {
	CanMutate() bool
	Next() (Value, error)
}

// Empty array iterator

type emptyArrayIterator struct {
	readOnly bool
}

var _ ArrayIterator = &emptyArrayIterator{}

var emptyMutableArrayIterator = &emptyArrayIterator{readOnly: false}
var emptyReadOnlyArrayIterator = &emptyArrayIterator{readOnly: true}

func (i *emptyArrayIterator) CanMutate() bool {
	return !i.readOnly
}

func (*emptyArrayIterator) Next() (Value, error) {
	return nil, nil
}

// Mutable array iterator

type mutableArrayIterator struct {
	array     *Array
	nextIndex uint64
	lastIndex uint64 // noninclusive index
}

var _ ArrayIterator = &mutableArrayIterator{}

func (i *mutableArrayIterator) CanMutate() bool {
	return true
}

func (i *mutableArrayIterator) Next() (Value, error) {
	if i.nextIndex == i.lastIndex {
		// No more elements.
		return nil, nil
	}

	// Don't need to set up notification callback for v because
	// Get() returns value with notification already.
	v, err := i.array.Get(i.nextIndex)
	if err != nil {
		return nil, err
	}

	i.nextIndex++

	return v, nil
}

// Readonly array iterator

type ReadOnlyArrayIteratorMutationCallback func(mutatedValue Value)

type readOnlyArrayIterator struct {
	array                 *Array
	dataSlab              *ArrayDataSlab
	indexInDataSlab       uint64
	remainingCount        uint64 // needed for range iteration
	valueMutationCallback ReadOnlyArrayIteratorMutationCallback
}

// defaultReadOnlyArrayIteratorMutatinCallback is no-op.
var defaultReadOnlyArrayIteratorMutatinCallback ReadOnlyArrayIteratorMutationCallback = func(Value) {}

var _ ArrayIterator = &readOnlyArrayIterator{}

func (i *readOnlyArrayIterator) setMutationCallback(value Value) {

	unwrappedChild, _ := unwrapValue(value)

	if v, ok := unwrappedChild.(mutableValueNotifier); ok {
		v.setParentUpdater(func() (found bool, err error) {
			i.valueMutationCallback(value)
			return true, NewReadOnlyIteratorElementMutationError(i.array.ValueID(), v.ValueID())
		})
	}
}

func (i *readOnlyArrayIterator) CanMutate() bool {
	return false
}

func (i *readOnlyArrayIterator) Next() (Value, error) {
	if i.remainingCount == 0 {
		return nil, nil
	}

	if i.indexInDataSlab >= uint64(len(i.dataSlab.elements)) {
		// No more elements in current data slab.

		nextDataSlabID := i.dataSlab.next

		if nextDataSlabID == SlabIDUndefined {
			// No more elements in array.
			return nil, nil
		}

		// Load next data slab.
		slab, found, err := i.array.Storage.Retrieve(nextDataSlabID)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", nextDataSlabID))
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(nextDataSlabID, "slab not found during array iteration")
		}

		i.dataSlab = slab.(*ArrayDataSlab)
		i.indexInDataSlab = 0

		// Check current data slab isn't empty because i.remainingCount > 0.
		if len(i.dataSlab.elements) == 0 {
			return nil, NewSlabDataErrorf("data slab contains 0 elements, expect more")
		}
	}

	// At this point:
	// - There are elements to iterate in array (i.remainingCount > 0), and
	// - There are elements to iterate in i.dataSlab (i.indexInDataSlab < len(i.dataSlab.elements))

	element, err := i.dataSlab.elements[i.indexInDataSlab].StoredValue(i.array.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	i.indexInDataSlab++
	i.remainingCount--

	i.setMutationCallback(element)

	return element, nil
}

// Array loaded value iterator

type arrayLoadedElementIterator struct {
	storage SlabStorage
	slab    *ArrayDataSlab
	index   int
}

func (i *arrayLoadedElementIterator) next() (Value, error) {
	// Iterate loaded elements in data slab.
	for i.index < len(i.slab.elements) {
		element := i.slab.elements[i.index]
		i.index++

		v, err := getLoadedValue(i.storage, element)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getLoadedValue.
			return nil, err
		}
		if v == nil {
			// Skip this element because it references unloaded slab.
			// Try next element.
			continue
		}

		return v, nil
	}

	// Reach end of elements
	return nil, nil
}

type arrayLoadedSlabIterator struct {
	storage SlabStorage
	slab    *ArrayMetaDataSlab
	index   int
}

func (i *arrayLoadedSlabIterator) next() Slab {
	// Iterate loaded slabs in meta data slab.
	for i.index < len(i.slab.childrenHeaders) {
		header := i.slab.childrenHeaders[i.index]
		i.index++

		childSlab := i.storage.RetrieveIfLoaded(header.slabID)
		if childSlab == nil {
			// Skip this child because it references unloaded slab.
			// Try next child.
			continue
		}

		return childSlab
	}

	// Reach end of children.
	return nil
}

// ArrayLoadedValueIterator is used to iterate over loaded array elements.
type ArrayLoadedValueIterator struct {
	storage      SlabStorage
	parents      []*arrayLoadedSlabIterator // LIFO stack for parents of dataIterator
	dataIterator *arrayLoadedElementIterator
}

func (i *ArrayLoadedValueIterator) nextDataIterator() (*arrayLoadedElementIterator, error) {

	// Iterate parents (LIFO) to find next loaded array data slab.
	for len(i.parents) > 0 {
		lastParent := i.parents[len(i.parents)-1]

		nextChildSlab := lastParent.next()

		switch slab := nextChildSlab.(type) {
		case *ArrayDataSlab:
			// Create data iterator
			return &arrayLoadedElementIterator{
				storage: i.storage,
				slab:    slab,
			}, nil

		case *ArrayMetaDataSlab:
			// Push new parent to parents queue
			newParent := &arrayLoadedSlabIterator{
				storage: i.storage,
				slab:    slab,
			}
			i.parents = append(i.parents, newParent)

		case nil:
			// Reach end of last parent.
			// Reset last parent to nil and pop last parent from parents stack.
			lastParentIndex := len(i.parents) - 1
			i.parents[lastParentIndex] = nil
			i.parents = i.parents[:lastParentIndex]

		default:
			return nil, NewSlabDataErrorf("slab %s isn't ArraySlab", nextChildSlab.SlabID())
		}
	}

	// Reach end of parents stack.
	return nil, nil
}

// Next iterates and returns next loaded element.
// It returns nil Value at end of loaded elements.
func (i *ArrayLoadedValueIterator) Next() (Value, error) {
	if i.dataIterator != nil {
		element, err := i.dataIterator.next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by arrayLoadedElementIterator.next().
			return nil, err
		}
		if element != nil {
			return element, nil
		}

		// Reach end of element in current data slab.
		i.dataIterator = nil
	}

	// Get next data iterator.
	var err error
	i.dataIterator, err = i.nextDataIterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by arrayLoadedValueIterator.nextDataIterator().
		return nil, err
	}
	if i.dataIterator != nil {
		return i.Next()
	}

	// Reach end of loaded value iterator
	return nil, nil
}

// Iterate functions

type ArrayIterationFunc func(element Value) (resume bool, err error)

func iterateArray(iterator ArrayIterator, fn ArrayIterationFunc) error {
	for {
		value, err := iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayIterator.Next().
			return err
		}
		if value == nil {
			return nil
		}
		resume, err := fn(value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ArrayIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}
