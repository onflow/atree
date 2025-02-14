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

import "fmt"

type MapIterator interface {
	CanMutate() bool
	Next() (Value, Value, error)
	NextKey() (Value, error)
	NextValue() (Value, error)
}

// Empty map iterator

type emptyMapIterator struct {
	readOnly bool
}

var _ MapIterator = &emptyMapIterator{}

var emptyMutableMapIterator = &emptyMapIterator{readOnly: false}
var emptyReadOnlyMapIterator = &emptyMapIterator{readOnly: true}

func (i *emptyMapIterator) CanMutate() bool {
	return !i.readOnly
}

func (*emptyMapIterator) Next() (Value, Value, error) {
	return nil, nil, nil
}

func (*emptyMapIterator) NextKey() (Value, error) {
	return nil, nil
}

func (*emptyMapIterator) NextValue() (Value, error) {
	return nil, nil
}

// Mutable map iterator

type mutableMapIterator struct {
	m          *OrderedMap
	comparator ValueComparator
	hip        HashInputProvider
	nextKey    Value
}

var _ MapIterator = &mutableMapIterator{}

func (i *mutableMapIterator) CanMutate() bool {
	return true
}

func (i *mutableMapIterator) Next() (Value, Value, error) {
	if i.nextKey == nil {
		// No more elements.
		return nil, nil, nil
	}

	// Don't need to set up notification callback for v because
	// getElementAndNextKey() returns value with notification already.
	k, v, nk, err := i.m.getElementAndNextKey(i.comparator, i.hip, i.nextKey)
	if err != nil {
		return nil, nil, err
	}

	i.nextKey = nk

	return k, v, nil
}

func (i *mutableMapIterator) NextKey() (Value, error) {
	if i.nextKey == nil {
		// No more elements.
		return nil, nil
	}

	key := i.nextKey

	nk, err := i.m.getNextKey(i.comparator, i.hip, key)
	if err != nil {
		return nil, err
	}

	i.nextKey = nk

	return key, nil
}

func (i *mutableMapIterator) NextValue() (Value, error) {
	if i.nextKey == nil {
		// No more elements.
		return nil, nil
	}

	// Don't need to set up notification callback for v because
	// getElementAndNextKey() returns value with notification already.
	_, v, nk, err := i.m.getElementAndNextKey(i.comparator, i.hip, i.nextKey)
	if err != nil {
		return nil, err
	}

	i.nextKey = nk

	return v, nil
}

// Map readonly iterator

type ReadOnlyMapIteratorMutationCallback func(mutatedValue Value)

type readOnlyMapIterator struct {
	m                     *OrderedMap
	nextDataSlabID        SlabID
	elemIterator          *mapElementIterator
	keyMutationCallback   ReadOnlyMapIteratorMutationCallback
	valueMutationCallback ReadOnlyMapIteratorMutationCallback
}

// defaultReadOnlyMapIteratorMutatinCallback is no-op.
var defaultReadOnlyMapIteratorMutatinCallback ReadOnlyMapIteratorMutationCallback = func(Value) {}

var _ MapIterator = &readOnlyMapIterator{}

func (i *readOnlyMapIterator) setMutationCallback(key, value Value) {

	unwrappedKey, _ := unwrapValue(key)

	if k, ok := unwrappedKey.(mutableValueNotifier); ok {
		k.setParentUpdater(func() (found bool, err error) {
			i.keyMutationCallback(key)
			return true, NewReadOnlyIteratorElementMutationError(i.m.ValueID(), k.ValueID())
		})
	}

	unwrappedValue, _ := unwrapValue(value)

	if v, ok := unwrappedValue.(mutableValueNotifier); ok {
		v.setParentUpdater(func() (found bool, err error) {
			i.valueMutationCallback(value)
			return true, NewReadOnlyIteratorElementMutationError(i.m.ValueID(), v.ValueID())
		})
	}
}

func (i *readOnlyMapIterator) Next() (key Value, value Value, err error) {
	if i.elemIterator == nil {
		if i.nextDataSlabID == SlabIDUndefined {
			return nil, nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, nil, err
		}
	}

	var ks, vs Storable
	ks, vs, err = i.elemIterator.next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, nil, err
	}
	if ks != nil {
		key, err = ks.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map key's stored value")
		}

		value, err = vs.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map value's stored value")
		}

		i.setMutationCallback(key, value)

		return key, value, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.Next().
	return i.Next()
}

func (i *readOnlyMapIterator) NextKey() (key Value, err error) {
	if i.elemIterator == nil {
		if i.nextDataSlabID == SlabIDUndefined {
			return nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, err
		}
	}

	var ks Storable
	ks, _, err = i.elemIterator.next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, err
	}
	if ks != nil {
		key, err = ks.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map key's stored value")
		}

		i.setMutationCallback(key, nil)

		return key, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.NextKey().
	return i.NextKey()
}

func (i *readOnlyMapIterator) NextValue() (value Value, err error) {
	if i.elemIterator == nil {
		if i.nextDataSlabID == SlabIDUndefined {
			return nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, err
		}
	}

	var vs Storable
	_, vs, err = i.elemIterator.next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, err
	}
	if vs != nil {
		value, err = vs.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map value's stored value")
		}

		i.setMutationCallback(nil, value)

		return value, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.NextValue().
	return i.NextValue()
}

func (i *readOnlyMapIterator) advance() error {
	slab, found, err := i.m.Storage.Retrieve(i.nextDataSlabID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", i.nextDataSlabID))
	}
	if !found {
		return NewSlabNotFoundErrorf(i.nextDataSlabID, "slab not found during map iteration")
	}

	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return NewSlabDataErrorf("slab %s isn't MapDataSlab", i.nextDataSlabID)
	}

	i.nextDataSlabID = dataSlab.next

	i.elemIterator = &mapElementIterator{
		storage:  i.m.Storage,
		elements: dataSlab.elements,
	}

	return nil
}

func (i *readOnlyMapIterator) CanMutate() bool {
	return false
}

type mapElementIterator struct {
	storage        SlabStorage
	elements       elements
	index          int
	nestedIterator *mapElementIterator
}

func (i *mapElementIterator) next() (key MapKey, value MapValue, err error) {

	if i.nestedIterator != nil {
		key, value, err = i.nestedIterator.next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by mapElementIterator.next().
			return nil, nil, err
		}
		if key != nil {
			return key, value, nil
		}
		i.nestedIterator = nil
	}

	if i.index >= int(i.elements.Count()) {
		return nil, nil, nil
	}

	e, err := i.elements.Element(i.index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Element().
		return nil, nil, err
	}

	switch elm := e.(type) {
	case *singleElement:
		i.index++
		return elm.key, elm.value, nil

	case elementGroup:
		elems, err := elm.Elements(i.storage)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
			return nil, nil, err
		}

		i.nestedIterator = &mapElementIterator{
			storage:  i.storage,
			elements: elems,
		}

		i.index++
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return i.nestedIterator.next()

	default:
		return nil, nil, NewSlabDataError(fmt.Errorf("unexpected element type %T during map iteration", e))
	}
}

// Map loaded value iterator

type mapLoadedElementIterator struct {
	storage                SlabStorage
	elements               elements
	index                  int
	collisionGroupIterator *mapLoadedElementIterator
}

func (i *mapLoadedElementIterator) next() (key Value, value Value, err error) {
	// Iterate loaded elements in data slab (including elements in collision groups).
	for i.index < int(i.elements.Count()) || i.collisionGroupIterator != nil {

		// Iterate elements in collision group.
		if i.collisionGroupIterator != nil {
			key, value, err = i.collisionGroupIterator.next()
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by mapLoadedElementIterator.next().
				return nil, nil, err
			}
			if key != nil {
				return key, value, nil
			}

			// Reach end of collision group.
			i.collisionGroupIterator = nil
			continue
		}

		element, err := i.elements.Element(i.index)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elements.Element().
			return nil, nil, err
		}

		i.index++

		switch e := element.(type) {
		case *singleElement:

			keyValue, err := getLoadedValue(i.storage, e.key)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getLoadedValue.
				return nil, nil, err
			}
			if keyValue == nil {
				// Skip this element because element key references unloaded slab.
				// Try next element.
				continue
			}

			valueValue, err := getLoadedValue(i.storage, e.value)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getLoadedValue.
				return nil, nil, err
			}
			if valueValue == nil {
				// Skip this element because element value references unloaded slab.
				// Try next element.
				continue
			}

			return keyValue, valueValue, nil

		case *inlineCollisionGroup:
			elems, err := e.Elements(i.storage)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
				return nil, nil, err
			}

			i.collisionGroupIterator = &mapLoadedElementIterator{
				storage:  i.storage,
				elements: elems,
			}

			// Continue to iterate elements in collision group using collisionGroupIterator.
			continue

		case *externalCollisionGroup:
			externalSlab := i.storage.RetrieveIfLoaded(e.slabID)
			if externalSlab == nil {
				// Skip this collsion group because external slab isn't loaded.
				// Try next element.
				continue
			}

			dataSlab, ok := externalSlab.(*MapDataSlab)
			if !ok {
				return nil, nil, NewSlabDataErrorf("slab %s isn't MapDataSlab", e.slabID)
			}

			i.collisionGroupIterator = &mapLoadedElementIterator{
				storage:  i.storage,
				elements: dataSlab.elements,
			}

			// Continue to iterate elements in collision group using collisionGroupIterator.
			continue

		default:
			return nil, nil, NewSlabDataError(fmt.Errorf("unexpected element type %T during map iteration", element))
		}
	}

	// Reach end of map data slab.
	return nil, nil, nil
}

type mapLoadedSlabIterator struct {
	storage SlabStorage
	slab    *MapMetaDataSlab
	index   int
}

func (i *mapLoadedSlabIterator) next() Slab {
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

// MapLoadedValueIterator is used to iterate loaded map elements.
type MapLoadedValueIterator struct {
	storage      SlabStorage
	parents      []*mapLoadedSlabIterator // LIFO stack for parents of dataIterator
	dataIterator *mapLoadedElementIterator
}

func (i *MapLoadedValueIterator) nextDataIterator() (*mapLoadedElementIterator, error) {

	// Iterate parents (LIFO) to find next loaded map data slab.
	for len(i.parents) > 0 {
		lastParent := i.parents[len(i.parents)-1]

		nextChildSlab := lastParent.next()

		switch slab := nextChildSlab.(type) {
		case *MapDataSlab:
			// Create data iterator
			return &mapLoadedElementIterator{
				storage:  i.storage,
				elements: slab.elements,
			}, nil

		case *MapMetaDataSlab:
			// Push new parent to parents queue
			newParent := &mapLoadedSlabIterator{
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
			return nil, NewSlabDataErrorf("slab %s isn't MapSlab", nextChildSlab.SlabID())
		}
	}

	// Reach end of parents stack.
	return nil, nil
}

// Next iterates and returns next loaded element.
// It returns nil Value at end of loaded elements.
func (i *MapLoadedValueIterator) Next() (Value, Value, error) {
	if i.dataIterator != nil {
		key, value, err := i.dataIterator.next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by mapLoadedElementIterator.next().
			return nil, nil, err
		}
		if key != nil {
			return key, value, nil
		}

		// Reach end of element in current data slab.
		i.dataIterator = nil
	}

	// Get next data iterator.
	var err error
	i.dataIterator, err = i.nextDataIterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapLoadedValueIterator.nextDataIterator().
		return nil, nil, err
	}
	if i.dataIterator != nil {
		return i.Next()
	}

	// Reach end of loaded value iterator
	return nil, nil, nil
}

// Iterate functions

type MapEntryIterationFunc func(Value, Value) (resume bool, err error)

func iterateMap(iterator MapIterator, fn MapEntryIterationFunc) error {
	var err error
	var key, value Value
	for {
		key, value, err = iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.Next().
			return err
		}
		if key == nil {
			return nil
		}
		resume, err := fn(key, value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapEntryIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}

type MapElementIterationFunc func(Value) (resume bool, err error)

func iterateMapKeys(iterator MapIterator, fn MapElementIterationFunc) error {
	var err error
	var key Value
	for {
		key, err = iterator.NextKey()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.NextKey().
			return err
		}
		if key == nil {
			return nil
		}
		resume, err := fn(key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapElementIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}

func iterateMapValues(iterator MapIterator, fn MapElementIterationFunc) error {
	var err error
	var value Value
	for {
		value, err = iterator.NextValue()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.NextValue().
			return err
		}
		if value == nil {
			return nil
		}
		resume, err := fn(value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapElementIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}
