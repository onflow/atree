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

type MapKey Storable

type MapValue Storable

// element is one indivisible unit that must stay together (e.g. collision group)
type element interface {
	fmt.Stringer

	getElementAndNextKey(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, MapKey, error)

	Get(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

	// Set returns updated element, which may be a different type of element because of hash collision.
	Set(
		storage SlabStorage,
		address Address,
		b DigesterBuilder,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		hip HashInputProvider,
		key Value,
		value Value,
	) (newElem element, keyStorable MapKey, existingMapValueStorable MapValue, err error)

	// Remove returns matched key, value, and updated element.
	// Updated element may be nil, modified, or a different type of element.
	Remove(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, element, error)

	Encode(*Encoder) error

	hasPointer() bool

	Size() uint32

	Count(storage SlabStorage) (uint32, error)

	PopIterate(SlabStorage, MapPopIterationFunc) error

	Iterate(SlabStorage, func(key MapKey, value MapValue) error) error
}

// elementGroup is a group of elements that must stay together during splitting or rebalancing.
type elementGroup interface {
	element

	Inline() bool

	// Elements returns underlying elements.
	Elements(storage SlabStorage) (elements, error)
}

// singleElement

type singleElement struct {
	key   MapKey
	value MapValue
	size  uint32
}

var _ element = &singleElement{}

func newSingleElement(storage SlabStorage, address Address, key Value, value Value) (*singleElement, error) {

	ks, err := key.Storable(storage, address, maxInlineMapKeySize)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's storable")
	}

	vs, err := value.Storable(storage, address, maxInlineMapValueSize(ks.ByteSize()))
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
	}

	return &singleElement{
		key:   ks,
		value: vs,
		size:  singleElementPrefixSize + ks.ByteSize() + vs.ByteSize(),
	}, nil
}

func (e *singleElement) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	k, v, err := e.Get(storage, digester, level, hkey, comparator, key)

	nextKey := MapKey(nil)
	return k, v, nextKey, err
}

func (e *singleElement) Get(storage SlabStorage, _ Digester, _ uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}
	if equal {
		return e.key, e.value, nil
	}
	return nil, nil, NewKeyNotFoundError(key)
}

// Set updates value if key matches, otherwise returns inlineCollisionGroup with existing and new elements.
// NOTE: Existing key needs to be rehashed because we store minimum digest for non-collision element.
//
//	Rehashing only happens when we create new inlineCollisionGroup.
//	Adding new element to existing inlineCollisionGroup doesn't require rehashing.
func (e *singleElement) Set(
	storage SlabStorage,
	address Address,
	b DigesterBuilder,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (element, MapKey, MapValue, error) {

	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}

	// Key matches, overwrite existing value
	if equal {
		existingMapValueStorable := e.value

		valueStorable, err := value.Storable(storage, address, maxInlineMapValueSize(e.key.ByteSize()))
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Value interface.
			return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
		}

		e.value = valueStorable
		e.size = singleElementPrefixSize + e.key.ByteSize() + e.value.ByteSize()
		return e, e.key, existingMapValueStorable, nil
	}

	// Hash collision detected

	// Create collision group with existing and new elements

	if level+1 == digester.Levels() {

		// Create singleElements group
		group := &inlineCollisionGroup{
			elements: newSingleElementsWithElement(level+1, e),
		}

		// Add new key and value to collision group
		// Don't need to wrap error as external error because err is already categorized by inlineCollisionGroup.Set().
		return group.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)

	}

	// Generate digest for existing key (see function comment)
	kv, err := e.key.StoredValue(storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's stored value")
	}

	existingKeyDigest, err := b.Digest(hip, kv)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigestBuilder interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's digester")
	}
	defer putDigester(existingKeyDigest)

	d, err := existingKeyDigest.Digest(level + 1)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digester interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get key's digest at level %d", level+1))
	}

	group := &inlineCollisionGroup{
		elements: newHkeyElementsWithElement(level+1, d, e),
	}

	// Add new key and value to collision group
	// Don't need to wrap error as external error because err is already categorized by inlineCollisionGroup.Set().
	return group.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
}

// Remove returns key, value, and nil element if key matches, otherwise returns error.
func (e *singleElement) Remove(storage SlabStorage, _ Digester, _ uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, element, error) {

	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}

	if equal {
		return e.key, e.value, nil, nil
	}

	return nil, nil, nil, NewKeyNotFoundError(key)
}

func (e *singleElement) hasPointer() bool {
	return hasPointer(e.key) || hasPointer(e.value)
}

func (e *singleElement) Size() uint32 {
	return e.size
}

func (e *singleElement) Count(_ SlabStorage) (uint32, error) {
	return 1, nil
}

func (e *singleElement) PopIterate(_ SlabStorage, fn MapPopIterationFunc) error {
	fn(e.key, e.value)
	return nil
}

func (e *singleElement) Iterate(_ SlabStorage, fn func(key MapKey, value MapValue) error) error {
	err := fn(e.key, e.value)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by fn callback.
		return wrapErrorAsExternalErrorIfNeeded(err)
	}
	return nil
}

func (e *singleElement) String() string {
	return fmt.Sprintf("%s:%s", e.key, e.value)
}

// inlined collision group

type inlineCollisionGroup struct {
	elements
}

var _ element = &inlineCollisionGroup{}
var _ elementGroup = &inlineCollisionGroup{}

func (e *inlineCollisionGroup) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {

	// Adjust level and hkey for collision group.
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey.
	// Don't need to wrap error as external error because err is already categorized by elements.Get().
	return e.elements.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
}

func (e *inlineCollisionGroup) Get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey
	// Don't need to wrap error as external error because err is already categorized by elements.Get().
	return e.elements.Get(storage, digester, level, hkey, comparator, key)
}

func (e *inlineCollisionGroup) Set(
	storage SlabStorage,
	address Address,
	b DigesterBuilder,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (element, MapKey, MapValue, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	keyStorable, existingMapValueStorable, err := e.elements.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Set().
		return nil, nil, nil, err
	}

	if level == 1 {
		// Export oversized inline collision group to separate slab (external collision group)
		// for first level collision.
		if e.Size() > maxInlineMapElementSize {

			id, err := storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(
					err,
					fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			// Create MapDataSlab
			slab := &MapDataSlab{
				header: MapSlabHeader{
					slabID:   id,
					size:     mapDataSlabPrefixSize + e.elements.Size(),
					firstKey: e.firstKey(),
				},
				elements:       e.elements, // elems shouldn't be copied
				anySize:        true,
				collisionGroup: true,
			}

			err = storeSlab(storage, slab)
			if err != nil {
				return nil, nil, nil, err
			}

			// Create and return externalCollisionGroup (wrapper of newly created MapDataSlab)
			return &externalCollisionGroup{
				slabID: id,
				size:   externalCollisionGroupPrefixSize + SlabIDStorable(id).ByteSize(),
			}, keyStorable, existingMapValueStorable, nil
		}
	}

	return e, keyStorable, existingMapValueStorable, nil
}

// Remove returns key, value, and updated element if key is found.
// Updated element can be modified inlineCollisionGroup, or singleElement.
func (e *inlineCollisionGroup) Remove(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, element, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	k, v, err := e.elements.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Remove().
		return nil, nil, nil, err
	}

	// If there is only one single element in this group, return the single element (no collision).
	if e.elements.Count() == 1 {
		elem, err := e.Element(0)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elements.Element().
			return nil, nil, nil, err
		}
		if _, ok := elem.(elementGroup); !ok {
			return k, v, elem, nil
		}
	}

	return k, v, e, nil
}

func (e *inlineCollisionGroup) hasPointer() bool {
	return e.elements.hasPointer()
}

func (e *inlineCollisionGroup) Size() uint32 {
	return inlineCollisionGroupPrefixSize + e.elements.Size()
}

func (e *inlineCollisionGroup) Inline() bool {
	return true
}

func (e *inlineCollisionGroup) Elements(_ SlabStorage) (elements, error) {
	return e.elements, nil
}

func (e *inlineCollisionGroup) Count(_ SlabStorage) (uint32, error) {
	return e.elements.Count(), nil
}

func (e *inlineCollisionGroup) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {
	// Don't need to wrap error as external error because err is already categorized by elements.PopIterate().
	return e.elements.PopIterate(storage, fn)
}

func (e *inlineCollisionGroup) Iterate(storage SlabStorage, fn func(key MapKey, value MapValue) error) error {
	// Don't need to wrap error as external error because err is already categorized by elements.Iterate().
	return e.elements.Iterate(storage, fn)
}

func (e *inlineCollisionGroup) String() string {
	return "inline[" + e.elements.String() + "]"
}

// External collision group

type externalCollisionGroup struct {
	slabID SlabID
	size   uint32
}

var _ element = &externalCollisionGroup{}
var _ elementGroup = &externalCollisionGroup{}

func (e *externalCollisionGroup) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, nil, err
	}

	// Adjust level and hkey for collision group.
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey.
	// Don't need to wrap error as external error because err is already categorized by MapSlab.getElementAndNextKey().
	return slab.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
}

func (e *externalCollisionGroup) Get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, err
	}

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey
	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return slab.Get(storage, digester, level, hkey, comparator, key)
}

func (e *externalCollisionGroup) Set(
	storage SlabStorage,
	_ Address,
	b DigesterBuilder,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (element, MapKey, MapValue, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, nil, err
	}

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	keyStorable, existingMapValueStorable, err := slab.Set(storage, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, nil, nil, err
	}
	return e, keyStorable, existingMapValueStorable, nil
}

// Remove returns key, value, and updated element if key is found.
// Updated element can be modified externalCollisionGroup, or singleElement.
// TODO: updated element can be inlineCollisionGroup if size < maxInlineMapElementSize.
func (e *externalCollisionGroup) Remove(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, element, error) {

	slab, found, err := storage.Retrieve(e.slabID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", e.slabID))
	}
	if !found {
		return nil, nil, nil, NewSlabNotFoundErrorf(e.slabID, "external collision slab not found")
	}

	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return nil, nil, nil, NewSlabDataErrorf("slab %s isn't MapDataSlab", e.slabID)
	}

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	k, v, err := dataSlab.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapDataSlab.Remove().
		return nil, nil, nil, err
	}

	// TODO: if element size < maxInlineMapElementSize, return inlineCollisionGroup

	// If there is only one single element in this group, return the single element and remove external slab from storage.
	if dataSlab.Count() == 1 {
		elem, err := dataSlab.Element(0)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elements.Element().
			return nil, nil, nil, err
		}
		if _, ok := elem.(elementGroup); !ok {
			err := storage.Remove(e.slabID)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", e.slabID))
			}
			return k, v, elem, nil
		}
	}

	return k, v, e, nil
}

func (e *externalCollisionGroup) hasPointer() bool {
	return true
}

func (e *externalCollisionGroup) Size() uint32 {
	return e.size
}

func (e *externalCollisionGroup) Inline() bool {
	return false
}

func (e *externalCollisionGroup) Elements(storage SlabStorage) (elements, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}
	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return nil, NewSlabDataErrorf("slab %s isn't MapDataSlab", e.slabID)
	}
	return dataSlab.elements, nil
}

func (e *externalCollisionGroup) Count(storage SlabStorage) (uint32, error) {
	elements, err := e.Elements(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by externalCollisionGroup.Elements().
		return 0, err
	}
	return elements.Count(), nil
}

func (e *externalCollisionGroup) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {
	elements, err := e.Elements(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by externalCollisionGroup.Elements().
		return err
	}

	err = elements.PopIterate(storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.PopIterate().
		return err
	}

	err = storage.Remove(e.slabID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", e.slabID))
	}
	return nil
}

func (e *externalCollisionGroup) Iterate(storage SlabStorage, fn func(key MapKey, value MapValue) error) error {
	elements, err := e.Elements(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by externalCollisionGroup.Elements().
		return err
	}

	err = elements.Iterate(storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Iterate().
		return err
	}

	return nil
}

func (e *externalCollisionGroup) String() string {
	return fmt.Sprintf("external(%s)", e.slabID)
}
