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

type MapSlabHeader struct {
	slabID   SlabID // id is used to retrieve slab from storage
	size     uint32 // size is used to split and merge; leaf: size of all element; internal: size of all headers
	firstKey Digest // firstKey (first hashed key) is used to lookup value
}

type MapSlab interface {
	Slab

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

	Set(
		storage SlabStorage,
		b DigesterBuilder,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		hip HashInputProvider,
		key Value,
		value Value,
	) (MapKey, MapValue, error)

	Remove(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetSlabID(SlabID)

	Header() MapSlabHeader

	ExtraData() *MapExtraData
	RemoveExtraData() *MapExtraData
	SetExtraData(*MapExtraData)

	PopIterate(SlabStorage, MapPopIterationFunc) error

	Inlined() bool
	Inlinable(maxInlineSize uint64) bool
	Inline(SlabStorage) error
	Uninline(SlabStorage) error
}

func getMapSlab(storage SlabStorage, id SlabID) (MapSlab, error) {
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "map slab not found")
	}
	mapSlab, ok := slab.(MapSlab)
	if !ok {
		return nil, NewSlabDataErrorf("slab %s isn't MapSlab", id)
	}
	return mapSlab, nil
}

func firstMapDataSlab(storage SlabStorage, slab MapSlab) (*MapDataSlab, error) {
	switch slab := slab.(type) {
	case *MapDataSlab:
		return slab, nil

	case *MapMetaDataSlab:
		firstChildID := slab.childrenHeaders[0].slabID
		firstChild, err := getMapSlab(storage, firstChildID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return nil, err
		}
		// Don't need to wrap error as external error because err is already categorized by firstMapDataSlab().
		return firstMapDataSlab(storage, firstChild)

	default:
		return nil, NewUnreachableError()
	}
}
