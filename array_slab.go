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

type ArraySlabHeader struct {
	slabID SlabID // id is used to retrieve slab from storage
	size   uint32 // size is used to split and merge; leaf: size of all element; internal: size of all headers
	count  uint32 // count is used to lookup element; leaf: number of elements; internal: number of elements in all its headers
}

type ArraySlab interface {
	Slab

	Get(storage SlabStorage, index uint64) (Storable, error)
	Set(storage SlabStorage, address Address, index uint64, value Value) (Storable, error)
	Insert(storage SlabStorage, address Address, index uint64, value Value) error
	Remove(storage SlabStorage, index uint64) (Storable, error)

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetSlabID(SlabID)

	Header() ArraySlabHeader

	ExtraData() *ArrayExtraData
	RemoveExtraData() *ArrayExtraData
	SetExtraData(*ArrayExtraData)

	PopIterate(SlabStorage, ArrayPopIterationFunc) error

	Inlined() bool
	Inlinable(maxInlineSize uint32) bool
	Inline(SlabStorage) error
	Uninline(SlabStorage) error
}

func getArraySlab(storage SlabStorage, id SlabID) (ArraySlab, error) {
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		// err can be an external error because storage is an interface.
		return nil, wrapErrorAsExternalErrorIfNeeded(err)
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "array slab not found")
	}
	arraySlab, ok := slab.(ArraySlab)
	if !ok {
		return nil, NewSlabDataErrorf("slab %s isn't ArraySlab", id)
	}
	return arraySlab, nil
}

func firstArrayDataSlab(storage SlabStorage, slab ArraySlab) (*ArrayDataSlab, error) {
	switch slab := slab.(type) {
	case *ArrayDataSlab:
		return slab, nil

	case *ArrayMetaDataSlab:
		firstChildID := slab.childrenHeaders[0].slabID
		firstChild, err := getArraySlab(storage, firstChildID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return nil, err
		}
		// Don't need to wrap error as external error because err is already categorized by firstArrayDataSlab().
		return firstArrayDataSlab(storage, firstChild)

	default:
		return nil, NewUnreachableError()
	}
}

// getArrayDataSlabWithIndex returns data slab containing element at specified index
func getArrayDataSlabWithIndex(storage SlabStorage, slab ArraySlab, index uint64) (*ArrayDataSlab, uint64, error) {
	if slab.IsData() {
		dataSlab := slab.(*ArrayDataSlab)
		if index >= uint64(len(dataSlab.elements)) {
			return nil, 0, NewIndexOutOfBoundsError(index, 0, uint64(len(dataSlab.elements)))
		}
		return dataSlab, index, nil
	}

	metaSlab := slab.(*ArrayMetaDataSlab)
	_, adjustedIndex, childID, err := metaSlab.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, 0, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, 0, err
	}

	// Don't need to wrap error as external error because err is already categorized by getArrayDataSlabWithIndex().
	return getArrayDataSlabWithIndex(storage, child, adjustedIndex)
}
