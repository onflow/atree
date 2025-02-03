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

// Exported functions of PersistentSlabStorage for testing.
var (
	GetBaseStorage = (*PersistentSlabStorage).getBaseStorage
	GetCache       = (*PersistentSlabStorage).getCache
	GetDeltas      = (*PersistentSlabStorage).getDeltas
	GetCBOREncMode = (*PersistentSlabStorage).getCBOREncMode
	GetCBORDecMode = (*PersistentSlabStorage).getCBORDecMode
)

// Exported function of slab size settings for testing.
var (
	TargetSlabSize = targetSlabSize
)

// Exported function of Array for testing.
var (
	GetArrayRootSlab                 = (*Array).rootSlab
	ArrayHasParentUpdater            = (*Array).hasParentUpdater
	GetArrayMutableElementIndexCount = (*Array).getMutableElementIndexCount
)

// Exported function of OrderedMap for testing.
var (
	GetMapRootSlab        = (*OrderedMap).rootSlab
	GetMapDigesterBuilder = (*OrderedMap).getDigesterBuilder
)

// Exported function of MapDataSlab for testing.
var (
	IsMapDataSlabCollisionGroup = (*MapDataSlab).isCollisionGroup
	GetMapDataSlabElementCount  = (*MapDataSlab).elementCount
)

func NewArrayRootDataSlab(id SlabID, storables []Storable) ArraySlab {
	size := uint32(arrayRootDataSlabPrefixSize)

	for _, storable := range storables {
		size += storable.ByteSize()
	}

	return &ArrayDataSlab{
		header: ArraySlabHeader{
			slabID: id,
			size:   size,
			count:  uint32(len(storables)),
		},
		elements: storables,
	}
}

func GetArrayMetaDataSlabChildInfo(metaDataSlab *ArrayMetaDataSlab) (childSlabIDs []SlabID, childCounts []uint32) {
	childSlabIDs = make([]SlabID, len(metaDataSlab.childrenHeaders))
	childCounts = make([]uint32, len(metaDataSlab.childrenHeaders))

	for i, childHeader := range metaDataSlab.childrenHeaders {
		childSlabIDs[i] = childHeader.slabID
		childCounts[i] = childHeader.count
	}

	return childSlabIDs, childCounts
}

func GetMapMetaDataSlabChildInfo(metaDataSlab *MapMetaDataSlab) (childSlabIDs []SlabID, childSizes []uint32, childFirstKeys []Digest) {
	childSlabIDs = make([]SlabID, len(metaDataSlab.childrenHeaders))
	childSizes = make([]uint32, len(metaDataSlab.childrenHeaders))
	childFirstKeys = make([]Digest, len(metaDataSlab.childrenHeaders))

	for i, childHeader := range metaDataSlab.childrenHeaders {
		childSlabIDs[i] = childHeader.slabID
		childSizes[i] = childHeader.size
		childFirstKeys[i] = childHeader.firstKey
	}

	return childSlabIDs, childSizes, childFirstKeys
}

func GetMutableValueNotifierValueID(v Value) (ValueID, error) {
	m, ok := v.(mutableValueNotifier)
	if !ok {
		return ValueID{}, fmt.Errorf("v (%T) isn't mutableValueNotifier", v)
	}
	return m.ValueID(), nil
}
