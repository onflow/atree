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

type MapStats struct {
	Levels                 uint64
	ElementCount           uint64
	MetaDataSlabCount      uint64
	DataSlabCount          uint64
	CollisionDataSlabCount uint64
	StorableSlabCount      uint64
}

func (s *MapStats) SlabCount() uint64 {
	return s.DataSlabCount + s.MetaDataSlabCount + s.CollisionDataSlabCount + s.StorableSlabCount
}

// GetMapStats returns stats about the map slabs.
func GetMapStats(m *OrderedMap) (MapStats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	dataSlabCount := uint64(0)
	collisionDataSlabCount := uint64(0)
	storableDataSlabCount := uint64(0)

	nextLevelIDs := []SlabID{m.SlabID()}

	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getMapSlab().
				return MapStats{}, err
			}

			switch slab := slab.(type) {
			case *MapDataSlab:
				dataSlabCount++

				elementGroups := []elements{slab.elements}

				for len(elementGroups) > 0 {

					var nestedElementGroups []elements

					for _, group := range elementGroups {
						for i := 0; i < int(group.Count()); i++ {
							elem, err := group.Element(i)
							if err != nil {
								// Don't need to wrap error as external error because err is already categorized by elements.Element().
								return MapStats{}, err
							}

							switch e := elem.(type) {
							case elementGroup:
								nestedGroup := e

								if !nestedGroup.Inline() {
									collisionDataSlabCount++
								}

								nested, err := nestedGroup.Elements(m.Storage)
								if err != nil {
									// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
									return MapStats{}, err
								}

								nestedElementGroups = append(nestedElementGroups, nested)

							case *singleElement:
								if _, ok := e.key.(SlabIDStorable); ok {
									storableDataSlabCount++
								}
								if _, ok := e.value.(SlabIDStorable); ok {
									storableDataSlabCount++
								}
								// This handles use case of inlined array or map value containing SlabID
								ids := getSlabIDFromStorable(e.value, nil)
								storableDataSlabCount += uint64(len(ids))
							}
						}
					}

					elementGroups = nestedElementGroups
				}

			case *MapMetaDataSlab:
				metaDataSlabCount++

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(SlabIDStorable)
					if !ok {
						return MapStats{}, NewFatalError(fmt.Errorf("metadata slab's child storables are not of type SlabIDStorable"))
					}
					nextLevelIDs = append(nextLevelIDs, SlabID(id))
				}
			}
		}

		level++
	}

	return MapStats{
		Levels:                 level,
		ElementCount:           m.Count(),
		MetaDataSlabCount:      metaDataSlabCount,
		DataSlabCount:          dataSlabCount,
		CollisionDataSlabCount: collisionDataSlabCount,
		StorableSlabCount:      storableDataSlabCount,
	}, nil
}
