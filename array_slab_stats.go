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

type ArrayStats struct {
	Levels            uint64
	ElementCount      uint64
	MetaDataSlabCount uint64
	DataSlabCount     uint64
	StorableSlabCount uint64
}

func (s *ArrayStats) SlabCount() uint64 {
	return s.DataSlabCount + s.MetaDataSlabCount + s.StorableSlabCount
}

// GetArrayStats returns stats about array slabs.
func GetArrayStats(a *Array) (ArrayStats, error) {
	level := uint64(0)
	metaDataSlabCount := uint64(0)
	dataSlabCount := uint64(0)
	storableSlabCount := uint64(0)

	nextLevelIDs := []SlabID{a.SlabID()}

	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getArraySlab(a.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getArraySlab().
				return ArrayStats{}, err
			}

			switch slab.(type) {
			case *ArrayDataSlab:
				dataSlabCount++

				ids := getSlabIDFromStorable(slab, nil)
				storableSlabCount += uint64(len(ids))

			case *ArrayMetaDataSlab:
				metaDataSlabCount++

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(SlabIDStorable)
					if !ok {
						return ArrayStats{}, NewFatalError(fmt.Errorf("metadata slab's child storables are not of type SlabIDStorable"))
					}
					nextLevelIDs = append(nextLevelIDs, SlabID(id))
				}
			}
		}

		level++

	}

	return ArrayStats{
		Levels:            level,
		ElementCount:      a.Count(),
		MetaDataSlabCount: metaDataSlabCount,
		DataSlabCount:     dataSlabCount,
		StorableSlabCount: storableSlabCount,
	}, nil
}
