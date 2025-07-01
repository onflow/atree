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
	"errors"
	"fmt"
	"slices"
	"strings"
)

func PrintMap(m *OrderedMap) {
	dumps, err := DumpMapSlabs(m)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(strings.Join(dumps, "\n"))
}

func DumpMapSlabs(m *OrderedMap) ([]string, error) {
	var dumps []string

	nextLevelIDs := []SlabID{m.SlabID()}

	var overflowIDs []SlabID
	var collisionSlabIDs []SlabID

	level := 0
	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getMapSlab(m.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getMapSlab().
				return nil, err
			}

			switch slab := slab.(type) {
			case *MapDataSlab:
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, slab))

				for i := 0; i < int(slab.elements.Count()); i++ {
					elem, err := slab.elements.Element(i)
					if err != nil {
						// Don't need to wrap error as external error because err is already categorized by elements.Element().
						return nil, err
					}
					if group, ok := elem.(elementGroup); ok {
						if !group.Inline() {
							extSlab := group.(*externalCollisionGroup)
							collisionSlabIDs = append(collisionSlabIDs, extSlab.slabID)
						}
					}
				}

				overflowIDs = getSlabIDFromStorable(slab, overflowIDs)

			case *MapMetaDataSlab:
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, slab))

				for _, storable := range slab.ChildStorables() {
					id, ok := storable.(SlabIDStorable)
					if !ok {
						return nil, NewFatalError(errors.New("metadata slab's child storables are not of type SlabIDStorable"))
					}
					nextLevelIDs = append(nextLevelIDs, SlabID(id))
				}
			}
		}

		level++
	}

	for _, id := range collisionSlabIDs {
		slab, err := getMapSlab(m.Storage, id)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return nil, err
		}
		dumps = append(dumps, fmt.Sprintf("collision: %s", slab.String()))
	}

	// overflowIDs include collisionSlabIDs
	for _, id := range overflowIDs {
		if slices.Contains(collisionSlabIDs, id) {
			continue
		}
		slab, found, err := m.Storage.Retrieve(id)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(id, "slab not found during map slab dump")
		}
		dumps = append(dumps, slab.String())
	}

	return dumps, nil
}
