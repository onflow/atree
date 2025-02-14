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
	"strings"
)

// PrintArray prints array slab data to stdout.
func PrintArray(a *Array) {
	dumps, err := DumpArraySlabs(a)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(strings.Join(dumps, "\n"))
}

func DumpArraySlabs(a *Array) ([]string, error) {
	var dumps []string

	nextLevelIDs := []SlabID{a.SlabID()}

	var overflowIDs []SlabID

	level := 0
	for len(nextLevelIDs) > 0 {

		ids := nextLevelIDs

		nextLevelIDs = []SlabID(nil)

		for _, id := range ids {

			slab, err := getArraySlab(a.Storage, id)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getArraySlab().
				return nil, err
			}

			switch slab := slab.(type) {
			case *ArrayDataSlab:
				dumps = append(dumps, fmt.Sprintf("level %d, %s", level+1, slab))

				overflowIDs = getSlabIDFromStorable(slab, overflowIDs)

			case *ArrayMetaDataSlab:
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

	for _, id := range overflowIDs {
		slab, found, err := a.Storage.Retrieve(id)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
		}
		if !found {
			return nil, NewSlabNotFoundErrorf(id, "slab not found during array slab dump")
		}
		dumps = append(dumps, slab.String())
	}

	return dumps, nil
}
