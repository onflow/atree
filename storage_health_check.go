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

// CheckStorageHealth checks for the health of slab storage.
// It traverses the slabs and checks these factors:
// - All non-root slabs only has a single parent reference (no double referencing)
// - Every child of a parent shares the same ownership (childSlabID.Address == parentSlabID.Address)
// - The number of root slabs are equal to the expected number (skipped if expectedNumberOfRootSlabs is -1)
// This should be used for testing purposes only, as it might be slow to process
func CheckStorageHealth(storage SlabStorage, expectedNumberOfRootSlabs int) (map[SlabID]struct{}, error) {
	parentOf := make(map[SlabID]SlabID)
	leaves := make([]SlabID, 0)

	slabIterator, err := storage.SlabIterator()
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create slab iterator")
	}

	slabs := map[SlabID]Slab{}

	for {
		id, slab := slabIterator()
		if id == SlabIDUndefined {
			break
		}

		if _, ok := slabs[id]; ok {
			return nil, NewFatalError(fmt.Errorf("duplicate slab %s", id))
		}
		slabs[id] = slab

		atLeastOneExternalSlab := false
		childStorables := slab.ChildStorables()

		for len(childStorables) > 0 {

			var next []Storable

			for _, s := range childStorables {

				if sids, ok := s.(SlabIDStorable); ok {
					sid := SlabID(sids)
					if _, found := parentOf[sid]; found {
						return nil, NewFatalError(fmt.Errorf("two parents are captured for the slab %s", sid))
					}
					parentOf[sid] = id
					atLeastOneExternalSlab = true
				}

				// This handles inlined slab because inlined slab is a child storable (s) and
				// we traverse s.ChildStorables() for its inlined elements.
				next = append(next, s.ChildStorables()...)
			}

			childStorables = next
		}

		if !atLeastOneExternalSlab {
			leaves = append(leaves, id)
		}
	}

	rootsMap := make(map[SlabID]struct{})
	visited := make(map[SlabID]struct{})
	var id SlabID
	for _, leaf := range leaves {
		id = leaf
		if _, ok := visited[id]; ok {
			return nil, NewFatalError(fmt.Errorf("at least two references found to the leaf slab %s", id))
		}
		visited[id] = struct{}{}
		for {
			parentID, found := parentOf[id]
			if !found {
				// we reach the root
				rootsMap[id] = struct{}{}
				break
			}
			visited[parentID] = struct{}{}

			childSlab, ok, err := storage.Retrieve(id)
			if !ok {
				return nil, NewSlabNotFoundErrorf(id, "failed to get child slab")
			}
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve child slab %s", id))
			}

			parentSlab, ok, err := storage.Retrieve(parentID)
			if !ok {
				return nil, NewSlabNotFoundErrorf(id, "failed to get parent slab")
			}
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve parent slab %s", parentID))
			}

			childOwner := childSlab.SlabID().address
			parentOwner := parentSlab.SlabID().address

			if childOwner != parentOwner {
				return nil, NewFatalError(
					fmt.Errorf(
						"parent and child are not owned by the same account: child.owner %s, parent.owner %s",
						childOwner,
						parentOwner,
					))
			}
			id = parentID
		}
	}

	if len(visited) != len(slabs) {

		var unreachableID SlabID
		var unreachableSlab Slab

		for id, slab := range slabs {
			if _, ok := visited[id]; !ok {
				unreachableID = id
				unreachableSlab = slab
				break
			}
		}

		return nil, NewFatalError(
			fmt.Errorf(
				"slab was not reachable from leaves: %s: %s",
				unreachableID,
				unreachableSlab,
			))
	}

	if (expectedNumberOfRootSlabs >= 0) && (len(rootsMap) != expectedNumberOfRootSlabs) {
		return nil, NewFatalError(
			fmt.Errorf(
				"number of root slabs doesn't match: expected %d, got %d",
				expectedNumberOfRootSlabs,
				len(rootsMap),
			))
	}

	return rootsMap, nil
}
