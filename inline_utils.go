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

// uninlineStorableIfNeeded uninlines given storable if needed, and
// returns uninlined Storable and its ValueID.
// If given storable is a WrapperStorable, this function uninlines
// wrapped storable if needed and returns a new WrapperStorable
// with wrapped uninlined storable and its ValidID.
func uninlineStorableIfNeeded(storage SlabStorage, storable Storable) (Storable, ValueID, bool, error) {
	if storable == nil {
		return storable, emptyValueID, false, nil
	}

	switch s := storable.(type) {
	case ArraySlab: // inlined array slab
		err := s.Uninline(storage)
		if err != nil {
			return nil, emptyValueID, false, err
		}

		slabID := s.SlabID()

		newStorable := SlabIDStorable(slabID)
		valueID := slabIDToValueID(slabID)

		return newStorable, valueID, true, nil

	case MapSlab: // inlined map slab
		err := s.Uninline(storage)
		if err != nil {
			return nil, emptyValueID, false, err
		}

		slabID := s.SlabID()

		newStorable := SlabIDStorable(slabID)
		valueID := slabIDToValueID(slabID)

		return newStorable, valueID, true, nil

	case SlabIDStorable: // uninlined slab
		valueID := slabIDToValueID(SlabID(s))

		return storable, valueID, false, nil

	case WrapperStorable:
		unwrappedStorable := unwrapStorable(s)

		// Uninline wrapped storable if needed.
		uninlinedWrappedStorable, valueID, uninlined, err := uninlineStorableIfNeeded(storage, unwrappedStorable)
		if err != nil {
			return nil, emptyValueID, false, err
		}

		if !uninlined {
			return storable, valueID, uninlined, nil
		}

		// Create a new WrapperStorable with uninlinedWrappedStorable
		newStorable := s.WrapAtreeStorable(uninlinedWrappedStorable)

		return newStorable, valueID, uninlined, nil
	}

	return storable, emptyValueID, false, nil
}
