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

// orderedMapState holds the mutable state shared by all *OrderedMap instances
// over the same logical container.
// See arrayState for the general rationale;
// this is the symmetric type for maps.
//
// parentUpdater is intentionally NOT on the shared state
// (see arrayState for the same reasoning — set per-instance, may be a trap callback).
//
// digesterBuilder is per-instance
// because it is provided by the caller of NewMapWithRootID;
// siblings constructed with different builders (rare but legal) keep their own.
// The seed they configure is encoded in the map's ExtraData,
// so siblings using different builders still compute the same hkeys.
type orderedMapState struct {
	root MapSlab
}

// OrderedMapState is an opaque handle to atree's shared per-container state for an *OrderedMap.
// SlabStorage implementations store and return these values
// via the OrderedMapState / SetOrderedMapState methods on SlabStorage.
//
// External code should treat values of this type as opaque:
// they should not be constructed (its zero value is meaningless)
// or introspected (all fields are unexported).
// Pass them verbatim between SlabStorage method calls.
type OrderedMapState = orderedMapState

func newOrderedMapState(root MapSlab) *orderedMapState {
	return &orderedMapState{root: root}
}
