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

// arrayState holds the mutable state shared by all *Array instances
// over the same logical container (same root SlabID / ValueID).
//
// Multiple *Array Go instances can exist for the same logical container
// when a caller obtains a handle through different paths
// (e.g., two Get calls on the same parent slot,
// or constructing via NewArrayWithRootID while another instance already exists).
// All such instances point at the same *arrayState,
// so structural changes
// (splitRoot, promoteChildAsNewRoot, root replacement on merge, etc.)
// performed through any one instance are observed by all the others.
// This is what eliminates the "stale sibling root" hazard
// that previously required workarounds at the Cadence layer.
//
// Lifetime:
// states are registered on the SlabStorage by SlabID.
// State entries live for the lifetime of the storage;
// see StateRegistry for why they must survive SlabStorage.Remove.
// Live *Array instances pointing to a state keep the state alive via the Go pointer;
// the storage registry holds a strong reference until the storage is dropped.
//
// parentUpdater is intentionally NOT on the shared state.
// It is set per *Array instance based on HOW that instance was obtained:
// a regular Get from a parent installs a real parent-notification callback;
// a readonly iterator installs a "trap callback" that errors on mutation.
// Two siblings over the same slab can therefore legitimately have different parentUpdaters,
// and a mutation must invoke only the originating instance's callback.
type arrayState struct {
	root ArraySlab

	// mutableElementIndex tracks index of mutable element, such as Array and OrderedMap.
	// This is needed by mutable element to properly update itself through parentUpdater.
	// WARNING: since mutableElementIndex is created lazily, we need to create mutableElementIndex
	// if it is nil before adding/updating elements.  Range, delete, and read are no-ops on nil Go map.
	// TODO: maybe optimize by replacing map to get faster updates.
	mutableElementIndex map[ValueID]uint64
}

// ArrayState is an opaque handle to atree's shared per-container state for an *Array.
// SlabStorage implementations store and return these values
// via the ArrayState / SetArrayState methods on SlabStorage.
//
// External code should treat values of this type as opaque:
// they should not be constructed (its zero value is meaningless)
// or introspected (all fields are unexported).
// Pass them verbatim between SlabStorage method calls.
type ArrayState = arrayState

func newArrayState(root ArraySlab) *arrayState {
	return &arrayState{root: root}
}
