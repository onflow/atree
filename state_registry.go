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

// StateRegistry is the interface for managing shared state associated with slabs.
// Implementations may embed *BaseStateRegistry to inherit working defaults;
// atree's BasicSlabStorage and PersistentSlabStorage do so.
//
// NOTE: state must SURVIVE SlabStorage.Remove.
// atree internally calls Remove not only on container destruction
// but also when a child slab is inlined into its parent
// (ArrayDataSlab.Inline / MapDataSlab.Inline).
// In the inline case the container continues to exist logically (embedded in the parent),
// and its state must remain
// so future *Array / *OrderedMap instances for that container
// share the same canonical view as any pre-existing siblings.
// Implementations must NOT eagerly drop state in their Remove method.
//
// State entries therefore live for the lifetime of the storage.
// For per-transaction storage (the common case) this is bounded.
// Callers that want explicit cleanup can use RemoveStateForSlab on *BaseStateRegistry directly.
type StateRegistry interface {
	ArrayState(rootID SlabID) *ArrayState
	SetArrayState(rootID SlabID, state *ArrayState)
	OrderedMapState(rootID SlabID) *OrderedMapState
	SetOrderedMapState(rootID SlabID, state *OrderedMapState)
}

// BaseStateRegistry is an embeddable helper
// providing default implementations of the four state-registry methods on SlabStorage
// (ArrayState, SetArrayState, OrderedMapState, SetOrderedMapState).
//
// SlabStorage implementations should embed *BaseStateRegistry
// to get a working registry without writing the boilerplate themselves:
//
//	type MyStorage struct {
//	    *atree.BaseStateRegistry
//	    // other fields ...
//	}
//
//	func NewMyStorage() *MyStorage {
//	    return &MyStorage{BaseStateRegistry: atree.NewBaseStateRegistry()}
//	}
//
// Implementations must NOT eagerly drop state from their Remove(SlabID) method;
// see the note on StateRegistry for why.
// atree's own BasicSlabStorage and PersistentSlabStorage do not clear state on Remove, deliberately.
// Callers that want explicit cleanup
// (e.g. at the end of a long-lived storage's lifecycle)
// can call RemoveStateForSlab directly.
type BaseStateRegistry struct {
	arrayStates      map[SlabID]*ArrayState
	orderedMapStates map[SlabID]*OrderedMapState
}

var _ StateRegistry = &BaseStateRegistry{}

// NewBaseStateRegistry returns an empty registry.
// Registry maps are lazily initialized on first use,
// so a zero-valued *BaseStateRegistry also works;
// this constructor is provided for explicitness.
func NewBaseStateRegistry() *BaseStateRegistry {
	return &BaseStateRegistry{}
}

// ArrayState returns the registered *ArrayState for the given root slab ID,
// or nil if none.
func (r *BaseStateRegistry) ArrayState(rootID SlabID) *ArrayState {
	return r.arrayStates[rootID]
}

// SetArrayState registers state under the given root slab ID.
// Lazily initializes the underlying map.
func (r *BaseStateRegistry) SetArrayState(rootID SlabID, state *ArrayState) {
	if r.arrayStates == nil {
		r.arrayStates = make(map[SlabID]*ArrayState)
	}
	r.arrayStates[rootID] = state
}

// OrderedMapState returns the registered *OrderedMapState for the given root slab ID,
// or nil if none.
func (r *BaseStateRegistry) OrderedMapState(rootID SlabID) *OrderedMapState {
	return r.orderedMapStates[rootID]
}

// SetOrderedMapState registers state under the given root slab ID.
// Lazily initializes the underlying map.
func (r *BaseStateRegistry) SetOrderedMapState(rootID SlabID, state *OrderedMapState) {
	if r.orderedMapStates == nil {
		r.orderedMapStates = make(map[SlabID]*OrderedMapState)
	}
	r.orderedMapStates[rootID] = state
}

// RemoveStateForSlab clears any registered shared state under the given root slab ID.
// This is an escape hatch for callers that want to bound memory growth for a long-lived storage.
//
// SlabStorage implementations should NOT call this from Remove(SlabID):
// atree's Inline path uses storage.Remove for slabs whose containers remain logically alive
// (just inlined),
// so dropping their state there causes sibling-divergence bugs.
// See the note on StateRegistry.
func (r *BaseStateRegistry) RemoveStateForSlab(rootID SlabID) {
	delete(r.arrayStates, rootID)
	delete(r.orderedMapStates, rootID)
}
