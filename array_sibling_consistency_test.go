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

package atree_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"
	testutils "github.com/onflow/atree/test_utils"
)

// TestArraySiblingConsistencyAfterSplitRoot verifies that
// two *Array instances obtained for the same inlined inner array
// observe the same canonical state
// after a structural change (splitRoot) initiated through one of them.
//
// Without shared state,
// sibling[1] would retain a pointer to the pre-split root slab
// whose own SlabID has been reassigned by splitRoot —
// sibling[1].Count() would return a child-slab count
// rather than the canonical post-split count.
func TestArraySiblingConsistencyAfterSplitRoot(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	// Outer array holding one inner array. Both stored at the same address.
	outer, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)
	require.NoError(t, inner.Append(testutils.NewUint64ValueFromInteger(0)))

	require.NoError(t, outer.Append(inner))

	// Obtain two sibling *Array instances for the same inner container by
	// calling outer.Get(0) twice. Pre-refactor these would have been two
	// distinct *atree.Array Go objects with their own root pointers.
	a, err := outer.Get(0)
	require.NoError(t, err)
	sibling1 := a.(*atree.Array)

	a, err = outer.Get(0)
	require.NoError(t, err)
	sibling2 := a.(*atree.Array)

	// Sanity: same ValueID, single-slab to start.
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.IsWithinSingleSlab(),
		"initial inner array must be in a single slab")

	// Mutate through sibling1 enough to force a slab split.
	// Threshold is 256 bytes;
	// appending 200 uint64s definitely exceeds it.
	const appendCount = 200
	for i := uint64(0); i < appendCount; i++ {
		require.NoError(t, sibling1.Append(testutils.NewUint64ValueFromInteger(int(i))))
	}

	// Confirm splitRoot actually fired:
	// root is now a MetaDataSlab, not a DataSlab.
	// Without this assertion the test could pass trivially
	// if atree's sizing kept the data in one slab
	// (in which case there'd be no sibling-divergence opportunity to test).
	require.False(t, sibling1.IsWithinSingleSlab(),
		"splitRoot must have fired during the appends")

	// Both siblings must observe the post-split state.
	require.Equal(t, uint64(1+appendCount), sibling1.Count(),
		"sibling1 (mutated) must see appended count")
	require.Equal(t, uint64(1+appendCount), sibling2.Count(),
		"sibling2 (untouched) must see post-split count through shared state")

	// Mutate through sibling2; sibling1 must see it.
	require.NoError(t, sibling2.Append(testutils.NewUint64ValueFromInteger(9999)))
	require.Equal(t, uint64(2+appendCount), sibling1.Count(),
		"sibling1 must observe sibling2's append through shared state")
	require.Equal(t, uint64(2+appendCount), sibling2.Count())

	// Spot-check element accessibility through both siblings.
	last1, err := sibling1.Get(sibling1.Count() - 1)
	require.NoError(t, err)
	last2, err := sibling2.Get(sibling2.Count() - 1)
	require.NoError(t, err)
	require.Equal(t, testutils.NewUint64ValueFromInteger(9999), last1)
	require.Equal(t, testutils.NewUint64ValueFromInteger(9999), last2)
}

// TestArraySiblingConsistencyAfterPromoteRoot exercises the dual of the split case:
// enough removals to trigger promoteChildAsNewRoot.
// The previous Cadence-side staleness check
// (cached valueID vs live ValueID())
// did NOT detect this case
// because promoteChildAsNewRoot keeps the root slab ID stable on `a`;
// sibling instances would retain a pointer to the orphaned old root struct.
// With shared state, both siblings observe the new root via state.root.
func TestArraySiblingConsistencyAfterPromoteRoot(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Populate enough elements to require multi-slab structure.
	const initialCount = 200
	for i := uint64(0); i < initialCount; i++ {
		require.NoError(t, inner.Append(testutils.NewUint64ValueFromInteger(int(i))))
	}

	require.NoError(t, outer.Append(inner))

	// Two sibling instances.
	a, err := outer.Get(0)
	require.NoError(t, err)
	sibling1 := a.(*atree.Array)

	a, err = outer.Get(0)
	require.NoError(t, err)
	sibling2 := a.(*atree.Array)

	// Sanity: the 200-element inner must be multi-slab after population.
	// If this fails, the test setup didn't create a structure that can undergo promote.
	require.False(t, sibling1.IsWithinSingleSlab(),
		"populated inner array must span multiple slabs")

	// Remove enough elements through sibling1 to trigger root promotion
	// (when meta slab shrinks to one child,
	// atree promotes the child to be the new root).
	for sibling1.Count() > 1 {
		_, err := sibling1.Remove(sibling1.Count() - 1)
		require.NoError(t, err)
	}

	// Confirm promoteChildAsNewRoot actually fired:
	// root is back to a DataSlab.
	// The only path from MetaDataSlab back to DataSlab under removals is promote,
	// so this assertion proves the structural op happened during the shrink.
	require.True(t, sibling1.IsWithinSingleSlab(),
		"promoteChildAsNewRoot must have fired during the removals")

	require.Equal(t, uint64(1), sibling1.Count())
	require.Equal(t, uint64(1), sibling2.Count(),
		"sibling2 must observe post-promote count through shared state")

	// The remaining element must be readable through both siblings.
	v1, err := sibling1.Get(0)
	require.NoError(t, err)
	v2, err := sibling2.Get(0)
	require.NoError(t, err)
	require.Equal(t, v1, v2)
}

// TestArraySiblingConsistencyAcrossSplitAndPromote exercises a sequence
// that drives both structural operations through different siblings:
//   - grow via sibling1 until splitRoot fires
//   - shrink via sibling2 until promoteChildAsNewRoot fires
//
// Each transition must leave both siblings observing the same live canonical state.
// Without shared state,
// the second sibling's first touch after either transition would silently use a stale root.
func TestArraySiblingConsistencyAcrossSplitAndPromote(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)
	require.NoError(t, inner.Append(testutils.NewUint64ValueFromInteger(0)))

	require.NoError(t, outer.Append(inner))

	a, err := outer.Get(0)
	require.NoError(t, err)
	sibling1 := a.(*atree.Array)

	a, err = outer.Get(0)
	require.NoError(t, err)
	sibling2 := a.(*atree.Array)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.IsWithinSingleSlab(),
		"initial inner array must be in a single slab")

	// Grow through sibling1 → splitRoot.
	const grow = 200
	for i := uint64(0); i < grow; i++ {
		require.NoError(t, sibling1.Append(testutils.NewUint64ValueFromInteger(int(i))))
	}
	require.False(t, sibling1.IsWithinSingleSlab(),
		"splitRoot must have fired during the grow phase")
	require.Equal(t, uint64(1+grow), sibling2.Count(),
		"sibling2 must see post-split count")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID(),
		"split must preserve matching ValueIDs through shared state")

	// Shrink through sibling2 → promoteChildAsNewRoot.
	for sibling2.Count() > 1 {
		_, err := sibling2.Remove(sibling2.Count() - 1)
		require.NoError(t, err)
	}
	require.True(t, sibling1.IsWithinSingleSlab(),
		"promoteChildAsNewRoot must have fired during the shrink phase")
	require.Equal(t, uint64(1), sibling1.Count(),
		"sibling1 must see post-promote count")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID(),
		"promote must preserve matching ValueIDs through shared state")

	// Final cross-check: append via sibling1, observe via sibling2.
	require.NoError(t, sibling1.Append(testutils.NewUint64ValueFromInteger(42)))
	require.Equal(t, uint64(2), sibling2.Count())
	v, err := sibling2.Get(1)
	require.NoError(t, err)
	require.Equal(t, testutils.NewUint64ValueFromInteger(42), v)
}

// TestArraySiblingTestStructuralAssertionsAreMeaningful is a meta-test
// that validates the structural assertions used in the sibling tests above
// (`require.False(sibling.IsWithinSingleSlab(), ...)`) actually do real work.
// The risk we're guarding against:
// a future atree change
// that increases the slab threshold or shrinks element encodings
// could silently make the sibling tests' 200-element grow loop fit in a single slab —
// splitRoot never fires,
// no sibling divergence is possible,
// and the consistency assertions pass trivially.
//
// This test runs the same grow pattern under two thresholds:
//   - 256 bytes (what the sibling tests use): split must fire
//   - 16 KiB (large): split must NOT fire
//
// If either case behaves wrong, this test fails —
// and the sibling tests' structural assertions are confirmed to be a real guard.
func TestArraySiblingTestStructuralAssertionsAreMeaningful(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	growAndCheck := func(threshold uint32) (singleSlabAtEnd bool) {
		atree.SetThreshold(threshold)
		defer atree.SetThreshold(1024)

		storage := newTestPersistentStorage(t)
		arr, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const grow = 200
		for i := uint64(0); i < grow; i++ {
			require.NoError(t, arr.Append(testutils.NewUint64ValueFromInteger(int(i))))
		}
		return arr.IsWithinSingleSlab()
	}

	require.False(t, growAndCheck(256),
		"with threshold=256 (what the sibling tests use), 200 uint64s "+
			"must overflow a single slab and force splitRoot — if this "+
			"passes (i.e. returns single-slab), the sibling tests' structural "+
			"assertions would pass trivially without a real split")

	require.True(t, growAndCheck(16*1024),
		"with threshold=16KiB, 200 uint64s must fit in a single slab — "+
			"if this fails, our 'no-split' baseline doesn't hold and the "+
			"meta-test can't distinguish the two configurations")
}

// TestArraySiblingConsistencyAcrossInlineTransition guards against a subtle state-lifetime hazard:
// when an uninlined child container shrinks enough to fit inside its parent slab,
// atree calls ArrayDataSlab.Inline,
// which internally calls storage.Remove(slabID) to remove the child slab from storage
// (its data now lives embedded in the parent).
//
// The container itself is NOT destroyed —
// it continues to exist inlined.
// But if SlabStorage.Remove eagerly drops the shared state registry entry,
// every live sibling *Array pointing at that state
// would silently lose canonical state on the next structural change.
//
// The test forces the inline transition while holding sibling instances,
// then verifies they continue to observe consistent state.
func TestArraySiblingConsistencyAcrossInlineTransition(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Grow inner past the inline threshold BEFORE attaching to outer,
	// so that when it's added to outer
	// it remains uninlined.
	const growSize = 100
	for i := uint64(0); i < growSize; i++ {
		require.NoError(t, inner.Append(testutils.NewUint64ValueFromInteger(int(i))))
	}

	require.NoError(t, outer.Append(inner))

	// Two sibling instances of the inner, while it's uninlined.
	a, err := outer.Get(0)
	require.NoError(t, err)
	sibling1 := a.(*atree.Array)

	a, err = outer.Get(0)
	require.NoError(t, err)
	sibling2 := a.(*atree.Array)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.False(t, sibling1.Inlined())

	// Shrink inner through sibling1 until atree re-inlines it.
	// This triggers ArrayDataSlab.Inline which calls storage.Remove on the inner's slab ID —
	// exactly the path that would drop our registry entry if we cleaned up state on Remove.
	for sibling1.Count() > 0 && !sibling1.Inlined() {
		_, err := sibling1.Remove(sibling1.Count() - 1)
		require.NoError(t, err)
	}
	require.True(t, sibling1.Inlined(),
		"inner must be inlined after the shrink so the test exercises the Inline path")

	// Sibling2 must observe the post-inline state.
	// If state was dropped,
	// sibling2 still holds a pointer to the old state struct,
	// and a fresh Get on outer would build a new state —
	// siblings would diverge.
	require.Equal(t, sibling1.Count(), sibling2.Count(),
		"sibling2 must observe sibling1's removals across the inline transition")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling2.Inlined(),
		"sibling2 must see the inlined state through shared state")

	// Cross-check: append through sibling2, observe through sibling1.
	require.NoError(t, sibling2.Append(testutils.NewUint64ValueFromInteger(42)))
	require.Equal(t, sibling2.Count(), sibling1.Count(),
		"sibling1 must observe sibling2's append")

	// Critical assertion:
	// a FRESH load via outer.Get(0) after the inline transition
	// must return a *Array sharing the SAME state as the pre-inline siblings —
	// not a freshly-allocated state.
	//
	// If storage.Remove (triggered by Inline) dropped the registry entry,
	// this Get would create a new *arrayState and register it.
	// The fresh state would initially point at the same slab struct,
	// but the moment a structural change happens through any instance,
	// the two states diverge silently.
	a, err = outer.Get(0)
	require.NoError(t, err)
	sibling3 := a.(*atree.Array)

	// Trigger a structural change through sibling3:
	// grow it back to multi-slab, forcing splitRoot.
	// With a shared state, sibling1's view updates too.
	// With dropped state, sibling1 keeps reading from a stale root.
	for i := uint64(0); i < 200; i++ {
		require.NoError(t, sibling3.Append(testutils.NewUint64ValueFromInteger(int(i))))
	}
	require.False(t, sibling3.IsWithinSingleSlab(),
		"sibling3 must have triggered splitRoot during the regrowth")
	require.Equal(t, sibling3.Count(), sibling1.Count(),
		"sibling1 must see sibling3's post-split count via shared state — "+
			"if this fails, the inline transition dropped the registry "+
			"entry and sibling3 got an independent state")
	require.Equal(t, sibling1.ValueID(), sibling3.ValueID())
}

// TestArrayBatchBuildWithDistinctInlinedMaps verifies that constructing
// an array from three distinct inlined OrderedMaps produces an array
// whose elements retain their original distinguishing entries.
//
// Scenario (mirrors Cadence's `NewArrayValue([struct1, struct2, struct3])`
// which Transfers each value via `CopyNonRefSimple` before adding to the array):
//   - Construct three *OrderedMap values m1, m2, m3,
//     each with a single distinguishing entry.
//   - Copy each via CopyNonRefSimple to simulate Cadence's transfer step.
//   - Build a new *Array via NewArrayFromBatchData,
//     supplying the copies as its elements.
//
// All values must live in a single storage:
// atree's shared-state design assumes SlabIDs are unique within a storage,
// but each storage has its own monotonic SlabID counter starting from zero,
// so mixing values across multiple storages can produce SlabID collisions
// that the shared-state registry cannot disambiguate.
func TestArrayBatchBuildWithDistinctInlinedMaps(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestBasicStorage(t)
	var address atree.Address

	const mapCount = 3
	copies := make([]*atree.OrderedMap, mapCount)
	for i := range copies {
		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		prev, err := m.Set(
			testutils.CompareValue, testutils.GetHashInput,
			testutils.NewUint64ValueFromInteger(0),
			testutils.NewUint64ValueFromInteger(i+1),
		)
		require.NoError(t, err)
		require.Nil(t, prev)

		copied, err := m.CopyNonRefSimple(address, atree.NewDefaultDigesterBuilder())
		require.NoError(t, err)

		copies[i] = copied
	}

	idx := 0
	arr, err := atree.NewArrayFromBatchData(
		storage,
		address,
		typeInfo,
		func() (atree.Value, error) {
			if idx >= mapCount {
				return nil, nil
			}
			m := copies[idx]
			idx++
			return m, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(mapCount), arr.Count())

	// Iterate the array and verify each element retained its distinct content.
	iter, err := arr.ReadOnlyIterator()
	require.NoError(t, err)

	for i := 0; i < mapCount; i++ {
		v, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, v, "iterator must produce an element at index %d", i)

		gotMap, ok := v.(*atree.OrderedMap)
		require.True(t, ok, "element %d must be an *OrderedMap", i)

		gotValue, err := gotMap.Get(
			testutils.CompareValue, testutils.GetHashInput,
			testutils.NewUint64ValueFromInteger(0),
		)
		require.NoError(t, err)

		expected := testutils.NewUint64ValueFromInteger(i + 1)
		require.Equal(t, expected, gotValue,
			"element %d must retain its distinguishing entry", i)
	}
}

// TestArraySiblingConsistencyAfterPopIterate verifies that
// PopIterate, which replaces state.root with a brand-new empty *ArrayDataSlab,
// propagates that replacement to every sibling Go handle.
//
// PopIterate is the only operation that swaps state.root out for a freshly
// allocated slab (array.go: `a.state.root = &ArrayDataSlab{...}`).
// If state were not shared, the second sibling would keep pointing at the
// pre-pop root and continue reporting the old count.
// Mutating through sibling2 after the pop must also be observed by sibling1.
func TestArraySiblingConsistencyAfterPopIterate(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Populate enough to force multi-slab so PopIterate exercises the
	// non-trivial case (an inlined single-slab case wouldn't change state.root).
	const initialCount = 200
	for i := uint64(0); i < initialCount; i++ {
		require.NoError(t, inner.Append(testutils.NewUint64ValueFromInteger(int(i))))
	}

	require.NoError(t, outer.Append(inner))

	a, err := outer.Get(0)
	require.NoError(t, err)
	sibling1 := a.(*atree.Array)

	a, err = outer.Get(0)
	require.NoError(t, err)
	sibling2 := a.(*atree.Array)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.False(t, sibling1.IsWithinSingleSlab(),
		"populated inner array must span multiple slabs")

	rootIDBeforePop := sibling1.SlabID()

	// Pop through sibling1. After this, state.root has been replaced with
	// a freshly allocated empty *ArrayDataSlab carrying the original root SlabID.
	require.NoError(t, sibling1.PopIterate(func(atree.Storable) {}))

	require.Equal(t, uint64(0), sibling1.Count(), "sibling1 must observe empty array post-pop")
	require.Equal(t, uint64(0), sibling2.Count(),
		"sibling2 must observe the new empty root through shared state")
	require.Equal(t, rootIDBeforePop, sibling2.SlabID(),
		"PopIterate must preserve the canonical root SlabID")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.IsWithinSingleSlab(),
		"post-pop root must be a single empty data slab")
	require.True(t, sibling2.IsWithinSingleSlab())

	// Mutate via sibling2; sibling1 must see it through shared state.
	require.NoError(t, sibling2.Append(testutils.NewUint64ValueFromInteger(7)))
	require.Equal(t, uint64(1), sibling1.Count(),
		"sibling1 must observe sibling2's post-pop append")
	v, err := sibling1.Get(0)
	require.NoError(t, err)
	require.Equal(t, testutils.NewUint64ValueFromInteger(7), v)
}

// TestArraySiblingConsistencyAcrossUninlineTransition is the reverse direction
// of TestArraySiblingConsistencyAcrossInlineTransition:
// start with an inlined inner, mutate enough through one sibling to force
// the inner to be uninlined, and verify every sibling observes the transition.
//
// The uninline transition runs through *Array.Storable when the parent re-stores
// the child: once the inner's root becomes too large to inline (or becomes a
// MetaDataSlab, which is never inlinable), `state.root.Uninline` flips `inlined`
// to false on the shared slab and stores it in storage. A second sibling holding
// only its own root pointer (pre-PR) would still report Inlined() == true.
func TestArraySiblingConsistencyAcrossUninlineTransition(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Build an inlined inner: empty array attached to outer is inlined by default.
	inner, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)
	require.NoError(t, outer.Append(inner))

	a, err := outer.Get(0)
	require.NoError(t, err)
	sibling1 := a.(*atree.Array)

	a, err = outer.Get(0)
	require.NoError(t, err)
	sibling2 := a.(*atree.Array)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.Inlined(),
		"freshly attached empty inner must be inlined")
	require.True(t, sibling2.Inlined())

	// Grow via sibling1 until it can no longer be inlined.
	// Each Append eventually triggers parent re-store → inner.Storable() →
	// Uninline when the slab exceeds the inline size or becomes a meta slab.
	for i := uint64(0); sibling1.Inlined(); i++ {
		require.NoError(t, sibling1.Append(testutils.NewUint64ValueFromInteger(int(i))))

		// Guard against an infinite loop if a future change quietly raises
		// the inline threshold above what 1000 Appends can exceed.
		require.Less(t, i, uint64(1000),
			"sibling1 must transition to uninlined within a bounded number of appends")
	}

	require.False(t, sibling1.Inlined(),
		"sibling1 must observe the uninline transition")
	require.False(t, sibling2.Inlined(),
		"sibling2 must observe the uninline transition through shared state")
	require.Equal(t, sibling1.Count(), sibling2.Count())
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.NotEqual(t, atree.SlabIDUndefined, sibling2.SlabID(),
		"uninlined sibling must expose a real SlabID")

	// Cross-check: append through sibling2; sibling1 must see it.
	require.NoError(t, sibling2.Append(testutils.NewUint64ValueFromInteger(9999)))
	require.Equal(t, sibling2.Count(), sibling1.Count())
	last, err := sibling1.Get(sibling1.Count() - 1)
	require.NoError(t, err)
	require.Equal(t, testutils.NewUint64ValueFromInteger(9999), last)
}

// TestArrayTrapCallbackDoesNotFireOnSiblingMutation pins down the contract
// introduced by HasReadOnlyMutationCallback / setReadOnlyMutationCallback:
// a per-instance trap callback on one sibling must NOT fire when an
// unrelated sibling triggers a structural change through the shared state.
//
// Two *Array Go handles for the same inner exist:
//   - sibling1, obtained via outer.Get(0): real parent-notification callback.
//   - sibling2, obtained via outer.ReadOnlyIterator().Next(): trap callback.
//
// Mutations through sibling1 must succeed and propagate state to sibling2
// without firing sibling2's trap (state propagation does not invoke
// sibling.parentUpdater on uninvolved siblings).
// Mutations through sibling2 must trip the trap and return
// ReadOnlyIteratorElementMutationError.
func TestArrayTrapCallbackDoesNotFireOnSiblingMutation(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)
	require.NoError(t, inner.Append(testutils.NewUint64ValueFromInteger(0)))

	require.NoError(t, outer.Append(inner))

	// sibling1: real parent-notification callback.
	a, err := outer.Get(0)
	require.NoError(t, err)
	sibling1 := a.(*atree.Array)
	require.True(t, sibling1.HasParentUpdater())
	require.False(t, sibling1.HasReadOnlyMutationCallback(),
		"Get-loaded sibling must carry a real callback")

	// sibling2: trap callback installed by the readonly iterator.
	iter, err := outer.ReadOnlyIterator()
	require.NoError(t, err)
	v, err := iter.Next()
	require.NoError(t, err)
	sibling2 := v.(*atree.Array)
	require.True(t, sibling2.HasParentUpdater())
	require.True(t, sibling2.HasReadOnlyMutationCallback(),
		"iterator-loaded sibling must carry a trap callback")

	// Sanity: siblings share state.
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())

	// Mutate through sibling1 (real callback). Must succeed.
	require.NoError(t, sibling1.Append(testutils.NewUint64ValueFromInteger(1)))
	require.Equal(t, uint64(2), sibling1.Count())
	require.Equal(t, uint64(2), sibling2.Count(),
		"sibling2 must observe sibling1's mutation through shared state")

	// sibling2's trap must still be installed; sibling1's must still be real.
	require.False(t, sibling1.HasReadOnlyMutationCallback(),
		"sibling1's real callback must not be replaced by sibling2's trap")
	require.True(t, sibling2.HasReadOnlyMutationCallback(),
		"sibling2's trap must survive sibling1's mutation "+
			"(state propagation must not invoke uninvolved siblings' updaters)")

	// Mutate through sibling2 (trap callback). Must return the trap error.
	err = sibling2.Append(testutils.NewUint64ValueFromInteger(2))
	var mutationError *atree.ReadOnlyIteratorElementMutationError
	require.ErrorAs(t, err, &mutationError,
		"mutating through a trap-bearing sibling must return ReadOnlyIteratorElementMutationError")
}

// TestNewArrayWithRootIDReturnsSameState verifies:
// two calls to NewArrayWithRootID for the same rootID must return *Array
// instances backed by the same shared state — i.e. the same Go pointer for
// `state.root`. Tested indirectly via GetArrayRootSlab pointer equality.
func TestNewArrayWithRootIDReturnsSameState(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	arr, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)
	require.NoError(t, arr.Append(testutils.NewUint64ValueFromInteger(0)))

	rootID := arr.SlabID()
	require.NotEqual(t, atree.SlabIDUndefined, rootID,
		"standalone array must have a real SlabID for NewArrayWithRootID to succeed")

	a1, err := atree.NewArrayWithRootID(storage, rootID)
	require.NoError(t, err)

	a2, err := atree.NewArrayWithRootID(storage, rootID)
	require.NoError(t, err)

	require.NotSame(t, a1, a2,
		"each NewArrayWithRootID call must return a distinct *Array Go object")
	require.Same(t, atree.GetArrayRootSlab(a1), atree.GetArrayRootSlab(a2),
		"two NewArrayWithRootID calls for the same rootID must back the *Array "+
			"instances with the same shared state.root pointer")
	require.Equal(t, a1.ValueID(), a2.ValueID())

	// Functional cross-check: mutate through a1, observe through a2.
	require.NoError(t, a1.Append(testutils.NewUint64ValueFromInteger(42)))
	require.Equal(t, a1.Count(), a2.Count(),
		"a2 must observe a1's mutation through the shared state")
}

// TestNewArrayWithRootIDAfterDestroy verifies that a registered state
// does not outlive its container's destruction:
// after the root slab is removed from storage,
// NewArrayWithRootID must return SlabNotFoundError —
// not a zombie *Array served from the leftover registry state —
// and must clear the leftover state from the registry.
//
// (The registry deliberately survives storage.Remove
// because Remove is also called when a container is inlined while still alive;
// the constructors distinguish the two cases
// by checking slab existence for non-inlined roots.)
func TestNewArrayWithRootIDAfterDestroy(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	arr, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)
	require.NoError(t, arr.Append(testutils.NewUint64ValueFromInteger(0)))

	rootID := arr.SlabID()
	require.NotEqual(t, atree.SlabIDUndefined, rootID)
	require.NotNil(t, storage.ArrayState(rootID),
		"constructing the array must have registered its state")

	// Destroy the standalone single-slab container
	// by removing its root slab.
	require.NoError(t, storage.Remove(rootID))

	_, err = atree.NewArrayWithRootID(storage, rootID)
	var slabNotFoundError *atree.SlabNotFoundError
	require.ErrorAs(t, err, &slabNotFoundError,
		"NewArrayWithRootID on a destroyed container must return SlabNotFoundError")

	require.Nil(t, storage.ArrayState(rootID),
		"detecting the destroyed container must clear the leftover registry state")

	// A second call goes down the no-state path and must fail the same way.
	_, err = atree.NewArrayWithRootID(storage, rootID)
	require.ErrorAs(t, err, &slabNotFoundError)
}
