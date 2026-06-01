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
