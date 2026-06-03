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

// TestMapSiblingConsistencyAfterSplitRoot is the OrderedMap counterpart
// to TestArraySiblingConsistencyAfterSplitRoot:
// two *OrderedMap instances obtained for the same inner map
// must observe the same canonical state
// after a structural change initiated through one of them.
func TestMapSiblingConsistencyAfterSplitRoot(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	k0 := testutils.NewUint64ValueFromInteger(0)
	v0 := testutils.NewUint64ValueFromInteger(0)
	prev, err := inner.Set(testutils.CompareValue, testutils.GetHashInput, k0, v0)
	require.NoError(t, err)
	require.Nil(t, prev)

	prev, err = outer.Set(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0), inner)
	require.NoError(t, err)
	require.Nil(t, prev)

	// Two sibling instances over the same inner map.
	innerVal, err := outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling1 := innerVal.(*atree.OrderedMap)

	innerVal, err = outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling2 := innerVal.(*atree.OrderedMap)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.IsWithinSingleSlab(),
		"initial inner map must be in a single slab")

	// Insert through sibling1 enough to force a split.
	const insertCount = 200
	for i := uint64(1); i <= insertCount; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		v := testutils.NewUint64ValueFromInteger(int(i))
		prev, err := sibling1.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, prev)
	}

	// Confirm splitRoot actually fired.
	require.False(t, sibling1.IsWithinSingleSlab(),
		"splitRoot must have fired during the inserts")

	require.Equal(t, uint64(1+insertCount), sibling1.Count())
	require.Equal(t, uint64(1+insertCount), sibling2.Count(),
		"sibling2 must observe post-split count through shared state")

	// Mutate through sibling2; sibling1 must see it.
	prev, err = sibling2.Set(testutils.CompareValue, testutils.GetHashInput,
		testutils.NewUint64ValueFromInteger(99999),
		testutils.NewUint64ValueFromInteger(99999))
	require.NoError(t, err)
	require.Nil(t, prev)
	require.Equal(t, uint64(2+insertCount), sibling1.Count(),
		"sibling1 must observe sibling2's insert")
}

// TestMapSiblingConsistencyAfterPromoteRoot exercises the dual of
// TestMapSiblingConsistencyAfterSplitRoot for OrderedMap:
// enough removals through one sibling to trigger promoteChildAsNewRoot.
// The previous Cadence-side staleness check
// (cached valueID vs live ValueID())
// would NOT detect this case for either Array or Map
// because promoteChildAsNewRoot keeps the root's slab ID stable;
// shared state is what makes the post-promote count visible to the other sibling.
func TestMapSiblingConsistencyAfterPromoteRoot(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// Populate enough entries to require multi-slab structure
	// (will force the root to become a meta slab with multiple children).
	const initialCount = 200
	for i := uint64(0); i < initialCount; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		v := testutils.NewUint64ValueFromInteger(int(i))
		prev, err := inner.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, prev)
	}

	prev, err := outer.Set(testutils.CompareValue, testutils.GetHashInput,
		testutils.NewUint64ValueFromInteger(0), inner)
	require.NoError(t, err)
	require.Nil(t, prev)

	// Two sibling instances.
	innerVal, err := outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling1 := innerVal.(*atree.OrderedMap)

	innerVal, err = outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling2 := innerVal.(*atree.OrderedMap)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.False(t, sibling1.IsWithinSingleSlab(),
		"populated inner map must span multiple slabs")

	// Remove through sibling1 until the meta slab is forced to promote.
	for i := uint64(1); i < initialCount; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		_, _, err := sibling1.Remove(testutils.CompareValue, testutils.GetHashInput, k)
		require.NoError(t, err)
	}

	// Confirm promoteChildAsNewRoot actually fired.
	require.True(t, sibling1.IsWithinSingleSlab(),
		"promoteChildAsNewRoot must have fired during the removals")

	require.Equal(t, uint64(1), sibling1.Count())
	require.Equal(t, uint64(1), sibling2.Count(),
		"sibling2 must observe post-promote count through shared state")

	// The remaining entry must be readable through both siblings.
	k0 := testutils.NewUint64ValueFromInteger(0)
	v1, err := sibling1.Get(testutils.CompareValue, testutils.GetHashInput, k0)
	require.NoError(t, err)
	v2, err := sibling2.Get(testutils.CompareValue, testutils.GetHashInput, k0)
	require.NoError(t, err)
	require.Equal(t, v1, v2)

	// Insert through sibling2; sibling1 must see it
	// (and the live ValueID must match —
	// both siblings share the same state.root).
	prev, err = sibling2.Set(testutils.CompareValue, testutils.GetHashInput,
		testutils.NewUint64ValueFromInteger(99999),
		testutils.NewUint64ValueFromInteger(99999))
	require.NoError(t, err)
	require.Nil(t, prev)
	require.Equal(t, uint64(2), sibling1.Count(),
		"sibling1 must observe sibling2's post-promote insert")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID(),
		"siblings must keep matching ValueIDs after every structural op")
}

// TestMapSiblingConsistencyAcrossSplitAndPromote is the OrderedMap
// counterpart to TestArraySiblingConsistencyAcrossSplitAndPromote:
// a sequence that drives both structural operations through different siblings.
//   - grow via sibling1 until splitRoot fires
//   - shrink via sibling2 until promoteChildAsNewRoot fires
//
// Each transition must leave both siblings observing the same live canonical state.
func TestMapSiblingConsistencyAcrossSplitAndPromote(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	k0 := testutils.NewUint64ValueFromInteger(0)
	v0 := testutils.NewUint64ValueFromInteger(0)
	prev, err := inner.Set(testutils.CompareValue, testutils.GetHashInput, k0, v0)
	require.NoError(t, err)
	require.Nil(t, prev)

	prev, err = outer.Set(testutils.CompareValue, testutils.GetHashInput,
		testutils.NewUint64ValueFromInteger(0), inner)
	require.NoError(t, err)
	require.Nil(t, prev)

	innerVal, err := outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling1 := innerVal.(*atree.OrderedMap)

	innerVal, err = outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling2 := innerVal.(*atree.OrderedMap)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.IsWithinSingleSlab(),
		"initial inner map must be in a single slab")

	// Grow through sibling1 → splitRoot.
	const grow = 200
	for i := uint64(1); i <= grow; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		v := testutils.NewUint64ValueFromInteger(int(i))
		prev, err := sibling1.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, prev)
	}
	require.False(t, sibling1.IsWithinSingleSlab(),
		"splitRoot must have fired during the grow phase")
	require.Equal(t, uint64(1+grow), sibling2.Count(),
		"sibling2 must see post-split count")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID(),
		"split must preserve matching ValueIDs through shared state")

	// Shrink through sibling2 → promoteChildAsNewRoot.
	for i := uint64(1); i <= grow; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		_, _, err := sibling2.Remove(testutils.CompareValue, testutils.GetHashInput, k)
		require.NoError(t, err)
	}
	require.True(t, sibling1.IsWithinSingleSlab(),
		"promoteChildAsNewRoot must have fired during the shrink phase")
	require.Equal(t, uint64(1), sibling1.Count(),
		"sibling1 must see post-promote count")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID(),
		"promote must preserve matching ValueIDs through shared state")

	// Final cross-check: insert via sibling1, observe via sibling2.
	k42 := testutils.NewUint64ValueFromInteger(42)
	v42 := testutils.NewUint64ValueFromInteger(42)
	prev, err = sibling1.Set(testutils.CompareValue, testutils.GetHashInput, k42, v42)
	require.NoError(t, err)
	require.Nil(t, prev)
	require.Equal(t, uint64(2), sibling2.Count())
	v, err := sibling2.Get(testutils.CompareValue, testutils.GetHashInput, k42)
	require.NoError(t, err)
	require.Equal(t, v42, v)
}

// TestMapSiblingTestStructuralAssertionsAreMeaningful is the OrderedMap
// counterpart to TestArraySiblingTestStructuralAssertionsAreMeaningful.
// It validates that the structural assertions used in the map sibling tests
// actually distinguish "split fired" from "split skipped" —
// if a future atree change made the test setup fit in a single slab,
// the sibling-consistency assertions would pass trivially.
//
// This test runs the same insert pattern under two thresholds:
//   - 256 bytes (what the sibling tests use): split must fire
//   - 16 KiB (large): split must NOT fire
func TestMapSiblingTestStructuralAssertionsAreMeaningful(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	growAndCheck := func(threshold uint32) (singleSlabAtEnd bool) {
		atree.SetThreshold(threshold)
		defer atree.SetThreshold(1024)

		storage := newTestPersistentStorage(t)
		m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
		require.NoError(t, err)

		const grow = 200
		for i := uint64(0); i < grow; i++ {
			k := testutils.NewUint64ValueFromInteger(int(i))
			v := testutils.NewUint64ValueFromInteger(int(i))
			prev, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
			require.NoError(t, err)
			require.Nil(t, prev)
		}
		return m.IsWithinSingleSlab()
	}

	require.False(t, growAndCheck(256),
		"with threshold=256 (what the sibling tests use), 200 entries "+
			"must overflow a single slab and force splitRoot — if this "+
			"passes (single-slab), the sibling tests' structural assertions "+
			"would pass trivially without a real split")

	require.True(t, growAndCheck(16*1024),
		"with threshold=16KiB, 200 entries must fit in a single slab — "+
			"if this fails, our 'no-split' baseline doesn't hold and the "+
			"meta-test can't distinguish the two configurations")
}

// TestMapSiblingConsistencyAcrossInlineTransition is the OrderedMap
// counterpart to TestArraySiblingConsistencyAcrossInlineTransition.
// It exercises the same state-lifetime hazard:
// MapDataSlab.Inline internally calls storage.Remove(slabID) on the child,
// and our shared-state registry must survive that call
// so future *OrderedMap instances for the now-inlined container
// share state with any existing siblings.
func TestMapSiblingConsistencyAcrossInlineTransition(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// Grow inner past the inline threshold before attaching it.
	const growSize = 100
	for i := uint64(0); i < growSize; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		v := testutils.NewUint64ValueFromInteger(int(i))
		prev, err := inner.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, prev)
	}

	prev, err := outer.Set(testutils.CompareValue, testutils.GetHashInput,
		testutils.NewUint64ValueFromInteger(0), inner)
	require.NoError(t, err)
	require.Nil(t, prev)

	// Two sibling instances while inner is uninlined.
	innerVal, err := outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling1 := innerVal.(*atree.OrderedMap)

	innerVal, err = outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling2 := innerVal.(*atree.OrderedMap)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.False(t, sibling1.Inlined())

	// Shrink inner through sibling1 until atree re-inlines it.
	// This triggers MapDataSlab.Inline → storage.Remove.
	for i := uint64(0); sibling1.Count() > 0 && !sibling1.Inlined(); i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		_, _, err := sibling1.Remove(testutils.CompareValue, testutils.GetHashInput, k)
		require.NoError(t, err)
	}
	require.True(t, sibling1.Inlined(),
		"inner must be inlined after the shrink so the test exercises the Inline path")

	require.Equal(t, sibling1.Count(), sibling2.Count(),
		"sibling2 must observe sibling1's removals across the inline transition")

	// Critical: a fresh load after the inline transition must share the same state
	// as the pre-inline siblings.
	// Trigger a structural change through this fresh load
	// and verify pre-existing siblings see it.
	innerVal, err = outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling3 := innerVal.(*atree.OrderedMap)

	for i := uint64(0); i < 200; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i + 1000))
		v := testutils.NewUint64ValueFromInteger(int(i + 1000))
		prev, err := sibling3.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, prev)
	}
	require.False(t, sibling3.IsWithinSingleSlab(),
		"sibling3 must have triggered splitRoot during the regrowth")
	require.Equal(t, sibling3.Count(), sibling1.Count(),
		"sibling1 must see sibling3's post-split count via shared state — "+
			"if this fails, the inline transition dropped the registry "+
			"entry and sibling3 got an independent state")
	require.Equal(t, sibling1.ValueID(), sibling3.ValueID())
}

// TestMapBuildWithDistinctInlinedMaps is the OrderedMap counterpart to
// TestArrayBatchBuildWithDistinctInlinedMaps:
// inserting three distinct inlined OrderedMap values into a parent OrderedMap
// produces a parent whose entries retain their original distinguishing content.
//
// Scenario:
//   - Construct three inner *OrderedMap values m1, m2, m3,
//     each with a single distinguishing entry.
//   - Copy each via CopyNonRefSimple to simulate a transfer.
//   - Insert each copy as the value of an entry in a parent *OrderedMap.
//
// All values must live in a single storage:
// atree's shared-state design assumes SlabIDs are unique within a storage,
// but each storage has its own monotonic SlabID counter starting from zero,
// so mixing values across multiple storages can produce SlabID collisions
// that the shared-state registry cannot disambiguate.
func TestMapBuildWithDistinctInlinedMaps(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestBasicStorage(t)
	var address atree.Address

	const innerCount = 3
	copies := make([]*atree.OrderedMap, innerCount)
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

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	for i, copied := range copies {
		prev, err := outer.Set(
			testutils.CompareValue, testutils.GetHashInput,
			testutils.NewUint64ValueFromInteger(i),
			copied,
		)
		require.NoError(t, err)
		require.Nil(t, prev)
	}
	require.Equal(t, uint64(innerCount), outer.Count())

	// Look up each entry and verify the inner map retained its distinct content.
	for i := 0; i < innerCount; i++ {
		v, err := outer.Get(
			testutils.CompareValue, testutils.GetHashInput,
			testutils.NewUint64ValueFromInteger(i),
		)
		require.NoError(t, err)

		gotMap, ok := v.(*atree.OrderedMap)
		require.True(t, ok, "entry %d's value must be an *OrderedMap", i)

		gotValue, err := gotMap.Get(
			testutils.CompareValue, testutils.GetHashInput,
			testutils.NewUint64ValueFromInteger(0),
		)
		require.NoError(t, err)

		expected := testutils.NewUint64ValueFromInteger(i + 1)
		require.Equal(t, expected, gotValue,
			"entry %d's inner map must retain its distinguishing entry", i)
	}
}

// TestMapSiblingConsistencyAfterPopIterate is the OrderedMap counterpart
// to TestArraySiblingConsistencyAfterPopIterate.
// PopIterate replaces state.root with a freshly allocated empty *MapDataSlab
// (map.go: `m.state.root = &MapDataSlab{...}`); the replacement must be
// observed by every sibling Go handle through the shared state.
func TestMapSiblingConsistencyAfterPopIterate(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// Populate enough to force multi-slab so PopIterate exercises the
	// non-trivial case.
	const initialCount = 200
	for i := uint64(0); i < initialCount; i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		v := testutils.NewUint64ValueFromInteger(int(i))
		prev, err := inner.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, prev)
	}

	prev, err := outer.Set(testutils.CompareValue, testutils.GetHashInput,
		testutils.NewUint64ValueFromInteger(0), inner)
	require.NoError(t, err)
	require.Nil(t, prev)

	innerVal, err := outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling1 := innerVal.(*atree.OrderedMap)

	innerVal, err = outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling2 := innerVal.(*atree.OrderedMap)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.False(t, sibling1.IsWithinSingleSlab(),
		"populated inner map must span multiple slabs")

	rootIDBeforePop := sibling1.SlabID()

	// Pop through sibling1. State.root is replaced with a new empty *MapDataSlab
	// carrying the original root SlabID.
	require.NoError(t, sibling1.PopIterate(func(atree.Storable, atree.Storable) {}))

	require.Equal(t, uint64(0), sibling1.Count(), "sibling1 must observe empty map post-pop")
	require.Equal(t, uint64(0), sibling2.Count(),
		"sibling2 must observe the new empty root through shared state")
	require.Equal(t, rootIDBeforePop, sibling2.SlabID(),
		"PopIterate must preserve the canonical root SlabID")
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.IsWithinSingleSlab(),
		"post-pop root must be a single empty data slab")
	require.True(t, sibling2.IsWithinSingleSlab())

	// Mutate via sibling2; sibling1 must see it through shared state.
	k7 := testutils.NewUint64ValueFromInteger(7)
	v7 := testutils.NewUint64ValueFromInteger(7)
	prev, err = sibling2.Set(testutils.CompareValue, testutils.GetHashInput, k7, v7)
	require.NoError(t, err)
	require.Nil(t, prev)
	require.Equal(t, uint64(1), sibling1.Count(),
		"sibling1 must observe sibling2's post-pop insert")
	got, err := sibling1.Get(testutils.CompareValue, testutils.GetHashInput, k7)
	require.NoError(t, err)
	require.Equal(t, v7, got)
}

// TestMapSiblingConsistencyAcrossUninlineTransition is the OrderedMap counterpart
// to TestArraySiblingConsistencyAcrossUninlineTransition: start with an inlined
// inner map, mutate enough through one sibling to force uninlining, and verify
// every sibling observes the transition.
func TestMapSiblingConsistencyAcrossUninlineTransition(t *testing.T) {

	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	// An empty inner attached to outer is inlined by default.
	inner, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	prev, err := outer.Set(testutils.CompareValue, testutils.GetHashInput,
		testutils.NewUint64ValueFromInteger(0), inner)
	require.NoError(t, err)
	require.Nil(t, prev)

	innerVal, err := outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling1 := innerVal.(*atree.OrderedMap)

	innerVal, err = outer.Get(testutils.CompareValue, testutils.GetHashInput, testutils.NewUint64ValueFromInteger(0))
	require.NoError(t, err)
	sibling2 := innerVal.(*atree.OrderedMap)

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.True(t, sibling1.Inlined(),
		"freshly attached empty inner must be inlined")
	require.True(t, sibling2.Inlined())

	// Grow via sibling1 until the inner can no longer be inlined.
	for i := uint64(0); sibling1.Inlined(); i++ {
		k := testutils.NewUint64ValueFromInteger(int(i))
		v := testutils.NewUint64ValueFromInteger(int(i))
		prev, err := sibling1.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
		require.NoError(t, err)
		require.Nil(t, prev)

		require.Less(t, i, uint64(1000),
			"sibling1 must transition to uninlined within a bounded number of inserts")
	}

	require.False(t, sibling1.Inlined(),
		"sibling1 must observe the uninline transition")
	require.False(t, sibling2.Inlined(),
		"sibling2 must observe the uninline transition through shared state")
	require.Equal(t, sibling1.Count(), sibling2.Count())
	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())
	require.NotEqual(t, atree.SlabIDUndefined, sibling2.SlabID(),
		"uninlined sibling must expose a real SlabID")

	// Cross-check: insert through sibling2; sibling1 must see it.
	kX := testutils.NewUint64ValueFromInteger(99999)
	vX := testutils.NewUint64ValueFromInteger(99999)
	prev, err = sibling2.Set(testutils.CompareValue, testutils.GetHashInput, kX, vX)
	require.NoError(t, err)
	require.Nil(t, prev)
	require.Equal(t, sibling2.Count(), sibling1.Count())
	got, err := sibling1.Get(testutils.CompareValue, testutils.GetHashInput, kX)
	require.NoError(t, err)
	require.Equal(t, vX, got)
}

// TestMapTrapCallbackDoesNotFireOnSiblingMutation is the OrderedMap counterpart
// to TestArrayTrapCallbackDoesNotFireOnSiblingMutation. A trap callback on a
// readonly-iterator-loaded sibling must not fire when a separate sibling
// initiates a structural change, but must fire when the trap-bearing sibling
// itself is mutated.
func TestMapTrapCallbackDoesNotFireOnSiblingMutation(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	outer, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	inner, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	k0 := testutils.NewUint64ValueFromInteger(0)
	v0 := testutils.NewUint64ValueFromInteger(0)
	prev, err := inner.Set(testutils.CompareValue, testutils.GetHashInput, k0, v0)
	require.NoError(t, err)
	require.Nil(t, prev)

	outerKey := testutils.NewUint64ValueFromInteger(0)
	prev, err = outer.Set(testutils.CompareValue, testutils.GetHashInput, outerKey, inner)
	require.NoError(t, err)
	require.Nil(t, prev)

	// sibling1: real parent-notification callback.
	v, err := outer.Get(testutils.CompareValue, testutils.GetHashInput, outerKey)
	require.NoError(t, err)
	sibling1 := v.(*atree.OrderedMap)
	require.True(t, sibling1.HasParentUpdater())
	require.False(t, sibling1.HasReadOnlyMutationCallback(),
		"Get-loaded sibling must carry a real callback")

	// sibling2: trap callback installed by the readonly iterator.
	// Use a key-value iterator so the value (the inner *OrderedMap) carries the trap.
	iter, err := outer.ReadOnlyIterator()
	require.NoError(t, err)
	_, v, err = iter.Next()
	require.NoError(t, err)
	sibling2 := v.(*atree.OrderedMap)
	require.True(t, sibling2.HasParentUpdater())
	require.True(t, sibling2.HasReadOnlyMutationCallback(),
		"iterator-loaded sibling must carry a trap callback")

	require.Equal(t, sibling1.ValueID(), sibling2.ValueID())

	// Mutate through sibling1 (real callback). Must succeed.
	k1 := testutils.NewUint64ValueFromInteger(1)
	v1 := testutils.NewUint64ValueFromInteger(1)
	prev, err = sibling1.Set(testutils.CompareValue, testutils.GetHashInput, k1, v1)
	require.NoError(t, err)
	require.Nil(t, prev)
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
	k2 := testutils.NewUint64ValueFromInteger(2)
	v2 := testutils.NewUint64ValueFromInteger(2)
	_, err = sibling2.Set(testutils.CompareValue, testutils.GetHashInput, k2, v2)
	var mutationError *atree.ReadOnlyIteratorElementMutationError
	require.ErrorAs(t, err, &mutationError,
		"mutating through a trap-bearing sibling must return ReadOnlyIteratorElementMutationError")
}

// TestNewMapWithRootIDReturnsSameState pins the idempotence contract for
// OrderedMap analogous to TestNewArrayWithRootIDReturnsSameState:
// two calls to NewMapWithRootID for the same rootID must return *OrderedMap
// instances backed by the same shared state.root pointer, AND each call must
// re-seed the caller's digester from the canonical extraData.Seed so all
// siblings compute consistent hkeys.
func TestNewMapWithRootIDReturnsSameState(t *testing.T) {

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	k0 := testutils.NewUint64ValueFromInteger(0)
	v0 := testutils.NewUint64ValueFromInteger(0)
	prev, err := m.Set(testutils.CompareValue, testutils.GetHashInput, k0, v0)
	require.NoError(t, err)
	require.Nil(t, prev)

	rootID := m.SlabID()
	require.NotEqual(t, atree.SlabIDUndefined, rootID)
	canonicalSeed := m.Seed()

	// Each NewMapWithRootID call passes a fresh DigesterBuilder; the function
	// must seed it from the canonical extra data so hkeys match.
	db1 := atree.NewDefaultDigesterBuilder()
	m1, err := atree.NewMapWithRootID(storage, rootID, db1)
	require.NoError(t, err)

	db2 := atree.NewDefaultDigesterBuilder()
	m2, err := atree.NewMapWithRootID(storage, rootID, db2)
	require.NoError(t, err)

	require.NotSame(t, m1, m2,
		"each NewMapWithRootID call must return a distinct *OrderedMap Go object")
	require.Same(t, atree.GetMapRootSlab(m1), atree.GetMapRootSlab(m2),
		"two NewMapWithRootID calls for the same rootID must back the "+
			"*OrderedMap instances with the same shared state.root pointer")
	require.Equal(t, m1.ValueID(), m2.ValueID())
	require.Equal(t, canonicalSeed, m1.Seed(),
		"NewMapWithRootID must report the canonical seed")
	require.Equal(t, canonicalSeed, m2.Seed())

	// Functional cross-check: insert with m1's digester, look up with m2's digester.
	// If either digester wasn't re-seeded from the canonical extra data, the
	// hkeys would diverge and the lookup would miss.
	kX := testutils.NewUint64ValueFromInteger(123)
	vX := testutils.NewUint64ValueFromInteger(456)
	prev, err = m1.Set(testutils.CompareValue, testutils.GetHashInput, kX, vX)
	require.NoError(t, err)
	require.Nil(t, prev)

	got, err := m2.Get(testutils.CompareValue, testutils.GetHashInput, kX)
	require.NoError(t, err)
	require.Equal(t, vX, got,
		"m2 must find the key inserted via m1 — proves both digesters "+
			"were re-seeded with the canonical seed and the *OrderedMap instances "+
			"share the same state.root")
}
