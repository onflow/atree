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
