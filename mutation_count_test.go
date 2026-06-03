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

// MutationCount is exposed on *Array / *OrderedMap so that callers caching
// the value at wrapper construction can detect when their .root pointer has
// been orphaned by a structural mutation that preserves the orphaned slab's
// SlabID — namely promoteChildAsNewRoot and PopIterate. The counter lives on
// the slab struct so that two Go-level container structs that share an
// orphaned slab pointer both observe the same bumped value.
//
// splitRoot is NOT a bump site: ArraySlab.Split / MapSlab.Split return the
// receiver as the LEFT child, so the "old root" stays in the tree as a child
// slab. If a subsequent promote picks that slab, *Array.MutationCount()
// would otherwise erroneously report staleness for the wrapper that
// initiated the operations. splitRoot already perturbs the old root's
// SlabID, which is the signal sibling wrappers observe via the ValueID-based
// staleness check.
//
// These tests pin the contract:
//   1. Counter starts at 0 for a freshly constructed container.
//   2. Element-level operations that do not detach a root leave the counter
//      unchanged (Get / Set / Append/Insert below split threshold / Remove
//      that does not promote).
//   3. splitRoot does NOT bump the counter (regression test: bumping there
//      would corrupt the live counter for the initiating wrapper if the
//      demoted left child is later re-promoted to root).
//   4. promoteChildAsNewRoot bumps the OLD root's counter exactly once.
//   5. PopIterate bumps the OLD root's counter exactly once.
//   6. A pointer to the OLD root captured before promote/PopIterate sees the
//      bumped value; the live *Array.MutationCount() reads the NEW root's
//      counter, which is fresh (= 0) and therefore matches what the
//      initiating wrapper would have cached.
//   7. The wrapper that initiated a full grow+collapse cycle does not see a
//      false-positive staleness on the live counter.

// --- Invariant tests: ArraySlab.Split / MapSlab.Split return the receiver
// as the LEFT child ---
//
// The decision NOT to bump MutationCount at splitRoot rests on this
// invariant: if Split started returning a fresh struct for the left side
// instead, the old root would become orphaned and splitRoot would need to
// behave like promote/PopIterate (bump). These tests pin the contract so a
// future Split refactor breaks loudly here rather than silently
// re-introducing the bug.

func TestSplitRoot_Array_DemotedRootIsReusedAsLeftChild(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Capture the initial root by Go pointer.
	oldRoot := atree.GetArrayRootSlab(array)
	require.True(t, oldRoot.IsData(), "initial root is a data slab")

	// Force splitRoot. Stop as soon as the first split fires so that the
	// data-slab root we captured stays at the topmost child level — any
	// further splits would demote it deeper into the tree.
	for i := uint64(0); ; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
		if !atree.GetArrayRootSlab(array).IsData() {
			break
		}
	}
	require.False(t, atree.GetArrayRootSlab(array).IsData(),
		"prerequisite: a split must have occurred")

	// The new root is a metadata slab.
	metaRoot, ok := atree.GetArrayRootSlab(array).(*atree.ArrayMetaDataSlab)
	require.True(t, ok)

	// The left child SlabID, as recorded by splitRoot, must point back to
	// the same Go struct as the captured old root.
	childSlabIDs, _ := atree.GetArrayMetaDataSlabChildInfo(metaRoot)
	require.GreaterOrEqual(t, len(childSlabIDs), 2,
		"split must produce at least two children")

	leftChild, found, err := storage.Retrieve(childSlabIDs[0])
	require.NoError(t, err)
	require.True(t, found, "left child must be retrievable from storage")

	require.Same(t, oldRoot, leftChild,
		"ArraySlab.Split must return the receiver as the LEFT child; if "+
			"this fails, splitRoot's old root becomes orphaned and "+
			"MutationCount must be bumped there too (currently it is not)")

	// Bonus: the SlabID on the demoted slab is the fresh one splitRoot
	// generated via SetSlabID(sID), which is also what's recorded in the
	// new root's left header.
	require.Equal(t, leftChild.SlabID(), oldRoot.SlabID(),
		"SetSlabID side effect is observable on the captured pointer")
}

func TestSplitRoot_Map_DemotedRootIsReusedAsLeftChild(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	oldRoot := atree.GetMapRootSlab(m)
	require.True(t, oldRoot.IsData(), "initial root is a data slab")

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput
	// Stop at the first split so the captured root stays at the topmost
	// child level rather than being demoted deeper by subsequent splits.
	for i := uint64(0); ; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
		if !atree.GetMapRootSlab(m).IsData() {
			break
		}
	}
	require.False(t, atree.GetMapRootSlab(m).IsData(),
		"prerequisite: a split must have occurred")

	metaRoot, ok := atree.GetMapRootSlab(m).(*atree.MapMetaDataSlab)
	require.True(t, ok)

	childSlabIDs, _, _ := atree.GetMapMetaDataSlabChildInfo(metaRoot)
	require.GreaterOrEqual(t, len(childSlabIDs), 2,
		"split must produce at least two children")

	leftChild, found, err := storage.Retrieve(childSlabIDs[0])
	require.NoError(t, err)
	require.True(t, found, "left child must be retrievable from storage")

	require.Same(t, oldRoot, leftChild,
		"MapSlab.Split must return the receiver as the LEFT child; if "+
			"this fails, splitRoot's old root becomes orphaned and "+
			"MutationCount must be bumped there too (currently it is not)")

	require.Equal(t, leftChild.SlabID(), oldRoot.SlabID(),
		"SetSlabID side effect is observable on the captured pointer")
}

// --- Array ---

func TestMutationCount_Array_StartsAtZero(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	require.Equal(t, uint64(0), array.MutationCount())
	require.Equal(t, uint64(0), atree.GetArrayRootSlab(array).MutationCount())
}

func TestMutationCount_Array_GetDoesNotBump(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	require.NoError(t, array.Append(testutils.Uint64Value(0)))
	require.NoError(t, array.Append(testutils.Uint64Value(1)))
	require.NoError(t, array.Append(testutils.Uint64Value(2)))

	for i := uint64(0); i < array.Count(); i++ {
		_, err := array.Get(i)
		require.NoError(t, err)
	}

	require.Equal(t, uint64(0), array.MutationCount())
}

func TestMutationCount_Array_ElementOpsBelowSplitDoNotBump(t *testing.T) {
	t.Parallel()

	// Keep the default threshold so a handful of appends stays in one slab.
	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < 10; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}
	for i := uint64(0); i < 10; i++ {
		_, err := array.Set(i, testutils.Uint64Value(i*2))
		require.NoError(t, err)
	}
	for i := uint64(0); i < 5; i++ {
		_, err := array.Remove(0)
		require.NoError(t, err)
	}
	for i := uint64(0); i < 3; i++ {
		require.NoError(t, array.Insert(0, testutils.Uint64Value(100+i)))
	}

	require.True(t, atree.GetArrayRootSlab(array).IsData(),
		"tree should still be a single data slab")
	require.Equal(t, uint64(0), array.MutationCount())
}

func TestMutationCount_Array_PopIterateBumpsOldRoot(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < 5; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}

	oldRoot := atree.GetArrayRootSlab(array)
	require.Equal(t, uint64(0), oldRoot.MutationCount())

	err = array.PopIterate(func(_ atree.Storable) {})
	require.NoError(t, err)

	require.NotSame(t, oldRoot, atree.GetArrayRootSlab(array),
		"PopIterate must replace the root with a fresh empty data slab")
	require.Equal(t, uint64(1), oldRoot.MutationCount(),
		"PopIterate must bump the orphaned old root exactly once")
	require.Equal(t, uint64(0), array.MutationCount(),
		"live MutationCount reads the fresh new root")
}

// TestMutationCount_Array_SplitRootDoesNotBumpCounter is a regression guard:
// the obvious "bump on every root swap" reading of the design would bump
// here too, but ArraySlab.Split returns the receiver as the left child, so
// the old root is not orphaned — it remains in the tree. Bumping it would
// pollute a later promote.
func TestMutationCount_Array_SplitRootDoesNotBumpCounter(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	oldRoot := atree.GetArrayRootSlab(array)
	originalSlabID := oldRoot.SlabID()
	require.Equal(t, uint64(0), oldRoot.MutationCount())

	for i := uint64(0); i < 200; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}

	require.False(t, atree.GetArrayRootSlab(array).IsData(),
		"prerequisite: a split must have occurred")
	require.NotSame(t, oldRoot, atree.GetArrayRootSlab(array))

	// splitRoot reuses oldRoot as the LEFT child. Its counter must remain
	// 0 — otherwise a later promoteChildAsNewRoot that re-promotes this
	// slab would make the live root carry a non-zero counter, producing a
	// false stale signal to the wrapper that initiated the operations.
	require.Equal(t, uint64(0), oldRoot.MutationCount(),
		"splitRoot must not bump the demoted slab")

	// Sibling wrappers detect splitRoot via the ValueID change instead:
	// splitRoot called SetSlabID on the demoted slab.
	require.NotEqual(t, originalSlabID, oldRoot.SlabID(),
		"splitRoot must change the demoted slab's SlabID — that is the "+
			"sibling-detection signal for splits")
}

func TestMutationCount_Array_RemoveUntilPromoteBumpsOldRoot(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Grow to multi-level.
	for i := uint64(0); i < 200; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}
	require.False(t, atree.GetArrayRootSlab(array).IsData(),
		"prerequisite: tree must be multi-level before testing promote")

	// Capture the metadata-slab root that promote will displace.
	oldMetaRoot := atree.GetArrayRootSlab(array)
	require.Equal(t, uint64(0), oldMetaRoot.MutationCount(),
		"the post-split root starts with a fresh counter (splitRoot does not bump)")

	// Remove until the tree collapses back to a single data slab.
	for array.Count() > 0 && !atree.GetArrayRootSlab(array).IsData() {
		_, err := array.Remove(0)
		require.NoError(t, err)
	}

	// Promote happened: root is now a data slab, with a different struct
	// identity than the metadata-slab root we captured.
	require.True(t, atree.GetArrayRootSlab(array).IsData(),
		"tree must have collapsed back to a single data slab via promote")
	require.NotSame(t, oldMetaRoot, atree.GetArrayRootSlab(array),
		"root struct must have changed identity after promoteChildAsNewRoot")

	// The orphaned metadata-slab root must have its counter bumped exactly
	// once. Cascading promotes (multi-level collapse) each bump their own
	// orphaned slab — but oldMetaRoot is the topmost, displaced by the
	// first promote in the cascade.
	require.Equal(t, uint64(1), oldMetaRoot.MutationCount(),
		"promoteChildAsNewRoot must bump the orphaned old root exactly once")

	// The live root is the freshly promoted slab — counter 0.
	require.Equal(t, uint64(0), array.MutationCount(),
		"live MutationCount reads the freshly promoted child")
}

// TestMutationCount_Array_SiblingObservesPromoteWithStableValueID is the
// canonical regression test for the gap promoteChildAsNewRoot leaves in the
// SlabID-based detection.
func TestMutationCount_Array_SiblingObservesPromoteWithStableValueID(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	// Grow to multi-level FIRST.
	for i := uint64(0); i < 200; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}
	require.False(t, atree.GetArrayRootSlab(array).IsData())

	// "Sibling" snapshot: a wrapper constructed at this point would cache
	// the live ValueID and MutationCount, both reflecting the multi-level
	// tree state.
	siblingRoot := atree.GetArrayRootSlab(array)
	cachedValueID := array.ValueID()
	cachedMutationCount := array.MutationCount()

	// Now collapse the tree via removes — triggers promoteChildAsNewRoot.
	for array.Count() > 0 && !atree.GetArrayRootSlab(array).IsData() {
		_, err := array.Remove(0)
		require.NoError(t, err)
	}
	require.True(t, atree.GetArrayRootSlab(array).IsData(),
		"prerequisite: a promote must have occurred")

	// The value ID is stable across promote — promote assigns the original
	// rootID to the promoted child and never perturbs the orphaned old
	// root's SlabID. This is the gap a SlabID-based check misses.
	require.Equal(t, cachedValueID, array.ValueID(),
		"ValueID is preserved across promote — this is the gap that "+
			"SlabID-based staleness checks miss")

	// What detects the gap: the orphaned old root's counter is bumped, and
	// a "sibling" view via siblingRoot.MutationCount() observes the bump.
	require.Greater(t, siblingRoot.MutationCount(), cachedMutationCount,
		"the orphaned old root's counter must diverge from the cached value")
}

// TestMutationCount_Array_NoFalseStaleForInitiatingWrapper covers the
// regression that motivated removing the splitRoot bump: a wrapper that
// itself initiates a grow+collapse cycle must not see a divergent live
// counter from its cached value.
func TestMutationCount_Array_NoFalseStaleForInitiatingWrapper(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	primaryCachedCount := array.MutationCount()
	require.Equal(t, uint64(0), primaryCachedCount)

	// Full grow + collapse cycle (multiple splits + multiple promotes).
	for i := uint64(0); i < 500; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}
	for array.Count() > 0 {
		_, err := array.Remove(0)
		require.NoError(t, err)
	}

	require.Equal(t, primaryCachedCount, array.MutationCount(),
		"the wrapper that initiated all operations must not see a false stale: "+
			"each swap bumps only the OLD root being detached, and the live "+
			"root is always a fresh slab with counter 0")
}

// TestMutationCount_Array_OrphanedRootIsNotFurtherBumped pins down the
// monotonicity contract: once a slab is detached, future operations through
// the *Array must not retroactively touch its counter.
func TestMutationCount_Array_OrphanedRootIsNotFurtherBumped(t *testing.T) {
	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	array, err := atree.NewArray(storage, address, typeInfo)
	require.NoError(t, err)

	for i := uint64(0); i < 5; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}

	// First PopIterate orphans the initial root.
	root0 := atree.GetArrayRootSlab(array)
	err = array.PopIterate(func(_ atree.Storable) {})
	require.NoError(t, err)
	require.Equal(t, uint64(1), root0.MutationCount())

	// Now run more operations through *Array. root0 is no longer accessed
	// by *Array.root, so its counter must remain 1.
	for i := uint64(0); i < 5; i++ {
		require.NoError(t, array.Append(testutils.Uint64Value(i)))
	}
	err = array.PopIterate(func(_ atree.Storable) {})
	require.NoError(t, err)

	require.Equal(t, uint64(1), root0.MutationCount(),
		"a slab no longer referenced by *Array must not receive further bumps")
}

// --- OrderedMap ---

func TestMutationCount_Map_StartsAtZero(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	require.Equal(t, uint64(0), m.MutationCount())
	require.Equal(t, uint64(0), atree.GetMapRootSlab(m).MutationCount())
}

func TestMutationCount_Map_GetDoesNotBump(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	for i := uint64(0); i < 5; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i*10))
		require.NoError(t, err)
	}
	for i := uint64(0); i < 5; i++ {
		_, err := m.Get(cmp, hip, testutils.Uint64Value(i))
		require.NoError(t, err)
	}

	require.Equal(t, uint64(0), m.MutationCount())
}

func TestMutationCount_Map_ElementOpsBelowSplitDoNotBump(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	for i := uint64(0); i < 5; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}
	for i := uint64(0); i < 5; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i*100))
		require.NoError(t, err)
	}
	for i := uint64(0); i < 2; i++ {
		_, _, err := m.Remove(cmp, hip, testutils.Uint64Value(i))
		require.NoError(t, err)
	}

	require.True(t, atree.GetMapRootSlab(m).IsData(),
		"tree should still be a single data slab")
	require.Equal(t, uint64(0), m.MutationCount())
}

func TestMutationCount_Map_PopIterateBumpsOldRoot(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	for i := uint64(0); i < 5; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}

	oldRoot := atree.GetMapRootSlab(m)
	require.Equal(t, uint64(0), oldRoot.MutationCount())

	err = m.PopIterate(func(_, _ atree.Storable) {})
	require.NoError(t, err)

	require.NotSame(t, oldRoot, atree.GetMapRootSlab(m),
		"PopIterate must replace the root with a fresh empty data slab")
	require.Equal(t, uint64(1), oldRoot.MutationCount(),
		"PopIterate must bump the orphaned old root exactly once")
	require.Equal(t, uint64(0), m.MutationCount(),
		"live MutationCount reads the fresh new root")
}

func TestMutationCount_Map_SplitRootDoesNotBumpCounter(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	oldRoot := atree.GetMapRootSlab(m)
	require.Equal(t, uint64(0), oldRoot.MutationCount())

	for i := uint64(0); i < 300; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}

	require.False(t, atree.GetMapRootSlab(m).IsData(),
		"prerequisite: a split must have occurred")
	require.NotSame(t, oldRoot, atree.GetMapRootSlab(m))
	require.Equal(t, uint64(0), oldRoot.MutationCount(),
		"splitRoot must not bump the demoted slab")
}

func TestMutationCount_Map_RemoveUntilPromoteBumpsOldRoot(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	for i := uint64(0); i < 300; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}
	require.False(t, atree.GetMapRootSlab(m).IsData())

	oldMetaRoot := atree.GetMapRootSlab(m)
	require.Equal(t, uint64(0), oldMetaRoot.MutationCount())

	for i := uint64(0); i < 300; i++ {
		if atree.GetMapRootSlab(m).IsData() {
			break
		}
		_, _, err := m.Remove(cmp, hip, testutils.Uint64Value(i))
		require.NoError(t, err)
	}

	require.True(t, atree.GetMapRootSlab(m).IsData(),
		"map must have collapsed back to a single data slab via promote")
	require.NotSame(t, oldMetaRoot, atree.GetMapRootSlab(m),
		"root struct must have changed identity after promoteChildAsNewRoot")
	require.Equal(t, uint64(1), oldMetaRoot.MutationCount(),
		"promoteChildAsNewRoot must bump exactly once")
	require.Equal(t, uint64(0), m.MutationCount(),
		"live MutationCount reads the fresh promoted child")
}

func TestMutationCount_Map_SiblingObservesPromoteWithStableValueID(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	for i := uint64(0); i < 300; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}
	require.False(t, atree.GetMapRootSlab(m).IsData())

	siblingRoot := atree.GetMapRootSlab(m)
	cachedValueID := m.ValueID()
	cachedMutationCount := m.MutationCount()

	for i := uint64(0); i < 300; i++ {
		if atree.GetMapRootSlab(m).IsData() {
			break
		}
		_, _, err := m.Remove(cmp, hip, testutils.Uint64Value(i))
		require.NoError(t, err)
	}
	require.True(t, atree.GetMapRootSlab(m).IsData())

	require.Equal(t, cachedValueID, m.ValueID(),
		"ValueID is preserved across promote — this is the gap the counter closes")
	require.Greater(t, siblingRoot.MutationCount(), cachedMutationCount,
		"the orphaned old root's counter must diverge from the cached value")
}

func TestMutationCount_Map_NoFalseStaleForInitiatingWrapper(t *testing.T) {
	atree.SetThreshold(256)
	defer atree.SetThreshold(1024)

	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	primaryCachedCount := m.MutationCount()
	require.Equal(t, uint64(0), primaryCachedCount)

	for i := uint64(0); i < 500; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}
	for i := uint64(0); i < 500; i++ {
		_, _, err := m.Remove(cmp, hip, testutils.Uint64Value(i))
		require.NoError(t, err)
	}

	require.Equal(t, primaryCachedCount, m.MutationCount(),
		"the wrapper that initiated the operations must not see a false stale")
}

func TestMutationCount_Map_OrphanedRootIsNotFurtherBumped(t *testing.T) {
	typeInfo := testutils.NewSimpleTypeInfo(42)
	storage := newTestPersistentStorage(t)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	m, err := atree.NewMap(storage, address, atree.NewDefaultDigesterBuilder(), typeInfo)
	require.NoError(t, err)

	cmp := testutils.CompareValue
	hip := testutils.GetHashInput

	for i := uint64(0); i < 5; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}

	root0 := atree.GetMapRootSlab(m)
	err = m.PopIterate(func(_, _ atree.Storable) {})
	require.NoError(t, err)
	require.Equal(t, uint64(1), root0.MutationCount())

	for i := uint64(0); i < 5; i++ {
		_, err := m.Set(cmp, hip, testutils.Uint64Value(i), testutils.Uint64Value(i))
		require.NoError(t, err)
	}
	err = m.PopIterate(func(_, _ atree.Storable) {})
	require.NoError(t, err)

	require.Equal(t, uint64(1), root0.MutationCount(),
		"a slab no longer referenced by *OrderedMap must not receive further bumps")
}
