/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

type Slab interface {
	Storable

	ID() StorageID
	Split(SlabStorage) (Slab, Slab, error)
	Merge(Slab) error
	// LendToRight rebalances slabs by moving elements from left to right
	LendToRight(Slab) error
	// BorrowFromRight rebalances slabs by moving elements from right to left
	BorrowFromRight(Slab) error
}
