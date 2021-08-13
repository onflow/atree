/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import "errors"

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

// TODO: make it inline.
func IsRootOfAnObject(slabData []byte) (bool, error) {
	if len(slabData) < 2 {
		return false, errors.New("slab data is too short")
	}

	flag := slabData[1]

	return isRoot(flag), nil
}

// TODO: make it inline.
func HasPointers(slabData []byte) (bool, error) {
	if len(slabData) < 2 {
		return false, errors.New("slab data is too short")
	}

	flag := slabData[1]

	return hasPointers(flag), nil
}

// TODO: make it inline.
func HasSizeLimit(slabData []byte) (bool, error) {
	if len(slabData) < 2 {
		return false, errors.New("slab data is too short")
	}

	flag := slabData[1]

	return hasSizeLimit(flag), nil
}
