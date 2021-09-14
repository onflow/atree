/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

type Value interface {
	Storable(SlabStorage, Address, uint64) (Storable, error)
}

type ComparableValue interface {
	Value
	Hashable
}

type Comparator func(SlabStorage, Value, Storable) (bool, error)
