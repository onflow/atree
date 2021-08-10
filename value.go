/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

type Value interface {
	Storable(SlabStorage, Address) (Storable, error)
	DeepCopy(SlabStorage, Address) (Value, error)
}

type ComparableValue interface {
	Value
	Hashable
	Equal(other Value) bool
}
