/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

type Value interface {
	Storable(SlabStorage, Address, uint64) (Storable, error)
	DeepCopy(SlabStorage, Address) (result Value, isCopy bool, err error)
	DeepRemove(storage SlabStorage) error
}

type ComparableValue interface {
	Value
	Hashable
	Equal(other Value) bool
}
