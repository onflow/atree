/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

type Value interface{
	Storable() Storable
	DeepCopy(SlabStorage) (Value, error)
}