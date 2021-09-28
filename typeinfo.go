/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"github.com/fxamacker/cbor/v2"
)

type TypeInfo interface {
	Encode(*cbor.StreamEncoder) error
}

type TypeInfoDecoder func(
	decoder *cbor.StreamDecoder,
) (
	TypeInfo,
	error,
)
