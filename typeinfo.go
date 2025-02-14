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

package atree

import (
	"bytes"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

type TypeInfo interface {
	Encode(*cbor.StreamEncoder) error
	IsComposite() bool
	Copy() TypeInfo
}

type TypeInfoDecoder func(
	decoder *cbor.StreamDecoder,
) (
	TypeInfo,
	error,
)

// encodeTypeInfo encodes TypeInfo either:
// - as is (for TypeInfo in root slab extra data section), or
// - as index of inlined TypeInfos (for TypeInfo in inlined slab extra data section)
type encodeTypeInfo func(*Encoder, TypeInfo) error

// defaultEncodeTypeInfo encodes TypeInfo as is.
func defaultEncodeTypeInfo(enc *Encoder, typeInfo TypeInfo) error {
	return typeInfo.Encode(enc.CBOR)
}

func decodeTypeInfoRefIfNeeded(inlinedTypeInfo []TypeInfo, defaultTypeInfoDecoder TypeInfoDecoder) TypeInfoDecoder {
	if len(inlinedTypeInfo) == 0 {
		return defaultTypeInfoDecoder
	}

	return func(decoder *cbor.StreamDecoder) (TypeInfo, error) {
		rawTypeInfo, err := decoder.DecodeRawBytes()
		if err != nil {
			return nil, NewDecodingError(fmt.Errorf("failed to decode raw type info: %w", err))
		}

		if len(rawTypeInfo) > len(typeInfoRefTagHeadAndTagNumber) &&
			bytes.Equal(
				rawTypeInfo[:len(typeInfoRefTagHeadAndTagNumber)],
				typeInfoRefTagHeadAndTagNumber) {

			// Type info is encoded as type info ref.

			var index uint64

			err = cbor.Unmarshal(rawTypeInfo[len(typeInfoRefTagHeadAndTagNumber):], &index)
			if err != nil {
				return nil, NewDecodingError(err)
			}

			if index >= uint64(len(inlinedTypeInfo)) {
				return nil, NewDecodingError(fmt.Errorf("failed to decode type info ref: expect index < %d, got %d", len(inlinedTypeInfo), index))
			}

			return inlinedTypeInfo[int(index)], nil
		}

		// Decode type info as is.

		dec := cbor.NewByteStreamDecoder(rawTypeInfo)

		return defaultTypeInfoDecoder(dec)
	}
}
