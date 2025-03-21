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
	"fmt"

	"github.com/SophisticaSean/cbor/v2"
)

type ArrayExtraData struct {
	TypeInfo TypeInfo // array type
}

var _ ExtraData = &ArrayExtraData{}

const arrayExtraDataLength = 1

func newArrayExtraDataFromData(
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*ArrayExtraData,
	[]byte,
	error,
) {
	dec := decMode.NewByteStreamDecoder(data)

	extraData, err := newArrayExtraData(dec, decodeTypeInfo)
	if err != nil {
		return nil, data, err
	}

	return extraData, data[dec.NumBytesDecoded():], nil
}

// newArrayExtraData decodes CBOR array to extra data:
//
//	cborArray{type info}
func newArrayExtraData(dec *cbor.StreamDecoder, decodeTypeInfo TypeInfoDecoder) (*ArrayExtraData, error) {
	length, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if length != arrayExtraDataLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"array extra data has invalid length %d, want %d",
				length,
				arrayExtraDataLength,
			))
	}

	typeInfo, err := decodeTypeInfo(dec)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfoDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode type info")
	}

	return &ArrayExtraData{TypeInfo: typeInfo}, nil
}

// Encode encodes extra data as CBOR array:
//
//	[type info]
func (a *ArrayExtraData) Encode(enc *Encoder, encodeTypeInfo encodeTypeInfo) error {
	err := enc.CBOR.EncodeArrayHead(arrayExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	err = encodeTypeInfo(enc, a.TypeInfo)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfo interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode type info")
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (a *ArrayExtraData) isExtraData() bool {
	return true
}

func (a *ArrayExtraData) Type() TypeInfo {
	return a.TypeInfo
}
