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

	cbor "github.com/fxamacker/cbor/v2/cborstream"
)

type MapExtraData struct {
	TypeInfo TypeInfo
	Count    uint64
	Seed     uint64
}

var _ ExtraData = &MapExtraData{}

const mapExtraDataLength = 3

// newMapExtraDataFromData decodes CBOR array to extra data:
//
//	[type info, count, seed]
func newMapExtraDataFromData(
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapExtraData,
	[]byte,
	error,
) {
	dec := decMode.NewByteStreamDecoder(data)

	extraData, err := newMapExtraData(dec, decodeTypeInfo)
	if err != nil {
		return nil, data, err
	}

	return extraData, data[dec.NumBytesDecoded():], nil
}

func newMapExtraData(dec *cbor.StreamDecoder, decodeTypeInfo TypeInfoDecoder) (*MapExtraData, error) {

	length, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if length != mapExtraDataLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"data has invalid length %d, want %d",
				length,
				mapExtraDataLength,
			))
	}

	typeInfo, err := decodeTypeInfo(dec)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfoDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode type info")
	}

	count, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	seed, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	return &MapExtraData{
		TypeInfo: typeInfo,
		Count:    count,
		Seed:     seed,
	}, nil
}

func (m *MapExtraData) isExtraData() bool {
	return true
}

func (m *MapExtraData) Type() TypeInfo {
	return m.TypeInfo
}

// Encode encodes extra data as CBOR array:
//
//	[type info, count, seed]
func (m *MapExtraData) Encode(enc *Encoder, encodeTypeInfo encodeTypeInfo) error {

	err := enc.CBOR.EncodeArrayHead(mapExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	err = encodeTypeInfo(enc, m.TypeInfo)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfo interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode type info")
	}

	err = enc.CBOR.EncodeUint64(m.Count)
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.EncodeUint64(m.Seed)
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}
