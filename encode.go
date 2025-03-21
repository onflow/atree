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
	"io"
	"math"

	"github.com/SophisticaSean/cbor/v2"
)

// Encoder writes atree slabs to io.Writer.
type Encoder struct {
	io.Writer
	CBOR              *cbor.StreamEncoder
	Scratch           [64]byte
	encMode           cbor.EncMode
	_inlinedExtraData *InlinedExtraData
}

func NewEncoder(w io.Writer, encMode cbor.EncMode) *Encoder {
	streamEncoder := encMode.NewStreamEncoder(w)
	return &Encoder{
		Writer:  w,
		CBOR:    streamEncoder,
		encMode: encMode,
	}
}

func (enc *Encoder) inlinedExtraData() *InlinedExtraData {
	if enc._inlinedExtraData == nil {
		enc._inlinedExtraData = newInlinedExtraData()
	}
	return enc._inlinedExtraData
}

func (enc *Encoder) hasInlinedExtraData() bool {
	if enc._inlinedExtraData == nil {
		return false
	}
	return !enc._inlinedExtraData.empty()
}

func EncodeSlab(slab Slab, encMode cbor.EncMode) ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf, encMode)

	err := slab.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode storable")
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return nil, NewEncodingError(err)
	}

	return buf.Bytes(), nil
}

func GetUintCBORSize(n uint64) uint32 {
	if n <= 23 {
		return 1
	}
	if n <= math.MaxUint8 {
		return 2
	}
	if n <= math.MaxUint16 {
		return 3
	}
	if n <= math.MaxUint32 {
		return 5
	}
	return 9
}
