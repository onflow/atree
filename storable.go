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

type Storable interface {
	Encode(*Encoder) error

	ByteSize() uint32

	StoredValue(storage SlabStorage) (Value, error)

	// ChildStorables only returns child storables in this storable
	// (not recursive).  This function shouldn't load extra slabs.
	ChildStorables() []Storable
}

// ComparableStorable is an interface that supports comparison and cloning of Storable.
// This is only used for compact keys.
type ComparableStorable interface {
	Storable

	// Equal returns true if the given storable is equal to this storable.
	Equal(Storable) bool

	// Less returns true if the given storable is less than this storable.
	Less(Storable) bool

	// ID returns a unique identifier.
	ID() string

	Copy() Storable
}

// ContainerStorable is an interface that supports Storable containing other storables.
type ContainerStorable interface {
	Storable

	// HasPointer returns true if any of its child storables is SlabIDStorable
	// (references to another slab).  This function is used during encoding.
	HasPointer() bool
}

// WrapperStorable is an interface that supports storable wrapping another storable.
type WrapperStorable interface {
	Storable

	// UnwrapAtreeStorable returns innermost wrapped Storable.
	UnwrapAtreeStorable() Storable

	// WrapAtreeStorable returns a new WrapperStorable with given storable as innermost wrapped storable.
	WrapAtreeStorable(Storable) Storable
}

func hasPointer(storable Storable) bool {
	if cs, ok := storable.(ContainerStorable); ok {
		return cs.HasPointer()
	}
	return false
}

func unwrapStorable(s Storable) Storable {
	switch s := s.(type) {
	case WrapperStorable:
		return s.UnwrapAtreeStorable()
	default:
		return s
	}
}

const (
	// WARNING: tag numbers defined in here in github.com/onflow/atree
	// MUST not overlap with tag numbers used by Cadence internal value encoding.
	// As of Aug. 14, 2024, Cadence uses tag numbers from 128 to 230.
	// See runtime/interpreter/encode.go at github.com/onflow/cadence.

	// Atree reserves CBOR tag numbers [240, 255] for internal use.
	// Applications must use non-overlapping CBOR tag numbers to encode
	// elements managed by atree containers.
	minInternalCBORTagNumber = 240
	maxInternalCBORTagNumber = 255

	// Reserved CBOR tag numbers for atree internal use.

	// Replace _ when new tag number is needed (use higher tag numbers first).
	// Atree will use higher tag numbers first because Cadence will use lower tag numbers first.
	// This approach allows more flexibility in case we need to revisit ranges used by Atree and Cadence.

	_ = 240
	_ = 241
	_ = 242
	_ = 243
	_ = 244
	_ = 245

	CBORTagTypeInfoRef = 246

	CBORTagInlinedArrayExtraData      = 247
	CBORTagInlinedMapExtraData        = 248
	CBORTagInlinedCompactMapExtraData = 249

	CBORTagInlinedArray      = 250
	CBORTagInlinedMap        = 251
	CBORTagInlinedCompactMap = 252

	CBORTagInlineCollisionGroup   = 253
	CBORTagExternalCollisionGroup = 254

	CBORTagSlabID = 255
)

// IsCBORTagNumberRangeAvailable returns true if the specified range is not reserved for internal use by atree.
// Applications must only use available (unreserved) CBOR tag numbers to encode elements in atree managed containers.
func IsCBORTagNumberRangeAvailable(minTagNum, maxTagNum uint64) (bool, error) {
	if minTagNum > maxTagNum {
		return false, NewUserError(fmt.Errorf("min CBOR tag number %d must be <= max CBOR tag number %d", minTagNum, maxTagNum))
	}

	return maxTagNum < minInternalCBORTagNumber || minTagNum > maxInternalCBORTagNumber, nil
}

// ReservedCBORTagNumberRange returns minTagNum and maxTagNum of the range of CBOR tag numbers
// reserved for internal use by atree.
func ReservedCBORTagNumberRange() (minTagNum, maxTagNum uint64) {
	return minInternalCBORTagNumber, maxInternalCBORTagNumber
}

type SlabIDStorable SlabID

var _ ContainerStorable = SlabIDStorable{}

func (v SlabIDStorable) HasPointer() bool {
	return true
}

func (v SlabIDStorable) ChildStorables() []Storable {
	return nil
}

func (v SlabIDStorable) StoredValue(storage SlabStorage) (Value, error) {
	id := SlabID(v)
	if err := id.Valid(); err != nil {
		// Don't need to wrap error as external error because err is already categorized by SlabID.Valid().
		return nil, err
	}

	slab, found, err := storage.Retrieve(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "slab not found for stored value")
	}
	value, err := slab.StoredValue(storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}
	return value, nil
}

// Encode encodes SlabIDStorable as
//
//	cbor.Tag{
//			Number:  cborTagSlabID,
//			Content: byte(v),
//	}
func (v SlabIDStorable) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagSlabID,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	copy(enc.Scratch[:], v.address[:])
	copy(enc.Scratch[8:], v.index[:])

	err = enc.CBOR.EncodeBytes(enc.Scratch[:SlabIDLength])
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (v SlabIDStorable) ByteSize() uint32 {
	// tag number (2 bytes) + byte string header (1 byte) + slab id (16 bytes)
	return 2 + 1 + SlabIDLength
}

func (v SlabIDStorable) String() string {
	return fmt.Sprintf("SlabIDStorable(%d)", v)
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

func DecodeSlabIDStorable(dec *cbor.StreamDecoder) (Storable, error) {
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	id, err := NewSlabIDFromRawBytes(b)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
		return nil, err
	}

	return SlabIDStorable(id), nil
}

func getLoadedValue(storage SlabStorage, storable Storable) (Value, error) {
	switch storable := storable.(type) {
	case SlabIDStorable:
		slab := storage.RetrieveIfLoaded(SlabID(storable))
		if slab == nil {
			// Skip because it references unloaded slab.
			return nil, nil
		}

		v, err := slab.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil

	case WrapperStorable:
		// Check if wrapped storable is SlabIDStorable.
		wrappedStorable := unwrapStorable(storable)

		if wrappedSlabIDStorable, isSlabIDStorable := wrappedStorable.(SlabIDStorable); isSlabIDStorable {
			slab := storage.RetrieveIfLoaded(SlabID(wrappedSlabIDStorable))
			if slab == nil {
				// Skip because it references unloaded slab.
				return nil, nil
			}
		}

		v, err := storable.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil

	default:
		v, err := storable.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil
	}
}
