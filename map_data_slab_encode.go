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

import "fmt"

// Encode encodes this map data slab to the given encoder.
//
// Root DataSlab Header:
//
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | extra data (if root) | inlined extra data (if present) | next slab ID (if non-empty) |
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See InlinedExtraData.Encode() for inlined extra data section format.
// See hkeyElements.Encode() and singleElements.Encode() for elements section format.
func (m *MapDataSlab) Encode(enc *Encoder) error {

	if m.inlined {
		return m.encodeAsInlined(enc)
	}

	// Encoding is done in two steps:
	//
	// 1. Encode map elements using a new buffer while collecting inlined extra data from inlined elements.
	// 2. Encode slab with deduplicated inlined extra data and copy encoded elements from previous buffer.

	// Get a buffer from a pool to encode elements.
	elementBuf := getBuffer()
	defer putBuffer(elementBuf)

	elemEnc := NewEncoder(elementBuf, enc.encMode)

	err := m.encodeElements(elemEnc)
	if err != nil {
		return err
	}

	const version = 1

	slabType := slabMapData
	if m.collisionGroup {
		slabType = slabMapCollisionGroup
	}

	h, err := newMapSlabHead(version, slabType)
	if err != nil {
		return NewEncodingError(err)
	}

	if m.HasPointer() {
		h.setHasPointers()
	}

	if m.next != SlabIDUndefined {
		h.setHasNextSlabID()
	}

	if m.anySize {
		h.setNoSizeLimit()
	}

	if m.extraData != nil {
		h.setRoot()
	}

	if elemEnc.hasInlinedExtraData() {
		h.setHasInlinedSlabs()
	}

	// Encode head
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data
	if m.extraData != nil {
		// Use defaultEncodeTypeInfo to encode root level TypeInfo as is.
		err = m.extraData.Encode(enc, defaultEncodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapExtraData.Encode().
			return err
		}
	}

	// Encode inlined types
	if elemEnc.hasInlinedExtraData() {
		err = elemEnc.inlinedExtraData().Encode(enc)
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode next slab ID for non-root slab
	if m.next != SlabIDUndefined {
		n, err := m.next.ToRawBytes(enc.Scratch[:])
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by SlabID.ToRawBytes().
			return err
		}

		// Write scratch content to encoder
		_, err = enc.Write(enc.Scratch[:n])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode elements
	err = enc.CBOR.EncodeRawBytes(elementBuf.Bytes())
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (m *MapDataSlab) encodeElements(enc *Encoder) error {
	err := m.elements.Encode(enc)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Encode().
		return err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// encodeAsInlined encodes inlined map data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedMap,
// and tag contant as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
func (m *MapDataSlab) encodeAsInlined(enc *Encoder) error {
	if m.extraData == nil {
		return NewEncodingError(
			fmt.Errorf("failed to encode non-root map data slab as inlined"))
	}

	if !m.inlined {
		return NewEncodingError(
			fmt.Errorf("failed to encode standalone map data slab as inlined"))
	}

	if hkeys, keys, values, ok := m.canBeEncodedAsCompactMap(); ok {
		return encodeAsInlinedCompactMap(enc, m.header.slabID, m.extraData, hkeys, keys, values)
	}

	return m.encodeAsInlinedMap(enc)
}

func (m *MapDataSlab) encodeAsInlinedMap(enc *Encoder) error {

	extraDataIndex, err := enc.inlinedExtraData().addMapExtraData(m.extraData)
	if err != nil {
		// err is already categorized by InlinedExtraData.addMapExtraData().
		return err
	}

	if extraDataIndex > maxInlinedExtraDataIndex {
		return NewEncodingError(fmt.Errorf("extra data index %d exceeds limit %d", extraDataIndex, maxInlinedExtraDataIndex))
	}

	// Encode tag number and array head of 3 elements
	err = enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagInlinedMap,
		// array head of 3 elements
		0x83,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: extra data index
	// NOTE: encoded extra data index is fixed sized CBOR uint
	err = enc.CBOR.EncodeRawBytes([]byte{
		0x18,
		byte(extraDataIndex),
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 1: slab index
	err = enc.CBOR.EncodeBytes(m.header.slabID.index[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// element 2: map elements
	err = m.elements.Encode(enc)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Encode().
		return err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// encodeAsInlinedCompactMap encodes hkeys, keys, and values as inlined compact map value.
func encodeAsInlinedCompactMap(
	enc *Encoder,
	slabID SlabID,
	extraData *MapExtraData,
	hkeys []Digest,
	keys []ComparableStorable,
	values []Storable,
) error {

	extraDataIndex, cachedKeys, err := enc.inlinedExtraData().addCompactMapExtraData(extraData, hkeys, keys)
	if err != nil {
		// err is already categorized by InlinedExtraData.addCompactMapExtraData().
		return err
	}

	if len(keys) != len(cachedKeys) {
		return NewEncodingError(fmt.Errorf("number of elements %d is different from number of elements in cached compact map type %d", len(keys), len(cachedKeys)))
	}

	if extraDataIndex > maxInlinedExtraDataIndex {
		// This should never happen because of slab size.
		return NewEncodingError(fmt.Errorf("extra data index %d exceeds limit %d", extraDataIndex, maxInlinedExtraDataIndex))
	}

	// Encode tag number and array head of 3 elements
	err = enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagInlinedCompactMap,
		// array head of 3 elements
		0x83,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: extra data index
	// NOTE: encoded extra data index is fixed sized CBOR uint
	err = enc.CBOR.EncodeRawBytes([]byte{
		0x18,
		byte(extraDataIndex),
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 1: slab id
	err = enc.CBOR.EncodeBytes(slabID.index[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// element 2: compact map values in the order of cachedKeys
	err = encodeCompactMapValues(enc, cachedKeys, keys, values)
	if err != nil {
		// err is already categorized by encodeCompactMapValues().
		return err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// encodeCompactMapValues encodes compact values as an array of values ordered by cachedKeys.
func encodeCompactMapValues(
	enc *Encoder,
	cachedKeys []ComparableStorable,
	keys []ComparableStorable,
	values []Storable,
) error {

	var err error

	err = enc.CBOR.EncodeArrayHead(uint64(len(cachedKeys)))
	if err != nil {
		return NewEncodingError(err)
	}

	keyIndexes := make([]int, len(keys))
	for i := range keyIndexes {
		keyIndexes[i] = i
	}

	// Encode values in the same order as cachedKeys.
	for i, cachedKey := range cachedKeys {
		found := false
		for j := i; j < len(keyIndexes); j++ {
			index := keyIndexes[j]
			key := keys[index]

			if cachedKey.Equal(key) {
				found = true
				keyIndexes[i], keyIndexes[j] = keyIndexes[j], keyIndexes[i]

				err = values[index].Encode(enc)
				if err != nil {
					// Wrap err as external error (if needed) because err is returned by Storable interface.
					return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map value storable")
				}

				break
			}
		}
		if !found {
			return NewEncodingError(fmt.Errorf("failed to find key %v", cachedKey))
		}
	}

	return nil
}

// canBeEncodedAsCompactMap returns true if:
// - map data slab is inlined
// - map type is composite type
// - no collision elements
// - keys are stored inline (not in a separate slab)
func (m *MapDataSlab) canBeEncodedAsCompactMap() ([]Digest, []ComparableStorable, []Storable, bool) {
	if !m.inlined {
		return nil, nil, nil, false
	}

	if !m.extraData.TypeInfo.IsComposite() {
		return nil, nil, nil, false
	}

	elements, ok := m.elements.(*hkeyElements)
	if !ok {
		return nil, nil, nil, false
	}

	keys := make([]ComparableStorable, m.extraData.Count)
	values := make([]Storable, m.extraData.Count)

	for i, e := range elements.elems {
		se, ok := e.(*singleElement)
		if !ok {
			// Has collision element
			return nil, nil, nil, false
		}

		if _, ok = se.key.(SlabIDStorable); ok {
			// Key is stored in a separate slab
			return nil, nil, nil, false
		}

		key, ok := se.key.(ComparableStorable)
		if !ok {
			// Key can't be compared (sorted)
			return nil, nil, nil, false
		}

		keys[i] = key
		values[i] = se.value
	}

	return elements.hkeys, keys, values, true
}
