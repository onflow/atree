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

func newElementFromData(
	cborDec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	slabID SlabID,
	inlinedExtraData []ExtraData,
) (element, error) {
	nt, err := cborDec.NextType()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	switch nt {
	case cbor.ArrayType:
		// Don't need to wrap error as external error because err is already categorized by newSingleElementFromData().
		return newSingleElementFromData(cborDec, decodeStorable, slabID, inlinedExtraData)

	case cbor.TagType:
		tagNum, err := cborDec.DecodeTagNumber()
		if err != nil {
			return nil, NewDecodingError(err)
		}
		switch tagNum {
		case CBORTagInlineCollisionGroup:
			// Don't need to wrap error as external error because err is already categorized by newInlineCollisionGroupFromData().
			return newInlineCollisionGroupFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
		case CBORTagExternalCollisionGroup:
			// Don't need to wrap error as external error because err is already categorized by newExternalCollisionGroupFromData().
			return newExternalCollisionGroupFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
		default:
			return nil, NewDecodingError(fmt.Errorf("failed to decode element: unrecognized tag number %d", tagNum))
		}

	default:
		return nil, NewDecodingError(fmt.Errorf("failed to decode element: unrecognized CBOR type %s", nt))
	}
}

func newSingleElementFromData(
	cborDec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	slabID SlabID,
	inlinedExtraData []ExtraData,
) (*singleElement, error) {
	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if elemCount != 2 {
		return nil, NewDecodingError(
			fmt.Errorf("failed to decode single element: expect array of 2 elements, got %d elements", elemCount),
		)
	}

	key, err := decodeStorable(cborDec, slabID, inlinedExtraData)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode key's storable")
	}

	value, err := decodeStorable(cborDec, slabID, inlinedExtraData)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode value's storable")
	}

	return &singleElement{
		key:   key,
		value: value,
		size:  singleElementPrefixSize + key.ByteSize() + value.ByteSize(),
	}, nil
}

func newInlineCollisionGroupFromData(
	cborDec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	slabID SlabID,
	inlinedExtraData []ExtraData,
) (*inlineCollisionGroup, error) {
	elements, err := newElementsFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromData().
		return nil, err
	}

	return &inlineCollisionGroup{elements}, nil
}

func newExternalCollisionGroupFromData(
	cborDec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	slabID SlabID,
	inlinedExtraData []ExtraData,
) (*externalCollisionGroup, error) {
	storable, err := decodeStorable(cborDec, slabID, inlinedExtraData)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode Storable")
	}

	idStorable, ok := storable.(SlabIDStorable)
	if !ok {
		return nil, NewDecodingError(fmt.Errorf("failed to decode external collision group: expect slab ID, got %T", storable))
	}

	return &externalCollisionGroup{
		slabID: SlabID(idStorable),
		size:   externalCollisionGroupPrefixSize + idStorable.ByteSize(),
	}, nil
}
