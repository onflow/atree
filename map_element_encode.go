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

// Encode encodes singleElement to the given encoder.
//
//	CBOR encoded array of 2 elements (key, value).
func (e *singleElement) Encode(enc *Encoder) error {

	// Encode CBOR array head for 2 elements
	err := enc.CBOR.EncodeRawBytes([]byte{0x82})
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode key
	err = e.key.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map key storable")
	}

	// Encode value
	err = e.value.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map value storable")
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// Encode encodes inlineCollisionGroup to the given encoder.
//
//	CBOR tag (number: CBORTagInlineCollisionGroup, content: elements)
func (e *inlineCollisionGroup) Encode(enc *Encoder) error {

	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number CBORTagInlineCollisionGroup
		0xd8, CBORTagInlineCollisionGroup,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	err = e.elements.Encode(enc)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Encode().
		return err
	}

	// TODO: is Flush necessary?
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// Encode encodes externalCollisionGroup to the given encoder.
//
//	CBOR tag (number: CBORTagExternalCollisionGroup, content: slab ID)
func (e *externalCollisionGroup) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number CBORTagExternalCollisionGroup
		0xd8, CBORTagExternalCollisionGroup,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	err = SlabIDStorable(e.slabID).Encode(enc)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.Encode().
		return err
	}

	// TODO: is Flush necessary?
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}
