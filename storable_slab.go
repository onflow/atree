/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2021 Dapper Labs, Inc.
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

// StorableSlab allows storing storables (CBOR encoded data) directly in a slab.
// Eventually we will only have a dictionary at the account storage root,
// so this won't be needed, but during the refactor we have the need to store
// other non-dictionary values (e.g. strings, integers, etc.) directly in accounts
// (i.e. directly in slabs aka registers)
type StorableSlab struct {
	slabID   SlabID
	storable Storable
}

var _ Slab = &StorableSlab{}

func NewStorableSlab(storage SlabStorage, address Address, storable Storable) (Storable, error) {
	id, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf(
				"failed to generate slab ID for address 0x%x",
				address,
			),
		)
	}

	slab := &StorableSlab{
		slabID:   id,
		storable: storable,
	}

	err = storage.Store(id, slab)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
	}

	return SlabIDStorable(id), nil
}

func (s *StorableSlab) String() string {
	return fmt.Sprintf("StorableSlab id:%s storable:%s", s.slabID, s.storable)
}

func (s *StorableSlab) ChildStorables() []Storable {
	return []Storable{s.storable}
}

func (s *StorableSlab) Encode(enc *Encoder) error {

	const version = 1

	h, err := newStorableSlabHead(version)
	if err != nil {
		return NewEncodingError(err)
	}

	h.setNoSizeLimit()

	if hasPointer(s.storable) {
		h.setHasPointers()
	}

	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	err = s.storable.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode storable")
	}

	return nil
}

func (s *StorableSlab) ByteSize() uint32 {
	return versionAndFlagSize + s.storable.ByteSize()
}

func (s *StorableSlab) SlabID() SlabID {
	return s.slabID
}

func (s *StorableSlab) StoredValue(storage SlabStorage) (Value, error) {
	value, err := s.storable.StoredValue(storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}
	return value, nil
}

func (*StorableSlab) Split(_ SlabStorage) (Slab, Slab, error) {
	return nil, nil, NewNotApplicableError("StorableSlab", "Slab", "Split")
}

func (*StorableSlab) Merge(_ Slab) error {
	return NewNotApplicableError("StorableSlab", "Slab", "Merge")
}

func (*StorableSlab) LendToRight(_ Slab) error {
	return NewNotApplicableError("StorableSlab", "Slab", "LendToRight")
}

func (*StorableSlab) BorrowFromRight(_ Slab) error {
	return NewNotApplicableError("StorableSlab", "Slab", "BorrowFromRight")
}
