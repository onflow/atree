package atree

import (
	"errors"
)

// StorableSlab allows storing storables (CBOR encoded data) directly in a slab.
// Eventually we will only have a dictionary at the account storage root,
// so this won't be needed, but during the refactor we have the need to store
// other non-dictionary values (e.g. strings, integers, etc.) directly in accounts
// (i.e. directly in slabs aka registers)
//
type StorableSlab struct {
	StorageID StorageID
	Storable  Storable
}

var _ Slab = StorableSlab{}

func (s StorableSlab) Encode(enc *Encoder) error {
	// Encode version
	enc.Scratch[0] = 0

	// Encode flag
	enc.Scratch[1] = flagStorable

	const versionAndFlagSize = 2
	_, err := enc.Write(enc.Scratch[:versionAndFlagSize])
	if err != nil {
		return err
	}

	return s.Storable.Encode(enc)
}

func (s StorableSlab) ByteSize(storage SlabStorage) uint32 {
	const versionAndFlagSize = 2
	return versionAndFlagSize + s.Storable.ByteSize(storage)
}

func (s StorableSlab) ID() StorageID {
	return s.StorageID
}

func (s StorableSlab) StoredValue(storage SlabStorage) (Value, error) {
	return s.Storable.StoredValue(storage)
}

func (StorableSlab) Split(_ SlabStorage) (Slab, Slab, error) {
	return nil, nil, errors.New("not applicable")
}

func (StorableSlab) Merge(_ Slab, _ SlabStorage) error {
	return errors.New("not applicable")
}

func (StorableSlab) LendToRight(_ Slab, _ SlabStorage) error {
	return errors.New("not applicable")
}

func (StorableSlab) BorrowFromRight(_ Slab, _ SlabStorage) error {
	return errors.New("not applicable")
}
