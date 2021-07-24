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
	Storable
}

func (StorableSlab) Split(_ SlabStorage) (Slab, Slab, error) {
	return nil, nil, errors.New("not applicable")
}

func (StorableSlab) Merge(Slab) error {
	return errors.New("not applicable")
}

func (StorableSlab) LendToRight(Slab) error {
	return errors.New("not applicable")
}

func (StorableSlab) BorrowFromRight(Slab) error {
	return errors.New("not applicable")
}

