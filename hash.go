package atree

import (
	"encoding/binary"

	"github.com/aead/siphash"
)

type Hashable interface {
	HashCode() ([]byte, error)
}

type Hasher interface {
	Hash(Hashable) ([]uint64, error)
	DigestSize() int
}

type sipHash128 struct {
	secretKey [16]byte
}

var _ Hasher = &sipHash128{}

func (h *sipHash128) Hash(hashable Hashable) ([]uint64, error) {
	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	b := siphash.Sum128(msg, &h.secretKey)

	h1 := binary.BigEndian.Uint64(b[:])
	h2 := binary.BigEndian.Uint64(b[8:])
	return []uint64{h1, h2}, nil
}

func (h *sipHash128) DigestSize() int {
	return 16
}
