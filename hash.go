package atree

import (
	"encoding/binary"
	"errors"

	"github.com/aead/siphash"
)

type Hashable interface {
	HashCode() ([]byte, error)
}

type Hasher interface {
	Digest(Hashable) (Digest, error)
	DigestSize() int
}

type Digest interface {
	// DigestPrefix returns digest before specified level.
	// If level is 0, DigestPrefix returns nil.
	DigestPrefix(level int) ([]uint64, error)

	// Digest returns digest at specified level.
	Digest(level int) (uint64, error)

	// Size returns total digest size in bytes.
	Size() int

	Levels() int
}

type sipHash128 struct {
	secretKey [16]byte
}

var _ Hasher = &sipHash128{}

type sipHash128Digest struct {
	d []uint64
}

var _ Digest = &sipHash128Digest{}

func (h *sipHash128) Digest(hashable Hashable) (Digest, error) {
	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	b := siphash.Sum128(msg, &h.secretKey)

	h1 := binary.BigEndian.Uint64(b[:])
	h2 := binary.BigEndian.Uint64(b[8:])

	return sipHash128Digest{[]uint64{h1, h2}}, nil
}

func (h *sipHash128) DigestSize() int {
	return 16
}

func (d sipHash128Digest) DigestPrefix(level int) ([]uint64, error) {
	if level > len(d.d) {
		return nil, errors.New("digest level out of bounds")
	}
	return d.d[:level], nil
}

func (d sipHash128Digest) Digest(level int) (uint64, error) {
	if level >= len(d.d) {
		return 0, errors.New("digest level out of bounds")
	}
	return d.d[level], nil
}

func (d sipHash128Digest) Size() int {
	return 16
}

func (d sipHash128Digest) Levels() int {
	return len(d.d)
}
