package atree

import (
	"encoding/binary"
	"errors"

	"github.com/dchest/siphash"
	"github.com/zeebo/blake3"
)

type Hashable interface {
	GetHashInput() ([]byte, error)
}

type Digest uint64

type DigesterBuilder interface {
	SetSeed(k0 uint64, k1 uint64)
	Digest(Hashable) (Digester, error)
}

type Digester interface {
	// DigestPrefix returns digests before specified level.
	// If level is 0, DigestPrefix returns nil.
	DigestPrefix(level int) ([]Digest, error)

	// Digest returns digest at specified level.
	Digest(level int) (Digest, error)

	Levels() int
}

type basicDigesterBuilder struct {
	k0 uint64
	k1 uint64
}

var _ DigesterBuilder = &basicDigesterBuilder{}

type basicDigester struct {
	k0         uint64
	k1         uint64
	sipHash    [2]uint64
	blake3Hash [4]uint64
	msg        []byte
}

var (
	emptySipHash    [2]uint64
	emptyBlake3Hash [4]uint64
)

func newDefaultDigesterBuilder() DigesterBuilder {
	return newBasicDigesterBuilder()
}

func newBasicDigesterBuilder() *basicDigesterBuilder {
	return &basicDigesterBuilder{}
}

func (bdb *basicDigesterBuilder) SetSeed(k0 uint64, k1 uint64) {
	bdb.k0 = k0
	bdb.k1 = k1
}

func (bdb *basicDigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	if bdb.k0 == 0 {
		return nil, NewHashError(errors.New("k0 is uninitialized"))
	}

	msg, err := hashable.GetHashInput()
	if err != nil {
		return nil, err
	}

	return &basicDigester{k0: bdb.k0, k1: bdb.k1, msg: msg}, nil
}

func (bd *basicDigester) DigestPrefix(level int) ([]Digest, error) {
	if level > bd.Levels() {
		return nil, errors.New("digest level out of bounds")
	}
	var prefix []Digest
	for i := 0; i < level; i++ {
		d, err := bd.Digest(i)
		if err != nil {
			return nil, err
		}
		prefix = append(prefix, d)
	}
	return prefix, nil
}

func (bd *basicDigester) Digest(level int) (Digest, error) {
	if level >= bd.Levels() {
		return 0, errors.New("digest level out of bounds")
	}

	switch level {
	case 0, 1:
		{
			if bd.sipHash == emptySipHash {
				bd.sipHash[0], bd.sipHash[1] = siphash.Hash128(bd.k0, bd.k1, bd.msg)
			}
			return Digest(bd.sipHash[level]), nil
		}
	case 2, 3:
		{
			if bd.blake3Hash == emptyBlake3Hash {
				sum := blake3.Sum256(bd.msg)
				bd.blake3Hash[0] = binary.BigEndian.Uint64(sum[:])
				bd.blake3Hash[1] = binary.BigEndian.Uint64(sum[8:])
				bd.blake3Hash[2] = binary.BigEndian.Uint64(sum[16:])
				bd.blake3Hash[3] = binary.BigEndian.Uint64(sum[24:])
			}
			return Digest(bd.blake3Hash[level-2]), nil
		}
	default: // list mode
		return 0, nil
	}
}

func (bd *basicDigester) Levels() int {
	return 4
}
