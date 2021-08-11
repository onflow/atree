package atree

import (
	"encoding/binary"
	"errors"

	"github.com/dchest/siphash"
	"github.com/zeebo/blake3"
)

type Hashable interface {
	HashCode() ([]byte, error)
}

type Digest uint64

type DigesterBuilder interface {
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
	secretKey [16]byte
}

var _ DigesterBuilder = &basicDigesterBuilder{}

type basicDigester struct {
	secretKey  [16]byte
	sipHash    [2]uint64
	blake3Hash [4]uint64
	msg        []byte
}

var (
	emptySipHash    [2]uint64
	emptyBlake3Hash [4]uint64
)

func NewBasicDigesterBuilder(key [16]byte) *basicDigesterBuilder {
	return &basicDigesterBuilder{secretKey: key}
}

func (bdb *basicDigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	return &basicDigester{secretKey: bdb.secretKey, msg: msg}, nil
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
				k0 := binary.BigEndian.Uint64(bd.secretKey[:])
				k1 := binary.BigEndian.Uint64(bd.secretKey[8:])
				bd.sipHash[0], bd.sipHash[1] = siphash.Hash128(k0, k1, bd.msg)
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

func (d *basicDigester) Levels() int {
	return 4
}
