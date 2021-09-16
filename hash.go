package atree

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/dchest/siphash"
	"github.com/zeebo/blake3"
)

type Hashable interface {
	GetHashInput([]byte) ([]byte, error)
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

	// Reset data for reuse
	Reset()

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
	scratch    [32]byte
	msg        []byte
}

// basicDigesterPool caches unused basicDigester objects for later reuse.
var basicDigesterPool = sync.Pool{
	New: func() interface{} {
		return &basicDigester{}
	},
}

func getBasicDigester() *basicDigester {
	return basicDigesterPool.Get().(*basicDigester)
}

func putBasicDigester(e Digester) {
	if _, ok := e.(*basicDigester); !ok {
		return
	}
	e.Reset()
	basicDigesterPool.Put(e)
}

var (
	emptySipHash    [2]uint64
	emptyBlake3Hash [4]uint64
)

func NewDefaultDigesterBuilder() DigesterBuilder {
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

	digester := getBasicDigester()

	msg, err := hashable.GetHashInput(digester.scratch[:])
	if err != nil {
		putBasicDigester(digester)
		return nil, err
	}

	digester.k0 = bdb.k0
	digester.k1 = bdb.k1
	digester.msg = msg
	return digester, nil
}

func (bd *basicDigester) Reset() {
	bd.k0 = 0
	bd.k1 = 0
	bd.sipHash = emptySipHash
	bd.blake3Hash = emptyBlake3Hash
	bd.msg = nil
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
