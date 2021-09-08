package atree

import (
	"encoding/binary"
	"errors"

	"github.com/dchest/siphash"
	orisanoV3 "github.com/orisano/wyhash/v3"
	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"
)

// xxh128DigesterBuilder creates Digester with XXH128 and BLAKE3.
type xxh128DigesterBuilder struct {
}

var _ DigesterBuilder = &xxh128DigesterBuilder{}

// xxh128pDigesterBuilder creates Digester with XXH128 with 8 bytes prefix and BLAKE3.
type xxh128pDigesterBuilder struct {
	k0 uint64
	k1 uint64
}

var _ DigesterBuilder = &xxh128pDigesterBuilder{}

// xxh128pDigesterBuilder creates Digester with XXH128 with 8 bytes prefix using Hasher interface and BLAKE3.
type xxh128pDigesterBuilder2 struct {
	k0 uint64
	k1 uint64
}

var _ DigesterBuilder = &xxh128pDigesterBuilder2{}

// wyv3DigesterBuilder creates Digester with wyhash v3 and BLAKE3.
type wyv3DigesterBuilder struct {
	k0 uint64
	k1 uint64
}

var _ DigesterBuilder = &wyv3DigesterBuilder{}

// HASH ALGO ALT: Replace SipHash128 with XXH128 for super fast first two levels:
//
//    XXH128 -- first 64 bit of digest (with prefix if prefix isn't empty)
//    XXH128 -- next 64 bit of digest
//    BLAKE3 -- first 64 bit of digest, without key
//    BLAKE3 -- next 64 bit of digest, without key
//    no more hashing, linear search all collisions
type xxh128Digester struct {
	xxh128Hash [2]uint64
	blake3Hash [4]uint64
	prefix     []byte
	msg        []byte
}

var _ Digester = &xxh128Digester{}

// Same as xxh128Digester using xxh3.Hasher.
type xxh128Digester2 struct {
	hasher     *xxh3.Hasher
	xxh128Hash [2]uint64
	blake3Hash [4]uint64
	prefix     []byte
	msg        []byte
}

var _ Digester = &xxh128Digester2{}

// HASH ALGO ALT: Use wyhash v3 before SipHash 128 for super fast first first level:
//
//    wyhash v3 -- first 64 bit of digest
//    SipHash -- first 64 bit of digest
//    SipHash -- next 64 bit of digest
//    BLAKE3 -- first 64 bit of digest, without key
//    BLAKE3 -- next 64 bit of digest, without key
//    no more hashing, linear search all collisions
type wyv3Digester struct {
	k0         uint64
	k1         uint64
	wyv3Hash   uint64
	sipHash    [2]uint64
	blake3Hash [4]uint64
	msg        []byte
}

var _ Digester = &wyv3Digester{}

var (
	emptyXXH128Hash [2]uint64
)

func newXXH128DigesterBuilder() *xxh128DigesterBuilder {
	return &xxh128DigesterBuilder{}
}

func (bdb *xxh128DigesterBuilder) SetSeed(_ uint64, _ uint64) {
	// noop
}

func (bdb *xxh128DigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	return &xxh128Digester{msg: msg}, nil
}

func newXXH128pDigesterBuilder() *xxh128pDigesterBuilder {
	return &xxh128pDigesterBuilder{}
}

func (bdb *xxh128pDigesterBuilder) SetSeed(k0 uint64, k1 uint64) {
	bdb.k0 = k0
	bdb.k1 = k1
}

func (bdb *xxh128pDigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	if bdb.k0 == 0 {
		return nil, NewHashError(errors.New("k0 is uninitialized"))
	}

	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, bdb.k0)

	return &xxh128Digester{prefix: prefix, msg: msg}, nil
}

func newXXH128pDigesterBuilder2() *xxh128pDigesterBuilder2 {
	return &xxh128pDigesterBuilder2{}
}

func (bdb *xxh128pDigesterBuilder2) SetSeed(k0 uint64, k1 uint64) {
	bdb.k0 = k0
	bdb.k1 = k1
}

func (bdb *xxh128pDigesterBuilder2) Digest(hashable Hashable) (Digester, error) {
	if bdb.k0 == 0 {
		return nil, NewHashError(errors.New("k0 is uninitialized"))
	}

	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, bdb.k0)

	return &xxh128Digester2{hasher: xxh3.New(), prefix: prefix, msg: msg}, nil
}

func (bd *xxh128Digester) DigestPrefix(level int) ([]Digest, error) {
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

func (bd *xxh128Digester) Digest(level int) (Digest, error) {
	if level >= bd.Levels() {
		return 0, errors.New("digest level out of bounds")
	}

	switch level {
	case 0, 1:
		{
			if bd.xxh128Hash == emptyXXH128Hash {
				var uint128 xxh3.Uint128

				if len(bd.prefix) == 0 {
					uint128 = xxh3.Hash128(bd.msg)
				} else {
					b := make([]byte, len(bd.prefix)+len(bd.msg))
					n := copy(b, bd.prefix)
					copy(b[n:], bd.msg)

					uint128 = xxh3.Hash128(b)
				}

				bd.xxh128Hash[0], bd.xxh128Hash[1] = uint128.Lo, uint128.Hi
			}
			return Digest(bd.xxh128Hash[level]), nil
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

func (bd *xxh128Digester) Levels() int {
	return 4
}

func (bd *xxh128Digester2) DigestPrefix(level int) ([]Digest, error) {
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

func (bd *xxh128Digester2) Digest(level int) (Digest, error) {
	if level >= bd.Levels() {
		return 0, errors.New("digest level out of bounds")
	}

	switch level {
	case 0, 1:
		{
			if bd.xxh128Hash == emptyXXH128Hash {
				var uint128 xxh3.Uint128

				if len(bd.prefix) == 0 {
					uint128 = xxh3.Hash128(bd.msg)
				} else {
					_, err := bd.hasher.Write(bd.prefix)
					if err != nil {
						return Digest(0), err
					}
					_, err = bd.hasher.Write(bd.msg)
					if err != nil {
						return Digest(0), err
					}
					uint128 = bd.hasher.Sum128()
				}

				bd.xxh128Hash[0], bd.xxh128Hash[1] = uint128.Lo, uint128.Hi
			}
			return Digest(bd.xxh128Hash[level]), nil
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

func (bd *xxh128Digester2) Levels() int {
	return 4
}
func newWYV3DigesterBuilder() *wyv3DigesterBuilder {
	return &wyv3DigesterBuilder{}
}

func (bdb *wyv3DigesterBuilder) SetSeed(k0 uint64, k1 uint64) {
	bdb.k0 = k0
	bdb.k1 = k1
}

func (bdb *wyv3DigesterBuilder) Digest(hashable Hashable) (Digester, error) {
	if bdb.k0 == 0 {
		return nil, NewHashError(errors.New("k0 is uninitialized"))
	}

	msg, err := hashable.HashCode()
	if err != nil {
		return nil, err
	}

	return &wyv3Digester{k0: bdb.k0, k1: bdb.k1, msg: msg}, nil
}

func (bd *wyv3Digester) DigestPrefix(level int) ([]Digest, error) {
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

func (bd *wyv3Digester) Digest(level int) (Digest, error) {
	if level >= bd.Levels() {
		return 0, errors.New("digest level out of bounds")
	}

	switch level {
	case 0:
		if bd.wyv3Hash == 0 {
			bd.wyv3Hash = orisanoV3.Sum64(bd.k0, bd.msg)
		}
		return Digest(bd.wyv3Hash), nil
	case 1, 2:
		{
			if bd.sipHash == emptySipHash {
				bd.sipHash[0], bd.sipHash[1] = siphash.Hash128(bd.k0, bd.k1, bd.msg)
			}
			return Digest(bd.sipHash[level-1]), nil
		}
	case 3, 4:
		{
			if bd.blake3Hash == emptyBlake3Hash {
				sum := blake3.Sum256(bd.msg)
				bd.blake3Hash[0] = binary.BigEndian.Uint64(sum[:])
				bd.blake3Hash[1] = binary.BigEndian.Uint64(sum[8:])
				bd.blake3Hash[2] = binary.BigEndian.Uint64(sum[16:])
				bd.blake3Hash[3] = binary.BigEndian.Uint64(sum[24:])
			}
			return Digest(bd.blake3Hash[level-3]), nil
		}
	default: // list mode
		return 0, nil
	}
}

func (bd *wyv3Digester) Levels() int {
	return 5
}
