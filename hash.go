/*
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

import (
	"encoding/binary"
	"sync"

	"github.com/fxamacker/circlehash"
	"github.com/zeebo/blake3"
)

type HashInputProvider func(value Value, buffer []byte) ([]byte, error)

type Digest uint64

type DigesterBuilder interface {
	SetSeed(k0 uint64, k1 uint64)
	Digest(HashInputProvider, Value) (Digester, error)
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
	circleHash64 uint64
	blake3Hash   [4]uint64
	scratch      [32]byte
	msg          []byte
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

func putDigester(e Digester) {
	if _, ok := e.(*basicDigester); !ok {
		return
	}
	e.Reset()
	basicDigesterPool.Put(e)
}

var (
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

func (bdb *basicDigesterBuilder) Digest(hip HashInputProvider, value Value) (Digester, error) {
	if bdb.k0 == 0 {
		return nil, NewHashSeedUninitializedError()
	}

	digester := getBasicDigester()

	msg, err := hip(value, digester.scratch[:])
	if err != nil {
		putDigester(digester)
		return nil, err
	}

	digester.msg = msg
	digester.circleHash64 = circlehash.Hash64(msg, bdb.k0)

	return digester, nil
}

func (bd *basicDigester) Reset() {
	bd.circleHash64 = 0
	bd.blake3Hash = emptyBlake3Hash
	bd.msg = nil
}

func (bd *basicDigester) DigestPrefix(level int) ([]Digest, error) {
	if level > bd.Levels() {
		// level must be [0, bd.Levels()] (inclusive) for prefix
		return nil, NewHashLevelErrorf("cannot get digest < level %d: level must be [0, %d]", level, bd.Levels())
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
		// level must be [0, bd.Levels()) (not inclusive) for digest
		return 0, NewHashLevelErrorf("cannot get digest at level %d: level must be [0, %d)", level, bd.Levels())
	}

	switch level {
	case 0:
		return Digest(bd.circleHash64), nil

	case 1, 2, 3:
		if bd.blake3Hash == emptyBlake3Hash {
			sum := blake3.Sum256(bd.msg)
			bd.blake3Hash[0] = binary.BigEndian.Uint64(sum[:])
			bd.blake3Hash[1] = binary.BigEndian.Uint64(sum[8:])
			bd.blake3Hash[2] = binary.BigEndian.Uint64(sum[16:])
			bd.blake3Hash[3] = binary.BigEndian.Uint64(sum[24:])
		}
		return Digest(bd.blake3Hash[level-1]), nil

	default: // list mode
		return 0, nil
	}
}

func (bd *basicDigester) Levels() int {
	return 4
}
