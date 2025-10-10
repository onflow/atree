/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright Flow Foundation
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

package atree_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	"github.com/onflow/atree"
	testutils "github.com/onflow/atree/test_utils"
)

type collisionDigesterBuilder struct {
	digest            uint64
	collisionCount    uint32
	maxCollisionCount uint32
}

var _ atree.DigesterBuilder = &collisionDigesterBuilder{}

func NewCollisionDigesterBuilder(maxCollisionLimitPerDigest uint32) atree.DigesterBuilder {
	return &collisionDigesterBuilder{
		maxCollisionCount: maxCollisionLimitPerDigest + 1,
	}
}

func (db *collisionDigesterBuilder) Digest(hip atree.HashInputProvider, value atree.Value) (atree.Digester, error) {

	if db.collisionCount < db.maxCollisionCount {
		db.collisionCount++
	} else {
		db.digest++
		db.collisionCount = 0
	}
	firstLevelHash := db.digest

	var scratch [32]byte
	msg, err := hip(value, scratch[:])
	if err != nil {
		return nil, err
	}

	return &collisionDigester{
		firstLevelHash: firstLevelHash,
		msg:            msg,
	}, nil
}

func (db *collisionDigesterBuilder) SetSeed(uint64, uint64) {
}

type collisionDigester struct {
	firstLevelHash uint64
	blake3Hash     [4]uint64
	msg            []byte
}

var _ atree.Digester = &collisionDigester{}

func (d *collisionDigester) Digest(level uint) (atree.Digest, error) {
	if level >= d.Levels() {
		return atree.Digest(0), fmt.Errorf("invalid digest level %d", level)
	}

	switch level {
	case 0:
		return atree.Digest(d.firstLevelHash), nil
	default:
		if d.blake3Hash == emptyBlake3Hash {
			sum := blake3.Sum256(d.msg)
			d.blake3Hash[0] = binary.BigEndian.Uint64(sum[:])
			d.blake3Hash[1] = binary.BigEndian.Uint64(sum[8:])
			d.blake3Hash[2] = binary.BigEndian.Uint64(sum[16:])
			d.blake3Hash[3] = binary.BigEndian.Uint64(sum[24:])
		}
		return atree.Digest(d.blake3Hash[level-1]), nil
	}
}

func (d *collisionDigester) DigestPrefix(uint) ([]atree.Digest, error) {
	return nil, nil
}

func (d *collisionDigester) Levels() uint {
	return 4
}

func (d *collisionDigester) Reset() {
}

var (
	emptyBlake3Hash [4]uint64
)

func BenchmarkCollisionPerDigest(b *testing.B) {

	savedMaxCollisionLimitPerDigest := atree.MaxCollisionLimitPerDigest
	defer func() {
		atree.MaxCollisionLimitPerDigest = savedMaxCollisionLimitPerDigest
	}()

	const mapCount = 1_000_000

	collisionPerDigests := []uint32{0, 10, 255, 500, 1_000, 2_000, 5_000, 10_000}

	for _, collisionPerDigest := range collisionPerDigests {

		name := fmt.Sprintf("%d elements %d collision per digest", mapCount, collisionPerDigest)

		b.Run(name, func(b *testing.B) {

			atree.MaxCollisionLimitPerDigest = collisionPerDigest

			digesterBuilder := NewCollisionDigesterBuilder(collisionPerDigest)
			keyValues := make(map[atree.Value]atree.Value, mapCount)
			for i := range mapCount {
				k := testutils.Uint64Value(i)
				v := testutils.Uint64Value(i)
				keyValues[k] = v
			}

			typeInfo := testutils.NewSimpleTypeInfo(42)
			address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}
			storage := newTestPersistentStorage(b)

			m, err := atree.NewMap(storage, address, digesterBuilder, typeInfo)
			require.NoError(b, err)

			b.StartTimer()

			for range b.N {
				for k, v := range keyValues {
					_, _ = m.Set(testutils.CompareValue, testutils.GetHashInput, k, v)
				}
			}
		})
	}
}
