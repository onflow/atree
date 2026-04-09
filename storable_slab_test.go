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
	"strings"
	"testing"

	"github.com/onflow/atree"
	testutils "github.com/onflow/atree/test_utils"

	"github.com/stretchr/testify/require"
)

func TestNewStorableSlab(t *testing.T) {
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("large string", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		v := testutils.NewStringValue(strings.Repeat("a", 1_000))

		// Create StorableSlab for a large string value.
		s, err := atree.NewStorableSlab(storage, address, v, v.ByteSize())
		require.NoError(t, err)

		slabIDStorable, ok := s.(atree.SlabIDStorable)
		require.True(t, ok)

		// Retrieve StorableSlab from storage.
		slab, found, err := storage.Retrieve(atree.SlabID(slabIDStorable))
		require.NoError(t, err)
		require.True(t, found)

		// Retrieve large string value from StorableSlab
		v2, err := slab.(*atree.StorableSlab).StoredValue(storage)
		require.NoError(t, err)
		require.Equal(t, v, v2)
	})
}
