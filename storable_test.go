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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/onflow/atree"
)

func TestIsCBORTagNumberRangeAvailable(t *testing.T) {
	t.Parallel()

	minTagNum, maxTagNum := atree.ReservedCBORTagNumberRange()

	t.Run("error", func(t *testing.T) {
		_, err := atree.IsCBORTagNumberRangeAvailable(maxTagNum, minTagNum)
		var userError *atree.UserError
		require.ErrorAs(t, err, &userError)
	})

	t.Run("identical", func(t *testing.T) {
		available, err := atree.IsCBORTagNumberRangeAvailable(minTagNum, maxTagNum)
		require.NoError(t, err)
		require.False(t, available)
	})

	t.Run("subrange", func(t *testing.T) {
		available, err := atree.IsCBORTagNumberRangeAvailable(minTagNum, maxTagNum-1)
		require.NoError(t, err)
		require.False(t, available)

		available, err = atree.IsCBORTagNumberRangeAvailable(minTagNum+1, maxTagNum)
		require.NoError(t, err)
		require.False(t, available)
	})

	t.Run("partial overlap", func(t *testing.T) {
		available, err := atree.IsCBORTagNumberRangeAvailable(minTagNum-1, maxTagNum-1)
		require.NoError(t, err)
		require.False(t, available)

		available, err = atree.IsCBORTagNumberRangeAvailable(minTagNum+1, maxTagNum+1)
		require.NoError(t, err)
		require.False(t, available)
	})

	t.Run("non-overlap", func(t *testing.T) {
		available, err := atree.IsCBORTagNumberRangeAvailable(minTagNum-10, minTagNum-1)
		require.NoError(t, err)
		require.True(t, available)

		available, err = atree.IsCBORTagNumberRangeAvailable(minTagNum-1, minTagNum-1)
		require.NoError(t, err)
		require.True(t, available)

		available, err = atree.IsCBORTagNumberRangeAvailable(maxTagNum+1, maxTagNum+10)
		require.NoError(t, err)
		require.True(t, available)

		available, err = atree.IsCBORTagNumberRangeAvailable(maxTagNum+10, maxTagNum+10)
		require.NoError(t, err)
		require.True(t, available)
	})
}
