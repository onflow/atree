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
	"math"
	"testing"

	"github.com/onflow/atree"
	testutils "github.com/onflow/atree/test_utils"

	"github.com/stretchr/testify/require"
)

func TestConversionBetweenByteArrayAndByteSlice(t *testing.T) {
	t.Parallel()

	typeInfo := testutils.NewSimpleTypeInfo(42)
	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	estimatedElementSize := testutils.Uint8Value(math.MaxUint8).ByteSize()

	t.Run("empty byte array", func(t *testing.T) {
		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())

		// Convert atree array to []byte.
		data, err := atree.ByteArrayToByteSlice[testutils.Uint8Value](array)
		require.NoError(t, err)
		require.Equal(t, 0, len(data))

		// Convert []byte to atree array.
		convertedArray, err := atree.ByteSliceToByteArray[testutils.Uint8Value](storage, address, typeInfo, data, estimatedElementSize)
		require.NoError(t, err)
		require.Equal(t, uint64(0), convertedArray.Count())

		testArray(t, storage, typeInfo, address, convertedArray, nil, false, 2)
	})

	t.Run("single-slab byte array", func(t *testing.T) {
		const arrayCount = 10

		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())

		expectedByteSlice := make([]byte, arrayCount)
		expectedValues := make(testutils.ExpectedArrayValue, arrayCount)
		for i := range arrayCount {
			b := byte(i)
			expectedByteSlice[i] = b

			v := testutils.Uint8Value(b)
			expectedValues[i] = testutils.Uint8Value(b)

			err = array.Append(v)
			require.NoError(t, err)
		}
		require.Equal(t, uint64(arrayCount), array.Count())
		require.True(t, atree.GetArrayRootSlab(array).IsData())

		// Convert atree array to []byte.
		data, err := atree.ByteArrayToByteSlice[testutils.Uint8Value](array)
		require.NoError(t, err)
		require.Equal(t, arrayCount, len(data))
		require.Equal(t, expectedByteSlice, data)

		// Convert []byte to atree array.
		convertedArray, err := atree.ByteSliceToByteArray[testutils.Uint8Value](storage, address, typeInfo, data, estimatedElementSize)
		require.NoError(t, err)
		require.Equal(t, uint64(arrayCount), convertedArray.Count())

		for i := range arrayCount {
			originalElement, err := array.Get(uint64(i))
			require.NoError(t, err)

			convertedElement, err := convertedArray.Get(uint64(i))
			require.NoError(t, err)

			require.Equal(t, originalElement, convertedElement)
		}

		testArray(t, storage, typeInfo, address, convertedArray, expectedValues, false, 2)
	})

	t.Run("multi-slab byte array", func(t *testing.T) {
		const arrayCount = 4096

		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())

		expectedByteSlice := make([]byte, arrayCount)
		expectedValues := make(testutils.ExpectedArrayValue, arrayCount)
		for i := range arrayCount {
			b := uint8(i % 256)
			expectedByteSlice[i] = b

			v := testutils.Uint8Value(b)
			expectedValues[i] = testutils.Uint8Value(b)

			err = array.Append(v)
			require.NoError(t, err)
		}
		require.Equal(t, uint64(arrayCount), array.Count())
		require.False(t, atree.GetArrayRootSlab(array).IsData())

		// Convert atree array to []byte.
		data, err := atree.ByteArrayToByteSlice[testutils.Uint8Value](array)
		require.NoError(t, err)
		require.Equal(t, arrayCount, len(data))
		require.Equal(t, expectedByteSlice, data)

		// Convert []byte to atree array.
		convertedArray, err := atree.ByteSliceToByteArray[testutils.Uint8Value](storage, address, typeInfo, data, estimatedElementSize)
		require.NoError(t, err)
		require.Equal(t, uint64(arrayCount), convertedArray.Count())

		for i := range arrayCount {
			originalElement, err := array.Get(uint64(i))
			require.NoError(t, err)

			convertedElement, err := convertedArray.Get(uint64(i))
			require.NoError(t, err)

			require.Equal(t, originalElement, convertedElement)
		}

		testArray(t, storage, typeInfo, address, convertedArray, expectedValues, false, 2)
	})

	t.Run("array with non byte elements", func(t *testing.T) {
		const arrayCount = 10

		storage := newTestPersistentStorage(t)

		array, err := atree.NewArray(storage, address, typeInfo)
		require.NoError(t, err)
		require.Equal(t, uint64(0), array.Count())

		for i := range arrayCount {
			b := byte(i)
			v := testutils.Uint64Value(b)

			err = array.Append(v)
			require.NoError(t, err)
		}
		require.Equal(t, uint64(arrayCount), array.Count())
		require.True(t, atree.GetArrayRootSlab(array).IsData())

		_, err = atree.ByteArrayToByteSlice[testutils.Uint8Value](array)
		require.Error(t, err)
		var unexpectedElementTypeError *atree.UnexpectedElementTypeError
		require.ErrorAs(t, err, &unexpectedElementTypeError)
		require.ErrorContains(t, err, "invalid element type: expected testutils.Uint8Value, got testutils.Uint64Value")
	})
}
