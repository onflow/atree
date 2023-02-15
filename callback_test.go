/*
 * Atree - Scalable Arrays and Ordered Maps
 *
 * Copyright 2023 Dapper Labs, Inc.
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
	"fmt"
	"strings"
	"testing"

	"sync/atomic"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ Callback = &testCallback{}

type testCallback struct {
	beforeEncode func(uint32) error
}

func (t testCallback) BeforeEncode(size uint32) error {
	return t.beforeEncode(size)
}

func TestCallbackOnEncode(t *testing.T) {

	t.Parallel()

	t.Run("strings", func(t *testing.T) {
		t.Parallel()

		typeInfo := testTypeInfo{42}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		values := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			v := NewStringValue(strings.Repeat("a", 22))
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		var count, totalBytes uint32 = 0, 0

		callback := &testCallback{
			beforeEncode: func(size uint32) error {
				atomic.AddUint32(&count, 1)
				atomic.AddUint32(&totalBytes, size)
				return nil
			},
		}

		_, err = storage.Encode(callback)
		require.NoError(t, err)
		assert.Equal(t, arraySize, int(count))
		assert.Equal(t, 460, int(totalBytes))
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		typeInfo := testTypeInfo{42}
		storage := newTestBasicStorage(t)
		address := Address{1, 2, 3, 4, 5, 6, 7, 8}

		array, err := NewArray(storage, address, typeInfo)
		require.NoError(t, err)

		const arraySize = 20
		values := make([]Value, arraySize)
		for i := 0; i < arraySize; i++ {
			v := NewStringValue(strings.Repeat("a", 22))
			values[i] = v
			err := array.Append(v)
			require.NoError(t, err)
		}

		var count, totalBytes uint32 = 0, 0
		const terminateAt uint32 = 12

		callback := &testCallback{
			beforeEncode: func(size uint32) error {
				atomic.AddUint32(&count, 1)
				atomic.AddUint32(&totalBytes, size)
				if atomic.LoadUint32(&count) >= terminateAt {
					return fmt.Errorf("array too large")
				}
				return nil
			},
		}

		_, err = storage.Encode(callback)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "array too large")
		assert.Equal(t, terminateAt, count)
		assert.Equal(t, 276, int(totalBytes))
	})
}
