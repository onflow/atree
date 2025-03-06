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

package atree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSliceSplit(t *testing.T) {
	t.Parallel()

	t.Run("count is out of range", func(t *testing.T) {
		var s []int
		assert.Panics(t, func() { _, _ = split(s, len(s)+1) })

		s = []int{1}
		assert.Panics(t, func() { _, _ = split(s, len(s)+1) })
	})

	t.Run("empty slice", func(t *testing.T) {
		var s []int
		left, right := split(s, 0)
		require.Equal(t, 0, len(left))
		require.Equal(t, 0, len(right))
	})

	t.Run("non-empty slice", func(t *testing.T) {

		// split []int{1, 2, 3, 4, 5} at 0
		{
			s := []int{1, 2, 3, 4, 5}
			left, right := split(s, 0)

			require.Equal(t, 0, len(left))
			require.Equal(t, []int{0, 0, 0, 0, 0}, left[:cap(left)])

			require.Equal(t, 5, len(right))
			require.Equal(t, []int{1, 2, 3, 4, 5}, right)
		}

		// split []int{1, 2, 3, 4, 5} at 3
		{
			s := []int{1, 2, 3, 4, 5}
			p := &s[0]
			left, right := split(s, 3)

			require.Equal(t, 3, len(left))
			require.Equal(t, []int{1, 2, 3}, left)
			require.Equal(t, []int{1, 2, 3, 0, 0}, left[:cap(left)])
			require.Equal(t, p, &left[0])

			require.Equal(t, 2, len(right))
			require.Equal(t, []int{4, 5}, right)
		}

		// split []int{1, 2, 3, 4, 5} at len(s)
		{
			s := []int{1, 2, 3, 4, 5}
			p := &s[0]
			left, right := split(s, 5)

			require.Equal(t, 5, len(left))
			require.Equal(t, []int{1, 2, 3, 4, 5}, left)
			require.Equal(t, p, &left[0])

			require.Equal(t, 0, len(right))
		}
	})
}

func TestSliceMerge(t *testing.T) {
	t.Parallel()

	t.Run("empty slices", func(t *testing.T) {
		s1 := []int{}
		s2 := []int{}
		s := merge(s1, s2)
		require.Equal(t, 0, len(s))
	})

	t.Run("empty left slice", func(t *testing.T) {
		s1 := []int{}
		s2 := []int{1, 2, 3}
		s := merge(s1, s2)
		require.Equal(t, 3, len(s))
		require.Equal(t, []int{1, 2, 3}, s)
	})

	t.Run("empty right slice", func(t *testing.T) {
		s1 := []int{1, 2, 3}
		p := &s1[0]
		s2 := []int{}
		s := merge(s1, s2)
		require.Equal(t, 3, len(s))
		require.Equal(t, []int{1, 2, 3}, s)
		require.Equal(t, p, &s[0])
	})

	t.Run("non-empty slices", func(t *testing.T) {
		s1 := make([]int, 3, 5)
		s1[0] = 1
		s1[1] = 2
		s1[2] = 3
		p := &s1[0]
		s2 := []int{4, 5}
		s := merge(s1, s2)
		require.Equal(t, 5, len(s))
		require.Equal(t, []int{1, 2, 3, 4, 5}, s)
		require.Equal(t, p, &s[0])
	})
}

func TestSliceMoveFromLeftToRight(t *testing.T) {
	t.Parallel()

	t.Run("count is out of range", func(t *testing.T) {
		assert.Panics(t, func() { _, _ = lendToRight([]int{}, []int{0, 1, 2}, 1) })

		assert.Panics(t, func() { _, _ = lendToRight([]int{0}, []int{0, 1, 2}, 2) })
	})

	t.Run("move 0 element", func(t *testing.T) {
		// left is empty slice
		{
			s1 := []int{}
			s2 := []int{3, 4}
			p2 := &s2[0]
			s1b, s2b := lendToRight(s1, s2, 0)
			require.Equal(t, []int{}, s1b)
			require.Equal(t, []int{3, 4}, s2b)
			require.Equal(t, p2, &s2b[0])
		}

		// left is non-empty slice
		{
			s1 := []int{1, 2}
			p1 := &s1[0]
			s2 := []int{3, 4}
			p2 := &s2[0]
			s1b, s2b := lendToRight(s1, s2, 0)
			require.Equal(t, []int{1, 2}, s1b)
			require.Equal(t, p1, &s1b[0])
			require.Equal(t, []int{3, 4}, s2b)
			require.Equal(t, p2, &s2b[0])
		}
	})

	t.Run("move all elements", func(t *testing.T) {
		// right is empty slice
		{
			s1 := []int{1, 2, 3}
			oldlen := len(s1)
			s2 := []int{}
			s1b, s2b := lendToRight(s1, s2, len(s1))
			require.Equal(t, []int{}, s1b)
			require.Equal(t, []int{0, 0, 0}, s1b[:oldlen])
			require.Equal(t, []int{1, 2, 3}, s2b)
		}

		// right is non-empty slice
		{
			s1 := []int{1, 2, 3}
			oldlen := len(s1)
			s2 := []int{4, 5}
			s1b, s2b := lendToRight(s1, s2, len(s1))
			require.Equal(t, []int{}, s1b)
			require.Equal(t, []int{0, 0, 0}, s1b[:oldlen])
			require.Equal(t, []int{1, 2, 3, 4, 5}, s2b)
		}
	})

	t.Run("move some elements", func(t *testing.T) {
		s1 := []int{1, 2, 3}
		oldlen := len(s1)
		s2 := []int{4, 5}
		s1b, s2b := lendToRight(s1, s2, 2)
		require.Equal(t, []int{1}, s1b)
		require.Equal(t, []int{1, 0, 0}, s1b[:oldlen])
		require.Equal(t, []int{2, 3, 4, 5}, s2b)
	})
}

func TestSliceMoveFromRightToLeft(t *testing.T) {
	t.Parallel()

	t.Run("count is out of range", func(t *testing.T) {
		assert.Panics(t, func() { _, _ = borrowFromRight([]int{}, []int{}, 1) })

		assert.Panics(t, func() { _, _ = borrowFromRight([]int{}, []int{1}, 2) })
	})

	t.Run("move 0 element", func(t *testing.T) {
		// right is empty slice
		{
			s1 := []int{3, 4}
			s2 := []int{}
			s1b, s2b := borrowFromRight(s1, s2, 0)
			require.Equal(t, []int{3, 4}, s1b)
			require.Equal(t, []int{}, s2b)
		}

		// right is non-empty slice
		{
			s1 := []int{1, 2, 3}
			p1 := &s1[0]
			s2 := []int{4, 5}
			p2 := &s2[0]
			s1b, s2b := borrowFromRight(s1, s2, 0)
			require.Equal(t, []int{1, 2, 3}, s1b)
			require.Equal(t, p1, &s1b[0])
			require.Equal(t, []int{4, 5}, s2b)
			require.Equal(t, p2, &s2b[0])
		}
	})

	t.Run("move all element", func(t *testing.T) {
		// left is empty slice
		{
			s1 := []int{}
			s2 := []int{1, 2, 3}
			oldlen := len(s2)
			s1b, s2b := borrowFromRight(s1, s2, len(s2))
			require.Equal(t, []int{1, 2, 3}, s1b)
			require.Equal(t, []int{}, s2b)
			require.Equal(t, []int{0, 0, 0}, s2b[:oldlen])
		}

		// left is non-empty slice
		{
			s1 := []int{1, 2, 3}
			s2 := []int{4, 5}
			oldlen := len(s2)
			s1b, s2b := borrowFromRight(s1, s2, len(s2))
			require.Equal(t, []int{1, 2, 3, 4, 5}, s1b)
			require.Equal(t, []int{}, s2b)
			require.Equal(t, []int{0, 0}, s2b[:oldlen])
		}
	})

	t.Run("move some element", func(t *testing.T) {
		s1 := []int{1, 2}
		s2 := []int{3, 4, 5}
		oldlen := len(s2)
		s1b, s2b := borrowFromRight(s1, s2, 2)
		require.Equal(t, []int{1, 2, 3, 4}, s1b)
		require.Equal(t, []int{5}, s2b)
		require.Equal(t, []int{5, 0, 0}, s2b[:oldlen])
	})
}
