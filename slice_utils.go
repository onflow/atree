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

import "slices"

// split splits s into two slices with left slice of leftCount length and right slice of remaining elements.
// Returned left is resliced s, and returned right is new slice.
func split[S ~[]E, E any](s S, leftCount int) (left S, right S) {
	_ = s[leftCount:] // bounds check

	right = slices.Clone(s[leftCount:])
	left = slices.Delete(s, leftCount, len(s))
	return left, right
}

// merge returnes concatenated left and right slices.
// If left slice has sufficient capacity, returned slice is resliced to append elements from right.
// Right slice is cleared.
func merge[S ~[]E, E any](left, right S) S {
	left = append(left, right...)
	clear(right)
	return left
}

// lendToRight moves elements from tail of left slice to head of right slice.
func lendToRight[S ~[]E, E any](left, right S, count int) (S, S) {
	leftIndex := len(left) - count

	_ = left[leftIndex:] // bounds check

	// Prepend elements from the tail of left slice to the head of right slice.
	right = slices.Insert(
		right,
		0,
		left[leftIndex:]...,
	)

	// Remove moved elements from left
	left = slices.Delete(left, leftIndex, len(left))

	return left, right
}

// borrowFromRight moves elements from head of right slice to tail of left slice.
func borrowFromRight[S ~[]E, E any](left, right S, count int) (S, S) {
	_ = right[:count] // bounds check

	// Append moved elements to left
	left = append(left, right[:count]...)

	// Move remaining elements in the right slice to the front
	right = slices.Insert(
		right[:0],
		0,
		right[count:]...)

	// Clear moved elements to prevent memory leak
	clear(right[len(right):cap(right)])

	return left, right
}
