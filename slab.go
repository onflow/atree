/*
 * Atree - Scalable Arrays and Ordered Maps
 *
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

type Slab interface {
	Storable

	ID() StorageID
	Split(SlabStorage) (Slab, Slab, error)
	Merge(Slab) error
	// LendToRight rebalances slabs by moving elements from left to right
	LendToRight(Slab) error
	// BorrowFromRight rebalances slabs by moving elements from right to left
	BorrowFromRight(Slab) error
}

func IsRootOfAnObject(slabData []byte) (bool, error) {
	if len(slabData) < versionAndFlagSize {
		return false, NewDecodingErrorf("data is too short")
	}

	flag := slabData[1]

	return isRoot(flag), nil
}

func HasPointers(slabData []byte) (bool, error) {
	if len(slabData) < versionAndFlagSize {
		return false, NewDecodingErrorf("data is too short")
	}

	flag := slabData[1]

	return hasPointers(flag), nil
}

func HasSizeLimit(slabData []byte) (bool, error) {
	if len(slabData) < versionAndFlagSize {
		return false, NewDecodingErrorf("data is too short")
	}

	flag := slabData[1]

	return hasSizeLimit(flag), nil
}
