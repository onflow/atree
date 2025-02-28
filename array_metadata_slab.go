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
	"fmt"
	"math"
	"slices"
	"strings"
)

const (
	// 32 is faster than 24 and 40.
	linearScanThreshold = 32
)

// ArrayMetaDataSlab is internal node, implementing ArraySlab.
type ArrayMetaDataSlab struct {
	header          ArraySlabHeader
	childrenHeaders []ArraySlabHeader
	// Cumulative counts in the children.
	// For example, if the counts in childrenHeaders are [10, 15, 12],
	// childrenCountSum is [10, 25, 37]
	childrenCountSum []uint32

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *ArrayExtraData
}

var _ ArraySlab = &ArrayMetaDataSlab{}
var _ Slab = &ArrayMetaDataSlab{}
var _ Storable = &ArrayMetaDataSlab{}

// Array operations (get, set, insert, remove, and pop iterate)

// TODO: improve naming
func (a *ArrayMetaDataSlab) childSlabIndexInfo(
	index uint64,
) (
	childHeaderIndex int,
	adjustedIndex uint64,
	childID SlabID,
	err error,
) {
	if index >= uint64(a.header.count) {
		return 0, 0, SlabID{}, NewIndexOutOfBoundsError(index, 0, uint64(a.header.count))
	}

	// Either perform a linear scan (for small number of children),
	// or a binary search

	count := len(a.childrenCountSum)

	if count < linearScanThreshold {
		for i, countSum := range a.childrenCountSum {
			if index < uint64(countSum) {
				childHeaderIndex = i
				break
			}
		}
	} else {
		low, high := 0, count
		for low < high {
			// The following line is borrowed from Go runtime .
			mid := int(uint(low+high) >> 1) // avoid overflow when computing mid
			midCountSum := uint64(a.childrenCountSum[mid])

			if midCountSum < index {
				low = mid + 1
			} else if midCountSum > index {
				high = mid
			} else {
				low = mid + 1
				break

			}
		}
		childHeaderIndex = low
	}

	childHeader := a.childrenHeaders[childHeaderIndex]
	adjustedIndex = index + uint64(childHeader.count) - uint64(a.childrenCountSum[childHeaderIndex])
	childID = childHeader.slabID

	return childHeaderIndex, adjustedIndex, childID, nil
}

func (a *ArrayMetaDataSlab) Get(storage SlabStorage, index uint64) (Storable, error) {

	_, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}

	// Don't need to wrap error as external error because err is already categorized by ArraySlab.Get().
	return child.Get(storage, adjustedIndex)
}

func (a *ArrayMetaDataSlab) Set(storage SlabStorage, address Address, index uint64, value Value) (Storable, error) {

	childHeaderIndex, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}

	existingElem, err := child.Set(storage, address, adjustedIndex, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Set().
		return nil, err
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Update may increase or decrease the size,
	// check if full and for underflow

	if child.IsFull() {
		err = a.SplitChildSlab(storage, child, childHeaderIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.SplitChildSlab().
			return nil, err
		}
		return existingElem, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err = a.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, err
		}
		return existingElem, nil
	}

	err = storeSlab(storage, a)
	if err != nil {
		return nil, err
	}

	return existingElem, nil
}

// Insert inserts v into the correct child slab.
// index must be >=0 and <= a.header.count.
// If index == a.header.count, Insert appends v to the end of underlying slab.
func (a *ArrayMetaDataSlab) Insert(storage SlabStorage, address Address, index uint64, value Value) error {
	if index > uint64(a.header.count) {
		return NewIndexOutOfBoundsError(index, 0, uint64(a.header.count))
	}

	var childID SlabID
	var childHeaderIndex int
	var adjustedIndex uint64
	if index == uint64(a.header.count) {
		childHeaderIndex = len(a.childrenHeaders) - 1
		h := a.childrenHeaders[childHeaderIndex]
		childID = h.slabID
		adjustedIndex = uint64(h.count)
	} else {
		var err error
		childHeaderIndex, adjustedIndex, childID, err = a.childSlabIndexInfo(index)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
			return err
		}
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return err
	}

	err = child.Insert(storage, address, adjustedIndex, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Insert().
		return err
	}

	a.header.count++

	// Increment childrenCountSum from childHeaderIndex
	for i := childHeaderIndex; i < len(a.childrenCountSum); i++ {
		a.childrenCountSum[i]++
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Insertion increases the size,
	// check if full

	if child.IsFull() {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.SplitChildSlab().
		return a.SplitChildSlab(storage, child, childHeaderIndex)
	}

	// Insertion always increases the size,
	// so there is no need to check underflow

	return storeSlab(storage, a)
}

func (a *ArrayMetaDataSlab) Remove(storage SlabStorage, index uint64) (Storable, error) {

	if index >= uint64(a.header.count) {
		return nil, NewIndexOutOfBoundsError(index, 0, uint64(a.header.count))
	}

	childHeaderIndex, adjustedIndex, childID, err := a.childSlabIndexInfo(index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArrayMetadataSlab.childSlabIndexInfo().
		return nil, err
	}

	child, err := getArraySlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getArraySlab().
		return nil, err
	}

	v, err := child.Remove(storage, adjustedIndex)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Remove().
		return nil, err
	}

	a.header.count--

	// Decrement childrenCountSum from childHeaderIndex
	for i := childHeaderIndex; i < len(a.childrenCountSum); i++ {
		a.childrenCountSum[i]--
	}

	a.childrenHeaders[childHeaderIndex] = child.Header()

	// Removal decreases the size,
	// check for underflow

	if underflowSize, isUnderflow := child.IsUnderflow(); isUnderflow {
		err = a.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArrayMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, err
		}
	}

	// Removal always decreases the size,
	// so there is no need to check isFull

	err = storeSlab(storage, a)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (a *ArrayMetaDataSlab) PopIterate(storage SlabStorage, fn ArrayPopIterationFunc) error {

	// Iterate child slabs backwards
	for i := len(a.childrenHeaders) - 1; i >= 0; i-- {

		childID := a.childrenHeaders[i].slabID

		child, err := getArraySlab(storage, childID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return err
		}

		err = child.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by ArraySlab.PopIterate().
			return err
		}

		// Remove child slab
		err = storage.Remove(childID)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
		}
	}

	// All child slabs are removed.

	// Reset meta data slab
	a.childrenCountSum = nil
	a.childrenHeaders = nil
	a.header.count = 0
	a.header.size = arrayMetaDataSlabPrefixSize

	return nil
}

// Slab operations (split, merge, and lend/borrow)

func (a *ArrayMetaDataSlab) SplitChildSlab(storage SlabStorage, child ArraySlab, childHeaderIndex int) error {
	leftSlab, rightSlab, err := child.Split(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Split().
		return err
	}

	left := leftSlab.(ArraySlab)
	right := rightSlab.(ArraySlab)

	// Add new child slab (right) to childrenHeaders
	a.childrenHeaders = append(a.childrenHeaders, ArraySlabHeader{})
	if childHeaderIndex < len(a.childrenHeaders)-2 {
		copy(a.childrenHeaders[childHeaderIndex+2:], a.childrenHeaders[childHeaderIndex+1:])
	}
	a.childrenHeaders[childHeaderIndex] = left.Header()
	a.childrenHeaders[childHeaderIndex+1] = right.Header()

	// Adjust childrenCountSum
	a.childrenCountSum = append(a.childrenCountSum, uint32(0))
	copy(a.childrenCountSum[childHeaderIndex+1:], a.childrenCountSum[childHeaderIndex:])
	a.childrenCountSum[childHeaderIndex] -= right.Header().count

	// Increase header size
	a.header.size += arraySlabHeaderSize

	// Store modified slabs
	err = storeSlab(storage, left)
	if err != nil {
		return err
	}

	err = storeSlab(storage, right)
	if err != nil {
		return err
	}

	return storeSlab(storage, a)
}

// MergeOrRebalanceChildSlab merges or rebalances child slab.
// If merged, then parent slab's data is adjusted.
//
// +-----------------------+-----------------------+----------------------+-----------------------+
// |                       | no left sibling (sib) | left sib can't lend  | left sib can lend     |
// +=======================+=======================+======================+=======================+
// | no right sib          | panic                 | merge with left      | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can't lend  | merge with right      | merge with smaller   | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can lend    | rebalance with right  | rebalance with right | rebalance with bigger |
// +-----------------------+-----------------------+----------------------+-----------------------+
func (a *ArrayMetaDataSlab) MergeOrRebalanceChildSlab(
	storage SlabStorage,
	child ArraySlab,
	childHeaderIndex int,
	underflowSize uint32,
) error {

	// Retrieve left and right siblings of the same parent.
	var leftSib, rightSib ArraySlab
	if childHeaderIndex > 0 {
		leftSibID := a.childrenHeaders[childHeaderIndex-1].slabID

		var err error
		leftSib, err = getArraySlab(storage, leftSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return err
		}
	}
	if childHeaderIndex < len(a.childrenHeaders)-1 {
		rightSibID := a.childrenHeaders[childHeaderIndex+1].slabID

		var err error
		rightSib, err = getArraySlab(storage, rightSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return err
		}
	}

	leftCanLend := leftSib != nil && leftSib.CanLendToRight(underflowSize)
	rightCanLend := rightSib != nil && rightSib.CanLendToLeft(underflowSize)

	// Child can rebalance elements with at least one sibling.
	if leftCanLend || rightCanLend {

		// Rebalance with right sib
		if !leftCanLend {
			baseCountSum := a.childrenCountSum[childHeaderIndex] - child.Header().count

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.BorrowFromRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex] = child.Header()
			a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex] = baseCountSum + child.Header().count

			// Store modified slabs
			err = storeSlab(storage, child)
			if err != nil {
				return err
			}
			err = storeSlab(storage, rightSib)
			if err != nil {
				return err
			}
			return storeSlab(storage, a)
		}

		// Rebalance with left sib
		if !rightCanLend {
			baseCountSum := a.childrenCountSum[childHeaderIndex-1] - leftSib.Header().count

			err := leftSib.LendToRight(child)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.LendToRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex-1] = baseCountSum + leftSib.Header().count

			// Store modified slabs
			err = storeSlab(storage, leftSib)
			if err != nil {
				return err
			}
			err = storeSlab(storage, child)
			if err != nil {
				return err
			}
			return storeSlab(storage, a)
		}

		// Rebalance with bigger sib
		if leftSib.ByteSize() > rightSib.ByteSize() {
			baseCountSum := a.childrenCountSum[childHeaderIndex-1] - leftSib.Header().count

			err := leftSib.LendToRight(child)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.LendToRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			a.childrenHeaders[childHeaderIndex] = child.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex-1] = baseCountSum + leftSib.Header().count

			// Store modified slabs
			err = storeSlab(storage, leftSib)
			if err != nil {
				return err
			}

			err = storeSlab(storage, child)
			if err != nil {
				return err
			}

			return storeSlab(storage, a)

		} else {
			// leftSib.ByteSize() <= rightSib.ByteSize

			baseCountSum := a.childrenCountSum[childHeaderIndex] - child.Header().count

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by ArraySlab.BorrowFromRight().
				return err
			}

			a.childrenHeaders[childHeaderIndex] = child.Header()
			a.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// Adjust childrenCountSum
			a.childrenCountSum[childHeaderIndex] = baseCountSum + child.Header().count

			// Store modified slabs
			err = storeSlab(storage, child)
			if err != nil {
				return err
			}

			err = storeSlab(storage, rightSib)
			if err != nil {
				return err
			}

			return storeSlab(storage, a)
		}
	}

	// Child can't rebalance with any sibling.  It must merge with one sibling.

	if leftSib == nil {
		// Merge (left) child slab with rightSib

		leftSlabIndex := childHeaderIndex
		rightSlabIndex := childHeaderIndex + 1

		return a.mergeChildren(
			storage,
			child,
			rightSib,
			leftSlabIndex,
			rightSlabIndex,
		)
	}

	if rightSib == nil {
		// Merge leftSib with (right) child slab

		leftSlabIndex := childHeaderIndex - 1
		rightSlabIndex := childHeaderIndex

		return a.mergeChildren(
			storage,
			leftSib,
			child,
			leftSlabIndex,
			rightSlabIndex,
		)
	}

	// Merge with smaller sib

	if leftSib.ByteSize() < rightSib.ByteSize() {
		// Merge leftSib with (right) child slab

		leftSlabIndex := childHeaderIndex - 1
		rightSlabIndex := childHeaderIndex

		return a.mergeChildren(
			storage,
			leftSib,
			child,
			leftSlabIndex,
			rightSlabIndex,
		)
	}

	// leftSib.ByteSize > rightSib.ByteSize

	// Merge (left) child slab with rightSib

	leftSlabIndex := childHeaderIndex
	rightSlabIndex := childHeaderIndex + 1

	return a.mergeChildren(
		storage,
		child,
		rightSib,
		leftSlabIndex,
		rightSlabIndex,
	)
}

func (a *ArrayMetaDataSlab) mergeChildren(
	storage SlabStorage,
	leftChildSlab ArraySlab,
	rightChildSlab ArraySlab,
	leftChildSlabIndex int,
	rightChildSlabIndex int,
) error {

	err := leftChildSlab.Merge(rightChildSlab)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by ArraySlab.Merge().
		return err
	}

	mergedSlab := leftChildSlab
	obseleteSlab := rightChildSlab

	a.updateChildrenHeadersAfterMerge(mergedSlab.Header(), leftChildSlabIndex, rightChildSlabIndex)

	a.header.size -= arraySlabHeaderSize

	// Store merged child slab in storage
	err = storeSlab(storage, mergedSlab)
	if err != nil {
		return err
	}

	// Store modified parent slab in storage
	err = storeSlab(storage, a)
	if err != nil {
		return err
	}

	// Remove obselete slab from storage
	err = storage.Remove(obseleteSlab.SlabID())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", obseleteSlab.SlabID()))
	}

	return nil
}

func (a *ArrayMetaDataSlab) updateChildrenHeadersAfterMerge(
	mergedSlabHeader ArraySlabHeader,
	leftSlabIndex int,
	rightSlabIndex int,
) {
	// Update left slab header
	a.childrenHeaders[leftSlabIndex] = mergedSlabHeader

	// Remove right slab header
	a.childrenHeaders = slices.Delete[[]ArraySlabHeader](
		a.childrenHeaders,
		rightSlabIndex,
		rightSlabIndex+1,
	)

	// Update left slab count sum
	a.childrenCountSum[leftSlabIndex] = a.childrenCountSum[rightSlabIndex]

	// Remove right slab count sum
	a.childrenCountSum = slices.Delete[[]uint32](
		a.childrenCountSum,
		rightSlabIndex,
		rightSlabIndex+1,
	)
}

func (a *ArrayMetaDataSlab) Merge(slab Slab) error {

	// The assumption len > 0 holds in all cases except for the root slab

	baseCountSum := a.childrenCountSum[len(a.childrenCountSum)-1]
	leftSlabChildrenCount := len(a.childrenHeaders)

	rightSlab := slab.(*ArrayMetaDataSlab)
	a.childrenHeaders = append(a.childrenHeaders, rightSlab.childrenHeaders...)
	a.header.size += rightSlab.header.size - arrayMetaDataSlabPrefixSize
	a.header.count += rightSlab.header.count

	// Adjust childrenCountSum
	for i := leftSlabChildrenCount; i < len(a.childrenHeaders); i++ {
		baseCountSum += a.childrenHeaders[i].count
		a.childrenCountSum = append(a.childrenCountSum, baseCountSum)
	}

	return nil
}

func (a *ArrayMetaDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {

	if len(a.childrenHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		return nil, nil, NewSlabSplitErrorf("ArrayMetaDataSlab (%s) has less than 2 child headers", a.header.slabID)
	}

	childrenCount := len(a.childrenHeaders)
	leftChildrenCount := int(math.Ceil(float64(childrenCount) / 2))
	rightChildrenCount := childrenCount - leftChildrenCount

	leftSize := leftChildrenCount * arraySlabHeaderSize

	leftCount := uint32(0)
	for i := range leftChildrenCount {
		leftCount += a.childrenHeaders[i].count
	}

	// Construct right slab
	sID, err := storage.GenerateSlabID(a.header.slabID.address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(
			err,
			fmt.Sprintf("failed to generate slab ID for address 0x%x", a.header.slabID.address))
	}

	rightSlab := &ArrayMetaDataSlab{
		header: ArraySlabHeader{
			slabID: sID,
			size:   a.header.size - uint32(leftSize),
			count:  a.header.count - leftCount,
		},
	}

	rightSlab.childrenHeaders = make([]ArraySlabHeader, rightChildrenCount)
	copy(rightSlab.childrenHeaders, a.childrenHeaders[leftChildrenCount:])

	rightSlab.childrenCountSum = make([]uint32, len(rightSlab.childrenHeaders))
	countSum := uint32(0)
	for i := range rightSlab.childrenCountSum {
		countSum += rightSlab.childrenHeaders[i].count
		rightSlab.childrenCountSum[i] = countSum
	}

	// Modify left (original)slab
	a.childrenHeaders = a.childrenHeaders[:leftChildrenCount]
	a.childrenCountSum = a.childrenCountSum[:leftChildrenCount]
	a.header.count = leftCount
	a.header.size = arrayMetaDataSlabPrefixSize + uint32(leftSize)

	return a, rightSlab, nil
}

func (a *ArrayMetaDataSlab) LendToRight(slab Slab) error {
	rightSlab := slab.(*ArrayMetaDataSlab)

	childrenHeadersLen := len(a.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftChildrenHeadersLen := childrenHeadersLen / 2
	rightChildrenHeadersLen := childrenHeadersLen - leftChildrenHeadersLen

	// Update right slab childrenHeaders by prepending borrowed children headers
	rightChildrenHeaders := make([]ArraySlabHeader, rightChildrenHeadersLen)
	n := copy(rightChildrenHeaders, a.childrenHeaders[leftChildrenHeadersLen:])
	copy(rightChildrenHeaders[n:], rightSlab.childrenHeaders)
	rightSlab.childrenHeaders = rightChildrenHeaders

	// Rebuild right slab childrenCountSum
	rightSlab.childrenCountSum = make([]uint32, len(rightSlab.childrenHeaders))
	countSum := uint32(0)
	for i := range rightSlab.childrenCountSum {
		countSum += rightSlab.childrenHeaders[i].count
		rightSlab.childrenCountSum[i] = countSum
	}

	// Update right slab header
	rightSlab.header.count = 0
	for i := range rightSlab.childrenHeaders {
		rightSlab.header.count += rightSlab.childrenHeaders[i].count
	}
	rightSlab.header.size = arrayMetaDataSlabPrefixSize + uint32(len(rightSlab.childrenHeaders))*arraySlabHeaderSize

	// Update left slab (original)
	a.childrenHeaders = a.childrenHeaders[:leftChildrenHeadersLen]
	a.childrenCountSum = a.childrenCountSum[:leftChildrenHeadersLen]

	a.header.count = 0
	for i := range a.childrenHeaders {
		a.header.count += a.childrenHeaders[i].count
	}
	a.header.size = arrayMetaDataSlabPrefixSize + uint32(leftChildrenHeadersLen)*arraySlabHeaderSize

	return nil
}

func (a *ArrayMetaDataSlab) BorrowFromRight(slab Slab) error {
	originalLeftSlabCountSum := a.header.count
	originalLeftSlabHeaderLen := len(a.childrenHeaders)

	rightSlab := slab.(*ArrayMetaDataSlab)

	childrenHeadersLen := len(a.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftSlabHeaderLen := childrenHeadersLen / 2
	rightSlabHeaderLen := childrenHeadersLen - leftSlabHeaderLen

	// Update left slab (original)
	a.childrenHeaders = append(a.childrenHeaders, rightSlab.childrenHeaders[:leftSlabHeaderLen-len(a.childrenHeaders)]...)

	countSum := originalLeftSlabCountSum
	for i := originalLeftSlabHeaderLen; i < len(a.childrenHeaders); i++ {
		countSum += a.childrenHeaders[i].count
		a.childrenCountSum = append(a.childrenCountSum, countSum)
	}
	a.header.count = countSum
	a.header.size = arrayMetaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*arraySlabHeaderSize

	// Update right slab
	rightSlab.childrenHeaders = rightSlab.childrenHeaders[len(rightSlab.childrenHeaders)-rightSlabHeaderLen:]
	rightSlab.childrenCountSum = rightSlab.childrenCountSum[:len(rightSlab.childrenHeaders)]

	countSum = uint32(0)
	for i := range rightSlab.childrenCountSum {
		countSum += rightSlab.childrenHeaders[i].count
		rightSlab.childrenCountSum[i] = countSum
	}
	rightSlab.header.count = countSum
	rightSlab.header.size = arrayMetaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*arraySlabHeaderSize

	return nil
}

func (a ArrayMetaDataSlab) IsFull() bool {
	return a.header.size > uint32(maxThreshold)
}

func (a ArrayMetaDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > a.header.size {
		return uint32(minThreshold) - a.header.size, true
	}
	return 0, false
}

func (a *ArrayMetaDataSlab) CanLendToLeft(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / arraySlabHeaderSize))
	return a.header.size-arraySlabHeaderSize*n > uint32(minThreshold)
}

func (a *ArrayMetaDataSlab) CanLendToRight(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / arraySlabHeaderSize))
	return a.header.size-arraySlabHeaderSize*n > uint32(minThreshold)
}

// Inline operations

func (a *ArrayMetaDataSlab) Inlinable(_ uint64) bool {
	return false
}

func (a *ArrayMetaDataSlab) Inline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to inline ArrayMetaDataSlab %s: ArrayMetaDataSlab can't be inlined", a.header.slabID))
}

func (a *ArrayMetaDataSlab) Uninline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to uninline ArrayMetaDataSlab %s: ArrayMetaDataSlab is already unlined", a.header.slabID))
}

func (a *ArrayMetaDataSlab) Inlined() bool {
	return false
}

// Other operations

func (a *ArrayMetaDataSlab) IsData() bool {
	return false
}

func (a *ArrayMetaDataSlab) SetSlabID(id SlabID) {
	a.header.slabID = id
}

func (a *ArrayMetaDataSlab) Header() ArraySlabHeader {
	return a.header
}

func (a *ArrayMetaDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *ArrayMetaDataSlab) SlabID() SlabID {
	return a.header.slabID
}

func (a *ArrayMetaDataSlab) ExtraData() *ArrayExtraData {
	return a.extraData
}

func (a *ArrayMetaDataSlab) RemoveExtraData() *ArrayExtraData {
	extraData := a.extraData
	a.extraData = nil
	return extraData
}

func (a *ArrayMetaDataSlab) SetExtraData(extraData *ArrayExtraData) {
	a.extraData = extraData
}

func (a *ArrayMetaDataSlab) String() string {
	elemsStr := make([]string, len(a.childrenHeaders))
	for i, h := range a.childrenHeaders {
		elemsStr[i] = fmt.Sprintf("{id:%s size:%d count:%d}", h.slabID, h.size, h.count)
	}

	return fmt.Sprintf("ArrayMetaDataSlab id:%s size:%d count:%d children: [%s]",
		a.header.slabID,
		a.header.size,
		a.header.count,
		strings.Join(elemsStr, " "),
	)
}

func (a *ArrayMetaDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	if a.extraData == nil {
		return nil, NewNotValueError(a.SlabID())
	}
	return &Array{
		Storage: storage,
		root:    a,
	}, nil
}

func (a *ArrayMetaDataSlab) ChildStorables() []Storable {

	childIDs := make([]Storable, len(a.childrenHeaders))

	for i, h := range a.childrenHeaders {
		childIDs[i] = SlabIDStorable(h.slabID)
	}

	return childIDs
}
