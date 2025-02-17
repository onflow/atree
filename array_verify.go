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
	"bytes"
	"fmt"
	"reflect"
)

type TypeInfoComparator func(TypeInfo, TypeInfo) bool

func VerifyArray(
	a *Array,
	address Address,
	typeInfo TypeInfo,
	tic TypeInfoComparator,
	hip HashInputProvider,
	inlineEnabled bool,
) error {
	return verifyArray(
		a,
		address,
		typeInfo,
		tic,
		hip,
		inlineEnabled,
		map[SlabID]struct{}{})
}

func verifyArray(
	a *Array,
	address Address,
	typeInfo TypeInfo,
	tic TypeInfoComparator,
	hip HashInputProvider,
	inlineEnabled bool,
	slabIDs map[SlabID]struct{},
) error {
	// Verify array address (independent of array inlined status)
	if address != a.Address() {
		return NewFatalError(fmt.Errorf("array address %v, got %v", address, a.Address()))
	}

	// Verify array value ID (independent of array inlined status)
	err := verifyArrayValueID(a)
	if err != nil {
		return err
	}

	// Verify array slab ID (dependent of array inlined status)
	err = verifyArraySlabID(a)
	if err != nil {
		return err
	}

	// Verify array extra data
	extraData := a.root.ExtraData()
	if extraData == nil {
		return NewFatalError(fmt.Errorf("root slab %d doesn't have extra data", a.root.SlabID()))
	}

	// Verify that extra data has correct type information
	if typeInfo != nil && !tic(extraData.TypeInfo, typeInfo) {
		return NewFatalError(fmt.Errorf(
			"root slab %d type information %v is wrong, want %v",
			a.root.SlabID(),
			extraData.TypeInfo,
			typeInfo,
		))
	}

	v := &arrayVerifier{
		storage:       a.Storage,
		address:       address,
		tic:           tic,
		hip:           hip,
		inlineEnabled: inlineEnabled,
	}

	// Verify array slabs
	computedCount, dataSlabIDs, nextDataSlabIDs, err := v.verifySlab(a.root, 0, nil, []SlabID{}, []SlabID{}, slabIDs)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by verifySlab().
		return err
	}

	// Verify array count
	if computedCount != uint32(a.Count()) {
		return NewFatalError(fmt.Errorf("root slab %d count %d is wrong, want %d", a.root.SlabID(), a.Count(), computedCount))
	}

	// Verify next data slab ids
	if !reflect.DeepEqual(dataSlabIDs[1:], nextDataSlabIDs) {
		return NewFatalError(fmt.Errorf("chained next data slab ids %v are wrong, want %v",
			nextDataSlabIDs, dataSlabIDs[1:]))
	}

	return nil
}

type arrayVerifier struct {
	storage       SlabStorage
	address       Address
	tic           TypeInfoComparator
	hip           HashInputProvider
	inlineEnabled bool
}

// verifySlab verifies ArraySlab in memory which can be inlined or not inlined.
func (v *arrayVerifier) verifySlab(
	slab ArraySlab,
	level int,
	headerFromParentSlab *ArraySlabHeader,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	slabIDs map[SlabID]struct{},
) (
	elementCount uint32,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	err error,
) {
	id := slab.Header().slabID

	// Verify SlabID is unique
	if _, exist := slabIDs[id]; exist {
		return 0, nil, nil, NewFatalError(fmt.Errorf("found duplicate slab ID %s", id))
	}

	slabIDs[id] = struct{}{}

	// Verify slab address (independent of array inlined status)
	if v.address != id.address {
		return 0, nil, nil, NewFatalError(fmt.Errorf("array slab address %v, got %v", v.address, id.address))
	}

	// Verify that inlined slab is not in storage
	if slab.Inlined() {
		_, exist, err := v.storage.Retrieve(id)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storage interface.
			return 0, nil, nil, wrapErrorAsExternalErrorIfNeeded(err)
		}
		if exist {
			return 0, nil, nil, NewFatalError(fmt.Errorf("inlined slab %s is in storage", id))
		}
	}

	if level > 0 {
		// Verify that non-root slab doesn't have extra data
		if slab.ExtraData() != nil {
			return 0, nil, nil, NewFatalError(fmt.Errorf("non-root slab %s has extra data", id))
		}

		// Verify that non-root slab doesn't underflow
		if underflowSize, underflow := slab.IsUnderflow(); underflow {
			return 0, nil, nil, NewFatalError(fmt.Errorf("slab %s underflows by %d bytes", id, underflowSize))
		}

	}

	// Verify that slab doesn't overflow
	if slab.IsFull() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("slab %s overflows", id))
	}

	// Verify that header is in sync with header from parent slab
	if headerFromParentSlab != nil {
		if !reflect.DeepEqual(*headerFromParentSlab, slab.Header()) {
			return 0, nil, nil, NewFatalError(fmt.Errorf("slab %s header %+v is different from header %+v from parent slab",
				id, slab.Header(), headerFromParentSlab))
		}
	}

	switch slab := slab.(type) {
	case *ArrayDataSlab:
		return v.verifyDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, slabIDs)

	case *ArrayMetaDataSlab:
		return v.verifyMetaDataSlab(slab, level, dataSlabIDs, nextDataSlabIDs, slabIDs)

	default:
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArraySlab is either *ArrayDataSlab or *ArrayMetaDataSlab, got %T", slab))
	}
}

func (v *arrayVerifier) verifyDataSlab(
	dataSlab *ArrayDataSlab,
	level int,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	slabIDs map[SlabID]struct{},
) (
	elementCount uint32,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	err error,
) {
	id := dataSlab.header.slabID

	if !dataSlab.IsData() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArrayDataSlab %s is not data", id))
	}

	// Verify that element count is the same as header.count
	if uint32(len(dataSlab.elements)) != dataSlab.header.count {
		return 0, nil, nil, NewFatalError(fmt.Errorf("data slab %s header count %d is wrong, want %d",
			id, dataSlab.header.count, len(dataSlab.elements)))
	}

	// Verify that only root data slab can be inlined
	if dataSlab.Inlined() {
		if level > 0 {
			return 0, nil, nil, NewFatalError(fmt.Errorf("non-root slab %s is inlined", id))
		}
		if dataSlab.extraData == nil {
			return 0, nil, nil, NewFatalError(fmt.Errorf("inlined slab %s doesn't have extra data", id))
		}
		if dataSlab.next != SlabIDUndefined {
			return 0, nil, nil, NewFatalError(fmt.Errorf("inlined slab %s has next slab ID", id))
		}
	}

	// Verify that aggregated element size + slab prefix is the same as header.size
	computedSize := uint32(arrayDataSlabPrefixSize)
	if level == 0 {
		computedSize = uint32(arrayRootDataSlabPrefixSize)
		if dataSlab.Inlined() {
			computedSize = uint32(inlinedArrayDataSlabPrefixSize)
		}
	}

	for _, e := range dataSlab.elements {
		computedSize += e.ByteSize()
	}

	if computedSize != dataSlab.header.size {
		return 0, nil, nil, NewFatalError(fmt.Errorf("data slab %s header size %d is wrong, want %d",
			id, dataSlab.header.size, computedSize))
	}

	dataSlabIDs = append(dataSlabIDs, id)

	if dataSlab.next != SlabIDUndefined {
		nextDataSlabIDs = append(nextDataSlabIDs, dataSlab.next)
	}

	for _, e := range dataSlab.elements {

		value, err := e.StoredValue(v.storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return 0, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err,
				fmt.Sprintf(
					"data slab %s element %s can't be converted to value",
					id, e,
				))
		}

		// Verify element size <= inline size
		if e.ByteSize() > uint32(maxInlineArrayElementSize) {
			return 0, nil, nil, NewFatalError(fmt.Errorf("data slab %s element %s size %d is too large, want < %d",
				id, e, e.ByteSize(), maxInlineArrayElementSize))
		}

		switch e := e.(type) {
		case SlabIDStorable:
			// Verify not-inlined element > inline size, or can't be inlined
			if v.inlineEnabled {
				err = verifyNotInlinedValueStatusAndSize(value, uint32(maxInlineArrayElementSize))
				if err != nil {
					return 0, nil, nil, err
				}
			}

		case *ArrayDataSlab:
			// Verify inlined element's inlined status
			if !e.Inlined() {
				return 0, nil, nil, NewFatalError(fmt.Errorf("inlined array inlined status is false"))
			}

		case *MapDataSlab:
			// Verify inlined element's inlined status
			if !e.Inlined() {
				return 0, nil, nil, NewFatalError(fmt.Errorf("inlined map inlined status is false"))
			}
		}

		// Verify element
		err = verifyValue(value, v.address, nil, v.tic, v.hip, v.inlineEnabled, slabIDs)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifyValue().
			return 0, nil, nil, fmt.Errorf(
				"data slab %s element %q isn't valid: %w",
				id, e, err,
			)
		}
	}

	return dataSlab.header.count, dataSlabIDs, nextDataSlabIDs, nil
}

func (v *arrayVerifier) verifyMetaDataSlab(
	metaSlab *ArrayMetaDataSlab,
	level int,
	dataSlabIDs []SlabID,
	nextDataSlabIDs []SlabID,
	slabIDs map[SlabID]struct{},
) (
	elementCount uint32,
	_dataSlabIDs []SlabID,
	_nextDataSlabIDs []SlabID,
	err error,
) {
	id := metaSlab.header.slabID

	if metaSlab.IsData() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArrayMetaDataSlab %s is data", id))
	}

	if metaSlab.Inlined() {
		return 0, nil, nil, NewFatalError(fmt.Errorf("ArrayMetaDataSlab %s shouldn't be inlined", id))
	}

	if level == 0 {
		// Verify that root slab has more than one child slabs
		if len(metaSlab.childrenHeaders) < 2 {
			return 0, nil, nil, NewFatalError(fmt.Errorf("root metadata slab %d has %d children, want at least 2 children ",
				id, len(metaSlab.childrenHeaders)))
		}
	}

	// Verify childrenCountSum
	if len(metaSlab.childrenCountSum) != len(metaSlab.childrenHeaders) {
		return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d has %d childrenCountSum, want %d",
			id, len(metaSlab.childrenCountSum), len(metaSlab.childrenHeaders)))
	}

	computedCount := uint32(0)
	// NOTE: We don't use range loop here because &h is passed as argument to another function.
	// If we use range, then h would be a temporary object and we'd be passing address of
	// temporary object to function, which can lead to bugs depending on usage. It's not a bug
	// with the current usage but it's less fragile to future changes by not using range here.
	for i := 0; i < len(metaSlab.childrenHeaders); i++ {
		h := metaSlab.childrenHeaders[i]

		childSlab, err := getArraySlab(v.storage, h.slabID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getArraySlab().
			return 0, nil, nil, err
		}

		// Verify child slabs
		var count uint32
		count, dataSlabIDs, nextDataSlabIDs, err =
			v.verifySlab(childSlab, level+1, &h, dataSlabIDs, nextDataSlabIDs, slabIDs)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by verifySlab().
			return 0, nil, nil, err
		}

		computedCount += count

		// Verify childrenCountSum
		if metaSlab.childrenCountSum[i] != computedCount {
			return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d childrenCountSum[%d] is %d, want %d",
				id, i, metaSlab.childrenCountSum[i], computedCount))
		}
	}

	// Verify that aggregated element count is the same as header.count
	if computedCount != metaSlab.header.count {
		return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d header count %d is wrong, want %d",
			id, metaSlab.header.count, computedCount))
	}

	// Verify that aggregated header size + slab prefix is the same as header.size
	computedSize := uint32(len(metaSlab.childrenHeaders)*arraySlabHeaderSize) + arrayMetaDataSlabPrefixSize
	if computedSize != metaSlab.header.size {
		return 0, nil, nil, NewFatalError(fmt.Errorf("metadata slab %d header size %d is wrong, want %d",
			id, metaSlab.header.size, computedSize))
	}

	return metaSlab.header.count, dataSlabIDs, nextDataSlabIDs, nil
}

// verifyArrayValueID verifies array ValueID is always the same as
// root slab's SlabID indepedent of array's inlined status.
func verifyArrayValueID(a *Array) error {
	rootSlabID := a.root.Header().slabID

	vid := a.ValueID()

	if !bytes.Equal(vid[:SlabAddressLength], rootSlabID.address[:]) {
		return NewFatalError(
			fmt.Errorf(
				"expect first %d bytes of array value ID as %v, got %v",
				SlabAddressLength,
				rootSlabID.address[:],
				vid[:SlabAddressLength]))
	}

	if !bytes.Equal(vid[SlabAddressLength:], rootSlabID.index[:]) {
		return NewFatalError(
			fmt.Errorf(
				"expect second %d bytes of array value ID as %v, got %v",
				SlabIndexLength,
				rootSlabID.index[:],
				vid[SlabAddressLength:]))
	}

	return nil
}

// verifyArraySlabID verifies array SlabID is either empty for inlined array, or
// same as root slab's SlabID for not-inlined array.
func verifyArraySlabID(a *Array) error {
	sid := a.SlabID()

	if a.Inlined() {
		if sid != SlabIDUndefined {
			return NewFatalError(
				fmt.Errorf(
					"expect empty slab ID for inlined array, got %v",
					sid))
		}
		return nil
	}

	rootSlabID := a.root.Header().slabID

	if sid == SlabIDUndefined {
		return NewFatalError(
			fmt.Errorf(
				"expect non-empty slab ID for not-inlined array, got %v",
				sid))
	}

	if sid != rootSlabID {
		return NewFatalError(
			fmt.Errorf(
				"expect array slab ID same as root slab's slab ID %s, got %s",
				rootSlabID,
				sid))
	}

	return nil
}

func verifyNotInlinedValueStatusAndSize(v Value, maxInlineSize uint32) error {

	switch v := v.(type) {
	case *Array:
		// Verify not-inlined array's inlined status
		if v.root.Inlined() {
			return NewFatalError(
				fmt.Errorf(
					"not-inlined array %s has inlined status",
					v.root.Header().slabID))
		}

		// Verify not-inlined array size.
		if v.root.IsData() {
			inlinableSize := v.root.ByteSize() - arrayRootDataSlabPrefixSize + inlinedArrayDataSlabPrefixSize
			if inlinableSize <= maxInlineSize {
				return NewFatalError(
					fmt.Errorf("not-inlined array root slab %s can be inlined, inlinable size %d <= max inline size %d",
						v.root.Header().slabID,
						inlinableSize,
						maxInlineSize))
			}
		}

	case *OrderedMap:
		// Verify not-inlined map's inlined status
		if v.Inlined() {
			return NewFatalError(
				fmt.Errorf(
					"not-inlined map %s has inlined status",
					v.root.Header().slabID))
		}

		// Verify not-inlined map size.
		if v.root.IsData() {
			inlinableSize := v.root.ByteSize() - mapRootDataSlabPrefixSize + inlinedMapDataSlabPrefixSize
			if inlinableSize <= maxInlineSize {
				return NewFatalError(
					fmt.Errorf("not-inlined map root slab %s can be inlined, inlinable size %d <= max inline size %d",
						v.root.Header().slabID,
						inlinableSize,
						maxInlineSize))
			}
		}
	}

	return nil
}
