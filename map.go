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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/fxamacker/cbor/v2"
	"github.com/fxamacker/circlehash"
)

const (
	digestSize = 8

	// Encoded size of single element prefix size: CBOR array header (1 byte)
	singleElementPrefixSize = 1

	// Encoded size of inline collision group prefix size: CBOR tag number (2 bytes)
	inlineCollisionGroupPrefixSize = 2

	// Encoded size of external collision group prefix size: CBOR tag number (2 bytes)
	externalCollisionGroupPrefixSize = 2

	// Encoded size of digests: CBOR byte string head (3 bytes)
	digestPrefixSize = 3

	// Encoded size of number of elements: CBOR array head (3 bytes).
	elementPrefixSize = 3

	// hkey elements prefix size:
	// CBOR array header (1 byte) + level (1 byte) + hkeys byte string header (3 bytes) + elements array header (3 bytes)
	// Support up to 8,191 elements in the map per data slab.
	hkeyElementsPrefixSize = 1 + 1 + digestPrefixSize + elementPrefixSize

	// single elements prefix size:
	// CBOR array header (1 byte) + encoded level (1 byte) + hkeys byte string header (1 bytes) + elements array header (3 bytes)
	// Support up to 65,535 elements in the map per data slab.
	singleElementsPrefixSize = 1 + 1 + 1 + elementPrefixSize

	// slab header size: slab index (8 bytes) + size (2 bytes) + first digest (8 bytes)
	// Support up to 65,535 bytes for slab size limit (default limit is 1536 max bytes).
	mapSlabHeaderSize = slabIndexSize + 2 + digestSize

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + address (8 bytes) + child header count (2 bytes)
	// Support up to 65,535 children per metadata slab.
	mapMetaDataSlabPrefixSize = versionAndFlagSize + slabAddressSize + 2

	// version (1 byte) + flag (1 byte) + next id (16 bytes)
	mapDataSlabPrefixSize = versionAndFlagSize + slabIDSize

	// version (1 byte) + flag (1 byte)
	mapRootDataSlabPrefixSize = versionAndFlagSize

	// maxDigestLevel is max levels of 64-bit digests allowed
	maxDigestLevel = 8

	// typicalRandomConstant is a 64-bit value that has qualities
	// of a typical random value (e.g. hamming weight, number of
	// consecutive groups of 1-bits, etc.) so it can be useful as
	// a const part of a seed, round constant inside a permutation, etc.
	// CAUTION: We only store 64-bit seed, so some hashes with 64-bit seed like
	// CircleHash64f don't use this const.  However, other hashes such as
	// CircleHash64fx and SipHash might use this const as part of their
	// 128-bit seed (when they don't use 64-bit -> 128-bit seed expansion func).
	typicalRandomConstant = uint64(0x1BD11BDAA9FC1A22) // DO NOT MODIFY
)

// MaxCollisionLimitPerDigest is the noncryptographic hash collision limit
// (per digest per map) we enforce in the first level. In the same map
// for the same digest, having a non-intentional collision should be rare and
// several collisions should be extremely rare.  The default limit should
// be high enough to ignore accidental collisions while mitigating attacks.
var MaxCollisionLimitPerDigest = uint32(255)

type MapKey Storable

type MapValue Storable

// element is one indivisible unit that must stay together (e.g. collision group)
type element interface {
	fmt.Stringer

	Get(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapValue, error)

	// Set returns updated element, which may be a different type of element because of hash collision.
	Set(
		storage SlabStorage,
		address Address,
		b DigesterBuilder,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		hip HashInputProvider,
		key Value,
		value Value,
	) (newElem element, existingValue MapValue, err error)

	// Remove returns matched key, value, and updated element.
	// Updated element may be nil, modified, or a different type of element.
	Remove(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, element, error)

	Encode(*Encoder) error

	hasPointer() bool

	Size() uint32

	Count(storage SlabStorage) (uint32, error)

	PopIterate(SlabStorage, MapPopIterationFunc) error
}

// elementGroup is a group of elements that must stay together during splitting or rebalancing.
type elementGroup interface {
	element

	Inline() bool

	// Elements returns underlying elements.
	Elements(storage SlabStorage) (elements, error)
}

// elements is a list of elements.
type elements interface {
	fmt.Stringer

	Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapValue, error)
	Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level uint, hkey Digest, comparator ValueComparator, hip HashInputProvider, key Value, value Value) (existingValue MapValue, err error)
	Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error)

	Merge(elements) error
	Split() (elements, elements, error)

	LendToRight(elements) error
	BorrowFromRight(elements) error

	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	Element(int) (element, error)

	Encode(*Encoder) error

	hasPointer() bool

	firstKey() Digest

	Count() uint32

	Size() uint32

	PopIterate(SlabStorage, MapPopIterationFunc) error
}

type singleElement struct {
	key   MapKey
	value MapValue
	size  uint32
}

var _ element = &singleElement{}

type inlineCollisionGroup struct {
	elements
}

var _ element = &inlineCollisionGroup{}
var _ elementGroup = &inlineCollisionGroup{}

type externalCollisionGroup struct {
	slabID SlabID
	size   uint32
}

var _ element = &externalCollisionGroup{}
var _ elementGroup = &externalCollisionGroup{}

type hkeyElements struct {
	hkeys []Digest  // sorted list of unique hashed keys
	elems []element // elements corresponding to hkeys
	size  uint32    // total byte sizes
	level uint
}

var _ elements = &hkeyElements{}

type singleElements struct {
	elems []*singleElement // list of key+value pairs
	size  uint32           // total key+value byte sizes
	level uint
}

var _ elements = &singleElements{}

type MapSlabHeader struct {
	slabID   SlabID // id is used to retrieve slab from storage
	size     uint32 // size is used to split and merge; leaf: size of all element; internal: size of all headers
	firstKey Digest // firstKey (first hashed key) is used to lookup value
}

type MapExtraData struct {
	TypeInfo TypeInfo
	Count    uint64
	Seed     uint64
}

// MapDataSlab is leaf node, implementing MapSlab.
// anySize is true for data slab that isn't restricted by size requirement.
type MapDataSlab struct {
	next   SlabID
	header MapSlabHeader

	elements

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *MapExtraData

	anySize        bool
	collisionGroup bool
}

var _ MapSlab = &MapDataSlab{}

// MapMetaDataSlab is internal node, implementing MapSlab.
type MapMetaDataSlab struct {
	header          MapSlabHeader
	childrenHeaders []MapSlabHeader

	// extraData is data that is prepended to encoded slab data.
	// It isn't included in slab size calculation for splitting and merging.
	extraData *MapExtraData
}

var _ MapSlab = &MapMetaDataSlab{}

type MapSlab interface {
	Slab

	Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapValue, error)
	Set(storage SlabStorage, b DigesterBuilder, digester Digester, level uint, hkey Digest, comparator ValueComparator, hip HashInputProvider, key Value, value Value) (existingValue MapValue, err error)
	Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error)

	IsData() bool

	IsFull() bool
	IsUnderflow() (uint32, bool)
	CanLendToLeft(size uint32) bool
	CanLendToRight(size uint32) bool

	SetSlabID(SlabID)

	Header() MapSlabHeader

	ExtraData() *MapExtraData
	RemoveExtraData() *MapExtraData
	SetExtraData(*MapExtraData)

	PopIterate(SlabStorage, MapPopIterationFunc) error
}

type OrderedMap struct {
	Storage         SlabStorage
	root            MapSlab
	digesterBuilder DigesterBuilder
}

var _ Value = &OrderedMap{}

const mapExtraDataLength = 3

// newMapExtraDataFromData decodes CBOR array to extra data:
//
//	[type info, count, seed]
func newMapExtraDataFromData(
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapExtraData,
	[]byte,
	error,
) {
	dec := decMode.NewByteStreamDecoder(data)

	extraData, err := newMapExtraData(dec, decodeTypeInfo)
	if err != nil {
		return nil, data, err
	}

	return extraData, data[dec.NumBytesDecoded():], nil
}

func newMapExtraData(dec *cbor.StreamDecoder, decodeTypeInfo TypeInfoDecoder) (*MapExtraData, error) {

	length, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if length != mapExtraDataLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"data has invalid length %d, want %d",
				length,
				mapExtraDataLength,
			))
	}

	typeInfo, err := decodeTypeInfo(dec)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfoDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode type info")
	}

	count, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	seed, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	return &MapExtraData{
		TypeInfo: typeInfo,
		Count:    count,
		Seed:     seed,
	}, nil
}

// Encode encodes extra data as CBOR array:
//
//	[type info, count, seed]
func (m *MapExtraData) Encode(enc *Encoder) error {

	err := enc.CBOR.EncodeArrayHead(mapExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	err = m.TypeInfo.Encode(enc.CBOR)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by TypeInfo interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode type info")
	}

	err = enc.CBOR.EncodeUint64(m.Count)
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.EncodeUint64(m.Seed)
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func newElementFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder) (element, error) {
	nt, err := cborDec.NextType()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	switch nt {
	case cbor.ArrayType:
		// Don't need to wrap error as external error because err is already categorized by newSingleElementFromData().
		return newSingleElementFromData(cborDec, decodeStorable)

	case cbor.TagType:
		tagNum, err := cborDec.DecodeTagNumber()
		if err != nil {
			return nil, NewDecodingError(err)
		}
		switch tagNum {
		case CBORTagInlineCollisionGroup:
			// Don't need to wrap error as external error because err is already categorized by newInlineCollisionGroupFromData().
			return newInlineCollisionGroupFromData(cborDec, decodeStorable)
		case CBORTagExternalCollisionGroup:
			// Don't need to wrap error as external error because err is already categorized by newExternalCollisionGroupFromData().
			return newExternalCollisionGroupFromData(cborDec, decodeStorable)
		default:
			return nil, NewDecodingError(fmt.Errorf("failed to decode element: unrecognized tag number %d", tagNum))
		}

	default:
		return nil, NewDecodingError(fmt.Errorf("failed to decode element: unrecognized CBOR type %s", nt))
	}
}

func newSingleElement(storage SlabStorage, address Address, key Value, value Value) (*singleElement, error) {

	ks, err := key.Storable(storage, address, maxInlineMapKeySize)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's storable")
	}

	vs, err := value.Storable(storage, address, maxInlineMapValueSize(uint64(ks.ByteSize())))
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Value interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
	}

	return &singleElement{
		key:   ks,
		value: vs,
		size:  singleElementPrefixSize + ks.ByteSize() + vs.ByteSize(),
	}, nil
}

func newSingleElementFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder) (*singleElement, error) {
	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if elemCount != 2 {
		return nil, NewDecodingError(fmt.Errorf("failed to decode single element: expect array of 2 elements, got %d elements", elemCount))
	}

	key, err := decodeStorable(cborDec, SlabIDUndefined)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode key's storable")
	}

	value, err := decodeStorable(cborDec, SlabIDUndefined)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode value's storable")
	}

	return &singleElement{
		key:   key,
		value: value,
		size:  singleElementPrefixSize + key.ByteSize() + value.ByteSize(),
	}, nil
}

// Encode encodes singleElement to the given encoder.
//
//	CBOR encoded array of 2 elements (key, value).
func (e *singleElement) Encode(enc *Encoder) error {

	// Encode CBOR array head for 2 elements
	err := enc.CBOR.EncodeRawBytes([]byte{0x82})
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode key
	err = e.key.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map key")
	}

	// Encode value
	err = e.value.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map value")
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (e *singleElement) Get(storage SlabStorage, _ Digester, _ uint, _ Digest, comparator ValueComparator, key Value) (MapValue, error) {
	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}
	if equal {
		return e.value, nil
	}
	return nil, NewKeyNotFoundError(key)
}

// Set updates value if key matches, otherwise returns inlineCollisionGroup with existing and new elements.
// NOTE: Existing key needs to be rehashed because we store minimum digest for non-collision element.
//
//	Rehashing only happens when we create new inlineCollisionGroup.
//	Adding new element to existing inlineCollisionGroup doesn't require rehashing.
func (e *singleElement) Set(
	storage SlabStorage,
	address Address,
	b DigesterBuilder,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (element, MapValue, error) {

	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}

	// Key matches, overwrite existing value
	if equal {
		existingValue := e.value

		valueStorable, err := value.Storable(storage, address, maxInlineMapValueSize(uint64(e.key.ByteSize())))
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Value interface.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
		}

		e.value = valueStorable
		e.size = singleElementPrefixSize + e.key.ByteSize() + e.value.ByteSize()
		return e, existingValue, nil
	}

	// Hash collision detected

	// Create collision group with existing and new elements

	if level+1 == digester.Levels() {

		// Create singleElements group
		group := &inlineCollisionGroup{
			elements: newSingleElementsWithElement(level+1, e),
		}

		// Add new key and value to collision group
		// Don't need to wrap error as external error because err is already categorized by inlineCollisionGroup.Set().
		return group.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)

	}

	// Generate digest for existing key (see function comment)
	kv, err := e.key.StoredValue(storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's stored value")
	}

	existingKeyDigest, err := b.Digest(hip, kv)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigestBuilder interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's digester")
	}
	defer putDigester(existingKeyDigest)

	d, err := existingKeyDigest.Digest(level + 1)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digester interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get key's digest at level %d", level+1))
	}

	group := &inlineCollisionGroup{
		elements: newHkeyElementsWithElement(level+1, d, e),
	}

	// Add new key and value to collision group
	// Don't need to wrap error as external error because err is already categorized by inlineCollisionGroup.Set().
	return group.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
}

// Remove returns key, value, and nil element if key matches, otherwise returns error.
func (e *singleElement) Remove(storage SlabStorage, _ Digester, _ uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, element, error) {

	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}

	if equal {
		return e.key, e.value, nil, nil
	}

	return nil, nil, nil, NewKeyNotFoundError(key)
}

func (e *singleElement) hasPointer() bool {
	return hasPointer(e.key) || hasPointer(e.value)
}

func (e *singleElement) Size() uint32 {
	return e.size
}

func (e *singleElement) Count(_ SlabStorage) (uint32, error) {
	return 1, nil
}

func (e *singleElement) PopIterate(_ SlabStorage, fn MapPopIterationFunc) error {
	fn(e.key, e.value)
	return nil
}

func (e *singleElement) String() string {
	return fmt.Sprintf("%s:%s", e.key, e.value)
}

func newInlineCollisionGroupFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder) (*inlineCollisionGroup, error) {
	elements, err := newElementsFromData(cborDec, decodeStorable)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromData().
		return nil, err
	}

	return &inlineCollisionGroup{elements}, nil
}

// Encode encodes inlineCollisionGroup to the given encoder.
//
//	CBOR tag (number: CBORTagInlineCollisionGroup, content: elements)
func (e *inlineCollisionGroup) Encode(enc *Encoder) error {

	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number CBORTagInlineCollisionGroup
		0xd8, CBORTagInlineCollisionGroup,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	err = e.elements.Encode(enc)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Encode().
		return err
	}

	// TODO: is Flush necessary?
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (e *inlineCollisionGroup) Get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapValue, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey
	// Don't need to wrap error as external error because err is already categorized by elements.Get().
	return e.elements.Get(storage, digester, level, hkey, comparator, key)
}

func (e *inlineCollisionGroup) Set(
	storage SlabStorage,
	address Address,
	b DigesterBuilder,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (element, MapValue, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	existingValue, err := e.elements.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Set().
		return nil, nil, err
	}

	if level == 1 {
		// Export oversized inline collision group to separate slab (external collision group)
		// for first level collision.
		if e.Size() > uint32(maxInlineMapElementSize) {

			id, err := storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, nil, wrapErrorfAsExternalErrorIfNeeded(
					err,
					fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			// Create MapDataSlab
			slab := &MapDataSlab{
				header: MapSlabHeader{
					slabID:   id,
					size:     mapDataSlabPrefixSize + e.elements.Size(),
					firstKey: e.elements.firstKey(),
				},
				elements:       e.elements, // elems shouldn't be copied
				anySize:        true,
				collisionGroup: true,
			}

			err = storage.Store(id, slab)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", id))
			}

			// Create and return externalCollisionGroup (wrapper of newly created MapDataSlab)
			return &externalCollisionGroup{
				slabID: id,
				size:   externalCollisionGroupPrefixSize + SlabIDStorable(id).ByteSize(),
			}, existingValue, nil
		}
	}

	return e, existingValue, nil
}

// Remove returns key, value, and updated element if key is found.
// Updated element can be modified inlineCollisionGroup, or singleElement.
func (e *inlineCollisionGroup) Remove(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, element, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	k, v, err := e.elements.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Remove().
		return nil, nil, nil, err
	}

	// If there is only one single element in this group, return the single element (no collision).
	if e.elements.Count() == 1 {
		elem, err := e.elements.Element(0)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elements.Element().
			return nil, nil, nil, err
		}
		if _, ok := elem.(elementGroup); !ok {
			return k, v, elem, nil
		}
	}

	return k, v, e, nil
}

func (e *inlineCollisionGroup) hasPointer() bool {
	return e.elements.hasPointer()
}

func (e *inlineCollisionGroup) Size() uint32 {
	return inlineCollisionGroupPrefixSize + e.elements.Size()
}

func (e *inlineCollisionGroup) Inline() bool {
	return true
}

func (e *inlineCollisionGroup) Elements(_ SlabStorage) (elements, error) {
	return e.elements, nil
}

func (e *inlineCollisionGroup) Count(_ SlabStorage) (uint32, error) {
	return e.elements.Count(), nil
}

func (e *inlineCollisionGroup) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {
	// Don't need to wrap error as external error because err is already categorized by elements.PopIterate().
	return e.elements.PopIterate(storage, fn)
}

func (e *inlineCollisionGroup) String() string {
	return "inline[" + e.elements.String() + "]"
}

func newExternalCollisionGroupFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder) (*externalCollisionGroup, error) {

	storable, err := decodeStorable(cborDec, SlabIDUndefined)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode Storable")
	}

	idStorable, ok := storable.(SlabIDStorable)
	if !ok {
		return nil, NewDecodingError(fmt.Errorf("failed to decode external collision group: expect slab ID, got %T", storable))
	}

	return &externalCollisionGroup{
		slabID: SlabID(idStorable),
		size:   externalCollisionGroupPrefixSize + idStorable.ByteSize(),
	}, nil
}

// Encode encodes externalCollisionGroup to the given encoder.
//
//	CBOR tag (number: CBORTagExternalCollisionGroup, content: slab ID)
func (e *externalCollisionGroup) Encode(enc *Encoder) error {
	err := enc.CBOR.EncodeRawBytes([]byte{
		// tag number CBORTagExternalCollisionGroup
		0xd8, CBORTagExternalCollisionGroup,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	err = SlabIDStorable(e.slabID).Encode(enc)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by SlabIDStorable.Encode().
		return err
	}

	// TODO: is Flush necessary?
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (e *externalCollisionGroup) Get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapValue, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey
	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return slab.Get(storage, digester, level, hkey, comparator, key)
}

func (e *externalCollisionGroup) Set(storage SlabStorage, _ Address, b DigesterBuilder, digester Digester, level uint, _ Digest, comparator ValueComparator, hip HashInputProvider, key Value, value Value) (element, MapValue, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, err
	}

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	existingValue, err := slab.Set(storage, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, nil, err
	}
	return e, existingValue, nil
}

// Remove returns key, value, and updated element if key is found.
// Updated element can be modified externalCollisionGroup, or singleElement.
// TODO: updated element can be inlineCollisionGroup if size < maxInlineMapElementSize.
func (e *externalCollisionGroup) Remove(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, element, error) {

	slab, found, err := storage.Retrieve(e.slabID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", e.slabID))
	}
	if !found {
		return nil, nil, nil, NewSlabNotFoundErrorf(e.slabID, "external collision slab not found")
	}

	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return nil, nil, nil, NewSlabDataErrorf("slab %s isn't MapDataSlab", e.slabID)
	}

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	k, v, err := dataSlab.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapDataSlab.Remove().
		return nil, nil, nil, err
	}

	// TODO: if element size < maxInlineMapElementSize, return inlineCollisionGroup

	// If there is only one single element in this group, return the single element and remove external slab from storage.
	if dataSlab.elements.Count() == 1 {
		elem, err := dataSlab.elements.Element(0)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elements.Element().
			return nil, nil, nil, err
		}
		if _, ok := elem.(elementGroup); !ok {
			err := storage.Remove(e.slabID)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", e.slabID))
			}
			return k, v, elem, nil
		}
	}

	return k, v, e, nil
}

func (e *externalCollisionGroup) hasPointer() bool {
	return true
}

func (e *externalCollisionGroup) Size() uint32 {
	return e.size
}

func (e *externalCollisionGroup) Inline() bool {
	return false
}

func (e *externalCollisionGroup) Elements(storage SlabStorage) (elements, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}
	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return nil, NewSlabDataErrorf("slab %s isn't MapDataSlab", e.slabID)
	}
	return dataSlab.elements, nil
}

func (e *externalCollisionGroup) Count(storage SlabStorage) (uint32, error) {
	elements, err := e.Elements(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by externalCollisionGroup.Elements().
		return 0, err
	}
	return elements.Count(), nil
}

func (e *externalCollisionGroup) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {
	elements, err := e.Elements(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by externalCollisionGroup.Elements().
		return err
	}

	err = elements.PopIterate(storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.PopIterate().
		return err
	}

	err = storage.Remove(e.slabID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", e.slabID))
	}
	return nil
}

func (e *externalCollisionGroup) String() string {
	return fmt.Sprintf("external(%s)", e.slabID)
}

func newElementsFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder) (elements, error) {

	arrayCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != 3 {
		return nil, NewDecodingError(fmt.Errorf("decoding elements failed: expect array of 3 elements, got %d elements", arrayCount))
	}

	level, err := cborDec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	digestBytes, err := cborDec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if len(digestBytes)%digestSize != 0 {
		return nil, NewDecodingError(fmt.Errorf("decoding digests failed: number of bytes is not multiple of %d", digestSize))
	}

	digestCount := len(digestBytes) / digestSize
	hkeys := make([]Digest, digestCount)
	for i := 0; i < digestCount; i++ {
		hkeys[i] = Digest(binary.BigEndian.Uint64(digestBytes[i*digestSize:]))
	}

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if digestCount != 0 && uint64(digestCount) != elemCount {
		return nil, NewDecodingError(fmt.Errorf("decoding elements failed: number of hkeys %d isn't the same as number of elements %d", digestCount, elemCount))
	}

	if digestCount == 0 && elemCount > 0 {
		// elements are singleElements

		// Decode elements
		size := uint32(singleElementsPrefixSize)
		elems := make([]*singleElement, elemCount)
		for i := 0; i < int(elemCount); i++ {
			elem, err := newSingleElementFromData(cborDec, decodeStorable)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by newSingleElementFromData().
				return nil, err
			}

			elems[i] = elem
			size += elem.Size()
		}

		// Create singleElements
		elements := &singleElements{
			elems: elems,
			level: uint(level),
			size:  size,
		}

		return elements, nil
	}

	// elements are hkeyElements

	// Decode elements
	size := uint32(hkeyElementsPrefixSize)
	elems := make([]element, elemCount)
	for i := 0; i < int(elemCount); i++ {
		elem, err := newElementFromData(cborDec, decodeStorable)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newElementFromData().
			return nil, err
		}

		elems[i] = elem
		size += digestSize + elem.Size()
	}

	// Create hkeyElements
	elements := &hkeyElements{
		hkeys: hkeys,
		elems: elems,
		level: uint(level),
		size:  size,
	}

	return elements, nil
}

func newHkeyElements(level uint) *hkeyElements {
	return &hkeyElements{
		level: level,
		size:  hkeyElementsPrefixSize,
	}
}

func newHkeyElementsWithElement(level uint, hkey Digest, elem element) *hkeyElements {
	return &hkeyElements{
		hkeys: []Digest{hkey},
		elems: []element{elem},
		size:  hkeyElementsPrefixSize + digestSize + elem.Size(),
		level: level,
	}
}

// Encode encodes hkeyElements to the given encoder.
//
//	CBOR encoded array [
//	    0: level (uint)
//	    1: hkeys (byte string)
//	    2: elements (array)
//	]
func (e *hkeyElements) Encode(enc *Encoder) error {

	if e.level > maxDigestLevel {
		return NewFatalError(fmt.Errorf("hash level %d exceeds max digest level %d", e.level, maxDigestLevel))
	}

	// Encode CBOR array head of 3 elements (level, hkeys, elements)
	const cborArrayHeadOfThreeElements = 0x83
	enc.Scratch[0] = cborArrayHeadOfThreeElements

	// Encode hash level
	enc.Scratch[1] = byte(e.level)

	// Encode hkeys as byte string

	// Encode hkeys bytes header manually for fix-sized encoding
	// TODO: maybe make this header dynamic to reduce size
	// CBOR byte string head 0x59 indicates that the number of bytes in byte string are encoded in the next 2 bytes.
	const cborByteStringHead = 0x59
	enc.Scratch[2] = cborByteStringHead

	binary.BigEndian.PutUint16(enc.Scratch[3:], uint16(len(e.hkeys)*8))

	// Write scratch content to encoder
	const totalSize = 5
	err := enc.CBOR.EncodeRawBytes(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode hkeys
	for i := 0; i < len(e.hkeys); i++ {
		binary.BigEndian.PutUint64(enc.Scratch[:], uint64(e.hkeys[i]))
		err = enc.CBOR.EncodeRawBytes(enc.Scratch[:digestSize])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode elements

	// Encode elements array header manually for fix-sized encoding
	// TODO: maybe make this header dynamic to reduce size
	// CBOR array head 0x99 indicating that the number of array elements are encoded in the next 2 bytes.
	const cborArrayHead = 0x99
	enc.Scratch[0] = cborArrayHead
	binary.BigEndian.PutUint16(enc.Scratch[1:], uint16(len(e.elems)))
	err = enc.CBOR.EncodeRawBytes(enc.Scratch[:3])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode each element
	for _, e := range e.elems {
		err = e.Encode(enc)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by element.Encode().
			return err
		}
	}

	// TODO: is Flush necessary
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (e *hkeyElements) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapValue, error) {

	if level >= digester.Levels() {
		return nil, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
	}

	// binary search by hkey

	// Find index that e.hkeys[h] == hkey
	equalIndex := -1
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// No matching hkey
	if equalIndex == -1 {
		return nil, NewKeyNotFoundError(key)
	}

	elem := e.elems[equalIndex]

	// Don't need to wrap error as external error because err is already categorized by element.Get().
	return elem.Get(storage, digester, level, hkey, comparator, key)
}

func (e *hkeyElements) Set(storage SlabStorage, address Address, b DigesterBuilder, digester Digester, level uint, hkey Digest, comparator ValueComparator, hip HashInputProvider, key Value, value Value) (MapValue, error) {

	// Check hkeys are not empty
	if level >= digester.Levels() {
		return nil, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
	}

	if len(e.hkeys) == 0 {
		// first element

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, err
		}

		e.hkeys = []Digest{hkey}

		e.elems = []element{newElem}

		e.size += digestSize + newElem.Size()

		return nil, nil
	}

	if hkey < e.hkeys[0] {
		// prepend key and value

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, err
		}

		e.hkeys = append(e.hkeys, Digest(0))
		copy(e.hkeys[1:], e.hkeys)
		e.hkeys[0] = hkey

		e.elems = append(e.elems, nil)
		copy(e.elems[1:], e.elems)
		e.elems[0] = newElem

		e.size += digestSize + newElem.Size()

		return nil, nil
	}

	if hkey > e.hkeys[len(e.hkeys)-1] {
		// append key and value

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, err
		}

		e.hkeys = append(e.hkeys, hkey)

		e.elems = append(e.elems, newElem)

		e.size += digestSize + newElem.Size()

		return nil, nil
	}

	equalIndex := -1   // first index that m.hkeys[h] == hkey
	lessThanIndex := 0 // last index that m.hkeys[h] > hkey
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			lessThanIndex = h
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// hkey digest has collision.
	if equalIndex != -1 {
		// New element has the same digest as existing elem.
		// elem is existing element before new element is inserted.
		elem := e.elems[equalIndex]

		// Enforce MaxCollisionLimitPerDigest at the first level (noncryptographic hash).
		if e.level == 0 {

			// Before new element with colliding digest is inserted,
			// existing elem is a single element or a collision group.
			// elem.Count() returns 1 for single element,
			// and returns > 1 for collision group.
			elementCount, err := elem.Count(storage)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by element.Count().
				return nil, err
			}
			if elementCount == 0 {
				return nil, NewMapElementCountError("expect element count > 0, got element count == 0")
			}

			// collisionCount is elementCount-1 because:
			// - if elem is single element, collision count is 0 (no collsion yet)
			// - if elem is collision group, collision count is 1 less than number
			//   of elements in collision group.
			collisionCount := elementCount - 1

			// Check if existing collision count reached MaxCollisionLimitPerDigest
			if collisionCount >= MaxCollisionLimitPerDigest {
				// Enforce collision limit on inserts and ignore updates.
				_, err = elem.Get(storage, digester, level, hkey, comparator, key)
				if err != nil {
					var knfe *KeyNotFoundError
					if errors.As(err, &knfe) {
						// Don't allow any more collisions for a digest that
						// already reached MaxCollisionLimitPerDigest.
						return nil, NewCollisionLimitError(MaxCollisionLimitPerDigest)
					}
				}
			}
		}

		elem, existingValue, err := elem.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by element.Set().
			return nil, err
		}

		e.elems[equalIndex] = elem

		// Recompute slab size by adding all element sizes instead of using the size diff of old and new element because
		// oldElem can be the same storable when the same value is reset and oldElem.ByteSize() can equal storable.ByteSize().
		// Given this, size diff of the old and new element can be 0 even when its actual size changed.
		size := uint32(hkeyElementsPrefixSize)
		for _, element := range e.elems {
			size += element.Size() + digestSize
		}
		e.size = size

		return existingValue, nil
	}

	// No matching hkey

	newElem, err := newSingleElement(storage, address, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newSingleElement().
		return nil, err
	}

	// insert into sorted hkeys
	e.hkeys = append(e.hkeys, Digest(0))
	copy(e.hkeys[lessThanIndex+1:], e.hkeys[lessThanIndex:])
	e.hkeys[lessThanIndex] = hkey

	// insert into sorted elements
	e.elems = append(e.elems, nil)
	copy(e.elems[lessThanIndex+1:], e.elems[lessThanIndex:])
	e.elems[lessThanIndex] = newElem

	e.size += digestSize + newElem.Size()

	return nil, nil
}

func (e *hkeyElements) Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	// Check digest level
	if level >= digester.Levels() {
		return nil, nil, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
	}

	if len(e.hkeys) == 0 || hkey < e.hkeys[0] || hkey > e.hkeys[len(e.hkeys)-1] {
		return nil, nil, NewKeyNotFoundError(key)
	}

	// binary search by hkey

	// Find index that e.hkeys[h] == hkey
	equalIndex := -1
	i, j := 0, len(e.hkeys)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if e.hkeys[h] > hkey {
			j = h
		} else if e.hkeys[h] < hkey {
			i = h + 1
		} else {
			equalIndex = h
			break
		}
	}

	// No matching hkey
	if equalIndex == -1 {
		return nil, nil, NewKeyNotFoundError(key)
	}

	elem := e.elems[equalIndex]

	oldElemSize := elem.Size()

	k, v, elem, err := elem.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by element.Remove().
		return nil, nil, err
	}

	if elem == nil {
		// Remove this element
		copy(e.elems[equalIndex:], e.elems[equalIndex+1:])
		// Zero out last element to prevent memory leak
		e.elems[len(e.elems)-1] = nil
		// Reslice elements
		e.elems = e.elems[:len(e.elems)-1]

		// Remove hkey for this element
		copy(e.hkeys[equalIndex:], e.hkeys[equalIndex+1:])
		e.hkeys = e.hkeys[:len(e.hkeys)-1]

		// Adjust size
		e.size -= digestSize + oldElemSize

		return k, v, nil
	}

	e.elems[equalIndex] = elem

	e.size += elem.Size() - oldElemSize

	return k, v, nil
}

func (e *hkeyElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, NewIndexOutOfBoundsError(uint64(i), 0, uint64(len(e.elems)))
	}
	return e.elems[i], nil
}

func (e *hkeyElements) hasPointer() bool {
	for _, elem := range e.elems {
		if elem.hasPointer() {
			return true
		}
	}
	return false
}

func (e *hkeyElements) Merge(elems elements) error {

	rElems, ok := elems.(*hkeyElements)
	if !ok {
		return NewSlabMergeError(fmt.Errorf("cannot merge elements of different types (%T, %T)", e, elems))
	}

	e.hkeys = append(e.hkeys, rElems.hkeys...)
	e.elems = append(e.elems, rElems.elems...)
	e.size += rElems.Size() - hkeyElementsPrefixSize

	// Set merged elements to nil to prevent memory leak
	for i := 0; i < len(rElems.elems); i++ {
		rElems.elems[i] = nil
	}

	return nil
}

func (e *hkeyElements) Split() (elements, elements, error) {

	// This computes the ceil of split to give the first slab more elements.
	dataSize := e.Size() - hkeyElementsPrefixSize
	midPoint := (dataSize + 1) >> 1

	leftSize := uint32(0)
	leftCount := 0
	for i, elem := range e.elems {
		elemSize := elem.Size() + digestSize
		if leftSize+elemSize >= midPoint {
			// i is mid point element.  Place i on the small side.
			if leftSize <= dataSize-leftSize-elemSize {
				leftSize += elemSize
				leftCount = i + 1
			} else {
				leftCount = i
			}
			break
		}
		// left slab size < midPoint
		leftSize += elemSize
	}

	rightCount := len(e.elems) - leftCount

	// Create right slab elements
	rightElements := &hkeyElements{level: e.level}

	rightElements.hkeys = make([]Digest, rightCount)
	copy(rightElements.hkeys, e.hkeys[leftCount:])

	rightElements.elems = make([]element, rightCount)
	copy(rightElements.elems, e.elems[leftCount:])

	rightElements.size = dataSize - leftSize + hkeyElementsPrefixSize

	e.hkeys = e.hkeys[:leftCount]
	e.elems = e.elems[:leftCount]
	e.size = hkeyElementsPrefixSize + leftSize

	// NOTE: prevent memory leak
	for i := leftCount; i < len(e.hkeys); i++ {
		e.elems[i] = nil
	}

	return e, rightElements, nil
}

// LendToRight rebalances elements by moving elements from left to right
func (e *hkeyElements) LendToRight(re elements) error {

	minSize := minThreshold - mapDataSlabPrefixSize - hkeyElementsPrefixSize

	rightElements := re.(*hkeyElements)

	if e.level != rightElements.level {
		return NewSlabRebalanceError(
			NewHashLevelErrorf("left slab digest level %d != right slab digest level %d", e.level, rightElements.level),
		)
	}

	count := len(e.elems) + len(rightElements.elems)
	size := e.Size() + rightElements.Size() - hkeyElementsPrefixSize*2

	leftCount := len(e.elems)
	leftSize := e.Size() - hkeyElementsPrefixSize

	midPoint := (size + 1) >> 1

	// Left elements size is as close to midPoint as possible while right elements size >= minThreshold
	for i := len(e.elems) - 1; i >= 0; i-- {
		elemSize := e.elems[i].Size() + digestSize
		if leftSize-elemSize < midPoint && size-leftSize >= uint32(minSize) {
			break
		}
		leftSize -= elemSize
		leftCount--
	}

	// Update the right elements
	//
	// It is easier and less error-prone to realloc elements for the right elements.

	hkeys := make([]Digest, count-leftCount)
	n := copy(hkeys, e.hkeys[leftCount:])
	copy(hkeys[n:], rightElements.hkeys)

	elements := make([]element, count-leftCount)
	n = copy(elements, e.elems[leftCount:])
	copy(elements[n:], rightElements.elems)

	rightElements.hkeys = hkeys
	rightElements.elems = elements
	rightElements.size = size - leftSize + hkeyElementsPrefixSize

	// Update left slab
	// NOTE: prevent memory leak
	for i := leftCount; i < len(e.elems); i++ {
		e.elems[i] = nil
	}
	e.hkeys = e.hkeys[:leftCount]
	e.elems = e.elems[:leftCount]
	e.size = hkeyElementsPrefixSize + leftSize

	return nil
}

// BorrowFromRight rebalances slabs by moving elements from right slab to left slab.
func (e *hkeyElements) BorrowFromRight(re elements) error {

	minSize := minThreshold - mapDataSlabPrefixSize - hkeyElementsPrefixSize

	rightElements := re.(*hkeyElements)

	if e.level != rightElements.level {
		return NewSlabRebalanceError(
			NewHashLevelErrorf("left slab digest level %d != right slab digest level %d", e.level, rightElements.level),
		)
	}

	size := e.Size() + rightElements.Size() - hkeyElementsPrefixSize*2

	leftCount := len(e.elems)
	leftSize := e.Size() - hkeyElementsPrefixSize

	midPoint := (size + 1) >> 1

	for _, elem := range rightElements.elems {
		elemSize := elem.Size() + digestSize
		if leftSize+elemSize > midPoint {
			if size-leftSize-elemSize >= uint32(minSize) {
				// Include this element in left elements
				leftSize += elemSize
				leftCount++
			}
			break
		}
		leftSize += elemSize
		leftCount++
	}

	rightStartIndex := leftCount - len(e.elems)

	// Update left elements
	e.hkeys = append(e.hkeys, rightElements.hkeys[:rightStartIndex]...)
	e.elems = append(e.elems, rightElements.elems[:rightStartIndex]...)
	e.size = leftSize + hkeyElementsPrefixSize

	// Update right slab
	// TODO: copy elements to front instead?
	// NOTE: prevent memory leak
	for i := 0; i < rightStartIndex; i++ {
		rightElements.elems[i] = nil
	}
	rightElements.hkeys = rightElements.hkeys[rightStartIndex:]
	rightElements.elems = rightElements.elems[rightStartIndex:]
	rightElements.size = size - leftSize + hkeyElementsPrefixSize

	return nil
}

func (e *hkeyElements) CanLendToLeft(size uint32) bool {
	if len(e.elems) == 0 {
		return false
	}

	if len(e.elems) < 2 {
		return false
	}

	minSize := minThreshold - mapDataSlabPrefixSize
	if e.Size()-size < uint32(minSize) {
		return false
	}

	lendSize := uint32(0)
	for i := 0; i < len(e.elems); i++ {
		lendSize += e.elems[i].Size() + digestSize
		if e.Size()-lendSize < uint32(minSize) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

func (e *hkeyElements) CanLendToRight(size uint32) bool {
	if len(e.elems) == 0 {
		return false
	}

	if len(e.elems) < 2 {
		return false
	}

	minSize := minThreshold - mapDataSlabPrefixSize
	if e.Size()-size < uint32(minSize) {
		return false
	}

	lendSize := uint32(0)
	for i := len(e.elems) - 1; i >= 0; i-- {
		lendSize += e.elems[i].Size() + digestSize
		if e.Size()-lendSize < uint32(minSize) {
			return false
		}
		if lendSize >= size {
			return true
		}
	}
	return false
}

func (e *hkeyElements) Size() uint32 {
	return e.size
}

func (e *hkeyElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *hkeyElements) firstKey() Digest {
	if len(e.hkeys) > 0 {
		return e.hkeys[0]
	}
	return 0
}

func (e *hkeyElements) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {

	// Iterate and reset elements backwards
	for i := len(e.elems) - 1; i >= 0; i-- {
		elem := e.elems[i]

		err := elem.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by element.PopIterate().
			return err
		}
	}

	// Reset data slab
	e.hkeys = nil
	e.elems = nil
	e.size = hkeyElementsPrefixSize

	return nil
}

func (e *hkeyElements) String() string {
	var s []string

	for i := 0; i < len(e.elems); i++ {
		s = append(s, fmt.Sprintf("%d:%s", e.hkeys[i], e.elems[i].String()))
	}

	return strings.Join(s, " ")
}

func newSingleElementsWithElement(level uint, elem *singleElement) *singleElements {
	return &singleElements{
		level: level,
		size:  singleElementsPrefixSize + elem.size,
		elems: []*singleElement{elem},
	}
}

// Encode encodes singleElements to the given encoder.
//
//	CBOR encoded array [
//	    0: level (uint)
//	    1: hkeys (0 length byte string)
//	    2: elements (array)
//	]
func (e *singleElements) Encode(enc *Encoder) error {

	if e.level > maxDigestLevel {
		return NewFatalError(fmt.Errorf("digest level %d exceeds max digest level %d", e.level, maxDigestLevel))
	}

	// Encode CBOR array header for 3 elements (level, hkeys, elements)
	enc.Scratch[0] = 0x83

	// Encode hash level
	enc.Scratch[1] = byte(e.level)

	// Encode hkeys (empty byte string)
	enc.Scratch[2] = 0x40

	// Encode elements

	// Encode elements array header manually for fix-sized encoding
	// TODO: maybe make this header dynamic to reduce size
	enc.Scratch[3] = 0x99
	binary.BigEndian.PutUint16(enc.Scratch[4:], uint16(len(e.elems)))

	// Write scratch content to encoder
	const totalSize = 6
	err := enc.CBOR.EncodeRawBytes(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode each element
	for _, e := range e.elems {
		err = e.Encode(enc)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by singleElement.Encode().
			return err
		}
	}

	// TODO: is Flush necessar?
	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (e *singleElements) Get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapValue, error) {

	if level != digester.Levels() {
		return nil, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search by key
	for _, elem := range e.elems {
		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}
		if equal {
			return elem.value, nil
		}
	}

	return nil, NewKeyNotFoundError(key)
}

func (e *singleElements) Set(storage SlabStorage, address Address, _ DigesterBuilder, digester Digester, level uint, _ Digest, comparator ValueComparator, _ HashInputProvider, key Value, value Value) (MapValue, error) {

	if level != digester.Levels() {
		return nil, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search key and update value
	for i := 0; i < len(e.elems); i++ {
		elem := e.elems[i]

		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}

		if equal {
			existingValue := elem.value

			vs, err := value.Storable(storage, address, maxInlineMapValueSize(uint64(elem.key.ByteSize())))
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by Value interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
			}

			elem.value = vs
			elem.size = singleElementPrefixSize + elem.key.ByteSize() + elem.value.ByteSize()

			// Recompute slab size by adding all element sizes instead of using the size diff of old and new element because
			// oldElem can be the same storable when the same value is reset and oldElem.ByteSize() can equal storable.ByteSize().
			// Given this, size diff of the old and new element can be 0 even when its actual size changed.
			size := uint32(singleElementsPrefixSize)
			for _, element := range e.elems {
				size += element.Size()
			}
			e.size = size

			return existingValue, nil
		}
	}

	// no matching key, append new element to the end.
	newElem, err := newSingleElement(storage, address, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newSingleElement().
		return nil, err
	}
	e.elems = append(e.elems, newElem)
	e.size += newElem.size

	return nil, nil
}

func (e *singleElements) Remove(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	if level != digester.Levels() {
		return nil, nil, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search by key
	for i, elem := range e.elems {

		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}

		if equal {
			// Remove this element
			copy(e.elems[i:], e.elems[i+1:])
			// Zero out last element to prevent memory leak
			e.elems[len(e.elems)-1] = nil
			// Reslice elements
			e.elems = e.elems[:len(e.elems)-1]

			// Adjust size
			e.size -= elem.Size()

			return elem.key, elem.value, nil
		}
	}

	return nil, nil, NewKeyNotFoundError(key)
}

func (e *singleElements) Element(i int) (element, error) {
	if i >= len(e.elems) {
		return nil, NewIndexOutOfBoundsError(uint64(i), 0, uint64(len(e.elems)))
	}
	return e.elems[i], nil
}

func (e *singleElements) Merge(_ elements) error {
	return NewNotApplicableError("singleElements", "elements", "Merge")
}

func (e *singleElements) Split() (elements, elements, error) {
	return nil, nil, NewNotApplicableError("singleElements", "elements", "Split")
}

func (e *singleElements) LendToRight(_ elements) error {
	return NewNotApplicableError("singleElements", "elements", "LendToRight")
}

func (e *singleElements) BorrowFromRight(_ elements) error {
	return NewNotApplicableError("singleElements", "elements", "BorrowFromRight")
}

func (e *singleElements) CanLendToLeft(_ uint32) bool {
	return false
}

func (e *singleElements) CanLendToRight(_ uint32) bool {
	return false
}

func (e *singleElements) hasPointer() bool {
	for _, elem := range e.elems {
		if elem.hasPointer() {
			return true
		}
	}
	return false
}

func (e *singleElements) Count() uint32 {
	return uint32(len(e.elems))
}

func (e *singleElements) firstKey() Digest {
	return 0
}

func (e *singleElements) Size() uint32 {
	return e.size
}

func (e *singleElements) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {

	// Iterate and reset elements backwards
	for i := len(e.elems) - 1; i >= 0; i-- {
		elem := e.elems[i]

		err := elem.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by singleElement.PopIterate().
			return err
		}
	}

	// Reset data slab
	e.elems = nil
	e.size = singleElementsPrefixSize

	return nil
}

func (e *singleElements) String() string {
	var s []string

	for i := 0; i < len(e.elems); i++ {
		s = append(s, fmt.Sprintf(":%s", e.elems[i].String()))
	}

	return strings.Join(s, " ")
}

func newMapDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for map data slab")
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	mapType := h.getSlabMapType()

	if mapType != slabMapData && mapType != slabMapCollisionGroup {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want map data slab flag or map collision group flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newMapDataSlabFromDataV0(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	case 1:
		return newMapDataSlabFromDataV1(id, h, data, decMode, decodeStorable, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for map data slab", h.version())
	}
}

// newMapDataSlabFromDataV0 decodes data in version 0:
//
// Root DataSlab Header:
//
//	+-------------------------------+------------+-------------------------------+
//	| slab version + flag (2 bytes) | extra data | slab version + flag (2 bytes) |
//	+-------------------------------+------------+-------------------------------+
//
// Non-root DataSlab Header (18 bytes):
//
//	+-------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | next sib slab ID (16 bytes) |
//	+-------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See hkeyElements.Encode() and singleElements.Encode() for elements section format.
func newMapDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapDataSlab,
	error,
) {
	var err error
	var extraData *MapExtraData

	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		data = data[versionAndFlagSize:]
	}

	var next SlabID

	if !h.isRoot() {
		// Check data length for next slab ID
		if len(data) < slabIDSize {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		// Decode next slab ID
		var err error
		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		data = data[slabIDSize:]
	}

	// Decode elements
	cborDec := decMode.NewByteStreamDecoder(data)
	elements, err := newElementsFromData(cborDec, decodeStorable)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromDataV0().
		return nil, err
	}

	// Compute slab size for version 1.
	slabSize := versionAndFlagSize + elements.Size()
	if !h.isRoot() {
		slabSize += slabIDSize
	}

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: elements.firstKey(),
	}

	return &MapDataSlab{
		next:           next,
		header:         header,
		elements:       elements,
		extraData:      extraData,
		anySize:        !h.hasSizeLimit(),
		collisionGroup: h.getSlabMapType() == slabMapCollisionGroup,
	}, nil
}

// newMapDataSlabFromDataV1 decodes data in version 1:
//
// Root DataSlab Header:
//
//	+-------------------------------+------------+
//	| slab version + flag (2 bytes) | extra data |
//	+-------------------------------+------------+
//
// Non-root DataSlab Header (18 bytes):
//
//	+-------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | next sib slab ID (16 bytes) |
//	+-------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See hkeyElements.Encode() and singleElements.Encode() for elements section format.
func newMapDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapDataSlab,
	error,
) {
	var err error
	var extraData *MapExtraData
	var next SlabID

	// Decode extra data
	if h.isRoot() {
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}
	}

	// Decode next slab ID
	if h.hasNextSlabID() {
		if len(data) < slabIDSize {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		data = data[slabIDSize:]
	}

	// Decode elements
	cborDec := decMode.NewByteStreamDecoder(data)
	elements, err := newElementsFromData(cborDec, decodeStorable)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromDataV1().
		return nil, err
	}

	// Compute slab size.
	slabSize := versionAndFlagSize + elements.Size()
	if !h.isRoot() {
		slabSize += slabIDSize
	}

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: elements.firstKey(),
	}

	return &MapDataSlab{
		next:           next,
		header:         header,
		elements:       elements,
		extraData:      extraData,
		anySize:        !h.hasSizeLimit(),
		collisionGroup: h.getSlabMapType() == slabMapCollisionGroup,
	}, nil
}

// Encode encodes this map data slab to the given encoder.
//
// Root DataSlab Header:
//
//	+-------------------------------+------------+
//	| slab version + flag (2 bytes) | extra data |
//	+-------------------------------+------------+
//
// Non-root DataSlab Header (18 bytes):
//
//	+-------------------------------+-------------------------+
//	| slab version + flag (2 bytes) | next slab ID (16 bytes) |
//	+-------------------------------+-------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See hkeyElements.Encode() and singleElements.Encode() for elements section format.
func (m *MapDataSlab) Encode(enc *Encoder) error {

	const version = 1

	slabType := slabMapData
	if m.collisionGroup {
		slabType = slabMapCollisionGroup
	}

	h, err := newMapSlabHead(version, slabType)
	if err != nil {
		return NewEncodingError(err)
	}

	if m.hasPointer() {
		h.setHasPointers()
	}

	if m.next != SlabIDUndefined {
		h.setHasNextSlabID()
	}

	if m.anySize {
		h.setNoSizeLimit()
	}

	if m.extraData != nil {
		h.setRoot()
	}

	// Write head (version + flag)
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data
	if m.extraData != nil {
		err = m.extraData.Encode(enc)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapExtraData.Encode().
			return err
		}
	}

	// Encode next slab ID
	if m.next != SlabIDUndefined {
		n, err := m.next.ToRawBytes(enc.Scratch[:])
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by SlabID.ToRawBytes().
			return err
		}

		// Write scratch content to encoder
		_, err = enc.Write(enc.Scratch[:n])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode elements
	err = m.elements.Encode(enc)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Encode().
		return err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (m *MapDataSlab) hasPointer() bool {
	return m.elements.hasPointer()
}

func (m *MapDataSlab) ChildStorables() []Storable {
	return elementsStorables(m.elements, nil)
}

func (m *MapDataSlab) getPrefixSize() uint32 {
	if m.extraData != nil {
		return mapRootDataSlabPrefixSize
	}
	return mapDataSlabPrefixSize
}

func elementsStorables(elems elements, childStorables []Storable) []Storable {

	switch v := elems.(type) {

	case *hkeyElements:
		for i := 0; i < len(v.elems); i++ {
			childStorables = elementStorables(v.elems[i], childStorables)
		}

	case *singleElements:
		for i := 0; i < len(v.elems); i++ {
			childStorables = elementStorables(v.elems[i], childStorables)
		}

	}

	return childStorables
}

func elementStorables(e element, childStorables []Storable) []Storable {

	switch v := e.(type) {

	case *externalCollisionGroup:
		return append(childStorables, SlabIDStorable(v.slabID))

	case *inlineCollisionGroup:
		return elementsStorables(v.elements, childStorables)

	case *singleElement:
		return append(childStorables, v.key, v.value)
	}

	panic(NewUnreachableError())
}

func (m *MapDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	if m.extraData == nil {
		return nil, NewNotValueError(m.SlabID())
	}

	digestBuilder := NewDefaultDigesterBuilder()

	digestBuilder.SetSeed(m.extraData.Seed, typicalRandomConstant)

	return &OrderedMap{
		Storage:         storage,
		root:            m,
		digesterBuilder: digestBuilder,
	}, nil
}

func (m *MapDataSlab) Set(storage SlabStorage, b DigesterBuilder, digester Digester, level uint, hkey Digest, comparator ValueComparator, hip HashInputProvider, key Value, value Value) (MapValue, error) {

	existingValue, err := m.elements.Set(storage, m.SlabID().address, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Set().
		return nil, err
	}

	// Adjust header's first key
	m.header.firstKey = m.elements.firstKey()

	// Adjust header's slab size
	m.header.size = m.getPrefixSize() + m.elements.Size()

	// Store modified slab
	err = storage.Store(m.header.slabID, m)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
	}

	return existingValue, nil
}

func (m *MapDataSlab) Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	k, v, err := m.elements.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Remove().
		return nil, nil, err
	}

	// Adjust header's first key
	m.header.firstKey = m.elements.firstKey()

	// Adjust header's slab size
	m.header.size = m.getPrefixSize() + m.elements.Size()

	// Store modified slab
	err = storage.Store(m.header.slabID, m)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
	}

	return k, v, nil
}

func (m *MapDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if m.elements.Count() < 2 {
		// Can't split slab with less than two elements
		return nil, nil, NewSlabSplitErrorf("MapDataSlab (%s) has less than 2 elements", m.header.slabID)
	}

	leftElements, rightElements, err := m.elements.Split()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Split().
		return nil, nil, err
	}

	sID, err := storage.GenerateSlabID(m.SlabID().address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", m.SlabID().address))
	}

	// Create new right slab
	rightSlab := &MapDataSlab{
		header: MapSlabHeader{
			slabID:   sID,
			size:     mapDataSlabPrefixSize + rightElements.Size(),
			firstKey: rightElements.firstKey(),
		},
		next:     m.next,
		elements: rightElements,
		anySize:  m.anySize,
	}

	// Modify left (original) slab
	m.header.size = mapDataSlabPrefixSize + leftElements.Size()
	m.next = rightSlab.header.slabID
	m.elements = leftElements

	return m, rightSlab, nil
}

func (m *MapDataSlab) Merge(slab Slab) error {

	rightSlab := slab.(*MapDataSlab)

	err := m.elements.Merge(rightSlab.elements)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Merge().
		return err
	}

	m.header.size = mapDataSlabPrefixSize + m.elements.Size()
	m.header.firstKey = m.elements.firstKey()

	m.next = rightSlab.next

	return nil
}

func (m *MapDataSlab) LendToRight(slab Slab) error {
	rightSlab := slab.(*MapDataSlab)

	if m.anySize || rightSlab.anySize {
		return NewSlabRebalanceErrorf("any sized data slab doesn't need to rebalance")
	}

	rightElements := rightSlab.elements
	err := m.elements.LendToRight(rightElements)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.LendToRight().
		return err
	}

	// Update right slab
	rightSlab.elements = rightElements
	rightSlab.header.size = mapDataSlabPrefixSize + rightElements.Size()
	rightSlab.header.firstKey = rightElements.firstKey()

	// Update left slab
	m.header.size = mapDataSlabPrefixSize + m.elements.Size()

	return nil
}

func (m *MapDataSlab) BorrowFromRight(slab Slab) error {

	rightSlab := slab.(*MapDataSlab)

	if m.anySize || rightSlab.anySize {
		return NewSlabRebalanceErrorf("any sized data slab doesn't need to rebalance")
	}

	rightElements := rightSlab.elements
	err := m.elements.BorrowFromRight(rightElements)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.BorrowFromRight().
		return err
	}

	// Update right slab
	rightSlab.elements = rightElements
	rightSlab.header.size = mapDataSlabPrefixSize + rightElements.Size()
	rightSlab.header.firstKey = rightElements.firstKey()

	// Update left slab
	m.header.size = mapDataSlabPrefixSize + m.elements.Size()
	m.header.firstKey = m.elements.firstKey()

	return nil
}

func (m *MapDataSlab) IsFull() bool {
	if m.anySize {
		return false
	}
	return m.header.size > uint32(maxThreshold)
}

// IsUnderflow returns the number of bytes needed for the data slab
// to reach the min threshold.
// Returns true if the min threshold has not been reached yet.
func (m *MapDataSlab) IsUnderflow() (uint32, bool) {
	if m.anySize {
		return 0, false
	}
	if uint32(minThreshold) > m.header.size {
		return uint32(minThreshold) - m.header.size, true
	}
	return 0, false
}

// CanLendToLeft returns true if elements on the left of the slab could be removed
// so that the slab still stores more than the min threshold.
func (m *MapDataSlab) CanLendToLeft(size uint32) bool {
	if m.anySize {
		return false
	}
	return m.elements.CanLendToLeft(size)
}

// CanLendToRight returns true if elements on the right of the slab could be removed
// so that the slab still stores more than the min threshold.
func (m *MapDataSlab) CanLendToRight(size uint32) bool {
	if m.anySize {
		return false
	}
	return m.elements.CanLendToRight(size)
}

func (m *MapDataSlab) SetSlabID(id SlabID) {
	m.header.slabID = id
}

func (m *MapDataSlab) Header() MapSlabHeader {
	return m.header
}

func (m *MapDataSlab) IsData() bool {
	return true
}

func (m *MapDataSlab) SlabID() SlabID {
	return m.header.slabID
}

func (m *MapDataSlab) ByteSize() uint32 {
	return m.header.size
}

func (m *MapDataSlab) ExtraData() *MapExtraData {
	return m.extraData
}

func (m *MapDataSlab) RemoveExtraData() *MapExtraData {
	extraData := m.extraData
	m.extraData = nil
	return extraData
}

func (m *MapDataSlab) SetExtraData(extraData *MapExtraData) {
	m.extraData = extraData
}

func (m *MapDataSlab) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {
	err := m.elements.PopIterate(storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.PopIterate().
		return err
	}

	// Reset data slab
	m.header.size = m.getPrefixSize() + hkeyElementsPrefixSize
	m.header.firstKey = 0
	return nil
}

func (m *MapDataSlab) String() string {
	return fmt.Sprintf("MapDataSlab id:%s size:%d firstkey:%d elements: [%s]",
		m.header.slabID,
		m.header.size,
		m.header.firstKey,
		m.elements.String(),
	)
}

func newMapMetaDataSlabFromData(
	id SlabID,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (
	*MapMetaDataSlab,
	error,
) {
	// Check minimum data length
	if len(data) < versionAndFlagSize {
		return nil, NewDecodingErrorf("data is too short for map metadata slab")
	}

	h, err := newHeadFromData(data[:versionAndFlagSize])
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if h.getSlabMapType() != slabMapMeta {
		return nil, NewDecodingErrorf(
			"data has invalid head 0x%x, want map metadata slab flag",
			h[:],
		)
	}

	data = data[versionAndFlagSize:]

	switch h.version() {
	case 0:
		return newMapMetaDataSlabFromDataV0(id, h, data, decMode, decodeTypeInfo)

	case 1:
		return newMapMetaDataSlabFromDataV1(id, h, data, decMode, decodeTypeInfo)

	default:
		return nil, NewDecodingErrorf("unexpected version %d for map metadata slab", h.version())
	}
}

// newMapMetaDataSlabFromDataV0 decodes data in version 0:
//
// Root MetaDataSlab Header:
//
//	+-------------------------------+------------+-------------------------------+------------------------------+
//	| slab version + flag (2 bytes) | extra data | slab version + flag (2 bytes) | child header count (2 bytes) |
//	+-------------------------------+------------+-------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (4 bytes):
//
//	+-------------------------------+------------------------------+
//	| slab version + flag (2 bytes) | child header count (2 bytes) |
//	+-------------------------------+------------------------------+
//
// Content (n * 28 bytes):
//
//	[ +[slab ID (16 bytes), first key (8 bytes), size (4 bytes)]]
//
// See MapExtraData.Encode() for extra data section format.
func newMapMetaDataSlabFromDataV0(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (*MapMetaDataSlab, error) {
	const (
		mapMetaDataArrayHeadSizeV0 = 2
		mapSlabHeaderSizeV0        = slabIDSize + 4 + digestSize
	)

	var err error
	var extraData *MapExtraData

	// Check flag for extra data
	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}

		// Skip second head (version + flag) here because it is only present in root slab in version 0.
		if len(data) < versionAndFlagSize {
			return nil, NewDecodingErrorf("data is too short for array data slab")
		}

		data = data[versionAndFlagSize:]
	}

	// Check data length (after decoding extra data if present)
	if len(data) < mapMetaDataArrayHeadSizeV0 {
		return nil, NewDecodingErrorf("data is too short for map metadata slab")
	}

	// Decode number of child headers
	childHeaderCount := binary.BigEndian.Uint16(data)
	data = data[mapMetaDataArrayHeadSizeV0:]

	expectedDataLength := mapSlabHeaderSizeV0 * int(childHeaderCount)
	if len(data) != expectedDataLength {
		return nil, NewDecodingErrorf(
			"data has unexpected length %d, want %d",
			len(data),
			expectedDataLength,
		)
	}

	// Decode child headers
	childrenHeaders := make([]MapSlabHeader, childHeaderCount)
	offset := 0

	for i := 0; i < int(childHeaderCount); i++ {
		slabID, err := NewSlabIDFromRawBytes(data[offset:])
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		firstKeyOffset := offset + slabIDSize
		firstKey := binary.BigEndian.Uint64(data[firstKeyOffset:])

		sizeOffset := firstKeyOffset + digestSize
		size := binary.BigEndian.Uint32(data[sizeOffset:])

		childrenHeaders[i] = MapSlabHeader{
			slabID:   slabID,
			size:     size,
			firstKey: Digest(firstKey),
		}

		offset += mapSlabHeaderSizeV0
	}

	var firstKey Digest
	if len(childrenHeaders) > 0 {
		firstKey = childrenHeaders[0].firstKey
	}

	// Compute slab size in version 1.
	slabSize := mapMetaDataSlabPrefixSize + mapSlabHeaderSize*uint32(childHeaderCount)

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: firstKey,
	}

	return &MapMetaDataSlab{
		header:          header,
		childrenHeaders: childrenHeaders,
		extraData:       extraData,
	}, nil
}

// newMapMetaDataSlabFromDataV1 decodes data in version 1:
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+------------+--------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (4 bytes):
//
//	+------------------------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+--------------------------------+------------------------------+
//
// Content (n * 18 bytes):
//
//	[ +[slab index (8 bytes), first key (8 bytes), size (2 bytes)]]
//
// See MapExtraData.Encode() for extra data section format.
func newMapMetaDataSlabFromDataV1(
	id SlabID,
	h head,
	data []byte,
	decMode cbor.DecMode,
	decodeTypeInfo TypeInfoDecoder,
) (*MapMetaDataSlab, error) {

	var err error
	var extraData *MapExtraData

	if h.isRoot() {
		// Decode extra data
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}
	}

	// Check minimum data length after version, flag, and extra data are processed
	minLength := mapMetaDataSlabPrefixSize - versionAndFlagSize
	if len(data) < minLength {
		return nil, NewDecodingErrorf("data is too short for map metadata slab")
	}

	offset := 0

	// Decode shared address of headers
	var address Address
	copy(address[:], data[offset:])
	offset += slabAddressSize

	// Decode number of child headers
	const arrayHeaderSize = 2
	childHeaderCount := binary.BigEndian.Uint16(data[offset:])
	offset += arrayHeaderSize

	expectedDataLength := mapSlabHeaderSize * int(childHeaderCount)
	if len(data[offset:]) != expectedDataLength {
		return nil, NewDecodingErrorf(
			"data has unexpected length %d, want %d",
			len(data),
			expectedDataLength,
		)
	}

	// Decode child headers
	childrenHeaders := make([]MapSlabHeader, childHeaderCount)

	for i := 0; i < int(childHeaderCount); i++ {
		// Decode slab index
		var index SlabIndex
		copy(index[:], data[offset:])
		offset += slabIndexSize

		// Decode first key
		firstKey := binary.BigEndian.Uint64(data[offset:])
		offset += digestSize

		// Decode size
		size := binary.BigEndian.Uint16(data[offset:])
		offset += 2

		childrenHeaders[i] = MapSlabHeader{
			slabID:   SlabID{address, index},
			size:     uint32(size),
			firstKey: Digest(firstKey),
		}
	}

	var firstKey Digest
	if len(childrenHeaders) > 0 {
		firstKey = childrenHeaders[0].firstKey
	}

	slabSize := mapMetaDataSlabPrefixSize + mapSlabHeaderSize*uint32(childHeaderCount)

	header := MapSlabHeader{
		slabID:   id,
		size:     slabSize,
		firstKey: firstKey,
	}

	return &MapMetaDataSlab{
		header:          header,
		childrenHeaders: childrenHeaders,
		extraData:       extraData,
	}, nil
}

// Encode encodes map meta-data slab to the given encoder.
//
// Root MetaDataSlab Header:
//
//	+------------------------------+------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | extra data | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+------------+--------------------------------+------------------------------+
//
// Non-root MetaDataSlab Header (12 bytes):
//
//	+------------------------------+--------------------------------+------------------------------+
//	| slab version + flag (2 byte) | child shared address (8 bytes) | child header count (2 bytes) |
//	+------------------------------+--------------------------------+------------------------------+
//
// Content (n * 18 bytes):
//
//	[ +[slab index (8 bytes), first key (8 bytes), size (2 bytes)]]
//
// See MapExtraData.Encode() for extra data section format.
func (m *MapMetaDataSlab) Encode(enc *Encoder) error {

	const version = 1

	h, err := newMapSlabHead(version, slabMapMeta)
	if err != nil {
		return NewEncodingError(err)
	}

	if m.extraData != nil {
		h.setRoot()
	}

	// Write head (version and flag)
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data if present
	if m.extraData != nil {
		err = m.extraData.Encode(enc)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapExtraData.Encode().
			return err
		}
	}

	// Encode shared address to scratch
	copy(enc.Scratch[:], m.header.slabID.address[:])

	// Encode child header count to scratch
	const childHeaderCountOffset = slabAddressSize
	binary.BigEndian.PutUint16(
		enc.Scratch[childHeaderCountOffset:],
		uint16(len(m.childrenHeaders)),
	)

	// Write scratch content to encoder
	const totalSize = childHeaderCountOffset + 2
	_, err = enc.Write(enc.Scratch[:totalSize])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode children headers
	for _, h := range m.childrenHeaders {
		// Encode slab index to scratch
		copy(enc.Scratch[:], h.slabID.index[:])

		const firstKeyOffset = slabIndexSize
		binary.BigEndian.PutUint64(enc.Scratch[firstKeyOffset:], uint64(h.firstKey))

		const sizeOffset = firstKeyOffset + digestSize
		binary.BigEndian.PutUint16(enc.Scratch[sizeOffset:], uint16(h.size))

		const totalSize = sizeOffset + 2
		_, err = enc.Write(enc.Scratch[:totalSize])
		if err != nil {
			return NewEncodingError(err)
		}
	}

	return nil
}

func (m *MapMetaDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	if m.extraData == nil {
		return nil, NewNotValueError(m.SlabID())
	}

	digestBuilder := NewDefaultDigesterBuilder()

	digestBuilder.SetSeed(m.extraData.Seed, typicalRandomConstant)

	return &OrderedMap{
		Storage:         storage,
		root:            m,
		digesterBuilder: digestBuilder,
	}, nil
}

func (m *MapMetaDataSlab) ChildStorables() []Storable {
	childIDs := make([]Storable, len(m.childrenHeaders))

	for i, h := range m.childrenHeaders {
		childIDs[i] = SlabIDStorable(h.slabID)
	}

	return childIDs
}

func (m *MapMetaDataSlab) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapValue, error) {

	ans := -1
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	if ans == -1 {
		return nil, NewKeyNotFoundError(key)
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].slabID

	child, err := getMapSlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}

	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return child.Get(storage, digester, level, hkey, comparator, key)
}

func (m *MapMetaDataSlab) Set(storage SlabStorage, b DigesterBuilder, digester Digester, level uint, hkey Digest, comparator ValueComparator, hip HashInputProvider, key Value, value Value) (MapValue, error) {

	ans := 0
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].slabID

	child, err := getMapSlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}

	existingValue, err := child.Set(storage, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, err
	}

	m.childrenHeaders[childHeaderIndex] = child.Header()

	if childHeaderIndex == 0 {
		// Update firstKey.  May not be necessary.
		m.header.firstKey = m.childrenHeaders[childHeaderIndex].firstKey
	}

	if child.IsFull() {
		err := m.SplitChildSlab(storage, child, childHeaderIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.SplitChildSlab().
			return nil, err
		}
		return existingValue, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err := m.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, err
		}
		return existingValue, nil
	}

	err = storage.Store(m.header.slabID, m)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
	}
	return existingValue, nil
}

func (m *MapMetaDataSlab) Remove(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	ans := -1
	i, j := 0, len(m.childrenHeaders)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		if m.childrenHeaders[h].firstKey > hkey {
			j = h
		} else {
			ans = h
			i = h + 1
		}
	}

	if ans == -1 {
		return nil, nil, NewKeyNotFoundError(key)
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].slabID

	child, err := getMapSlab(storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, err
	}

	k, v, err := child.Remove(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Remove().
		return nil, nil, err
	}

	m.childrenHeaders[childHeaderIndex] = child.Header()

	if childHeaderIndex == 0 {
		// Update firstKey.  May not be necessary.
		m.header.firstKey = m.childrenHeaders[childHeaderIndex].firstKey
	}

	if child.IsFull() {
		err := m.SplitChildSlab(storage, child, childHeaderIndex)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.SplitChildSlab().
			return nil, nil, err
		}
		return k, v, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err := m.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, nil, err
		}
		return k, v, nil
	}

	err = storage.Store(m.header.slabID, m)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
	}
	return k, v, nil
}

func (m *MapMetaDataSlab) SplitChildSlab(storage SlabStorage, child MapSlab, childHeaderIndex int) error {
	leftSlab, rightSlab, err := child.Split(storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Split().
		return err
	}

	left := leftSlab.(MapSlab)
	right := rightSlab.(MapSlab)

	// Add new child slab (right) to childrenHeaders
	m.childrenHeaders = append(m.childrenHeaders, MapSlabHeader{})
	if childHeaderIndex < len(m.childrenHeaders)-2 {
		copy(m.childrenHeaders[childHeaderIndex+2:], m.childrenHeaders[childHeaderIndex+1:])
	}
	m.childrenHeaders[childHeaderIndex] = left.Header()
	m.childrenHeaders[childHeaderIndex+1] = right.Header()

	// Increase header size
	m.header.size += mapSlabHeaderSize

	// Store modified slabs
	err = storage.Store(left.SlabID(), left)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", left.SlabID()))
	}

	err = storage.Store(right.SlabID(), right)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", right.SlabID()))
	}

	err = storage.Store(m.header.slabID, m)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
	}

	return nil
}

// MergeOrRebalanceChildSlab merges or rebalances child slab.
// parent slab's data is adjusted.
// If merged, then parent slab's data is adjusted.
//
// +-----------------------+-----------------------+----------------------+-----------------------+
// |			   | no left sibling (sib) | left sib can't lend  | left sib can lend     |
// +=======================+=======================+======================+=======================+
// | no right sib          | panic                 | merge with left      | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can't lend  | merge with right      | merge with smaller   | rebalance with left   |
// +-----------------------+-----------------------+----------------------+-----------------------+
// | right sib can lend    | rebalance with right  | rebalance with right | rebalance with bigger |
// +-----------------------+-----------------------+----------------------+-----------------------+
func (m *MapMetaDataSlab) MergeOrRebalanceChildSlab(
	storage SlabStorage,
	child MapSlab,
	childHeaderIndex int,
	underflowSize uint32,
) error {

	// Retrieve left sibling of the same parent.
	var leftSib MapSlab
	if childHeaderIndex > 0 {
		leftSibID := m.childrenHeaders[childHeaderIndex-1].slabID

		var err error
		leftSib, err = getMapSlab(storage, leftSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return err
		}
	}

	// Retrieve right siblings of the same parent.
	var rightSib MapSlab
	if childHeaderIndex < len(m.childrenHeaders)-1 {
		rightSibID := m.childrenHeaders[childHeaderIndex+1].slabID

		var err error
		rightSib, err = getMapSlab(storage, rightSibID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return err
		}
	}

	leftCanLend := leftSib != nil && leftSib.CanLendToRight(underflowSize)
	rightCanLend := rightSib != nil && rightSib.CanLendToLeft(underflowSize)

	// Child can rebalance elements with at least one sibling.
	if leftCanLend || rightCanLend {

		// Rebalance with right sib
		if !leftCanLend {

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by MapSlab.BorrowFromRight().
				return err
			}

			m.childrenHeaders[childHeaderIndex] = child.Header()
			m.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// This is needed when child is at index 0 and it is empty.
			if childHeaderIndex == 0 {
				m.header.firstKey = child.Header().firstKey
			}

			// Store modified slabs
			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}

			err = storage.Store(rightSib.SlabID(), rightSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", rightSib.SlabID()))
			}

			err = storage.Store(m.header.slabID, m)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
			}
			return nil
		}

		// Rebalance with left sib
		if !rightCanLend {

			err := leftSib.LendToRight(child)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by MapSlab.LendToRight().
				return err
			}

			m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			m.childrenHeaders[childHeaderIndex] = child.Header()

			// Store modified slabs
			err = storage.Store(leftSib.SlabID(), leftSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
			}

			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}

			err = storage.Store(m.header.slabID, m)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
			}
			return nil
		}

		// Rebalance with bigger sib
		if leftSib.ByteSize() > rightSib.ByteSize() {

			err := leftSib.LendToRight(child)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by MapSlab.LendToRight().
				return err
			}

			m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()
			m.childrenHeaders[childHeaderIndex] = child.Header()

			// Store modified slabs
			err = storage.Store(leftSib.SlabID(), leftSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
			}

			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}

			err = storage.Store(m.header.slabID, m)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
			}
			return nil
		} else {
			// leftSib.ByteSize() <= rightSib.ByteSize

			err := child.BorrowFromRight(rightSib)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by MapSlab.BorrowFromRight().
				return err
			}

			m.childrenHeaders[childHeaderIndex] = child.Header()
			m.childrenHeaders[childHeaderIndex+1] = rightSib.Header()

			// This is needed when child is at index 0 and it is empty.
			if childHeaderIndex == 0 {
				m.header.firstKey = child.Header().firstKey
			}

			// Store modified slabs
			err = storage.Store(child.SlabID(), child)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
			}

			err = storage.Store(rightSib.SlabID(), rightSib)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", rightSib.SlabID()))
			}

			err = storage.Store(m.header.slabID, m)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
			}
			return nil
		}
	}

	// Child can't rebalance with any sibling.  It must merge with one sibling.

	if leftSib == nil {

		// Merge with right
		err := child.Merge(rightSib)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapSlab.Merge().
			return err
		}

		m.childrenHeaders[childHeaderIndex] = child.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex+1:], m.childrenHeaders[childHeaderIndex+2:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// This is needed when child is at index 0 and it is empty.
		if childHeaderIndex == 0 {
			m.header.firstKey = child.Header().firstKey
		}

		// Store modified slabs in storage
		err = storage.Store(child.SlabID(), child)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
		}
		err = storage.Store(m.header.slabID, m)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
		}

		// Remove right sib from storage
		err = storage.Remove(rightSib.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", rightSib.SlabID()))
		}
		return nil
	}

	if rightSib == nil {

		// Merge with left
		err := leftSib.Merge(child)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapSlab.Merge().
			return err
		}

		m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex:], m.childrenHeaders[childHeaderIndex+1:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(leftSib.SlabID(), leftSib)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
		}
		err = storage.Store(m.header.slabID, m)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
		}

		// Remove child from storage
		err = storage.Remove(child.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", child.SlabID()))
		}
		return nil
	}

	// Merge with smaller sib
	if leftSib.ByteSize() < rightSib.ByteSize() {
		err := leftSib.Merge(child)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapSlab.Merge().
			return err
		}

		m.childrenHeaders[childHeaderIndex-1] = leftSib.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex:], m.childrenHeaders[childHeaderIndex+1:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// Store modified slabs in storage
		err = storage.Store(leftSib.SlabID(), leftSib)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", leftSib.SlabID()))
		}
		err = storage.Store(m.header.slabID, m)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
		}

		// Remove child from storage
		err = storage.Remove(child.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", child.SlabID()))
		}
		return nil
	} else {
		// leftSib.ByteSize() > rightSib.ByteSize

		err := child.Merge(rightSib)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapSlab.Merge().
			return err
		}

		m.childrenHeaders[childHeaderIndex] = child.Header()

		// Update MetaDataSlab's childrenHeaders
		copy(m.childrenHeaders[childHeaderIndex+1:], m.childrenHeaders[childHeaderIndex+2:])
		m.childrenHeaders = m.childrenHeaders[:len(m.childrenHeaders)-1]

		m.header.size -= mapSlabHeaderSize

		// This is needed when child is at index 0 and it is empty.
		if childHeaderIndex == 0 {
			m.header.firstKey = child.Header().firstKey
		}

		// Store modified slabs in storage
		err = storage.Store(child.SlabID(), child)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", child.SlabID()))
		}
		err = storage.Store(m.header.slabID, m)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.header.slabID))
		}

		// Remove rightSib from storage
		err = storage.Remove(rightSib.SlabID())
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
			return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", rightSib.SlabID()))
		}
		return nil
	}
}

func (m *MapMetaDataSlab) Merge(slab Slab) error {
	rightSlab := slab.(*MapMetaDataSlab)

	m.childrenHeaders = append(m.childrenHeaders, rightSlab.childrenHeaders...)
	m.header.size += rightSlab.header.size - mapMetaDataSlabPrefixSize

	return nil
}

func (m *MapMetaDataSlab) Split(storage SlabStorage) (Slab, Slab, error) {
	if len(m.childrenHeaders) < 2 {
		// Can't split meta slab with less than 2 headers
		return nil, nil, NewSlabSplitErrorf("MapMetaDataSlab (%s) has less than 2 child headers", m.header.slabID)
	}

	leftChildrenCount := int(math.Ceil(float64(len(m.childrenHeaders)) / 2))
	leftSize := leftChildrenCount * mapSlabHeaderSize

	sID, err := storage.GenerateSlabID(m.SlabID().address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", m.SlabID().address))
	}

	// Construct right slab
	rightSlab := &MapMetaDataSlab{
		header: MapSlabHeader{
			slabID:   sID,
			size:     m.header.size - uint32(leftSize),
			firstKey: m.childrenHeaders[leftChildrenCount].firstKey,
		},
	}

	rightSlab.childrenHeaders = make([]MapSlabHeader, len(m.childrenHeaders)-leftChildrenCount)
	copy(rightSlab.childrenHeaders, m.childrenHeaders[leftChildrenCount:])

	// Modify left (original) slab
	m.childrenHeaders = m.childrenHeaders[:leftChildrenCount]
	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftSize)

	return m, rightSlab, nil
}

func (m *MapMetaDataSlab) LendToRight(slab Slab) error {
	rightSlab := slab.(*MapMetaDataSlab)

	childrenHeadersLen := len(m.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftChildrenHeadersLen := childrenHeadersLen / 2
	rightChildrenHeadersLen := childrenHeadersLen - leftChildrenHeadersLen

	// Update right slab childrenHeaders by prepending borrowed children headers
	rightChildrenHeaders := make([]MapSlabHeader, rightChildrenHeadersLen)
	n := copy(rightChildrenHeaders, m.childrenHeaders[leftChildrenHeadersLen:])
	copy(rightChildrenHeaders[n:], rightSlab.childrenHeaders)
	rightSlab.childrenHeaders = rightChildrenHeaders

	// Update right slab header
	rightSlab.header.size = mapMetaDataSlabPrefixSize + uint32(rightChildrenHeadersLen)*mapSlabHeaderSize
	rightSlab.header.firstKey = rightSlab.childrenHeaders[0].firstKey

	// Update left slab (original)
	m.childrenHeaders = m.childrenHeaders[:leftChildrenHeadersLen]

	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftChildrenHeadersLen)*mapSlabHeaderSize

	return nil
}

func (m *MapMetaDataSlab) BorrowFromRight(slab Slab) error {

	rightSlab := slab.(*MapMetaDataSlab)

	childrenHeadersLen := len(m.childrenHeaders) + len(rightSlab.childrenHeaders)
	leftSlabHeaderLen := childrenHeadersLen / 2
	rightSlabHeaderLen := childrenHeadersLen - leftSlabHeaderLen

	// Update left slab (original)
	m.childrenHeaders = append(m.childrenHeaders, rightSlab.childrenHeaders[:leftSlabHeaderLen-len(m.childrenHeaders)]...)

	m.header.size = mapMetaDataSlabPrefixSize + uint32(leftSlabHeaderLen)*mapSlabHeaderSize

	// Update right slab
	rightSlab.childrenHeaders = rightSlab.childrenHeaders[len(rightSlab.childrenHeaders)-rightSlabHeaderLen:]

	rightSlab.header.size = mapMetaDataSlabPrefixSize + uint32(rightSlabHeaderLen)*mapSlabHeaderSize
	rightSlab.header.firstKey = rightSlab.childrenHeaders[0].firstKey

	return nil
}

func (m MapMetaDataSlab) IsFull() bool {
	return m.header.size > uint32(maxThreshold)
}

func (m MapMetaDataSlab) IsUnderflow() (uint32, bool) {
	if uint32(minThreshold) > m.header.size {
		return uint32(minThreshold) - m.header.size, true
	}
	return 0, false
}

func (m *MapMetaDataSlab) CanLendToLeft(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / mapSlabHeaderSize))
	return m.header.size-mapSlabHeaderSize*n > uint32(minThreshold)
}

func (m *MapMetaDataSlab) CanLendToRight(size uint32) bool {
	n := uint32(math.Ceil(float64(size) / mapSlabHeaderSize))
	return m.header.size-mapSlabHeaderSize*n > uint32(minThreshold)
}

func (m MapMetaDataSlab) IsData() bool {
	return false
}

func (m *MapMetaDataSlab) SetSlabID(id SlabID) {
	m.header.slabID = id
}

func (m *MapMetaDataSlab) Header() MapSlabHeader {
	return m.header
}

func (m *MapMetaDataSlab) ByteSize() uint32 {
	return m.header.size
}

func (m *MapMetaDataSlab) SlabID() SlabID {
	return m.header.slabID
}

func (m *MapMetaDataSlab) ExtraData() *MapExtraData {
	return m.extraData
}

func (m *MapMetaDataSlab) RemoveExtraData() *MapExtraData {
	extraData := m.extraData
	m.extraData = nil
	return extraData
}

func (m *MapMetaDataSlab) SetExtraData(extraData *MapExtraData) {
	m.extraData = extraData
}

func (m *MapMetaDataSlab) PopIterate(storage SlabStorage, fn MapPopIterationFunc) error {

	// Iterate child slabs backwards
	for i := len(m.childrenHeaders) - 1; i >= 0; i-- {

		childID := m.childrenHeaders[i].slabID

		child, err := getMapSlab(storage, childID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return err
		}

		err = child.PopIterate(storage, fn)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapSlab.PopIterate().
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
	m.childrenHeaders = nil
	m.header.firstKey = 0
	m.header.size = mapMetaDataSlabPrefixSize

	return nil
}

func (m *MapMetaDataSlab) String() string {
	elemsStr := make([]string, len(m.childrenHeaders))
	for i, h := range m.childrenHeaders {
		elemsStr[i] = fmt.Sprintf("{id:%s size:%d firstKey:%d}", h.slabID, h.size, h.firstKey)
	}

	return fmt.Sprintf("MapMetaDataSlab id:%s size:%d firstKey:%d children: [%s]",
		m.header.slabID,
		m.header.size,
		m.header.firstKey,
		strings.Join(elemsStr, " "),
	)
}

func NewMap(storage SlabStorage, address Address, digestBuilder DigesterBuilder, typeInfo TypeInfo) (*OrderedMap, error) {

	// Create root slab ID
	sID, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	// Create seed for non-crypto hash algos (CircleHash64, SipHash) to use.
	// Ideally, seed should be a nondeterministic 128-bit secret because
	// these hashes rely on its key being secret for its security.  Since
	// we handle collisions and based on other factors such as storage space,
	// the team decided we can use a 64-bit non-secret key instead of
	// a 128-bit secret key. And for performance reasons, we first use
	// noncrypto hash algos and fall back to crypto algo after collisions.
	// This is for creating the seed, so the seed used here is OK to be 0.
	// LittleEndian is needed for compatibility (same digest from []byte and
	// two uint64).
	a := binary.LittleEndian.Uint64(sID.address[:])
	b := binary.LittleEndian.Uint64(sID.index[:])
	k0 := circlehash.Hash64Uint64x2(a, b, uint64(0))

	// To save storage space, only store 64-bits of the seed.
	// Use a 64-bit const for the unstored half to create 128-bit seed.
	k1 := typicalRandomConstant

	digestBuilder.SetSeed(k0, k1)

	// Create extra data with type info and seed
	extraData := &MapExtraData{TypeInfo: typeInfo, Seed: k0}

	root := &MapDataSlab{
		header: MapSlabHeader{
			slabID: sID,
			size:   mapRootDataSlabPrefixSize + hkeyElementsPrefixSize,
		},
		elements:  newHkeyElements(0),
		extraData: extraData,
	}

	err = storage.Store(root.header.slabID, root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", root.header.slabID))
	}

	return &OrderedMap{
		Storage:         storage,
		root:            root,
		digesterBuilder: digestBuilder,
	}, nil
}

func NewMapWithRootID(storage SlabStorage, rootID SlabID, digestBuilder DigesterBuilder) (*OrderedMap, error) {
	if rootID == SlabIDUndefined {
		return nil, NewSlabIDErrorf("cannot create OrderedMap from undefined slab ID")
	}

	root, err := getMapSlab(storage, rootID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}

	extraData := root.ExtraData()
	if extraData == nil {
		return nil, NewNotValueError(rootID)
	}

	digestBuilder.SetSeed(extraData.Seed, typicalRandomConstant)

	return &OrderedMap{
		Storage:         storage,
		root:            root,
		digesterBuilder: digestBuilder,
	}, nil
}

func (m *OrderedMap) Has(comparator ValueComparator, hip HashInputProvider, key Value) (bool, error) {
	_, err := m.get(comparator, hip, key)
	if err != nil {
		var knf *KeyNotFoundError
		if errors.As(err, &knf) {
			return false, nil
		}
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Get().
		return false, err
	}
	return true, nil
}

func (m *OrderedMap) Get(comparator ValueComparator, hip HashInputProvider, key Value) (Value, error) {

	storable, err := m.get(comparator, hip, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
		return nil, err
	}

	v, err := storable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}
	return v, nil
}

func (m *OrderedMap) get(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return m.root.Get(m.Storage, keyDigest, level, hkey, comparator, key)
}

func (m *OrderedMap) Set(comparator ValueComparator, hip HashInputProvider, key Value, value Value) (Storable, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	existingValue, err := m.root.Set(m.Storage, m.digesterBuilder, keyDigest, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, err
	}

	if existingValue == nil {
		m.root.ExtraData().incrementCount()
	}

	if !m.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := m.root.(*MapMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err := m.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by OrderedMap.promoteChildAsNewRoot().
				return nil, err
			}
			return existingValue, nil
		}
	}

	if m.root.IsFull() {
		err := m.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.splitRoot().
			return nil, err
		}
	}

	return existingValue, nil
}

func (m *OrderedMap) Remove(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, Storable, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to create map key digest at level %d", level))
	}

	k, v, err := m.root.Remove(m.Storage, keyDigest, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Remove().
		return nil, nil, err
	}

	m.root.ExtraData().decrementCount()

	if !m.root.IsData() {
		// Set root to its child slab if root has one child slab.
		root := m.root.(*MapMetaDataSlab)
		if len(root.childrenHeaders) == 1 {
			err := m.promoteChildAsNewRoot(root.childrenHeaders[0].slabID)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by OrderedMap.promoteChildAsNewRoot().
				return nil, nil, err
			}
			return k, v, nil
		}
	}

	if m.root.IsFull() {
		err := m.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.splitRoot().
			return nil, nil, err
		}
	}

	return k, v, nil
}

func (m *OrderedMap) splitRoot() error {

	if m.root.IsData() {
		// Adjust root data slab size before splitting
		dataSlab := m.root.(*MapDataSlab)
		dataSlab.header.size = dataSlab.header.size - mapRootDataSlabPrefixSize + mapDataSlabPrefixSize
	}

	// Get old root's extra data and reset it to nil in old root
	extraData := m.root.RemoveExtraData()

	// Save root node id
	rootID := m.root.SlabID()

	// Assign a new slab ID to old root before splitting it.
	sID, err := m.Storage.GenerateSlabID(m.Address())
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", m.Address()))
	}

	oldRoot := m.root
	oldRoot.SetSlabID(sID)

	// Split old root
	leftSlab, rightSlab, err := oldRoot.Split(m.Storage)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Split().
		return err
	}

	left := leftSlab.(MapSlab)
	right := rightSlab.(MapSlab)

	// Create new MapMetaDataSlab with the old root's slab ID
	newRoot := &MapMetaDataSlab{
		header: MapSlabHeader{
			slabID:   rootID,
			size:     mapMetaDataSlabPrefixSize + mapSlabHeaderSize*2,
			firstKey: left.Header().firstKey,
		},
		childrenHeaders: []MapSlabHeader{left.Header(), right.Header()},
		extraData:       extraData,
	}

	m.root = newRoot

	err = m.Storage.Store(left.SlabID(), left)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", left.SlabID()))
	}
	err = m.Storage.Store(right.SlabID(), right)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", right.SlabID()))
	}
	err = m.Storage.Store(m.root.SlabID(), m.root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.root.SlabID()))
	}
	return nil
}

func (m *OrderedMap) promoteChildAsNewRoot(childID SlabID) error {

	child, err := getMapSlab(m.Storage, childID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return err
	}

	if child.IsData() {
		// Adjust data slab size before promoting non-root data slab to root
		dataSlab := child.(*MapDataSlab)
		dataSlab.header.size = dataSlab.header.size - mapDataSlabPrefixSize + mapRootDataSlabPrefixSize
	}

	extraData := m.root.RemoveExtraData()

	rootID := m.root.SlabID()

	m.root = child

	m.root.SetSlabID(rootID)

	m.root.SetExtraData(extraData)

	err = m.Storage.Store(rootID, m.root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", rootID))
	}

	err = m.Storage.Remove(childID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
	}
	return nil
}

func (m *OrderedMap) SlabID() SlabID {
	return m.root.SlabID()
}

func (m *OrderedMap) ValueID() ValueID {
	sid := m.SlabID()

	var id ValueID
	copy(id[:], sid.address[:])
	copy(id[8:], sid.index[:])

	return id
}

func (m *OrderedMap) StoredValue(_ SlabStorage) (Value, error) {
	return m, nil
}

func (m *OrderedMap) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
	return SlabIDStorable(m.SlabID()), nil
}

func (m *OrderedMap) Count() uint64 {
	return m.root.ExtraData().Count
}

func (m *OrderedMap) Address() Address {
	return m.root.SlabID().address
}

func (m *OrderedMap) Type() TypeInfo {
	if extraData := m.root.ExtraData(); extraData != nil {
		return extraData.TypeInfo
	}
	return nil
}

func (m *OrderedMap) String() string {
	iterator, err := m.Iterator()
	if err != nil {
		return err.Error()
	}

	var elemsStr []string
	for {
		k, v, err := iterator.Next()
		if err != nil {
			return err.Error()
		}
		if k == nil {
			break
		}
		elemsStr = append(elemsStr, fmt.Sprintf("%s:%s", k, v))
	}

	return fmt.Sprintf("[%s]", strings.Join(elemsStr, " "))
}

func getMapSlab(storage SlabStorage, id SlabID) (MapSlab, error) {
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", id))
	}
	if !found {
		return nil, NewSlabNotFoundErrorf(id, "map slab not found")
	}
	mapSlab, ok := slab.(MapSlab)
	if !ok {
		return nil, NewSlabDataErrorf("slab %s isn't MapSlab", id)
	}
	return mapSlab, nil
}

func firstMapDataSlab(storage SlabStorage, slab MapSlab) (MapSlab, error) {
	if slab.IsData() {
		return slab, nil
	}
	meta := slab.(*MapMetaDataSlab)
	firstChildID := meta.childrenHeaders[0].slabID
	firstChild, err := getMapSlab(storage, firstChildID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, err
	}
	// Don't need to wrap error as external error because err is already categorized by firstMapDataSlab().
	return firstMapDataSlab(storage, firstChild)
}

func (m *MapExtraData) incrementCount() {
	m.Count++
}

func (m *MapExtraData) decrementCount() {
	m.Count--
}

type MapElementIterator struct {
	storage        SlabStorage
	elements       elements
	index          int
	nestedIterator *MapElementIterator
}

func (i *MapElementIterator) Next() (key MapKey, value MapValue, err error) {

	if i.nestedIterator != nil {
		key, value, err = i.nestedIterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
			return nil, nil, err
		}
		if key != nil {
			return key, value, nil
		}
		i.nestedIterator = nil
	}

	if i.index >= int(i.elements.Count()) {
		return nil, nil, nil
	}

	e, err := i.elements.Element(i.index)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Element().
		return nil, nil, err
	}

	switch elm := e.(type) {
	case *singleElement:
		i.index++
		return elm.key, elm.value, nil

	case elementGroup:
		elems, err := elm.Elements(i.storage)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
			return nil, nil, err
		}

		i.nestedIterator = &MapElementIterator{
			storage:  i.storage,
			elements: elems,
		}

		i.index++
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return i.nestedIterator.Next()

	default:
		return nil, nil, NewSlabDataError(fmt.Errorf("unexpected element type %T during map iteration", e))
	}
}

type MapEntryIterationFunc func(Value, Value) (resume bool, err error)
type MapElementIterationFunc func(Value) (resume bool, err error)

type MapIterator struct {
	storage      SlabStorage
	id           SlabID
	elemIterator *MapElementIterator
}

func (i *MapIterator) Next() (key Value, value Value, err error) {
	if i.elemIterator == nil {
		if i.id == SlabIDUndefined {
			return nil, nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, nil, err
		}
	}

	var ks, vs Storable
	ks, vs, err = i.elemIterator.Next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, nil, err
	}
	if ks != nil {
		key, err = ks.StoredValue(i.storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map key's stored value")
		}

		value, err = vs.StoredValue(i.storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map value's stored value")
		}

		return key, value, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.Next().
	return i.Next()
}

func (i *MapIterator) NextKey() (key Value, err error) {
	if i.elemIterator == nil {
		if i.id == SlabIDUndefined {
			return nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, err
		}
	}

	var ks Storable
	ks, _, err = i.elemIterator.Next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, err
	}
	if ks != nil {
		key, err = ks.StoredValue(i.storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map key's stored value")
		}

		return key, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.NextKey().
	return i.NextKey()
}

func (i *MapIterator) NextValue() (value Value, err error) {
	if i.elemIterator == nil {
		if i.id == SlabIDUndefined {
			return nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, err
		}
	}

	var vs Storable
	_, vs, err = i.elemIterator.Next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, err
	}
	if vs != nil {
		value, err = vs.StoredValue(i.storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map value's stored value")
		}

		return value, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.NextValue().
	return i.NextValue()
}

func (i *MapIterator) advance() error {
	slab, found, err := i.storage.Retrieve(i.id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", i.id))
	}
	if !found {
		return NewSlabNotFoundErrorf(i.id, "slab not found during map iteration")
	}

	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return NewSlabDataErrorf("slab %s isn't MapDataSlab", i.id)
	}

	i.id = dataSlab.next

	i.elemIterator = &MapElementIterator{
		storage:  i.storage,
		elements: dataSlab.elements,
	}

	return nil
}

func (m *OrderedMap) Iterator() (*MapIterator, error) {
	slab, err := firstMapDataSlab(m.Storage, m.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstMapDataSlab().
		return nil, err
	}

	dataSlab := slab.(*MapDataSlab)

	return &MapIterator{
		storage: m.Storage,
		id:      dataSlab.next,
		elemIterator: &MapElementIterator{
			storage:  m.Storage,
			elements: dataSlab.elements,
		},
	}, nil
}

func (m *OrderedMap) Iterate(fn MapEntryIterationFunc) error {

	iterator, err := m.Iterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}

	var key, value Value
	for {
		key, value, err = iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.Next().
			return err
		}
		if key == nil {
			return nil
		}
		resume, err := fn(key, value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapEntryIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}

func (m *OrderedMap) IterateKeys(fn MapElementIterationFunc) error {

	iterator, err := m.Iterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}

	var key Value
	for {
		key, err = iterator.NextKey()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.NextKey().
			return err
		}
		if key == nil {
			return nil
		}
		resume, err := fn(key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapElementIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}

func (m *OrderedMap) IterateValues(fn MapElementIterationFunc) error {

	iterator, err := m.Iterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}

	var value Value
	for {
		value, err = iterator.NextValue()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.NextValue().
			return err
		}
		if value == nil {
			return nil
		}
		resume, err := fn(value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapElementIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}

type MapPopIterationFunc func(Storable, Storable)

// PopIterate iterates and removes elements backward.
// Each element is passed to MapPopIterationFunc callback before removal.
func (m *OrderedMap) PopIterate(fn MapPopIterationFunc) error {

	err := m.root.PopIterate(m.Storage, fn)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.PopIterate().
		return err
	}

	rootID := m.root.SlabID()

	// Set map count to 0 in extraData
	extraData := m.root.ExtraData()
	extraData.Count = 0

	// Set root to empty data slab
	m.root = &MapDataSlab{
		header: MapSlabHeader{
			slabID: rootID,
			size:   mapRootDataSlabPrefixSize + hkeyElementsPrefixSize,
		},
		elements:  newHkeyElements(0),
		extraData: extraData,
	}

	// Save root slab
	err = m.Storage.Store(m.root.SlabID(), m.root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", m.root.SlabID()))
	}
	return nil
}

func (m *OrderedMap) Seed() uint64 {
	return m.root.ExtraData().Seed
}

type MapElementProvider func() (Value, Value, error)

// NewMapFromBatchData returns a new map with elements provided by fn callback.
// Provided seed must be the same seed used to create the original map.
// And callback function must return elements in the same order as the original map.
// New map uses and stores the same seed as the original map.
// This function should only be used for copying a map.
func NewMapFromBatchData(
	storage SlabStorage,
	address Address,
	digesterBuilder DigesterBuilder,
	typeInfo TypeInfo,
	comparator ValueComparator,
	hip HashInputProvider,
	seed uint64,
	fn MapElementProvider,
) (
	*OrderedMap,
	error,
) {

	const defaultElementCountInSlab = 32

	if seed == 0 {
		return nil, NewHashSeedUninitializedError()
	}

	// Seed digester
	digesterBuilder.SetSeed(seed, typicalRandomConstant)

	var slabs []MapSlab

	id, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	elements := &hkeyElements{
		level: 0,
		size:  hkeyElementsPrefixSize,
		hkeys: make([]Digest, 0, defaultElementCountInSlab),
		elems: make([]element, 0, defaultElementCountInSlab),
	}

	count := uint64(0)

	var prevHkey Digest

	// Appends all elements
	for {
		key, value, err := fn()
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapElementProvider callback.
			return nil, wrapErrorAsExternalErrorIfNeeded(err)
		}
		if key == nil {
			break
		}

		digester, err := digesterBuilder.Digest(hip, key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
		}

		hkey, err := digester.Digest(0)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Digester interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to generate map key digest for level 0")
		}

		if hkey < prevHkey {
			// a valid map will always have sorted digests
			return nil, NewHashError(fmt.Errorf("digest isn't sorted (found %d before %d)", prevHkey, hkey))
		}

		if hkey == prevHkey && count > 0 {
			// found collision

			lastElementIndex := len(elements.elems) - 1

			prevElem := elements.elems[lastElementIndex]
			prevElemSize := prevElem.Size()

			elem, existingValue, err := prevElem.Set(storage, address, digesterBuilder, digester, 0, hkey, comparator, hip, key, value)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by element.Set().
				return nil, err
			}
			if existingValue != nil {
				return nil, NewDuplicateKeyError(key)
			}

			elements.elems[lastElementIndex] = elem
			elements.size += elem.Size() - prevElemSize

			putDigester(digester)

			count++

			continue
		}

		// no collision

		putDigester(digester)

		elem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElememt().
			return nil, err
		}

		// Finalize data slab
		currentSlabSize := mapDataSlabPrefixSize + elements.Size()
		newElementSize := digestSize + elem.Size()
		if currentSlabSize >= uint32(targetThreshold) ||
			currentSlabSize+newElementSize > uint32(maxThreshold) {

			// Generate storge id for next data slab
			nextID, err := storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			// Create data slab
			dataSlab := &MapDataSlab{
				header: MapSlabHeader{
					slabID:   id,
					size:     mapDataSlabPrefixSize + elements.Size(),
					firstKey: elements.firstKey(),
				},
				elements: elements,
				next:     nextID,
			}

			// Append data slab to dataSlabs
			slabs = append(slabs, dataSlab)

			// Save id
			id = nextID

			// Create new elements for next data slab
			elements = &hkeyElements{
				level: 0,
				size:  hkeyElementsPrefixSize,
				hkeys: make([]Digest, 0, defaultElementCountInSlab),
				elems: make([]element, 0, defaultElementCountInSlab),
			}
		}

		elements.hkeys = append(elements.hkeys, hkey)
		elements.elems = append(elements.elems, elem)
		elements.size += digestSize + elem.Size()

		prevHkey = hkey

		count++
	}

	// Create last data slab
	dataSlab := &MapDataSlab{
		header: MapSlabHeader{
			slabID:   id,
			size:     mapDataSlabPrefixSize + elements.Size(),
			firstKey: elements.firstKey(),
		},
		elements: elements,
	}

	// Append last data slab to slabs
	slabs = append(slabs, dataSlab)

	for len(slabs) > 1 {

		lastSlab := slabs[len(slabs)-1]

		// Rebalance last slab if needed
		if underflowSize, underflow := lastSlab.IsUnderflow(); underflow {

			leftSib := slabs[len(slabs)-2]

			if leftSib.CanLendToRight(underflowSize) {

				// Rebalance with left
				err := leftSib.LendToRight(lastSlab)
				if err != nil {
					// Don't need to wrap error as external error because err is already categorized by MapSlab.LendToRight().
					return nil, err
				}

			} else {

				// Merge with left
				err := leftSib.Merge(lastSlab)
				if err != nil {
					// Don't need to wrap error as external error because err is already categorized by MapSlab.Merge().
					return nil, err
				}

				// Remove last slab from slabs
				slabs[len(slabs)-1] = nil
				slabs = slabs[:len(slabs)-1]
			}
		}

		// All slabs are within target size range.

		if len(slabs) == 1 {
			// This happens when there were exactly two slabs and
			// last slab has merged with the first slab.
			break
		}

		// Store all slabs
		for _, slab := range slabs {
			err = storage.Store(slab.SlabID(), slab)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", slab.SlabID()))
			}
		}

		// Get next level meta slabs
		slabs, err = nextLevelMapSlabs(storage, address, slabs)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by nextLevelMapSlabs().
			return nil, err
		}

	}

	// found root slab
	root := slabs[0]

	// root is data slab, adjust its size
	if dataSlab, ok := root.(*MapDataSlab); ok {
		dataSlab.header.size = dataSlab.header.size - mapDataSlabPrefixSize + mapRootDataSlabPrefixSize
	}

	extraData := &MapExtraData{TypeInfo: typeInfo, Count: count, Seed: seed}

	// Set extra data in root
	root.SetExtraData(extraData)

	// Store root
	err = storage.Store(root.SlabID(), root)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to store slab %s", root.SlabID()))
	}

	return &OrderedMap{
		Storage:         storage,
		root:            root,
		digesterBuilder: digesterBuilder,
	}, nil
}

// nextLevelMapSlabs returns next level meta data slabs from slabs.
// slabs must have at least 2 elements.  It is reused and returned as next level slabs.
// Caller is responsible for rebalance last slab and storing returned slabs in storage.
func nextLevelMapSlabs(storage SlabStorage, address Address, slabs []MapSlab) ([]MapSlab, error) {

	maxNumberOfHeadersInMetaSlab := (maxThreshold - mapMetaDataSlabPrefixSize) / mapSlabHeaderSize

	nextLevelSlabsIndex := 0

	// Generate storge id
	id, err := storage.GenerateSlabID(address)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
	}

	childrenCount := maxNumberOfHeadersInMetaSlab
	if uint64(len(slabs)) < maxNumberOfHeadersInMetaSlab {
		childrenCount = uint64(len(slabs))
	}

	metaSlab := &MapMetaDataSlab{
		header: MapSlabHeader{
			slabID:   id,
			size:     mapMetaDataSlabPrefixSize,
			firstKey: slabs[0].Header().firstKey,
		},
		childrenHeaders: make([]MapSlabHeader, 0, childrenCount),
	}

	for i, slab := range slabs {

		if len(metaSlab.childrenHeaders) == int(maxNumberOfHeadersInMetaSlab) {

			slabs[nextLevelSlabsIndex] = metaSlab
			nextLevelSlabsIndex++

			// compute number of children for next meta data slab
			childrenCount = maxNumberOfHeadersInMetaSlab
			if uint64(len(slabs)-i) < maxNumberOfHeadersInMetaSlab {
				childrenCount = uint64(len(slabs) - i)
			}

			// Generate storge id for next meta data slab
			id, err = storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to generate slab ID for address 0x%x", address))
			}

			metaSlab = &MapMetaDataSlab{
				header: MapSlabHeader{
					slabID:   id,
					size:     mapMetaDataSlabPrefixSize,
					firstKey: slab.Header().firstKey,
				},
				childrenHeaders: make([]MapSlabHeader, 0, childrenCount),
			}
		}

		metaSlab.header.size += mapSlabHeaderSize

		metaSlab.childrenHeaders = append(metaSlab.childrenHeaders, slab.Header())
	}

	// Append last meta slab to slabs
	slabs[nextLevelSlabsIndex] = metaSlab
	nextLevelSlabsIndex++

	return slabs[:nextLevelSlabsIndex], nil
}

type mapLoadedElementIterator struct {
	storage                SlabStorage
	elements               elements
	index                  int
	collisionGroupIterator *mapLoadedElementIterator
}

func (i *mapLoadedElementIterator) next() (key Value, value Value, err error) {
	// Iterate loaded elements in data slab (including elements in collision groups).
	for i.index < int(i.elements.Count()) || i.collisionGroupIterator != nil {

		// Iterate elements in collision group.
		if i.collisionGroupIterator != nil {
			key, value, err = i.collisionGroupIterator.next()
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by mapLoadedElementIterator.next().
				return nil, nil, err
			}
			if key != nil {
				return key, value, nil
			}

			// Reach end of collision group.
			i.collisionGroupIterator = nil
			continue
		}

		element, err := i.elements.Element(i.index)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by elements.Element().
			return nil, nil, err
		}

		i.index++

		switch e := element.(type) {
		case *singleElement:

			keyValue, err := getLoadedValue(i.storage, e.key)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getLoadedValue.
				return nil, nil, err
			}
			if keyValue == nil {
				// Skip this element because element key references unloaded slab.
				// Try next element.
				continue
			}

			valueValue, err := getLoadedValue(i.storage, e.value)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by getLoadedValue.
				return nil, nil, err
			}
			if valueValue == nil {
				// Skip this element because element value references unloaded slab.
				// Try next element.
				continue
			}

			return keyValue, valueValue, nil

		case *inlineCollisionGroup:
			elems, err := e.Elements(i.storage)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by elementGroup.Elements().
				return nil, nil, err
			}

			i.collisionGroupIterator = &mapLoadedElementIterator{
				storage:  i.storage,
				elements: elems,
			}

			// Continue to iterate elements in collision group using collisionGroupIterator.
			continue

		case *externalCollisionGroup:
			externalSlab := i.storage.RetrieveIfLoaded(e.slabID)
			if externalSlab == nil {
				// Skip this collsion group because external slab isn't loaded.
				// Try next element.
				continue
			}

			dataSlab, ok := externalSlab.(*MapDataSlab)
			if !ok {
				return nil, nil, NewSlabDataErrorf("slab %s isn't MapDataSlab", e.slabID)
			}

			i.collisionGroupIterator = &mapLoadedElementIterator{
				storage:  i.storage,
				elements: dataSlab.elements,
			}

			// Continue to iterate elements in collision group using collisionGroupIterator.
			continue

		default:
			return nil, nil, NewSlabDataError(fmt.Errorf("unexpected element type %T during map iteration", element))
		}
	}

	// Reach end of map data slab.
	return nil, nil, nil
}

type mapLoadedSlabIterator struct {
	storage SlabStorage
	slab    *MapMetaDataSlab
	index   int
}

func (i *mapLoadedSlabIterator) next() Slab {
	// Iterate loaded slabs in meta data slab.
	for i.index < len(i.slab.childrenHeaders) {
		header := i.slab.childrenHeaders[i.index]
		i.index++

		childSlab := i.storage.RetrieveIfLoaded(header.slabID)
		if childSlab == nil {
			// Skip this child because it references unloaded slab.
			// Try next child.
			continue
		}

		return childSlab
	}

	// Reach end of children.
	return nil
}

// MapLoadedValueIterator is used to iterate loaded map elements.
type MapLoadedValueIterator struct {
	storage      SlabStorage
	parents      []*mapLoadedSlabIterator // LIFO stack for parents of dataIterator
	dataIterator *mapLoadedElementIterator
}

func (i *MapLoadedValueIterator) nextDataIterator() (*mapLoadedElementIterator, error) {

	// Iterate parents (LIFO) to find next loaded map data slab.
	for len(i.parents) > 0 {
		lastParent := i.parents[len(i.parents)-1]

		nextChildSlab := lastParent.next()

		switch slab := nextChildSlab.(type) {
		case *MapDataSlab:
			// Create data iterator
			return &mapLoadedElementIterator{
				storage:  i.storage,
				elements: slab.elements,
			}, nil

		case *MapMetaDataSlab:
			// Push new parent to parents queue
			newParent := &mapLoadedSlabIterator{
				storage: i.storage,
				slab:    slab,
			}
			i.parents = append(i.parents, newParent)

		case nil:
			// Reach end of last parent.
			// Reset last parent to nil and pop last parent from parents stack.
			lastParentIndex := len(i.parents) - 1
			i.parents[lastParentIndex] = nil
			i.parents = i.parents[:lastParentIndex]

		default:
			return nil, NewSlabDataErrorf("slab %s isn't MapSlab", nextChildSlab.SlabID())
		}
	}

	// Reach end of parents stack.
	return nil, nil
}

// Next iterates and returns next loaded element.
// It returns nil Value at end of loaded elements.
func (i *MapLoadedValueIterator) Next() (Value, Value, error) {
	if i.dataIterator != nil {
		key, value, err := i.dataIterator.next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by mapLoadedElementIterator.next().
			return nil, nil, err
		}
		if key != nil {
			return key, value, nil
		}

		// Reach end of element in current data slab.
		i.dataIterator = nil
	}

	// Get next data iterator.
	var err error
	i.dataIterator, err = i.nextDataIterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapLoadedValueIterator.nextDataIterator().
		return nil, nil, err
	}
	if i.dataIterator != nil {
		return i.Next()
	}

	// Reach end of loaded value iterator
	return nil, nil, nil
}

// LoadedValueIterator returns iterator to iterate loaded map elements.
func (m *OrderedMap) LoadedValueIterator() (*MapLoadedValueIterator, error) {
	switch slab := m.root.(type) {

	case *MapDataSlab:
		// Create a data iterator from root slab.
		dataIterator := &mapLoadedElementIterator{
			storage:  m.Storage,
			elements: slab.elements,
		}

		// Create iterator with data iterator (no parents).
		iterator := &MapLoadedValueIterator{
			storage:      m.Storage,
			dataIterator: dataIterator,
		}

		return iterator, nil

	case *MapMetaDataSlab:
		// Create a slab iterator from root slab.
		slabIterator := &mapLoadedSlabIterator{
			storage: m.Storage,
			slab:    slab,
		}

		// Create iterator with parent (data iterater is uninitialized).
		iterator := &MapLoadedValueIterator{
			storage: m.Storage,
			parents: []*mapLoadedSlabIterator{slabIterator},
		}

		return iterator, nil

	default:
		return nil, NewSlabDataErrorf("slab %s isn't MapSlab", slab.SlabID())
	}
}

// IterateLoadedValues iterates loaded map values.
func (m *OrderedMap) IterateLoadedValues(fn MapEntryIterationFunc) error {
	iterator, err := m.LoadedValueIterator()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.LoadedValueIterator().
		return err
	}

	var key, value Value
	for {
		key, value, err = iterator.Next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapLoadedValueIterator.Next().
			return err
		}
		if key == nil {
			return nil
		}
		resume, err := fn(key, value)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by MapEntryIterationFunc callback.
			return wrapErrorAsExternalErrorIfNeeded(err)
		}
		if !resume {
			return nil
		}
	}
}
