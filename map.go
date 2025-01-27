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

// NOTE: we use encoding size (in bytes) instead of Go type size for slab operations,
// such as merge and split, so size constants here are related to encoding size.
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
	mapSlabHeaderSize = SlabIndexLength + 2 + digestSize

	// meta data slab prefix size: version (1 byte) + flag (1 byte) + address (8 bytes) + child header count (2 bytes)
	// Support up to 65,535 children per metadata slab.
	mapMetaDataSlabPrefixSize = versionAndFlagSize + SlabAddressLength + 2

	// version (1 byte) + flag (1 byte) + next id (16 bytes)
	mapDataSlabPrefixSize = versionAndFlagSize + SlabIDLength

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

	// inlined map data slab prefix size:
	//   tag number (2 bytes) +
	//   3-element array head (1 byte) +
	//   extra data ref index (2 bytes) [0, 255] +
	//   value index head (1 byte) +
	//   value index (8 bytes)
	inlinedMapDataSlabPrefixSize = inlinedTagNumSize +
		inlinedCBORArrayHeadSize +
		inlinedExtraDataIndexSize +
		inlinedCBORValueIDHeadSize +
		inlinedValueIDSize
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

	getElementAndNextKey(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, MapKey, error)

	Get(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

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
	) (newElem element, keyStorable MapKey, existingMapValueStorable MapValue, err error)

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

	getElementAndNextKey(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, MapKey, error)

	Get(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

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
	) (MapKey, MapValue, error)

	Remove(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

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

var _ ExtraData = &MapExtraData{}

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
	inlined        bool
}

var _ MapSlab = &MapDataSlab{}
var _ ContainerStorable = &MapDataSlab{}

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

	getElementAndNextKey(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, MapKey, error)

	Get(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

	Set(
		storage SlabStorage,
		b DigesterBuilder,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		hip HashInputProvider,
		key Value,
		value Value,
	) (MapKey, MapValue, error)

	Remove(
		storage SlabStorage,
		digester Digester,
		level uint,
		hkey Digest,
		comparator ValueComparator,
		key Value,
	) (MapKey, MapValue, error)

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

	Inlined() bool
	Inlinable(maxInlineSize uint64) bool
	Inline(SlabStorage) error
	Uninline(SlabStorage) error
}

// OrderedMap is an ordered map of key-value pairs; keys can be any hashable type
// and values can be any serializable value type. It supports heterogeneous key
// or value types (e.g. first key storing a boolean and second key storing a string).
// OrderedMap keeps values in specific sorted order and operations are deterministic
// so the state of the segments after a sequence of operations are always unique.
//
// OrderedMap key-value pairs can be stored in one or more relatively fixed-sized segments.
//
// OrderedMap can be inlined into its parent container when the entire content fits in
// parent container's element size limit.  Specifically, OrderedMap with one segment
// which fits in size limit can be inlined, while OrderedMap with multiple segments
// can't be inlined.
type OrderedMap struct {
	Storage         SlabStorage
	root            MapSlab
	digesterBuilder DigesterBuilder

	// parentUpdater is a callback that notifies parent container when this map is modified.
	// If this callback is nil, this map has no parent.  Otherwise, this map has parent
	// and this callback must be used when this map is changed by Set and Remove.
	//
	// parentUpdater acts like "parent pointer".  It is not stored physically and is only in memory.
	// It is setup when child map is returned from parent's Get.  It is also setup when
	// new child is added to parent through Set or Insert.
	parentUpdater parentUpdater
}

var _ Value = &OrderedMap{}
var _ mutableValueNotifier = &OrderedMap{}

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

func (m *MapExtraData) isExtraData() bool {
	return true
}

func (m *MapExtraData) Type() TypeInfo {
	return m.TypeInfo
}

// Encode encodes extra data as CBOR array:
//
//	[type info, count, seed]
func (m *MapExtraData) Encode(enc *Encoder, encodeTypeInfo encodeTypeInfo) error {

	err := enc.CBOR.EncodeArrayHead(mapExtraDataLength)
	if err != nil {
		return NewEncodingError(err)
	}

	err = encodeTypeInfo(enc, m.TypeInfo)
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

func newElementFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder, slabID SlabID, inlinedExtraData []ExtraData) (element, error) {
	nt, err := cborDec.NextType()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	switch nt {
	case cbor.ArrayType:
		// Don't need to wrap error as external error because err is already categorized by newSingleElementFromData().
		return newSingleElementFromData(cborDec, decodeStorable, slabID, inlinedExtraData)

	case cbor.TagType:
		tagNum, err := cborDec.DecodeTagNumber()
		if err != nil {
			return nil, NewDecodingError(err)
		}
		switch tagNum {
		case CBORTagInlineCollisionGroup:
			// Don't need to wrap error as external error because err is already categorized by newInlineCollisionGroupFromData().
			return newInlineCollisionGroupFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
		case CBORTagExternalCollisionGroup:
			// Don't need to wrap error as external error because err is already categorized by newExternalCollisionGroupFromData().
			return newExternalCollisionGroupFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
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

func newSingleElementFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder, slabID SlabID, inlinedExtraData []ExtraData) (*singleElement, error) {
	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if elemCount != 2 {
		return nil, NewDecodingError(fmt.Errorf("failed to decode single element: expect array of 2 elements, got %d elements", elemCount))
	}

	key, err := decodeStorable(cborDec, slabID, inlinedExtraData)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by StorableDecoder callback.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to decode key's storable")
	}

	value, err := decodeStorable(cborDec, slabID, inlinedExtraData)
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
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map key storable")
	}

	// Encode value
	err = e.value.Encode(enc)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map value storable")
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (e *singleElement) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	k, v, err := e.Get(storage, digester, level, hkey, comparator, key)

	nextKey := MapKey(nil)
	return k, v, nextKey, err
}

func (e *singleElement) Get(storage SlabStorage, _ Digester, _ uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}
	if equal {
		return e.key, e.value, nil
	}
	return nil, nil, NewKeyNotFoundError(key)
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
) (element, MapKey, MapValue, error) {

	equal, err := comparator(storage, key, e.key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
	}

	// Key matches, overwrite existing value
	if equal {
		existingMapValueStorable := e.value

		valueStorable, err := value.Storable(storage, address, maxInlineMapValueSize(uint64(e.key.ByteSize())))
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Value interface.
			return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
		}

		e.value = valueStorable
		e.size = singleElementPrefixSize + e.key.ByteSize() + e.value.ByteSize()
		return e, e.key, existingMapValueStorable, nil
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
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's stored value")
	}

	existingKeyDigest, err := b.Digest(hip, kv)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigestBuilder interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get key's digester")
	}
	defer putDigester(existingKeyDigest)

	d, err := existingKeyDigest.Digest(level + 1)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digester interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get key's digest at level %d", level+1))
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

func newInlineCollisionGroupFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder, slabID SlabID, inlinedExtraData []ExtraData) (*inlineCollisionGroup, error) {
	elements, err := newElementsFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
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

func (e *inlineCollisionGroup) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {

	// Adjust level and hkey for collision group.
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey.
	// Don't need to wrap error as external error because err is already categorized by elements.Get().
	return e.elements.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
}

func (e *inlineCollisionGroup) Get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
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
) (element, MapKey, MapValue, error) {

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("inline collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	keyStorable, existingMapValueStorable, err := e.elements.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Set().
		return nil, nil, nil, err
	}

	if level == 1 {
		// Export oversized inline collision group to separate slab (external collision group)
		// for first level collision.
		if e.Size() > uint32(maxInlineMapElementSize) {

			id, err := storage.GenerateSlabID(address)
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
				return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(
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

			err = storeSlab(storage, slab)
			if err != nil {
				return nil, nil, nil, err
			}

			// Create and return externalCollisionGroup (wrapper of newly created MapDataSlab)
			return &externalCollisionGroup{
				slabID: id,
				size:   externalCollisionGroupPrefixSize + SlabIDStorable(id).ByteSize(),
			}, keyStorable, existingMapValueStorable, nil
		}
	}

	return e, keyStorable, existingMapValueStorable, nil
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

func newExternalCollisionGroupFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder, slabID SlabID, inlinedExtraData []ExtraData) (*externalCollisionGroup, error) {

	storable, err := decodeStorable(cborDec, slabID, inlinedExtraData)
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

func (e *externalCollisionGroup) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, nil, err
	}

	// Adjust level and hkey for collision group.
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	// Search key in collision group with adjusted hkeyPrefix and hkey.
	// Don't need to wrap error as external error because err is already categorized by MapSlab.getElementAndNextKey().
	return slab.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
}

func (e *externalCollisionGroup) Get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
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

	// Search key in collision group with adjusted hkeyPrefix and hkey
	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return slab.Get(storage, digester, level, hkey, comparator, key)
}

func (e *externalCollisionGroup) Set(
	storage SlabStorage,
	_ Address,
	b DigesterBuilder,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (element, MapKey, MapValue, error) {
	slab, err := getMapSlab(storage, e.slabID)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by getMapSlab().
		return nil, nil, nil, err
	}

	// Adjust level and hkey for collision group
	level++
	if level > digester.Levels() {
		return nil, nil, nil, NewHashLevelErrorf("external collision group digest level is %d, want <= %d", level, digester.Levels())
	}
	hkey, _ := digester.Digest(level)

	keyStorable, existingMapValueStorable, err := slab.Set(storage, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, nil, nil, err
	}
	return e, keyStorable, existingMapValueStorable, nil
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

func newElementsFromData(cborDec *cbor.StreamDecoder, decodeStorable StorableDecoder, slabID SlabID, inlinedExtraData []ExtraData) (elements, error) {

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
			elem, err := newSingleElementFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
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
		elem, err := newElementFromData(cborDec, decodeStorable, slabID, inlinedExtraData)
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

func (e *hkeyElements) getElement(
	digester Digester,
	level uint,
	hkey Digest,
	key Value,
) (element, int, error) {

	if level >= digester.Levels() {
		return nil, 0, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
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
		return nil, 0, NewKeyNotFoundError(key)
	}

	return e.elems[equalIndex], equalIndex, nil
}

func (e *hkeyElements) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	elem, _, err := e.getElement(digester, level, hkey, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by hkeyElements.getElement().
		return nil, nil, err
	}

	// Don't need to wrap error as external error because err is already categorized by element.Get().
	return elem.Get(storage, digester, level, hkey, comparator, key)
}

func (e *hkeyElements) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	elem, index, err := e.getElement(digester, level, hkey, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by hkeyElements.getElement().
		return nil, nil, nil, err
	}

	k, v, nk, err := elem.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by hkeyElements.get().
		return nil, nil, nil, err
	}

	if nk != nil {
		// Found next key in element group.
		return k, v, nk, nil
	}

	nextIndex := index + 1

	switch {
	case nextIndex < len(e.elems):
		// Next element is still in the same hkeyElements group.
		nextElement := e.elems[nextIndex]

		nextKey, err := firstKeyInElement(storage, nextElement)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by firstKeyInElement().
			return nil, nil, nil, err
		}

		return k, v, nextKey, nil

	case nextIndex == len(e.elems):
		// Next element is outside this hkeyElements group, so nextKey is nil.
		return k, v, nil, nil

	default: // nextIndex > len(e.elems)
		// This should never happen.
		return nil, nil, nil, NewUnreachableError()
	}
}

func (e *hkeyElements) Set(
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
) (MapKey, MapValue, error) {

	// Check hkeys are not empty
	if level >= digester.Levels() {
		return nil, nil, NewHashLevelErrorf("hkey elements digest level is %d, want < %d", level, digester.Levels())
	}

	if len(e.hkeys) == 0 {
		// first element

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, nil, err
		}

		e.hkeys = []Digest{hkey}

		e.elems = []element{newElem}

		e.size += digestSize + newElem.Size()

		return newElem.key, nil, nil
	}

	if hkey < e.hkeys[0] {
		// prepend key and value

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, nil, err
		}

		e.hkeys = append(e.hkeys, Digest(0))
		copy(e.hkeys[1:], e.hkeys)
		e.hkeys[0] = hkey

		e.elems = append(e.elems, nil)
		copy(e.elems[1:], e.elems)
		e.elems[0] = newElem

		e.size += digestSize + newElem.Size()

		return newElem.key, nil, nil
	}

	if hkey > e.hkeys[len(e.hkeys)-1] {
		// append key and value

		newElem, err := newSingleElement(storage, address, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newSingleElement().
			return nil, nil, err
		}

		e.hkeys = append(e.hkeys, hkey)

		e.elems = append(e.elems, newElem)

		e.size += digestSize + newElem.Size()

		return newElem.key, nil, nil
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
				return nil, nil, err
			}
			if elementCount == 0 {
				return nil, nil, NewMapElementCountError("expect element count > 0, got element count == 0")
			}

			// collisionCount is elementCount-1 because:
			// - if elem is single element, collision count is 0 (no collsion yet)
			// - if elem is collision group, collision count is 1 less than number
			//   of elements in collision group.
			collisionCount := elementCount - 1

			// Check if existing collision count reached MaxCollisionLimitPerDigest
			if collisionCount >= MaxCollisionLimitPerDigest {
				// Enforce collision limit on inserts and ignore updates.
				_, _, err = elem.Get(storage, digester, level, hkey, comparator, key)
				if err != nil {
					var knfe *KeyNotFoundError
					if errors.As(err, &knfe) {
						// Don't allow any more collisions for a digest that
						// already reached MaxCollisionLimitPerDigest.
						return nil, nil, NewCollisionLimitError(MaxCollisionLimitPerDigest)
					}
				}
			}
		}

		elem, keyStorable, existingMapValueStorable, err := elem.Set(storage, address, b, digester, level, hkey, comparator, hip, key, value)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by element.Set().
			return nil, nil, err
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

		return keyStorable, existingMapValueStorable, nil
	}

	// No matching hkey

	newElem, err := newSingleElement(storage, address, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newSingleElement().
		return nil, nil, err
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

	return newElem.key, nil, nil
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

func (e *singleElements) get(storage SlabStorage, digester Digester, level uint, _ Digest, comparator ValueComparator, key Value) (MapKey, MapValue, int, error) {

	if level != digester.Levels() {
		return nil, nil, 0, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search by key
	for i, elem := range e.elems {
		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, nil, 0, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}
		if equal {
			return elem.key, elem.value, i, nil
		}
	}

	return nil, nil, 0, NewKeyNotFoundError(key)
}

func (e *singleElements) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	k, v, _, err := e.get(storage, digester, level, hkey, comparator, key)
	return k, v, err
}

func (e *singleElements) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	k, v, index, err := e.get(storage, digester, level, hkey, comparator, key)
	if err != nil {
		return nil, nil, nil, err
	}

	nextIndex := index + 1

	switch {
	case nextIndex < len(e.elems):
		// Next element is still in the same singleElements group.
		nextKey := e.elems[nextIndex].key
		return k, v, nextKey, nil

	case nextIndex == len(e.elems):
		// Next element is outside this singleElements group, so nextKey is nil.
		return k, v, nil, nil

	default: // nextIndex > len(e.elems)
		// This should never happen.
		return nil, nil, nil, NewUnreachableError()
	}
}

func (e *singleElements) Set(
	storage SlabStorage,
	address Address,
	_ DigesterBuilder,
	digester Digester,
	level uint,
	_ Digest,
	comparator ValueComparator,
	_ HashInputProvider,
	key Value,
	value Value,
) (MapKey, MapValue, error) {

	if level != digester.Levels() {
		return nil, nil, NewHashLevelErrorf("single elements digest level is %d, want %d", level, digester.Levels())
	}

	// linear search key and update value
	for i := 0; i < len(e.elems); i++ {
		elem := e.elems[i]

		equal, err := comparator(storage, key, elem.key)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by ValueComparator callback.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to compare keys")
		}

		if equal {
			existingKeyStorable := elem.key
			existingValueStorable := elem.value

			vs, err := value.Storable(storage, address, maxInlineMapValueSize(uint64(elem.key.ByteSize())))
			if err != nil {
				// Wrap err as external error (if needed) because err is returned by Value interface.
				return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get value's storable")
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

			return existingKeyStorable, existingValueStorable, nil
		}
	}

	// no matching key, append new element to the end.
	newElem, err := newSingleElement(storage, address, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newSingleElement().
		return nil, nil, err
	}
	e.elems = append(e.elems, newElem)
	e.size += newElem.size

	return newElem.key, nil, nil
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
		if len(data) < SlabIDLength {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		// Decode next slab ID
		var err error
		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		data = data[SlabIDLength:]
	}

	// Decode elements
	cborDec := decMode.NewByteStreamDecoder(data)
	elements, err := newElementsFromData(cborDec, decodeStorable, id, nil)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromDataV0().
		return nil, err
	}

	// Compute slab size for version 1.
	slabSize := versionAndFlagSize + elements.Size()
	if !h.isRoot() {
		slabSize += SlabIDLength
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
// DataSlab Header:
//
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | extra data (if root) | inlined extra data (if present) | next slab ID (if non-empty) |
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See InlinedExtraData.Encode() for inlined extra data section format.
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
	var inlinedExtraData []ExtraData
	var next SlabID

	// Decode extra data
	if h.isRoot() {
		extraData, data, err = newMapExtraDataFromData(data, decMode, decodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newMapExtraDataFromData().
			return nil, err
		}
	}

	// Decode inlined extra data
	if h.hasInlinedSlabs() {
		inlinedExtraData, data, err = newInlinedExtraDataFromData(
			data,
			decMode,
			decodeStorable,
			decodeTypeInfo,
		)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by newInlinedExtraDataFromData().
			return nil, err
		}
	}

	// Decode next slab ID for non-root slab
	if h.hasNextSlabID() {
		if len(data) < SlabIDLength {
			return nil, NewDecodingErrorf("data is too short for map data slab")
		}

		next, err = NewSlabIDFromRawBytes(data)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by NewSlabIDFromRawBytes().
			return nil, err
		}

		data = data[SlabIDLength:]
	}

	// Decode elements
	cborDec := decMode.NewByteStreamDecoder(data)
	elements, err := newElementsFromData(cborDec, decodeStorable, id, inlinedExtraData)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromDataV1().
		return nil, err
	}

	// Compute slab size.
	slabSize := versionAndFlagSize + elements.Size()
	if !h.isRoot() {
		slabSize += SlabIDLength
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

// DecodeInlinedCompactMapStorable decodes inlined compact map data. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedCompactMap, and tag contant
// as 3-element array:
//
// - index of inlined extra data
// - value ID index
// - CBOR array of elements
//
// NOTE: This function doesn't decode tag number because tag number is decoded
// in the caller and decoder only contains tag content.
func DecodeInlinedCompactMapStorable(
	dec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	parentSlabID SlabID,
	inlinedExtraData []ExtraData,
) (
	Storable,
	error,
) {
	const inlinedMapDataSlabArrayCount = 3

	arrayCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != inlinedMapDataSlabArrayCount {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data, expect array of %d elements, got %d elements",
				inlinedMapDataSlabArrayCount,
				arrayCount))
	}

	// element 0: extra data index
	extraDataIndex, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if extraDataIndex >= uint64(len(inlinedExtraData)) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: inlined extra data index %d exceeds number of inlined extra data %d",
				extraDataIndex,
				len(inlinedExtraData)))
	}

	extraData, ok := inlinedExtraData[extraDataIndex].(*compactMapExtraData)
	if !ok {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: expect *compactMapExtraData, got %T",
				inlinedExtraData[extraDataIndex]))
	}

	// element 1: slab index
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if len(b) != SlabIndexLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: expect %d bytes for slab index, got %d bytes",
				SlabIndexLength,
				len(b)))
	}

	var index SlabIndex
	copy(index[:], b)

	slabID := NewSlabID(parentSlabID.address, index)

	// Decode values
	elemCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if elemCount != uint64(len(extraData.keys)) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode compact map values: got %d, expect %d",
				elemCount,
				extraData.mapExtraData.Count))
	}

	// Make a copy of digests because extraData is shared by all inlined compact map data referring to the same type.
	hkeys := make([]Digest, len(extraData.hkeys))
	copy(hkeys, extraData.hkeys)

	// Decode values
	elementsSize := uint32(hkeyElementsPrefixSize)
	elems := make([]element, elemCount)
	for i := 0; i < int(elemCount); i++ {
		value, err := decodeStorable(dec, slabID, inlinedExtraData)
		if err != nil {
			return nil, err
		}

		// Make a copy of key in case it is shared.
		key := extraData.keys[i].Copy()

		elemSize := singleElementPrefixSize + key.ByteSize() + value.ByteSize()
		elem := &singleElement{key, value, elemSize}

		elems[i] = elem
		elementsSize += digestSize + elem.Size()
	}

	// Create hkeyElements
	elements := &hkeyElements{
		hkeys: hkeys,
		elems: elems,
		level: 0,
		size:  elementsSize,
	}

	header := MapSlabHeader{
		slabID:   slabID,
		size:     inlinedMapDataSlabPrefixSize + elements.Size(),
		firstKey: elements.firstKey(),
	}

	return &MapDataSlab{
		header:   header,
		elements: elements,
		extraData: &MapExtraData{
			// Make a copy of extraData.TypeInfo because
			// inlined extra data are shared by all inlined slabs.
			TypeInfo: extraData.mapExtraData.TypeInfo.Copy(),
			Count:    extraData.mapExtraData.Count,
			Seed:     extraData.mapExtraData.Seed,
		},
		anySize:        false,
		collisionGroup: false,
		inlined:        true,
	}, nil
}

// DecodeInlinedMapStorable decodes inlined map data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedMap, and tag contant
// as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
//
// NOTE: This function doesn't decode tag number because tag number is decoded
// in the caller and decoder only contains tag content.
func DecodeInlinedMapStorable(
	dec *cbor.StreamDecoder,
	decodeStorable StorableDecoder,
	parentSlabID SlabID,
	inlinedExtraData []ExtraData,
) (
	Storable,
	error,
) {
	const inlinedMapDataSlabArrayCount = 3

	arrayCount, err := dec.DecodeArrayHead()
	if err != nil {
		return nil, NewDecodingError(err)
	}

	if arrayCount != inlinedMapDataSlabArrayCount {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined map data slab, expect array of %d elements, got %d elements",
				inlinedMapDataSlabArrayCount,
				arrayCount))
	}

	// element 0: extra data index
	extraDataIndex, err := dec.DecodeUint64()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if extraDataIndex >= uint64(len(inlinedExtraData)) {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: inlined extra data index %d exceeds number of inlined extra data %d",
				extraDataIndex,
				len(inlinedExtraData)))
	}
	extraData, ok := inlinedExtraData[extraDataIndex].(*MapExtraData)
	if !ok {
		return nil, NewDecodingError(
			fmt.Errorf(
				"extra data (%T) is wrong type, expect *MapExtraData",
				inlinedExtraData[extraDataIndex]))
	}

	// element 1: slab index
	b, err := dec.DecodeBytes()
	if err != nil {
		return nil, NewDecodingError(err)
	}
	if len(b) != SlabIndexLength {
		return nil, NewDecodingError(
			fmt.Errorf(
				"failed to decode inlined compact map data: expect %d bytes for slab index, got %d bytes",
				SlabIndexLength,
				len(b)))
	}

	var index SlabIndex
	copy(index[:], b)

	slabID := NewSlabID(parentSlabID.address, index)

	// Decode elements
	elements, err := newElementsFromData(dec, decodeStorable, slabID, inlinedExtraData)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by newElementsFromData().
		return nil, err
	}

	header := MapSlabHeader{
		slabID:   slabID,
		size:     inlinedMapDataSlabPrefixSize + elements.Size(),
		firstKey: elements.firstKey(),
	}

	// NOTE: extra data doesn't need to be copied because every inlined map has its own inlined extra data.

	return &MapDataSlab{
		header:   header,
		elements: elements,
		extraData: &MapExtraData{
			// Make a copy of extraData.TypeInfo because
			// inlined extra data are shared by all inlined slabs.
			TypeInfo: extraData.TypeInfo.Copy(),
			Count:    extraData.Count,
			Seed:     extraData.Seed,
		},
		anySize:        false,
		collisionGroup: false,
		inlined:        true,
	}, nil
}

// Encode encodes this map data slab to the given encoder.
//
// Root DataSlab Header:
//
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//	| slab version + flag (2 bytes) | extra data (if root) | inlined extra data (if present) | next slab ID (if non-empty) |
//	+-------------------------------+----------------------+---------------------------------+-----------------------------+
//
// Content:
//
//	CBOR encoded elements
//
// See MapExtraData.Encode() for extra data section format.
// See InlinedExtraData.Encode() for inlined extra data section format.
// See hkeyElements.Encode() and singleElements.Encode() for elements section format.
func (m *MapDataSlab) Encode(enc *Encoder) error {

	if m.inlined {
		return m.encodeAsInlined(enc)
	}

	// Encoding is done in two steps:
	//
	// 1. Encode map elements using a new buffer while collecting inlined extra data from inlined elements.
	// 2. Encode slab with deduplicated inlined extra data and copy encoded elements from previous buffer.

	// Get a buffer from a pool to encode elements.
	elementBuf := getBuffer()
	defer putBuffer(elementBuf)

	elemEnc := NewEncoder(elementBuf, enc.encMode)

	err := m.encodeElements(elemEnc)
	if err != nil {
		return err
	}

	const version = 1

	slabType := slabMapData
	if m.collisionGroup {
		slabType = slabMapCollisionGroup
	}

	h, err := newMapSlabHead(version, slabType)
	if err != nil {
		return NewEncodingError(err)
	}

	if m.HasPointer() {
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

	if elemEnc.hasInlinedExtraData() {
		h.setHasInlinedSlabs()
	}

	// Encode head
	_, err = enc.Write(h[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// Encode extra data
	if m.extraData != nil {
		// Use defaultEncodeTypeInfo to encode root level TypeInfo as is.
		err = m.extraData.Encode(enc, defaultEncodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapExtraData.Encode().
			return err
		}
	}

	// Encode inlined types
	if elemEnc.hasInlinedExtraData() {
		err = elemEnc.inlinedExtraData().Encode(enc)
		if err != nil {
			return NewEncodingError(err)
		}
	}

	// Encode next slab ID for non-root slab
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
	err = enc.CBOR.EncodeRawBytes(elementBuf.Bytes())
	if err != nil {
		return NewEncodingError(err)
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

func (m *MapDataSlab) encodeElements(enc *Encoder) error {
	err := m.elements.Encode(enc)
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

// encodeAsInlined encodes inlined map data slab. Encoding is
// version 1 with CBOR tag having tag number CBORTagInlinedMap,
// and tag contant as 3-element array:
//
//	+------------------+----------------+----------+
//	| extra data index | value ID index | elements |
//	+------------------+----------------+----------+
func (m *MapDataSlab) encodeAsInlined(enc *Encoder) error {
	if m.extraData == nil {
		return NewEncodingError(
			fmt.Errorf("failed to encode non-root map data slab as inlined"))
	}

	if !m.inlined {
		return NewEncodingError(
			fmt.Errorf("failed to encode standalone map data slab as inlined"))
	}

	if hkeys, keys, values, ok := m.canBeEncodedAsCompactMap(); ok {
		return encodeAsInlinedCompactMap(enc, m.header.slabID, m.extraData, hkeys, keys, values)
	}

	return m.encodeAsInlinedMap(enc)
}

func (m *MapDataSlab) encodeAsInlinedMap(enc *Encoder) error {

	extraDataIndex, err := enc.inlinedExtraData().addMapExtraData(m.extraData)
	if err != nil {
		// err is already categorized by InlinedExtraData.addMapExtraData().
		return err
	}

	if extraDataIndex > maxInlinedExtraDataIndex {
		return NewEncodingError(fmt.Errorf("extra data index %d exceeds limit %d", extraDataIndex, maxInlinedExtraDataIndex))
	}

	// Encode tag number and array head of 3 elements
	err = enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagInlinedMap,
		// array head of 3 elements
		0x83,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: extra data index
	// NOTE: encoded extra data index is fixed sized CBOR uint
	err = enc.CBOR.EncodeRawBytes([]byte{
		0x18,
		byte(extraDataIndex),
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 1: slab index
	err = enc.CBOR.EncodeBytes(m.header.slabID.index[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// element 2: map elements
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

// encodeAsInlinedCompactMap encodes hkeys, keys, and values as inlined compact map value.
func encodeAsInlinedCompactMap(
	enc *Encoder,
	slabID SlabID,
	extraData *MapExtraData,
	hkeys []Digest,
	keys []ComparableStorable,
	values []Storable,
) error {

	extraDataIndex, cachedKeys, err := enc.inlinedExtraData().addCompactMapExtraData(extraData, hkeys, keys)
	if err != nil {
		// err is already categorized by InlinedExtraData.addCompactMapExtraData().
		return err
	}

	if len(keys) != len(cachedKeys) {
		return NewEncodingError(fmt.Errorf("number of elements %d is different from number of elements in cached compact map type %d", len(keys), len(cachedKeys)))
	}

	if extraDataIndex > maxInlinedExtraDataIndex {
		// This should never happen because of slab size.
		return NewEncodingError(fmt.Errorf("extra data index %d exceeds limit %d", extraDataIndex, maxInlinedExtraDataIndex))
	}

	// Encode tag number and array head of 3 elements
	err = enc.CBOR.EncodeRawBytes([]byte{
		// tag number
		0xd8, CBORTagInlinedCompactMap,
		// array head of 3 elements
		0x83,
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 0: extra data index
	// NOTE: encoded extra data index is fixed sized CBOR uint
	err = enc.CBOR.EncodeRawBytes([]byte{
		0x18,
		byte(extraDataIndex),
	})
	if err != nil {
		return NewEncodingError(err)
	}

	// element 1: slab id
	err = enc.CBOR.EncodeBytes(slabID.index[:])
	if err != nil {
		return NewEncodingError(err)
	}

	// element 2: compact map values in the order of cachedKeys
	err = encodeCompactMapValues(enc, cachedKeys, keys, values)
	if err != nil {
		// err is already categorized by encodeCompactMapValues().
		return err
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return NewEncodingError(err)
	}

	return nil
}

// encodeCompactMapValues encodes compact values as an array of values ordered by cachedKeys.
func encodeCompactMapValues(
	enc *Encoder,
	cachedKeys []ComparableStorable,
	keys []ComparableStorable,
	values []Storable,
) error {

	var err error

	err = enc.CBOR.EncodeArrayHead(uint64(len(cachedKeys)))
	if err != nil {
		return NewEncodingError(err)
	}

	keyIndexes := make([]int, len(keys))
	for i := 0; i < len(keys); i++ {
		keyIndexes[i] = i
	}

	// Encode values in the same order as cachedKeys.
	for i, cachedKey := range cachedKeys {
		found := false
		for j := i; j < len(keyIndexes); j++ {
			index := keyIndexes[j]
			key := keys[index]

			if cachedKey.Equal(key) {
				found = true
				keyIndexes[i], keyIndexes[j] = keyIndexes[j], keyIndexes[i]

				err = values[index].Encode(enc)
				if err != nil {
					// Wrap err as external error (if needed) because err is returned by Storable interface.
					return wrapErrorfAsExternalErrorIfNeeded(err, "failed to encode map value storable")
				}

				break
			}
		}
		if !found {
			return NewEncodingError(fmt.Errorf("failed to find key %v", cachedKey))
		}
	}

	return nil
}

// canBeEncodedAsCompactMap returns true if:
// - map data slab is inlined
// - map type is composite type
// - no collision elements
// - keys are stored inline (not in a separate slab)
func (m *MapDataSlab) canBeEncodedAsCompactMap() ([]Digest, []ComparableStorable, []Storable, bool) {
	if !m.inlined {
		return nil, nil, nil, false
	}

	if !m.extraData.TypeInfo.IsComposite() {
		return nil, nil, nil, false
	}

	elements, ok := m.elements.(*hkeyElements)
	if !ok {
		return nil, nil, nil, false
	}

	keys := make([]ComparableStorable, m.extraData.Count)
	values := make([]Storable, m.extraData.Count)

	for i, e := range elements.elems {
		se, ok := e.(*singleElement)
		if !ok {
			// Has collision element
			return nil, nil, nil, false
		}

		if _, ok = se.key.(SlabIDStorable); ok {
			// Key is stored in a separate slab
			return nil, nil, nil, false
		}

		key, ok := se.key.(ComparableStorable)
		if !ok {
			// Key can't be compared (sorted)
			return nil, nil, nil, false
		}

		keys[i] = key
		values[i] = se.value
	}

	return elements.hkeys, keys, values, true
}

func (m *MapDataSlab) HasPointer() bool {
	return m.elements.hasPointer()
}

func (m *MapDataSlab) ChildStorables() []Storable {
	return elementsStorables(m.elements, nil)
}

func (m *MapDataSlab) getPrefixSize() uint32 {
	if m.inlined {
		return inlinedMapDataSlabPrefixSize
	}
	if m.extraData != nil {
		return mapRootDataSlabPrefixSize
	}
	return mapDataSlabPrefixSize
}

func (m *MapDataSlab) Inlined() bool {
	return m.inlined
}

// Inlinable returns true if
// - map data slab is root slab
// - size of inlined map data slab <= maxInlineSize
func (m *MapDataSlab) Inlinable(maxInlineSize uint64) bool {
	if m.extraData == nil {
		// Non-root data slab is not inlinable.
		return false
	}

	inlinedSize := inlinedMapDataSlabPrefixSize + m.elements.Size()

	// Inlined byte size must be less than max inline size.
	return uint64(inlinedSize) <= maxInlineSize
}

// inline converts not-inlined MapDataSlab to inlined MapDataSlab and removes it from storage.
func (m *MapDataSlab) Inline(storage SlabStorage) error {
	if m.inlined {
		return NewFatalError(fmt.Errorf("failed to inline MapDataSlab %s: it is inlined already", m.header.slabID))
	}

	id := m.header.slabID

	// Remove slab from storage because it is going to be inlined.
	err := storage.Remove(id)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", id))
	}

	// Update data slab size from not inlined to inlined
	m.header.size = inlinedMapDataSlabPrefixSize + m.elements.Size()

	// Update data slab inlined status.
	m.inlined = true

	return nil
}

// uninline converts an inlined MapDataSlab to uninlined MapDataSlab and stores it in storage.
func (m *MapDataSlab) Uninline(storage SlabStorage) error {
	if !m.inlined {
		return NewFatalError(fmt.Errorf("failed to uninline MapDataSlab %s: it is not inlined", m.header.slabID))
	}

	// Update data slab size from inlined to not inlined.
	m.header.size = mapRootDataSlabPrefixSize + m.elements.Size()

	// Update data slab inlined status.
	m.inlined = false

	// Store slab in storage
	return storeSlab(storage, m)
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

func (m *MapDataSlab) Set(
	storage SlabStorage,
	b DigesterBuilder,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (MapKey, MapValue, error) {

	keyStorable, existingMapValueStorable, err := m.elements.Set(storage, m.SlabID().address, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by elements.Set().
		return nil, nil, err
	}

	// Adjust header's first key
	m.header.firstKey = m.elements.firstKey()

	// Adjust header's slab size
	m.header.size = m.getPrefixSize() + m.elements.Size()

	// Store modified slab
	if !m.inlined {
		err := storeSlab(storage, m)
		if err != nil {
			return nil, nil, err
		}
	}

	return keyStorable, existingMapValueStorable, nil
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
	if !m.inlined {
		err := storeSlab(storage, m)
		if err != nil {
			return nil, nil, err
		}
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
		mapSlabHeaderSizeV0        = SlabIDLength + 4 + digestSize
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

		firstKeyOffset := offset + SlabIDLength
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
	offset += SlabAddressLength

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
		offset += SlabIndexLength

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
		// Use defaultEncodeTypeInfo to encode root level TypeInfo as is.
		err = m.extraData.Encode(enc, defaultEncodeTypeInfo)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapExtraData.Encode().
			return err
		}
	}

	// Encode shared address to scratch
	copy(enc.Scratch[:], m.header.slabID.address[:])

	// Encode child header count to scratch
	const childHeaderCountOffset = SlabAddressLength
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

		const firstKeyOffset = SlabIndexLength
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

func (m *MapMetaDataSlab) Inlined() bool {
	return false
}

func (m *MapMetaDataSlab) Inlinable(_ uint64) bool {
	return false
}

func (m *MapMetaDataSlab) Inline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to inline MapMetaDataSlab %s: MapMetaDataSlab can't be inlined", m.header.slabID))
}

func (m *MapMetaDataSlab) Uninline(_ SlabStorage) error {
	return NewFatalError(fmt.Errorf("failed to uninline MapMetaDataSlab %s: MapMetaDataSlab is already unlined", m.header.slabID))
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

func (m *MapMetaDataSlab) getChildSlabByDigest(storage SlabStorage, hkey Digest, key Value) (MapSlab, int, error) {

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
		return nil, 0, NewKeyNotFoundError(key)
	}

	childHeaderIndex := ans

	childID := m.childrenHeaders[childHeaderIndex].slabID

	child, err := getMapSlab(storage, childID)
	if err != nil {
		return nil, 0, err
	}

	return child, childHeaderIndex, nil
}

func (m *MapMetaDataSlab) Get(storage SlabStorage, digester Digester, level uint, hkey Digest, comparator ValueComparator, key Value) (MapKey, MapValue, error) {
	child, _, err := m.getChildSlabByDigest(storage, hkey, key)
	if err != nil {
		return nil, nil, err
	}

	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return child.Get(storage, digester, level, hkey, comparator, key)
}

func (m *MapMetaDataSlab) getElementAndNextKey(
	storage SlabStorage,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	key Value,
) (MapKey, MapValue, MapKey, error) {
	child, index, err := m.getChildSlabByDigest(storage, hkey, key)
	if err != nil {
		return nil, nil, nil, err
	}

	k, v, nextKey, err := child.getElementAndNextKey(storage, digester, level, hkey, comparator, key)
	if err != nil {
		return nil, nil, nil, err
	}

	if nextKey != nil {
		// Next element is still in the same child slab.
		return k, v, nextKey, nil
	}

	// Next element is in the next child slab.

	nextIndex := index + 1

	switch {
	case nextIndex < len(m.childrenHeaders):
		// Next element is in the next child of this MapMetaDataSlab.
		nextChildID := m.childrenHeaders[nextIndex].slabID

		nextChild, err := getMapSlab(storage, nextChildID)
		if err != nil {
			return nil, nil, nil, err
		}

		nextKey, err = firstKeyInMapSlab(storage, nextChild)
		if err != nil {
			return nil, nil, nil, err
		}

		return k, v, nextKey, nil

	case nextIndex == len(m.childrenHeaders):
		// Next element is outside this MapMetaDataSlab, so nextKey is nil.
		return k, v, nil, nil

	default: // nextIndex > len(m.childrenHeaders)
		// This should never happen.
		return nil, nil, nil, NewUnreachableError()
	}
}

func (m *MapMetaDataSlab) Set(
	storage SlabStorage,
	b DigesterBuilder,
	digester Digester,
	level uint,
	hkey Digest,
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	value Value,
) (MapKey, MapValue, error) {

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
		return nil, nil, err
	}

	keyStorable, existingMapValueStorable, err := child.Set(storage, b, digester, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
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
		return keyStorable, existingMapValueStorable, nil
	}

	if underflowSize, underflow := child.IsUnderflow(); underflow {
		err := m.MergeOrRebalanceChildSlab(storage, child, childHeaderIndex, underflowSize)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapMetaDataSlab.MergeOrRebalanceChildSlab().
			return nil, nil, err
		}
		return keyStorable, existingMapValueStorable, nil
	}

	err = storeSlab(storage, m)
	if err != nil {
		return nil, nil, err
	}
	return keyStorable, existingMapValueStorable, nil
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

	err = storeSlab(storage, m)
	if err != nil {
		return nil, nil, err
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
	err = storeSlab(storage, left)
	if err != nil {
		return err
	}

	err = storeSlab(storage, right)
	if err != nil {
		return err
	}

	return storeSlab(storage, m)
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
			err = storeSlab(storage, child)
			if err != nil {
				return err
			}

			err = storeSlab(storage, rightSib)
			if err != nil {
				return err
			}

			return storeSlab(storage, m)
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
			err = storeSlab(storage, leftSib)
			if err != nil {
				return err
			}

			err = storeSlab(storage, child)
			if err != nil {
				return err
			}

			return storeSlab(storage, m)
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
			err = storeSlab(storage, leftSib)
			if err != nil {
				return err
			}

			err = storeSlab(storage, child)
			if err != nil {
				return err
			}

			return storeSlab(storage, m)

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
			err = storeSlab(storage, child)
			if err != nil {
				return err
			}

			err = storeSlab(storage, rightSib)
			if err != nil {
				return err
			}

			return storeSlab(storage, m)
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
		err = storeSlab(storage, child)
		if err != nil {
			return err
		}

		err = storeSlab(storage, m)
		if err != nil {
			return err
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
		err = storeSlab(storage, leftSib)
		if err != nil {
			return err
		}

		err = storeSlab(storage, m)
		if err != nil {
			return err
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
		err = storeSlab(storage, leftSib)
		if err != nil {
			return err
		}

		err = storeSlab(storage, m)
		if err != nil {
			return err
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
		err = storeSlab(storage, child)
		if err != nil {
			return err
		}

		err = storeSlab(storage, m)
		if err != nil {
			return err
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

	err = storeSlab(storage, root)
	if err != nil {
		return nil, err
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

func (m *OrderedMap) Inlined() bool {
	return m.root.Inlined()
}

func (m *OrderedMap) Inlinable(maxInlineSize uint64) bool {
	return m.root.Inlinable(maxInlineSize)
}

func (m *OrderedMap) setParentUpdater(f parentUpdater) {
	m.parentUpdater = f
}

func (m *OrderedMap) rootSlab() MapSlab {
	return m.root
}

func (m *OrderedMap) getDigesterBuilder() DigesterBuilder {
	return m.digesterBuilder
}

// setCallbackWithChild sets up callback function with child value (child)
// so parent map (m) can be notified when child value is modified.
func (m *OrderedMap) setCallbackWithChild(
	comparator ValueComparator,
	hip HashInputProvider,
	key Value,
	child Value,
	maxInlineSize uint64,
) {
	c, ok := child.(mutableValueNotifier)
	if !ok {
		return
	}

	vid := c.ValueID()

	c.setParentUpdater(func() (found bool, err error) {

		// Avoid unnecessary write operation on parent container.
		// Child value was stored as SlabIDStorable (not inlined) in parent container,
		// and continues to be stored as SlabIDStorable (still not inlinable),
		// so no update to parent container is needed.
		if !c.Inlined() && !c.Inlinable(maxInlineSize) {
			return true, nil
		}

		// Retrieve element value under the same key and
		// verify retrieved value is this child (c).
		_, valueStorable, err := m.get(comparator, hip, key)
		if err != nil {
			var knf *KeyNotFoundError
			if errors.As(err, &knf) {
				return false, nil
			}
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.Get().
			return false, err
		}

		// Verify retrieved element value is either SlabIDStorable or Slab, with identical value ID.
		switch valueStorable := valueStorable.(type) {
		case SlabIDStorable:
			sid := SlabID(valueStorable)
			if !vid.equal(sid) {
				return false, nil
			}

		case Slab:
			sid := valueStorable.SlabID()
			if !vid.equal(sid) {
				return false, nil
			}

		default:
			return false, nil
		}

		// Set child value with parent map using same key.
		// Set() calls c.Storable() which returns inlined or not-inlined child storable.
		existingValueStorable, err := m.set(comparator, hip, key, c)
		if err != nil {
			return false, err
		}

		// Verify overwritten storable has identical value ID.

		switch existingValueStorable := existingValueStorable.(type) {
		case SlabIDStorable:
			sid := SlabID(existingValueStorable)
			if !vid.equal(sid) {
				return false, NewFatalError(
					fmt.Errorf(
						"failed to reset child value in parent updater callback: overwritten SlabIDStorable %s != value ID %s",
						sid,
						vid))
			}

		case Slab:
			sid := existingValueStorable.SlabID()
			if !vid.equal(sid) {
				return false, NewFatalError(
					fmt.Errorf(
						"failed to reset child value in parent updater callback: overwritten Slab ID %s != value ID %s",
						sid,
						vid))
			}

		case nil:
			return false, NewFatalError(
				fmt.Errorf(
					"failed to reset child value in parent updater callback: overwritten value is nil"))

		default:
			return false, NewFatalError(
				fmt.Errorf(
					"failed to reset child value in parent updater callback: overwritten value is wrong type %T",
					existingValueStorable))
		}

		return true, nil
	})
}

// notifyParentIfNeeded calls parent updater if this map (m) is a child
// element in another container.
func (m *OrderedMap) notifyParentIfNeeded() error {
	if m.parentUpdater == nil {
		return nil
	}

	// If parentUpdater() doesn't find child map (m), then no-op on parent container
	// and unset parentUpdater callback in child map.  This can happen when child
	// map is an outdated reference (removed or overwritten in parent container).
	found, err := m.parentUpdater()
	if err != nil {
		return err
	}
	if !found {
		m.parentUpdater = nil
	}
	return nil
}

func (m *OrderedMap) Has(comparator ValueComparator, hip HashInputProvider, key Value) (bool, error) {
	_, _, err := m.get(comparator, hip, key)
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

	keyStorable, valueStorable, err := m.get(comparator, hip, key)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
		return nil, err
	}

	v, err := valueStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	// As a parent, this map (m) sets up notification callback with child
	// value (v) so this map can be notified when child value is modified.
	maxInlineSize := maxInlineMapValueSize(uint64(keyStorable.ByteSize()))
	m.setCallbackWithChild(comparator, hip, key, v, maxInlineSize)

	return v, nil
}

func (m *OrderedMap) get(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, Storable, error) {

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
		return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	// Don't need to wrap error as external error because err is already categorized by MapSlab.Get().
	return m.root.Get(m.Storage, keyDigest, level, hkey, comparator, key)
}

func (m *OrderedMap) getElementAndNextKey(comparator ValueComparator, hip HashInputProvider, key Value) (Value, Value, Value, error) {

	keyDigest, err := m.digesterBuilder.Digest(hip, key)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by DigesterBuilder interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to create map key digester")
	}
	defer putDigester(keyDigest)

	level := uint(0)

	hkey, err := keyDigest.Digest(level)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Digesert interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to get map key digest at level %d", level))
	}

	keyStorable, valueStorable, nextKeyStorable, err := m.root.getElementAndNextKey(m.Storage, keyDigest, level, hkey, comparator, key)
	if err != nil {
		return nil, nil, nil, err
	}

	k, err := keyStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	v, err := valueStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	var nextKey Value
	if nextKeyStorable != nil {
		nextKey, err = nextKeyStorable.StoredValue(m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}
	}

	// As a parent, this map (m) sets up notification callback with child
	// value (v) so this map can be notified when child value is modified.
	maxInlineSize := maxInlineMapValueSize(uint64(keyStorable.ByteSize()))
	m.setCallbackWithChild(comparator, hip, key, v, maxInlineSize)

	return k, v, nextKey, nil
}

func (m *OrderedMap) getNextKey(comparator ValueComparator, hip HashInputProvider, key Value) (Value, error) {

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

	_, _, nextKeyStorable, err := m.root.getElementAndNextKey(m.Storage, keyDigest, level, hkey, comparator, key)
	if err != nil {
		return nil, err
	}

	if nextKeyStorable == nil {
		return nil, nil
	}

	nextKey, err := nextKeyStorable.StoredValue(m.Storage)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by Storable interface.
		return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
	}

	return nextKey, nil
}

func (m *OrderedMap) Set(comparator ValueComparator, hip HashInputProvider, key Value, value Value) (Storable, error) {
	storable, err := m.set(comparator, hip, key, value)
	if err != nil {
		return nil, err
	}

	// If overwritten storable is an inlined slab, uninline the slab and store it in storage.
	// This is to prevent potential data loss because the overwritten inlined slab was not in
	// storage and any future changes to it would have been lost.
	switch s := storable.(type) {
	case ArraySlab: // inlined array slab
		err = s.Uninline(m.Storage)
		if err != nil {
			return nil, err
		}
		storable = SlabIDStorable(s.SlabID())

	case MapSlab: // inlined map slab
		err = s.Uninline(m.Storage)
		if err != nil {
			return nil, err
		}
		storable = SlabIDStorable(s.SlabID())
	}

	return storable, nil
}

func (m *OrderedMap) set(comparator ValueComparator, hip HashInputProvider, key Value, value Value) (Storable, error) {

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

	keyStorable, existingMapValueStorable, err := m.root.Set(m.Storage, m.digesterBuilder, keyDigest, level, hkey, comparator, hip, key, value)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapSlab.Set().
		return nil, err
	}

	if existingMapValueStorable == nil {
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
		}
	}

	if m.root.IsFull() {
		err := m.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.splitRoot().
			return nil, err
		}
	}

	// This map (m) is a parent to the new child (value), and this map
	// can also be a child in another container.
	//
	// As a parent, this map needs to setup notification callback with
	// the new child value, so it can be notified when child is modified.
	//
	// If this map is a child, it needs to notify its parent because its
	// content (maybe also its size) is changed by this "Set" operation.

	// If this map is a child, it notifies parent by invoking callback because
	// this map is changed by setting new child.
	err = m.notifyParentIfNeeded()
	if err != nil {
		return nil, err
	}

	// As a parent, this map sets up notification callback with child value
	// so this map can be notified when child value is modified.
	//
	// Setting up notification with new child value can happen at any time
	// (either before or after this map notifies its parent) because
	// setting up notification doesn't trigger any read/write ops on parent or child.
	maxInlineSize := maxInlineMapValueSize(uint64(keyStorable.ByteSize()))
	m.setCallbackWithChild(comparator, hip, key, value, maxInlineSize)

	return existingMapValueStorable, nil
}

func (m *OrderedMap) Remove(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, Storable, error) {
	keyStorable, valueStorable, err := m.remove(comparator, hip, key)
	if err != nil {
		return nil, nil, err
	}

	// If overwritten storable is an inlined slab, uninline the slab and store it in storage.
	// This is to prevent potential data loss because the overwritten inlined slab was not in
	// storage and any future changes to it would have been lost.
	switch s := valueStorable.(type) {
	case ArraySlab:
		err = s.Uninline(m.Storage)
		if err != nil {
			return nil, nil, err
		}
		valueStorable = SlabIDStorable(s.SlabID())

	case MapSlab:
		err = s.Uninline(m.Storage)
		if err != nil {
			return nil, nil, err
		}
		valueStorable = SlabIDStorable(s.SlabID())
	}

	return keyStorable, valueStorable, nil
}

func (m *OrderedMap) remove(comparator ValueComparator, hip HashInputProvider, key Value) (Storable, Storable, error) {

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
		}
	}

	if m.root.IsFull() {
		err := m.splitRoot()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by OrderedMap.splitRoot().
			return nil, nil, err
		}
	}

	// If this map is a child, it notifies parent by invoking callback because
	// this map is changed by removing element.
	err = m.notifyParentIfNeeded()
	if err != nil {
		return nil, nil, err
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

	err = storeSlab(m.Storage, left)
	if err != nil {
		return err
	}

	err = storeSlab(m.Storage, right)
	if err != nil {
		return err
	}

	return storeSlab(m.Storage, m.root)
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

	err = storeSlab(m.Storage, m.root)
	if err != nil {
		return err
	}

	err = m.Storage.Remove(childID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to remove slab %s", childID))
	}
	return nil
}

func (m *OrderedMap) SlabID() SlabID {
	if m.root.Inlined() {
		return SlabIDUndefined
	}
	return m.root.SlabID()
}

func (m *OrderedMap) ValueID() ValueID {
	return slabIDToValueID(m.root.SlabID())
}

// Storable returns OrderedMap m as either:
// - SlabIDStorable, or
// - inlined data slab storable
func (m *OrderedMap) Storable(_ SlabStorage, _ Address, maxInlineSize uint64) (Storable, error) {

	inlined := m.root.Inlined()
	inlinable := m.root.Inlinable(maxInlineSize)

	switch {

	case inlinable && inlined:
		// Root slab is inlinable and was inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return m.root, nil

	case !inlinable && !inlined:
		// Root slab is not inlinable and was not inlined.
		// Return root slab as storable, no size adjustment and change to storage.
		return SlabIDStorable(m.SlabID()), nil

	case inlinable && !inlined:
		// Root slab is inlinable and was NOT inlined.

		// Inline root data slab.
		err := m.root.Inline(m.Storage)
		if err != nil {
			return nil, err
		}

		return m.root, nil

	case !inlinable && inlined:
		// Root slab is NOT inlinable and was inlined.

		// Uninline root slab.
		err := m.root.Uninline(m.Storage)
		if err != nil {
			return nil, err
		}

		return SlabIDStorable(m.SlabID()), nil

	default:
		panic("not reachable")
	}
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

func (m *OrderedMap) SetType(typeInfo TypeInfo) error {
	extraData := m.root.ExtraData()
	extraData.TypeInfo = typeInfo

	m.root.SetExtraData(extraData)

	if m.Inlined() {
		// Map is inlined.

		// Notify parent container so parent slab is saved in storage with updated TypeInfo of inlined array.
		return m.notifyParentIfNeeded()
	}

	// Map is standalone.

	// Store modified root slab in storage since typeInfo is part of extraData stored in root slab.
	return storeSlab(m.Storage, m.root)
}

func (m *OrderedMap) String() string {
	iterator, err := m.ReadOnlyIterator()
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

func firstMapDataSlab(storage SlabStorage, slab MapSlab) (*MapDataSlab, error) {
	switch slab := slab.(type) {
	case *MapDataSlab:
		return slab, nil

	case *MapMetaDataSlab:
		firstChildID := slab.childrenHeaders[0].slabID
		firstChild, err := getMapSlab(storage, firstChildID)
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by getMapSlab().
			return nil, err
		}
		// Don't need to wrap error as external error because err is already categorized by firstMapDataSlab().
		return firstMapDataSlab(storage, firstChild)

	default:
		return nil, NewUnreachableError()
	}
}

func (m *MapExtraData) incrementCount() {
	m.Count++
}

func (m *MapExtraData) decrementCount() {
	m.Count--
}

type mapElementIterator struct {
	storage        SlabStorage
	elements       elements
	index          int
	nestedIterator *mapElementIterator
}

func (i *mapElementIterator) next() (key MapKey, value MapValue, err error) {

	if i.nestedIterator != nil {
		key, value, err = i.nestedIterator.next()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by mapElementIterator.next().
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

		i.nestedIterator = &mapElementIterator{
			storage:  i.storage,
			elements: elems,
		}

		i.index++
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return i.nestedIterator.next()

	default:
		return nil, nil, NewSlabDataError(fmt.Errorf("unexpected element type %T during map iteration", e))
	}
}

type MapEntryIterationFunc func(Value, Value) (resume bool, err error)
type MapElementIterationFunc func(Value) (resume bool, err error)

type MapIterator interface {
	CanMutate() bool
	Next() (Value, Value, error)
	NextKey() (Value, error)
	NextValue() (Value, error)
}

type emptyMapIterator struct {
	readOnly bool
}

var _ MapIterator = &emptyMapIterator{}

var emptyMutableMapIterator = &emptyMapIterator{readOnly: false}
var emptyReadOnlyMapIterator = &emptyMapIterator{readOnly: true}

func (i *emptyMapIterator) CanMutate() bool {
	return !i.readOnly
}

func (*emptyMapIterator) Next() (Value, Value, error) {
	return nil, nil, nil
}

func (*emptyMapIterator) NextKey() (Value, error) {
	return nil, nil
}

func (*emptyMapIterator) NextValue() (Value, error) {
	return nil, nil
}

type mutableMapIterator struct {
	m          *OrderedMap
	comparator ValueComparator
	hip        HashInputProvider
	nextKey    Value
}

var _ MapIterator = &mutableMapIterator{}

func (i *mutableMapIterator) CanMutate() bool {
	return true
}

func (i *mutableMapIterator) Next() (Value, Value, error) {
	if i.nextKey == nil {
		// No more elements.
		return nil, nil, nil
	}

	// Don't need to set up notification callback for v because
	// getElementAndNextKey() returns value with notification already.
	k, v, nk, err := i.m.getElementAndNextKey(i.comparator, i.hip, i.nextKey)
	if err != nil {
		return nil, nil, err
	}

	i.nextKey = nk

	return k, v, nil
}

func (i *mutableMapIterator) NextKey() (Value, error) {
	if i.nextKey == nil {
		// No more elements.
		return nil, nil
	}

	key := i.nextKey

	nk, err := i.m.getNextKey(i.comparator, i.hip, key)
	if err != nil {
		return nil, err
	}

	i.nextKey = nk

	return key, nil
}

func (i *mutableMapIterator) NextValue() (Value, error) {
	if i.nextKey == nil {
		// No more elements.
		return nil, nil
	}

	// Don't need to set up notification callback for v because
	// getElementAndNextKey() returns value with notification already.
	_, v, nk, err := i.m.getElementAndNextKey(i.comparator, i.hip, i.nextKey)
	if err != nil {
		return nil, err
	}

	i.nextKey = nk

	return v, nil
}

type ReadOnlyMapIteratorMutationCallback func(mutatedValue Value)

type readOnlyMapIterator struct {
	m                     *OrderedMap
	nextDataSlabID        SlabID
	elemIterator          *mapElementIterator
	keyMutationCallback   ReadOnlyMapIteratorMutationCallback
	valueMutationCallback ReadOnlyMapIteratorMutationCallback
}

// defaultReadOnlyMapIteratorMutatinCallback is no-op.
var defaultReadOnlyMapIteratorMutatinCallback ReadOnlyMapIteratorMutationCallback = func(Value) {}

var _ MapIterator = &readOnlyMapIterator{}

func (i *readOnlyMapIterator) setMutationCallback(key, value Value) {
	if k, ok := key.(mutableValueNotifier); ok {
		k.setParentUpdater(func() (found bool, err error) {
			i.keyMutationCallback(key)
			return true, NewReadOnlyIteratorElementMutationError(i.m.ValueID(), k.ValueID())
		})
	}

	if v, ok := value.(mutableValueNotifier); ok {
		v.setParentUpdater(func() (found bool, err error) {
			i.valueMutationCallback(value)
			return true, NewReadOnlyIteratorElementMutationError(i.m.ValueID(), v.ValueID())
		})
	}
}

func (i *readOnlyMapIterator) Next() (key Value, value Value, err error) {
	if i.elemIterator == nil {
		if i.nextDataSlabID == SlabIDUndefined {
			return nil, nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, nil, err
		}
	}

	var ks, vs Storable
	ks, vs, err = i.elemIterator.next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, nil, err
	}
	if ks != nil {
		key, err = ks.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map key's stored value")
		}

		value, err = vs.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map value's stored value")
		}

		i.setMutationCallback(key, value)

		return key, value, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.Next().
	return i.Next()
}

func (i *readOnlyMapIterator) NextKey() (key Value, err error) {
	if i.elemIterator == nil {
		if i.nextDataSlabID == SlabIDUndefined {
			return nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, err
		}
	}

	var ks Storable
	ks, _, err = i.elemIterator.next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, err
	}
	if ks != nil {
		key, err = ks.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map key's stored value")
		}

		i.setMutationCallback(key, nil)

		return key, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.NextKey().
	return i.NextKey()
}

func (i *readOnlyMapIterator) NextValue() (value Value, err error) {
	if i.elemIterator == nil {
		if i.nextDataSlabID == SlabIDUndefined {
			return nil, nil
		}

		err = i.advance()
		if err != nil {
			// Don't need to wrap error as external error because err is already categorized by MapIterator.advance().
			return nil, err
		}
	}

	var vs Storable
	_, vs, err = i.elemIterator.next()
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by MapElementIterator.Next().
		return nil, err
	}
	if vs != nil {
		value, err = vs.StoredValue(i.m.Storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get map value's stored value")
		}

		i.setMutationCallback(nil, value)

		return value, nil
	}

	i.elemIterator = nil

	// Don't need to wrap error as external error because err is already categorized by MapIterator.NextValue().
	return i.NextValue()
}

func (i *readOnlyMapIterator) advance() error {
	slab, found, err := i.m.Storage.Retrieve(i.nextDataSlabID)
	if err != nil {
		// Wrap err as external error (if needed) because err is returned by SlabStorage interface.
		return wrapErrorfAsExternalErrorIfNeeded(err, fmt.Sprintf("failed to retrieve slab %s", i.nextDataSlabID))
	}
	if !found {
		return NewSlabNotFoundErrorf(i.nextDataSlabID, "slab not found during map iteration")
	}

	dataSlab, ok := slab.(*MapDataSlab)
	if !ok {
		return NewSlabDataErrorf("slab %s isn't MapDataSlab", i.nextDataSlabID)
	}

	i.nextDataSlabID = dataSlab.next

	i.elemIterator = &mapElementIterator{
		storage:  i.m.Storage,
		elements: dataSlab.elements,
	}

	return nil
}

func (i *readOnlyMapIterator) CanMutate() bool {
	return false
}

// Iterator returns mutable iterator for map elements.
// Mutable iterator handles:
// - indirect element mutation, such as modifying nested container
// - direct element mutation, such as overwriting existing element with new element
// Mutable iterator doesn't handle:
// - inserting new elements into the map
// - removing existing elements from the map
// NOTE: Use readonly iterator if mutation is not needed for better performance.
func (m *OrderedMap) Iterator(comparator ValueComparator, hip HashInputProvider) (MapIterator, error) {
	if m.Count() == 0 {
		return emptyMutableMapIterator, nil
	}

	keyStorable, err := firstKeyInMapSlab(m.Storage, m.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstKeyInMapSlab().
		return nil, err
	}

	if keyStorable == nil {
		// This should never happen because m.Count() > 0.
		return nil, NewSlabDataErrorf("failed to find first key in map while map count > 0")
	}

	key, err := keyStorable.StoredValue(m.Storage)
	if err != nil {
		return nil, err
	}

	return &mutableMapIterator{
		m:          m,
		comparator: comparator,
		hip:        hip,
		nextKey:    key,
	}, nil
}

// ReadOnlyIterator returns readonly iterator for map elements.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use ReadOnlyIteratorWithMutationCallback().
func (m *OrderedMap) ReadOnlyIterator() (MapIterator, error) {
	return m.ReadOnlyIteratorWithMutationCallback(nil, nil)
}

// ReadOnlyIteratorWithMutationCallback returns readonly iterator for map elements.
// keyMutatinCallback and valueMutationCallback are useful for logging, etc. with
// more context when mutation occurs.  Mutation handling here is the same with or
// without these callbacks.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback and valueMutationCallback are called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use ReadOnlyIterator().
func (m *OrderedMap) ReadOnlyIteratorWithMutationCallback(
	keyMutatinCallback ReadOnlyMapIteratorMutationCallback,
	valueMutationCallback ReadOnlyMapIteratorMutationCallback,
) (MapIterator, error) {
	if m.Count() == 0 {
		return emptyReadOnlyMapIterator, nil
	}

	dataSlab, err := firstMapDataSlab(m.Storage, m.root)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by firstMapDataSlab().
		return nil, err
	}

	if keyMutatinCallback == nil {
		keyMutatinCallback = defaultReadOnlyMapIteratorMutatinCallback
	}

	if valueMutationCallback == nil {
		valueMutationCallback = defaultReadOnlyMapIteratorMutatinCallback
	}

	return &readOnlyMapIterator{
		m:              m,
		nextDataSlabID: dataSlab.next,
		elemIterator: &mapElementIterator{
			storage:  m.Storage,
			elements: dataSlab.elements,
		},
		keyMutationCallback:   keyMutatinCallback,
		valueMutationCallback: valueMutationCallback,
	}, nil
}

func iterateMap(iterator MapIterator, fn MapEntryIterationFunc) error {
	var err error
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

func (m *OrderedMap) Iterate(comparator ValueComparator, hip HashInputProvider, fn MapEntryIterationFunc) error {
	iterator, err := m.Iterator(comparator, hip)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}
	return iterateMap(iterator, fn)
}

// IterateReadOnly iterates readonly map elements.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyWithMutationCallback().
func (m *OrderedMap) IterateReadOnly(
	fn MapEntryIterationFunc,
) error {
	return m.IterateReadOnlyWithMutationCallback(fn, nil, nil)
}

// IterateReadOnlyWithMutationCallback iterates readonly map elements.
// keyMutatinCallback and valueMutationCallback are useful for logging, etc. with
// more context when mutation occurs.  Mutation handling here is the same with or
// without these callbacks.
// If elements are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback/valueMutationCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnly().
func (m *OrderedMap) IterateReadOnlyWithMutationCallback(
	fn MapEntryIterationFunc,
	keyMutatinCallback ReadOnlyMapIteratorMutationCallback,
	valueMutationCallback ReadOnlyMapIteratorMutationCallback,
) error {
	iterator, err := m.ReadOnlyIteratorWithMutationCallback(keyMutatinCallback, valueMutationCallback)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.ReadOnlyIterator().
		return err
	}
	return iterateMap(iterator, fn)
}

func iterateMapKeys(iterator MapIterator, fn MapElementIterationFunc) error {
	var err error
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

func (m *OrderedMap) IterateKeys(comparator ValueComparator, hip HashInputProvider, fn MapElementIterationFunc) error {
	iterator, err := m.Iterator(comparator, hip)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}
	return iterateMapKeys(iterator, fn)
}

// IterateReadOnlyKeys iterates readonly map keys.
// If keys are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of key containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyKeysWithMutationCallback().
func (m *OrderedMap) IterateReadOnlyKeys(
	fn MapElementIterationFunc,
) error {
	return m.IterateReadOnlyKeysWithMutationCallback(fn, nil)
}

// IterateReadOnlyKeysWithMutationCallback iterates readonly map keys.
// keyMutatinCallback is useful for logging, etc. with more context
// when mutation occurs.  Mutation handling here is the same with or
// without this callback.
// If keys are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of key containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnlyKeys().
func (m *OrderedMap) IterateReadOnlyKeysWithMutationCallback(
	fn MapElementIterationFunc,
	keyMutatinCallback ReadOnlyMapIteratorMutationCallback,
) error {
	iterator, err := m.ReadOnlyIteratorWithMutationCallback(keyMutatinCallback, nil)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.ReadOnlyIterator().
		return err
	}
	return iterateMapKeys(iterator, fn)
}

func iterateMapValues(iterator MapIterator, fn MapElementIterationFunc) error {
	var err error
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

func (m *OrderedMap) IterateValues(comparator ValueComparator, hip HashInputProvider, fn MapElementIterationFunc) error {
	iterator, err := m.Iterator(comparator, hip)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.Iterator().
		return err
	}
	return iterateMapValues(iterator, fn)
}

// IterateReadOnlyValues iterates readonly map values.
// If values are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback is needed (e.g. for logging mutation, etc.), use IterateReadOnlyValuesWithMutationCallback().
func (m *OrderedMap) IterateReadOnlyValues(
	fn MapElementIterationFunc,
) error {
	return m.IterateReadOnlyValuesWithMutationCallback(fn, nil)
}

// IterateReadOnlyValuesWithMutationCallback iterates readonly map values.
// valueMutationCallback is useful for logging, etc. with more context
// when mutation occurs.  Mutation handling here is the same with or
// without this callback.
// If values are mutated:
// - those changes are not guaranteed to persist.
// - mutation functions of child containers return ReadOnlyIteratorElementMutationError.
// - keyMutatinCallback is called if provided
// NOTE:
// Use readonly iterator if mutation is not needed for better performance.
// If callback isn't needed, use IterateReadOnlyValues().
func (m *OrderedMap) IterateReadOnlyValuesWithMutationCallback(
	fn MapElementIterationFunc,
	valueMutationCallback ReadOnlyMapIteratorMutationCallback,
) error {
	iterator, err := m.ReadOnlyIteratorWithMutationCallback(nil, valueMutationCallback)
	if err != nil {
		// Don't need to wrap error as external error because err is already categorized by OrderedMap.ReadOnlyIterator().
		return err
	}
	return iterateMapValues(iterator, fn)
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

	inlined := m.root.Inlined()

	prefixSize := uint32(mapRootDataSlabPrefixSize)
	if inlined {
		prefixSize = uint32(inlinedMapDataSlabPrefixSize)
	}

	// Set root to empty data slab
	m.root = &MapDataSlab{
		header: MapSlabHeader{
			slabID: rootID,
			size:   prefixSize + hkeyElementsPrefixSize,
		},
		elements:  newHkeyElements(0),
		extraData: extraData,
		inlined:   inlined,
	}

	if !m.Inlined() {
		// Save root slab
		err = storeSlab(m.Storage, m.root)
		if err != nil {
			return err
		}
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

			elem, _, existingMapValueStorable, err := prevElem.Set(storage, address, digesterBuilder, digester, 0, hkey, comparator, hip, key, value)
			if err != nil {
				// Don't need to wrap error as external error because err is already categorized by element.Set().
				return nil, err
			}
			if existingMapValueStorable != nil {
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
			err = storeSlab(storage, slab)
			if err != nil {
				return nil, err
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
	err = storeSlab(storage, root)
	if err != nil {
		return nil, err
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

// ReadOnlyLoadedValueIterator returns iterator to iterate loaded map elements.
func (m *OrderedMap) ReadOnlyLoadedValueIterator() (*MapLoadedValueIterator, error) {
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

// IterateReadOnlyLoadedValues iterates loaded map values.
func (m *OrderedMap) IterateReadOnlyLoadedValues(fn MapEntryIterationFunc) error {
	iterator, err := m.ReadOnlyLoadedValueIterator()
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

func firstKeyInMapSlab(storage SlabStorage, slab MapSlab) (MapKey, error) {
	dataSlab, err := firstMapDataSlab(storage, slab)
	if err != nil {
		return nil, err
	}
	return firstKeyInElements(storage, dataSlab.elements)
}

func firstKeyInElements(storage SlabStorage, elems elements) (MapKey, error) {
	switch elements := elems.(type) {
	case *hkeyElements:
		if len(elements.elems) == 0 {
			return nil, nil
		}
		firstElem := elements.elems[0]
		return firstKeyInElement(storage, firstElem)

	case *singleElements:
		if len(elements.elems) == 0 {
			return nil, nil
		}
		firstElem := elements.elems[0]
		return firstElem.key, nil

	default:
		return nil, NewUnreachableError()
	}
}

func firstKeyInElement(storage SlabStorage, elem element) (MapKey, error) {
	switch elem := elem.(type) {
	case *singleElement:
		return elem.key, nil

	case elementGroup:
		group, err := elem.Elements(storage)
		if err != nil {
			return nil, err
		}
		return firstKeyInElements(storage, group)

	default:
		return nil, NewUnreachableError()
	}
}
