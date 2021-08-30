/*
 * Copyright 2021 Dapper Labs, Inc.  All rights reserved.
 */

package atree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

const (
	// version (1 byte) + flag (1 byte) + cbor tag number (2 bytes) + cbor array head (1 byte) +
	// cbor array head for keys (9 bytes) +
	// cbor array head for keystrings + values (9 bytes)
	basicDictDataSlabPrefixSize = 1 + 1 + 2 + 1 + 9 + 9

	cborTagDictionaryValue = 0x81
)

type HasKeyString interface {
	KeyString() string
}

func dictionaryKey(keyValue Value) string {
	hasKeyString, ok := keyValue.(HasKeyString)
	if !ok {
		panic(NewNotImplementedError("HasKeyString"))
	}
	return hasKeyString.KeyString()
}

type BasicDictDataSlab struct {
	header     MapSlabHeader
	keyStrings []string // for encoding
	keys       []Storable
	entries    map[string]Storable
}

func (a *BasicDictDataSlab) StoredValue(storage SlabStorage) (Value, error) {
	return &BasicDict{storage: storage, root: a}, nil
}

type BasicDict struct {
	storage SlabStorage
	root    *BasicDictDataSlab
}

var _ Value = &BasicDict{}

func (a *BasicDict) Storable(_ SlabStorage, _ Address, _ uint64) (Storable, error) {
	return a.root, nil
}

func NewBasicDictDataSlab(storage SlabStorage, address Address) *BasicDictDataSlab {
	sID, err := storage.GenerateStorageID(address)
	if err != nil {
		panic(err)
	}
	return &BasicDictDataSlab{
		header: MapSlabHeader{
			id:   sID,
			size: basicDictDataSlabPrefixSize,
		},
		entries: make(map[string]Storable),
	}
}

func newBasicDictDataSlabFromData(
	id StorageID,
	data []byte,
	decMode cbor.DecMode,
	decodeStorable StorableDecoder,
) (
	*BasicDictDataSlab,
	error,
) {
	if len(data) < basicDictDataSlabPrefixSize {
		return nil, errors.New("data is too short for basic dict")
	}

	// Check flag
	flag := data[1]
	if getSlabMapType(flag) != slabBasicDictionary {
		return nil, fmt.Errorf("data has invalid flag 0x%x, want 0x%x", flag, slabBasicDictionary)
	}

	cborDec := decMode.NewByteStreamDecoder(data[versionAndFlagSize:])

	tagNumber, err := cborDec.DecodeTagNumber()
	if err != nil {
		return nil, err
	}

	if tagNumber != cborTagDictionaryValue {
		return nil, NewDecodingError(fmt.Errorf("data has invalid tag number 0x%x, want 0x%x", tagNumber, cborTagDictionaryValue))
	}

	elemCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, err
	}

	if elemCount != 2 {
		return nil, NewDecodingError(fmt.Errorf("data has invalid array length 0x%x, want 2", elemCount))
	}

	// Decode keys
	keyCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, err
	}

	keys := make([]Storable, keyCount)
	for i := 0; i < int(keyCount); i++ {
		storable, err := decodeStorable(cborDec, StorageIDUndefined)
		if err != nil {
			return nil, err
		}
		keys[i] = storable
	}

	valueCount, err := cborDec.DecodeArrayHead()
	if err != nil {
		return nil, err
	}

	if valueCount != keyCount*2 {
		return nil, NewDecodingError(fmt.Errorf("invalid value count %d, want %d", valueCount, keyCount*2))
	}

	// Decode values
	keyStrings := make([]string, keyCount)
	entries := make(map[string]Storable, valueCount)
	for i := 0; i < int(valueCount); i += 2 {
		keyString, err := cborDec.DecodeString()
		if err != nil {
			return nil, err
		}
		keyStrings[i/2] = keyString

		storable, err := decodeStorable(cborDec, StorageIDUndefined)
		if err != nil {
			return nil, err
		}
		entries[keyString] = storable
	}

	return &BasicDictDataSlab{
		header:     MapSlabHeader{id: id, size: uint32(len(data)), firstKey: 0},
		keyStrings: keyStrings,
		keys:       keys,
		entries:    entries,
	}, nil
}

// Encode encodes BasicDictDataSlab as
// cbor.Tag{
//			Number: cborTagDictionaryValue,
//			Content: cborArray{
//				encodedDictionaryKeys:          []interface{}(keys),
//				encodedDictionaryValues:        []interface{}(keyString + value),
//			},
// }
func (a *BasicDictDataSlab) Encode(enc *Encoder) error {

	flag := maskSlabRoot | maskSlabAnySize | maskBasicDictionary

	// Encode version
	enc.Scratch[0] = 0x0

	// Encode flag
	enc.Scratch[1] = flag

	// Encode CBOR tag number cborTagDictionaryValue
	enc.Scratch[2] = 0xd8
	enc.Scratch[3] = cborTagDictionaryValue

	// Encode CBOR array head of 2 elements
	enc.Scratch[4] = 0x82

	// Encode CBOR array size for 9 bytes
	enc.Scratch[5] = 0x80 | 27
	binary.BigEndian.PutUint64(enc.Scratch[6:], uint64(len(a.keys)))

	_, err := enc.Write(enc.Scratch[:14])
	if err != nil {
		return err
	}

	// Encode keys
	for i := 0; i < len(a.keys); i++ {
		err := a.keys[i].Encode(enc)
		if err != nil {
			return err
		}
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return err
	}

	// Encode CBOR array size for 9 bytes
	enc.Scratch[0] = 0x80 | 27
	binary.BigEndian.PutUint64(enc.Scratch[1:], uint64(len(a.entries)*2))

	_, err = enc.Write(enc.Scratch[:9])
	if err != nil {
		return err
	}

	// Encode values
	for _, keyString := range a.keyStrings {
		// Encode key string

		// Encode CBOR text string size for 9 bytes
		enc.Scratch[0] = 0x60 | 27
		binary.BigEndian.PutUint64(enc.Scratch[1:], uint64(len(keyString)))

		_, err = enc.Write(enc.Scratch[:9])
		if err != nil {
			return err
		}

		_, err := enc.Write([]byte(keyString))
		if err != nil {
			return err
		}

		// Encode value
		value, ok := a.entries[keyString]
		if !ok {
			return NewEncodingError(NewKeyNotFoundError(keyString))
		}
		err = value.Encode(enc)
		if err != nil {
			return err
		}

		// Flush
		err = enc.CBOR.Flush()
		if err != nil {
			return err
		}
	}

	err = enc.CBOR.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (m *BasicDictDataSlab) Get(_ SlabStorage, keyValue Value) (Storable, error) {
	key := dictionaryKey(keyValue)

	value, ok := m.entries[key]

	if ok {
		return value, nil
	}

	return nil, &KeyNotFoundError{}
}

func (a *BasicDictDataSlab) Set(storage SlabStorage, address Address, keyValue Value, value Storable) error {

	keyString := dictionaryKey(keyValue)

	oldValue, ok := a.entries[keyString]

	if ok {

		// Replace existing value
		a.entries[keyString] = value

		// Adjust slab size
		a.header.size = a.header.size - oldValue.ByteSize() + value.ByteSize()

	} else {

		// Append new value (always inline large keys)
		key, err := keyValue.Storable(storage, address, math.MaxUint64)
		if err != nil {
			return err
		}

		a.keyStrings = append(a.keyStrings, keyString)
		a.keys = append(a.keys, key)
		a.entries[keyString] = value

		// Adjust slab size
		a.header.size += 9 + uint32(len(keyString)) + key.ByteSize() + value.ByteSize()
	}

	err := storage.Store(a.header.id, a)
	if err != nil {
		return err
	}

	return nil
}

func (a *BasicDictDataSlab) Remove(storage SlabStorage, keyValue Value) (Storable, error) {

	keyString := dictionaryKey(keyValue)

	oldValue, ok := a.entries[keyString]

	if !ok {
		return nil, NewKeyNotFoundError(keyValue)
	}

	for i, ks := range a.keyStrings {
		if ks == keyString {
			oldKey := a.keys[i]
			oldKeyString := a.keyStrings[i]

			// Remove from keyStrings
			copy(a.keyStrings[i:], a.keyStrings[i+1:])
			a.keyStrings = a.keyStrings[:len(a.keyStrings)-1]

			// Remove from keys
			copy(a.keys[i:], a.keys[i+1:])
			a.keys = a.keys[:len(a.keys)-1]

			// Remove from entries
			delete(a.entries, keyString)

			// Adjust size
			a.header.size -= 9 + uint32(len(oldKeyString)) + oldKey.ByteSize() + oldValue.ByteSize()

			// Store slab
			err := storage.Store(a.header.id, a)
			if err != nil {
				return nil, err
			}

			return oldValue, nil
		}
	}

	panic("shouldn't reach here")
}

func (a *BasicDictDataSlab) Count() uint64 {
	return uint64(len(a.keys))
}

func (a *BasicDictDataSlab) Header() MapSlabHeader {
	return a.header
}

func (a *BasicDictDataSlab) ByteSize() uint32 {
	return a.header.size
}

func (a *BasicDictDataSlab) ID() StorageID {
	return a.header.id
}

func (a *BasicDictDataSlab) String() string {
	var s []string
	for k, v := range a.entries {
		s = append(s, fmt.Sprintf("%s:%s", k, v))
	}
	return "[" + strings.Join(s, " ") + "]"
}

func (a *BasicDictDataSlab) Split(_ SlabStorage) (Slab, Slab, error) {
	return nil, nil, errors.New("not applicable")
}

func (a *BasicDictDataSlab) Merge(_ Slab) error {
	return errors.New("not applicable")
}

func (a *BasicDictDataSlab) LendToRight(_ Slab) error {
	return errors.New("not applicable")
}

func (a *BasicDictDataSlab) BorrowFromRight(_ Slab) error {
	return errors.New("not applicable")
}

func NewBasicDict(storage SlabStorage, address Address) *BasicDict {
	return &BasicDict{
		storage: storage,
		root:    NewBasicDictDataSlab(storage, address),
	}
}

func (a *BasicDict) StorageID() StorageID {
	return a.root.ID()
}

func (a *BasicDict) Address() Address {
	return a.StorageID().Address
}

func NewBasicDictWithRootID(storage SlabStorage, id StorageID) (*BasicDict, error) {
	if id == StorageIDUndefined {
		return nil, fmt.Errorf("invalid storage id")
	}
	slab, found, err := storage.Retrieve(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("slab %d not found", id)
	}
	dataSlab, ok := slab.(*BasicDictDataSlab)
	if !ok {
		return nil, fmt.Errorf("slab %d is not BasicDictDataSlab", id)
	}
	return &BasicDict{storage: storage, root: dataSlab}, nil
}

func (a *BasicDict) Get(key Value) (Value, error) {
	storable, err := a.root.Get(a.storage, key)
	if err != nil {
		return nil, err
	}
	return storable.StoredValue(a.storage)
}

func (a *BasicDict) Set(key Value, v Value) error {
	// don't inline large value
	storable, err := v.Storable(a.storage, a.Address(), math.MaxUint64)
	if err != nil {
		return err
	}
	return a.root.Set(a.storage, a.Address(), key, storable)
}

func (a *BasicDict) Remove(key Value) (Value, error) {
	storable, err := a.root.Remove(a.storage, key)
	if err != nil {
		return nil, err
	}
	return storable.StoredValue(a.storage)
}

func (a *BasicDict) Count() uint64 {
	return a.root.Count()
}

func (a *BasicDict) String() string {
	return a.root.String()
}
