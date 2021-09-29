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

import (
	"fmt"
)

type FatalError struct {
	err error
}

func NewFatalError(err error) error {
	return &FatalError{err: err}
}

func (e *FatalError) Error() string {
	return fmt.Sprintf("fatal error: %s", e.err.Error())
}

func (e *FatalError) Unwrap() error { return e.err }

// IndexOutOfBoundsError is returned when an insert or delete operation is attempted on an array index which is out of bounds
type IndexOutOfBoundsError struct {
	index uint64
	min   uint64
	max   uint64
}

// NewIndexOutOfBoundsError constructs a IndexOutOfBoundsError
func NewIndexOutOfBoundsError(index, min, max uint64) *IndexOutOfBoundsError {
	return &IndexOutOfBoundsError{index: index, min: min, max: max}
}

func (e *IndexOutOfBoundsError) Error() string {
	return fmt.Sprintf("the given index %d is not in the acceptable range (%d-%d)", e.index, e.min, e.max)
}

// MaxArraySizeError is returned when an insert or delete operation is attempted on an array which has reached maximum size
type MaxArraySizeError struct {
	maxLen uint64
}

// NewMaxArraySizeError constructs a MaxArraySizeError
func NewMaxArraySizeError(maxLen uint64) *MaxArraySizeError {
	return &MaxArraySizeError{maxLen: maxLen}
}

func (e *MaxArraySizeError) Error() string {
	return fmt.Sprintf("array has reach its maximum number of elements %d", e.maxLen)
}

func (e *MaxArraySizeError) Fatal() error {
	return NewFatalError(e)
}

// NotValueError is returned when we try to create Value objects from non-root slabs.
type NotValueError struct {
}

// NewNotValueError constructs a NotValueError.
func NewNotValueError() *NotValueError {
	return &NotValueError{}
}

func (e *NotValueError) Error() string {
	return "non-root slabs must not be used to create Value object"
}

// NonStorableElementError is returned when we try to store a non-storable element.
type NonStorableElementError struct {
	element interface{}
}

// NonStorableElementError constructs a NonStorableElementError
func NewNonStorableElementError(element interface{}) *NonStorableElementError {
	return &NonStorableElementError{element: element}
}

func (e *NonStorableElementError) Error() string {
	return fmt.Sprintf("a non-storable element of type %T found when storing object", e.element)
}

// MaxKeySizeError is returned when a dictionary key is too large
type MaxKeySizeError struct {
	keyStr     string
	maxKeySize uint64
}

// NewMaxKeySizeError constructs a MaxKeySizeError
func NewMaxKeySizeError(keyStr string, maxKeySize uint64) *MaxKeySizeError {
	return &MaxKeySizeError{keyStr: keyStr, maxKeySize: maxKeySize}
}

func (e *MaxKeySizeError) Error() string {
	return fmt.Sprintf("the given key (%s) is larger than the maximum limit (%d)", e.keyStr, e.maxKeySize)
}

// KeyNotFoundError is returned when the key not found in the dictionary
type KeyNotFoundError struct {
	key interface{}
}

// NewMaxKeySizeError constructs a KeyNotFoundError
func NewKeyNotFoundError(key interface{}) *KeyNotFoundError {
	return &KeyNotFoundError{key: key}
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key (%s) not found", e.key)
}

// HashError is a fatal error returned when hash calculation fails
type HashError struct {
	err error
}

// NewHashError constructs a HashError
func NewHashError(err error) error {
	return NewFatalError(&HashError{err: err})
}

func (e *HashError) Error() string {
	return fmt.Sprintf("atree hasher failed: %s", e.err.Error())
}

// Unwrap returns the wrapped err
func (e *HashError) Unwrap() error { return e.err }

// StorageError is always a fatal error returned when storage fails
type StorageError struct {
	err error
}

// NewStorageError constructs a StorageError
func NewStorageError(err error) error {
	return NewFatalError(&StorageError{err: err})
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("storage failed: %s", e.err.Error())
}

// Unwrap returns the wrapped err
func (e *StorageError) Unwrap() error { return e.err }

// SlabNotFoundError is always a fatal error returned when an slab is not found
type SlabNotFoundError struct {
	storageID StorageID
	err       error
}

// NewSlabNotFoundError constructs a SlabNotFoundError
func NewSlabNotFoundError(storageID StorageID, err error) error {
	return NewFatalError(&SlabNotFoundError{storageID: storageID, err: err})
}

// NewSlabNotFoundErrorf constructs a new SlabNotFoundError with error formating
func NewSlabNotFoundErrorf(storageID StorageID, msg string, args ...interface{}) error {
	return NewSlabNotFoundError(storageID, fmt.Errorf(msg, args...))
}

func (e *SlabNotFoundError) Error() string {
	return fmt.Sprintf("slab with the given storageID (%s) not found. %s", e.storageID.String(), e.err.Error())
}

// Unwrap returns the wrapped err
func (e *SlabNotFoundError) Unwrap() error { return e.err }

// SlabSplitError is alwyas a fatal error returned when splitting an slab has failed
type SlabSplitError struct {
	err error
}

// NewSlabSplitError constructs a SlabSplitError
func NewSlabSplitError(err error) error {
	return NewFatalError(&SlabSplitError{err: err})
}

// NewSlabSplitErrorf constructs a new SlabSplitError with error formating
func NewSlabSplitErrorf(msg string, args ...interface{}) error {
	return NewSlabSplitError(fmt.Errorf(msg, args...))
}

func (e *SlabSplitError) Error() string {
	return fmt.Sprintf("slab can not split. %s", e.err.Error())
}

func (e *SlabSplitError) Unwrap() error { return e.err }

// SlabMergeError is always a fatal error returned when merging two slabs fails
type SlabMergeError struct {
	err error
}

// NewSlabMergeError constructs a SlabMergeError
func NewSlabMergeError(err error) error {
	return NewFatalError(&SlabMergeError{err: err})
}

// NewSlabMergeErrorf constructs a new SlabMergeError with error formating
func NewSlabMergeErrorf(msg string, args ...interface{}) error {
	return NewSlabMergeError(fmt.Errorf(msg, args...))
}

func (e *SlabMergeError) Error() string {
	return fmt.Sprintf("cannot merge slabs: %s", e.err.Error())
}

func (e *SlabMergeError) Unwrap() error { return e.err }

// SlabRebalanceError is alwyas a fatal error returned when rebalancing a slab has failed
type SlabRebalanceError struct {
	err error
}

// NewSlabRebalanceError constructs a SlabRebalanceError
func NewSlabRebalanceError(err error) error {
	return NewFatalError(&SlabRebalanceError{err: err})
}

// NewSlabErrorf constructs a new SlabError with error formating
func NewSlabRebalanceErrorf(msg string, args ...interface{}) error {
	return NewSlabRebalanceError(fmt.Errorf(msg, args...))
}

func (e *SlabRebalanceError) Error() string {
	return fmt.Sprintf("slab rebalancing error: %s", e.err.Error())
}

func (e *SlabRebalanceError) Unwrap() error { return e.err }

// SlabError is a always fatal error returned when something is wrong with the content or type of the slab
// you can make this a fatal error by calling Fatal()
type SlabDataError struct {
	err error
}

// NewSlabDataError constructs a SlabDataError
func NewSlabDataError(err error) error {
	return NewFatalError(&SlabDataError{err: err})
}

// NewSlabDataErrorf constructs a new SlabError with error formating
func NewSlabDataErrorf(msg string, args ...interface{}) error {
	return NewSlabDataError(fmt.Errorf(msg, args...))
}

func (e *SlabDataError) Error() string {
	return fmt.Sprintf("slab data error: %s", e.err.Error())
}

func (e *SlabDataError) Unwrap() error { return e.err }

// EncodingError is a fatal error returned when a encoding operation fails
type EncodingError struct {
	err error
}

// NewEncodingError constructs a EncodingError
func NewEncodingError(err error) error {
	return NewFatalError(&EncodingError{err: err})
}

// NewEncodingErrorf constructs a new EncodingError with error formating
func NewEncodingErrorf(msg string, args ...interface{}) error {
	return NewEncodingError(fmt.Errorf(msg, args...))
}

func (e *EncodingError) Error() string {
	return fmt.Sprintf("Encoding has failed %s", e.err.Error())
}

func (e *EncodingError) Unwrap() error { return e.err }

// DecodingError is a fatal error returned when a decoding operation fails
type DecodingError struct {
	err error
}

// NewDecodingError constructs a DecodingError
func NewDecodingError(err error) error {
	return NewFatalError(&DecodingError{err: err})
}

// NewDecodingErrorf constructs a new DecodingError with error formating
func NewDecodingErrorf(msg string, args ...interface{}) error {
	return NewDecodingError(fmt.Errorf(msg, args...))
}

func (e *DecodingError) Error() string {
	return fmt.Sprintf("Decoding has failed %s", e.err.Error())
}

func (e *DecodingError) Unwrap() error { return e.err }

// NotImplementedError is a fatal error returned when a method is called which is not yet implemented
// this is a temporary error
type NotImplementedError struct {
	methodName string
}

// NewNotImplementedError constructs a NotImplementedError
func NewNotImplementedError(methodName string) error {
	return NewFatalError(&NotImplementedError{methodName: methodName})
}

func (e *NotImplementedError) Error() string {
	return fmt.Sprintf("method (%s) is not implemented.", e.methodName)
}

// InterfaceNotImplementedError is a fatal error returned when an interface is not implemented.
type InterfaceNotImplementedError struct {
	interfaceName string
}

// NewInterfaceNotImplementedError constructs a InterfaceNotImplementedError
func NewInterfaceNotImplementedError(interfaceName string) error {
	return NewFatalError(&InterfaceNotImplementedError{interfaceName: interfaceName})
}

func (e *InterfaceNotImplementedError) Error() string {
	return fmt.Sprintf("interface (%s) is not implemented.", e.interfaceName)
}

// HashLevelError is a fatal error returned when hash level is wrong.
type HashLevelError struct {
	msg string
}

// NewHashLevelError constructs a HashLevelError
func NewHashLevelErrorf(msg string, args ...interface{}) error {
	return NewFatalError(&HashLevelError{msg: fmt.Sprintf(msg, args...)})
}

func (e *HashLevelError) Error() string {
	return fmt.Sprintf("atree hasher level failed: %s", e.msg)
}

// NotApplicableError is a fatal error returned when a not applicable method is called
type NotApplicableError struct {
	methodName string
}

// NewNotApplicableError constructs a NotImplementedError
func NewNotApplicableError(methodName string) error {
	return NewFatalError(&NotImplementedError{methodName: methodName})
}

func (e *NotApplicableError) Error() string {
	return fmt.Sprintf("method (%s) is not applicable.", e.methodName)
}

// TypeAssertionError is a fatal error returned when an object can't be type asserted to an expected type.
type TypeAssertionError struct {
	wantType string
	gotType  string
}

// NewTypeAssertionError constructs a TypeAssertionError.
func NewTypeAssertionError(wantType string, gotType string) error {
	return NewFatalError(&TypeAssertionError{wantType: wantType, gotType: gotType})
}

func (e *TypeAssertionError) Error() string {
	return fmt.Sprintf("type assertion failed: want %s, got %s", e.wantType, e.gotType)
}
