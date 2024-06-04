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
	"errors"
	"fmt"
	"runtime/debug"
)

type ExternalError struct {
	msg string
	err error
}

func NewExternalError(err error, msg string) error {
	return &ExternalError{msg: msg, err: err}
}

func (e *ExternalError) Error() string {
	if e.msg == "" {
		return e.err.Error()
	}
	return fmt.Sprintf("%s: %s", e.msg, e.err.Error())
}

func (e *ExternalError) Unwrap() error {
	return e.err
}

type UserError struct {
	err error
}

func NewUserError(err error) error {
	return &UserError{err: err}
}

func (e *UserError) Error() string {
	return e.err.Error()
}

func (e *UserError) Unwrap() error {
	return e.err
}

type FatalError struct {
	err error
}

func NewFatalError(err error) error {
	return &FatalError{err: err}
}

func (e *FatalError) Error() string {
	return e.err.Error()
}

func (e *FatalError) Unwrap() error {
	return e.err
}

// SliceOutOfBoundsError is returned when index for array slice is out of bounds.
type SliceOutOfBoundsError struct {
	startIndex uint64
	endIndex   uint64
	min        uint64
	max        uint64
}

// NewSliceOutOfBoundsError constructs a SliceOutOfBoundsError.
func NewSliceOutOfBoundsError(startIndex, endIndex, min, max uint64) error {
	return NewUserError(
		&SliceOutOfBoundsError{
			startIndex: startIndex,
			endIndex:   endIndex,
			min:        min,
			max:        max,
		},
	)
}

func (e *SliceOutOfBoundsError) Error() string {
	return fmt.Sprintf("slice [%d:%d] is out of bounds with range %d-%d", e.startIndex, e.endIndex, e.min, e.max)
}

// InvalidSliceIndexError is returned when array slice index is invalid, such as startIndex > endIndex
// This error can be returned even when startIndex and endIndex are both within bounds.
type InvalidSliceIndexError struct {
	startIndex uint64
	endIndex   uint64
}

// NewInvalidSliceIndexError constructs an InvalidSliceIndexError
func NewInvalidSliceIndexError(startIndex, endIndex uint64) error {
	return NewUserError(
		&InvalidSliceIndexError{
			startIndex: startIndex,
			endIndex:   endIndex,
		},
	)
}

func (e *InvalidSliceIndexError) Error() string {
	return fmt.Sprintf("invalid slice index: %d > %d", e.startIndex, e.endIndex)
}

// IndexOutOfBoundsError is returned when get, insert or delete operation is attempted on an array index which is out of bounds
type IndexOutOfBoundsError struct {
	index uint64
	min   uint64
	max   uint64
}

// NewIndexOutOfBoundsError constructs a IndexOutOfBoundsError
func NewIndexOutOfBoundsError(index, min, max uint64) error {
	return NewUserError(
		&IndexOutOfBoundsError{
			index: index,
			min:   min,
			max:   max,
		},
	)
}

func (e *IndexOutOfBoundsError) Error() string {
	return fmt.Sprintf("index %d is outside required range (%d-%d)", e.index, e.min, e.max)
}

// NotValueError is returned when we try to create Value objects from non-root slabs.
type NotValueError struct {
	id SlabID
}

// NewNotValueError constructs a NotValueError.
func NewNotValueError(id SlabID) error {
	return NewFatalError(&NotValueError{id: id})
}

func (e *NotValueError) Error() string {
	return fmt.Sprintf("slab (%s) cannot be used to create Value object", e.id)
}

// DuplicateKeyError is returned when the duplicate key is found in the dictionary when none is expected.
type DuplicateKeyError struct {
	key interface{}
}

func NewDuplicateKeyError(key interface{}) error {
	return NewFatalError(&DuplicateKeyError{key: key})
}

func (e *DuplicateKeyError) Error() string {
	return fmt.Sprintf("duplicate key (%s)", e.key)
}

// KeyNotFoundError is returned when the key not found in the dictionary
type KeyNotFoundError struct {
	key interface{}
}

// NewKeyNotFoundError constructs a KeyNotFoundError
func NewKeyNotFoundError(key interface{}) error {
	return NewUserError(&KeyNotFoundError{key: key})
}

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("key (%s) not found", e.key)
}

// HashSeedUninitializedError is a fatal error returned when hash seed is uninitialized.
type HashSeedUninitializedError struct {
}

func NewHashSeedUninitializedError() error {
	return NewFatalError(&HashSeedUninitializedError{})
}

func (e *HashSeedUninitializedError) Error() string {
	return "uninitialized hash seed"
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
	return fmt.Sprintf("hasher error: %s", e.err.Error())
}

// SlabIDError is returned when slab id can't be created or it's invalid.
type SlabIDError struct {
	msg string
}

// NewSlabIDError constructs a fatal error of SlabIDError.
func NewSlabIDError(msg string) error {
	return NewFatalError(&SlabIDError{msg: msg})
}

// NewSlabIDErrorf constructs a fatal error of SlabIDError.
func NewSlabIDErrorf(msg string, args ...interface{}) error {
	return NewSlabIDError(fmt.Sprintf(msg, args...))
}

func (e *SlabIDError) Error() string {
	return fmt.Sprintf("slab id error: %s", e.msg)
}

// SlabNotFoundError is always a fatal error returned when an slab is not found
type SlabNotFoundError struct {
	slabID SlabID
	err    error
}

// NewSlabNotFoundError constructs a SlabNotFoundError
func NewSlabNotFoundError(slabID SlabID, err error) error {
	return NewFatalError(&SlabNotFoundError{slabID: slabID, err: err})
}

// NewSlabNotFoundErrorf constructs a new SlabNotFoundError with error formating
func NewSlabNotFoundErrorf(slabID SlabID, msg string, args ...interface{}) error {
	return NewSlabNotFoundError(slabID, fmt.Errorf(msg, args...))
}

func (e *SlabNotFoundError) Error() string {
	return fmt.Sprintf("slab (%s) not found: %s", e.slabID.String(), e.err.Error())
}

// SlabSplitError is always a fatal error returned when splitting an slab has failed
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
	return fmt.Sprintf("slab failed to split: %s", e.err.Error())
}

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
	return fmt.Sprintf("slabs failed to merge: %s", e.err.Error())
}

// SlabRebalanceError is always a fatal error returned when rebalancing a slab has failed
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
	return fmt.Sprintf("slabs failed to rebalance: %s", e.err.Error())
}

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
	return fmt.Sprintf("encoding error: %s", e.err.Error())
}

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
	return fmt.Sprintf("decoding error: %s", e.err.Error())
}

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

// HashLevelError is a fatal error returned when hash level is wrong.
type HashLevelError struct {
	msg string
}

// NewHashLevelError constructs a HashLevelError
func NewHashLevelErrorf(msg string, args ...interface{}) error {
	return NewFatalError(&HashLevelError{msg: fmt.Sprintf(msg, args...)})
}

func (e *HashLevelError) Error() string {
	return fmt.Sprintf("atree hash level error: %s", e.msg)
}

// NotApplicableError is a fatal error returned when a not applicable method is called
type NotApplicableError struct {
	typeName, interfaceName, methodName string
}

// NewNotApplicableError constructs a NotImplementedError
func NewNotApplicableError(typeName, interfaceName, methodName string) error {
	return NewFatalError(
		&NotApplicableError{
			typeName:      typeName,
			interfaceName: interfaceName,
			methodName:    methodName,
		})
}

func (e *NotApplicableError) Error() string {
	return fmt.Sprintf("%s.%s is not applicable for type %s", e.interfaceName, e.methodName, e.typeName)
}

// UnreachableError is used by panic when unreachable code is reached.
// This is copied from Cadence.
type UnreachableError struct {
	Stack []byte
}

func NewUnreachableError() error {
	return NewFatalError(&UnreachableError{Stack: debug.Stack()})
}

func (e UnreachableError) Error() string {
	return fmt.Sprintf("unreachable\n%s", e.Stack)
}

// CollisionLimitError is a fatal error returned when a noncryptographic hash collision
// would exceed collision limit (per digest per map) we enforce in the first level.
type CollisionLimitError struct {
	collisionLimitPerDigest uint32 // limit <= 255 is recommended, larger values are useful for tests
}

// NewCollisionLimitError constructs a CollisionLimitError
func NewCollisionLimitError(collisionLimitPerDigest uint32) error {
	return NewFatalError(&CollisionLimitError{collisionLimitPerDigest: collisionLimitPerDigest})
}

func (e *CollisionLimitError) Error() string {
	return fmt.Sprintf("collision limit per digest %d already reached", e.collisionLimitPerDigest)
}

// MapElementCountError is a fatal error returned when element count is unexpected.
// It is an implemtation error.
type MapElementCountError struct {
	msg string
}

// NewMapElementCountError constructs a MapElementCountError.
func NewMapElementCountError(msg string) error {
	return NewFatalError(&MapElementCountError{msg: msg})
}

func (e *MapElementCountError) Error() string {
	return e.msg
}

func wrapErrorAsExternalErrorIfNeeded(err error) error {
	return wrapErrorfAsExternalErrorIfNeeded(err, "")
}

func wrapErrorfAsExternalErrorIfNeeded(err error, msg string) error {
	if err == nil {
		return nil
	}

	var userError *UserError
	var fatalError *FatalError
	var externalError *ExternalError

	if errors.As(err, &userError) ||
		errors.As(err, &fatalError) ||
		errors.As(err, &externalError) {
		// No-op if err is already categorized.
		return err
	}

	// Create new external error wrapping err with context.
	return NewExternalError(err, msg)
}
