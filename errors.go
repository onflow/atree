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
func (e HashError) Unwrap() error { return e.err }

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
func (e StorageError) Unwrap() error { return e.err }

// SlabNotFoundError is usually a fatal error returned when an slab is not found
type SlabNotFoundError struct {
	storageID StorageID
	err       error
}

// NewSlabNotFoundError constructs a SlabNotFoundError
func NewSlabNotFoundError(storageID StorageID, fatal bool, err error) error {
	if fatal {
		return NewFatalError(&SlabNotFoundError{storageID: storageID, err: err})
	}
	return &SlabNotFoundError{storageID: storageID, err: err}
}

// NewSlabNotFoundErrorf constructs a new SlabNotFoundError with error formating
func NewSlabNotFoundErrorf(storageID StorageID, fatal bool, msg string, args ...interface{}) error {
	return NewSlabNotFoundError(storageID, fatal, fmt.Errorf(msg, args...))
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

// SlabError is a usually fatal error returned when something is wrong with the content or type of the slab
// you can make this a fatal error by calling Fatal()
type SlabDataError struct {
	err error
}

// NewSlabDataError constructs a SlabDataError
func NewSlabDataError(err error, fatal bool) error {
	if fatal {
		return NewFatalError(&SlabDataError{err: err})
	}
	return &SlabDataError{err: err}
}

// NewSlabDataErrorf constructs a new SlabError with error formating
func NewSlabDataErrorf(fatal bool, msg string, args ...interface{}) error {
	return NewSlabDataError(fmt.Errorf(msg, args...), fatal)
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
	return fmt.Sprintf("Encoding has failed %s", e.err.Error())
}

func (e *DecodingError) Unwrap() error { return e.err }

// DecodingError is a fatal error returned when a method is called which is not yet implemented
// this is a temporary error
type NotImplementedError struct {
	methodName string
}

// NewDecodingError constructs a DecodingError
func NewNotImplementedError(methodName string) error {
	return NewFatalError(&NotImplementedError{methodName: methodName})
}

func (e *NotImplementedError) Error() string {
	return fmt.Sprintf("method (%s) is not implemented.", e.methodName)
}
