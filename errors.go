package atree

import "fmt"

type Error interface {
	// returns true if the error is fatal
	IsFatal() bool
	// and anything else that is needed to be an error
	error
}

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

// IsFatal returns true if the error is fatal
func (e *IndexOutOfBoundsError) IsFatal() bool {
	return false
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

// IsFatal returns true if the error is fatal
func (e *MaxArraySizeError) IsFatal() bool {
	return false
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

// IsFatal returns true if the error is fatal
func (e *NonStorableElementError) IsFatal() bool {
	return false
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

// IsFatal returns true if the error is fatal
func (e *MaxKeySizeError) IsFatal() bool {
	return false
}

// HashError is a fatal error returned when hash calculation fails
type HashError struct {
	err error
}

// NewHashError constructs a HashError
func NewHashError(err error) *HashError {
	return &HashError{err: err}
}

func (e *HashError) Error() string {
	return fmt.Sprintf("atree hasher failed: %s", e.err.Error())
}

// IsFatal returns true if the error is fatal
func (e *HashError) IsFatal() bool {
	return true
}

// Unwrap returns the wrapped err
func (e HashError) Unwrap() error {
	return e.err
}

// StorageError is a fatal error returned when storage fails
type StorageError struct {
	err error
}

// NewStorageError constructs a StorageError
func NewStorageError(err error) *StorageError {
	return &StorageError{err: err}
}

func (e *StorageError) Error() string {
	return fmt.Sprintf("storage failed: %s", e.err.Error())
}

// IsFatal returns true if the error is fatal
func (e *StorageError) IsFatal() bool {
	return true
}

// Unwrap returns the wrapped err
func (e StorageError) Unwrap() error {
	return e.err
}

// SlabNotFoundError is a fatal error returned when an slab is not found
type SlabNotFoundError struct {
	storageID StorageID
}

// NewSlabNotFoundError constructs a SlabNotFoundError
func NewSlabNotFoundError(storageID StorageID) *SlabNotFoundError {
	return &SlabNotFoundError{storageID: storageID}
}

func (e *SlabNotFoundError) Error() string {
	return fmt.Sprintf("slab with the given storageID (%s) not found", e.storageID.String())
}

// IsFatal returns true if the error is fatal
func (e *SlabNotFoundError) IsFatal() bool {
	return true
}

// WrongSlabTypeFoundError is a fatal error returned when an slab is loaded but has an unexpected type
type WrongSlabTypeFoundError struct {
	storageID StorageID
}

// NewWrongSlabTypeFoundError constructs a WrongSlabTypeFoundError
func NewWrongSlabTypeFoundError(storageID StorageID) *WrongSlabTypeFoundError {
	return &WrongSlabTypeFoundError{storageID: storageID}
}

func (e *WrongSlabTypeFoundError) Error() string {
	return fmt.Sprintf("slab with the given storageID (%s) has a wrong type", e.storageID.String())
}

// IsFatal returns true if the error is fatal
func (e *WrongSlabTypeFoundError) IsFatal() bool {
	return true
}

// DigestLevelNotMatchError is a fatal error returned when a digest level in the dictionary is not matched
type DigestLevelNotMatchError struct {
	got      uint8
	expected uint8
}

// NewDigestLevelNotMatchError constructs a DigestLevelNotMatchError
func NewDigestLevelNotMatchError(got, expected uint8) *DigestLevelNotMatchError {
	return &DigestLevelNotMatchError{got: got, expected: expected}
}

func (e *DigestLevelNotMatchError) Error() string {
	return fmt.Sprintf("got digest level of %d but was expecting %d", e.got, e.expected)
}

// IsFatal returns true if the error is fatal
func (e *DigestLevelNotMatchError) IsFatal() bool {
	return true
}

// EncodingError is a fatal error returned when a encoding operation fails
type EncodingError struct {
	err error
}

// NewEncodingError constructs a EncodingError
func NewEncodingError(err error) *EncodingError {
	return &EncodingError{err: err}
}

func (e *EncodingError) Error() string {
	return fmt.Sprintf("Encoding has failed %s", e.err.Error())
}

// IsFatal returns true if the error is fatal
func (e *EncodingError) IsFatal() bool {
	return true
}

// DecodingError is a fatal error returned when a decoding operation fails
type DecodingError struct {
	err error
}

// NewDecodingError constructs a DigestLevelNotMatchError
func NewDecodingError(err error) *DecodingError {
	return &DecodingError{err: err}
}

func (e *DecodingError) Error() string {
	return fmt.Sprintf("Encoding has failed %s", e.err.Error())
}

// IsFatal returns true if the error is fatal
func (e *DecodingError) IsFatal() bool {
	return true
}
