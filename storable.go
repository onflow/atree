package main

import "fmt"

// TODO: implement Encode() and Decode()
type Storable interface {
	ByteSize() uint32
	ID() StorageID

	String() string
}

type Uint8Value uint8

func (v Uint8Value) ByteSize() uint32 {
	return 1
}

func (v Uint8Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint8Value) String() string {
	return fmt.Sprintf("%d", uint8(v))
}

type Uint16Value uint8

func (v Uint16Value) ByteSize() uint32 {
	return 2
}

func (v Uint16Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint16Value) String() string {
	return fmt.Sprintf("%d", uint16(v))
}

type Uint32Value uint32

func (v Uint32Value) ByteSize() uint32 {
	return 4
}

func (v Uint32Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint32Value) String() string {
	return fmt.Sprintf("%d", uint32(v))
}

type Uint64Value uint64

func (v Uint64Value) ByteSize() uint32 {
	return 8
}

func (v Uint64Value) ID() StorageID {
	return StorageIDUndefined
}

func (v Uint64Value) String() string {
	return fmt.Sprintf("%d", uint64(v))
}

type StorageIDValue StorageID

func (v StorageIDValue) ByteSize() uint32 {
	return 8
}

func (v StorageIDValue) ID() StorageID {
	return StorageID(v)
}

func (v StorageIDValue) String() string {
	return fmt.Sprintf("id(%d)", v)
}
