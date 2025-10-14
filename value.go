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

type Value interface {
	// Storable returns storable which byte size doesn't exceed the given maxInlineSize.
	// If value's storable is larger than the given maxInlineSize, caller should
	// create StorableSlab containing the storable, store StorableSlab in the given
	// storage with the given address, and return StorableSlab's SlabID as SlabIDStorable.
	Storable(_ SlabStorage, _ Address, maxInlineSize uint32) (Storable, error)
}

// WrapperValue is an interface that supports value wrapping another value.
type WrapperValue interface {
	Value

	// UnwrapAtreeValue returns innermost wrapped Value and wrapper size.
	UnwrapAtreeValue() (Value, uint32)
}

type ValueComparator func(SlabStorage, Value, Storable) (bool, error)

type StorableComparator func(Storable, Storable) bool

type parentUpdater func() (found bool, err error)

// mutableValueNotifier is an interface that allows mutable child value to notify and update parent.
type mutableValueNotifier interface {
	Value
	ValueID() ValueID
	setParentUpdater(parentUpdater)
	Inlined() bool
	// Inlinable returns true if a mutable value can be inlined by fitting within the given maxInlineSize.
	Inlinable(maxInlineSize uint32) bool
}

func unwrapValue(v Value) (Value, uint32) {
	switch v := v.(type) {
	case WrapperValue:
		return v.UnwrapAtreeValue()
	default:
		return v, 0
	}
}
