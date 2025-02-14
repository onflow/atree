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

type Storable interface {
	Encode(*Encoder) error

	ByteSize() uint32

	StoredValue(storage SlabStorage) (Value, error)

	// ChildStorables only returns child storables in this storable
	// (not recursive).  This function shouldn't load extra slabs.
	ChildStorables() []Storable
}

// ComparableStorable is an interface that supports comparison and cloning of Storable.
// This is only used for compact keys.
type ComparableStorable interface {
	Storable

	// Equal returns true if the given storable is equal to this storable.
	Equal(Storable) bool

	// Less returns true if the given storable is less than this storable.
	Less(Storable) bool

	// ID returns a unique identifier.
	ID() string

	Copy() Storable
}

// ContainerStorable is an interface that supports Storable containing other storables.
type ContainerStorable interface {
	Storable

	// HasPointer returns true if any of its child storables is SlabIDStorable
	// (references to another slab).  This function is used during encoding.
	HasPointer() bool
}

// WrapperStorable is an interface that supports storable wrapping another storable.
type WrapperStorable interface {
	Storable

	// UnwrapAtreeStorable returns innermost wrapped Storable.
	UnwrapAtreeStorable() Storable

	// WrapAtreeStorable returns a new WrapperStorable with given storable as innermost wrapped storable.
	WrapAtreeStorable(Storable) Storable
}

func hasPointer(storable Storable) bool {
	if cs, ok := storable.(ContainerStorable); ok {
		return cs.HasPointer()
	}
	return false
}

func unwrapStorable(s Storable) Storable {
	switch s := s.(type) {
	case WrapperStorable:
		return s.UnwrapAtreeStorable()
	default:
		return s
	}
}

func getLoadedValue(storage SlabStorage, storable Storable) (Value, error) {
	switch storable := storable.(type) {
	case SlabIDStorable:
		slab := storage.RetrieveIfLoaded(SlabID(storable))
		if slab == nil {
			// Skip because it references unloaded slab.
			return nil, nil
		}

		v, err := slab.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil

	case WrapperStorable:
		// Check if wrapped storable is SlabIDStorable.
		wrappedStorable := unwrapStorable(storable)

		if wrappedSlabIDStorable, isSlabIDStorable := wrappedStorable.(SlabIDStorable); isSlabIDStorable {
			slab := storage.RetrieveIfLoaded(SlabID(wrappedSlabIDStorable))
			if slab == nil {
				// Skip because it references unloaded slab.
				return nil, nil
			}
		}

		v, err := storable.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil

	default:
		v, err := storable.StoredValue(storage)
		if err != nil {
			// Wrap err as external error (if needed) because err is returned by Storable interface.
			return nil, wrapErrorfAsExternalErrorIfNeeded(err, "failed to get storable's stored value")
		}

		return v, nil
	}
}
