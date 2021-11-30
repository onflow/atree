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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsRootOfAnObject(t *testing.T) {
	// We just need first 2 bytes of slab data to test.
	testCases := []struct {
		name   string
		isRoot bool
		data   []byte
	}{
		{name: "array data as root", isRoot: true, data: []byte{0x00, 0x80}},
		{name: "array metadata as root", isRoot: true, data: []byte{0x00, 0x81}},
		{name: "map data as root", isRoot: true, data: []byte{0x00, 0x88}},
		{name: "map metadata as root", isRoot: true, data: []byte{0x00, 0x89}},
		{name: "array data as non-root", isRoot: false, data: []byte{0x00, 0x00}},
		{name: "array metadata as non-root", isRoot: false, data: []byte{0x00, 0x01}},
		{name: "map data as non-root", isRoot: false, data: []byte{0x00, 0x08}},
		{name: "map metadata as non-root", isRoot: false, data: []byte{0x00, 0x09}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isRoot, err := IsRootOfAnObject(tc.data)
			require.NoError(t, err)
			require.Equal(t, tc.isRoot, isRoot)
		})
	}

	t.Run("data too short", func(t *testing.T) {
		var decodingError *DecodingError
		var isRoot bool
		var err error

		isRoot, err = IsRootOfAnObject(nil)
		require.False(t, isRoot)
		require.ErrorAs(t, err, &decodingError)

		isRoot, err = IsRootOfAnObject([]byte{})
		require.False(t, isRoot)
		require.ErrorAs(t, err, &decodingError)

		isRoot, err = IsRootOfAnObject([]byte{0x00})
		require.False(t, isRoot)
		require.ErrorAs(t, err, &decodingError)
	})
}

func TestHasPointers(t *testing.T) {
	// We just need first 2 bytes of slab data to test.
	testCases := []struct {
		name        string
		hasPointers bool
		data        []byte
	}{
		{name: "array data has pointer", hasPointers: true, data: []byte{0x00, 0x40}},
		{name: "array metadata has pointer", hasPointers: true, data: []byte{0x00, 0x41}},
		{name: "map data has pointer", hasPointers: true, data: []byte{0x00, 0x48}},
		{name: "map metadata has pointer", hasPointers: true, data: []byte{0x00, 0x49}},
		{name: "array data no pointer", hasPointers: false, data: []byte{0x00, 0x00}},
		{name: "array metadata no pointer", hasPointers: false, data: []byte{0x00, 0x01}},
		{name: "map data no pointer", hasPointers: false, data: []byte{0x00, 0x08}},
		{name: "map metadata no pointer", hasPointers: false, data: []byte{0x00, 0x09}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasPointers, err := HasPointers(tc.data)
			require.NoError(t, err)
			require.Equal(t, tc.hasPointers, hasPointers)
		})
	}

	t.Run("data too short", func(t *testing.T) {
		var decodingError *DecodingError
		var hasPointers bool
		var err error

		hasPointers, err = HasPointers(nil)
		require.False(t, hasPointers)
		require.ErrorAs(t, err, &decodingError)

		hasPointers, err = HasPointers([]byte{})
		require.False(t, hasPointers)
		require.ErrorAs(t, err, &decodingError)

		hasPointers, err = HasPointers([]byte{0x00})
		require.False(t, hasPointers)
		require.ErrorAs(t, err, &decodingError)
	})
}

func TestHasSizeLimit(t *testing.T) {
	// We just need first 2 bytes of slab data to test.
	testCases := []struct {
		name         string
		hasSizeLimit bool
		data         []byte
	}{
		{name: "array data without size limit", hasSizeLimit: false, data: []byte{0x00, 0x20}},
		{name: "array metadata without size limit", hasSizeLimit: false, data: []byte{0x00, 0x21}},
		{name: "map data without size limit", hasSizeLimit: false, data: []byte{0x00, 0x28}},
		{name: "map metadata without size limit", hasSizeLimit: false, data: []byte{0x00, 0x29}},
		{name: "array data with size limit", hasSizeLimit: true, data: []byte{0x00, 0x00}},
		{name: "array metadata with size limit", hasSizeLimit: true, data: []byte{0x00, 0x01}},
		{name: "map data with size limit", hasSizeLimit: true, data: []byte{0x00, 0x08}},
		{name: "map metadata with size limit", hasSizeLimit: true, data: []byte{0x00, 0x09}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasSizeLimit, err := HasSizeLimit(tc.data)
			require.NoError(t, err)
			require.Equal(t, tc.hasSizeLimit, hasSizeLimit)
		})
	}

	t.Run("data too short", func(t *testing.T) {
		var decodingError *DecodingError
		var hasSizeLimit bool
		var err error

		hasSizeLimit, err = HasSizeLimit(nil)
		require.False(t, hasSizeLimit)
		require.ErrorAs(t, err, &decodingError)

		hasSizeLimit, err = HasSizeLimit([]byte{})
		require.False(t, hasSizeLimit)
		require.ErrorAs(t, err, &decodingError)

		hasSizeLimit, err = HasSizeLimit([]byte{0x00})
		require.False(t, hasSizeLimit)
		require.ErrorAs(t, err, &decodingError)
	})
}
