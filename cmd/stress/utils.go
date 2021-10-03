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

package main

import (
	"math/rand"

	"github.com/onflow/atree"
)

var (
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func randStr(n int) string {
	r := make([]rune, n)
	for i := range r {
		r[i] = letters[rand.Intn(len(letters))]
	}
	return string(r)
}

// TODO: add nested array and nested map
func randomValue() atree.Value {
	switch rand.Intn(5) {
	case 0:
		return Uint8Value(rand.Intn(255))
	case 1:
		return Uint16Value(rand.Intn(6535))
	case 2:
		return Uint32Value(rand.Intn(4294967295))
	case 3:
		return Uint64Value(rand.Intn(1844674407370955161))
	case 4:
		slen := rand.Intn(1024)
		return NewStringValue(randStr(slen))
	default:
		return Uint8Value(rand.Intn(255))
	}
}
