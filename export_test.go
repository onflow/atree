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

// Exported functions of PersistentSlabStorage for testing.
var (
	GetBaseStorage = (*PersistentSlabStorage).getBaseStorage
	GetCache       = (*PersistentSlabStorage).getCache
	GetDeltas      = (*PersistentSlabStorage).getDeltas
	GetCBOREncMode = (*PersistentSlabStorage).getCBOREncMode
	GetCBORDecMode = (*PersistentSlabStorage).getCBORDecMode
)

// Exported function of slab size settings for testing.
var (
	TargetSlabSize = targetSlabSize
)

// Exported function of Array for testing.
var (
	GetArrayRootSlab                 = (*Array).rootSlab
	ArrayHasParentUpdater            = (*Array).hasParentUpdater
	GetArrayMutableElementIndexCount = (*Array).getMutableElementIndexCount
)

// Exported function of OrderedMap for testing.
var (
	GetMapRootSlab        = (*OrderedMap).rootSlab
	GetMapDigesterBuilder = (*OrderedMap).getDigesterBuilder
)
