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

package test_utils

import (
	"github.com/onflow/atree"
)

type InMemBaseStorage struct {
	segments         map[atree.SlabID][]byte
	slabIndex        map[atree.Address]atree.SlabIndex
	bytesRetrieved   int
	bytesStored      int
	segmentsReturned map[atree.SlabID]struct{}
	segmentsUpdated  map[atree.SlabID]struct{}
	segmentsTouched  map[atree.SlabID]struct{}
}

var _ atree.BaseStorage = &InMemBaseStorage{}

func NewInMemBaseStorage() *InMemBaseStorage {
	return NewInMemBaseStorageFromMap(
		make(map[atree.SlabID][]byte),
	)
}

func NewInMemBaseStorageFromMap(segments map[atree.SlabID][]byte) *InMemBaseStorage {
	return &InMemBaseStorage{
		segments:         segments,
		slabIndex:        make(map[atree.Address]atree.SlabIndex),
		segmentsReturned: make(map[atree.SlabID]struct{}),
		segmentsUpdated:  make(map[atree.SlabID]struct{}),
		segmentsTouched:  make(map[atree.SlabID]struct{}),
	}
}

func (s *InMemBaseStorage) Retrieve(id atree.SlabID) ([]byte, bool, error) {
	seg, ok := s.segments[id]
	s.bytesRetrieved += len(seg)
	s.segmentsReturned[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return seg, ok, nil
}

func (s *InMemBaseStorage) Store(id atree.SlabID, data []byte) error {
	s.segments[id] = data
	s.bytesStored += len(data)
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	return nil
}

func (s *InMemBaseStorage) Remove(id atree.SlabID) error {
	s.segmentsUpdated[id] = struct{}{}
	s.segmentsTouched[id] = struct{}{}
	delete(s.segments, id)
	return nil
}

func (s *InMemBaseStorage) GenerateSlabID(address atree.Address) (atree.SlabID, error) {
	index := s.slabIndex[address]
	nextIndex := index.Next()

	s.slabIndex[address] = nextIndex
	return atree.NewSlabID(address, nextIndex), nil
}

func (s *InMemBaseStorage) SegmentCounts() int {
	return len(s.segments)
}

func (s *InMemBaseStorage) Size() int {
	total := 0
	for _, seg := range s.segments {
		total += len(seg)
	}
	return total
}

func (s *InMemBaseStorage) BytesRetrieved() int {
	return s.bytesRetrieved
}

func (s *InMemBaseStorage) BytesStored() int {
	return s.bytesStored
}

func (s *InMemBaseStorage) SegmentsReturned() int {
	return len(s.segmentsReturned)
}

func (s *InMemBaseStorage) SegmentsUpdated() int {
	return len(s.segmentsUpdated)
}

func (s *InMemBaseStorage) SegmentsTouched() int {
	return len(s.segmentsTouched)
}

func (s *InMemBaseStorage) ResetReporter() {
	s.bytesStored = 0
	s.bytesRetrieved = 0
	s.segmentsReturned = make(map[atree.SlabID]struct{})
	s.segmentsUpdated = make(map[atree.SlabID]struct{})
	s.segmentsTouched = make(map[atree.SlabID]struct{})
}
