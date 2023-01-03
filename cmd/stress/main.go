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
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/onflow/atree"
)

const maxStatusLength = 128

type Status interface {
	Write()
}

func writeStatus(status string) {
	// Clear old status
	s := fmt.Sprintf("\r%s\r", strings.Repeat(" ", maxStatusLength))
	_, _ = io.WriteString(os.Stdout, s)

	// Write new status
	_, _ = io.WriteString(os.Stdout, status)
}

func updateStatus(sigc <-chan os.Signal, status Status) {

	status.Write()

	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-ticker.C:
			status.Write()

		case <-sigc:
			status.Write()
			fmt.Fprintf(os.Stdout, "\n")

			ticker.Stop()
			os.Exit(1)
		}
	}
}

func main() {

	var typ string
	var maxLength uint64
	var seedHex string
	var minHeapAllocMiB, maxHeapAllocMiB uint64

	flag.StringVar(&typ, "type", "array", "array or map")
	flag.Uint64Var(&maxLength, "maxlen", 10_000, "max number of elements")
	flag.StringVar(&seedHex, "seed", "", "seed for prng in hex (default is Unix time)")
	flag.Uint64Var(&minHeapAllocMiB, "minheap", 1000, "min HeapAlloc in MiB to stop extra removal of elements")
	flag.Uint64Var(&maxHeapAllocMiB, "maxheap", 2000, "max HeapAlloc in MiB to trigger extra removal of elements")

	flag.Parse()

	var seed int64
	if len(seedHex) != 0 {
		var err error
		seed, err = strconv.ParseInt(strings.ReplaceAll(seedHex, "0x", ""), 16, 64)
		if err != nil {
			panic("Failed to parse seed flag (hex string)")
		}
	}

	r = newRand(seed)

	typ = strings.ToLower(typ)

	if typ != "array" && typ != "map" {
		fmt.Fprintf(os.Stderr, "Please specify type as either \"array\" or \"map\"")
		return
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	// Create storage
	encMode, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create CBOR encoding mode: %s\n", err)
		return
	}

	decMode, err := cbor.DecOptions{}.DecMode()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create CBOR decoding mode: %s\n", err)
		return
	}

	baseStorage := NewInMemBaseStorage()

	storage := atree.NewPersistentSlabStorage(
		baseStorage,
		encMode,
		decMode,
		decodeStorable,
		decodeTypeInfo,
	)

	typeInfo := testTypeInfo{value: 123}

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	switch typ {

	case "array":
		fmt.Printf("Starting array stress test, minMapHeapAlloc = %d MiB, maxMapHeapAlloc = %d MiB\n", minHeapAllocMiB, maxHeapAllocMiB)

		status := newArrayStatus()

		go updateStatus(sigc, status)

		testArray(storage, address, typeInfo, maxLength, status, minHeapAllocMiB, maxHeapAllocMiB)

	case "map":
		fmt.Printf("Starting map stress test, minMapHeapAlloc = %d MiB, maxMapHeapAlloc = %d MiB\n", minHeapAllocMiB, maxHeapAllocMiB)

		status := newMapStatus()

		go updateStatus(sigc, status)

		testMap(storage, address, typeInfo, maxLength, status, minHeapAllocMiB, maxHeapAllocMiB)
	}

}
