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

	"github.com/onflow/atree"

	"github.com/fxamacker/cbor/v2"
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

var cborEncMode = func() cbor.EncMode {
	encMode, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		panic(fmt.Sprintf("Failed to create CBOR encoding mode: %s", err))
	}
	return encMode
}()

var cborDecMode = func() cbor.DecMode {
	decMode, err := cbor.DecOptions{}.DecMode()
	if err != nil {
		panic(fmt.Sprintf("Failed to create CBOR decoding mode: %s\n", err))
	}
	return decMode
}()

var (
	flagType                                 string
	flagCheckSlabEnabled                     bool
	flagMaxLength                            uint64
	flagSeedHex                              string
	flagMinHeapAllocMiB, flagMaxHeapAllocMiB uint64
	flagMinOpsForStorageHealthCheck          uint64
)

func main() {

	flag.StringVar(&flagType, "type", "array", "array or map")
	flag.BoolVar(&flagCheckSlabEnabled, "slabcheck", false, "in memory and serialized slab check")
	flag.Uint64Var(&flagMinOpsForStorageHealthCheck, "minOpsForStorageHealthCheck", 100, "number of operations for storage health check")
	flag.Uint64Var(&flagMaxLength, "maxlen", 10_000, "max number of elements")
	flag.StringVar(&flagSeedHex, "seed", "", "seed for prng in hex (default is Unix time)")
	flag.Uint64Var(&flagMinHeapAllocMiB, "minheap", 1000, "min HeapAlloc in MiB to stop extra removal of elements")
	flag.Uint64Var(&flagMaxHeapAllocMiB, "maxheap", 2000, "max HeapAlloc in MiB to trigger extra removal of elements")

	flag.Parse()

	var seed int64
	if len(flagSeedHex) != 0 {
		var err error
		seed, err = strconv.ParseInt(strings.ReplaceAll(flagSeedHex, "0x", ""), 16, 64)
		if err != nil {
			panic("Failed to parse seed flag (hex string)")
		}
	}

	r = newRand(seed)

	flagType = strings.ToLower(flagType)

	if flagType != "array" && flagType != "map" {
		fmt.Fprintf(os.Stderr, "Please specify type as either \"array\" or \"map\"")
		return
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)

	baseStorage := NewInMemBaseStorage()

	storage := atree.NewPersistentSlabStorage(
		baseStorage,
		cborEncMode,
		cborDecMode,
		decodeStorable,
		decodeTypeInfo,
	)

	address := atree.Address{1, 2, 3, 4, 5, 6, 7, 8}

	switch flagType {

	case "array":
		var msg string
		if flagCheckSlabEnabled {
			msg = fmt.Sprintf("Starting array stress test with slab check, minMapHeapAlloc = %d MiB, maxMapHeapAlloc = %d MiB", flagMinHeapAllocMiB, flagMaxHeapAllocMiB)
		} else {
			msg = fmt.Sprintf("Starting array stress test, minMapHeapAlloc = %d MiB, maxMapHeapAlloc = %d MiB", flagMinHeapAllocMiB, flagMaxHeapAllocMiB)
		}
		fmt.Println(msg)

		status := newArrayStatus()

		go updateStatus(sigc, status)

		testArray(storage, address, status)

	case "map":
		var msg string
		if flagCheckSlabEnabled {
			msg = fmt.Sprintf("Starting map stress test with slab check, minMapHeapAlloc = %d MiB, maxMapHeapAlloc = %d MiB", flagMinHeapAllocMiB, flagMaxHeapAllocMiB)
		} else {
			msg = fmt.Sprintf("Starting map stress test, minMapHeapAlloc = %d MiB, maxMapHeapAlloc = %d MiB", flagMinHeapAllocMiB, flagMaxHeapAllocMiB)
		}
		fmt.Println(msg)

		status := newMapStatus()

		go updateStatus(sigc, status)

		testMap(storage, address, status)
	}

}
