# AGENTS.md

Guidance for AI coding agents (Claude Code, Codex, Cursor, Copilot, and others) working in
this repository. Loaded into agent context automatically — keep concise.

## Overview

atree is a Go library providing two persistent, slab-based data structures used by the
Cadence runtime on Flow: a Scalable Array Type (SAT) and an Ordered Map Type (OMT). Data
is segmented into relatively small fixed-size byte slabs arranged as a high-fanout B+ tree
so that only modified slabs need to be re-hashed and re-written (`README.md`, `doc.go`).
Module path is `github.com/onflow/atree`; Go 1.24 per `go.mod`; CI matrix runs against Go
1.24 and 1.25 (`.github/workflows/ci.yml`).

## Build and Test Commands

There is no Makefile in this repo. Use `go` and `golangci-lint` directly. Commands below
are taken verbatim from the CI workflows.

- `go build ./...` — build all packages (`ci.yml`).
- `go test -timeout 180m -race -v ./...` — full test suite with race detector; tests are
  slow, hence the 180-minute timeout (`ci.yml`).
- `go test -timeout 180m -coverprofile=coverage.txt -covermode=atomic` — coverage run used
  by `coverage.yml`; reports uploaded to Codecov.
- `golangci-lint run --timeout=15m` — lint; CI pins `golangci-lint` v2.5.0
  (`.github/workflows/safer-golangci-lint.yml`).
- `./check-headers.sh` — fails if any `.go` file lacks the Apache 2.0 license header or a
  `Code generated (from|by)` marker.
- `go test -bench=. -run=^$ ./...` — run Go benchmarks. Benchmark functions live in
  `array_bench_test.go`, `array_benchmark_test.go`, `mapcollision_bench_test.go`, and
  `storage_bench_test.go`. There is no standalone benchmark script.
- `go run ./cmd/smoke -type=array` (or `-type=map`) — run the smoke/stress test binary.
  Flags include `-slabcheck`, `-maxlen`, `-seed`, `-minheap`, `-maxheap`, `-count`,
  `-wrappervalue`, `-minOpsForStorageHealthCheck` (`cmd/smoke/main.go`).

## Architecture

All production code lives in the root `atree` package (single flat package; no `pkg/` or
`internal/`). Files are grouped by concern and by data structure:

- Array (SAT): `array.go`, `array_data_slab.go` (+ `_encode.go`/`_decode.go`),
  `array_metadata_slab.go` (+ `_encode.go`/`_decode.go`), `array_iterator.go`,
  `array_extradata.go`, `array_serialization_verify.go`, `array_verify.go`,
  `array_conversion.go`, `array_slab.go`, `array_slab_stats.go`, `array_size_consts.go`,
  `array_dump.go`.
- Map (OMT): `map.go`, `map_data_slab.go` (+ encode/decode), `map_metadata_slab.go` (+
  encode/decode), `map_element.go`, `map_elements.go`, `map_elements_hashkey.go`,
  `map_elements_nokey.go`, `map_iterator.go`, `map_extradata.go`, `compactmap_extradata.go`,
  `map_verify.go`, `map_serialization_verify.go`, `map_slab.go`, `map_slab_stats.go`,
  `map_size_consts.go`, `map_dump.go`.
- Storage and slab primitives: `storage.go` (`PersistentSlabStorage`), `slab.go`,
  `slab_id.go`, `slab_id_storable.go`, `storable.go`, `storable_slab.go`, `value.go`,
  `value_id.go`, `extradata.go`, `buffer.go`, `storage_health_check.go`.
- Cross-cutting: `encode.go`, `decode.go`, `cbor_tag_nums.go`, `hash.go`, `errors.go`,
  `flag.go`, `typeinfo.go`, `settings.go`, `math_utils.go`, `slice_utils.go`,
  `inline_utils.go`.
- `cmd/smoke/` — randomized long-running stress binary that uses `NewPersistentSlabStorage`
  plus the in-memory base storage from `test_utils` (`cmd/smoke/main.go`).
- `test_utils/` — shared helpers (`InMemBaseStorage`, `DecodeStorable`, value/typeinfo
  helpers) importable as `github.com/onflow/atree/test_utils`; used by tests and by
  `cmd/smoke`.
- `files/` — README images only. `logs/` — historical perf logs (2021-07-07, 2021-07-08)
  ignored by coverage (`codecov.yml`).

CBOR is the on-disk encoding (`github.com/fxamacker/cbor/v2` in `go.mod`). Hashing uses
`fxamacker/circlehash` (CircleHash64f) and `zeebo/blake3` / `lukechampine.com/blake3`;
BLAKE3 digests are computed lazily on collision, see the OMT description in `README.md`.

## Conventions and Gotchas

- **CONTRIBUTING.md is stale on build commands.** It says "Code should pass the linter:
  `make lint`" and "Code should pass all tests: `make test`", but no `Makefile` exists in
  this repo. Use `go test ./...` and `golangci-lint run` directly (as CI does).
- **Slab size is global.** `settings.go` holds package-level `targetThreshold`,
  `minThreshold`, `maxThreshold` initialized via `setThreshold(defaultSlabSize)` in `init`.
  Defaults: `defaultSlabSize=1024`, `minSlabSize=256`, `maxSlabSize=32*1024`. Changing
  thresholds in a test affects other tests — use the test-only `SetThreshold` export
  (`export_test.go`) and restore it on teardown.
- **Invariants enforced in slab logic:** per comment in `settings.go`, each element must
  not exceed half of the slab size (including encoding overhead and digest), and a data
  slab must hold at least 2 elements when its size exceeds `maxThreshold`.
- **License header required on every `.go` file.** `check-headers.sh` scans for
  `Licensed under the Apache License` or `Code generated (from|by)` and fails CI
  otherwise. New files must include the Apache-2.0 header copied from any existing file
  (e.g. `doc.go`).
- **Imports are grouped with `github.com/onflow/atree` as a local prefix** via
  `goimports` configured in `.golangci.yml` (`formatters.settings.goimports.local-prefixes`).
  Run `golangci-lint run` before committing; the `linters` workflow will reject otherwise.
- **Two map element layouts coexist:** `map_elements_hashkey.go` (hash-keyed) and
  `map_elements_nokey.go` (no-key compact form). Many map changes must be mirrored across
  both; `map_verify.go` and `map_serialization_verify.go` are the canonical cross-checks.
- **`export_test.go` is the only sanctioned way to access unexported internals from
  tests.** It re-exports `getBaseStorage`, `getCache`, `getDeltas`, CBOR modes,
  `targetSlabSize`, `maxInlineMapValueSize`, and `setThreshold`. Prefer extending this
  file over adding new exported production APIs purely for testing.
- **Only 64-bit architectures are officially supported** (`SECURITY.md`). Do not
  introduce assumptions that break on 64-bit; do not add 32-bit-specific workarounds
  without maintainer guidance.
- **Codecov excludes `cmd/**/*` and `logs/`** (`codecov.yml`). Coverage must not drop for
  the rest per `README.md` ("pull requests should not lower the code coverage percent").
- **CODEOWNERS:** `@fxamacker @ramtinms @turbolent` — PRs require their review
  (`CODEOWNERS`).
- **Tests are heavy.** `array_test.go` (~303 KB), `map_test.go` (~617 KB), and
  `storage_test.go` (~114 KB) are enormous; scope test runs with `-run` while iterating:
  e.g. `go test -run TestArrayAppend -race -v`.
- **Commit style** (`CONTRIBUTING.md`): imperative, present tense, first line <= 72
  chars; reference issues/PRs after the first line.
- **CI triggers** only on branches `main`, `feature/**`, `release-**`
  (`ci.yml`/`coverage.yml`/`safer-golangci-lint.yml`). Feature branches outside these
  prefixes will not run CI on push.

## Files Not to Modify

- `logs/2021-07-07/`, `logs/2021-07-08/` — historical performance logs; do not edit.
- `files/example.jpg`, `files/logo.png` — README assets.
- `go.sum` — regenerate via `go mod tidy`, do not hand-edit.
- `LICENSE`, `NOTICE` — legal text; change only via a legal review.
