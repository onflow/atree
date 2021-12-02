# Atree

[![](https://github.com/onflow/atree/workflows/ci/badge.svg)](https://github.com/onflow/atree/actions?query=workflow%3Aci)
[![](https://github.com/onflow/atree/workflows/linters/badge.svg)](https://github.com/onflow/atree/actions?query=workflow%3Alinters)
[![CodeQL](https://github.com/onflow/atree/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/onflow/atree/actions/workflows/codeql-analysis.yml)


<p align="left">
  <img src="https://raw.githubusercontent.com/onflow/atree/master/files/logo.jpg" width="350"/>
</p>

Atree provides scalable arrays and scalable ordered maps.  It is used by [Cadence](https://github.com/onflow/cadence) in the [Flow](https://github.com/onflow/flow-go) blockchain.

Inspired by common tricks used in different flavours of B+ Trees, Atree provides two types of data structures: Ordered Map Type (OMT) and Scalable Array Type (SAT).

Scalable Array Type (SAT) is a heterogeneous variable-size array, storing any type of values into a smaller ordered list of values and provides efficient functionality to lookup, insert and remove elements anywhere in the array.

Ordered Map Type (OMT) is an ordered map of key-value pairs; keys can be any hashable type and values can be any serializable value type. it supports heterogeneous key or value types (e.g. first key storing a boolean and second key storing a reference to another array). OMT keeps values in specific sorted order and operations are deterministic so the state of the segments after a sequence of operations are always unique.

Under the hood, Atree uses some new type of high-fanout B+ tree and some heuristics to balance the trade-off between latency of operations and the number of reads and writes.

Each data structure holds the data as several relatively fixed-size segments of bytes (also known as slabs) forming a tree and as the size of data structures grows or shrinks, it adjusts the number of segments used. After each operation, Atree tries to keep segment size within an acceptable size range by merging segments when needed (lower than min threshold) and splitting large-size slabs (above max threshold) or moving some values to neighbouring segments (rebalancing).

In order to minimize the number of bytes touched after each operation, Atree uses a deterministic greedy approach ("Optimistic Encasing Algorithm") to postpone merge, split and rebalancing the tree as much as possible. in other words, It tolerates the tree to get unbalanced with the cost of keeping some space for future insertions or growing a segment a bit larger than what it should be which would minimize the number of segments (and bytes) that are touched at each operation.

For more details about operations please check the documentation inside the code.

## API Reference

Atree's API is [documented](https://pkg.go.dev/github.com/onflow/atree#section-documentation) with godoc at pkg.go.dev and will be updated when new versions of Atree are tagged.  Additional documentation will be added as time allows.

## Contributing

If you would like to contribute to Atree, have a look at the [contributing guide](https://github.com/onflow/atree/blob/main/CONTRIBUTING.md).

Additionally, all non-error code paths must be covered by tests.  And pull requests should not lower the code coverage percent.

## License

The Atree library is licensed under the terms of the Apache license. See [LICENSE](LICENSE) for more information.

Copyright © 2021 Dapper Labs, Inc.
