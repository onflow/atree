
<p align="left">
  <img src="https://raw.githubusercontent.com/onflow/atree/ec159f3a81cbc6f1338f7f594987f483ddd1e0bd/files/logo.png" width="150"/>
</p>

[![](https://github.com/onflow/atree/workflows/ci/badge.svg)](https://github.com/onflow/atree/actions?query=workflow%3Aci)
[![](https://github.com/onflow/atree/workflows/linters/badge.svg)](https://github.com/onflow/atree/actions?query=workflow%3Alinters)
[![CodeQL](https://github.com/onflow/atree/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/onflow/atree/actions/workflows/codeql-analysis.yml)

# Atree 

*Atree* provides scalable arrays and scalable ordered maps.  It is used by [Cadence](https://github.com/onflow/cadence) in the [Flow](https://github.com/onflow/flow-go) blockchain.

Inspired by patterns used in modern variants of B+ Trees, Atree provides two types of data structures: Ordered Map Type (OMT) and Scalable Array Type (SAT).

Scalable Array Type (SAT) is a heterogeneous variable-size array, storing any type of values into a smaller ordered list of values and provides efficient functionality to lookup, insert and remove elements anywhere in the array.

Ordered Map Type (OMT) is an ordered map of key-value pairs; keys can be any hashable type and values can be any serializable value type. it supports heterogeneous key or value types (e.g. first key storing a boolean and second key storing a string). OMT keeps values in specific sorted order and operations are deterministic so the state of the segments after a sequence of operations are always unique.

Under the hood, Atree uses some new type of high-fanout B+ tree and some heuristics to balance the trade-off between latency of operations and the number of reads and writes.

Each data structure holds the data as several relatively fixed-size segments of bytes (also known as slabs) forming a tree and as the size of data structures grows or shrinks, it adjusts the number of segments used. After each operation, Atree tries to keep segment size within an acceptable size range by merging segments when needed (lower than min threshold) and splitting large-size slabs (above max threshold) or moving some values to neighbouring segments (rebalancing).

In order to minimize the number of bytes touched after each operation, Atree uses a deterministic greedy approach ("Optimistic Encasing Algorithm") to postpone merge, split and rebalancing the tree as much as possible. in other words, It tolerates the tree to get unbalanced with the cost of keeping some space for future insertions or growing a segment a bit larger than what it should be which would minimize the number of segments (and bytes) that are touched at each operation.

## Example 

<p align="left">
  <img src="https://raw.githubusercontent.com/onflow/atree/e47e7e8016bd781211c01c6ec423ae9df8a34b72/files/example.jpg" width="600"/>
</p>

**1** - An ordered map meta data slab keeps the very first key hash of any children to navigate the path. it uses combination of linear scan and binary search to find the next slab. 

**2** - Similarly a array meta data slab keeps the count of each children and uses that navigate the path. 

**3** - Nested structures (e.g. map holding an array under a key) are handled by storing nested map or array separate and using one way references from parent to the nested object.

**4** - Extremely large objects are handled by storing them as an external data slab and use of pointers. This way we maintain the size requirments of slabs and preserve the performance of atree.

**5** - Atree Ordered Map is resiliant against hash-flooding attacks, by using multiple levels of hashing: starting from fast noncryptographic ones for performance follows by cryptographic hashes when collisions happens and finally a layer of value chaining with linear lookup.

**6** - Forwarding data slab pointers are used to make sequntial iterations more effient.

Additional documentation will be added as time allows, but for more details about operations please check out the documentation inside the code.

## API Reference

Atree's API is [documented](https://pkg.go.dev/github.com/onflow/atree#section-documentation) with godoc at pkg.go.dev and will be updated when new versions of Atree are tagged.  

## Contributing

If you would like to contribute to Atree, have a look at the [contributing guide](https://github.com/onflow/atree/blob/main/CONTRIBUTING.md).

Additionally, all non-error code paths must be covered by tests.  And pull requests should not lower the code coverage percent.

## License

The Atree library is licensed under the terms of the Apache license. See [LICENSE](LICENSE) for more information.

Logo is based on the artwork of Raisul Hadi licensed under Creative Commons.

Copyright © 2021 Dapper Labs, Inc.

