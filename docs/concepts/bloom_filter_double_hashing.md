# Bloom Filter Double Hashing Optimization

Bloom filters rely on `k` hash probes to decide whether an element is potentially
present in the set. A naïve implementation would rebuild the hashing state for
every probe, which can be expensive when `k` is large. The
**Kirsch–Mitzenmacher** optimization demonstrates that we can derive all probe
indices from two base hashes while maintaining the same false-positive
probability as using `k` independent hashes.

## Core Idea

For each element, we compute two base hash values `h1` and `h2`. Every probe
position `h_i` in the Bloom filter can then be derived from these values using

```
h_i = (h1 + i * h2) mod m
```

where `m` is the size of the Bloom filter bitset and `i` ranges from `0` to
`k - 1`. This approach keeps the probes evenly distributed while reducing the
number of hash computations from `k` to 2.

## Implementation Details

In `bloomfilter.go`, the base hashes are generated once per key using
`murmur3.Sum128WithSeed`. The 128-bit output yields two 64-bit values that serve
as `h1` and `h2`. Probe indices are derived in a tight loop without rebuilding
the Murmur3 state on each iteration.

A few practical details ensure robustness:

* **Non-zero step size:** When `h2` is a multiple of the Bloom filter size, the
  stride would be zero. To avoid this, the implementation forces the step to be
  odd by adding `1` when necessary, ensuring progress across the bitset.
* **Empty filters:** The methods defensively handle zero-length bitsets by
  returning early to avoid division-by-zero or modulo-by-zero operations.

## Benefits

This strategy drastically reduces CPU usage and heap allocations during insert
and lookup operations. The benchmarks in `bloomfilter_test.go` show the
improvement by comparing the optimized insert and lookup operations against the
previous approach.

## Further Reading

* Kirsch, A. & Mitzenmacher, M. (2008). *Less Hashing, Same Performance: Building
  a Better Bloom Filter.*
* Randall, D. (2002). *The No-Hashing Trick: Running Bloom Filters with Two Hash
  Functions.*
