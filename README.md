The H2Histogram provides a histogram that is conceptually similar to
[HdrHistogram](http://hdrhistogram.org) but with base-2 buckets, which makes it
noticeably faster. This introduces small modifications to the configurable
options as well.

This module is a pure Javascript implementation of the algorithm, which is
described at [h2histogram.org](https://h2histogram.org).

H2Encoding encodes values from the integer range [0, 2^n) into base-2 logarithmic
bins with a controllable relative error bound.

The number of bins must be less than 2^32, and the largest encodable value must
be less than 2^53.

The histogram is designed to encode integer values only.

## Two APIs

This package ships two complementary surfaces:

1. **`H2Encoding` / `H2HistogramBuilder` / `H2Histogram`** — the original
   JavaScript-native API, parameterized by `{ a, b, n }`, where `a` sets the
   width (`2^a`) of the linear buckets on the low end.

2. **Canonical-compatible API** (`Config`, `Histogram`, `SparseHistogram`,
   `CumulativeHistogram`, `Bucket`) — mirrors the iopsystems
   [`histogram`](https://github.com/iopsystems/histogram) Rust crate (the
   canonical implementation) and its
   [Python](https://github.com/iopsystems/h2histogram-py) and
   [Go](https://github.com/iopsystems/h2histogram-go) ports, using
   `groupingPower` / `maxValuePower`.

The two are interchangeable: a canonical `Config(groupingPower, maxValuePower)`
is exactly `new H2Encoding({ a: 0, b: groupingPower, n: maxValuePower })` — the
canonical implementation always uses width-1 linear buckets — so the canonical
API reuses the same (property-tested) encoding under the hood and produces
**byte-for-byte identical bucketing** to the Rust crate. This means a histogram
recorded by Rezolus, or by the Python/Go implementations, can be loaded and
analyzed here (and vice versa).

### 53-bit values

JavaScript numbers are 64-bit floats, so unlike the Rust/Go implementations
(which support the full `u64` range up to `maxValuePower = 64`), this library
caps `maxValuePower` at **53** — values up to `2^53 - 1`. Below that limit the
bucketing is identical across all implementations.

## Canonical API quick start

```js
import { Histogram } from 'h2-histogram';

const h = new Histogram(7, 53); // groupingPower, maxValuePower (default 53)
h.increment(42);
h.record(1000, 5);              // value, count
h.recordMany([12, 15, 900]);    // bulk

h.totalCount();                 // 8

const p99 = h.percentile(0.99); // a Bucket, or null if empty
p99.range;                      // [lo, hi] inclusive
p99.midpoint;                   // midpoint estimate

// Combine / reduce
const coarse = h.downsample(4); // fewer buckets, higher error, same total count
const sparse = h.toSparse();    // columnar (index, count) form for storage
```

### Fast repeated quantile queries

For a snapshot you'll query many times, convert to a `CumulativeHistogram`
(the crate's `CumulativeROHistogram`). It stores non-zero buckets with
**cumulative** counts, so percentiles are answered with a binary search, and it
precomputes a midpoint-estimated `mean`:

```js
const c = h.toCumulative();      // read-only; also SparseHistogram#toCumulative()
c.percentile(0.99);              // O(log n) -> Bucket (individual count)
c.mean();                        // midpoint-estimated mean, computed once
c.bucketQuantileRange(0);        // [lower, upper] quantile fraction of a stored bucket
for (const [bucket, lo, hi] of c.iterWithQuantiles()) {
  // each non-zero bucket with its quantile span
}
```

### Interop with columnar (Rezolus) data

```js
import { Config, SparseHistogram, CumulativeHistogram } from 'h2-histogram';

const config = new Config(3, 53);                 // Rezolus-style config
const sparse = SparseHistogram.fromParts(config, bucketIndices, bucketCounts);
const cumulative = sparse.toCumulative();
cumulative.percentile(0.999);
```

## API overview

| Type | Purpose |
|------|---------|
| `Config` | Bucketing parameters; `valueToIndex`, `indexToRange`, `totalBuckets`, `error`, `fromTotalBuckets` |
| `Histogram` | Dense histogram; `increment`, `record`, `recordMany`, `percentile(s)`, `merge`, `subtract`, `downsample`, `toSparse`, `toCumulative`, `fromBuckets` |
| `SparseHistogram` | Columnar `(index, count)` form; `fromHistogram`, `fromParts`, `toDense`, `toCumulative` |
| `CumulativeHistogram` | Read-only cumulative form (crate's `CumulativeROHistogram`); binary-search `percentile(s)`, `mean`, `bucketQuantileRange`, `iterWithQuantiles` |
| `Bucket` | A bucket's `count` and inclusive `[start, end]` range, plus `midpoint`/`width` |
| `H2Encoding`, `H2HistogramBuilder`, `H2Histogram`, `encode32`, `decode32` | The original `{ a, b, n }` API (unchanged) |

## Examples

Runnable examples live in [`examples/`](examples):

- [`basic_usage.js`](examples/basic_usage.js) — record a distribution and query percentiles
- [`cumulative_quantiles.js`](examples/cumulative_quantiles.js) — fast repeated quantiles, mean, and per-bucket quantile spans via `CumulativeHistogram`
- [`interop_columnar.js`](examples/interop_columnar.js) — load a histogram from columnar `(index, count)` data (the Rezolus / cross-language storage form)

```bash
node examples/basic_usage.js
```

## Related implementations

The h2 histogram bucketing is implemented in several languages, all producing
byte-for-byte identical buckets so histograms interoperate across them:

- [**Rust**](https://github.com/iopsystems/histogram) — the canonical
  implementation (`histogram` crate)
- [**Python**](https://github.com/iopsystems/h2histogram-py)
- [**Go**](https://github.com/iopsystems/h2histogram-go)
- [**JavaScript**](https://github.com/iopsystems/h2histogram-js) — this
  repository (values up to `2^53 - 1`)

### Reporting and analytics phases (canonical API)

Keep recording on `Histogram` and choose reporting work according to the phase:

| Phase | API | Storage and cost |
| --- | --- | --- |
| Record/reset | `record`, `increment`, `reset()` | Recording updates one bucket without count validation; reset fills the existing `Float64Array`. No cached total/min/max is maintained. |
| Reused dense reports | `snapshotInto(destination)`, `drainInto(destination)` | Validate compatible geometry and counts before copying; preserve destination storage. Drain then resets the source and rejects overlapping byte ranges (disjoint views of one buffer are supported). Returns the destination. |
| Aggregation | `checkedAddAssign(other)`, `Histogram.checkedSum(histograms)` | In-place addition checks every count before mutation; disjoint same-buffer views and exact self-addition are supported, while partial overlap is rejected. Sum checks every configuration first, rejects an empty collection, and returns private storage even for one input or repeated references. `merge` retains its original unchecked arithmetic. |
| Scalar reporting | `percentile(p)`, dense/cumulative `quantile(p)` | Direct scan of dense or sparse counts, or cumulative binary search; allocates only the returned `Bucket`. No batch containers or dense reconstruction. |
| Reused batch reports | `percentilesInto(requests, output)` on all three classes | Reuses the caller's ordinary outer array; allocates a new pair and `Bucket` per request, so retained pairs remain unchanged and shared or frozen pair slots are supported. Preserves order and duplicates. No request sorting/copy is needed. Dense/sparse queries scan per request after one total scan; cumulative queries use binary search per request. |
| Owned transforms | sparse/cumulative `merge(other)`, `downsample(groupingPower)`; cumulative `toSparse()` | Sorted column operations without dense reconstruction. Merge accepts either sparse or cumulative input and returns the receiver's representation. Downsampling requires lower grouping power and recomputes cumulative means using output bucket midpoints. |

Owned `checkedSum` validates all configurations before allocating its result,
then copies and validates the first source once. Each remaining source is checked
and added in one pass over the private result; an error discards that result and
leaves every source untouched. It does not use `checkedAddAssign`'s separate
validation pass, which is necessary when preserving an existing destination.

Percentiles are fractions in `[0, 1]`. Empty histograms return `null` from
queries; `percentilesInto` also clears its output. Empty requests always produce an empty array without scanning totals. Invalid requests are checked before changing
output. The outer output array must be mutable. Request and output arrays must be distinct. The allocating `percentiles`
API retains its original result shape; dense and sparse `percentiles` use a sorted scan,
which may suit large batches better than repeated buffer queries.

Counts must be non-negative safe integers, at most `Number.MAX_SAFE_INTEGER`
(`2^53 - 1`), not Rust's `u64` limit. Callers of the unchecked recording and legacy `merge` paths must preserve these
limits themselves. An aggregate total may exceed this limit across buckets: `totalCount`, percentile
reports, and cumulative construction reject that case. Recording performs its original counter update with no extra count checks or
scans. Imports, checked aggregation, lifecycle copies, and reports validate counts
at their boundaries. Imports validate indices and counts;
sparse zero counts are accepted and removed only after validation.
Cumulative inputs accept equal adjacent prefix counts (zero individual counts),
but prefix values must be positive safe integers. Means and percentile fraction
arithmetic remain floating-point estimates.

Sparse and cumulative constructors copy and freeze their column arrays; their
public `index`, `count`, and `config` references cannot be reassigned. Configs
are immutable. Cumulative means therefore cannot become stale through snapshot
accessors. The accepted legacy `{ validate: false }` constructor option no longer
bypasses validation. Factories also pass through constructor validation and copying;
this additional snapshot-boundary work keeps the invariant in one place and does
not affect recording. Dense `buckets` remains a mutable escape hatch: callers must
preserve its shape and safe-integer counts. No snapshot/drain operation is atomic
or thread-safe; callers must provide exclusive access, including when sharing
buffers with workers. These APIs introduce no concurrent recorder.

JavaScript arrays expose no portable capacity or shrink-to-fit contract, so no
compaction API is provided. Snapshot column copying/freezing and returned objects
still allocate. Ordinary loops allow runtime JIT optimization, with no forced
SIMD, WASM, BigInt counter family, or new runtime dependency. The legacy
`H2Encoding`, `H2HistogramBuilder`, and `H2Histogram` surface remains available.

Run `node benchmarks/reporting.js 2000` for separate recording, reuse, queries,
snapshots, aggregation, and native-transform timings. Inputs are prepared before
timing, operations warm up first, output describes included ownership costs,
and ordinary garbage collection may be included. These measurements are local
runtime evidence; they do not establish Rust-equivalent speedups or portable
allocation-byte guarantees.

Development validation for this change used dependency versions resolved from the
existing manifest's semver ranges. The installed pnpm version could not read the
older repository lockfile; the unrelated lockfile was left unchanged.
