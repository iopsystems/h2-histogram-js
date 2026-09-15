# Changelog

## Unreleased

### Breaking compatibility changes

- Sparse and cumulative snapshots copy and freeze their `index` and `count` arrays. Their column and `config` references cannot be reassigned. Mutate source data before constructing a new snapshot, rather than editing snapshot columns.
- The legacy cumulative constructor option `{ validate: false }` remains accepted but no longer disables validation. All constructors validate imported indices and counts; factories also incur constructor validation and copying.
- Sparse zero counts are validated and then removed, so stored column lengths can shrink. Sparse-to-cumulative conversion drops zero entries; direct cumulative imports accept equal positive adjacent prefixes but reject zero prefixes.
- Counts must be non-negative safe integers, at most `Number.MAX_SAFE_INTEGER`. `totalCount`, percentile reports, and cumulative construction also require the aggregate total to fit this bound. Checked aggregation validates per-bucket sums, so `checkedSum([h])` can succeed even when `h.totalCount()` throws. Recording and legacy dense `merge` retain unchecked arithmetic; callers must preserve count bounds.

### Fixes

- Dense checked addition and drain allow disjoint views of the same backing buffer. Partial overlap is rejected; exact-alias addition remains supported and aliased drain remains rejected.
- `percentilesInto` reuses only its mutable outer output array and replaces each tuple with a newly allocated tuple and `Bucket`. Previously returned tuples remain unchanged, including shared or frozen tuples supplied as output slots.
