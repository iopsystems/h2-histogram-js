export const DEBUG = true;

export class H2Encoding {
  /**
   * H2Encoding encodes values from the integer range [0, 2^n) into base-2 logarithmic
   * bins with a controllable relative error bound.
   *
   * The number of bins must be less than 2^32, and the largest encodable value must
   * be less than 2^53.
   *
   * The histogram is designed to encode integer values only.
   *
   * @param {object} options
   * @param {number} options.a - The `a` parameter controls the width of bins on
   * the low end of the value range. Each bin is 2^a wide, so the absolute error on
   * the low end is 2^a. Since the value range includes zero, there must be some
   * "minimum unit" below which an absolute error is tolerable, since otherwise there
   * would need to be infinite bins in order to satisfy the relative error constraint
   * on values ever closer to zero. You can think of 2^a as this minimum unit.
   * @param {number} options.b - The `b` parameter controls the width of bins on the
   * high end of the value range. To bound the relative error, every power-of-2 range
   * such as `[2, 4)` or `[4, 8)` is split into 2^b bins each, which upper-bounds the
   * relative error bound by `2^-b`.
   * @param {number} options.n? - the maximum encodable value `2^n-1`. (default: 53).
   * */
  constructor({ a, b, n = 53 }) {
    const c = a + b + 1;
    assertSafeInteger(a);
    assertSafeInteger(b);
    assertSafeInteger(n);
    assert(n <= 53, () => `expected n <= 53, got ${n}`);
    assert(c < 32, () => `expected cutoff c = a + b + 1 <= 32, got ${a + b + 1}`);

    this.a = u32(a);
    this.b = u32(b);
    this.n = u32(n);
    this.c = u32(c);

    // The maxiumum possible bin index (ie. numBins - 1)
    const maxCode = H2Encoding.maxCodeForParams({ a, b, n });
    assert(maxCode < 2 ** 32, `the number of bins for these parameters exceeds 2^32: ${maxCode + 1}`);
    this.maxCode = u32(maxCode);
  }

  /**
   * Convenience constructor that allows specifying a histogram with more intuitive parameters.
   * @param {object} options
   * @param {number} options.relativeError - relative error bound for this histogram, in (0, 1].
   * @param {number} [options.minimumUnit] - smallest distinguishable unit, below which we do not
   * care about relative error. Eg. if our data comes as nanoseconds but we only care about
   * relative error in terms of microseconds, minimumUnit should be set to 1000. (default: 1)
   * @param {number} [options.maxValue] - maximum encodable value (default: 2^53 - 1)
   */
  static params({ relativeError, minimumUnit = 1, maxValue = 2 ** 53 - 1 }) {
    assert(relativeError > 0 && relativeError <= 1, () => `expected relative error to be in (0, 1], got ${relativeError}`);
    // Since we use bit shifts to handle the parameters, we need `a` >= 0, so the minimum
    // unit must be a positive number greater than 1.
    // There's no conceptual issue with smaller numbers, but they are hard to support with bit math.
    assert(minimumUnit >= 1, () => `expected minimumUnit > 1, got ${minimumUnit}`);
    // Mandate that maxValue is an integer in order to avoid issues with floating-point
    // rounding, eg. Math.log2(1.0000000000000002 + 1) === 1
    assert(maxValue >= 1, () => `expected maxValue >= 1, got ${maxValue}`);
    assertSafeInteger(maxValue);
    const a = Math.floor(Math.log2(minimumUnit));
    let b = -Math.floor(Math.log2(relativeError));
    // since `2^n` is the first unrepresentable value,
    // add 1 to maxValue so that we can represent it.
    const n = Math.ceil(Math.log2(maxValue + 1));
    return new H2Encoding({ a, b, n });
  }

  /**
   * Return the bin index of the value, given this histogram's parameters.
   * Values can be any number (including non-integers) within the value range.
   * @param {number} value
   */
  encode(value) {
    // We allow non-integral inputs since JS numbers are 64-bit floats.
    const { a, b, c } = this;
    assertSafeInteger(value);
    assert(value >= 0 && value <= this.maxValue(), "expected value in histogram range [0, 2^n)");

    if (value < u32(1 << c)) {
      // We're below the cutoff.
      // The bin width below the cutoff is 1 << a and we can use a bit shift
      // to compute the bin since we know the value is less than 2^32.
      return value >>> a;
    }

    // We're above the cutoff.
    // Compute the bin offset by figuring out which log segment we're in,
    // as well as which bin inside that log segment we're in.

    // The log segment containing the value
    const v = Math.floor(Math.log2(value));

    // The bin offset within the v-th log segment.
    // To compute this with bit shifts: (value - u32(1 << v)) >>> (v - b)
    // - `value - (1 << v)` zeros the topmost (v-th) bit.
    // - `>>> (v - b)` extracts the top `b` bits of the value, corresponding
    //   to the bin index within the v-th log segment.
    //
    // To account for larger-than-32-bit values, however, we do this without bit shifts:
    const binsWithinSeg = Math.floor((value - 2 ** v) / 2 ** (v - b));
    DEBUG && assertSafeInteger(binsWithinSeg);

    // We want to calculate the number of bins that precede the v-th log segment.
    // 1. The linear section below the cutoff has twice as many bins as any log segment
    //    above the cutoff, for a total of 2^(b+1) = 2*2^b bins below the cutoff.
    // 2. Above the cutoff, there are `v - c` log segments before the v-th log segment,
    //    each with 2^b bins, for a total of (v - c) * 2^b bins above the cutoff.
    // Taken together, there are (v - c + 2) * 2^b bins preceding the v-th log segment.
    // Since the number of bins is always less than 2^32, this can be done with bit ops.
    const binsBelowSeg = u32((2 + v - c) << b);

    return binsBelowSeg + binsWithinSeg;
  }

  /**
   * @param {number} code
   */
  decode(code) {
    // todo: make this more efficient
    return { lower: this.lower(code), upper: this.upper(code) };
  }

  // todo: why is this so much simpler?
  // https://github.com/pelikan-io/rustcommon/blob/main/histogram/src/config.rs#L157C16-L157C16
  /**
   * Given a bin index, returns the lowest value that bin can contain.
   * @param {number} code
   */
  lower(code) {
    const { a, b, c } = this;

    // There are 2^(c - a) = 2^(b + 1) bins below the cutoff.
    const binsBelowCutoff = u32(1 << (c - a));
    if (code < binsBelowCutoff) {
      return u32(code << a);
    }

    // The number of bins in 0..code that are above the cutoff point
    const n = code - binsBelowCutoff;

    // The index of the log segment we're in: there are `c` log
    // segments below the cutoff and `n >> b` above, since each
    // one is divided into 2^b bins.
    const seg = c + (n >>> b);

    // By definition, the lowest value in a log segment is 2^seg
    // do this without bit shifts, since those return a 32-bit signed integer.
    const segStart = 2 ** seg;

    // The bin we're in within that segment, given by the low bits of n:
    // the bit shifts remove the `b` lowest bits, leaving only the high
    // bits, which we then subtract from `n` to keep only the low bits.
    const bin = n - u32((n >>> b) << b);

    // The width of an individual bin within this log segment (segStart >>> b)
    const binWidth = Math.floor(segStart / 2 ** b);

    // The lowest value represented by this bin is simple to compute:
    // start where the logarithmic segment begins, and increment by the
    // linear bin index within the segment times the bin width.
    return segStart + bin * binWidth;
  }

  /**
   * Given a bin index, returns the highest integer value that bin can contain.
   * For example, if the bin spans the range [0, 3], `upper` will return 3.
   * @param {number} code
   */
  upper(code) {
    DEBUG && assert(code <= this.maxCode, () => `code (${code}) cannot exceed maxCode (${this.maxCode})`);
    if (code === this.maxCode) {
      return this.maxValue();
    } else {
      return this.lower(code + 1) - 1;
    }
  }

  /**
   * Return the bin width of the given bin code.
   * @param {number} code
   */
  binWidth(code) {
    assert(0 <= code && code <= this.maxCode, `code (${code}) must be in [0, maxCode] ([0, ${this.maxCode}])`);
    return this.upper(code) - this.lower(code) + 1;
  }

  /**
   *  Return the maximum value representable by these histogram parameters.
   */
  maxValue() {
    return 2 ** this.n - 1;
  }

  /**
   * Absolute error on the low end of the histogram, below the cutoff
   */
  absoluteError() {
    return 2 ** this.a;
  }

  /**
   * Relative error on the high end of the histogram, above the cutoff
   */
  relativeError() {
    return 2 ** -this.b;
  }

  /**
   * Transition point below which is relative error and
   * above which is alsolute error
   */
  relativeAbsoluteCutoff() {
    return 2 ** this.c;
  }

  /**
   * Returns the number of bins represented by this encoding.
   * Note that the result may be 2^32, which exceeds the maximum
   * representable value of an unsigned 32-bit integer.
   */
  numBins() {
    return this.maxCode + 1;
  }

  /**
   * Return the maximum bin index for the given {a, b, n} parameters.
   * @param {{ a: number, b: number, n?: number }} options
   * */
  static maxCodeForParams({ a, b, n = 53 }) {
    const c = a + b + 1;
    // todo: should this check that the number of bins is a safe integer?
    if (n < c) {
      // Each log segment is covered by bins of width 2^a and there are n log segments,
      // giving us 2^(n - a) bins in total. Also, we always maintain a minimum of 1 bin.
      return 2 ** Math.max(n - a, 0) - 1;
    } else {
      // See the comment in `encode` about `binsBelowSeg` for a derivation of this expression
      return (2 + n - c) * 2 ** b - 1;
    }
  }
}

export class H2HistogramBuilder {
  /**
   * @param {H2Encoding} encoding
   */
  constructor(encoding) {
    // Use a Float64Array to permit counts up to 2^53.
    this.counts = new Float64Array(encoding.numBins());
    this.encoding = encoding;
  }

  /**
   * Increment the bin containing `value` by `count`.
   * @param {number} value
   */
  incrementValue(value, count = 1) {
    const bin = this.encoding.encode(value);
    this.counts[bin] += count;
  }

  /**
   * Increment the bin `bin` by `count`.
   * @param {number} bin
   */
  incrementBin(bin, count = 1) {
    this.counts[bin] += count;
  }

  /**
   * Import `counts` as represented in a dense Histogram
   * @param {number[] | Float64Array} counts
   */
  loadDenseCounts(counts) {
    for (let i = 0; i < counts.length; i++) {
      const index = i;
      const count = counts[i];
      this.incrementBin(index, count);
    }
  }

  /**
   * Import `bins` and `counts` as represented in a sparse Histogram
   * @param {number[] | Uint32Array} bins
   * @param {number[] | Float64Array} counts
   */
  loadSparseCounts(bins, counts) {
    assert(bins.length === counts.length, () => `bins.length (${bins.length}) must equal counts.length (${counts.length})`);
    for (let i = 0; i < bins.length; i++) {
      const index = bins[i];
      const count = counts[i];
      this.incrementBin(index, count);
    }
  }

  build() {
    // Sparsify by storing only the nonzero bins
    const bins = [];
    const counts = this.counts;
    for (let i = 0; i < counts.length; i++) {
      const count = counts[i];
      if (count > 0) {
        counts[bins.length] = count;
        bins.push(i);
      }
    }
    return new H2Histogram(this.encoding, bins, counts.subarray(0, bins.length));
  }
}

/**
 * Sparse histogram representation storing nonzero bins and their counts.
 */
export class H2Histogram {
  /**
   * @param {H2Encoding} encoding
   * @param {number[] | Uint32Array} bins
   * @param {number[] | Float64Array} counts
   */
  constructor(encoding, bins, counts) {
    assert(bins.length === counts.length, () => `bins.length (${bins.length}) must equal counts.length (${counts.length})`);
    // todo: assert no duplicates - or are duplicates fine (if inefficient)?
    // todo: could (should) we re-use the counts array?
    const cumulativeCounts = new Float64Array(counts);
    for (let i = 1; i < cumulativeCounts.length; i++) {
      cumulativeCounts[i] += cumulativeCounts[i - 1];
    }
    this.bins = bins;
    this.cumulativeCounts = cumulativeCounts;
    this.encoding = encoding;
    this.numObservations =
      counts.length === 0 ? 0 : this.cumulativeCounts[this.cumulativeCounts.length - 1];
  }

  /**
   * Return an upper bound on the number of observations at or below `value`.
   * @param {number} value
   */
  cumulativeCount(value) {
    if (this.numObservations === 0) {
      return 0;
    }

    if (value > this.encoding.maxValue()) {
      return this.numObservations;
    }

    // The index of the bin containing `value`.
    // We want to know the count up to and including this bin,
    // but not including any subsequent bins.
    const bin = this.encoding.encode(value);

    // The number of observations that are in or below that bin.
    // `i` tells us the index of the first bin above the bin containing `value`.
    const i = partitionPoint(this.bins.length, (i) => this.bins[i] <= bin);

    // We want the count from the bin before that one.
    return i === 0 ? 0 : this.cumulativeCounts[i - 1];
  }

  /**
   * Return an upper bound on the fraction of observations at or below `value` .
   * Like cumulative_count, but returns the fraction of the data rather than a count.
   * @param {number} value
   */
  cdf(value) {
    if (this.numObservations === 0) {
      return 1.0;
    }
    return this.cumulativeCount(value) / this.numObservations;
  }

  /**
   * Return an upper bound on the value of the q-th quantile.
   * Returns zero if the histogram contains no observations.
   * @param {number} q - the quantile, in [0, 1]
   */
  quantile(q) {
    DEBUG && assert(0 <= q && q <= 1, () => `expected quantile q to be in [0, 1], got ${q}`);

    if (this.numObservations === 0) {
      return 0;
    }

    // Number of observations at or below the q-th quantile
    const k = this.quantileToCount(q);
    // this.bins[i] is the index of the bin containing the k-th observation.
    // There are two levels of indexing here, since `bins` itself contains "indices"
    const i = Math.min(
      partitionPoint(
        this.cumulativeCounts.length,
        (i) => this.cumulativeCounts[i] < k
      ),
      this.cumulativeCounts.length - 1
    );

    // Maximum value in that bin

    return this.encoding.upper(this.bins[i]);
  }

  /**
   * Return an upper bound in [1, count] on the number of observations that lie
   * at or below the q-th quantile. E.g. if there are 2 observations,
   * - quantile_to_count(0) == 0
   * - quantile_to_count(0.25) == 1,
   * - quantile_to_count(0.75) == 2
   * - quantile_to_count(1.0) == 2
  /**
   * @param {number} q
   */
  quantileToCount(q) {
    DEBUG && assert(0.0 <= q && q <= 1.0, () => `expected quantile q to be in [0, 1], got ${q}`);
    if (q == 0.0) {
      return 1;
    }
    return Math.ceil(q * this.numObservations);
  }
}

/**
 * Returns the largest index for which `pred` returns true, plus one.
 * If the predicate does not return true for any index, returns 0.
 * The predicate function `pred` is required to be monotonic, ie.
 * to return `true` for all inputs below some cutoff, and `false`
 * for all inputs above that cutoff.
 *
 * This implementation is adapted from https://orlp.net/blog/bitwise-binary-search/
 *
 * That post contains optimized versions of this function, but here I opted for the
 * clearest implementation, at a slight performance cost.
 *
 * @param {number} n
 * @param {(index: number) => boolean} pred
 */
function partitionPoint(n, pred) {
  DEBUG && assert(n < 2 ** 32, () => `expected n to be < 2^32, got ${n}`);
  DEBUG && assertSafeInteger(n);
  let b = 0;
  let bit = bitFloor(n);
  while (bit !== 0) {
    const i = ((b | bit) - 1) >>> 0;
    if (i < n && pred(i)) {
      b |= bit;
    }
    bit >>>= 1;
  }
  return b >>> 0;
}

/**
 * If x is not zero, calculates the largest integral power of two that is not greater than x.
 * If x is zero, returns zero.
 * Like the function in the C++ standard library: https://en.cppreference.com/w/cpp/numeric/bit_floor
 * @param {number} n
 */
function bitFloor(n) {
  DEBUG && assert(n < 2 ** 32, () => `expected n to be < 2^32, got ${n}`);
  if (n === 0) {
    return 0;
  }
  const msb = 31 - Math.clz32(n);
  return (1 << msb) >>> 0;
}

/**
 * Coerces x to an unsigned 32-bit unsigned integer. This is provided as
 * a convenience function on top of unsigned shift that does some sanity
 * checks in debug mode.
 * @param {number} x
 */
function u32(x) {
  DEBUG && assert(Number.isInteger(x), () => `expected integer x, got ${x}`);
  // Allow bit patterns representing negative numbers, eg. 1 << 31
  DEBUG && assert(Math.abs(x) < 2 ** 32, () => `expected x < 2^32, got ${x}`);
  return x >>> 0;
}

/**
 * A miniature implementation of H2 histogram encoding for values <= 2^32-1.
 * Returns the bin index of the bin containing `value`.
 *
 * @param {number} value
 * @param {number} a
 * @param {number} b
 */
export function encode32(value, a, b) {
  assertValid32(value, a, b);
  const c = a + b + 1;
  if (value < u32(1 << c)) return value >>> a;
  const logSegment = u32(31 - Math.clz32(value));
  return u32((value >>> (logSegment - b)) + u32((logSegment - c + 1) << b));
}

/**
 * A miniature implementation of H2 histogram decoding for values <= 2^32-1.
 * Returns an object { lower, upper } representing the inclusive bounds
 * [lower, upper] for the `index`-th bin.
 *
 * @param {number} index
 * @param {number} a
 * @param {number} b
 */
export function decode32(index, a, b) {
  assertValid32(index, a, b);
  const c = a + b + 1;
  let lower, binWidth;
  const binsBelowCutoff = u32(1 << (c - a));
  if (index < binsBelowCutoff) {
    // we're in the linear section of the histogram
    // where each bin is 2^a wide
    lower = u32(index << a);
    binWidth = u32(1 << a);
  } else {
    // we're in the log section of the histogram
    // with 2^b bins per log segment
    const logSegment = c + ((index - binsBelowCutoff) >>> b);
    const binOffset = index & (u32(1 << b) - 1);
    lower = u32(1 << logSegment) + u32(binOffset << (logSegment - b));
    binWidth = u32(1 << (logSegment - b));
  }
  return { lower, upper: u32(lower + (binWidth - 1)) };
}

/**
 * Common assertions on the input arguments to encode32 and decode32.
 *
 * @param {number} x - code or value
 * @param {number} a - histogram `a` parameter
 * @param {number} b - histogram `b` parameter
 */
function assertValid32(x, a, b) {
  assertSafeInteger(x);
  assertSafeInteger(a);
  assertSafeInteger(b);
  assert(x <= 2 ** 32 - 1, () => `expected x < 2^32, got ${x}`);
  assert(a + b + 1 < 32, () => `expected a + b + 1 < 32, got ${a + b + 1}`);
}

/**
 *
 * @param {boolean} condition
 * @param {string | (() => string) } [message] - error message as a string or zero-argument function,
 * to allow deferring the evaluation of an expensive message until the time an error occurs.
 */
function assert(condition, message) {
  const prefix = 'assertion error';
  if (condition !== true) {
    const text = typeof message === "function" ? message() : message;
    throw new Error(text === undefined ? prefix : `${prefix}: ${text}`);
  }
};

/**
 * @param {number} x
 */
function assertSafeInteger(x) {
  assert(Number.isSafeInteger(x), () => `expected safe integer, got ${x}`);
}

/**
 * @param {any} x
 */
function assertDefined(x) {
  assert(x !== undefined, 'expected a defined value, got undefined');
};

// ---------------------------------------------------------------------------
// Canonical-compatible API
// ---------------------------------------------------------------------------
//
// The types below mirror the iopsystems `histogram` crate (the canonical Rust
// implementation) and its Python and Go ports, using the same
// `groupingPower` / `maxValuePower` parameters and the same `Config`,
// `Histogram`, `SparseHistogram`, `CumulativeHistogram` and `Bucket` surface.
//
// In the crate's terms, the linear region always uses width-1 buckets, which
// corresponds to `a = 0` in the `H2Encoding` above. So a canonical `Config`
// with `groupingPower = g` and `maxValuePower = n` is exactly
// `new H2Encoding({ a: 0, b: g, n })`, and this layer reuses that
// (property-tested) encoding to guarantee byte-for-byte identical bucketing.
//
// The one deviation from the canonical implementation is the value range:
// JavaScript numbers are 64-bit floats, so the largest `maxValuePower` we
// support is 53 (values up to 2^53 - 1) rather than 64.

/** The largest `maxValuePower` representable with 64-bit float integers. */
export const MAX_VALUE_POWER = 53;

/**
 * Immutable bucketing configuration, equivalent to the crate's `Config`.
 *
 * Construct with `Config.new(groupingPower, maxValuePower)`.
 */
export class Config {
  /**
   * @param {number} groupingPower - number of buckets spanning each power of
   *   two; the relative error is `2^-groupingPower`.
   * @param {number} [maxValuePower] - largest representable value is
   *   `2^maxValuePower - 1` (default 53, the JS maximum).
   */
  constructor(groupingPower, maxValuePower = MAX_VALUE_POWER) {
    assertSafeInteger(groupingPower);
    assertSafeInteger(maxValuePower);
    assert(maxValuePower <= MAX_VALUE_POWER, () => `max_value_power must be <= ${MAX_VALUE_POWER}, got ${maxValuePower}`);
    assert(groupingPower >= 0 && maxValuePower >= 0, () => `grouping_power and max_value_power must be non-negative`);
    assert(groupingPower < maxValuePower, () => `grouping_power (${groupingPower}) must be less than max_value_power (${maxValuePower})`);

    this.groupingPower = groupingPower;
    this.maxValuePower = maxValuePower;
    // The linear region uses width-1 buckets, i.e. a = 0 in H2Encoding terms.
    this.encoding = new H2Encoding({ a: 0, b: groupingPower, n: maxValuePower });
    this.cutoffPower = groupingPower + 1;
    this.cutoffValue = 2 ** this.cutoffPower;
    this.max = this.encoding.maxValue();
    Object.freeze(this.encoding);
    Object.freeze(this);
  }

  /**
   * Create and validate a Config. Mirrors the crate's constructor.
   * @param {number} groupingPower
   * @param {number} [maxValuePower]
   */
  static new(groupingPower, maxValuePower = MAX_VALUE_POWER) {
    return new Config(groupingPower, maxValuePower);
  }

  /**
   * Infer a config from a known bucket count and `maxValuePower`. Useful for
   * dense columns that store only the counts. Throws if no grouping power
   * produces `totalBuckets`.
   * @param {number} totalBuckets
   * @param {number} [maxValuePower]
   */
  static fromTotalBuckets(totalBuckets, maxValuePower = MAX_VALUE_POWER) {
    for (let groupingPower = 0; groupingPower < maxValuePower; groupingPower++) {
      const candidate = new Config(groupingPower, maxValuePower);
      if (candidate.totalBuckets === totalBuckets) {
        return candidate;
      }
    }
    throw new Error(`no grouping_power with max_value_power=${maxValuePower} yields ${totalBuckets} buckets`);
  }

  /** Total number of buckets for this configuration. */
  get totalBuckets() {
    return this.encoding.numBins();
  }

  /**
   * Relative error (as a percentage) of the logarithmic buckets. Linear
   * buckets have width 1 and no error; a config with no logarithmic buckets
   * has zero error.
   */
  error() {
    if (this.groupingPower === this.maxValuePower - 1) {
      return 0.0;
    }
    return 100.0 / 2 ** this.groupingPower;
  }

  /**
   * Return the bucket index that `value` falls into. Throws if the value is
   * out of range.
   * @param {number} value
   */
  valueToIndex(value) {
    return this.encoding.encode(value);
  }

  /**
   * Inclusive lower bound of the bucket at `index`.
   * @param {number} index
   */
  indexToLowerBound(index) {
    return this.encoding.lower(index);
  }

  /**
   * Inclusive upper bound of the bucket at `index`.
   * @param {number} index
   */
  indexToUpperBound(index) {
    return this.encoding.upper(index);
  }

  /**
   * Inclusive `[lower, upper]` range for the bucket at `index`.
   * @param {number} index
   */
  indexToRange(index) {
    return [this.indexToLowerBound(index), this.indexToUpperBound(index)];
  }

  /**
   * @param {Config} other
   */
  equals(other) {
    return other instanceof Config
      && this.groupingPower === other.groupingPower
      && this.maxValuePower === other.maxValuePower;
  }
}

/**
 * A single histogram bucket: a count and an inclusive value range.
 */
export class Bucket {
  /**
   * @param {number} count
   * @param {number} start - inclusive lower bound
   * @param {number} end - inclusive upper bound
   */
  constructor(count, start, end) {
    this.count = count;
    this.start = start;
    this.end = end;
  }

  /** The inclusive `[start, end]` range of the bucket. */
  get range() {
    return [this.start, this.end];
  }

  /** Arithmetic midpoint of the range; a reasonable point estimate. */
  get midpoint() {
    return (this.start + this.end) / 2;
  }

  /** Number of distinct integer values the bucket covers. */
  get width() {
    return this.end - this.start + 1;
  }
}

/**
 * A dense h2 histogram with a counter for every bucket.
 *
 * This is the JS analogue of the crate's `Histogram`. Counts are stored in a
 * `Float64Array`, so per-bucket counts are exact up to Number.MAX_SAFE_INTEGER.
 */
export class Histogram {
  /**
   * @param {number} groupingPower
   * @param {number} [maxValuePower]
   * @param {{ config?: Config }} [options]
   */
  constructor(groupingPower, maxValuePower = MAX_VALUE_POWER, { config } = {}) {
    this.config = config ?? new Config(groupingPower, maxValuePower);
    this.buckets = new Float64Array(this.config.totalBuckets);
  }

  /**
   * Create an empty histogram from an existing Config.
   * @param {Config} config
   */
  static withConfig(config) {
    return new Histogram(config.groupingPower, config.maxValuePower, { config });
  }

  /**
   * Create a histogram from a full, dense list of bucket counts.
   * @param {number} groupingPower
   * @param {number} maxValuePower
   * @param {number[] | Float64Array} buckets
   */
  static fromBuckets(groupingPower, maxValuePower, buckets) {
    const config = new Config(groupingPower, maxValuePower);
    assert(buckets.length === config.totalBuckets, () => `expected ${config.totalBuckets} buckets, got ${buckets.length}`);
    const h = Histogram.withConfig(config);
    for (const count of buckets) checkedCount(count);
    h.buckets.set(buckets);
    return h;
  }

  /** Total number of observations recorded. */
  totalCount() {
    let total = 0;
    for (let i = 0; i < this.buckets.length; i++) {
      total = checkedCount(total + checkedCount(this.buckets[i]));
    }
    return total;
  }

  /**
   * Add one observation of `value` (or `count` observations).
   * @param {number} value
   * @param {number} [count]
   */
  increment(value, count = 1) {
    checkedCount(count);
    const index = this.config.valueToIndex(value);
    this.buckets[index] = checkedCount(checkedCount(this.buckets[index]) + count);
  }

  /**
   * Add `count` observations of `value`. Alias for `increment`.
   * @param {number} value
   * @param {number} [count]
   */
  record(value, count = 1) {
    this.increment(value, count);
  }

  /**
   * Record many values at once. If `counts` is provided it must be the same
   * length as `values` and supplies a weight for each value.
   * @param {Iterable<number>} values
   * @param {Iterable<number>} [counts]
   */
  recordMany(values, counts) {
    if (counts !== undefined) {
      const vi = values[Symbol.iterator]();
      const ci = counts[Symbol.iterator]();
      while (true) {
        const v = vi.next();
        const c = ci.next();
        if (v.done || c.done) break;
        this.record(v.value, c.value);
      }
      return;
    }
    for (const value of values) {
      this.increment(value);
    }
  }

  /** Iterate every bucket (including empty ones) as `Bucket` objects. */
  *[Symbol.iterator]() {
    for (let i = 0; i < this.buckets.length; i++) {
      const [start, end] = this.config.indexToRange(i);
      yield new Bucket(this.buckets[i], start, end);
    }
  }

  /** Return only the buckets with a non-zero count. */
  nonzeroBuckets() {
    const out = [];
    for (let i = 0; i < this.buckets.length; i++) {
      if (this.buckets[i] > 0) {
        const [start, end] = this.config.indexToRange(i);
        out.push(new Bucket(this.buckets[i], start, end));
      }
    }
    return out;
  }

  /**
   * @param {Histogram} other
   */
  _checkCompatible(other) {
    assert(this.config.equals(other.config), () => `histograms have incompatible configurations`);
  }

  /** Clear counters while retaining the backing Float64Array. */
  reset() {
    this.buckets.fill(0);
    return this;
  }

  /** Copy into compatible existing storage; requires exclusive access.
   * @param {Histogram} destination
   */
  snapshotInto(destination) {
    this._checkCompatible(destination);
    assert(this.buckets.length === this.config.totalBuckets && destination.buckets.length === this.config.totalBuckets, 'invalid dense shape');
    for (const count of this.buckets) checkedCount(count);
    destination.buckets.set(this.buckets);
    return destination;
  }

  /** Copy then reset; neither operation is concurrent/atomic.
   * @param {Histogram} destination
   */
  drainInto(destination) {
    assert(destination !== this && destination.buckets.buffer !== this.buckets.buffer, 'cannot drain into aliased storage');
    this.snapshotInto(destination);
    this.reset();
    return destination;
  }

  /** Validate every bucket before changing any destination count.
   * @param {Histogram} other
   */
  checkedAddAssign(other) {
    this._checkCompatible(other);
    assert(this.buckets.length === this.config.totalBuckets && other.buckets.length === this.config.totalBuckets, 'invalid dense shape');
    for (let i = 0; i < this.buckets.length; i++) {
      checkedCount(checkedCount(this.buckets[i]) + checkedCount(other.buckets[i]));
    }
    // Overlapping views with different offsets would invalidate the preflight.
    assert(this.buckets.buffer !== other.buckets.buffer || this.buckets.byteOffset === other.buckets.byteOffset, 'overlapping dense storage');
    for (let i = 0; i < this.buckets.length; i++) this.buckets[i] += other.buckets[i];
    return this;
  }

  /** Owned sum: validates all configurations before constructing output.
   * @param {Histogram[]} histograms
   */
  static checkedSum(histograms) {
    assert(histograms.length > 0, 'cannot sum an empty histogram collection');
    const first = histograms[0];
    for (const histogram of histograms) first._checkCompatible(histogram);
    const result = Histogram.withConfig(first.config);
    for (const histogram of histograms) result.checkedAddAssign(histogram);
    return result;
  }

  /**
   * Return a new histogram that is the element-wise sum of both. Both must
   * share the same configuration.
   * @param {Histogram} other
   */
  merge(other) {
    return Histogram.checkedSum([this, other]);
  }

  /**
   * Return a new histogram that is the element-wise difference. Throws if any
   * bucket would go negative.
   * @param {Histogram} other
   */
  subtract(other) {
    this._checkCompatible(other);
    const result = Histogram.withConfig(this.config);
    for (let i = 0; i < this.buckets.length; i++) {
      const diff = this.buckets[i] - other.buckets[i];
      assert(diff >= 0, () => `subtraction would produce a negative bucket count`);
      result.buckets[i] = diff;
    }
    return result;
  }

  /**
   * Return a coarser histogram with a smaller `groupingPower`. The new
   * grouping power must be strictly less than the current one.
   * @param {number} groupingPower
   */
  downsample(groupingPower) {
    assert(groupingPower < this.config.groupingPower, () => `target grouping_power must be less than the current grouping_power`);
    const result = new Histogram(groupingPower, this.config.maxValuePower);
    for (let i = 0; i < this.buckets.length; i++) {
      const count = this.buckets[i];
      if (count > 0) {
        result.record(this.config.indexToLowerBound(i), count);
      }
    }
    return result;
  }

  /** Write ordered pairs into a reusable array; empty histograms clear it and return null.
   * @param {number[]} percentiles
   * @param {[number, Bucket][]} output
   * @returns {[number, Bucket][] | null}
   */
  percentilesInto(percentiles, output) {
    return queryInto(this, percentiles, output);
  }

  /**
   * Return the bucket at a single `percentile` in [0, 1], or `null` if the
   * histogram is empty. `0.5` is the median (same convention as the crate).
   * @param {number} percentile
   */
  percentile(percentile) {
    checkPercentile(percentile);
    const total = this.totalCount();
    if (total === 0) return null;
    const target = Math.max(1, Math.ceil(percentile * total));
    let running = 0;
    for (let i = 0; i < this.buckets.length; i++) {
      running += this.buckets[i];
      if (running >= target) return new Bucket(this.buckets[i], this.config.indexToLowerBound(i), this.config.indexToUpperBound(i));
    }
    return null;
  }

  /**
   * Return `[percentile, Bucket]` pairs for each requested percentile, in the
   * original order. Returns `null` if the histogram is empty.
   * @param {number[]} percentiles
   * @returns {[number, Bucket][] | null}
   */
  percentiles(percentiles) {
    for (const p of percentiles) {
      assert(p >= 0 && p <= 1, () => `percentiles must be in the range [0, 1], got ${p}`);
    }
    const total = this.totalCount();
    if (total === 0) {
      return null;
    }

    const sortedUnique = Array.from(new Set(percentiles)).sort((x, y) => x - y);
    /** @type {Map<number, Bucket>} */
    const results = new Map();
    let bucketIdx = 0;
    let partialSum = this.buckets[0];

    for (const p of sortedUnique) {
      const target = Math.max(1, Math.ceil(p * total));
      while (true) {
        if (partialSum >= target || bucketIdx === this.buckets.length - 1) {
          const [start, end] = this.config.indexToRange(bucketIdx);
          results.set(p, new Bucket(this.buckets[bucketIdx], start, end));
          break;
        }
        bucketIdx += 1;
        partialSum += this.buckets[bucketIdx];
      }
    }

    /** @type {[number, Bucket][]} */
    const out = [];
    for (const p of percentiles) {
      out.push([p, /** @type {Bucket} */ (results.get(p))]);
    }
    return out;
  }

  /**
   * Alias for `percentile` (the crate uses `quantile`).
   * @param {number} quantile
   */
  quantile(quantile) {
    return this.percentile(quantile);
  }

  /** Convert to the sparse (columnar) representation. */
  toSparse() {
    return SparseHistogram.fromHistogram(this);
  }

  /** Convert to a read-only cumulative histogram for fast quantiles. */
  toCumulative() {
    return CumulativeHistogram.fromHistogram(this);
  }

  /**
   * @param {Histogram} other
   */
  equals(other) {
    if (!(other instanceof Histogram) || !this.config.equals(other.config)) {
      return false;
    }
    if (this.buckets.length !== other.buckets.length) {
      return false;
    }
    for (let i = 0; i < this.buckets.length; i++) {
      if (this.buckets[i] !== other.buckets[i]) {
        return false;
      }
    }
    return true;
  }
}

/**
 * Sparse, columnar representation storing only non-zero buckets as parallel
 * `index` / `count` arrays in ascending index order. Equivalent to the crate's
 * `SparseHistogram` and the form Rezolus uses for its columnar layout.
 */
export class SparseHistogram {
  /**
   * @param {Config} config
   * @param {number[]} index
   * @param {number[]} count
   */
  constructor(config, index = [], count = []) {
    this.config = config;
    validateParts(config, index, count, false);
    this.index = Object.freeze(Array.from(index));
    this.count = Object.freeze(Array.from(count));
    Object.defineProperties(this, { config: { writable: false }, index: { writable: false }, count: { writable: false } });
  }

  /**
   * Build a sparse histogram from a dense Histogram.
   * @param {Histogram} histogram
   */
  static fromHistogram(histogram) {
    const index = [];
    const count = [];
    for (let i = 0; i < histogram.buckets.length; i++) {
      const c = checkedCount(histogram.buckets[i]);
      if (c > 0) {
        index.push(i);
        count.push(c);
      }
    }
    return new SparseHistogram(histogram.config, index, count);
  }

  /**
   * Create a sparse histogram from raw parts, validating invariants.
   * @param {Config} config
   * @param {number[] | Uint32Array} index
   * @param {number[] | Float64Array} count
   */
  static fromParts(config, index, count) {
    assert(index.length === count.length, () => `index and count must have the same length`);
    const total = config.totalBuckets;
    let prev = -1;
    for (const i of index) {
      assert(i >= 0 && i < total, () => `index ${i} out of range for config`);
      assert(i > prev, () => `indices must be strictly ascending`);
      prev = i;
    }
    return new SparseHistogram(config, Array.from(index), Array.from(count));
  }

  /** @param {SparseHistogram | CumulativeHistogram} other */
  merge(other) {
    const parts = mergeParts(this, other);
    return new SparseHistogram(this.config, parts.index, parts.count);
  }

  /** @param {number} groupingPower */
  downsample(groupingPower) {
    const parts = downsampleParts(this, groupingPower);
    return new SparseHistogram(parts.config, parts.index, parts.count);
  }

  get length() {
    return this.index.length;
  }

  isEmpty() {
    return this.index.length === 0;
  }

  totalCount() {
    let total = 0;
    for (const c of this.count) {
      total = checkedCount(total + c);
    }
    return total;
  }

  /** Iterate non-zero buckets as `Bucket` objects. */
  *[Symbol.iterator]() {
    for (let k = 0; k < this.index.length; k++) {
      const [start, end] = this.config.indexToRange(this.index[k]);
      yield new Bucket(this.count[k], start, end);
    }
  }

  /** Convert to a dense Histogram. */
  toDense() {
    const h = Histogram.withConfig(this.config);
    for (let k = 0; k < this.index.length; k++) {
      h.buckets[this.index[k]] = this.count[k];
    }
    return h;
  }

  /** Convert to a read-only CumulativeHistogram. */
  toCumulative() {
    return CumulativeHistogram.fromSparse(this);
  }

  /** Write ordered pairs into a reusable array; empty histograms clear it and return null.
   * @param {number[]} percentiles
   * @param {[number, Bucket][]} output
   * @returns {[number, Bucket][] | null}
   */
  percentilesInto(percentiles, output) {
    return queryInto(this, percentiles, output);
  }

  /** @param {number} percentile */
  percentile(percentile) {
    checkPercentile(percentile);
    const total = this.totalCount();
    if (total === 0) return null;
    const target = Math.max(1, Math.ceil(percentile * total));
    let running = 0;
    for (let k = 0; k < this.index.length; k++) {
      running += this.count[k];
      if (running >= target) return new Bucket(this.count[k], this.config.indexToLowerBound(this.index[k]), this.config.indexToUpperBound(this.index[k]));
    }
    return null;
  }

  /**
   * @param {number[]} percentiles
   */
  percentiles(percentiles) {
    return this.percentilesInto(percentiles, []);
  }
}

/**
 * Read-only histogram with cumulative counts for fast quantile queries.
 * Corresponds to the crate's `CumulativeROHistogram`: it stores only non-zero
 * buckets, but `count[i]` is the running prefix sum, so percentile queries are
 * answered with a binary search (O(log n)). A midpoint-estimated `mean` is
 * computed once at construction.
 */
export class CumulativeHistogram {
  /**
   * @param {Config} config
   * @param {number[]} index
   * @param {number[]} count - cumulative (prefix-sum) counts
   * @param {{ validate?: boolean }} [options]
   */
  constructor(config, index, count, { validate = true } = {}) {
    this.config = config;
    // The legacy validate option remains accepted, but cannot bypass invariants.
    validateParts(config, index, count, true);
    this.index = Object.freeze(Array.from(index));
    this.count = Object.freeze(Array.from(count));
    this._mean = this._computeMean();
    Object.defineProperties(this, { config: { writable: false }, index: { writable: false }, count: { writable: false }, _mean: { writable: false } });
  }

  /**
   * Create from raw parts. `count` must be cumulative (prefix sums).
   * @param {Config} config
   * @param {number[]} index
   * @param {number[]} count
   */
  static fromParts(config, index, count) {
    return new CumulativeHistogram(config, Array.from(index), Array.from(count));
  }

  /**
   * Build from a dense Histogram.
   * @param {Histogram} histogram
   */
  static fromHistogram(histogram) {
    const index = [];
    const count = [];
    let running = 0;
    for (let i = 0; i < histogram.buckets.length; i++) {
      const n = checkedCount(histogram.buckets[i]);
      if (n > 0) {
        running = checkedCount(running + n);
        index.push(i);
        count.push(running);
      }
    }
    return new CumulativeHistogram(histogram.config, index, count, { validate: false });
  }

  /**
   * Build from a SparseHistogram.
   * @param {SparseHistogram} sparse
   */
  static fromSparse(sparse) {
    const index = [];
    const count = [];
    let running = 0;
    for (let i = 0; i < sparse.length; i++) {
      const n = sparse.count[i];
      if (n === 0) continue;
      running = checkedCount(running + n);
      index.push(sparse.index[i]);
      count.push(running);
    }
    return new CumulativeHistogram(sparse.config, index, count, { validate: false });
  }

  _validate() {
    validateParts(this.config, this.index, this.count, true);
  }

  /**
   * @param {number} position
   */
  _individualCount(position) {
    return position === 0 ? this.count[0] : this.count[position] - this.count[position - 1];
  }

  _computeMean() {
    if (this.count.length === 0) {
      return null;
    }
    const total = this.count[this.count.length - 1];
    if (total === 0) {
      return null;
    }
    let weighted = 0;
    for (let i = 0; i < this.index.length; i++) {
      const [start, end] = this.config.indexToRange(this.index[i]);
      weighted += ((start + end) / 2) * this._individualCount(i);
    }
    return weighted / total;
  }

  /** @param {SparseHistogram | CumulativeHistogram} other */
  merge(other) {
    const parts = mergeParts(this, other);
    return cumulativeParts(this.config, parts.index, parts.count);
  }

  /** @param {number} groupingPower */
  downsample(groupingPower) {
    const parts = downsampleParts(this, groupingPower);
    return cumulativeParts(parts.config, parts.index, parts.count);
  }

  get length() {
    return this.index.length;
  }

  isEmpty() {
    return this.index.length === 0;
  }

  totalCount() {
    return this.count.length === 0 ? 0 : this.count[this.count.length - 1];
  }

  /** Midpoint-estimated mean of all observations, or `null` if empty. */
  mean() {
    return this._mean;
  }

  /**
   * First position whose cumulative count is >= `target`.
   * @param {number} target
   */
  _findQuantilePosition(target) {
    const pos = bisectLeft(this.count, target);
    return Math.min(pos, this.count.length - 1);
  }

  /** Write ordered pairs into a reusable array; empty histograms clear it and return null.
   * @param {number[]} percentiles
   * @param {[number, Bucket][]} output
   * @returns {[number, Bucket][] | null}
   */
  percentilesInto(percentiles, output) {
    return queryInto(this, percentiles, output);
  }

  /**
   * Return the Bucket at `percentile` in [0, 1] (individual count), or `null`
   * if the histogram is empty.
   * @param {number} percentile
   */
  percentile(percentile) {
    checkPercentile(percentile);
    const total = this.totalCount();
    if (total === 0) return null;
    const pos = this._findQuantilePosition(Math.max(1, Math.ceil(percentile * total)));
    const index = this.index[pos];
    return new Bucket(this._individualCount(pos), this.config.indexToLowerBound(index), this.config.indexToUpperBound(index));
  }

  /**
   * Return `[percentile, Bucket]` pairs, one per requested percentile.
   * @param {number[]} percentiles
   * @returns {[number, Bucket][] | null}
   */
  percentiles(percentiles) {
    for (const p of percentiles) {
      assert(p >= 0 && p <= 1, () => `percentiles must be in the range [0, 1], got ${p}`);
    }
    if (this.count.length === 0) {
      return null;
    }
    const total = this.count[this.count.length - 1];
    if (total === 0) {
      return null;
    }
    /** @type {[number, Bucket][]} */
    const out = [];
    for (const p of percentiles) {
      const target = Math.max(1, Math.ceil(p * total));
      const pos = this._findQuantilePosition(target);
      const [start, end] = this.config.indexToRange(this.index[pos]);
      out.push([p, new Bucket(this._individualCount(pos), start, end)]);
    }
    return out;
  }

  /**
   * @param {number} quantile
   */
  quantile(quantile) {
    return this.percentile(quantile);
  }

  /**
   * Return `[lower, upper]` quantile fractions for the `bucketIdx`-th stored
   * bucket, or `null` if empty or out of range.
   * @param {number} bucketIdx
   */
  bucketQuantileRange(bucketIdx) {
    if (bucketIdx < 0 || bucketIdx >= this.count.length) {
      return null;
    }
    const total = this.count[this.count.length - 1];
    if (total === 0) {
      return null;
    }
    const lower = bucketIdx === 0 ? 0 : this.count[bucketIdx - 1] / total;
    const upper = this.count[bucketIdx] / total;
    return [lower, upper];
  }

  /** Iterate non-zero buckets (with individual counts) as `Bucket` objects. */
  *[Symbol.iterator]() {
    for (let i = 0; i < this.index.length; i++) {
      const [start, end] = this.config.indexToRange(this.index[i]);
      yield new Bucket(this._individualCount(i), start, end);
    }
  }

  /** Iterate `[Bucket, lowerQuantile, upperQuantile]` per non-zero bucket. */
  *iterWithQuantiles() {
    const total = this.count.length === 0 ? 0 : this.count[this.count.length - 1];
    for (let i = 0; i < this.index.length; i++) {
      const lower = i === 0 ? 0 : this.count[i - 1] / total;
      const upper = this.count[i] / total;
      const [start, end] = this.config.indexToRange(this.index[i]);
      yield [new Bucket(this._individualCount(i), start, end), lower, upper];
    }
  }

  /** Convert prefix counts directly to independent sparse columns. */
  toSparse() {
    return new SparseHistogram(this.config, Array.from(this.index), this.count.map((_, i) => this._individualCount(i)));
  }

  /** Reconstruct a dense Histogram. */
  toDense() {
    const h = Histogram.withConfig(this.config);
    for (let i = 0; i < this.index.length; i++) {
      h.buckets[this.index[i]] = this._individualCount(i);
    }
    return h;
  }
}

/**
 * Return the leftmost index at which `target` could be inserted into the sorted
 * array `arr` to keep it sorted, i.e. the first index `i` with `arr[i] >= target`.
 * @param {readonly number[] | Float64Array} arr
 * @param {number} target
 */
function bisectLeft(arr, target) {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (arr[mid] < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

/** @param {number} count */
function checkedCount(count) {
  assert(Number.isSafeInteger(count) && count >= 0, 'count must be a non-negative safe integer');
  return count;
}

/** @param {number} percentile */
function checkPercentile(percentile) {
  assert(Number.isFinite(percentile) && percentile >= 0 && percentile <= 1, 'percentiles must be in the range [0, 1]');
}

/** @param {Config} config @param {ArrayLike<number>} index @param {ArrayLike<number>} count @param {boolean} cumulative */
function validateParts(config, index, count, cumulative) {
  assert(index.length === count.length, 'index and count must have the same length');
  let previousIndex = -1, previousCount = 0;
  for (let k = 0; k < index.length; k++) {
    assert(Number.isSafeInteger(index[k]) && index[k] > previousIndex && index[k] < config.totalBuckets, 'indices must be strictly ascending integers in range');
    checkedCount(count[k]);
    if (cumulative) assert(count[k] > 0 && count[k] >= previousCount, 'cumulative counts must be positive and non-decreasing');
    previousIndex = index[k]; previousCount = count[k];
  }
}

/** @param {Histogram | SparseHistogram | CumulativeHistogram} histogram @param {number[]} percentiles @param {[number, Bucket][]} output */
function queryInto(histogram, percentiles, output) {
  assert(percentiles !== /** @type {unknown} */ (output), 'requests and output must not alias');
  for (const p of percentiles) checkPercentile(p);
  const total = histogram.totalCount();
  if (total === 0) { output.length = 0; return null; }
  for (let q = 0; q < percentiles.length; q++) {
    const p = percentiles[q], target = Math.max(1, Math.ceil(p * total));
    let index = 0, count = 0;
    if (histogram instanceof CumulativeHistogram) {
      const pos = histogram._findQuantilePosition(target);
      index = histogram.index[pos]; count = histogram._individualCount(pos);
    } else {
      let running = 0;
      const counts = histogram instanceof Histogram ? histogram.buckets : histogram.count;
      for (let i = 0; i < counts.length; i++) {
        running += counts[i];
        if (running >= target) {
          index = histogram instanceof Histogram ? i : histogram.index[i]; count = counts[i]; break;
        }
      }
    }
    const bucket = new Bucket(count, histogram.config.indexToLowerBound(index), histogram.config.indexToUpperBound(index));
    if (output[q]) { output[q][0] = p; output[q][1] = bucket; }
    else output[q] = [p, bucket];
  }
  output.length = percentiles.length;
  return output;
}

/** @param {SparseHistogram | CumulativeHistogram} histogram @param {number} position */
function individualCount(histogram, position) {
  return histogram instanceof CumulativeHistogram ? histogram._individualCount(position) : histogram.count[position];
}

/** @param {SparseHistogram | CumulativeHistogram} a @param {SparseHistogram | CumulativeHistogram} b */
function mergeParts(a, b) {
  assert(a.config.equals(b.config), 'histograms have incompatible configurations');
  /** @type {number[]} */
  const index = [];
  /** @type {number[]} */
  const count = [];
  let i = 0, j = 0;
  while (i < a.length || j < b.length) {
    let idx, n;
    if (j === b.length || (i < a.length && a.index[i] < b.index[j])) {
      idx = a.index[i]; n = individualCount(a, i++);
    } else if (i === a.length || b.index[j] < a.index[i]) {
      idx = b.index[j]; n = individualCount(b, j++);
    } else {
      idx = a.index[i]; n = checkedCount(individualCount(a, i++) + individualCount(b, j++));
    }
    if (n > 0) { index.push(idx); count.push(n); }
  }
  return { index, count };
}

/** @param {SparseHistogram | CumulativeHistogram} histogram @param {number} groupingPower */
function downsampleParts(histogram, groupingPower) {
  assert(groupingPower < histogram.config.groupingPower, 'target grouping_power must be less than the current grouping_power');
  const config = new Config(groupingPower, histogram.config.maxValuePower);
  /** @type {number[]} */
  const index = [];
  /** @type {number[]} */
  const count = [];
  for (let i = 0; i < histogram.length; i++) {
    const n = individualCount(histogram, i);
    if (n === 0) continue;
    const idx = config.valueToIndex(histogram.config.indexToLowerBound(histogram.index[i]));
    if (index.length && index[index.length - 1] === idx) count[count.length - 1] = checkedCount(count[count.length - 1] + n);
    else { index.push(idx); count.push(n); }
  }
  return { config, index, count };
}

/** @param {Config} config @param {number[]} index @param {number[]} count */
function cumulativeParts(config, index, count) {
  let running = 0;
  for (let i = 0; i < count.length; i++) count[i] = running = checkedCount(running + count[i]);
  return new CumulativeHistogram(config, index, count);
}
