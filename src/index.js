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
    DEBUG && assert(code <= this.maxCode);
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
    assert(0 <= code && code <= this.maxCode);
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
    assert(bins.length === counts.length);
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
    DEBUG && assert(0 <= q && q <= 1);

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
    DEBUG && assert(0.0 <= q && q <= 1.0);
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
  DEBUG && assert(n < 2 ** 32);
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
  DEBUG && assert(n < 2 ** 32);
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
  DEBUG && assert(Number.isInteger(x));
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
  return u32((value >>> (logSegment - b)) + ((logSegment - c + 1) << b));
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
  assert(x <= 2 ** 32 - 1);
  assertSafeInteger(a);
  assertSafeInteger(b);
  const c = a + b + 1;
  assert(c < 32);
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
