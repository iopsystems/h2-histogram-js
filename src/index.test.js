import * as fc from 'fast-check';
import { describe, expect, it, test } from 'vitest';
import { H2Encoding, H2Histogram, H2HistogramBuilder, decode32, encode32 } from './index.js';

// todo: is relative error < or <= 2^-b?

describe('H2Encoding', () => {
  test('H2Encoding.with', () => {
    const enc = H2Encoding.with({ minimumUnit: 1, relativeError: 0.01, maxValue: 1e6 });
    expect(enc.a).toBe(0);
    expect(enc.b).toBe(7);
    expect(enc.n).toBe(20);

    fc.assert(fc.property(
      fc.double({ min: 1, max: 10, noNaN: true, }), // minimum unit
      // @ts-ignore
      fc.double({ min: 0.0001, max: 1, noNaN: true, }), // relative error
      fc.integer({ min: 1, max: enc.maxValue() }), // maximum value
      (minimumUnit, relativeError, maxValue) => {
        const enc = H2Encoding.with({ minimumUnit, relativeError, maxValue });
        expect(enc.absoluteError()).toBeLessThanOrEqual(minimumUnit);
        expect(enc.relativeError()).toBeLessThanOrEqual(relativeError);
        expect(enc.maxValue()).toBeGreaterThanOrEqual(maxValue);
      })
    );
  }); 

  test('H2Encoding.encode', () => {
    let enc = new H2Encoding({ a: 1, b: 2, n: 6 });
    let bins = [
      // note: the last 2 values, currently 15, would be 16 if n was greater than 6.
      0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 8, 8, 9, 9, 9, 9, 10, 10,
      10, 10, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 13, 13, 13, 13, 13,
      13, 13, 13, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15
    ];
    for (let i = 0; i < bins.length; i++) {
      const bin = bins[i];
      expect(enc.encode(i)).toBe(bin);
    }
    expect(() => enc.encode(bins.length + 1)).toThrow();
  });

  test('Values are encoded into the bin that contains them', () => {
    /**
     * @param {H2Encoding} enc
     */
    function propertyTest(enc) {
      fc.assert(fc.property(
        fc.double({ min: 0, max: enc.maxValue(), noNaN: true, }),
        // @ts-ignore
        value => {
          const code = enc.encode(value);
          if (value < 2 ** 32) {
            expect(encode32(value, enc.a, enc.b)).toBe(code);
            const { lower, upper } = decode32(code, enc.a, enc.b);
            expect(lower <= value && value <= upper).toBe(true);
          }
          const lower = enc.lower(code);
          const higher = enc.upper(code);
          expect(lower <= value && value <= higher).toBe(true);
        }
      ));
    }

    propertyTest(new H2Encoding({ a: 0, b: 7, n: 20 }));
    propertyTest(new H2Encoding({ a: 0, b: 0, n: 0 }));
    propertyTest(new H2Encoding({ a: 0, b: 0, n: 53 }));
    propertyTest(new H2Encoding({ a: 20, b: 0, n: 53 }));
    propertyTest(new H2Encoding({ a: 0, b: 20, n: 53 }));
    propertyTest(new H2Encoding({ a: 0, b: 20, n: 21 }));
    propertyTest(new H2Encoding({ a: 30, b: 0, n: 21 }));
    propertyTest(new H2Encoding({ a: 0, b: 30, n: 1 }));

    for (let a = 0; a < 5; a++) {
      for (let b = 0; b < 5; b++) {
        propertyTest(new H2Encoding({ a, b, n: 11 }));
      }
    }
  });

  test('H2Encoding.numBins', () => {
    expect(new H2Encoding({ a: 0, b: 0, n: 0 }).numBins()).toBe(1);
    expect(new H2Encoding({ a: 0, b: 0, n: 6 }).numBins()).toBe(7);
    expect(new H2Encoding({ a: 0, b: 0, n: 7 }).numBins()).toBe(8);
    expect(new H2Encoding({ a: 0, b: 2, n: 6 }).numBins()).toBe(20);
    expect(new H2Encoding({ a: 1, b: 2, n: 6 }).numBins()).toBe(16);
    expect(new H2Encoding({ a: 1, b: 0, n: 6 }).numBins()).toBe(6);
    expect(new H2Encoding({ a: 1, b: 1, n: 6 }).numBins()).toBe(10);
    expect(new H2Encoding({ a: 0, b: 1, n: 6 }).numBins()).toBe(12);
    expect(new H2Encoding({ a: 2, b: 0, n: 4 }).numBins()).toBe(3);
    expect(new H2Encoding({ a: 2, b: 1, n: 4 }).numBins()).toBe(4);
    expect(new H2Encoding({ a: 2, b: 2, n: 6 }).numBins()).toBe(12);
    expect(new H2Encoding({ a: 2, b: 2, n: 5 }).numBins()).toBe(8);
    expect(new H2Encoding({ a: 2, b: 2, n: 4 }).numBins()).toBe(4);
    expect(new H2Encoding({ a: 2, b: 3, n: 3 }).numBins()).toBe(2);
    expect(new H2Encoding({ a: 2, b: 3, n: 2 }).numBins()).toBe(1);
    expect(new H2Encoding({ a: 2, b: 3, n: 1 }).numBins()).toBe(1);
    expect(new H2Encoding({ a: 2, b: 3, n: 0 }).numBins()).toBe(1);

    // 2 bins in the linear segment, 1 in the log segment
    expect(new H2Encoding({ a: 30, b: 0, n: 32 }).numBins()).toBe(3);

    // 2 bins in the linear segment, 22 in the log segment
    expect(new H2Encoding({ a: 30, b: 0, n: 53 }).numBins()).toBe(24);
  });

  // c = a + b + 1 out of bounds (above 31)
  expect(() => new H2Encoding({ a: 20, b: 20, n: 53 })).toThrow();
  
  // n out of bounds
  expect(() => new H2Encoding({ a: 0, b: 0, n: 54 })).toThrow();
});

test('H2Histogram', () => {
  {
    // Basic tests with a 3-value histogram containing [1e5, 2e5, 3e5]
    const enc = H2Encoding.with({ relativeError: 0.01, maxValue: 1e6 });
    const builder = new H2HistogramBuilder(enc);
    builder.incrementValue(1e5, 100);
    builder.incrementValue(2e5, 100);
    builder.incrementValue(3e5, 100);
    const hist = builder.build();
    expect(hist.numObservations).toBe(300);
    expect(hist.cumulativeCount(100)).toBe(0);
    expect(hist.quantile(0)).toBeGreaterThan(1e5);
    expect(hist.quantile(0)).toBeLessThan(1.01 * 1e5);
    expect(hist.quantile(0.5)).toBeGreaterThan(2e5);
    expect(hist.quantile(0.5)).toBeLessThan(1.01 * 2e5);
    expect(hist.quantile(1)).toBeGreaterThanOrEqual(3e5);
    expect(hist.quantile(1)).toBeLessThan(1.01 * 3e5);

    expect(hist.cdf(0.999 * 1e5)).toBe(1 / 3);
    expect(hist.cdf(1e5)).toBe(1 / 3);
    expect(hist.cdf(1.1e5)).toBe(1 / 3);
    expect(hist.cdf(1.5e5)).toBe(1 / 3);

    expect(hist.cdf(2e5)).toBe(2 / 3);
    expect(hist.cdf(2.1e5)).toBe(2 / 3);
    expect(hist.cdf(2.5e5)).toBe(2 / 3);
    expect(hist.cdf(2.9e5)).toBe(2 / 3);
    expect(hist.cdf(2.999e5)).toBe(3 / 3);

    expect(hist.cdf(3e5)).toBe(3 / 3);
    expect(hist.cdf(5e5)).toBe(3 / 3);

    expect(hist.cdf(1e9)).toBe(3 / 3);

    expect(hist.quantile(0.25)).toBeGreaterThan(1e5);
    expect(hist.quantile(0.75)).toBeGreaterThan(2e5);
    expect(hist.quantile(1)).toBeGreaterThan(3e5);
  }

  {
    /**
     * Calculate quantiles using the nearest-rank method:
     * https://en.wikipedia.org/wiki/Percentile#The_nearest-rank_method
     * @param {Float64Array} values
     * @param {number} q
     */
    function nearestRankQuantile(values, q) {
      if (values.length === 0) {
        return 0;
      }
      return values[(q === 0 ? 1 : Math.ceil(q * values.length)) - 1];
    }

    // Property test that quantiles are retrieved within the expected margin of error (compute-intensive)
    fc.assert(
      fc.property(
        fc.double({ min: 0.01, max: 1, minExcluded: true, noNaN: true }), // relativeError
        // @ts-ignore
        fc.integer({ min: 1, max: 2 ** 53 - 1 }),
        (relativeError, maxValue) => {
          const enc = H2Encoding.with({ relativeError, maxValue });
          const cutoff = enc.relativeAbsoluteCutoff();
          const absoluteError = enc.absoluteError();
          fc.assert(
            fc.property(
              fc.float64Array({
                min: 0,
                max: enc.maxValue(),
                noNaN: true,
                size: "xlarge"
              }),
              // @ts-ignore
              (values) => {
                values.sort();
                const builder = new H2HistogramBuilder(enc);
                for (const value of values) {
                  builder.incrementValue(value);
                }
                const hist = builder.build();

                for (let i = 0; i < 100; i++) {
                  const q = Math.random(); // note: will never generate 1.0
                  const baseline = nearestRankQuantile(values, q);
                  const ours = hist.quantile(q);
                  if (baseline < cutoff) {
                    expect(Math.abs(ours - baseline)).toBeLessThanOrEqual(absoluteError);
                  } else {
                    expect(Math.abs(baseline - ours) / baseline).toBeLessThanOrEqual(relativeError);
                  }
                }
              }
            ),
            // Run less iterations of this assert since it is nested inside another assert
            { numRuns: 2 });
        }
      )
    );
  }
});
