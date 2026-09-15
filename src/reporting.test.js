import { describe, it, expect } from 'vitest';
import { Config, Histogram, SparseHistogram, CumulativeHistogram } from './index.js';
const make = () => { const h = new Histogram(2, 8); h.record(0, 2); h.record(17, 3); return h; };
describe('reporting and analytics contracts', () => {
  it('reuses dense storage for reset, snapshot and drain with independent copies', () => {
    const h = make(), dst = make(), storage = dst.buckets, original = h.buckets;
    expect(h.snapshotInto(dst)).toBe(dst); expect(dst.equals(h)).toBe(true);
    h.reset(); expect(h.buckets).toBe(original); expect(h.totalCount()).toBe(0); expect(dst.totalCount()).toBe(5);
    expect(dst.drainInto(h)).toBe(h); expect(dst.buckets).toBe(storage); expect(dst.totalCount()).toBe(0); expect(h.totalCount()).toBe(5);
    expect(() => h.drainInto(h)).toThrow(); expect(h.totalCount()).toBe(5);
    expect(() => h.snapshotInto(new Histogram(1, 8))).toThrow();
  });
  it('checks complete addition before mutation and validates sum geometry first', () => {
    const dst = make(), before = dst.merge(new Histogram(2, 8)), other = make();
    other.buckets[other.buckets.length - 1] = Number.MAX_SAFE_INTEGER;
    dst.buckets[dst.buckets.length - 1] = 1; const saved = Array.from(dst.buckets);
    expect(() => dst.checkedAddAssign(other)).toThrow(); expect(Array.from(dst.buckets)).toEqual(saved);
    expect(() => Histogram.checkedSum([])).toThrow();
    expect(() => Histogram.checkedSum([other, other, new Histogram(1, 8)])).toThrow(/config/);
    const sum = Histogram.checkedSum([before, before]); expect(sum.totalCount()).toBe(10); expect(before.totalCount()).toBe(5);
    const copy = Histogram.checkedSum([before]); copy.reset(); expect(before.totalCount()).toBe(5);
    before.checkedAddAssign(before); expect(before.totalCount()).toBe(10);
  });
  it('queries scalars directly and reuses ordered batch outputs without dense reconstruction', () => {
    for (const h of [make(), make().toSparse(), make().toCumulative()]) {
      const expected = [1, 0, .5, .5].map(p => [p, h.percentile(p)]);
      if ('toDense' in h) h.toDense = () => { throw Error('dense reconstruction'); };
      h.percentiles = () => { throw Error('batch delegation'); };
      expect(h.percentile(.5)).toEqual(expected[2][1]);
      const out = /** @type {[number, import('./index.js').Bucket][]} */ ([]);
      expect(h.percentilesInto([1, 0, .5, .5], out)).toBe(out); expect(out).toEqual(expected);
      expect(h.percentilesInto([], out)).toBe(out); expect(out).toEqual([]);
    }
    const empty = new Histogram(2, 8), out = /** @type {[number, import('./index.js').Bucket][]} */ ([]);
    expect(empty.percentilesInto([], out)).toBe(out); expect(out).toEqual([]);
  });
  it('merges and downsamples sparse and cumulative snapshots natively with new means', () => {
    for (const h of [make().toSparse(), make().toCumulative()]) {
      if ('toDense' in h) h.toDense = () => { throw Error('dense reconstruction'); };
      expect(h.merge(h).toDense().equals(make().merge(make()))).toBe(true);
      const coarse = h.downsample(1); expect(coarse.toDense().equals(make().downsample(1))).toBe(true);
      if (coarse instanceof CumulativeHistogram) {
        expect(coarse.mean()).toBe(make().downsample(1).toCumulative().mean());
        expect(coarse.toSparse().toDense().equals(coarse.toDense())).toBe(true);
      }
    }
  });
  it('validates imported indices, counts and exact cumulative bounds', () => {
    const cfg = new Config(2, 8);
    for (const C of [SparseHistogram, CumulativeHistogram]) {
      for (const count of [-1, .5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) expect(() => new C(cfg, [0], [count])).toThrow();
      expect(() => new C(cfg, [.5], [1])).toThrow(); expect(() => new C(cfg, [0, 0], [1, 2])).toThrow();
    }
    const h = new Histogram(2, 8);
    for (const count of [-1, .5, Number.MAX_SAFE_INTEGER + 1]) { const a = [...h.buckets]; a[0] = count; expect(() => Histogram.fromBuckets(2, 8, a)).toThrow(); }
    h.record(0, Number.MAX_SAFE_INTEGER); expect(h.totalCount()).toBe(Number.MAX_SAFE_INTEGER);
    h.record(1); expect(() => h.totalCount()).toThrow(); expect(() => h.toCumulative()).toThrow();
  });
  it('owns immutable snapshot arrays so means cannot become stale', () => {
    const cfg = new Config(2, 8), index = [0], count = [2];
    const c = new CumulativeHistogram(cfg, index, count); index[0] = 1; count[0] = 8;
    expect(c.mean()).toBe(0); expect(c.totalCount()).toBe(2);
    expect(Reflect.set(c.index, '0', 1)).toBe(false);
    expect(Reflect.set(c.count, '0', 9)).toBe(false);
    expect(c.mean()).toBe(0); expect(c.totalCount()).toBe(2);
  });
});

it('validates mutable dense escape hatch on reporting and preserves output on invalid requests', () => {
  const h = make(); h.buckets[3] = -1;
  expect(() => h.toSparse()).toThrow(); expect(() => h.toCumulative()).toThrow();
  const out = /** @type {[number, import('./index.js').Bucket][]} */ ([[0, make().percentile(0)]]);
  const saved = [...out]; expect(() => make().percentilesInto([0, NaN], out)).toThrow(); expect(out).toEqual(saved);
});

it('handles zero entries, transform overflow and snapshot property reassignment', () => {
  const cfg = new Config(2, 8), max = Number.MAX_SAFE_INTEGER;
  const sparse = new SparseHistogram(cfg, [0, 1, 2], [0, 2, 0]);
  expect(sparse.percentile(0)?.count).toBe(2); expect(sparse.merge(sparse).count).toEqual([4]);
  const large = new SparseHistogram(cfg, [8, 9], [max, 1]);
  expect(() => large.downsample(1)).toThrow(); expect(() => large.toCumulative()).toThrow();
  expect(() => new SparseHistogram(cfg, [0], [max]).merge(new SparseHistogram(cfg, [0], [1]))).toThrow();
  const c = make().toCumulative();
  for (const key of ['index', 'count', 'config', '_mean']) expect(() => Reflect.set(c, key, [])).not.toThrow();
  expect(c.totalCount()).toBe(5); expect(c.mean()).toBe(10.5);
});

it('normalizes zero sparse counts when building cumulative snapshots', () => {
  const cfg = new Config(2, 8);
  const c = new SparseHistogram(cfg, [0, 1, 2], [0, 2, 0]).toCumulative();
  expect(c.index).toEqual([1]); expect(c.count).toEqual([2]); expect(c.mean()).toBe(1);
  expect(new SparseHistogram(cfg, [0], [0]).toCumulative().percentile(1)).toBe(null);
});

it('sums directly into private output without reused-destination preflight passes', () => {
  const a = make(), b = make();
  const original = Histogram.prototype.checkedAddAssign;
  Histogram.prototype.checkedAddAssign = () => { throw Error('reused-destination path'); };
  try {
    const result = Histogram.checkedSum([a, b, a]);
    expect(result.totalCount()).toBe(15);
    expect(a.totalCount()).toBe(5); expect(b.totalCount()).toBe(5);
    result.reset(); expect(a.totalCount()).toBe(5);
    const last = a.buckets.length - 1;
    a.buckets[last] = Number.MAX_SAFE_INTEGER; b.buckets[last] = 1;
    const savedA = [...a.buckets], savedB = [...b.buckets];
    expect(() => Histogram.checkedSum([a, b])).toThrow(/safe integer/);
    expect([...a.buckets]).toEqual(savedA); expect([...b.buckets]).toEqual(savedB);
  } finally {
    Histogram.prototype.checkedAddAssign = original;
  }
});


it('preserves the unchecked recording and legacy merge arithmetic paths', () => {
  const h = new Histogram(2, 8); h.record(0, Number.MAX_SAFE_INTEGER); h.record(0);
  expect(h.buckets[0]).toBe(2 ** 53); expect(() => h.toSparse()).toThrow();
  const a = new Histogram(2, 8); a.record(0, Number.MAX_SAFE_INTEGER);
  const b = new Histogram(2, 8); b.record(0);
  expect(a.merge(b).buckets[0]).toBe(2 ** 53); expect(() => Histogram.checkedSum([a, b])).toThrow();
});

it('normalizes validated sparse zero entries and rejects malformed dense snapshot shapes', () => {
  const cfg = new Config(2, 8);
  expect(new SparseHistogram(cfg, [0, 1], [0, 0]).isEmpty()).toBe(true);
  expect(() => new SparseHistogram(cfg, [0, 0], [0, 0])).toThrow();
  for (const length of [cfg.totalBuckets - 1, cfg.totalBuckets + 1]) {
    const h = make(); h.buckets = new Float64Array(length);
    expect(() => h.toSparse()).toThrow(); expect(() => h.toCumulative()).toThrow();
  }
});

it('returns an empty output without scanning totals for empty requests', () => {
  for (const h of [make(), make().toSparse(), make().toCumulative()]) {
    h.totalCount = () => { throw Error('total scan'); };
    const output = /** @type {[number, import('./index.js').Bucket][]} */ ([]);
    expect(h.percentilesInto([], output)).toBe(output);
  }
});

it('answers allocating sparse batches without repeated scalar or output-buffer scans', () => {
  const sparse = make().toSparse();
  sparse.percentilesInto = () => { throw Error('per-request batch'); };
  sparse.percentile = () => { throw Error('scalar'); };
  expect(sparse.percentiles([1, 0, .5, .5])?.map(([p, bucket]) => [p, bucket.count, bucket.start])).toEqual([
    [1, 3, 16], [0, 2, 0], [.5, 3, 16], [.5, 3, 16],
  ]);
});
