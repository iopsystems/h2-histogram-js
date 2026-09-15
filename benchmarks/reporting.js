// Run: node benchmarks/reporting.js [iterations]. Inputs are prepared outside timing.
import { performance } from 'node:perf_hooks';
import { Histogram } from '../src/index.js';
const iterations = Number(process.argv[2] ?? 2000);
if (!Number.isSafeInteger(iterations) || iterations < 1) throw Error('iterations must be a positive safe integer');
const dense = new Histogram(7, 32);
for (let i = 0; i < 2048; i++) dense.record((i * 104729) % (2 ** 32), (i % 7) + 1);
const other = Histogram.checkedSum([dense]);
const destination = new Histogram(7, 32);
const sparse = dense.toSparse(), cumulative = dense.toCumulative();
const requests = [.99, 0, .5, .9, .5, 1], output = [];
let sink = 0;
function phase(name, operation, ownership) {
  for (let i = 0; i < Math.min(100, iterations); i++) operation();
  const start = performance.now();
  for (let i = 0; i < iterations; i++) operation();
  const elapsed = performance.now() - start;
  console.log(JSON.stringify({ phase: name, iterations, nsPerOperation: elapsed * 1e6 / iterations, ownership }));
}
console.log(JSON.stringify({ node: process.version, platform: process.platform, arch: process.arch, denseBuckets: dense.buckets.length, occupiedBuckets: sparse.length, inputsPreparedOutsideTiming: true }));
phase('record', () => destination.record(123), 'existing dense destination; one count update');
phase('reset', () => destination.reset(), 'existing dense backing storage');
phase('snapshotInto', () => dense.snapshotInto(destination), 'existing destination, validation and copy included');
phase('drainInto', () => { dense.snapshotInto(destination); destination.drainInto(other); }, 'refill copy plus drain/reset included; all dense storage reused');
phase('checkedAddAssign', () => { destination.reset(); destination.checkedAddAssign(dense); }, 'reset plus checked addition; destination storage reused');
phase('checkedSum', () => { sink += Histogram.checkedSum([dense, dense]).buckets[0]; }, 'request array and owned dense result allocated inside timing');
phase('dense scalar', () => { sink += dense.percentile(.99).count; }, 'one Bucket allocated per query');
phase('sparse scalar', () => { sink += sparse.percentile(.99).count; }, 'one Bucket allocated per query');
phase('cumulative scalar', () => { sink += cumulative.percentile(.99).count; }, 'one Bucket allocated per query');
phase('dense percentilesInto', () => { dense.percentilesInto(requests, output); sink += output[0][1].count; }, 'requests, result array and warmed pairs reused; Buckets allocated');
phase('cumulative percentilesInto', () => { cumulative.percentilesInto(requests, output); sink += output[0][1].count; }, 'requests, result array and warmed pairs reused; Buckets allocated');
phase('sparse snapshot', () => { sink += dense.toSparse().length; }, 'owned sparse columns allocated, validated and frozen');
phase('cumulative snapshot', () => { sink += dense.toCumulative().mean(); }, 'owned cumulative columns and cached mean constructed');
phase('sparse merge', () => { sink += sparse.merge(sparse).length; }, 'owned sparse output; sorted merge and column copies included');
phase('cumulative downsample', () => { sink += cumulative.downsample(5).mean(); }, 'owned cumulative output; mapped counts and new mean included');
console.log(JSON.stringify({ sink, gc: 'ordinary runtime GC may occur during timing; no forced GC or allocation-byte claims' }));
