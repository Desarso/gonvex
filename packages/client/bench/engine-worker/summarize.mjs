import { readFile, writeFile } from 'node:fs/promises';
const result = JSON.parse(await readFile(new URL(process.argv[2] ?? 'results.json', import.meta.url)));
const quantile = (values, p) => { const sorted = values.filter(Number.isFinite).sort((a, b) => a - b); return sorted.length ? sorted[Math.ceil(p * sorted.length) - 1] : null; };
let seed = 0x109112;
const random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 2 ** 32; };
const bootstrap = (before, after) => {
  const ratios = [];
  for (let iteration = 0; iteration < 10_000; iteration++) {
    const left = before.map(() => before[Math.floor(random() * before.length)]);
    const right = after.map(() => after[Math.floor(random() * after.length)]);
    ratios.push(quantile(right, .5) / quantile(left, .5));
  }
  return { ratio: quantile(after, .5) / quantile(before, .5), ci95: [quantile(ratios, .025), quantile(ratios, .975)] };
};
const summary = { metadata: result.metadata, samplesPerMode: result.samples.filter(sample => sample.mode === 'worker').length, metrics: {} };
for (const workload of ['cold', 'burst', 'snapshot', 'drain']) {
  const rows = {};
  const scriptTimingValid = workload !== 'cold' || result.metadata.coldSamplingAligned === true;
  for (const mode of ['legacy', 'worker']) {
    const samples = result.samples.filter(sample => sample.mode === mode && sample[workload]).map(sample => sample[workload]);
    rows[mode] = { wallMedianMs: quantile(samples.map(sample => sample.wallMs), .5), wallP95Ms: quantile(samples.map(sample => sample.wallMs), .95), scriptMedianMs: scriptTimingValid ? quantile(samples.map(sample => sample.scriptMs), .5) : null, longTasks: samples.flatMap(sample => sample.longTasks).length, maxLongTaskMs: Math.max(0, ...samples.flatMap(sample => sample.longTasks)), clickP95Ms: quantile(samples.flatMap(sample => sample.clicks), .95), frameP95Ms: quantile(samples.flatMap(sample => sample.frames), .95) };
    if (workload === 'drain') Object.assign(rows[mode], {
      admissionMedianMs: quantile(samples.map(sample => sample.admissionMs), .5),
      admissionP95Ms: quantile(samples.map(sample => sample.admissionMs), .95),
      postAdmissionMedianMs: quantile(samples.map(sample => sample.wallMs - sample.admissionMs), .5),
      // Aggregate average per round, not the p95 of individual command latency.
      averageAdmissionPerIntentMedianMs: quantile(samples.map(sample => sample.admissionMs / 500), .5),
    });
  }
  const before = result.samples.filter(sample => sample.mode === 'legacy' && sample[workload]).map(sample => sample[workload].scriptMs).filter(Number.isFinite);
  const after = result.samples.filter(sample => sample.mode === 'worker' && sample[workload]).map(sample => sample[workload].scriptMs).filter(Number.isFinite);
  summary.metrics[workload] = { ...rows, scriptTimingValid, scriptRatio: scriptTimingValid && before.length && after.length ? bootstrap(before, after) : null };
}
summary.gate = { burst: summary.metrics.burst.worker.maxLongTaskMs <= 50 ? 'PASS' : 'FAIL', correctness: result.metadata.correctness, multiTab: result.metadata.multiTab, maxLoadFraction: Math.max(...result.samples.map(sample => sample.load[0])) / result.metadata.cores };
await writeFile(new URL('summary.json', import.meta.url), JSON.stringify(summary, null, 2));
process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
