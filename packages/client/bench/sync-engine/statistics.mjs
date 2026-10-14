// Seeded bootstrap keeps reports and gate decisions reproducible from raw samples.
export const quantile = (values, q) => [...values].sort((a, b) => a - b)[Math.max(0, Math.ceil(values.length * q) - 1)];
export const median = values => {
  const sorted = [...values].sort((a, b) => a - b), middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
};
function random() {
  let seed = 0x47565801;
  return () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 2 ** 32; };
}
export function bootstrap(a, b, statistic = median, paired = false) {
  const rng = random(), values = [];
  for (let round = 0; round < 4000; round++) {
    const indices = a.map(() => Math.floor(rng() * a.length));
    const aa = indices.map(index => a[index]);
    const bb = b && (paired ? indices.map(index => b[index]) : b.map(() => b[Math.floor(rng() * b.length)]));
    values.push(bb ? statistic(aa) / statistic(bb) : statistic(aa));
  }
  return [quantile(values, .025), quantile(values, .975)];
}
export function summary(samples) {
  const mean = samples.reduce((sum, value) => sum + value, 0) / samples.length;
  const cv = Math.sqrt(samples.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (samples.length - 1)) / mean;
  return { median: median(samples), p95: quantile(samples, .95), medianCI: bootstrap(samples), p95CI: bootstrap(samples, undefined, values => quantile(values, .95)), cv, samples };
}
export function comparison(engine, legacy) {
  return { median: median(engine) / median(legacy), medianCI: bootstrap(engine, legacy, median, true),
    p95: quantile(engine, .95) / quantile(legacy, .95), p95CI: bootstrap(engine, legacy, values => quantile(values, .95), true) };
}
