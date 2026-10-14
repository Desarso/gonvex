import { BinaryReplicaCodec } from '../../../protocol/dist/binary-replica.js';

export function stats(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const median = values => { const s = [...values].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
  let seed = 42;
  const bootstrap = Array.from({ length: 1000 }, () => median(samples.map(() => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return samples[seed % samples.length];
  }))).sort((a, b) => a - b);
  return { median: median(samples), p95: sorted[Math.floor(sorted.length * .95)], medianCI95: [bootstrap[25], bootstrap[975]] };
}

export function measure(json, binary, heap, gc) {
  const result = {};
  for (const mode of ['json', 'binary']) {
    const samples = [];
    let peakLiveHeapDelta = 0;
    const repeats = json.length === 1 && json[0].length < 5000 ? 1000 : 1;
    for (let iteration = -5; iteration < 25; iteration++) {
      gc?.();
      const before = heap();
      let decoded;
      const start = performance.now();
      for (let repeat = 0; repeat < repeats; repeat++) {
        const codec = new BinaryReplicaCodec();
        decoded = [];
        for (let i = 0; i < json.length; i++) decoded.push(mode === 'json' ? JSON.parse(json[i]) : codec.decode(binary[i]));
      }
      const duration = (performance.now() - start) / repeats;
      peakLiveHeapDelta = Math.max(peakLiveHeapDelta, heap() - before);
      if (iteration >= 0) samples.push(duration);
      // Consume the same result without stringifying inside the timed section.
      if (decoded.length !== json.length) throw new Error('decode failed');
    }
    result[mode] = { ms: stats(samples), peakLiveHeapDelta, repeatsPerSample: repeats };
  }
  return result;
}
export { BinaryReplicaCodec };
