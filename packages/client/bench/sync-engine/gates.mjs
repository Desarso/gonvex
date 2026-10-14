import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { bootstrap, quantile } from './statistics.mjs';
export function evaluate(results) {
  if (results.kind !== 'native-browser-storage-v1') throw new Error('Gates require native browser results, never emulator timings');
  if (!results.browsers?.length) throw new Error('Results contain no browser measurements');
  const gates = [];
  for (const browser of results.browsers) {
    const find = metric => browser.cases.find(value => value.metric === metric);
    const add = (name, cases, ci, limit, adapters = ['legacy', 'engine']) => {
      const missing = cases.some(value => !value);
      const noisy = results.complete !== true || !browser.quiescent || cases.some(value => value && adapters.some(adapter => value[adapter].cv > (results.environment?.options?.cv ?? .15)));
      const verdict = missing || noisy ? 'NOISY' : ci[0] > limit ? 'FAIL' : ci[1] <= limit ? 'PASS' : 'NOISY';
      gates.push({ browser: browser.name, gate: name, verdict, ci, limit,
        reason: results.complete !== true ? 'run incomplete' : missing ? 'case not measured' : !browser.quiescent ? 'not quiescent' : noisy ? 'required adapter CV exceeds threshold' : '95% bootstrap interval' });
    };
    const small = find('single-row-10000'), large = find('single-row-50000');
    add('single-row durable apply p95 <= 10 ms', [large], large?.engine.p95CI, 10, ['engine']);
    const scalingCI = small && large ? bootstrap(large.engine.samples, small.engine.samples, values => quantile(values, .95)) : undefined;
    add('50k/10k engine p95 <= 1.25x', [small, large], scalingCI, 1.25, ['engine']);
    for (const kind of ['startup-working-set', 'delete-100-windows']) for (const count of [10000, 50000]) {
      const value = find(`${kind}-${count}`);
      add(`${kind}-${count} engine/legacy p95 <= 1x`, [value], value?.ratio.p95CI, 1);
    }
  }
  return gates;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const gates = evaluate(JSON.parse(await readFile(process.argv[2] ?? new URL('./results.json', import.meta.url), 'utf8')));
  for (const gate of gates) console.log(`${gate.verdict} ${gate.browser} ${gate.gate}: ${gate.ci?.map(value => value.toFixed(3)).join('..') ?? 'missing'} (${gate.reason})`);
  if (gates.some(gate => gate.verdict === 'FAIL')) process.exitCode = 1;
}
