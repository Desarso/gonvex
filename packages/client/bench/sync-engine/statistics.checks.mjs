import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { median, quantile, summary, comparison, bootstrap } from './statistics.mjs';
import { evaluate } from './gates.mjs';

test('nearest-rank p95, even median and deterministic bootstrap', () => {
  const samples = Array.from({ length: 20 }, (_, index) => index + 1);
  assert.equal(median(samples), 10.5);
  assert.equal(quantile(samples, .95), 19);
  assert.deepEqual(bootstrap(samples), bootstrap(samples));
  assert.ok(summary(samples).cv > .15);
  assert.deepEqual(comparison(samples.map(value => value * 2), samples).medianCI, [2, 2]);
});
const values = (metric, legacy, engine) => ({ metric, legacy: summary(Array(20).fill(legacy)), engine: summary(Array(20).fill(engine)),
  ratio: comparison(Array(20).fill(engine), Array(20).fill(legacy)), status: 'OK' });
const result = cases => ({ kind: 'native-browser-storage-v1', complete: true, browsers: [{ name: 'chromium', quiescent: true, cases }] });
test('gate boundaries use confidence intervals and missing cases never pass', () => {
  const cases = [values('single-row-10000', 10, 1), values('single-row-50000', 100, 1)];
  assert.deepEqual(evaluate(result(cases)).slice(0, 2).map(gate => gate.verdict), ['PASS', 'PASS']);
  cases[1] = values('single-row-50000', 100, 11);
  assert.deepEqual(evaluate(result(cases)).slice(0, 2).map(gate => gate.verdict), ['FAIL', 'FAIL']);
  cases[1].engine.p95CI = [9, 12];
  assert.equal(evaluate(result(cases))[0].verdict, 'NOISY');
  cases[1].status = 'NOISY'; cases[1].engine.cv = .2;
  assert.equal(evaluate(result(cases))[1].verdict, 'NOISY');
  assert.ok(evaluate(result([])).every(gate => gate.verdict === 'NOISY'));
  const partial = result(cases); partial.complete = false;
  assert.ok(evaluate(partial).every(gate => gate.verdict === 'NOISY'));
  const busy = result(cases); busy.browsers[0].quiescent = false;
  assert.ok(evaluate(busy).every(gate => gate.verdict === 'NOISY'));
  assert.throws(() => evaluate({ kind: 'emulator', browsers: [] }), /native browser/);
});

test('engine-only gates do not inherit irrelevant legacy CV noise', () => {
  const cases = [values('single-row-10000', 10, 1), values('single-row-50000', 100, 1)];
  for (const value of cases) { value.legacy.cv = .5; value.status = 'NOISY'; }
  assert.deepEqual(evaluate(result(cases)).slice(0, 2).map(gate => gate.verdict), ['PASS', 'PASS']);
  const startup = values('startup-working-set-10000', 1, 2); startup.legacy.cv = .5;
  cases.push(startup);
  assert.equal(evaluate(result(cases))[2].verdict, 'NOISY');
});

test('gate CLI exits nonzero only for clear FAIL, and rejects empty results', () => {
  const directory = mkdtempSync(tmpdir() + '/sync-gate-check-');
  try {
    const data = result([values('single-row-10000', 10, 1), values('single-row-50000', 100, 11)]);
    const path = directory + '/results.json';
    const run = () => { writeFileSync(path, JSON.stringify(data)); return spawnSync(process.execPath, [fileURLToPath(new URL('./gates.mjs', import.meta.url)), path], { encoding: 'utf8' }); };
    assert.equal(run().status, 1);
    data.browsers[0].quiescent = false;
    const noisy = run(); assert.equal(noisy.status, 0); assert.match(noisy.stdout, /NOISY/);
    data.browsers = []; assert.equal(run().status, 1);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
