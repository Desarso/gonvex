import { test } from 'node:test';
import assert from 'node:assert/strict';
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
  cases[1].status = 'NOISY';
  assert.equal(evaluate(result(cases))[1].verdict, 'NOISY');
  assert.ok(evaluate(result([])).every(gate => gate.verdict === 'NOISY'));
  const partial = result(cases); partial.complete = false;
  assert.ok(evaluate(partial).every(gate => gate.verdict === 'NOISY'));
  const busy = result(cases); busy.browsers[0].quiescent = false;
  assert.ok(evaluate(busy).every(gate => gate.verdict === 'NOISY'));
  assert.throws(() => evaluate({ kind: 'emulator', browsers: [] }), /native browser/);
});
