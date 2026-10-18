import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { fixtureWrites, fixtureEncodedWrites } from './fixture.mjs';
test('emits exactly the same synthetic wire bytes, including Unicode and rare rows', () => {
  for (const start of [0, 1, 990, 997, 1000, 99950]) for (const count of [0, 1, 50, 100, 512]) {
    const expected = JSON.stringify(fixtureWrites(start, count)), actual = fixtureEncodedWrites(start, count);
    assert.equal(actual, expected);
    assert.equal(createHash('sha256').update(actual).digest('hex'), createHash('sha256').update(expected).digest('hex'));
  }
});
test('refuses an unbounded or invalid fixture frame', () => {
  for (const count of [-1, 513, Infinity, 1.5]) assert.throws(()=>fixtureEncodedWrites(0,count));
});
