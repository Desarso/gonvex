import { test } from 'node:test';
import assert from 'node:assert/strict';
import { guardSQLiteSahpoolLogging } from './sqlite-log-guards.mjs';
function fixture() {
  return `class Pool {
    #verbosity;
    constructor(level){this.#verbosity=level}
    #logImpl(level,...args){if(this.#verbosity>level) output.push(args)};
    log(...args){this.#logImpl(2, ...args)};
  }
  const output=[];let allocations=0;const pool=new Pool(level);
  ${Array.from({length:12},(_,i)=>`pool.log((allocations++, 'site-${i}'));`).join('\n')}
  return {output,allocations};`;
}
test('skips disabled log arguments and preserves all enabled debug messages', () => {
  const run = new Function('level', guardSQLiteSahpoolLogging(fixture()));
  assert.deepEqual(run(2), {output:[],allocations:0});
  assert.deepEqual(run(3), {output:Array.from({length:12},(_,i)=>['site-'+i]),allocations:12});
});
test('fails closed when the pinned initializer or log-site count changes', () => {
  assert.throws(()=>guardSQLiteSahpoolLogging(fixture().replace('pool.log(', 'other.log(')), /log sites/);
  assert.throws(()=>guardSQLiteSahpoolLogging(fixture().replace('log(...args)', 'debug(...args)')), /implementation/);
  assert.throws(()=>guardSQLiteSahpoolLogging(guardSQLiteSahpoolLogging(fixture())), /implementation/);
});
