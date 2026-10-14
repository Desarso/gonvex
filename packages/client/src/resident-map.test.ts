import { expect, it } from 'vitest';
import { ResidentMap } from './resident-map.js';

it('shares isolated versions while preserving native Map order and values', () => {
  let actual = new ResidentMap<number>();
  const expected = new Map<string, number>();
  let seed = 42;
  const random = () => (seed = (Math.imul(seed,1664525)+1013904223)>>>0);
  for (let i=0;i<4000;i++) {
    const prior = actual;
    const saved = [...prior];
    actual = actual.fork();
    const id = String(random()%200);
    if (random()%3) { actual.set(id,i); expected.set(id,i); }
    else expect(actual.delete(id)).toBe(expected.delete(id));
    expect([...prior]).toEqual(saved);
    expect([...actual]).toEqual([...expected]);
    expect(actual.size).toBe(expected.size);
    expect(actual.get(id)).toBe(expected.get(id));
    expect(actual.has(id)).toBe(expected.has(id));
  }
  const prior = actual;
  actual = actual.fork(); actual.clear();
  expect(actual.size).toBe(0); expect([...prior]).toEqual([...expected]);
});

it('protects both branches when either side of a fork is edited in place',()=>{
  const original=new ResidentMap<number>();original.set('a',1).set('b',2);
  const fork=original.fork();original.set('a',3).delete('b');
  expect([...fork]).toEqual([['a',1],['b',2]]);
  fork.set('b',4).set('c',5);expect([...original]).toEqual([['a',3]]);
});

it('isolates edits across page boundaries, bucket collisions and compaction', () => {
  const original = new ResidentMap<number | undefined>();
  const expected = new Map<string, number | undefined>();
  for (let i=0;i<5000;i++) { original.set(String(i),i); expected.set(String(i),i); }
  original.set('undefined',undefined); expected.set('undefined',undefined);
  const captured = [...original];
  const next = original.fork();
  for (let i=0;i<4000;i++) { next.delete(String(i)); expected.delete(String(i)); }
  for (let i=0;i<500;i++) { next.set(String(i),-i); expected.set(String(i),-i); }
  expect([...next]).toEqual([...expected]);
  expect([...original]).toEqual(captured);
  expect(next.has('undefined')).toBe(true);
  expect(next.get('undefined')).toBeUndefined();
  const positions = [...next.keys()].map(id=>next.position(id));
  expect(positions).toEqual([...positions].sort((a,b)=>a-b));
  expect(next['nextPosition']).toBeLessThan(next.size*2+1024);
  original.set('4096',99);
  expect(next.get('4096')).toBe(4096);
  const cleared = next.fork(); cleared.clear(); cleared.set('fresh',1);
  expect(cleared.sharesLineage(next)).toBe(false);
  expect([...next]).toEqual([...expected]);
});

it('grows bucket directories without changing captured slots or values',()=>{
  const original = new ResidentMap<number>();
  for (let i=0;i<2048;i++) original.set(String(i),i);
  expect(original['bucketCount']).toBe(16);
  const next = original.fork(); next.set('2048',2048);
  expect(next['bucketCount']).toBe(64);
  expect(original['bucketCount']).toBe(16);
  expect(original.get('2048')).toBeUndefined();
  for (let i=0;i<2048;i++) {
    expect(next.get(String(i))).toBe(i);
    expect(next.position(String(i))).toBe(original.position(String(i)));
  }
  original.set('0',-1);
  expect(next.get('0')).toBe(0);
});
