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
