import { expect, it } from 'vitest';
import { RowRecency } from './row-recency.js';

it('matches an LRU model through randomized reads, pins, releases, deletions and replacements',()=>{
  const actual=new RowRecency();const expected=new Map<string,{time:number;pinned:boolean}>();
  let clock=0,seed=42;const random=()=>(seed=(Math.imul(seed,1664525)+1013904223)>>>0);
  for(let i=0;i<10000;i++){
    const id=String(random()%100);const operation=random()%5;
    if(operation===0){const pinned=random()%2===0;actual.set(id,{entity:'tasks',id},pinned);expected.set(id,{time:++clock,pinned});}
    else if(operation===1){actual.delete(id);expected.delete(id);}
    else if(operation===2){actual.touch(id);const entry=expected.get(id);if(entry)entry.time=++clock;}
    else{const pinned=operation===3;actual.pin(id,pinned);const entry=expected.get(id);if(entry)entry.pinned=pinned;}
    const oldest=[...expected].filter(([,entry])=>!entry.pinned).sort((a,b)=>a[1].time-b[1].time)[0]?.[0];
    expect(actual.oldest()?.key).toBe(oldest);expect(actual.size).toBe(expected.size);
  }
  actual.clear();expect(actual.oldest()).toBeUndefined();expect(actual.size).toBe(0);
});

it('reuses entries on writes and preserves pinned access order on release',()=>{
  const actual = new RowRecency();
  actual.set('a',{entity:'tasks',id:'a'},false);
  actual.set('b',{entity:'tasks',id:'b'},false);
  const entry = actual['lookup']('a');
  actual.set('a',{entity:'tasks',id:'a'},false);
  expect(actual['lookup']('a')).toBe(entry);
  expect(actual.oldest()?.key).toBe('b');
  actual.set('b',{entity:'tasks',id:'b'},true);
  actual.touch('a'); actual.pin('b',false);
  expect(actual.oldest()?.key).toBe('b');
});

it('keeps independent row/execution versions and clears reused slots',()=>{
  const actual = new RowRecency();
  actual.set('tasks\0a',{entity:'tasks',id:'a'},false);
  expect(actual.setVersion('tasks\0a',4)).toBe(true);
  expect(actual.setVersion('tasks\0a',7,true)).toBe(true);
  actual.set('tasks\0a',{entity:'tasks',id:'a'},true);
  expect(actual.version('tasks\0a')).toBe(4);
  expect(actual.version('tasks\0a',true)).toBe(7);
  actual.set('tasks\0b',{entity:'tasks',id:'b'},false);
  actual.delete('tasks\0a');
  actual.set('tasks\0c',{entity:'tasks',id:'c'},false);
  expect(actual.version('tasks\0c')).toBeUndefined();
  expect(actual.version('tasks\0c',true)).toBeUndefined();
  expect(actual.setVersion('tasks\0missing',8)).toBe(false);
});
