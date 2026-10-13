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
