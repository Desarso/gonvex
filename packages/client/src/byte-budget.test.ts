import { expect, it } from 'vitest';
import { ByteLedger, BoundedByteCache } from './byte-budget.js';
it('enforces a hard byte ceiling and LRU order while keeping mounted pages pinned', () => {
  const ledger = new ByteLedger(), released: string[] = [];
  const cache = new BoundedByteCache<string, string>('pages', 100, 3, ledger, value => released.push(value));
  cache.put('a', 'a', 40); cache.put('b', 'b', 40);
  const unpin = cache.pin('a')!;
  cache.put('c', 'c', 60);
  expect(cache.get('b')).toBeUndefined(); expect(cache.get('a')).toBe('a');
  expect(ledger.bytes).toBe(100); expect(released).toEqual(['b']);
  expect(() => cache.put('d', 'd', 101)).toThrow('Memory budget');
  expect(ledger.bytes).toBe(100); expect(cache.get('c')).toBe('c');
  expect(() => cache.put('a', 'replacement', 1)).toThrow('Memory budget');
  unpin(); unpin(); cache.shrink(); expect(ledger.bytes).toBe(0);
  expect(cache.stats()).toMatchObject({ entries: 0, evictions: 3, refusals: 2 });
  cache.close(); cache.close(); expect(ledger.snapshot()).toEqual([]);
});
it('bounds entry metadata even when values are tiny and refuses admission without partial eviction', () => {
  const ledger = new ByteLedger(), cache = new BoundedByteCache('labels', 100000, 2, ledger);
  cache.put(1, 'one', 1); const unpin = cache.pin(1)!; cache.put(2, 'two', 1); cache.pin(2);
  expect(() => cache.put(3, 'three', 1)).toThrow('Memory budget');
  expect(cache.get(1)).toBe('one'); expect(cache.get(2)).toBe('two');
  unpin(); cache.put(3, 'three', 1); expect(cache.get(1)).toBeUndefined(); expect(cache.get(3)).toBe('three');
});
it('rejects a duplicate ledger and releases resources once even when cleanup throws', () => {
  const ledger = new ByteLedger(); let releases = 0;
  const cache = new BoundedByteCache('buffers', 100, 2, ledger, () => { releases++; throw Error('cleanup'); });
  expect(() => new BoundedByteCache('buffers', 100, 2, ledger)).toThrow('duplicate');
  cache.put('one', new Uint8Array(20), 64); cache.close(); cache.close();
  expect(releases).toBe(1); expect(ledger.bytes).toBe(0); expect(() => cache.put('two', new Uint8Array(1), 20)).toThrow('closed');
});
