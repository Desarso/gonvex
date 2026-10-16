import {expect, it} from 'vitest';
import {compactReplicaRow} from './compact-row.js';
it('preserves fields, insertion order, explicit nulls, numbers and nested immutable values', () => {
  const nested = Object.freeze({a:'shared'});
  const row = Object.fromEntries(Array.from({length:30},(_,i)=>['field'+i, i%3===0?null:i%3===1?'repeat':i+.5]));
  row.nested = nested;
  const next = compactReplicaRow(row);
  expect(next).toEqual(row); expect(Object.keys(next)).toEqual(Object.keys(row));
  expect(next.nested).toBe(nested); expect(next).not.toBe(row);
  next.field1='changed'; expect(row.field1).toBe('repeat');
});
it('preserves JSON __proto__ fields without changing the object prototype', () => {
  const row = JSON.parse('{"__proto__":{"safe":true},'+Array.from({length:20},(_,i)=>'"f'+i+'":null').join(',')+'}');
  const next = compactReplicaRow(row);
  expect(Object.getPrototypeOf(next)).toBe(Object.prototype);
  expect(Object.hasOwn(next,'__proto__')).toBe(true); expect(next).toEqual(row);
});
it('does not rebuild an already immutable shared row',()=>{
  const row=Object.freeze(Object.fromEntries(Array.from({length:30},(_,i)=>['field'+i,i])));
  expect(compactReplicaRow(row)).toBe(row);
});
