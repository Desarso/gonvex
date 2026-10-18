import {readFileSync} from 'node:fs';
import { expect, it } from 'vitest';
import { createHash } from 'node:crypto';
import { ReplicaSetDigest } from './replica-set-digest.js';
const hash = (text: string) => createHash('sha256').update(text).digest('hex');
it('is order independent, reversible and agrees with integer arithmetic across carries', () => {
  const hashes = Array.from({ length: 10000 }, (_, i) => hash('row ' + i));
  const first = new ReplicaSetDigest(), second = new ReplicaSetDigest();
  let sum = 0n;
  for (const h of hashes) { first.add(h); sum += BigInt('0x' + h.slice(0, 32)); }
  for (const h of [...hashes].reverse()) second.add(h);
  expect(first.digest()).toBe(second.digest());
  expect(first.digest()).toBe('sum128-v1:' + (sum % (1n << 128n)).toString(16).padStart(32, '0'));
  const restored = ReplicaSetDigest.decode(first.encode());
  for (const h of hashes) restored.remove(h);
  expect(restored.digest()).toBe(new ReplicaSetDigest().digest()); expect(restored.count).toBe(0);
  expect(restored.legacyDigest()).toBe(new ReplicaSetDigest().legacyDigest());
  for (let i = 0; i < 256; i++) expect(restored.bucket(i)).toBe('0'.repeat(32));
});
it('retains xor1 compatibility and a fixed receipt size independent of tenant size', () => {
  const digest = new ReplicaSetDigest(), h = hash('hello');
  const size = digest.encode().byteLength; digest.add(h);
  expect(digest.legacyDigest()).toBe('xor1:' + h); digest.add(h);
  expect(digest.legacyDigest()).toBe('xor1:' + '0'.repeat(64));
  expect(digest.digest()).not.toBe(new ReplicaSetDigest().digest());
  expect(digest.encode().byteLength).toBe(size);
  expect(ReplicaSetDigest.decode(digest.encode()).digest()).toBe(digest.digest());
});
it('rejects invalid hashes, underflow and malformed receipts without modifying the summary', () => {
  const digest = new ReplicaSetDigest(), initial = digest.encode();
  expect(() => digest.remove(hash('missing'))).toThrow('count'); expect(() => digest.add('bad')).toThrow('SHA-256');
  expect(digest.encode()).toEqual(initial);
  const invalid = initial.slice(); invalid[0] = 2;
  expect(() => ReplicaSetDigest.decode(invalid)).toThrow('format');
  expect(() => ReplicaSetDigest.decode(new Uint8Array(1))).toThrow('length');
});

it('matches the shared Rust/TypeScript receipt fixture byte for byte',()=>{
  const fixture=JSON.parse(readFileSync(new URL('../../protocol/fixtures/replica-set-digest.json',import.meta.url),'utf8'));
  const digest=new ReplicaSetDigest();
  for(const mutation of fixture.mutations) mutation.op==='add'?digest.add(mutation.hash):digest.remove(mutation.hash);
  expect(digest.count).toBe(fixture.count);expect(digest.digest()).toBe(fixture.digest);expect(digest.legacyDigest()).toBe(fixture.legacy);
  expect(Buffer.from(digest.encode()).toString('hex')).toBe(fixture.receipt);
  expect(ReplicaSetDigest.decode(new Uint8Array(Buffer.from(fixture.receipt,'hex'))).digest()).toBe(fixture.digest);
});
