import { expect, it } from 'vitest';
import { encodedFrameHasCanonicalStructure as eligible, admitSQLiteUTF8Frame, sqliteEncodedKeyCacheStats } from './sqlite-encoded-frame.js';
it('checks decoded object-key identity without decoding row values', () => {
  expect(eligible('[{"a":1,"nested":{"a":2}},{"a":3,"text":"{ \\"a\\":1 }"}]')).toBe(true);
  expect(eligible('[{"a":1,"a":2}]')).toBe(false);
  expect(eligible('[{"a":1,"\\u0061":2}]')).toBe(false);
  expect(eligible('[{"nested":{"😀":1,"\\ud83d\\ude00":2}}]')).toBe(false);
  expect(eligible(JSON.stringify([{ quote: '" \\ braces {} []', _id: 'a', values: [null, true, false, -0, 1e30, 1e-30] }]))).toBe(true);
});
it('retains the finite-number and exact depth boundary of the canonical decoder', () => {
  expect(eligible('[{"n":1e999}]')).toBe(false);
  expect(eligible('[{"n":-1e999}]')).toBe(false);
  expect(eligible('[{"row":{"data":'+'['.repeat(29)+'0'+']'.repeat(29)+'}}]')).toBe(true);
  for (const scalar of ['0','null','true','false','"text"'])
    expect(eligible('[{"row":{"data":'+'['.repeat(30)+scalar+']'.repeat(30)+'}}]')).toBe(false);
});

it('admits strict UTF-8 byte frames without accepting overlong, surrogate or truncated encodings', () => {
  const text = '[{"東京":"CAFÉ 😀","nested":{"a":1}}]';
  const bytes = new TextEncoder().encode(text);
  expect(() => admitSQLiteUTF8Frame(bytes, 4096)).not.toThrow();
  expect(eligible(bytes)).toBe(eligible(text));
  expect(eligible(new TextEncoder().encode('[{"東京":1,"\\u6771京":2}]'))).toBe(false);
  for (const malformed of [[128],[192,128],[193,191],[224,159,191],[237,160,128],[240,143,191,191],[244,144,128,128],[245,128,128,128],[226,130],[226,65,172]])
    expect(() => admitSQLiteUTF8Frame(Uint8Array.from(malformed),4096)).toThrow('Invalid UTF-8');
  for (const codepoint of [0x7f,0x80,0x7ff,0x800,0xd7ff,0xe000,0xffff,0x10000,0x10ffff])
    expect(() => admitSQLiteUTF8Frame(new TextEncoder().encode(JSON.stringify(String.fromCodePoint(codepoint))),4096)).not.toThrow();
  expect(() => admitSQLiteUTF8Frame(bytes, 40)).toThrow('budget');
});

it('caps repeated UTF-8 key decoding and checks raw tokens across hash collisions',()=>{
  const encode=(text:string)=>new TextEncoder().encode(text);
  for(let n=0;n<200;n++) expect(eligible(encode(JSON.stringify([{['field'+n]:n}])))).toBe(true);
  expect(sqliteEncodedKeyCacheStats()).toMatchObject({maxBytes:8192,maxEntries:32});
  expect(sqliteEncodedKeyCacheStats().bytes).toBeLessThanOrEqual(8192);
  expect(sqliteEncodedKeyCacheStats().entries).toBeLessThanOrEqual(32);
  expect(sqliteEncodedKeyCacheStats().evictions).toBeGreaterThan(0);
  // These quoted wire tokens collide under FNV-1a. A cache hit still
  // compares every byte; a digest is never object-key identity.
  const hash=(text:string)=>{let h=2166136261;for(const byte of encode(text))h=Math.imul(h^byte,16777619)>>>0;return h;};
  expect(hash('"92fe5e95"')).toBe(hash('"177585e0"'));
  expect(eligible(encode('[{"92fe5e95":1,"177585e0":2}]'))).toBe(true);
  expect(eligible(encode('[{"92fe5e95":1,"177585e0":2,"92fe5e95":3}]'))).toBe(false);
  expect(eligible(encode('[{"東京":1,"\\u6771京":2}]'))).toBe(false);
});
