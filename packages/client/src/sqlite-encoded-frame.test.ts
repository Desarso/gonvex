import { expect, it } from 'vitest';
import { encodedFrameHasCanonicalStructure as eligible } from './sqlite-encoded-frame.js';
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
