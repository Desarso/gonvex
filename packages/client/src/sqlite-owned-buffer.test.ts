import {expect,it} from 'vitest';
import {releaseOwnedSQLiteBytes} from './sqlite-owned-buffer.js';
it('releases a private backing store and leaves the source and repeated cleanup intact',()=>{
 const source=new Uint8Array([1,2,3]),owned=new Uint8Array(source);
 expect(releaseOwnedSQLiteBytes(owned)).toBe(3);expect(owned.byteLength).toBe(0);
 expect(source).toEqual(new Uint8Array([1,2,3]));expect(releaseOwnedSQLiteBytes(owned)).toBe(0);
});
it('keeps bounded ordinary cleanup when transfer is unavailable or refuses release',()=>{
 for(const method of [undefined,()=>{throw Error('unavailable');}]){
  const owned=new Uint8Array([1,2,3]);Object.defineProperty(owned.buffer,'transfer',{value:method});
  expect(releaseOwnedSQLiteBytes(owned)).toBe(0);expect(owned.byteLength).toBe(3);
 }
 expect(releaseOwnedSQLiteBytes('text')).toBe(0);
});
