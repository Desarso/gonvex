// Synthetic component data only. No application or customer records.
export function fixtureWrites(start, count) {
  return Array.from({length:count}, (_,index)=>{
    const id=start+index;
    return {table:'tasks',id:String(id),version:1,row:{_id:String(id),name:`Task ${String(id).padStart(8,'0')} café 東京`,workspace:String(id%10),due:id%365,version:1,description:'Synthetic maintenance procedure. '.repeat(15)+(id%997===0?'rare-needle 🔧':'')}};
  });
}

const ordinaryDescription = JSON.stringify('Synthetic maintenance procedure. '.repeat(15));
const rareDescription = JSON.stringify('Synthetic maintenance procedure. '.repeat(15) + 'rare-needle 🔧');
/** Identical wire bytes, without manufacturing row objects in the SQLite owner.
 * Real sync supplies encoded frames; fixture allocation is measured separately. */
export function fixtureEncodedWrites(start, count) {
  if (!Number.isSafeInteger(start) || start < 0 || !Number.isSafeInteger(count) || count < 0 || count > 512) throw Error('Invalid fixture frame');
  const rows = new Array(count);
  for (let index = 0; index < count; index++) {
    const id = start + index, key = JSON.stringify(String(id));
    const name = JSON.stringify(`Task ${String(id).padStart(8, '0')} café 東京`);
    rows[index] = `{"table":"tasks","id":${key},"version":1,"row":{"_id":${key},"name":${name},"workspace":"${id % 10}","due":${id % 365},"version":1,"description":${id % 997 === 0 ? rareDescription : ordinaryDescription}}}`;
  }
  return '[' + rows.join(',') + ']';
}

const encoder = new TextEncoder();
const ordinaryUTF8 = encoder.encode(ordinaryDescription);
const rareUTF8 = encoder.encode(rareDescription);
/** Direct byte transport fixture: no full UTF-16 frame or row-object graph. */
export function fixtureUTF8Writes(start, count, reusable) {
  if (!Number.isSafeInteger(start) || start < 0 || !Number.isSafeInteger(count) || count < 0 || count > 512 || !Number.isSafeInteger(start + count)) throw Error('Invalid fixture frame');
  if(reusable!==undefined&&(!(reusable instanceof Uint8Array)||reusable.byteLength<count*1024+2))throw Error('Fixture buffer capacity');
  const bytes = reusable ?? new Uint8Array(count * 1024 + 2); let at = 0;
  const text = value => { const encoded = encoder.encodeInto(value, bytes.subarray(at)); if (encoded.read !== value.length) throw Error('Fixture frame overflow'); at += encoded.written; };
  bytes[at++] = 91;
  for (let index = 0; index < count; index++) {
    const id = start + index, key = JSON.stringify(String(id));
    if (index) bytes[at++] = 44;
    text(`{"table":"tasks","id":${key},"version":1,"row":{"_id":${key},"name":${JSON.stringify(`Task ${String(id).padStart(8,'0')} café 東京`)},"workspace":"${id % 10}","due":${id % 365},"version":1,"description":`);
    const description = id % 997 === 0 ? rareUTF8 : ordinaryUTF8;
    bytes.set(description, at); at += description.length; bytes[at++] = 125; bytes[at++] = 125;
  }
  bytes[at++] = 93; return bytes.subarray(0, at);
}
