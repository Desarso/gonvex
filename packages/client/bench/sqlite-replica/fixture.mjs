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
