// Synthetic component data only. No application or customer records.
export function fixtureWrites(start, count) {
  return Array.from({length:count}, (_,index)=>{
    const id=start+index;
    return {table:'tasks',id:String(id),version:1,row:{_id:String(id),name:`Task ${String(id).padStart(8,'0')} café 東京`,workspace:String(id%10),due:id%365,version:1,description:'Synthetic maintenance procedure. '.repeat(15)+(id%997===0?'rare-needle 🔧':'')}};
  });
}
