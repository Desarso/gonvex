import {expect,it} from 'vitest';
import {SQLiteLastVisiblePages,type SQLiteDisplayFence} from './sqlite-last-visible-pages.js';
const fence:SQLiteDisplayFence={url:'ws://private/ws',artifact:'artifact',project:'project',tenant:'tenant',identity:{account:'account'},credential:'private-credential'};
function storage(){const values=new Map<string,string>();return {values,getItem:(key:string)=>values.get(key)??null,setItem:(key:string,value:string)=>{values.set(key,value);},removeItem:(key:string)=>{values.delete(key);}};}
const page=(name='Verified')=>({offset:0,nextOffset:null,generation:{epoch:1,sequence:2},rows:[{id:'a',value:{id:'a',name}}],window:{header:{entity:'tasks',completeness:'complete',truncated:false}}});
async function seed(s:ReturnType<typeof storage>){const writer=new SQLiteLastVisiblePages(s);await writer.open(fence);writer.remember('tasks',page(),'admission');await writer.persist();writer.close();}
it('restores a sealed display page once without persisting credentials or a second decoded graph',async()=>{
 const s=storage();await seed(s);expect([...s.values.values()].join('')).not.toContain(fence.credential);
 const reader=new SQLiteLastVisiblePages(s);expect(await reader.open(fence)).toBe(1);
 const saved=reader.read('tasks');expect(saved).toEqual(page());expect(reader.restored(saved)).toBe(true);expect(reader.read('tasks')).toBeUndefined();
 expect(reader.stats().bytes).toBeLessThan(2097152);
});
it('refuses another account, tenant, credential, project, endpoint or artifact',async()=>{
 for(const change of [{identity:{account:'other'}},{tenant:'other'},{credential:'other'},{project:'other'},{url:'ws://other/ws'},{artifact:'other'}]){
  const s=storage();await seed(s);const reader=new SQLiteLastVisiblePages(s);expect(await reader.open({...fence,...change})).toBe(0);expect(reader.read('tasks')).toBeUndefined();
 }
});
it('refuses altered payloads and clears the sealed copy on revocation or a changed admission',async()=>{
 const s=storage();await seed(s);
 const [key,raw]=[...s.values][0];s.values.set(key,raw.replace('Verified','Fabricated'));
 expect(await new SQLiteLastVisiblePages(s).open(fence)).toBe(0);
 await seed(s);const reader=new SQLiteLastVisiblePages(s);await reader.open(fence);
 expect(reader.admit('different-admission')).toBe(false);expect(reader.read('tasks')).toBeUndefined();expect(s.values.size).toBe(0);
 await seed(s);const logout=new SQLiteLastVisiblePages(s);await logout.open(fence);logout.revoke();expect(s.values.size).toBe(0);expect(logout.available()).toBe(false);
});
it('never accepts partial, truncated, continuation or oversized pages',async()=>{
 const s=storage(),writer=new SQLiteLastVisiblePages(s);await writer.open(fence);
 for(const value of [{...page(),nextOffset:1},{...page(),offset:1},{...page(),window:{header:{completeness:'partial'}}},{...page(),window:{header:{completeness:'complete',truncated:true}}},{...page(),rows:Array.from({length:129},()=>({id:'a',value:{}}))}]){
  writer.remember('bad',value,'admission');expect(writer.stats().entries).toBe(0);
 }
 writer.remember('large',page('x'.repeat(150000)),'admission');expect(writer.stats().entries).toBe(0);
});
it('does not let an in-flight old fence or seal revive cleared data',async()=>{
 const s=storage(),writer=new SQLiteLastVisiblePages(s);const opening=writer.open(fence);writer.revoke();expect(await opening).toBe(0);expect(writer.available()).toBe(false);
 await writer.open(fence);writer.remember('tasks',page(),'admission');const sealing=writer.persist();writer.revoke();await sealing;expect(s.values.size).toBe(0);
});
it('retires an old complete display page when the owner publishes partial coverage',async()=>{
 const s=storage(),writer=new SQLiteLastVisiblePages(s);await writer.open(fence);
 writer.remember('tasks',page(),'admission');await writer.persist();expect(s.values.size).toBe(1);
 writer.remember('tasks',{...page(),window:{header:{completeness:'partial',truncated:true}}},'admission');await writer.persist();
 expect(s.values.size).toBe(0);expect(await new SQLiteLastVisiblePages(s).open(fence)).toBe(0);
});
it('bounds retained entries and treats storage refusal or corruption as an ordinary miss',async()=>{
 const s=storage(),writer=new SQLiteLastVisiblePages(s);await writer.open(fence);
 for(let n=0;n<220;n++)writer.remember('page-'+n,page(),'admission');
 expect(writer.stats().entries).toBe(192);await writer.persist();expect([...s.values.values()][0].length*2).toBeLessThanOrEqual(2097152);
 s.values.set([...s.values.keys()][0],'not-json');expect(await new SQLiteLastVisiblePages(s).open(fence)).toBe(0);
 const denied=new SQLiteLastVisiblePages({getItem(){throw Error('denied');},setItem(){throw Error('quota');},removeItem(){throw Error('denied');}});
 expect(await denied.open(fence)).toBe(0);denied.remember('tasks',page(),'admission');await expect(denied.persist()).resolves.toBeUndefined();denied.revoke();
});
