import {expect,it} from 'vitest';
import {SQLiteOwnerTabs,type SQLiteOwnerChange} from './sqlite-owner-tabs.js';
it('bounds a frozen tab to one queued message through 100,000 commits and delivers the latest reset on acknowledgement',()=>{
 const sent:Array<{tab:string;change:SQLiteOwnerChange}>=[],tabs=new SQLiteOwnerTabs((tab,change)=>sent.push({tab,change}));tabs.register('frozen');
 for(let sequence=1;sequence<=100000;sequence++)tabs.publish(sequence%2?'tasks':'members',{epoch:1,sequence});
 expect(sent).toHaveLength(1);expect(tabs.stats()).toMatchObject({entries:1,inFlight:1,pending:1,sent:1});expect(tabs.stats().bytes).toBeLessThan(4096);
 tabs.acknowledge('frozen',sent[0].change.notification,{epoch:1,sequence:999});expect(sent).toHaveLength(1);
 tabs.acknowledge('frozen',sent[0].change.notification,sent[0].change);expect(sent).toHaveLength(2);expect(sent[1].change).toMatchObject({scope:'*',reset:true,epoch:1,sequence:100000});
 tabs.acknowledge('frozen',sent[0].change.notification,sent[0].change);expect(sent).toHaveLength(2);tabs.acknowledge('frozen',sent[1].change.notification,sent[1].change);expect(tabs.stats()).toMatchObject({inFlight:0,pending:0});tabs.close();
});
it('releases expired subscriptions and aborts their calls, then resets a resumed tab to the current generation',()=>{
 const sent:SQLiteOwnerChange[]=[],dropped:string[]=[],tabs=new SQLiteOwnerTabs((_tab,change)=>sent.push(change),tab=>dropped.push(tab)),signal=tabs.register('tab',100);
 tabs.publish('tasks',{epoch:2,sequence:5});tabs.expire(30200);expect(signal.aborted).toBe(true);expect(dropped).toEqual(['tab']);expect(tabs.stats().entries).toBe(0);
 const resumed=tabs.register('tab',30300);expect(resumed.aborted).toBe(false);expect(sent.at(-1)).toMatchObject({scope:'*',reset:true,epoch:2,sequence:5});
 tabs.publish('tasks',{epoch:1,sequence:999});expect(sent).toHaveLength(2);tabs.close();expect(resumed.aborted).toBe(true);expect(tabs.stats().bytes).toBe(0);
});
it('keeps same-scope coalescing precise and bounds tab metadata before admission',()=>{
 const sent:Array<{tab:string;change:SQLiteOwnerChange}>=[],tabs=new SQLiteOwnerTabs((tab,change)=>sent.push({tab,change}));
 for(let n=0;n<64;n++)tabs.register('tab-'+n);expect(()=>tabs.register('extra')).toThrow('allowance');
 tabs.publish('tasks',{epoch:1,sequence:1});tabs.publish('tasks',{epoch:1,sequence:2});tabs.publish('tasks',{epoch:1,sequence:3});expect(sent).toHaveLength(64);
 tabs.acknowledge('tab-0',sent[0].change.notification,sent[0].change);expect(sent.at(-1)!.change).toMatchObject({scope:'tasks',sequence:3});expect(sent.at(-1)!.change.reset).toBeUndefined();expect(tabs.stats().bytes).toBeLessThanOrEqual(tabs.stats().maxBytes);tabs.close();
});
it('targets credited control hints and keeps frozen hints bounded while preserving later data changes',()=>{
 const sent:Array<{tab:string;change:SQLiteOwnerChange}>=[],tabs=new SQLiteOwnerTabs((tab,change)=>sent.push({tab,change}));tabs.register('provider');tabs.register('other');
 for(let n=0;n<10000;n++)tabs.notify('provider',{epoch:1,sequence:0});expect(sent).toHaveLength(1);expect(sent[0]).toMatchObject({tab:'provider',change:{scope:'@control',sequence:0}});expect(tabs.stats()).toMatchObject({inFlight:1,pending:1});
 tabs.publish('tasks',{epoch:1,sequence:1});tabs.notify('provider',{epoch:1,sequence:1});expect(sent.filter(item=>item.tab==='provider')).toHaveLength(1);tabs.acknowledge('provider',sent[0].change.notification,sent[0].change);expect(sent.at(-1)).toMatchObject({tab:'provider',change:{scope:'*',reset:true,sequence:1}});expect(tabs.stats().bytes).toBeLessThan(4096);tabs.drop('provider');expect(()=>tabs.notify('provider',{epoch:1,sequence:1})).toThrow('detached');tabs.close();
});

it('preserves a fresh control hint that arrives after polling but before the previous notification is acknowledged',()=>{
 const sent:any[]=[],tabs=new SQLiteOwnerTabs((tab,change)=>sent.push({tab,change}));tabs.setGeneration({epoch:1,sequence:4});tabs.register('provider');const initial=sent[0];
 tabs.notify('provider',{epoch:1,sequence:4});expect(sent).toHaveLength(1);expect(tabs.stats().pending).toBe(1);tabs.acknowledge('provider',initial.change.notification,initial.change);expect(sent).toHaveLength(2);expect(sent[1].change.scope).toBe('@control');expect(sent[1].change.notification).toBeGreaterThan(initial.change.notification);tabs.close();
});

it('credits precommit display hints without advancing the committed generation across rollback',()=>{
 const sent:Array<{tab:string;change:SQLiteOwnerChange}>=[],tabs=new SQLiteOwnerTabs((tab,change)=>sent.push({tab,change}));
 tabs.setGeneration({epoch:3,sequence:7});tabs.register('reader');tabs.register('other');
 for(const item of [...sent])tabs.acknowledge(item.tab,item.change.notification,item.change);
 const before=sent.length;
 for(let n=0;n<10000;n++)tabs.hintPending('reader');
 expect(sent).toHaveLength(before+1);expect(sent.at(-1)).toMatchObject({tab:'reader',change:{scope:'@preview',epoch:3,sequence:7}});
 expect(tabs.stats()).toMatchObject({inFlight:1,pending:1});
 const hint=sent.at(-1)!;tabs.acknowledge('reader',hint.change.notification,hint.change);
 const coalesced=sent.at(-1)!;expect(coalesced.change.scope).toBe('@preview');expect(coalesced.change.sequence).toBe(7);
 tabs.acknowledge('reader',coalesced.change.notification,coalesced.change);
 // Rollback has no committed publication. A retry committing sequence8 must
 // still reach both tabs, and the next preview must use that committed8.
 tabs.publish('tasks',{epoch:3,sequence:8});
 for(const item of sent.slice(-2))tabs.acknowledge(item.tab,item.change.notification,item.change);
 tabs.hintPending('reader');expect(sent.at(-1)).toMatchObject({tab:'reader',change:{scope:'@preview',sequence:8}});
 tabs.drop('reader');expect(()=>tabs.hintPending('reader')).toThrow('detached');tabs.close();
});
