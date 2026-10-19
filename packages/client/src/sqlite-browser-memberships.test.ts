import {expect,it,vi} from 'vitest';
import {SQLiteBrowserMemberships} from './sqlite-browser-memberships.js';
it('keeps bounded ordered IDs while retained and releases them after the last observer',()=>{
 const store=new SQLiteBrowserMemberships(),notify=vi.fn(),release=store.retain('view'),stop=store.subscribe('view',notify);
 const value={entity:'tasks',ids:['a','b'],total:1000,source:'server' as const,completeness:'complete' as const,freshness:'current' as const};
 store.publish('view',value);expect(store.read('view')).toEqual(value);expect(notify).toHaveBeenCalledOnce();
 value.ids.push('caller-mutation');expect(store.read('view')!.ids).toEqual(['a','b']);
 release();expect(store.stats().entries).toBe(1);store.invalidate();expect(store.read('view')).toBeUndefined();expect(notify).toHaveBeenCalledTimes(2);
 stop();expect(store.stats().bytes).toBe(0);expect(store.stats().entries).toBe(0);
});
it('refuses oversized windows and keeps teardown safe after closing the client',()=>{
 const store=new SQLiteBrowserMemberships(),release=store.retain('view');
 expect(()=>store.publish('view',{entity:'tasks',ids:Array.from({length:257},(_,n)=>String(n)),source:'server',completeness:'complete',freshness:'current'})).toThrow('row budget');
 store.close();release();release();expect(store.stats().bytes).toBe(0);expect(store.stats().entries).toBe(0);
});
