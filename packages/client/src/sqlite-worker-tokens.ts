import {ByteLedger} from './byte-budget.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
export type SQLiteTokenRequest={id:string;connection:string;identityKey:string;forceRefreshToken:boolean};
type Pending={request:SQLiteTokenRequest;tab:string;bytes:number;timer:ReturnType<typeof setTimeout>;promise:Promise<string|null>;resolve(token:string|null):void;reject(error:unknown):void};
type Options={
 /** Only tabs currently mapped to the same connection and auth hint qualify. */
 recipient(connection:string,identityKey:string,exclude?:string):string|undefined;
 eligible(tab:string,connection:string,identityKey:string):boolean;
 /** One credited metadata hint prompts the tab to poll its pending controls. */
 notifyTab(tabID:string):Promise<void>;
 timeoutMs?:number;
};
function key(value:unknown,max:number):asserts value is string{if(typeof value!=='string'||!value||value.length>max)throw new TypeError('Invalid worker token identity');}
function unavailable(){return Object.assign(Error('Token provider is unavailable'),{code:'FENCED'});}
/** Credentials remain with the app auth provider. The worker requests a fresh
 * token from one eligible tab, correlates its reply and forgets the request.
 * The broker's credited hint coalesces while a renderer is frozen; no timer
 * posts a stream of messages and no token is broadcast to other tabs. */
export class SQLiteWorkerTokens {
 private readonly pending=new Map<string,Pending>();private readonly ledger=new ByteLedger(65536);private readonly unregister:()=>void;private closed=false;
 private readonly timeout:number;private bytes=0;private refusals=0;
 constructor(private readonly options:Options){this.timeout=options.timeoutMs??30000;if(!Number.isSafeInteger(this.timeout)||this.timeout<1||this.timeout>60000)throw new TypeError('Invalid token timeout');this.unregister=this.ledger.register('worker-token-requests',()=>this.stats());}
 fetch(connection:string,identityKey:string,forceRefreshToken:boolean):Promise<string|null>{
  key(connection,128);key(identityKey,512);if(typeof forceRefreshToken!=='boolean')throw new TypeError('Invalid token refresh request');if(this.closed)return Promise.reject(unavailable());
  const old=this.pending.get(connection);if(old){if(old.request.identityKey===identityKey&&old.request.forceRefreshToken===forceRefreshToken)return old.promise;return Promise.reject(Object.assign(Error('Token refresh already pending'),{code:'BUSY'}));}
  const tab=this.options.recipient(connection,identityKey);if(!tab)return Promise.reject(unavailable());key(tab,128);
  const request={id:crypto.randomUUID(),connection,identityKey,forceRefreshToken},bytes=sqlitePayloadBytes({request,tab},4096)+512;
  if(this.pending.size>=8||this.bytes+bytes>this.ledger.maxBytes){this.refusals++;return Promise.reject(Object.assign(Error('Token request budget exceeded'),{code:'BUDGET'}));}
  let resolve!:(token:string|null)=>void,reject!:(error:unknown)=>void;const promise=new Promise<string|null>((yes,no)=>{resolve=yes;reject=no;});
  const entry:Pending={request,tab,bytes,promise,resolve,reject,timer:setTimeout(()=>this.remove(entry,unavailable()),this.timeout)};this.pending.set(connection,entry);this.bytes+=bytes;
  this.hint(entry,tab);return promise;
 }
 private hint(entry:Pending,tab:string){void Promise.resolve().then(()=>{if(this.pending.get(entry.request.connection)===entry&&entry.tab===tab)return this.options.notifyTab(tab);}).catch(error=>{if(entry.tab===tab)this.remove(entry,error);});}
 private remove(entry:Pending,error?:unknown,token:string|null=null){if(this.pending.get(entry.request.connection)!==entry)return;this.pending.delete(entry.request.connection);this.bytes-=entry.bytes;clearTimeout(entry.timer);if(error)entry.reject(error);else entry.resolve(token);}
 poll(tab:string):SQLiteTokenRequest[]{key(tab,128);const requests=[...this.pending.values()].filter(entry=>entry.tab===tab).map(entry=>({...entry.request}));sqlitePayloadBytes(requests,32768);return requests;}
 respond(tab:string,input:{id:string;connection:string;identityKey:string;token?:string|null;error?:string}):void{
  key(tab,128);sqlitePayloadBytes(input,65536);key(input.id,128);key(input.connection,128);key(input.identityKey,512);
  if(input.token!==undefined&&input.token!==null&&(typeof input.token!=='string'||input.token.length>16384)||input.error!==undefined&&(typeof input.error!=='string'||input.error.length>512)||input.error!==undefined&&input.token!==undefined)throw new TypeError('Invalid token response');
  const entry=this.pending.get(input.connection);if(!entry||entry.tab!==tab||entry.request.id!==input.id||entry.request.identityKey!==input.identityKey)fail('FENCED','Token response no longer matches its authorized tab');
  if(!this.options.eligible(tab,input.connection,input.identityKey))fail('FENCED','Token provider admission changed');
  this.remove(entry,input.error?unavailable():undefined,input.token??null);
 }
 invalidate(connection:string){key(connection,128);const entry=this.pending.get(connection);if(entry)this.remove(entry,unavailable());}
 detachTab(tab:string){
  key(tab,128);for(const entry of this.pending.values())if(entry.tab===tab){const replacement=this.options.recipient(entry.request.connection,entry.request.identityKey,tab);if(!replacement){this.remove(entry,unavailable());continue;}key(replacement,128);this.bytes-=entry.bytes;entry.tab=replacement;entry.bytes=sqlitePayloadBytes({request:entry.request,tab:replacement},4096)+512;this.bytes+=entry.bytes;this.hint(entry,replacement);}
 }
 stats(){return {name:'worker-token-requests',bytes:this.bytes,maxBytes:this.ledger.maxBytes,entries:this.pending.size,maxEntries:8,evictions:0,refusals:this.refusals};}
 close(){if(this.closed)return;this.closed=true;for(const entry of this.pending.values())this.remove(entry,unavailable());this.unregister();}
}
