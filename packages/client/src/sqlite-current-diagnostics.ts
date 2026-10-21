import {sqlitePayloadBytes} from './sqlite-admission.js';
/** Current counters only. Histories belong to the diagnostic consumer. */
export function sqliteCurrentDiagnostics(value:Record<string,unknown>){
 const current:Record<string,unknown>={};
 for(const key of ['originEpoch','persistence','wasmBytes','sqliteMemory','statementBytes','clients','queuedOperations','queuedBytes','requests','requestBytes','program','pendingPages','workerLease','delivery']){
  const item=value[key];try{sqlitePayloadBytes({...current,[key]:item},49152);current[key]=item;}catch{/* Optional diagnostics must never overflow a reply. */}
 }
 for(const key of ['ownerQueue','transactions']){const source=value[key] as Record<string,unknown>|undefined;if(source)current[key]=Object.fromEntries(Object.entries(source).filter(([,v])=>typeof v==='number'));}
 sqlitePayloadBytes(current,65536);return current;
}
