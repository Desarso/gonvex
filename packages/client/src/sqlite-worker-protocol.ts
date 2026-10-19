// Internal worker IPC; independent of the negotiated application wire contract.
export const sqliteWorkerProtocol=2;
// Match outbound admission to the existing owner request envelope. Other tabs
// can still occupy these slots, so BUSY recovery remains necessary.
export const sqliteOwnerRequestCapacity=64;
// Both broker paths and the dedicated owner use one operation contract.
export const sqliteWorkerOperations: ReadonlySet<string> = new Set(['clientCall','nativeWindowPage','nativeEntityPage','snapshotActivate','snapshotAppendEncoded','snapshotAppend','snapshotRead','snapshotDiscard','canonicalCall', 'canonicalRead', 'canonicalGeneration', 'page', 'viewPage', 'count', 'apply', 'outbox', 'acknowledge', 'ping', 'stats', 'integrity', 'windowBegin', 'windowAppend', 'windowPublish', 'windowHeader', 'windowPage', 'windowHashes', 'windowCollect', 'bootstrapBegin', 'bootstrapAppend', 'bootstrapFinish']);

export const sqlitePublishingOperations: ReadonlySet<string> = new Set(['snapshotActivate','apply', 'acknowledge', 'bootstrapFinish']);

/** Scope argument positions are part of the internal adapter transport. */
export const sqliteCanonicalScopeArguments = {
  loadWorkingSet:0,loadEntityRows:0,loadWindowRows:0,loadSession:0,saveSession:0,readChanges:0,readViewPage:0,
  applyTransaction:2,advanceWatermark:2,replaceSnapshot:1,replaceWindow:2,applyWindowDelta:3,removeWindow:2,clear:0,
} as const;
export const sqliteCanonicalWrites:ReadonlySet<string>=new Set(['saveSession','applyTransaction','advanceWatermark','replaceSnapshot','replaceWindow','applyWindowDelta','removeWindow','clear']);
export function sqliteOperationPublishes(op:string,args:unknown):boolean {
  return sqlitePublishingOperations.has(op)||(op==='canonicalCall'&&!!args&&typeof args==='object'&&sqliteCanonicalWrites.has(String((args as {method?:unknown}).method)));
}
