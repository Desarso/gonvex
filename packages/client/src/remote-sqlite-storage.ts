import type { LocalReplicaStorage, ReplicaSnapshot } from './local-replica.js';
import type { SQLiteLocalReplicaStorage } from './sqlite-replica-storage.js';
import { readViewMessageBytes, serveRemoteReadView, withRemoteReadView, type RemoteReadLimits } from './remote-read-view.js';

export type RemoteStorageLimits = {
  maxInputBytes: number;
  maxQueuedBytes: number;
  maxOperations: number;
  maxResultBytes: number;
  timeoutMs: number;
  readView: RemoteReadLimits;
};
const methods = ['load', 'readChanges', 'loadWorkingSet', 'loadWindowRows', 'loadEntityRows', 'loadSession', 'saveSession',
  'applyTransaction', 'advanceWatermark', 'replaceSnapshot', 'replaceWindow', 'applyWindowDelta', 'removeWindow', 'clear'] as const;
type Method = typeof methods[number];
type Request = { id: number; method: Method | 'readView'; args: unknown[]; readPort?: MessagePort };
type Reply = { id: number; value?: unknown; error?: { name: string; message: string } };
const noop = () => {};
function validate(limits: RemoteStorageLimits) {
  for (const value of [limits.maxInputBytes, limits.maxQueuedBytes, limits.maxOperations, limits.maxResultBytes, limits.timeoutMs])
    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError('Positive finite storage-channel limits required');
}
function restored(error: { name: string; message: string }) {
  const value = error.name === 'RangeError' ? new RangeError(error.message) : new Error(error.message);
  value.name = error.name; return value;
}
function snapshotCursor(snapshot: ReplicaSnapshot): ReplicaSnapshot {
  return { entities: {}, liveQueries: {}, ...(snapshot.cursor ? { cursor: snapshot.cursor } : {}) };
}

/** Experimental storage transport. The host owns the one SQLite connection.
 * Caps cover admitted cloned messages, not the caller's existing graphs, native
 * database cache, window metadata or all renderer allocations. Oversized calls
 * fail explicitly; this transport does not silently truncate or split commits.
 * The application must not enable it until snapshot ingestion/migration and
 * native window metadata also obey the same bounds. */
export function remoteSQLiteStorage(port: MessagePort, limits: RemoteStorageLimits): LocalReplicaStorage & { dispose(): void } {
  validate(limits);
  let nextID = 0, queuedBytes = 0, disposed = false;
  const pending = new Map<number, { resolve(value: unknown): void; reject(error: unknown): void; bytes: number; timer: ReturnType<typeof setTimeout> }>();
  const leases = new Set<AbortController>();
  function admission(args: unknown[]): number {
    if (disposed) throw new Error('Storage channel is closed');
    const bytes = readViewMessageBytes(args, limits.maxInputBytes);
    if (pending.size >= limits.maxOperations || queuedBytes + bytes > limits.maxQueuedBytes)
      throw new RangeError('Storage operation queue budget exceeded');
    return bytes;
  }
  function dispose(error = new Error('Storage channel is closed')) {
    if (disposed) return;
    disposed = true;
    for (const controller of leases) controller.abort(error);
    for (const item of pending.values()) { clearTimeout(item.timer); item.reject(error); }
    pending.clear(); queuedBytes = 0; port.removeEventListener('message', receive); port.close();
  }
  const receive = (event: MessageEvent<Reply>) => {
    const message = event.data, item = pending.get(message.id); if (!item) return;
    pending.delete(message.id); queuedBytes -= item.bytes; clearTimeout(item.timer);
    message.error ? item.reject(restored(message.error)) : item.resolve(message.value);
  };
  port.addEventListener('message', receive); port.start();
  async function request(method: Request['method'], args: unknown[], readPort?: MessagePort): Promise<any> {
    const bytes = admission(args), id = ++nextID;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { dispose(new Error('Storage operation timed out')); }, limits.timeoutMs);
      pending.set(id, { resolve, reject, bytes, timer }); queuedBytes += bytes;
      try { port.postMessage({ id, method, args, ...(readPort ? { readPort } : {}) } satisfies Request, readPort ? [readPort] : []); }
      catch (error) { pending.delete(id); queuedBytes -= bytes; clearTimeout(timer); reject(error); }
    });
  }
  return {
    ownsWorkingSetRows: true,
    ownsHydrationRows: true,
    load: scope => request('load', [scope]),
    // The proxy intentionally does not advertise supportsResidentLookup.
    // LocalReplica supplies bounded row IDs instead of an uncloneable callback.
    readChanges: (scope, sequence, interest) => request('readChanges', [scope, sequence, interest]),
    loadWorkingSet: (scope, budget) => request('loadWorkingSet', [scope, budget]),
    loadWindowRows: (scope, signature) => request('loadWindowRows', [scope, signature]),
    loadEntityRows: (scope, entity, ids) => request('loadEntityRows', [scope, entity, ids]),
    loadSession: scope => request('loadSession', [scope]),
    saveSession: (scope, session) => request('saveSession', [scope, session]),
    // The normalized SQL implementation does not use the in-memory snapshot
    // for transaction application. Never clone the caller's whole replica.
    applyTransaction: (transaction, _snapshot, scope) => request('applyTransaction', [transaction, { entities: {}, liveQueries: {} }, scope]),
    advanceWatermark: (windows, cursor, scope) => request('advanceWatermark', [windows, cursor, scope]),
    replaceSnapshot: (snapshot, scope) => request('replaceSnapshot', [snapshot, scope]),
    replaceWindow: async (window, snapshot, scope, projection) => {
      readViewMessageBytes(window, limits.maxInputBytes);
      const rows = projection ?? window.ids.map(id => snapshot.entities[window.entity]?.[id]).filter(row => row !== undefined);
      return request('replaceWindow', [window, snapshotCursor(snapshot), scope, rows]);
    },
    applyWindowDelta: (window, delta, snapshot, scope) => request('applyWindowDelta', [window, delta, snapshotCursor(snapshot), scope]),
    removeWindow: (signature, snapshot, scope) => request('removeWindow', [signature, snapshotCursor(snapshot), scope]),
    clear: scope => request('clear', [scope]),
    async withReadView(scope, coverage, run) {
      const args = [scope, coverage]; admission(args);
      const channel = new MessageChannel(), controller = new AbortController(); leases.add(controller);
      const operation = request('readView', args, channel.port2);
      const result = withRemoteReadView(channel.port1, run, limits.readView, controller.signal);
      // An actor failure must also stop a callback waiting on a lease that
      // never opened. Both promises are observed immediately.
      void operation.catch(error => controller.abort(error));
      try {
        const [read, stored] = await Promise.allSettled([result, operation]);
        if (read.status === 'rejected') throw read.reason;
        if (stored.status === 'rejected') throw stored.reason;
        return read.value;
      }
      finally { leases.delete(controller); channel.port1.close(); channel.port2.close(); }
    },
    dispose,
  };
}

/** One FIFO actor for complete storage operations, including read-view leases.
 * Queue admission is checked at both ends. SQLite's per-statement lane alone
 * cannot keep an unrelated write out of another operation's transaction.
 * The host forwards readSignal to wasmSQLiteDatabase's readSignal callback.
 * Disposal stops new work and queued work; an admitted durable write finishes. */
export function serveSQLiteStorage(port: MessagePort, storage: SQLiteLocalReplicaStorage,
  limits: RemoteStorageLimits, controls: { readSignal?: (signal: AbortSignal | undefined) => void } = {}): { dispose(): Promise<void> } {
  validate(limits);
  const queue: Array<{ request: Request; bytes: number }> = [];
  let queuedBytes = 0, active = false, disposed = false, draining: Promise<void> = Promise.resolve();
  function reply(id: number, value?: unknown, error?: unknown) {
    if (error) port.postMessage({ id, error: { name: error instanceof Error ? error.name : 'Error', message: String(error instanceof Error ? error.message : error).slice(0, 2048) } } satisfies Reply);
    else { readViewMessageBytes(value, limits.maxResultBytes); port.postMessage({ id, value } satisfies Reply); }
  }
  async function drain() {
    if (active) return;
    active = true;
    try {
      while (queue.length) {
        const entry = queue.shift()!;
        const { id, method, args, readPort } = entry.request;
        try {
          if (disposed) throw new Error('Storage actor is closed');
          if (method === 'readView') {
            if (!readPort) throw new Error('Missing read-view port');
            await serveRemoteReadView(readPort, async (run, signal) => {
              controls.readSignal?.(signal);
              try { await storage.withReadView(args[0] as string, args[1] as Parameters<SQLiteLocalReplicaStorage['withReadView']>[1], run); }
              finally { controls.readSignal?.(undefined); }
            }, limits.readView);
            reply(id);
          } else {
            const fn = storage[method] as (...args: unknown[]) => Promise<unknown>;
            reply(id, await fn.apply(storage, args));
          }
        } catch (error) { reply(id, undefined, error); readPort?.close(); }
        finally { queuedBytes -= entry.bytes; }
      }
    } finally { active = false; }
  }
  const receive = (event: MessageEvent<Request>) => {
    const request = event.data;
    try {
      if (disposed) throw new Error('Storage actor is closed');
      if (!Number.isSafeInteger(request.id) || request.id < 1 || ![...methods, 'readView'].includes(request.method)) throw new Error('Invalid storage operation');
      const bytes = readViewMessageBytes(request.args, limits.maxInputBytes);
      if (queue.length + Number(active) >= limits.maxOperations || queuedBytes + bytes > limits.maxQueuedBytes) throw new RangeError('Storage actor queue budget exceeded');
      queue.push({ request, bytes }); queuedBytes += bytes;
      if (!active) draining = drain();
    } catch (error) { reply(request.id, undefined, error); request.readPort?.close(); }
  };
  port.addEventListener('message', receive); port.start();
  return { async dispose() { disposed = true; port.removeEventListener('message', receive); await draining; port.close(); } };
}
