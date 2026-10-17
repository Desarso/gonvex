import type { ReducerReadView } from '@gonvex/local-runtime/portable';

type Read = Parameters<ReducerReadView['select']>[0];
type Selection = Awaited<ReturnType<ReducerReadView['select']>>;
type Failure = { name: string; message: string; code?: string; read?: Read };
type Request = { kind: 'select'; id: number; read: Read; excluded: readonly string[] }
  | { kind: 'finish' } | { kind: 'abort' };
type Response = { kind: 'ready' | 'closed' }
  | { kind: 'result'; id: number; value: Selection }
  | { kind: 'error'; id?: number; error: Failure };
export type RemoteReadLimits = {
  maxInputBytes: number;
  maxResultBytes: number;
  maxResultRows: number;
  maxViewBytes: number;
  timeoutMs: number;
};
const noop = () => {};
function deferred<T>() {
  let resolve!: (value: T) => void, reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function failure(error: unknown): Failure {
  return { name: error instanceof Error ? error.name : 'Error', message: error instanceof Error ? error.message : String(error),
    ...(typeof (error as { code?: unknown })?.code === 'string' ? { code: (error as { code: string }).code } : {}),
    ...(error instanceof Error && error.name === 'IncompleteReplicaError' && (error as Error & { read?: Read }).read ? { read: (error as Error & { read: Read }).read } : {}) };
}
function restored(value: Failure): Error {
  const error = value.name === 'RangeError' ? new RangeError(value.message) : value.name === 'TypeError' ? new TypeError(value.message) : new Error(value.message);
  return Object.assign(error, { name: value.name, ...(value.code ? { code: value.code } : {}), ...(value.read ? { read: value.read } : {}) });
}
function checkLimits(limits: RemoteReadLimits) {
  for (const value of Object.values(limits)) if (!Number.isSafeInteger(value) || value < 1) throw new RangeError('Positive finite read-view limits required');
}
/** Bound traversal itself before structuredClone allocates a second graph.
 * Iterators avoid allocating an array of every key/child in a large argument.
 * This is a conservative graph estimate, not a renderer-private measurement. */
export function readViewMessageBytes(value: unknown, limit: number): number {
  let bytes = 0;
  const active = new WeakSet<object>();
  const stack: Array<{ owner?: object; iterator: Iterator<unknown> }> = [{ iterator: [value].values() }];
  while (stack.length) {
    const frame = stack[stack.length - 1]!, next = frame.iterator.next();
    if (next.done) { if (frame.owner) active.delete(frame.owner); stack.pop(); continue; }
    const item = next.value;
    if (item === null || item === undefined || typeof item === 'boolean' || typeof item === 'number') bytes += 8;
    else if (typeof item === 'string') bytes += item.length * 2 + 16;
    else if (typeof item === 'object') {
      if (active.has(item)) throw new TypeError('Cyclic replica read message');
      const prototype = Object.getPrototypeOf(item);
      if (!Array.isArray(item) && prototype !== Object.prototype && prototype !== null)
        throw new TypeError('Non-data object in replica read message');
      active.add(item); bytes += 64;
      const iterator = Array.isArray(item) ? (function* () {
        for (let index = 0; index < item.length; index++) {
          const descriptor = Object.getOwnPropertyDescriptor(item, String(index));
          if (descriptor && !('value' in descriptor)) throw new TypeError('Accessor in replica read message');
          yield descriptor?.value;
        }
      })() : (function* () {
        for (const key in item) if (Object.hasOwn(item, key)) {
          bytes += key.length * 2 + 16;
          if (bytes > limit) throw new RangeError('Replica read message budget exceeded');
          const descriptor = Object.getOwnPropertyDescriptor(item, key)!;
          if (!('value' in descriptor)) throw new TypeError('Accessor in replica read message');
          yield descriptor.value;
        }
      })();
      stack.push({ owner: item, iterator });
    } else throw new TypeError('Non-data value in replica read message');
    if (bytes > limit) throw new RangeError('Replica read message budget exceeded');
  }
  return bytes;
}

/** Main-thread side of one dedicated read-view channel. The worker holds one
 * coherent storage transaction for the entire callback, not one per select. */
export async function withRemoteReadView<T>(port: MessagePort, run: (view: ReducerReadView) => Promise<T>, limits: RemoteReadLimits, signal?: AbortSignal): Promise<T> {
  checkLimits(limits);
  const ready = deferred<void>(), closed = deferred<void>(), failed = deferred<never>();
  void ready.promise.catch(noop); void closed.promise.catch(noop); void failed.promise.catch(noop);
  const pending = new Map<number, { resolve: (value: Selection) => void; reject: (error: unknown) => void; bytes: number }>();
  let nextID = 0, queuedBytes = 0, ended = false;
  function reject(error: unknown) { ended = true; failed.reject(error); ready.reject(error); for (const request of pending.values()) request.reject(error); pending.clear(); queuedBytes = 0; }
  const receive = (event: MessageEvent<Response>) => {
    const message = event.data;
    if (message.kind === 'ready') ready.resolve();
    else if (message.kind === 'closed') closed.resolve();
    else if (message.kind === 'error' && message.id === undefined) {
      const error = restored(message.error); closed.reject(error); reject(error);
    }
    else if (message.kind === 'result' || message.kind === 'error') {
      const request = pending.get(message.id!); if (!request) return;
      pending.delete(message.id!); queuedBytes -= request.bytes;
      message.kind === 'result' ? request.resolve(message.value) : request.reject(restored(message.error));
    }
  };
  port.addEventListener('message', receive); port.start();
  const abort = () => reject(signal?.reason ?? new Error('Replica read view aborted'));
  signal?.addEventListener('abort', abort, { once: true });
  if (signal?.aborted) abort();
  const timer = setTimeout(() => {
    const error = new Error('Replica read view timed out');
    closed.reject(error); reject(error);
  }, limits.timeoutMs);
  try {
    await ready.promise;
    const value = await Promise.race([failed.promise, run({ select: (read, excluded = []) => {
      if (ended) return Promise.reject(new Error('Replica read view is closed'));
      const message: Request = { kind: 'select', id: ++nextID, read, excluded };
      const size = readViewMessageBytes(message, limits.maxInputBytes);
      if (queuedBytes + size > limits.maxInputBytes) return Promise.reject(new RangeError('Pending replica reads exceed budget'));
      queuedBytes += size;
      return new Promise<Selection>((resolve, reject) => {
        pending.set(message.id, { resolve, reject, bytes: size });
        try { port.postMessage(message); }
        catch (error) { pending.delete(message.id); queuedBytes -= size; reject(error); }
      });
    } })]);
    ended = true; port.postMessage({ kind: 'finish' } satisfies Request);
    await closed.promise;
    return value;
  } catch (error) {
    ended = true;
    port.postMessage({ kind: 'abort' } satisfies Request);
    // Wait for rollback/cleanup rather than abandoning a transaction lease.
    await closed.promise.catch(noop);
    throw error;
  } finally {
    ended = true; clearTimeout(timer); signal?.removeEventListener('abort', abort); port.removeEventListener('message', receive);
    reject(new Error('Replica read view is closed')); port.close();
  }
}

/** Worker side. The storage actor calls this as a single operation and must
 * serialize other operations until it settles. Finish is acknowledged only
 * after the underlying read transaction has committed or rolled back. The
 * host must propagate the signal to interruptible reads; this transport cannot
 * forcibly unwind an arbitrary native operation that ignores cancellation. */
export async function serveRemoteReadView(port: MessagePort,
  open: (run: (view: ReducerReadView) => Promise<void>, signal: AbortSignal) => Promise<void>, limits: RemoteReadLimits): Promise<void> {
  checkLimits(limits);
  const queue: Array<{ message: Request; bytes: number }> = [];
  const controller = new AbortController();
  let queuedBytes = 0, deliveredBytes = 0, wake: (() => void) | undefined, fatal: unknown, aborted = false;
  const receive = (event: MessageEvent<Request>) => {
    try {
      const message = event.data;
      const bytes = readViewMessageBytes(message, limits.maxInputBytes);
      if (queuedBytes + bytes > limits.maxInputBytes) throw new RangeError('Queued replica reads exceed budget');
      if (!['select', 'finish', 'abort'].includes(message.kind)) throw new Error('Unknown replica read message');
      if (message.kind === 'abort') { controller.abort(new Error('Remote replica read aborted')); queue.length = 0; queuedBytes = 0; }
      queue.push({ message, bytes }); queuedBytes += bytes; wake?.(); wake = undefined;
    } catch (error) { fatal = error; controller.abort(error); wake?.(); wake = undefined; }
  };
  port.addEventListener('message', receive); port.start();
  const timer = setTimeout(() => { fatal = new Error('Replica read lease timed out'); controller.abort(fatal); wake?.(); wake = undefined; }, limits.timeoutMs);
  try {
    await open(async view => {
      port.postMessage({ kind: 'ready' } satisfies Response);
      for (;;) {
        if (fatal) throw fatal;
        const next = queue.shift();
        if (!next) { await new Promise<void>(resolve => { wake = resolve; }); continue; }
        queuedBytes -= next.bytes;
        const message = next.message;
        if (message.kind === 'finish') return;
        if (message.kind === 'abort') { aborted = true; throw new Error('Remote replica read aborted'); }
        try {
          // The extra row detects oversized reads without building their full
          // result. A native adapter must also bound individual value decoding.
          if (message.read.limit !== undefined && (!Number.isSafeInteger(message.read.limit) || message.read.limit < 0)) throw new RangeError('Invalid replica read limit');
          const limit = Math.min(message.read.limit ?? limits.maxResultRows + 1, limits.maxResultRows + 1);
          const value = await view.select({ ...message.read, limit }, message.excluded);
          if (controller.signal.aborted) continue;
          if (value.rows.length > limits.maxResultRows) throw new RangeError('Replica read row budget exceeded');
          const bytes = readViewMessageBytes(value, limits.maxResultBytes);
          if (deliveredBytes + bytes > limits.maxViewBytes) throw new RangeError('Replica read-view result budget exceeded');
          deliveredBytes += bytes;
          port.postMessage({ kind: 'result', id: message.id, value } satisfies Response);
        } catch (error) { port.postMessage({ kind: 'error', id: message.id, error: failure(error) } satisfies Response); }
      }
    }, controller.signal);
    port.postMessage({ kind: 'closed' } satisfies Response);
  } catch (error) {
    port.postMessage(aborted ? { kind: 'closed' } satisfies Response : { kind: 'error', error: failure(error) } satisfies Response);
  } finally {
    clearTimeout(timer); port.removeEventListener('message', receive); wake = undefined; queue.length = 0; port.close();
  }
}
