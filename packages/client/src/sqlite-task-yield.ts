type RefPort = MessagePort & { ref?(): void; unref?(): void };
/** One coalesced task boundary, with no timer-nesting delay. The default pair
 * lives for the SDK worker lifetime; it never retains row data or a callback list. */
export class SQLiteTaskYield {
  private channel?: MessageChannel;
  private pending?: { promise: Promise<void>; resolve(): void; reject(error: Error): void };
  
  private closed = false;
  private readonly complete = () => {
    const pending = this.pending; this.pending = undefined;
    (this.channel?.port1 as RefPort | undefined)?.unref?.(); pending?.resolve();
  };
  yield(): Promise<void> {
    if (this.closed) return Promise.reject(new Error('SQLite task scheduler closed'));
    if (this.pending) return this.pending.promise;
    let resolve!: () => void, reject!: (error: Error) => void;
    const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    this.pending = { promise, resolve, reject };
    if (typeof MessageChannel === 'function') {
      if (!this.channel) {
        this.channel = new MessageChannel(); this.channel.port1.onmessage = this.complete; this.channel.port1.start();
        (this.channel.port2 as RefPort).unref?.();
      }
      (this.channel.port1 as RefPort).ref?.(); this.channel.port2.postMessage(0);
    } else queueMicrotask(this.complete);
    return promise;
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    this.channel?.port1.close(); this.channel?.port2.close(); this.channel = undefined;
    const pending = this.pending; this.pending = undefined;
    pending?.reject(new Error('SQLite task scheduler closed'));
  }
}
const workerTasks = new SQLiteTaskYield();
export const yieldSQLiteTask = () => workerTasks.yield();
