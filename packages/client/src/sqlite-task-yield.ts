type RefPort = MessagePort & { ref?(): void; unref?(): void };
/** One coalesced task boundary, with no timer-nesting delay.
 * User-visible priority prevents a yielded active SQL transaction from being
 * starved by incoming messages; queued callbacks never interrupt that transaction. The default pair
 * lives for the SDK worker lifetime; it never retains row data or a callback list. */
export class SQLiteTaskYield {
  private channel?: MessageChannel;
  private timer?: ReturnType<typeof setTimeout>;
  private pending?: { promise: Promise<void>; resolve(): void; reject(error: Error): void };
  
  private closed = false;
  private readonly complete = () => {
    const pending = this.pending; this.pending = undefined;this.timer=undefined;
    (this.channel?.port1 as RefPort | undefined)?.unref?.(); pending?.resolve();
  };
  yield(): Promise<void> {
    if (this.closed) return Promise.reject(new Error('SQLite task scheduler closed'));
    if (this.pending) return this.pending.promise;
    let resolve!: () => void, reject!: (error: Error) => void;
    const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    this.pending = { promise, resolve, reject };
    const scheduler=(globalThis as typeof globalThis & {scheduler?:{postTask(callback:()=>void,options:{priority:string}):Promise<void>}}).scheduler;
    if(scheduler?.postTask){void scheduler.postTask(this.complete,{priority:'user-visible'}).catch(error=>{const pending=this.pending;this.pending=undefined;pending?.reject(error);});}
    else if (typeof MessageChannel === 'function') {
      if (!this.channel) {
        this.channel = new MessageChannel(); this.channel.port1.onmessage = this.complete; this.channel.port1.start();
        (this.channel.port2 as RefPort).unref?.();
      }
      (this.channel.port1 as RefPort).ref?.(); this.channel.port2.postMessage(0);
    } else this.timer=setTimeout(this.complete,0);
    return promise;
  }
  close() {
    if (this.closed) return;
    this.closed = true;if(this.timer!==undefined)clearTimeout(this.timer);
    this.channel?.port1.close(); this.channel?.port2.close(); this.channel = undefined;
    const pending = this.pending; this.pending = undefined;
    pending?.reject(new Error('SQLite task scheduler closed'));
  }
}
const workerTasks = new SQLiteTaskYield();
export const yieldSQLiteTask = () => workerTasks.yield();
