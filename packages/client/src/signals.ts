/** A writable reactive value. Reads use explicit `.value` access; no proxies. */
export interface Signal<T> extends ReadonlySignal<T> {
  value: T;
}

/** The read-only surface shared by computed values and writable signals. */
export interface ReadonlySignal<T> {
  readonly value: T;
  /** Read the current value without making it a dependency of the active observer. */
  peek(): T;
  /** Listen for value changes. The returned function removes the listener. */
  subscribe(fn: (value: T) => void): () => void;
}

type Subscriber<T> = (value: T) => void;

/** Anything that can be read by a computed value or effect. */
interface Dependency {
  readonly dependents: Set<Observer>;
}

/** Anything whose dependencies are collected while it executes. */
interface Observer {
  readonly dependencies: Set<Dependency>;
  disposed: boolean;
  invalidate(): void;
}

/*
 * Dependency collection uses a stack rather than a single global variable. A
 * stack is important for computed chains: evaluating one computed may evaluate
 * another, and reads must attach to the innermost evaluation in progress.
 */
const evaluationStack: Observer[] = [];

let batchDepth = 0;
let isFlushing = false;
const pendingNotifications = new Set<Notifiable>();
const pendingEffects = new Set<EffectObserver>();

/*
 * A self-updating effect is allowed and is deferred until its current run has
 * completed. To turn accidental `effect(() => count.value++)` loops into a
 * useful error instead of locking the process, one effect may run at most this
 * many times in a single synchronous flush.
 */
const MAX_EFFECT_RUNS_PER_FLUSH = 100;

interface Notifiable {
  notifySubscribers(): void;
}

function track(dependency: Dependency): void {
  const observer = evaluationStack[evaluationStack.length - 1];
  if (observer === undefined || observer.disposed || observer.dependencies.has(dependency)) return;

  observer.dependencies.add(dependency);
  dependency.dependents.add(observer);
}

function detach(observer: Observer): void {
  for (const dependency of observer.dependencies) dependency.dependents.delete(observer);
  observer.dependencies.clear();
}

function queueNotification(node: Notifiable): void {
  pendingNotifications.add(node);
}

function queueEffect(observer: EffectObserver): void {
  if (!observer.disposed) pendingEffects.add(observer);
}

/** Flush synchronously unless a batch or reactive evaluation is still active. */
function flushIfReady(): void {
  if (batchDepth === 0 && evaluationStack.length === 0 && !isFlushing) flush();
}

function flush(): void {
  if (isFlushing) return;
  isFlushing = true;
  const effectRuns = new Map<EffectObserver, number>();

  try {
    // Subscribers run before effects. Either kind of callback may enqueue more
    // work, so alternate until both insertion-ordered sets are empty.
    while (pendingNotifications.size > 0 || pendingEffects.size > 0) {
      if (pendingNotifications.size > 0) {
        const notifications = Array.from(pendingNotifications);
        pendingNotifications.clear();
        for (const node of notifications) node.notifySubscribers();
      }

      if (pendingEffects.size > 0) {
        const effects = Array.from(pendingEffects);
        pendingEffects.clear();
        for (const observer of effects) {
          if (observer.disposed) continue;

          const runs = (effectRuns.get(observer) ?? 0) + 1;
          effectRuns.set(observer, runs);
          if (runs > MAX_EFFECT_RUNS_PER_FLUSH) {
            pendingEffects.clear();
            throw new Error(`Effect exceeded ${MAX_EFFECT_RUNS_PER_FLUSH} synchronous re-runs`);
          }
          observer.run();
        }
      }
    }
  } finally {
    isFlushing = false;
  }
}

class SignalNode<T> implements Signal<T>, Dependency, Notifiable {
  readonly dependents = new Set<Observer>();
  private readonly subscribers = new Set<Subscriber<T>>();

  constructor(private current: T) {}

  get value(): T {
    track(this);
    return this.current;
  }

  set value(next: T) {
    if (Object.is(this.current, next)) return;
    this.current = next;

    // Invalidate the graph immediately. This keeps reads made inside a batch
    // fresh, while actual callbacks remain coalesced until the batch ends.
    for (const observer of Array.from(this.dependents)) observer.invalidate();
    if (this.subscribers.size > 0) queueNotification(this);
    flushIfReady();
  }

  peek(): T {
    return this.current;
  }

  subscribe(fn: Subscriber<T>): () => void {
    this.subscribers.add(fn);
    return () => {
      this.subscribers.delete(fn);
    };
  }

  notifySubscribers(): void {
    // Checking membership makes disposal during an earlier callback effective
    // immediately, even though iteration itself uses a stable snapshot.
    for (const fn of Array.from(this.subscribers)) {
      if (this.subscribers.has(fn)) fn(this.current);
    }
  }
}

class ComputedNode<T> implements ReadonlySignal<T>, Dependency, Observer, Notifiable {
  readonly dependents = new Set<Observer>();
  readonly dependencies = new Set<Dependency>();
  disposed = false;

  private dirty = true;
  private evaluating = false;
  private initialized = false;
  private cached!: T;

  /*
   * Each subscriber keeps its own last-seen value. This avoids reporting a
   * change to a subscriber added midway through a batch, while still letting
   * computed values stay lazy when nobody reads or subscribes to them.
   */
  private readonly subscribers = new Map<Subscriber<T>, T>();

  constructor(private readonly compute: () => T) {}

  get value(): T {
    track(this);
    return this.recompute();
  }

  peek(): T {
    return this.recompute();
  }

  subscribe(fn: Subscriber<T>): () => void {
    this.subscribers.set(fn, this.peek());
    return () => {
      this.subscribers.delete(fn);
    };
  }

  invalidate(): void {
    if (this.dirty) return;
    this.dirty = true;

    // Propagating dirtiness (without recomputing) is what makes the graph
    // pull-based and also lets the effect queue deduplicate diamond paths.
    for (const observer of Array.from(this.dependents)) observer.invalidate();
    if (this.subscribers.size > 0) queueNotification(this);
  }

  notifySubscribers(): void {
    const next = this.peek();
    for (const [fn, previous] of Array.from(this.subscribers)) {
      if (!this.subscribers.has(fn) || Object.is(previous, next)) continue;
      this.subscribers.set(fn, next);
      fn(next);
    }
  }

  private recompute(): T {
    if (!this.dirty && this.initialized) return this.cached;
    if (this.evaluating) throw new Error("Circular computed dependency");

    detach(this);
    this.evaluating = true;
    // Clearing dirty before evaluation allows a write made during `compute` to
    // invalidate this node again, leaving it dirty for the following read.
    this.dirty = false;
    evaluationStack.push(this);
    try {
      this.cached = this.compute();
      this.initialized = true;
      return this.cached;
    } catch (error) {
      // Failed computations are retried on the next read.
      this.dirty = true;
      throw error;
    } finally {
      evaluationStack.pop();
      this.evaluating = false;
      flushIfReady();
    }
  }
}

class EffectObserver implements Observer {
  readonly dependencies = new Set<Dependency>();
  disposed = false;
  private running = false;

  constructor(private readonly callback: () => void) {}

  invalidate(): void {
    // If the callback writes one of its own dependencies, Set insertion queues
    // exactly one follow-up run after the current invocation returns.
    queueEffect(this);
  }

  run(): void {
    if (this.disposed || this.running) return;

    detach(this);
    this.running = true;
    evaluationStack.push(this);
    try {
      this.callback();
    } finally {
      evaluationStack.pop();
      this.running = false;
    }
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    pendingEffects.delete(this);
    detach(this);
  }
}

/** Create a writable reactive value. */
export function signal<T>(initial: T): Signal<T> {
  return new SignalNode(initial);
}

/** Create a lazy, cached value whose dependencies are discovered automatically. */
export function computed<T>(fn: () => T): ReadonlySignal<T> {
  return new ComputedNode(fn);
}

/** Run a callback now and again whenever one of the values it reads changes. */
export function effect(fn: () => void): () => void {
  const observer = new EffectObserver(fn);
  observer.run();
  flushIfReady();
  return () => observer.dispose();
}

/** Coalesce subscriber callbacks and effect runs until the outermost batch ends. */
export function batch(fn: () => void): void {
  batchDepth += 1;
  try {
    fn();
  } finally {
    batchDepth -= 1;
    flushIfReady();
  }
}
