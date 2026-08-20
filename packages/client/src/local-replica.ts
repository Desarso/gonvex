import type { JsonValue, ReplicaCursor } from "@gonvex/protocol";
import type { OptimisticPatch } from "./optimistic.js";

export type ReplicaRow = Record<string, JsonValue>;

export type ReplicaChange = {
  entity: string;
  id: string;
  operation: "insert" | "update" | "delete";
  oldValue?: ReplicaRow;
  newValue?: ReplicaRow;
  changedColumns?: string[];
};

export type LiveQueryMembership = {
  signature: string;
  entity: string;
  ids: string[];
  completeness: "complete" | "partial";
  source: "server" | "cache";
};

export type ReplicaTransaction = {
  cursor: ReplicaCursor;
  originCommandId?: string;
  changes: ReplicaChange[];
  memberships?: LiveQueryMembership[];
};

export type ReplicaSnapshot = {
  cursor?: ReplicaCursor;
  entities: Record<string, Record<string, ReplicaRow>>;
  liveQueries: Record<string, LiveQueryMembership>;
};

/** Opaque persistence namespace for one deployment/project/tenant identity. */
export type ReplicaScope = string;

const defaultReplicaScope: ReplicaScope = "default";

/**
 * Storage implementations must persist the complete transaction atomically.
 * The SQLite adapter maps this call to BEGIN/apply/cursor/COMMIT; IndexedDB
 * implementations use one readwrite transaction over the same stores.
 */
export interface LocalReplicaStorage {
  load(scope?: ReplicaScope): Promise<ReplicaSnapshot | undefined>;
  applyTransaction(transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
  /** Persist a normalized Query/Collection materialization atomically. */
  replaceSnapshot?(snapshot: ReplicaSnapshot, scope?: ReplicaScope): Promise<void>;
}

type PendingCommand = {
  commandId: string;
  patches: OptimisticPatch[];
  committedRevision?: number;
};

export type ReplicaFreshness = "current" | "verifying" | "offline";

export type LiveQueryResult<T extends ReplicaRow = ReplicaRow> = {
  rows: T[];
  source: "server" | "cache";
  completeness: "complete" | "partial";
  freshness: ReplicaFreshness;
};

export class LocalReplica {
  private cursorValue?: ReplicaCursor;
  private entities = new Map<string, Map<string, ReplicaRow>>();
  private liveQueries = new Map<string, LiveQueryMembership>();
  private pendingCommands = new Map<string, PendingCommand>();
  private listeners = new Set<() => void>();
  private persistence = Promise.resolve();
  private application = Promise.resolve();
  private hydration?: Promise<void>;
  private freshnessValue: ReplicaFreshness = "verifying";
  private versionValue = 0;
  private scopeValue: ReplicaScope = defaultReplicaScope;
  private scopeLoaded = false;
  private scopeActivation?: Promise<void>;
  private scopeActivationName?: ReplicaScope;
  private scopeActivationGeneration = 0;

  constructor(private readonly storage?: LocalReplicaStorage) {
    // A memory-only direct user has no asynchronous scope to hydrate and can
    // continue using the historical immediate APIs under the safe default.
    if (!storage) this.scopeLoaded = true;
  }

  hydrate(): Promise<void> {
    if (this.hydration) return this.hydration;
    this.hydration = this.activateScope(defaultReplicaScope);
    return this.hydration;
  }

  /**
   * Atomically switch the authoritative store to an opaque persistence scope.
   * A newer request supersedes an older one even if the older storage read is
   * slow (or resolves after auth has already changed again).
   */
  activateScope(scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    const nextScope = normalizeReplicaScope(scope);
    if (this.scopeLoaded && this.scopeValue === nextScope) return this.application;
    if (this.scopeActivation && this.scopeActivationName === nextScope) return this.scopeActivation;
    // Fail closed immediately while the async read is in flight. This keeps a
    // synchronous auth change from exposing the prior identity's rows.
    const wasLoaded = this.scopeLoaded;
    this.scopeLoaded = false;
    if (wasLoaded) this.notify();
    const generation = ++this.scopeActivationGeneration;
    const activation = this.application.then(async () => {
      // A prior transaction may still be in the storage queue. Wait for it so
      // a same-scope activation cannot load a snapshot from before its commit.
      await this.persistence;
      const snapshot = await this.storage?.load(nextScope);
      if (generation !== this.scopeActivationGeneration) return;
      this.scopeValue = nextScope;
      this.scopeLoaded = true;
      this.cursorValue = snapshot?.cursor;
      this.entities = snapshot ? entitiesFromSnapshot(snapshot.entities) : new Map();
      this.liveQueries = snapshot
        ? new Map(Object.entries(snapshot.liveQueries).map(([key, value]) => [key, cloneMembership(value)]))
        : new Map();
      // Optimistic commands belong to the old identity and must never be
      // projected while the newly restored scope is becoming authoritative.
      this.pendingCommands.clear();
      this.freshnessValue = "verifying";
      this.notify();
    });
    this.application = activation.catch(() => undefined);
    this.scopeActivation = activation;
    this.scopeActivationName = nextScope;
    return activation;
  }

  cursor() {
    return this.scopeLoaded && this.cursorValue ? { ...this.cursorValue } : undefined;
  }

  freshness() {
    return this.freshnessValue;
  }

  version() { return this.versionValue; }

  setFreshness(freshness: ReplicaFreshness) {
    if (freshness === this.freshnessValue) return;
    this.freshnessValue = freshness;
    this.notify();
  }

  subscribe(listener: () => void) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  applyOptimistic(commandId: string, patches: OptimisticPatch[]) {
    commandId = commandId.trim();
    if (!commandId) throw new Error("optimistic commandId is required");
    this.pendingCommands.set(commandId, { commandId, patches: patches.map(cloneOptimisticPatch) });
    this.notify();
  }

  acknowledgeCommand(commandId: string, committedRevision?: number) {
    const pending = this.pendingCommands.get(commandId);
    if (!pending) return;
    if (!committedRevision) {
      this.pendingCommands.delete(commandId);
      this.notify();
      return;
    }
    pending.committedRevision = committedRevision;
    this.reconcileCommands();
  }

  rejectCommand(commandId: string) {
    if (!this.pendingCommands.delete(commandId)) return;
    this.notify();
  }

  applyTransaction(transaction: ReplicaTransaction, scope?: ReplicaScope): Promise<void> {
    const requestedScope = scope === undefined ? undefined : normalizeReplicaScope(scope);
    const application = this.application.then(() => this.applyTransactionNow(transaction, requestedScope));
    this.application = application.catch(() => undefined);
    return application;
  }

  materializeWindow(input: {
    signature: string;
    entity: string;
    key: string;
    rows: ReplicaRow[];
    completeness: "complete" | "partial";
    source: "server" | "cache";
    cursor?: ReplicaCursor;
    scope?: ReplicaScope;
  }): Promise<void> {
    const application = this.application.then(() => this.materializeWindowNow(input));
    this.application = application.catch(() => undefined);
    return application;
  }

  private async materializeWindowNow(input: {
    signature: string;
    entity: string;
    key: string;
    rows: ReplicaRow[];
    completeness: "complete" | "partial";
    source: "server" | "cache";
    cursor?: ReplicaCursor;
    scope?: ReplicaScope;
  }) {
    if (input.scope !== undefined && normalizeReplicaScope(input.scope) !== this.scopeValue) return;
    if (!input.signature.trim() || !input.entity.trim() || !input.key.trim()) {
      throw new Error("replica materialization requires signature, entity, and key");
    }
    const nextEntities = cloneEntities(this.entities);
    const nextQueries = new Map(this.liveQueries);
    if (input.cursor && this.cursorValue && input.cursor.epoch !== this.cursorValue.epoch) {
      nextEntities.clear();
      nextQueries.clear();
    }
    const entityRows = nextEntities.get(input.entity) ?? new Map<string, ReplicaRow>();
    nextEntities.set(input.entity, entityRows);
    const ids: string[] = [];
    for (const row of input.rows) {
      const rawID = row[input.key];
      const id = typeof rawID === "string" || typeof rawID === "number" ? String(rawID) : "";
      if (!id) continue;
      ids.push(id);
      entityRows.set(id, cloneRow(row));
    }
    nextQueries.set(input.signature, {
      signature: input.signature,
      entity: input.entity,
      ids,
      completeness: input.completeness,
      source: input.source,
    });
    let nextCursor = this.cursorValue;
    if (input.cursor && (!nextCursor || input.cursor.epoch !== nextCursor.epoch || input.cursor.revision > nextCursor.revision)) {
      nextCursor = { ...input.cursor };
    }
    const snapshot = snapshotFrom(nextCursor, nextEntities, nextQueries);
    const writeScope = this.scopeValue;
    if (this.storage?.replaceSnapshot) {
      await this.persist(() => this.storage!.replaceSnapshot!(snapshot, writeScope));
    }
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    this.cursorValue = nextCursor;
    if (input.source === "server") this.freshnessValue = "current";
    this.notify();
  }

  private async applyTransactionNow(transaction: ReplicaTransaction, scope?: ReplicaScope) {
    if (scope !== undefined && scope !== this.scopeValue) return;
    validateTransaction(transaction);
    if (this.cursorValue?.epoch === transaction.cursor.epoch && transaction.cursor.revision <= this.cursorValue.revision) {
      return;
    }

    const nextEntities = cloneEntities(this.entities);
    const nextQueries = new Map(this.liveQueries);
    if (this.cursorValue && this.cursorValue.epoch !== transaction.cursor.epoch) {
      nextEntities.clear();
      nextQueries.clear();
    }
    for (const change of transaction.changes) {
      const rows = nextEntities.get(change.entity) ?? new Map<string, ReplicaRow>();
      nextEntities.set(change.entity, rows);
      if (change.operation === "delete") rows.delete(change.id);
      else if (change.newValue) rows.set(change.id, cloneRow(change.newValue));
    }
    for (const membership of transaction.memberships ?? []) {
      nextQueries.set(membership.signature, cloneMembership(membership));
    }

    const snapshot = snapshotFrom(transaction.cursor, nextEntities, nextQueries);
    const writeScope = this.scopeValue;
    await this.persist(() => this.storage?.applyTransaction(transaction, snapshot, writeScope));

    // Publish the whole committed transaction in one state swap and notify UI
    // exactly once. No subscriber can observe a partial entity/query update.
    this.entities = nextEntities;
    this.liveQueries = nextQueries;
    this.cursorValue = { ...transaction.cursor };
    this.freshnessValue = "current";
    if (transaction.originCommandId) this.pendingCommands.delete(transaction.originCommandId);
    this.reconcileCommands(false);
    this.notify();
  }

  entity<T extends ReplicaRow = ReplicaRow>(entity: string, id: string): T | undefined {
    if (!this.scopeLoaded) return undefined;
    let row = this.entities.get(entity)?.get(id);
    let selected = row ? cloneRow(row) : undefined;
    for (const command of this.pendingCommands.values()) {
      for (const patch of command.patches) {
        if ((patch.entity ?? patch.collection) !== entity || patch.rowId !== id) continue;
        if (patch.op === "delete") selected = undefined;
        if (patch.op === "insert") selected = cloneRow(patch.fields as ReplicaRow);
        if (patch.op === "upsert") selected = cloneRow(patch.fields as ReplicaRow);
        if (patch.op === "patch") selected = { ...(selected ?? {}), ...(patch.fields as ReplicaRow) };
      }
    }
    return selected as T | undefined;
  }

  liveQuery<T extends ReplicaRow = ReplicaRow>(signature: string): LiveQueryResult<T> {
    if (!this.scopeLoaded) {
      return { rows: [], source: "cache", completeness: "partial", freshness: this.freshnessValue };
    }
    const membership = this.liveQueries.get(signature);
    if (!membership) {
      return { rows: [], source: "cache", completeness: "partial", freshness: this.freshnessValue };
    }
    const rows = membership.ids
      .map((id) => this.entity<T>(membership.entity, id))
      .filter((row): row is T => row !== undefined);
    return {
      rows,
      source: this.freshnessValue === "current" ? membership.source : "cache",
      completeness: membership.completeness,
      freshness: this.freshnessValue,
    };
  }

  hasLiveQuery(signature: string) {
    return this.scopeLoaded && this.liveQueries.has(signature);
  }

  snapshot(): ReplicaSnapshot {
    if (!this.scopeLoaded) return { entities: {}, liveQueries: {} };
    return snapshotFrom(this.cursorValue, this.entities, this.liveQueries);
  }

  private reconcileCommands(notify = true) {
    const revision = this.cursorValue?.revision ?? 0;
    let changed = false;
    for (const [commandId, command] of this.pendingCommands) {
      if (command.committedRevision && revision >= command.committedRevision) {
        this.pendingCommands.delete(commandId);
        changed = true;
      }
    }
    if (changed && notify) this.notify();
  }

  private persist(operation: () => Promise<void> | undefined) {
    const attempt = this.persistence.then(async () => { await operation(); });
    this.persistence = attempt.catch(() => undefined);
    return attempt;
  }

  private notify() {
    this.versionValue += 1;
    for (const listener of [...this.listeners]) listener();
  }
}

export class MemoryLocalReplicaStorage implements LocalReplicaStorage {
  private readonly values = new Map<ReplicaScope, ReplicaSnapshot>();
  async load(scope: ReplicaScope = defaultReplicaScope) {
    const value = this.values.get(normalizeReplicaScope(scope));
    return value ? cloneSnapshot(value) : undefined;
  }
  async applyTransaction(_transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
  async replaceSnapshot(snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope) {
    this.values.set(normalizeReplicaScope(scope), cloneSnapshot(snapshot));
  }
}

function validateTransaction(transaction: ReplicaTransaction) {
  if (!transaction.cursor.epoch.trim() || transaction.cursor.revision <= 0) {
    throw new Error("replica transaction requires a positive revision and epoch");
  }
  for (const change of transaction.changes) {
    if (!change.entity.trim() || !change.id.trim()) throw new Error("replica change requires entity and id");
    if (change.operation !== "delete" && !change.newValue) throw new Error("replica upsert requires newValue");
  }
}

function cloneOptimisticPatch(patch: OptimisticPatch): OptimisticPatch {
  if (patch.op === "delete") return { ...patch };
  if (patch.op === "insert") return { ...patch, fields: structuredClone(patch.fields) };
  return { ...patch, fields: structuredClone(patch.fields) };
}

function cloneRow(row: ReplicaRow) { return structuredClone(row); }
function cloneMembership(value: LiveQueryMembership): LiveQueryMembership { return { ...value, ids: [...value.ids] }; }
function cloneEntities(source: Map<string, Map<string, ReplicaRow>>) {
  return new Map([...source].map(([entity, rows]) => [entity, new Map([...rows].map(([id, row]) => [id, cloneRow(row)]))]));
}
function entitiesFromSnapshot(source: ReplicaSnapshot["entities"]) {
  return new Map(Object.entries(source).map(([entity, rows]) => [entity, new Map(Object.entries(rows).map(([id, row]) => [id, cloneRow(row)]))]));
}
function snapshotFrom(cursor: ReplicaCursor | undefined, entities: Map<string, Map<string, ReplicaRow>>, liveQueries: Map<string, LiveQueryMembership>): ReplicaSnapshot {
  return {
    cursor: cursor ? { ...cursor } : undefined,
    entities: Object.fromEntries([...entities].map(([entity, rows]) => [entity, Object.fromEntries([...rows].map(([id, row]) => [id, cloneRow(row)]))])),
    liveQueries: Object.fromEntries([...liveQueries].map(([key, value]) => [key, cloneMembership(value)])),
  };
}
function cloneSnapshot(value: ReplicaSnapshot) {
  return snapshotFrom(value.cursor, entitiesFromSnapshot(value.entities), new Map(Object.entries(value.liveQueries)));
}

function normalizeReplicaScope(scope: ReplicaScope): ReplicaScope {
  if (typeof scope !== "string" || !scope.trim()) return defaultReplicaScope;
  return scope;
}
