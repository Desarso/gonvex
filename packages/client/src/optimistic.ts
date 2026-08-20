export type Row = Record<string, unknown>;

export type OptimisticProjection = {
  entity: string;
  key: string;
  resultPath: readonly string[];
};

export type OptimisticReducerDefinition = {
  entity: string;
  rowIdPath: readonly string[];
  fieldsPath: readonly string[];
};

type OptimisticRowPatch = {
  /** Canonical entity/table name. `collection` remains a compatibility alias. */
  entity?: string;
  collection?: string;
  rowId: string;
  op: "patch" | "insert";
  fields: Record<string, unknown>;
};

type OptimisticDeletePatch = {
  entity?: string;
  collection?: string;
  rowId: string;
  op: "delete";
  fields?: never;
};

export type OptimisticPatch = OptimisticRowPatch | OptimisticDeletePatch;

type OverlayEntry = {
  reducerId: string;
  patches: OptimisticPatch[];
  accepted: boolean;
  targets: Set<string>;
  acknowledgedTargets: Set<string>;
};

type CollectionCache = {
  base: readonly Row[];
  version: number;
  rows: Row[];
};

/**
 * Ordered pending entity reducers layered over immutable authoritative data.
 *
 * A source is one concrete sync/query subscription (including its arguments).
 * Successful reducers remain pending until every source that exposed the
 * optimistic row reports that reducer id in an authoritative update. This
 * prevents a fast RPC acknowledgement from revealing an older subscription
 * snapshot while its invalidation is still in flight.
 */
export class OptimisticOverlay {
  private readonly entries: OverlayEntry[] = [];
  private readonly listeners = new Set<(entity: string) => void>();
  private readonly cache = new Map<string, CollectionCache>();
  private version = 0;
  private settling = false;

  add(reducerId: string, patches: OptimisticPatch[], options: { accepted?: boolean } = {}): void {
    const normalized = patches.map(clonePatch).filter((patch) => patchEntity(patch) !== "");
    if (normalized.length === 0) return;
    this.entries.push({
      reducerId,
      patches: normalized,
      accepted: options.accepted === true,
      targets: new Set(),
      acknowledgedTargets: new Set(),
    });
    this.changed(affectedEntities(normalized));
  }

  /** Reserve a live projection even when its first snapshot is still pending. */
  expectSource(source: string, entity: string): void {
    let added = false;
    for (const entry of this.entries) {
      if (!entry.patches.some((patch) => patchEntity(patch) === entity)) continue;
      if (entry.targets.has(source)) continue;
      entry.targets.add(source);
      added = true;
    }
    if (!added) return;
    this.version += 1;
    this.cache.clear();
  }

  /** Mark the transport result successful without dropping visible optimism. */
  accept(reducerId: string): string[] {
    const entry = this.entries.find((candidate) => candidate.reducerId === reducerId);
    if (!entry) return [];
    entry.accepted = true;
    return this.settleReadyEntries();
  }

  /** Compatibility alias. Prefer accept followed by authoritative acknowledge. */
  settle(reducerId: string): void {
    this.accept(reducerId);
  }

  /** Remove patches after a deterministic server rejection. */
  reject(reducerId: string): void {
    this.remove(reducerId);
  }

  /**
   * Materialize entity rows for a concrete source. The three-argument overload
   * preserves the original collection-path API by using the entity as source.
   */
  apply(entity: string, rows: readonly Row[], keyField: string): Row[];
  apply(source: string, entity: string, rows: readonly Row[], keyField: string): Row[];
  apply(
    sourceOrEntity: string,
    entityOrRows: string | readonly Row[],
    rowsOrKey: readonly Row[] | string,
    maybeKey?: string,
  ): Row[] {
    const legacy = Array.isArray(entityOrRows);
    const source = legacy ? sourceOrEntity : sourceOrEntity;
    const entity = legacy ? sourceOrEntity : entityOrRows as string;
    const rows = (legacy ? entityOrRows : rowsOrKey) as readonly Row[];
    const keyField = (legacy ? rowsOrKey : maybeKey) as string;
    const cacheKey = `${source}\u0000${entity}`;
    const cached = this.cache.get(cacheKey);
    if (cached?.base === rows && cached.version === this.version) return cached.rows;

    let materialized = rows.slice();
    for (const entry of this.entries) {
      let touched = false;
      for (const patch of entry.patches) {
        if (patchEntity(patch) !== entity) continue;
        const index = materialized.findIndex((row) => {
          const key = row[keyField];
          return key !== null && key !== undefined && String(key) === patch.rowId;
        });
        if (patch.op === "patch") {
          if (index >= 0) {
            materialized[index] = { ...materialized[index], ...patch.fields };
            touched = true;
          }
        } else if (patch.op === "insert") {
          if (index < 0) materialized.push({ ...patch.fields, [keyField]: patch.rowId });
          touched = true;
        } else if (index >= 0) {
          materialized = [...materialized.slice(0, index), ...materialized.slice(index + 1)];
          touched = true;
        }
      }
      if (touched) {
        entry.targets.add(source);
      } else if (entry.targets.has(source)) {
        // The source cannot expose this row, so it is already reconciled from
        // that source's perspective.
        entry.acknowledgedTargets.add(source);
      }
    }

    this.cache.set(cacheKey, { base: rows, version: this.version, rows: materialized });
    return materialized;
  }

  /** Record authoritative delivery for a source; returns fully reconciled ids. */
  acknowledge(source: string, originCommandIds: readonly string[] | undefined): string[] {
    if (!originCommandIds?.length) return [];
    const ids = new Set(originCommandIds);
    for (const entry of this.entries) {
      if (ids.has(entry.reducerId) && entry.targets.has(source)) {
        entry.acknowledgedTargets.add(source);
      }
    }
    return this.settleReadyEntries();
  }

  /** Reconcile restored committed entries against an authoritative snapshot. */
  acknowledgeMatching(source: string, entity: string, rows: readonly Row[], keyField: string): string[] {
    for (const entry of this.entries) {
      if (!entry.accepted || !entry.targets.has(source)) continue;
      const patches = entry.patches.filter((patch) => {
        if (patchEntity(patch) !== entity) return false;
        if (patch.op === "delete") return patchMatchesRows(patch, rows, keyField);
        return rows.some((row) => {
          const key = row[keyField];
          return key !== null && key !== undefined && String(key) === patch.rowId;
        });
      });
      if (patches.length === 0 || !patches.every((patch) => patchMatchesRows(patch, rows, keyField))) continue;
      entry.acknowledgedTargets.add(source);
    }
    return this.settleReadyEntries();
  }

  /** Stop waiting for a subscription that no longer exists. */
  removeSource(source: string): string[] {
    for (const entry of this.entries) {
      entry.targets.delete(source);
      entry.acknowledgedTargets.delete(source);
    }
    for (const key of this.cache.keys()) {
      if (key.startsWith(`${source}\u0000`)) this.cache.delete(key);
    }
    return this.settleReadyEntries();
  }

  subscribe(listener: (entity: string) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  pendingFor(entity: string, rowId: string): boolean {
    return this.entries.some((entry) => entry.patches.some((patch) => (
      patchEntity(patch) === entity && patch.rowId === rowId
    )));
  }

  private settleReadyEntries(): string[] {
    // Removing an entry emits a new materialized snapshot. Query/sync
    // listeners may synchronously acknowledge that snapshot and re-enter this
    // method, so only the outer settlement pass is allowed to splice entries.
    // The nested acknowledgement still updates acknowledgedTargets; the outer
    // reverse loop observes that state when it reaches the remaining entry.
    if (this.settling) return [];
    this.settling = true;
    const settled: string[] = [];
    try {
      for (let index = this.entries.length - 1; index >= 0; index -= 1) {
        const entry = this.entries[index];
        if (!entry?.accepted) continue;
        if ([...entry.targets].some((target) => !entry.acknowledgedTargets.has(target))) continue;
        this.entries.splice(index, 1);
        settled.push(entry.reducerId);
        this.changed(affectedEntities(entry.patches));
      }
      return settled.reverse();
    } finally {
      this.settling = false;
    }
  }

  private remove(reducerId: string) {
    const index = this.entries.findIndex((entry) => entry.reducerId === reducerId);
    if (index < 0) return;
    const [entry] = this.entries.splice(index, 1);
    this.changed(affectedEntities(entry!.patches));
  }

  private changed(entities: Set<string>) {
    this.version += 1;
    this.cache.clear();
    for (const entity of entities) {
      for (const listener of Array.from(this.listeners)) {
        try {
          listener(entity);
        } catch {
          // A view listener must not be able to break reducer delivery.
        }
      }
    }
  }
}

export function optimisticPatchesFromReference(
  definition: OptimisticReducerDefinition | undefined,
  args: unknown,
): OptimisticPatch[] {
  if (!definition?.entity) return [];
  const record = isRecord(args) ? args : {};
  const configuredRowId = definition.rowIdPath.length > 0
    ? readPath(record, definition.rowIdPath)
    : undefined;
  const rowId = String(configuredRowId ?? record.id ?? record._id ?? "");
  const nested = readPath(record, definition.fieldsPath);
  if (!rowId || !isRecord(nested)) return [];
  return [{
    entity: definition.entity,
    rowId,
    op: "patch",
    fields: normalizeOptimisticFields(nested),
  }];
}

function normalizeOptimisticFields(fields: Record<string, unknown>) {
  const normalized: Record<string, unknown> = { ...fields };
  for (const [key, value] of Object.entries(fields)) {
    const camel = key.replace(/_([a-z0-9])/g, (_, character: string) => character.toUpperCase());
    if (!(camel in normalized)) normalized[camel] = value;
  }
  return normalized;
}

function readPath(value: unknown, path: readonly string[]): unknown {
  let current = value;
  for (const segment of path) {
    if (!isRecord(current)) return undefined;
    current = current[segment];
  }
  return current;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function patchEntity(patch: OptimisticPatch) {
  return String(patch.entity ?? patch.collection ?? "");
}

function affectedEntities(patches: readonly OptimisticPatch[]) {
  return new Set(patches.map(patchEntity).filter(Boolean));
}

function clonePatch(patch: OptimisticPatch): OptimisticPatch {
  if (patch.op === "delete") return { ...patch };
  return { ...patch, fields: { ...patch.fields } };
}

function patchMatchesRows(patch: OptimisticPatch, rows: readonly Row[], keyField: string) {
  const row = rows.find((candidate) => {
    const key = candidate[keyField];
    return key !== null && key !== undefined && String(key) === patch.rowId;
  });
  if (patch.op === "delete") return row === undefined;
  if (!row) return false;
  return Object.entries(patch.fields).every(([key, value]) => valuesEqual(row[key], value));
}

function valuesEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    return Array.isArray(left)
      && Array.isArray(right)
      && left.length === right.length
      && left.every((value, index) => valuesEqual(value, right[index]));
  }
  if (!isRecord(left) || !isRecord(right)) return false;
  const leftKeys = Object.keys(left).sort();
  const rightKeys = Object.keys(right).sort();
  return leftKeys.length === rightKeys.length
    && leftKeys.every((key, index) => key === rightKeys[index] && valuesEqual(left[key], right[key]));
}
