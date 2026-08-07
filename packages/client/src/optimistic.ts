export type Row = Record<string, unknown>;

type OptimisticRowPatch = {
  collection: string;
  rowId: string;
  op: "patch" | "insert";
  fields: Record<string, unknown>;
};

type OptimisticDeletePatch = {
  collection: string;
  rowId: string;
  op: "delete";
  fields?: never;
};

export type OptimisticPatch = OptimisticRowPatch | OptimisticDeletePatch;

type OverlayEntry = {
  mutationId: string;
  patches: OptimisticPatch[];
};

type CollectionCache = {
  base: readonly Row[];
  version: number;
  rows: Row[];
};

/**
 * An ordered, in-memory view of mutations that have not settled yet.
 *
 * Authoritative sync rows remain untouched. Consumers materialize this overlay
 * at read time, so rejecting an entry simply reveals the current server state.
 */
export class OptimisticOverlay {
  private readonly entries: OverlayEntry[] = [];
  private readonly listeners = new Set<(collection: string) => void>();
  private readonly cache = new Map<string, CollectionCache>();
  private version = 0;

  /** Append a mutation's patches after every currently pending mutation. */
  add(mutationId: string, patches: OptimisticPatch[]): void {
    if (patches.length === 0) return;
    this.entries.push({ mutationId, patches: patches.map(clonePatch) });
    this.changed(affectedCollections(patches));
  }

  /** Remove patches after a successful authoritative server result. */
  settle(mutationId: string): void {
    this.remove(mutationId);
  }

  /** Remove patches after a deterministic server rejection. */
  reject(mutationId: string): void {
    this.remove(mutationId);
  }

  /** Materialize one collection without mutating its authoritative rows. */
  apply(collection: string, rows: readonly Row[], keyField: string): Row[] {
    const cached = this.cache.get(collection);
    if (cached?.base === rows && cached.version === this.version) return cached.rows;

    let materialized = rows.slice();
    for (const entry of this.entries) {
      for (const patch of entry.patches) {
        if (patch.collection !== collection) continue;
        const index = materialized.findIndex((row) => {
          const key = row[keyField];
          return key !== null && key !== undefined && String(key) === patch.rowId;
        });
        if (patch.op === "patch") {
          if (index >= 0) {
            materialized[index] = { ...materialized[index], ...patch.fields };
          }
        } else if (patch.op === "insert") {
          if (index < 0) {
            materialized.push({ ...patch.fields, [keyField]: patch.rowId });
          }
        } else if (index >= 0) {
          materialized = [...materialized.slice(0, index), ...materialized.slice(index + 1)];
        }
      }
    }

    this.cache.set(collection, { base: rows, version: this.version, rows: materialized });
    return materialized;
  }

  /** Subscribe to collections whose materialized rows may have changed. */
  subscribe(listener: (collection: string) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** Whether any pending mutation touches this collection row. */
  pendingFor(collection: string, rowId: string): boolean {
    return this.entries.some((entry) => entry.patches.some((patch) => (
      patch.collection === collection && patch.rowId === rowId
    )));
  }

  private remove(mutationId: string) {
    const index = this.entries.findIndex((entry) => entry.mutationId === mutationId);
    if (index < 0) return;
    const [entry] = this.entries.splice(index, 1);
    this.changed(affectedCollections(entry!.patches));
  }

  private changed(collections: Set<string>) {
    this.version += 1;
    this.cache.clear();
    for (const collection of collections) {
      for (const listener of Array.from(this.listeners)) {
        try {
          listener(collection);
        } catch {
          // A view listener must not be able to break mutation delivery.
        }
      }
    }
  }
}

function affectedCollections(patches: readonly OptimisticPatch[]) {
  return new Set(patches.map((patch) => patch.collection));
}

function clonePatch(patch: OptimisticPatch): OptimisticPatch {
  if (patch.op === "delete") return { ...patch };
  return { ...patch, fields: { ...patch.fields } };
}
