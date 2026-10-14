import type { GonvexClient, CallOptions } from "../index.js";

export type AdmissionLane = <T>(run: () => Promise<T>) => Promise<T>;
const lanes = new WeakMap<CallOptions, AdmissionLane>();
export function engineAdmissionLane(options: CallOptions) { return lanes.get(options); }

/** A received burst owns the existing delivery, local and intent lanes. */
export class EngineAdmissionCohort {
  private readonly entries: Array<{ options: CallOptions & { engineFresh?: boolean }; begin(): Promise<{ operation: Promise<unknown> }>; resolve(value: { operation: Promise<unknown> }): void; reject(error: unknown): void }> = [];
  constructor(private readonly client: GonvexClient) {}
  enqueue(options: CallOptions & { engineFresh?: boolean }, begin: () => Promise<{ operation: Promise<unknown> }>) {
    return new Promise<{ operation: Promise<unknown> }>((resolve, reject) => this.entries.push({ options, begin, resolve, reject }));
  }
  flush() {
    if (!this.entries.length) return;
    const client = this.client, scope = client["outboxScope"], store = client["sharedOutboxStore"]!;
    let lane: Promise<unknown> = Promise.resolve();
    const admissionLane: AdmissionLane = run => {
      const operation = lane.then(run); lane = operation.catch(() => undefined); return operation;
    };
    const execute = async () => {
      for (const entry of this.entries) lanes.set(entry.options, admissionLane);
      try {
        let starts: Promise<unknown> = Promise.resolve(), pendingStarts = 0;
        await Promise.allSettled(this.entries.map(async entry => {
          try {
            const ordered = !entry.options.engineFresh || pendingStarts > 0;
            const starting = ordered ? starts.then(entry.begin) : entry.begin();
            if (ordered) {
              pendingStarts++;
              starts = starting.then(() => { pendingStarts--; }, () => { pendingStarts--; });
            }
            const started = await starting;
            // Each caller resolves at its own durable acknowledgement. The
            // shared lease lasts until every admitted operation has settled.
            entry.resolve(started); await started.operation;
          } catch (error) { entry.reject(error); }
        }));
      } finally { for (const entry of this.entries) lanes.delete(entry.options); }
    };
    void Promise.all([client["outboxReady"], client["replicaReady"]]).then(() =>
      store.coordinate!(scope, "delivery", () => client["inLocalLane"](() => store.coordinate!(scope, "intent", execute))),
    ).catch(error => { for (const entry of this.entries) entry.reject(error); });
  }
}
