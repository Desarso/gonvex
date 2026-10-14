import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import type { EngineClient, EngineEntitiesState } from "@gonvex/client/engine";
import { GonvexProvider, useEntityState, useLiveQueryState, useReplicaCollection } from "./index.js";

afterEach(() => { cleanup(); vi.useRealTimers(); });
it("worker hooks expose pending, publish results, and retain unchanged row identity", () => {
  vi.useFakeTimers();
  const listeners = new Set<() => void>();
  let entities: EngineEntitiesState = { status: "pending" };
  let rows: any[] | undefined;
  const ref = { kind: "query" as const, path: "tasks", delivery: "replica" as const, replica: { table: "tasks", key: "id", columns: ["id", "title"] } };
  const subscribe = (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; };
  const client = { engineMode: "worker", watchEntities: () => ({ onUpdate: subscribe, getSnapshot: () => entities }),
    watchReplica: () => ({ onUpdate: subscribe, localReplicaResult: () => rows }) } as unknown as EngineClient;
  const identities: any[] = [];
  function App() {
    const entity = useEntityState("tasks", "1"); const collection = useReplicaCollection(ref);
    if (entity.status === "ready") identities.push(entity.row);
    return <div>{entity.status}:{collection?.length ?? "loading"}</div>;
  }
  render(<GonvexProvider client={client}><App /></GonvexProvider>);
  expect(screen.getByText("pending:loading")).toBeTruthy();
  const row = Object.freeze({ id: "1", title: "same" });
  act(() => { entities = { status: "ready", rows: [row] }; rows = [row]; listeners.forEach(listener => listener()); vi.advanceTimersToNextFrame(); });
  expect(screen.getByText("ready:1")).toBeTruthy();
  act(() => { rows = [row, { id: "2", title: "new" }]; listeners.forEach(listener => listener()); vi.advanceTimersToNextFrame(); });
  expect(screen.getByText("ready:2")).toBeTruthy(); expect(identities.every(value => value === row)).toBe(true);
  act(() => { entities = { status: "pending" }; rows = undefined; listeners.forEach(listener => listener()); vi.advanceTimersToNextFrame(); });
  expect(screen.getByText("pending:loading")).toBeTruthy();
});

it("keeps skipped engine Live Queries empty without a pending subscription", () => {
  const watchRetained = vi.fn();
  const client = { engineMode: "worker", watchRetained } as unknown as EngineClient;
  let snapshot: ReturnType<typeof useLiveQueryState>;
  function App() {
    snapshot = useLiveQueryState({ kind: "query", path: "tasks.live" }, "skip");
    return <div>{snapshot.pending ? "pending" : "skipped"}</div>;
  }
  render(<GonvexProvider client={client}><App /></GonvexProvider>);
  expect(screen.getByText("skipped")).toBeTruthy();
  expect(snapshot!).toMatchObject({ rows: [], ids: [] });
  expect(watchRetained).not.toHaveBeenCalled();
});
