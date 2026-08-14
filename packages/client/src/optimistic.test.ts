import { describe, expect, it, vi } from "vitest";
import {
  OptimisticOverlay,
  optimisticPatchesFromReference,
  type Row,
} from "./optimistic";

describe("OptimisticOverlay", () => {
  it("folds patch, insert, and delete operations in entry order", () => {
    const overlay = new OptimisticOverlay();
    const base: Row[] = [
      { id: "a", title: "Original", count: 1 },
      { id: "b", title: "Remove me" },
    ];

    overlay.add("first", [
      { collection: "tasks.list", rowId: "a", op: "patch", fields: { title: "First" } },
      { collection: "tasks.list", rowId: "c", op: "insert", fields: { title: "Created" } },
    ]);
    overlay.add("second", [
      { collection: "tasks.list", rowId: "a", op: "patch", fields: { title: "Second", done: true } },
      { collection: "tasks.list", rowId: "b", op: "delete" },
      { collection: "tasks.list", rowId: "c", op: "patch", fields: { title: "Updated insert" } },
    ]);

    expect(overlay.apply("tasks.list", base, "id")).toEqual([
      { id: "a", title: "Second", count: 1, done: true },
      { id: "c", title: "Updated insert" },
    ]);
    expect(base).toEqual([
      { id: "a", title: "Original", count: 1 },
      { id: "b", title: "Remove me" },
    ]);
  });

  it("ignores missing patches and inserts whose id already exists", () => {
    const overlay = new OptimisticOverlay();
    overlay.add("mutation", [
      { collection: "tasks.list", rowId: "missing", op: "patch", fields: { title: "No row" } },
      { collection: "tasks.list", rowId: "a", op: "insert", fields: { title: "Duplicate" } },
    ]);

    expect(overlay.apply("tasks.list", [{ id: "a", title: "Base" }], "id")).toEqual([
      { id: "a", title: "Base" },
    ]);
  });

  it("memoizes by collection, base reference, and overlay version", () => {
    const overlay = new OptimisticOverlay();
    const base = [{ id: "a", title: "Base" }];

    const first = overlay.apply("tasks.list", base, "id");
    expect(overlay.apply("tasks.list", base, "id")).toBe(first);

    overlay.add("other", [
      { collection: "teams.list", rowId: "team-a", op: "insert", fields: { name: "Team" } },
    ]);
    const afterVersion = overlay.apply("tasks.list", base, "id");
    expect(afterVersion).not.toBe(first);
    expect(overlay.apply("tasks.list", base, "id")).toBe(afterVersion);

    expect(overlay.apply("tasks.list", [...base], "id")).not.toBe(afterVersion);
  });

  it("notifies each affected collection on add and settle", () => {
    const overlay = new OptimisticOverlay();
    const listener = vi.fn();
    const unsubscribe = overlay.subscribe(listener);
    overlay.add("mutation", [
      { collection: "tasks.list", rowId: "a", op: "delete" },
      { collection: "teams.list", rowId: "b", op: "delete" },
      { collection: "tasks.list", rowId: "c", op: "delete" },
    ]);
    overlay.settle("mutation");

    expect(listener.mock.calls.map(([collection]) => collection)).toEqual([
      "tasks.list", "teams.list", "tasks.list", "teams.list",
    ]);
    unsubscribe();
    overlay.add("later", [{ collection: "tasks.list", rowId: "d", op: "delete" }]);
    expect(listener).toHaveBeenCalledTimes(4);
  });

  it("rejects entries and reports pending rows", () => {
    const overlay = new OptimisticOverlay();
    const listener = vi.fn();
    overlay.subscribe(listener);
    overlay.add("mutation", [
      { collection: "tasks.list", rowId: "a", op: "patch", fields: { done: true } },
    ]);

    expect(overlay.pendingFor("tasks.list", "a")).toBe(true);
    expect(overlay.pendingFor("tasks.list", "b")).toBe(false);
    overlay.reject("mutation");
    expect(overlay.pendingFor("tasks.list", "a")).toBe(false);
    expect(listener).toHaveBeenCalledTimes(2);
  });

  it("uses id fallbacks when generated mutation metadata omits a row-id path", () => {
    expect(optimisticPatchesFromReference({
      entity: "tasks",
      rowIdPath: [],
      fieldsPath: ["updates"],
    }, {
      id: "task-a",
      updates: { priorityId: "priority-high" },
    })).toEqual([{
      entity: "tasks",
      rowId: "task-a",
      op: "patch",
      fields: { priorityId: "priority-high" },
    }]);
  });

  it("reconciles a restored multi-row mutation against only rows a source exposes", () => {
    const overlay = new OptimisticOverlay();
    overlay.add("mutation", [
      { entity: "tasks", rowId: "a", op: "patch", fields: { done: true } },
      { entity: "tasks", rowId: "b", op: "patch", fields: { done: true } },
    ], { accepted: true });
    overlay.apply("workspace-a", "tasks", [{ id: "a", done: true }], "id");

    expect(overlay.acknowledgeMatching(
      "workspace-a",
      "tasks",
      [{ id: "a", done: true }],
      "id",
    )).toEqual(["mutation"]);
    expect(overlay.pendingFor("tasks", "a")).toBe(false);
  });

  it("settles multiple accepted mutations safely when snapshot emission re-enters reconciliation", () => {
    const overlay = new OptimisticOverlay();
    const authoritative = [{ id: "a", done: true }];
    overlay.add("first", [
      { entity: "tasks", rowId: "a", op: "patch", fields: { done: true } },
    ]);
    overlay.add("second", [
      { entity: "tasks", rowId: "a", op: "patch", fields: { done: true } },
    ]);
    overlay.apply("workspace-a", "tasks", [{ id: "a", title: "Base" }], "id");
    overlay.accept("first");
    overlay.accept("second");
    overlay.subscribe(() => {
      overlay.acknowledgeMatching("workspace-a", "tasks", authoritative, "id");
    });

    expect(overlay.acknowledgeMatching(
      "workspace-a",
      "tasks",
      authoritative,
      "id",
    )).toEqual(["first", "second"]);
    expect(overlay.pendingFor("tasks", "a")).toBe(false);
  });
});
