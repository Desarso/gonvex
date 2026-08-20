import { describe, expect, it } from "vitest";

import { optimisticPatchesFromReference } from "./optimistic.js";

describe("optimistic transaction argument references", () => {
  it("resolves dynamic fields and nested values from reducer arguments", () => {
    expect(optimisticPatchesFromReference({
      effects: [{
        operation: "patch",
        entity: "tasks",
        id: ["taskId"],
        fields: {
          title: { $arg: "updates.title" },
          statusId: { $arg: ["statusId"] },
          audit: { actorMemberId: { $arg: "actorMemberId" } },
          fixed: true,
        },
      }],
    }, {
      taskId: "task-1",
      statusId: "in-progress",
      actorMemberId: "member-7",
      updates: { title: "Instant title" },
    })).toEqual([{
      entity: "tasks",
      rowId: "task-1",
      op: "patch",
      fields: {
        title: "Instant title",
        statusId: "in-progress",
        audit: { actorMemberId: "member-7" },
        fixed: true,
      },
    }]);
  });

  it("fails closed when an optimistic argument reference is missing", () => {
    expect(optimisticPatchesFromReference({
      effects: [{
        operation: "patch",
        entity: "tasks",
        id: ["taskId"],
        fields: { title: { $arg: "updates.title" } },
      }],
    }, { taskId: "task-1" })).toEqual([]);
  });
});
