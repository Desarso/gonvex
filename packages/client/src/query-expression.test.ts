import { describe, expect, it } from "vitest";
import { arg, field, runPortableQuery } from "./query-expression";

describe("portable query expression", () => {
  const rows = [
    { id: "1", title: "Broken Freezer", priority: "urgent", deadline: 3 },
    { id: "2", title: "Replace light", priority: "normal", deadline: 1 },
    { id: "3", title: "Freezer inspection", priority: "urgent", deadline: 2 },
  ];

  it("runs the same portable filter/sort/window against cached rows", () => {
    const result = runPortableQuery(rows, {
      entity: "tasks",
      where: { op: "and", expressions: [
        { op: "eq", left: field("priority"), right: arg("priority") },
        { op: "containsInsensitive", value: field("title"), search: arg("search") },
      ] },
      sort: [{ field: "deadline", direction: "asc" }],
      limit: 1,
    }, { priority: "urgent", search: "freezer" }, false);
    expect(result).toEqual({ rows: [rows[2]], completeness: "partial", supported: true });
  });

  it("refuses server-only operators instead of pretending partial data is exact", () => {
    expect(runPortableQuery(rows, {
      entity: "tasks",
      where: { op: "serverOnly", name: "postgres-tsquery" },
    }, {}, true)).toMatchObject({ supported: false, unsupportedOperator: "postgres-tsquery" });
  });
});
