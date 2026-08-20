# `@gonvex/module-sdk`

The language-neutral Gonvex module contract for Query, Reducer, and Action
definitions.

This package is deliberately a declaration layer. It does not open sockets,
load Postgres drivers, read environment variables, or execute handlers. A
Gonvex host supplies those capabilities through its module engine. The
portable `ModuleManifest` produced here is suitable for `module.json`, client
code generation, deployment validation, and non-TypeScript module adapters.

```ts
import { createModule, schema } from "@gonvex/module-sdk";

const app = createModule({ name: "whagons", version: "1" });

app.reducer("tasks.start", {
  args: schema.object({ taskId: schema.id("tasks") }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [
      { operation: "patch", entity: "tasks", id: ["taskId"], fields: { status: "in_progress" } },
    ],
  },
});

export default app;
```

For TypeScript modules, the top-level helpers are executable declarations. The
V8 runtime executes exported bindings (and calls their `handler`), so a module
can export a reducer directly:

```ts
import { reducer, schema } from "@gonvex/module-sdk";

export const startTask = reducer({
  args: schema.object({ taskId: schema.id("tasks") }),
  offline: { mode: "allowed", conflict: "expectedVersion" },
  optimistic: {
    effects: [
      { operation: "patch", entity: "tasks", id: ["taskId"], fields: { status: "in_progress" } },
    ],
  },
  run: async ({ db }, { taskId }) => db.update("tasks", taskId, { status: "in_progress" }),
});
```

The host-specific implementation is intentionally separate from this SDK.
The same manifest shape can describe a V8 TypeScript module or a Wasm module.

Executable handlers stay in a host-side registry. The registry dispatches only
when both the path and function kind match; it never creates database or
network capabilities itself:

```ts
const runtime = app.createRuntimeRegistry();
await runtime.dispatch({
  path: "tasks.start",
  kind: "reducer",
  context: reducerContext,
  args: { taskId: "task_123" },
});
```

`runtime.registrationPayload()` and `app.runtimePayload()` return deterministic,
handler-free payloads for a host loader. `QueryContext`, `ReducerContext`, and
`ActionContext` remain separate types, so capability boundaries are visible to
module authors and adapters.
