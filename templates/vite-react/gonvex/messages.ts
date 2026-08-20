import { liveQuery, reducer, schema, visibility } from "@gonvex/module-sdk";

export const messagesVisibility = visibility({
  table: "messages",
  key: "id",
  sets: {},
  where: { operator: "public" },
});

export const list = liveQuery({
  args: schema.object({}),
  result: schema.array(schema.object({
    id: schema.id("messages"),
    body: schema.string(),
    author: schema.string(),
    created_at: schema.datetime(),
  })),
  liveQueryPlan: {
    table: "messages",
    key: "id",
    columns: ["id", "body", "author", "created_at"],
    sort: {
      defaultColumn: "created_at",
      defaultDirection: "asc",
      allowedColumns: ["created_at"],
    },
  },
  run: async () => [],
});

export const send = reducer({
  args: schema.object({ body: schema.string() }),
  result: schema.object({
    id: schema.id("messages"),
    body: schema.string(),
    author: schema.string(),
    created_at: schema.datetime(),
  }),
  offline: { mode: "onlineOnly", reason: "server assigns the message id" },
  nonOptimisticReason: "server assigns the message id",
  run: async ({ now }, args) => ({
    id: "pending",
    body: args.body,
    author: "demo-user",
    created_at: new Date(now).toISOString(),
  }),
});
