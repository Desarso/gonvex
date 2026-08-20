import { cron, internalReducer, schema } from "@gonvex/module-sdk";

export const heartbeat = internalReducer({
  args: schema.object({}),
  result: schema.object({ ok: schema.boolean() }),
  run: async () => ({ ok: true }),
});

export const heartbeatSchedule = cron({
  name: "heartbeat",
  function: "system.heartbeat",
  intervalMs: 15_000,
});
