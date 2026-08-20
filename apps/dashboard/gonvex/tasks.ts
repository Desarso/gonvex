import { liveQuery, replicaCollection, reducer, schema, visibility } from "@gonvex/module-sdk";

export const dashboardDemoTasksVisibility = visibility({
  table: "dashboard_demo_tasks",
  key: "id",
  sets: {},
  where: { operator: "public" },
});

export const list = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({
    id: schema.id("dashboard_demo_tasks"),
    title: schema.string(),
    status: schema.string(),
  })),
  replica: {
    table: "dashboard_demo_tasks",
    key: "id",
    columns: ["id", "title", "status"],
    mode: "progressive",
    maxRows: 10_000,
    maxBytes: 50_000_000,
  },
  run: async () => [],
});

export const create = reducer({
  args: schema.object({ title: schema.string() }),
  result: schema.object({
    id: schema.id("dashboard_demo_tasks"),
    title: schema.string(),
    status: schema.string(),
  }),
  offline: { mode: "onlineOnly", reason: "dashboard data-generator operation" },
  nonOptimisticReason: "dashboard data-generator operation",
  run: async (_ctx, args) => ({ id: "task_dev", title: args.title, status: "todo" }),
});

export const randomizeStatusPriority = reducer({
  args: schema.object({ count: schema.integer() }),
  result: schema.object({
    updated: schema.integer(),
    requested: schema.integer(),
    durationMs: schema.integer(),
  }),
  offline: { mode: "onlineOnly", reason: "dashboard bulk benchmark operation" },
  nonOptimisticReason: "dashboard bulk benchmark operation",
  run: async () => ({ updated: 0, requested: 0, durationMs: 0 }),
});

export const grid = liveQuery({
  args: schema.object({
    offset: schema.integer(),
    limit: schema.integer(),
    search: schema.optional(schema.string()),
    sort: schema.optional(schema.string()),
    direction: schema.optional(schema.string()),
    filters: schema.optional(schema.array(schema.object({
      id: schema.optional(schema.string()),
      column: schema.string(),
      operator: schema.string(),
      value: schema.string(),
      valueTo: schema.optional(schema.string()),
    }))),
  }),
  result: schema.object({
    rows: schema.array(schema.record(schema.any())),
    total: schema.integer(),
    offset: schema.integer(),
    limit: schema.integer(),
  }),
  liveQueryPlan: {
    table: "dashboard_demo_tasks",
    key: "id",
    columns: ["id", "pg_id", "name", "title", "description", "form_id", "sla_id", "approval_id", "notes_count", "category_icon", "category_color", "category_name", "tag_names", "tag_colors", "attachment_count", "view_count", "status", "status_name", "status_color", "status_action", "status_icon", "status_working_animation", "status_initial", "priority", "priority_name", "priority_color", "assignee", "assignee_names", "assignee_ids", "assignee_avatar_urls", "all_user_names", "all_user_avatar_urls", "due_date", "due_at", "start_date", "spot_id", "spot_name", "workspace_name", "progress", "flag_color", "created_at", "updated_at"],
    resultPath: ["rows"],
    search: {
      argument: "search",
      columns: ["title", "description", "status_name", "priority_name", "assignee_names"],
    },
    filters: {
      argument: "filters",
      allowedColumns: ["id", "pg_id", "name", "title", "description", "form_id", "sla_id", "approval_id", "notes_count", "category_icon", "category_color", "category_name", "tag_names", "tag_colors", "attachment_count", "view_count", "status", "status_name", "status_color", "status_action", "status_icon", "status_working_animation", "status_initial", "priority", "priority_name", "priority_color", "assignee", "assignee_names", "assignee_ids", "assignee_avatar_urls", "all_user_names", "all_user_avatar_urls", "due_date", "due_at", "start_date", "spot_id", "spot_name", "workspace_name", "progress", "flag_color", "created_at", "updated_at"],
      allowedOperators: ["contains", "notContains", "equals", "notEquals", "startsWith", "endsWith", "empty", "notEmpty", "oneOf", "lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual", "inRange"],
    },
    sort: {
      columnArgument: "sort",
      directionArgument: "direction",
      defaultColumn: "created_at",
      defaultDirection: "desc",
      allowedColumns: ["created_at", "updated_at", "due_date", "title", "status_name", "priority_name"],
    },
    window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 100, maxLimit: 250, count: "exact" },
  },
  run: async (_ctx, args) => ({
    rows: [],
    total: 0,
    offset: args.offset,
    limit: args.limit,
  }),
});
