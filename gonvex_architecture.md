# Gonvex Architecture

## 0. Core Idea

**Gonvex** is a Convex-like fullstack backend framework, but built around:

- React-first frontend development
- colocated backend source inside the frontend app repo
- Go backend functions
- PostgreSQL as the primary application database
- WebSocket-based realtime transport
- realtime generated TypeScript React bindings for frontend type safety
- realtime queries by default, Convex-style
- realtime SQL/grid subscriptions
- S3-compatible file storage
- multi-tenant and multi-project runtime support

High-level goal:

```txt
Convex developer experience
+ React-first generated bindings
+ Go backend runtime
+ Postgres relational power
+ live SQL/data-grid support
+ TypeScript frontend safety
+ S3-compatible storage
```

One-line summary:

```txt
Gonvex is a React-first Go + Postgres Convex alternative where backend functions live beside the frontend app, generated TypeScript bindings update during development, normal queries are realtime by default, and heavy table/grid views use a live SQL engine that reruns, diffs, and streams row patches over WebSockets.
```

---

# 1. System Overview

```txt
React frontend app
  ↓ imports from
App-local gonvex/ backend source + generated bindings
  ↓
Generated TypeScript React API bindings
  ↓
Gonvex frontend client
  ↓
WebSocket transport
  ↓
Gonvex runtime server
  ↓
Go backend functions
  ↓
PostgreSQL / Storage / Actions / Search / Optional streaming engine
```

Core components:

```txt
1. React frontend SDK
2. Generated TypeScript React bindings
3. Go backend function runtime
4. Gonvex dev daemon
5. WebSocket protocol
6. PostgreSQL primary database
7. LiveGrid / LiveSQL engine
8. Dependency-aware invalidation
9. Postgres change capture
10. Storage layer
11. Scheduler/actions
12. Multi-tenant/multi-project routing
13. Optional RisingWave/Materialize/search integrations
```

---

# 2. Frontend SDK

The frontend SDK should feel close to Convex, but be React-first and have better support for heavy grids.

Example usage:

```ts
import { api } from "./gonvex/_generated/api";
import { useLiveGrid, useMutation, useQuery } from "./gonvex/_generated/react";

const task = useQuery(api.tasks.get, { id: "task_123" });

const createTask = useMutation(api.tasks.create);

const rows = useLiveGrid(api.tasks.grid, {
  search: "bug",
  filters: {
    status: "todo",
  },
  sort: [
    { field: "createdAt", dir: "desc" },
  ],
  limit: 100,
});
```

Frontend SDK responsibilities:

```txt
typed function calls
realtime React query subscriptions
mutation calls
action calls
live grid subscriptions
WebSocket connection management
auth token attachment
frontend cache
reconnect handling
optimistic updates
row diff application
AG Grid adapter
React hooks and provider
future Solid/Vue bindings
```

The frontend should never manually construct raw WebSocket messages during normal use. It should use generated typed APIs.

Primary React API:

```ts
import { api } from "./gonvex/_generated/api";
import { GonvexProvider, useMutation, useQuery } from "./gonvex/_generated/react";
```

`useQuery` is a live subscription, not a one-shot fetch. When the backend data that the query read changes, the runtime reruns the query and pushes the new result to React over WebSocket.

---

# 3. Generated TypeScript React Bindings

Gonvex should generate TypeScript and React bindings from app-local Go source in realtime during development.

Source of truth:

```txt
Go structs
Go function signatures
Go registration metadata
gonvex/schema.go database schema metadata
```

Generated output:

```txt
gonvex/_generated/api.ts
gonvex/_generated/types.ts
gonvex/_generated/react.ts
gonvex/_generated/client.ts
gonvex/_generated/schema.ts
```

The generated files live in the frontend project so normal TypeScript tooling, Vite HMR, editor autocomplete, and React type checking see changes immediately.

Example Go:

```go
package tasks

type CreateTaskArgs struct {
	Title    string `json:"title"`
	StatusID string `json:"statusId"`
}

type Task struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	StatusID  string `json:"statusId"`
	CreatedAt int64  `json:"createdAt"`
}

func Create(ctx *gonvex.MutationCtx, args CreateTaskArgs) (Task, error) {
	// ...
}
```

Generated TypeScript:

```ts
export type CreateTaskArgs = {
  title: string;
  statusId: string;
};

export type Task = {
  id: string;
  title: string;
  statusId: string;
  createdAt: number;
};

export const api = {
  tasks: {
    create: mutation<CreateTaskArgs, Task>("tasks.create"),
  },
};
```

Generated React hooks should be typed against the generated API:

```ts
const task = useQuery(api.tasks.get, { id: "task_123" });
const createTask = useMutation(api.tasks.create);

await createTask({
  title: "Fix login bug",
  statusId: "todo",
});
```

The code generator should run in watch mode under `npm run dev`. Adding, removing, renaming, or changing a backend function or schema should regenerate bindings without a manual command.

Frontend type safety:

```ts
await client.mutation(api.tasks.create, {
  title: "Fix login bug",
  statusId: "todo",
});
```

This should type-check.

This should fail:

```ts
await client.mutation(api.tasks.create, {
  title: "Fix login bug",
});
```

because `statusId` is missing.

---

# 4. Go Backend Function Model

Gonvex should support equivalents of Convex function types.

Function types:

```txt
Query       = deterministic read
Mutation    = transactional write
Action      = side effects / runners
HTTPAction  = HTTP/webhook endpoint
Internal    = backend-only function
LiveGrid    = realtime table/grid subscription
```

Example registration:

```go
func Register(app *gonvex.App) {
	app.Query("tasks.get", GetTask)
	app.Mutation("tasks.create", CreateTask)
	app.Action("email.send", SendEmail)
	app.HTTP("stripe.webhook", StripeWebhook)
	app.InternalMutation("billing.markPaid", MarkPaid)
	app.LiveGrid("tasks.grid", TasksGrid)
}
```

---

# 5. Queries

Queries are deterministic reads and should be realtime by default, like Convex queries.

Example:

```go
type GetTaskArgs struct {
	ID string `json:"id"`
}

func GetTask(ctx *gonvex.QueryCtx, args GetTaskArgs) (Task, error) {
	return gonvex.Get[Task](ctx, "tasks", args.ID)
}
```

Query properties:

```txt
read-only
safe to rerun
subscribed to by default from React useQuery
no external side effects
can be cached
can be invalidated
can push updated results over WebSocket
```

React behavior:

```txt
useQuery(api.tasks.get, args)
  ↓
opens query.subscribe over WebSocket
  ↓
runtime executes the Go query
  ↓
runtime records tables/rows/ranges read by the query
  ↓
mutation or CDC event invalidates matching subscriptions
  ↓
runtime reruns the query
  ↓
new result is pushed to React
```

Normal query invalidation should be Convex-style dependency tracking:

```txt
query runs inside instrumented QueryCtx
  ↓
every ctx.DB read records what was read
  ↓
runtime stores dependency metadata with the active subscription
  ↓
mutation commit or CDC event produces a write set
  ↓
matching query subscriptions rerun
```

Examples of dependencies captured from normal Go queries:

```txt
single row by table + primary key
table scan scoped by tenant
indexed predicate, such as tasks.status_id = 'todo'
range predicate, such as messages.created_at > cursor
ordered limited window, such as latest 50 messages in a room
permission/auth dependency, such as current user's role
```

For simple app queries, this gives the normal Convex feeling: if a mutation changes data that a subscribed query depends on, the query reruns automatically.

Queries should not be treated as request/response RPC from the frontend. The request/response mode may exist for server-side usage or tests, but the normal React client path is a live subscription.

Queries should not:

```txt
send emails
call Stripe
call OpenAI
mutate external state
use random values carelessly
depend on current time unless declared
```

---

# 6. Mutations

Mutations are transactional writes.

Example:

```go
func CreateTask(ctx *gonvex.MutationCtx, args CreateTaskArgs) (Task, error) {
	task := Task{
		ID:       gonvex.NewID("tasks"),
		Title:    args.Title,
		StatusID: args.StatusID,
	}

	err := ctx.DB.Insert("tasks", task)
	if err != nil {
		return Task{}, err
	}

	return task, nil
}
```

Mutation properties:

```txt
runs in a transaction
writes to Postgres
returns result to frontend
emits database changes
triggers invalidation after commit
```

Important rule:

```txt
Do not perform irreversible external side effects directly inside mutations.
```

Bad:

```go
func ChargeCard(ctx *gonvex.MutationCtx, args Args) error {
	stripe.Charge(...)
	ctx.DB.Insert(...)
	return nil
}
```

If the mutation retries, the card might be charged twice.

Better:

```txt
mutation creates payment_intent row
action performs Stripe call
internal mutation records result
```

---

# 7. Actions

Actions are runners/workers.

Use actions for:

```txt
OpenAI calls
Stripe calls
email sending
file processing
image processing
web scraping
ffmpeg
external HTTP APIs
long-running jobs
```

Example:

```go
func SendEmail(ctx *gonvex.ActionCtx, args SendEmailArgs) (SendEmailResult, error) {
	return ctx.Email.Send(args.To, args.Subject, args.Body)
}
```

Actions are:

```txt
not deterministic
not automatically reactive
not transactional with the database
allowed to talk to external systems
```

Actions can call queries/mutations internally.

---

# 8. HTTP Actions

HTTP actions are normal HTTP endpoints.

Use cases:

```txt
Stripe webhooks
GitHub webhooks
OAuth callbacks
public API endpoints
upload callbacks
health checks
```

Example:

```go
func StripeWebhook(ctx *gonvex.HTTPContext) error {
	payload := ctx.Request.Body
	signature := ctx.Request.Header.Get("Stripe-Signature")

	event, err := stripe.VerifyWebhook(payload, signature)
	if err != nil {
		return ctx.JSON(400, map[string]string{"error": "invalid webhook"})
	}

	return ctx.RunMutation("billing.handleStripeEvent", event)
}
```

---

# 9. Internal Functions

Internal functions are backend-only.

Use cases:

```txt
workers
scheduler
webhook processing
system maintenance
background cleanup
billing state changes
```

Example:

```go
app.InternalMutation("billing.markPaid", MarkPaid)
```

Frontend cannot call internal functions.

---

# 10. Determinism

Deterministic means:

```txt
same inputs + same database state = same result
```

Deterministic examples:

```txt
read task by ID
list recent messages
calculate total from stored rows
validate input
```

Non-deterministic examples:

```txt
time.Now()
rand.Int()
http.Get()
OpenAI call
Stripe call
filesystem read
```

Queries and mutations should be mostly deterministic because Gonvex may:

```txt
retry them
rerun them
cache them
invalidate them
subscribe to them
```

Actions exist for non-deterministic work.

---

# 11. PostgreSQL Primary Database

Postgres is the primary system of record.

Used for:

```txt
CRUD
transactions
relational data
constraints
indexes
joins
multi-tenant data
file metadata
audit logs
permissions
```

Gonvex should use Postgres for app data, not a custom document database.

Benefits:

```txt
SQL
indexes
joins
constraints
migrations
ecosystem
backup tools
extensions
full-text search
trigram search
```

---

# 12. Database Access Layers

Gonvex should support multiple DB access modes.

## 12.1 Structured DB API

Useful for dependency tracking.

```go
task, err := ctx.DB.Table("tasks").Get(args.ID)
```

```go
tasks, err := ctx.DB.Table("tasks").
	Where("status_id", "=", args.StatusID).
	OrderBy("created_at", "desc").
	Limit(50).
	Find()
```

Pros:

```txt
easy dependency tracking
typed helpers
safe query building
good for common app queries
```

## 12.2 Raw SQL

Useful for power users.

```go
rows, err := ctx.SQL(`
	SELECT *
	FROM tasks
	WHERE title ILIKE $1
	ORDER BY created_at DESC
	LIMIT 100
`, "%bug%")
```

Pros:

```txt
full SQL power
joins
aggregates
complex queries
easy for data grids
```

Cons:

```txt
harder dependency tracking
harder type generation
harder reactivity
```

## 12.3 LiveSQL / LiveGrid

Restricted analyzable SQL for realtime subscriptions.

```go
ctx.LiveSQL(`
	SELECT tasks.*, statuses.name AS status_name
	FROM tasks
	LEFT JOIN statuses ON statuses.id = tasks.status_id
	WHERE tasks.title ILIKE $1
	ORDER BY tasks.created_at DESC
	LIMIT 100
`, "%bug%")
```

Rules:

```txt
must be analyzable
must have one main table
small lookup joins allowed
bounded result window required
indexed sort recommended
volatile functions disallowed
complex recursive SQL disallowed
```

---

# 13. LiveGrid Engine

LiveGrid is the special realtime data-table layer.

It targets:

```txt
AG Grid
Glide Data Grid
admin dashboards
CRM tables
kanban views
search/filter/sort UIs
```

It does not need full arbitrary SQL. It needs a practical subset:

```txt
one main table
filters
sorts
substring search
pagination/windowing
small lookup joins
row-level frontend patches
```

Example schema:

```txt
tasks:
  id
  title
  status_id
  assignee_id
  created_at
  updated_at
  tenant_id

statuses:
  id
  name
  color
```

Example frontend:

```ts
const grid = useLiveGrid(api.tasks.grid, {
  search: "login",
  filters: {
    statusName: "todo",
  },
  sort: [
    { field: "createdAt", dir: "desc" },
  ],
  limit: 100,
});
```

Example SQL:

```sql
SELECT
  tasks.id,
  tasks.title,
  tasks.status_id,
  statuses.name AS status_name,
  tasks.created_at
FROM tasks
LEFT JOIN statuses ON statuses.id = tasks.status_id
WHERE tasks.tenant_id = $1
AND tasks.title ILIKE $2
AND statuses.name = $3
ORDER BY tasks.created_at DESC
LIMIT 100;
```

---

# 14. LiveGrid Runtime Flow

```txt
frontend subscribes
  ↓
Gonvex builds SQL query
  ↓
query runs against Postgres
  ↓
runtime stores result rows + row IDs + dependency metadata
  ↓
Postgres changes arrive through CDC/logical replication
  ↓
invalidation engine decides affected subscriptions
  ↓
Gonvex reruns affected query
  ↓
old result and new result are diffed
  ↓
patch is sent to frontend over WebSocket
```

Important strategy:

```txt
Do not perfectly compute the frontend diff from the DB event.
Instead, use the DB event to decide whether to rerun.
Then compute the diff by comparing old result vs new result.
```

This greatly simplifies correctness.

---

# 15. Dependency-Aware Invalidation

Goal:

```txt
rerun only subscriptions that might be affected
```

Gonvex should have two related invalidation paths:

```txt
normal query invalidation
  Convex-style dependency capture from instrumented Go DB reads

LiveSQL / LiveGrid invalidation
  SQL/query-plan metadata + predicate/range/window dependencies
```

Both paths share the same final mechanism:

```txt
database write event
  ↓
match event against active subscription dependencies
  ↓
rerun possibly affected subscriptions
  ↓
send new result or patch over WebSocket
```

Important principle:

```txt
over-invalidation is acceptable
under-invalidation is unacceptable
```

Rerunning too much is a performance issue.

Missing stale data is a correctness bug.

Dependency types:

```txt
table dependency
row dependency
predicate dependency
range dependency
sort/window dependency
lookup-table dependency
search dependency
permission dependency
tenant dependency
```

## Normal Query Dependencies

Normal Go queries should use instrumented database APIs where possible. The instrumentation records what the query read while the query is executing.

Example:

```go
func GetTask(ctx *gonvex.QueryCtx, args GetTaskArgs) (Task, error) {
	return gonvex.Get[Task](ctx, "tasks", args.ID)
}
```

Recorded dependency:

```txt
table: tasks
row id: task_123
tenant: tenant_1
```

List query example:

```go
func ListTodoTasks(ctx *gonvex.QueryCtx, args Args) ([]Task, error) {
	return gonvex.Query[Task](ctx, "tasks").
		Where("status_id", "=", "todo").
		OrderBy("created_at", gonvex.Desc).
		Limit(50).
		All()
}
```

Recorded dependencies:

```txt
table: tasks
tenant: tenant_1
predicate: status_id = todo
sort/window: created_at desc limit 50
selected columns: fields returned by Task
```

Mutation invalidation:

```txt
mutation writes rows in transaction
  ↓
runtime records write set before/after commit
  ↓
after commit, write set is matched against query dependencies
  ↓
affected queries rerun
```

For writes that bypass Gonvex mutations, Postgres CDC/logical replication produces the same kind of change event and goes through the same matcher.

## LiveSQL / LiveGrid Dependencies

Complex SQL cannot rely only on row IDs because rows can enter or leave a result due to filters, joins, search, sort, permissions, or window boundaries.

LiveSQL / LiveGrid should therefore require analyzable SQL or a structured query builder that produces dependency metadata before execution.

Supported realtime SQL should identify:

```txt
main table
tenant column
primary key
selected columns
filter columns
search columns
sort columns
limit/window
lookup joins
permission dependencies
```

For AG Grid, this metadata comes from the grid definition plus the current grid request:

```txt
grid schema defines allowed tables, joins, filters, sorts, and search fields
AG Grid sends current filter/sort/window/search model
Gonvex compiles this into parameterized SQL
Gonvex stores the compiled dependency metadata beside the active subscription
```

Invalidation strategy for complex SQL:

```txt
changed row table matches main table or lookup table
  ↓
tenant/permission scope matches
  ↓
changed columns overlap selected/filter/search/sort/join columns
  ↓
old row or new row may match predicate/window
  ↓
rerun SQL query
  ↓
diff old visible result against new visible result
  ↓
send AG Grid patch or replace-window patch
```

The important rule is that Gonvex does not need to derive the exact UI patch directly from the database change. It only needs to decide whether a subscription may be stale. The actual patch is computed by rerunning the query and diffing old vs new results.

Arbitrary raw SQL should not be realtime by default unless it can be analyzed. If Gonvex cannot extract dependency metadata, the options are:

```txt
reject realtime subscription
require explicit dependency declarations
fall back to broad table-level invalidation for the same tenant
mark the query as one-shot/non-realtime
```

Example dependency metadata:

```json
{
  "subId": "sub_123",
  "mainTable": "tasks",
  "mainPk": "id",
  "tenantId": "tenant_1",
  "filters": [
    { "field": "tasks.title", "op": "substring", "value": "bug" },
    { "field": "statuses.name", "op": "eq", "value": "todo" }
  ],
  "sort": [
    { "field": "tasks.created_at", "dir": "desc" }
  ],
  "limit": 100,
  "lookupTables": [
    {
      "table": "statuses",
      "join": "tasks.status_id = statuses.id",
      "fieldsUsed": ["name", "color"]
    }
  ]
}
```

---

# 16. Why Row Invalidation Alone Is Not Enough

Row invalidation works for:

```sql
SELECT * FROM users WHERE id = 'u1';
```

It fails for lists:

```sql
SELECT *
FROM messages
WHERE room_id = 'r1'
ORDER BY created_at DESC
LIMIT 50;
```

Current result contains:

```txt
m1 ... m50
```

A new row arrives:

```txt
m99 room_id='r1' created_at=now()
```

That row was not previously in the result, so tracking only current row IDs would miss it.

But it should enter the top 50 and push another row out.

Therefore LiveGrid needs predicate/range/window dependencies:

```txt
messages where room_id = r1
ordered by created_at desc
top 50 window
```

---

# 17. Change Capture

Gonvex needs to observe Postgres changes.

Possible mechanisms:

```txt
logical replication
triggers + NOTIFY
Debezium-style CDC
pgoutput plugin
wal2json
custom replication consumer
```

MVP options:

```txt
triggers + NOTIFY = simpler but less robust
logical replication = better long-term
```

Change event shape:

```json
{
  "table": "tasks",
  "op": "UPDATE",
  "old": {
    "id": "task_1",
    "status_id": "todo",
    "title": "Fix login"
  },
  "new": {
    "id": "task_1",
    "status_id": "done",
    "title": "Fix login"
  }
}
```

Old and new values are important because a row can:

```txt
enter a result
leave a result
move position
change displayed fields
change permissions
```

---

# 18. LiveGrid Invalidation Rules

For MVP, keep rules conservative.

## Main table changes

If a changed row belongs to the same tenant and table, rerun the subscription if:

```txt
changed columns overlap selected/filter/sort/search columns
or row was already in result
or row could enter the result
```

Simple MVP rule:

```txt
any change to main table for the same tenant reruns matching LiveGrid subscriptions
```

This is safe, though not perfectly efficient.

## Lookup table changes

Example:

```txt
tasks.status_id -> statuses.id
grid displays statuses.name
```

If a status name changes:

```txt
rerun grids that use statuses.name
```

Because it may affect:

```txt
displayed cell value
filter result
sort order
search match
```

## Search changes

If search is on `tasks.title`, then a title update can make a row enter or leave.

MVP:

```txt
title changed → rerun grid queries that search title
```

## Sort changes

If sort field changes:

```txt
created_at changed → rerun queries sorting by created_at
```

Because row position may change.

---

# 19. Diff Generation

After rerun:

```txt
old result
new result
↓
diff
↓
frontend patch
```

Patch types:

```txt
add row
update row
remove row
move row
replace page
reset query
```

AG Grid transaction format:

```ts
gridApi.applyTransaction({
  add: [...],
  update: [...],
  remove: [...],
});
```

For sorted grids, moves may be easier as:

```txt
replace visible window
```

MVP can send:

```json
{
  "type": "livegrid.replace",
  "subId": "sub_123",
  "rows": [...]
}
```

Then later optimize to:

```json
{
  "type": "livegrid.patch",
  "subId": "sub_123",
  "ops": [
    { "op": "remove", "id": "task_1" },
    { "op": "insert", "index": 0, "row": { "id": "task_9" } },
    { "op": "update", "id": "task_4", "row": { "title": "New title" } }
  ]
}
```

MVP recommendation:

```txt
Start with replace-window patches.
Then add row transaction patches.
```

---

# 20. WebSocket Protocol

Everything goes through WebSockets. Queries and LiveGrid are subscription protocols, not one-shot frontend fetches.

Message categories:

```txt
query.subscribe
query.unsubscribe
query.result
query.patch

mutation.call
mutation.result
mutation.error

action.call
action.result
action.error

livegrid.subscribe
livegrid.unsubscribe
livegrid.result
livegrid.replace
livegrid.patch

storage.createUploadUrl
storage.result

system.reload
system.error
```

Example subscription:

```json
{
  "type": "livegrid.subscribe",
  "id": "sub_123",
  "path": "tasks.grid",
  "args": {
    "search": "bug",
    "filters": {
      "status": "todo"
    },
    "sort": [
      { "field": "createdAt", "dir": "desc" }
    ],
    "limit": 100
  }
}
```

Example initial result:

```json
{
  "type": "livegrid.result",
  "id": "sub_123",
  "rows": [
    {
      "id": "task_1",
      "title": "Fix login bug",
      "statusName": "todo"
    }
  ]
}
```

Example patch:

```json
{
  "type": "livegrid.patch",
  "id": "sub_123",
  "ops": [
    {
      "op": "update",
      "id": "task_1",
      "row": {
        "id": "task_1",
        "title": "Fix login bug now",
        "statusName": "todo"
      }
    }
  ]
}
```

---

# 21. Dev Daemon

The Gonvex dev daemon is equivalent to `convex dev`.

Command:

```bash
npm run dev
```

The user should not need to run Vite and the Gonvex watcher separately during normal development. The generated template provides one script that starts both the frontend dev server and the Gonvex dev sync loop.

Important separation:

```txt
npm run dev starts:
  frontend Vite dev server
  Gonvex watcher/sync/codegen CLI

npm run dev does not have to start:
  the Gonvex backend runtime
  Postgres
  storage services
```

This mirrors `npx convex dev`: the local command watches app code and pushes live updates to a separate backend runtime. That runtime may be hosted Gonvex cloud, self-hosted Gonvex, a Docker dev runtime, or an independently started local process.

Underlying command:

```bash
gonvex dev
```

Responsibilities:

```txt
watch app-local gonvex/ Go files
build/typecheck backend code through Air or an embedded Air-like watcher
run tests/checks
extract functions/types/schema metadata
generate TypeScript React bindings in realtime
sync backend code to the configured runtime
trigger runtime reload of updated functions/schema
notify frontend of API changes
integrate with Vite
```

Expected project shape:

```txt
my-app/
  package.json
  src/
    App.tsx
  gonvex/
    tasks.go
    schema.go
    _generated/
      api.ts
      react.ts
      types.ts
```

The `gonvex/` directory is part of the frontend app repo, similar to Convex's app-local backend directory. Backend source is developed beside React code, but executed by the Gonvex runtime.

AI/developer workflow:

```txt
AI or developer edits gonvex/*.go
  ↓
Air or the embedded Go watcher detects the file change
  ↓
Gonvex builds/typechecks the backend function bundle
  ↓
Gonvex extracts function signatures, Go structs, and schema metadata
  ↓
backend manifest updates
  ↓
React TypeScript bindings regenerate in gonvex/_generated
  ↓
updated function/schema bundle syncs to the configured runtime
  ↓
separate runtime hot-reloads the updated functions/schema
  ↓
Vite/TypeScript sees the changed generated files
  ↓
frontend can immediately call or subscribe to the new function
```

The target feeling should be the same as Convex: create a backend function, save the file, import `api.someModule.someFunction` from React, and call it with full TypeScript autocomplete.

Example package script:

```json
{
  "scripts": {
    "dev": "gonvex dev -- vite"
  }
}
```

Template bootstrap:

```bash
npm create gonvex@latest my-app
```

This clones or scaffolds a React + Vite template with the `gonvex/` backend directory, generated binding imports, and the single `npm run dev` workflow already wired.

Dev daemon flow:

```txt
file changes
  ↓
Air-style watcher or embedded Go watcher detects changes
  ↓
Go build/typecheck
  ↓
AST/signature/schema extraction
  ↓
manifest generation
  ↓
TypeScript React codegen
  ↓
sync function/schema bundle to configured runtime
  ↓
configured runtime hot reloads
  ↓
frontend HMR sees updated bindings
```

Runtime targets for development:

```txt
local Docker runtime
local bare gonvex runtime process
remote dev runtime
hosted Gonvex cloud dev deployment
```

The frontend does not care where the runtime is running. `gonvex dev` owns syncing local backend code and manifest changes to that runtime, then tells the React client when APIs or active subscriptions need to reload.

The dev script owns live update coordination, not necessarily runtime lifecycle. Running the runtime locally is allowed for development, but it is a separate concern from the watcher that detects code changes, regenerates bindings, and syncs updates.

---

# 22. Backend Manifest

The dev daemon should generate a backend manifest.

Example:

```json
{
  "project": "my-app",
  "functions": {
    "tasks.create": {
      "kind": "mutation",
      "args": "CreateTaskArgs",
      "returns": "Task"
    },
    "tasks.grid": {
      "kind": "livegrid",
      "args": "TaskGridArgs",
      "returns": "TaskGridRow"
    }
  },
  "types": {
    "CreateTaskArgs": {
      "title": "string",
      "statusId": "string"
    }
  }
}
```

This manifest powers:

```txt
runtime dispatch
frontend codegen
API validation
docs
dev console
function discovery
```

---

# 23. Type Generation Strategy

Avoid protobuf at first.

Recommended MVP:

```txt
Go structs
→ JSON schema / internal manifest
→ generated TypeScript types
→ JSON over WebSocket
```

Why not protobuf initially:

```txt
more complexity
harder DX
harder debugging
less flexible during development
harder browser tooling
```

Later add optional binary transport:

```txt
MessagePack
CBOR
protobuf
FlatBuffers
```

But JSON + generated TS is enough for:

```txt
autocomplete
lint errors
typed function args
typed return values
safe frontend calls
```

---

# 24. Runtime Validation

Even with TypeScript types, runtime validation is still needed.

Reason:

```txt
clients can lie
old clients may call new server
external callers may send invalid JSON
malicious users exist
```

Validation sources:

```txt
Go struct tags
generated JSON schema
custom validators
database constraints
```

Example Go:

```go
type CreateTaskArgs struct {
	Title    string `json:"title" validate:"required,min=1,max=200"`
	StatusID string `json:"statusId" validate:"required"`
}
```

Generated TypeScript:

```ts
export type CreateTaskArgs = {
  title: string;
  statusId: string;
};
```

Runtime validation rejects invalid calls before function execution.

---

# 25. Storage Layer

Gonvex needs an equivalent of Convex Storage.

Use S3-compatible object storage.

Supported backends:

```txt
AWS S3
Cloudflare R2
MinIO
Backblaze B2
Tigris
Wasabi
Supabase Storage
DigitalOcean Spaces
```

Storage model:

```txt
Postgres stores metadata
S3-compatible storage stores bytes
Gonvex enforces permissions and signed URLs
```

---

# 26. Storage API

Backend API:

```go
ctx.Storage.GenerateUploadURL(...)
ctx.Storage.GenerateDownloadURL(fileID)
ctx.Storage.GetURL(fileID)
ctx.Storage.Delete(fileID)
ctx.Storage.GetMetadata(fileID)
ctx.Storage.CreateMultipartUpload(...)
ctx.Storage.CompleteMultipartUpload(...)
```

Frontend flow:

```txt
frontend requests upload URL
  ↓
Gonvex creates file metadata record
  ↓
Gonvex returns signed upload URL
  ↓
frontend uploads directly to S3/R2
  ↓
frontend calls mutation to attach file to app record
```

Example frontend:

```ts
const upload = await client.mutation(api.files.createUploadUrl, {
  contentType: file.type,
  size: file.size,
});

await fetch(upload.url, {
  method: "PUT",
  body: file,
  headers: {
    "Content-Type": file.type,
  },
});

await client.mutation(api.tasks.attachFile, {
  taskId,
  fileId: upload.fileId,
});
```

---

# 27. File Metadata Schema

Postgres table:

```sql
CREATE TABLE files (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  content_type TEXT,
  size_bytes BIGINT,
  checksum TEXT,
  visibility TEXT NOT NULL DEFAULT 'private',
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  uploaded_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ
);
```

Why metadata belongs in Postgres:

```txt
permissions
ownership
tenant isolation
references from app rows
deletion cleanup
audit logs
billing/storage usage
file lifecycle status
```

---

# 28. Storage Permissions

Files must be permissioned through Gonvex, not directly public by default.

Visibility options:

```txt
private
tenant
public
signed
```

Download flow:

```txt
frontend asks Gonvex for file URL
  ↓
Gonvex checks user permissions
  ↓
Gonvex returns short-lived signed URL
```

Example:

```go
func GetFileURL(ctx *gonvex.QueryCtx, args GetFileURLArgs) (string, error) {
	file, err := ctx.Storage.GetMetadata(args.FileID)
	if err != nil {
		return "", err
	}

	if !ctx.Auth.CanReadFile(file) {
		return "", gonvex.ErrForbidden
	}

	return ctx.Storage.GenerateDownloadURL(args.FileID, time.Minute*10)
}
```

---

# 29. Multi-Tenant Support

Gonvex should support native multi-tenancy.

Definitions:

```txt
Tenant = customer/org/database scope
Project = backend code bundle
```

Tenant isolation models:

```txt
single database + tenant_id column
schema per tenant
database per tenant
cluster per tenant
```

MVP recommendation:

```txt
single database + tenant_id column
```

Then support advanced modes later.

Every request context includes:

```go
ctx.TenantID
ctx.UserID
ctx.ProjectID
ctx.Role
```

Every query/mutation/livegrid must enforce tenant scope.

Example:

```sql
SELECT *
FROM tasks
WHERE tenant_id = $1
ORDER BY created_at DESC
LIMIT 100;
```

---

# 30. Multi-Project Support

Multi-project means one Gonvex runtime can host multiple backend code bundles.

```txt
project_a
  backend code A
  tenants 1..N

project_b
  backend code B
  tenants 1..N
```

Runtime routing:

```txt
request projectId
  ↓
load backend bundle
  ↓
resolve function path
  ↓
select tenant database/schema
  ↓
execute
```

This enables:

```txt
hosted Gonvex cloud
multiple apps on one runtime
project isolation
per-project deployment versions
```

---

# 31. Auth and Permissions

Gonvex runtime should attach auth context to every function.

Context fields:

```go
ctx.UserID
ctx.SessionID
ctx.TenantID
ctx.ProjectID
ctx.Role
ctx.Claims
```

Auth providers:

```txt
custom auth
JWT
Clerk
Auth.js
Firebase Auth
Supabase Auth
OIDC
SAML later
```

Permission checks must apply to:

```txt
queries
mutations
actions
live grids
storage
search
HTTP endpoints
```

Important realtime rule:

```txt
permissions are dependencies too
```

If a user's role changes, their active subscriptions may need to rerun or close.

---

# 32. Search

Search should be layered.

MVP:

```txt
Postgres ILIKE
Postgres pg_trgm indexes
Postgres full-text search
```

For serious search:

```txt
Meilisearch
Typesense
Tantivy
OpenSearch
Elasticsearch
```

For LiveGrid substring search:

```sql
WHERE title ILIKE '%' || $1 || '%'
```

Better Postgres index:

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX tasks_title_trgm_idx
ON tasks
USING gin (title gin_trgm_ops);
```

This supports much better substring search performance.

---

# 33. Optional RisingWave / Materialize Layer

This is not required for MVP.

Use for:

```txt
heavy realtime dashboards
aggregates
leaderboards
analytics
shared maintained views
large streaming computations
```

Architecture:

```txt
Postgres
  ↓ CDC
RisingWave/Materialize
  ↓ maintained views / subscriptions
Gonvex runtime
  ↓ WebSocket
Frontend
```

Use cases:

```txt
COUNT/SUM/GROUP BY dashboards
large live analytics
global metrics
leaderboards
streaming event analytics
```

Not ideal for:

```txt
every tiny personalized app query
every random AG Grid filter as permanent view
basic CRUD
transactional writes
```

Gonvex should keep this optional.

---

# 34. Why Not Use Only RisingWave/Materialize?

RisingWave and Materialize are streaming/incremental SQL engines.

They do not replace:

```txt
typed Go backend functions
mutations/actions
auth
permissions
storage
frontend SDK
TypeScript bindings
WebSocket sessions
multi-tenant app runtime
optimistic UI
scheduler
business logic
```

Gonvex remains the application framework.

RisingWave/Materialize can be a reactive compute layer.

---

# 35. Convex Compatibility Lessons

Convex is strong for:

```txt
small reactive app state
document-style queries
typed frontend calls
easy backend deployment
automatic invalidation
```

Convex is weak for:

```txt
arbitrary SQL
joins
server-side AG Grid
substring search + arbitrary sort
large relational dashboards
complex reporting
loading huge tables
```

Gonvex should preserve the DX while fixing the relational/grid problem.

---

# 36. AG Grid Adapter

Gonvex should provide an adapter for AG Grid server-side row model.

Frontend:

```ts
const datasource = createGonvexAgGridDatasource(api.tasks.grid, {
  pageSize: 100,
});

gridApi.setGridOption("serverSideDatasource", datasource);
```

Adapter responsibilities:

```txt
convert AG Grid filter model to Gonvex args
convert AG Grid sort model to Gonvex args
request row windows
subscribe to live changes
apply transactions
handle pagination/scrolling
unsubscribe old views
debounce search
```

AG Grid flow:

```txt
user scrolls
  ↓
request visible window
  ↓
Gonvex returns rows
  ↓
user filters/sorts
  ↓
old subscription closes
  ↓
new subscription opens
```

Important lifecycle:

```txt
one active grid view per user is fine
permanent materialized view per search is bad
```

---

# 37. Subscription Lifecycle

Every live subscription should have:

```txt
sub id
session id
user id
tenant id
project id
function path
args hash
created time
last active time
current result snapshot
dependency metadata
```

Lifecycle:

```txt
subscribe
rerun on changes
patch results
unsubscribe on component unmount
expire on disconnect
cleanup after TTL
```

TTL rules:

```txt
immediate cleanup on explicit unsubscribe
short grace period on disconnect
hard expiration after inactivity
```

---

# 38. Caching

Caching layers:

```txt
frontend cache
runtime subscription cache
Postgres query plan/cache
optional Redis
optional CDN for files
```

Frontend cache:

```txt
stores latest query results
applies patches
survives reconnect optionally
deduplicates identical subscriptions
```

Runtime cache:

```txt
stores active subscription snapshots
maps dependencies to subscriptions
deduplicates same live query among sessions
```

Potential optimization:

```txt
if multiple users subscribe to same tenant/query args,
run once and fan out result
```

---

# 39. Optimistic Updates

Mutations can support optimistic updates.

Frontend:

```ts
const createTask = useMutation(api.tasks.create).withOptimisticUpdate(
  (cache, args) => {
    cache.insert(api.tasks.grid, {
      id: "temp",
      title: args.title,
    });
  }
);
```

Runtime still provides authoritative result.

Important:

```txt
optimistic update should be reconciled with server patch
temporary IDs must be replaced
failed mutations must roll back
```

---

# 40. Scheduler

Equivalent to Convex scheduler.

Use cases:

```txt
run job later
send reminder
retry action
background cleanup
expire file uploads
billing sync
daily digest
```

API:

```go
ctx.Scheduler.RunAfter(time.Minute*5, "emails.sendReminder", args)
ctx.Scheduler.RunAt(t, "billing.chargeInvoice", args)
```

Scheduler table:

```sql
CREATE TABLE scheduled_jobs (
  id TEXT PRIMARY KEY,
  tenant_id TEXT,
  function_path TEXT NOT NULL,
  args JSONB NOT NULL,
  run_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

# 41. Deployment Model

Possible modes:

```txt
local dev
self-hosted single binary
Docker Compose
Kubernetes
hosted Gonvex cloud
```

Local dev stack:

```txt
Gonvex runtime
Postgres
optional MinIO
frontend Vite
```

Docker Compose services:

```yaml
services:
  gonvex:
    image: gonvex/runtime
  postgres:
    image: postgres:16
  minio:
    image: minio/minio
```

Production stack:

```txt
Gonvex runtime replicas
Postgres primary
object storage
worker pool
optional Redis
optional search
optional RisingWave
```

---

# 42. Observability

Gonvex should expose:

```txt
function logs
query timing
mutation timing
subscription count
livegrid rerun count
patch sizes
WebSocket connection count
Postgres latency
storage usage
action retries
error traces
```

Developer dashboard:

```txt
active functions
active subscriptions
recent mutations
slow queries
invalidations
storage files
scheduled jobs
tenant usage
```

---

# 43. Security

Security requirements:

```txt
all frontend calls authenticated where needed
tenant isolation enforced server-side
signed upload/download URLs expire
input validation on all function calls
SQL injection impossible through parameterization
LiveGrid filters compiled safely
actions cannot expose secrets to clients
internal functions cannot be called by frontend
```

SQL safety:

```txt
never concatenate raw user strings into SQL
compile filters into parameterized SQL
restrict allowed sort fields
restrict allowed filter fields
restrict allowed lookup joins
```

Good:

```go
query.Where("title", "ILIKE", "%"+args.Search+"%")
```

Bad:

```go
sql := "WHERE title ILIKE '%" + args.Search + "%'"
```

---

# 44. LiveGrid Query Builder

Define grid schema server-side.

Example:

```go
var TasksGrid = gonvex.DefineLiveGrid[TaskGridArgs, TaskGridRow](
	gonvex.LiveGridConfig{
		MainTable: "tasks",
		PrimaryKey: "id",
		TenantColumn: "tenant_id",

		AllowedFilters: []gonvex.FilterField{
			{Name: "title", Column: "tasks.title", Ops: []string{"contains", "eq"}},
			{Name: "statusName", Column: "statuses.name", Ops: []string{"eq", "contains"}},
			{Name: "createdAt", Column: "tasks.created_at", Ops: []string{"gte", "lte"}},
		},

		AllowedSorts: []gonvex.SortField{
			{Name: "createdAt", Column: "tasks.created_at"},
			{Name: "title", Column: "tasks.title"},
			{Name: "statusName", Column: "statuses.name"},
		},

		Joins: []gonvex.Join{
			{
				Table: "statuses",
				On: "statuses.id = tasks.status_id",
				Fields: []string{"name", "color"},
			},
		},
	},
)
```

This avoids arbitrary unsafe SQL while still supporting rich grids.

---

# 45. LiveGrid Query Args

Example:

```go
type TaskGridArgs struct {
	Search  string       `json:"search"`
	Filters []GridFilter `json:"filters"`
	Sort    []GridSort   `json:"sort"`
	Limit   int          `json:"limit"`
	Offset  int          `json:"offset"`
}

type GridFilter struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value"`
}

type GridSort struct {
	Field string `json:"field"`
	Dir   string `json:"dir"`
}
```

Generated TS:

```ts
export type TaskGridArgs = {
  search?: string;
  filters?: Array<{
    field: "title" | "statusName" | "createdAt";
    op: "eq" | "contains" | "gte" | "lte";
    value: unknown;
  }>;
  sort?: Array<{
    field: "createdAt" | "title" | "statusName";
    dir: "asc" | "desc";
  }>;
  limit: number;
  offset?: number;
};
```

This gives frontend type-safe grid fields.

---

# 46. Why This Is Easier Than Arbitrary Reactive SQL

Full arbitrary reactive SQL requires understanding:

```txt
joins
aggregates
windows
recursive CTEs
subqueries
volatile functions
ORDER BY/LIMIT
permissions
text search
UDFs
```

LiveGrid only supports:

```txt
known main table
known joins
known fields
known filters
known sorts
bounded windows
safe search
```

This makes dependency awareness realistic.

---

# 47. MVP Build Order

Recommended order:

```txt
1. Go function registry
2. WebSocket subscriptions/RPC
3. Generated TypeScript React bindings
4. Basic Postgres queries/mutations
5. Realtime useQuery rerun on mutation invalidation
6. npm create gonvex React template
7. gonvex dev + Vite single npm run dev workflow
8. Air-style Go watcher, sync CLI, and separate runtime hot reload
9. Auth context
10. Multi-tenant tenant_id enforcement
11. S3-compatible storage
12. LiveGrid static query without reactivity
13. Change capture from Postgres
14. LiveGrid rerun on table changes
15. Diff old/new rows
16. AG Grid adapter
17. Predicate-aware invalidation
18. Lookup-table invalidation
19. Search indexes
20. Scheduler/actions
21. Optional RisingWave/Materialize integration
```

---

# 48. MVP Scope

MVP should include:

```txt
React + Vite template
single npm run dev workflow
Go backend functions
app-local gonvex/ backend directory
Air-style Go watcher and runtime sync
TypeScript React codegen
WebSocket calls/subscriptions
Postgres CRUD
mutations
realtime queries
actions
storage with S3/R2/MinIO
one-table LiveGrid
basic filters
basic sorts
substring search
rerun + replace-window patch
tenant isolation
```

MVP should not include:

```txt
perfect arbitrary SQL reactivity
incremental SQL computation
distributed streaming engine
complex joins
recursive queries
offline sync
multi-region
advanced search engine
```

---

# 49. Production Scope

Production-ready version should add:

```txt
logical replication
row/predicate invalidation
lookup join invalidation
AG Grid transaction patches
scheduler
retries
observability dashboard
file cleanup jobs
permissions dependencies
connection pooling
rate limits
schema migrations
backup/restore guidance
```

---

# 50. Long-Term Vision

Long-term Gonvex can evolve into:

```txt
Go-native Convex alternative
Postgres-native app backend
realtime SQL/grid framework
typed frontend/backend system
multi-tenant backend platform
S3-compatible storage runtime
optional streaming analytics layer
```

Potential advanced features:

```txt
offline sync
local-first cache
RisingWave/Materialize adapters
search adapters
schema migration tool
AI-generated grid definitions
visual admin console
hosted cloud
team/project dashboard
edge subscriptions
```

---

# 51. Final Architecture Diagram

```txt
┌─────────────────────────────────────────────┐
│                 Frontend                    │
│ React / AG Grid                             │
└─────────────────────┬───────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│     Generated TypeScript React API           │
│ api.tasks.create / useQuery / useMutation    │
└─────────────────────┬───────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────┐
│              Gonvex Client SDK               │
│ queries / mutations / actions / live grids   │
└─────────────────────┬───────────────────────┘
                      │ WebSocket
                      ▼
┌─────────────────────────────────────────────┐
│              Gonvex Runtime                  │
│ auth / sessions / dispatch / subscriptions   │
└───────┬─────────────┬──────────────┬────────┘
        │             │              │
        ▼             ▼              ▼
┌────────────┐ ┌──────────────┐ ┌──────────────┐
│ Go funcs   │ │ LiveGrid     │ │ Storage API  │
│ Query/Mut  │ │ rerun+diff   │ │ S3/R2/MinIO  │
│ Action     │ │ patches      │ │              │
└─────┬──────┘ └──────┬───────┘ └──────┬───────┘
      │               │                │
      ▼               ▼                ▼
┌────────────────────────────┐  ┌────────────────┐
│        PostgreSQL           │  │ Object Storage │
│ app data / file metadata    │  │ file blobs     │
│ transactions / indexes      │  │                │
└──────────────┬─────────────┘  └────────────────┘
               │
               ▼
┌────────────────────────────┐
│ Change Capture              │
│ logical replication / CDC    │
└──────────────┬─────────────┘
               │
               ▼
┌────────────────────────────┐
│ Invalidation Engine          │
│ dependency matching          │
│ rerun affected subscriptions │
└────────────────────────────┘

Optional:
Postgres → CDC → RisingWave/Materialize → maintained views → Gonvex
```

---

# 52. Final Summary

Gonvex should not try to clone Materialize or become a full database engine at first.

The strongest practical design is:

```txt
Postgres for source-of-truth data
Go functions for backend logic
generated TypeScript React bindings for frontend typing
WebSockets for realtime calls/subscriptions
Convex-style realtime queries by default
LiveGrid for server-side AG Grid-style tables
dependency-aware over-invalidation for correctness
rerun + diff for frontend patches
S3-compatible storage for files
optional streaming engines for heavy analytics
```

This gives the best balance of:

```txt
developer experience
SQL power
realtime UI
buildability
open infrastructure
future scalability
```
