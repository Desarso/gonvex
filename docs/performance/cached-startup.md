# Cached startup handoff

The SDK now exposes `restoreSession(auth): Promise<boolean>`, `localSessionReady`,
`localSessionState()` and `onLocalSessionChange(listener)`. The states are
`unavailable`, `restored` and `authenticated`. A true local-ready result means the
matching replica and durable outbox have finished activation. It does not grant
server access. `authenticate()` keeps its existing server-only contract.

## Audit findings

Against staging 95da6c9, `packages/client/src/index.ts` restored saved directives
at lines 1046–1055 but required `localBinding`. It also required that binding to
save a session after `auth.result`. Whagons' `GonvexClientProvider.tsx` constructs
its app client without `localRuntime`, so the audit's assumption that this client
could already hydrate offline was incomplete. The SDK now persists and restores
read-only replicas too. Previously deployed read-only clients must complete one
successful online auth with this SDK before they have a saved directive.

The constructor at lines 772–774 loaded an empty placeholder before the real
visibility partition. Empty startup and quarantined partitions now activate
without storage reads. The focused test observes exactly one `loadWorkingSet`
call, for the saved visibility scope, and no second load for same-scope accepted
auth. The old two-pass count comes from the audited code path, not a timing
benchmark. No latency or battery benchmark was run.

Whagons' `DeveloperSession.restore()` awaited token fetching and server-only
`authenticate()`, and `GonvexSessionProvider.tsx:105` held children for both
`pending` and `!ready`. React previously reused warm external sessions only.
Native sessions can now also render while reconnecting; `isLocalSessionReady`
reports actual replica availability. Terminal account-session rejection signs
out and invalidates pending bootstrap/refresh work.

## Exact Whagons change

Apply [whagons-cached-startup.patch](./whagons-cached-startup.patch) in
`Whagons-International/whagons5-client` after the SDK release. The patch was
produced against the two files in `/home/gabriel/.t3/worktrees/whagons5-client/t3-fe644a9e`.
No files in that worktree were edited. The patch applies cleanly to copies of
those sources. It has not been built or run in the Whagons app.

The patch starts local restore with the canonical account and tenant hint before
awaiting a token. It publishes `ready: true, reconnecting: true` on successful
local activation, while server `authenticate()` continues in the background.
The provider allows children during that specific pending restore and exposes
`sessionState: 'reconnecting'`. Account/tenant changes immediately clear the old
application principal and invalidate stale transitions. Terminal own-account
rejection clears both the app and native session. A rejected developer grant
still recovers the original account.

`GonvexClientProvider.tsx` needs no change. Its existing replica storage and outbox
configuration are sufficient. No local reducer runtime or client-contract bump
is needed. No cached admission is added to developer entry, impersonation,
membership changes, or grant recovery.

After applying the patch, the app should verify: offline reload after accepted
online auth, cached shell mounting while token fetching stalls, runtime rejection
routing to sign-in, account/tenant changes during pending token fetch or server
auth, and developer-grant rejection recovering the actor. The patch deliberately
forgets the prior saved directive when clearing the principal on account/tenant
changes; returning to that scope requires accepted online auth to save it again.

## Validation

- `pnpm install --frozen-lockfile` succeeded. CLI-bin warnings occurred because
  the unrelated CLI package was not built.
- `pnpm --filter @gonvex/client exec vitest run src/cached-startup.test.ts src/local-reducers.test.ts` passed, 70 tests.
- `pnpm --filter @gonvex/react exec vitest run src/index.test.tsx src/cached-startup.test.tsx` passed, 78 tests.
- Client and React package typechecks and builds passed.
- Protocol, module-sdk and local-runtime builds passed to supply dependency artifacts.
- `git diff --check` and applying the Whagons patch with `git apply --check` to source copies passed.

The tests cover cold online auth, cached offline reads, actual React rendering
with the real client, server rejection clearing the provider and replica,
account/tenant isolation, conflicting JWT/hint accounts, sign-out, delayed
hydration after rejection, refresh-callback installation during accepted auth,
and durable local reducer admission/replay. No full suites or Rust tests ran.

## Compatibility and limits

There are no wire changes, capabilities, runtime changes, storage-schema changes,
package-version changes, or client-contract changes. Old clients and runtimes
keep their existing protocol behavior. Durable queued intents remain scoped and
are retained on sign-out/rejection; server replay still requires accepted auth.

Warm React admission still requires a current access token with the existing
30-second safety window and an unexpired refresh credential. An expired-token
bootstrap can still wait for refresh. Opaque tokens depend on the caller's
canonical account hint, since they cannot be decoded client-side. Offline
visibility uses the last accepted server scope until a rejection can be learned
from the server. This preserves the existing offline model.

The PR is for review only. Nothing was merged and no npm packages were published.
