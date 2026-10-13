# Cached startup

`restoreSession(auth): Promise<boolean>`, `localSessionReady`,
`localSessionState()` and `onLocalSessionChange(listener)` expose local
readability independently of server acceptance. States are `unavailable`,
`restored` and `authenticated`. A true local-ready result means the matching
replica and durable outbox have finished activation. It does not grant server
access. `authenticate()` keeps its server-only contract.

## Storage and startup

The audit against staging 95da6c9 found that saved-session persistence and
restoration required a local reducer runtime. Read-only replica clients now
persist and restore sessions when `localReplica.storage` is configured. Clients
without replica storage never restore a replica. Existing read-only clients
need one successful online authentication with this SDK to save a directive.

Empty startup and quarantined partitions activate without storage reads. The
focused test observes one `loadWorkingSet` call for the saved visibility scope,
with no second load when server authentication accepts the same scope. The old
two-pass count comes from the audited code path. No timing or battery benchmark
was run.

## Auth failures

Network failure, token-fetch failure, missing or unchanged refresh tokens, and
socket closure preserve cached sessions and rows. Membership-change events still
invalidate the old visibility partition while the server verifies new permissions.
The client retries through its existing reconnect backoff. Server operations and
outbox replay still require accepted server authentication.

`onAuthError((message, details) => ...)` supplies optional rejection details.
Only `details?.definitive` authorizes signing out or deleting an account session.
A server rejection followed by a fresh forced token rejected with
`credential_rejected` is terminal. Uncoded legacy errors preserve cached access
because they cannot distinguish credential rejection from infrastructure failure.
Explicit `revoked`, `member_removed` or `account_disabled` codes are also terminal.
Explicit app sign-out still clears the saved directive and immediately hides its rows.

The runtime sends an optional `code` on `auth.error`. Database credential
rejections use `credential_rejected`, missing membership uses `member_removed`,
and infrastructure failures use `transient`. Its session lookup combines invalid,
expired, revoked and disabled credentials into one error, so those cannot safely
be labeled revoked without refreshing first. Session revalidation errors also
remain transient because the existing runtime check does not distinguish
expiry from database failure.

Generic integration:

```ts
client.onAuthError((_message, details) => {
  if (details?.definitive) clearApplicationSession();
});
const ready = await client.restoreSession({
  project, tenant, identity: { sub: accountId, iss: project }, fetchToken,
});
if (ready) showCachedApplication();
```

Keep developer-grant recovery separate from account sign-out. Account or tenant
changes must invalidate prior application state immediately.

## Compatibility and limits

The auth error code is optional. Old clients ignore it and new clients accept
legacy frames. No capabilities negotiation is required for an optional response
field. No storage schema, package version or client-contract changes are made.
Durable intents remain isolated and retained after sign-out or rejection.

Warm React admission requires a current access token with the existing 30-second
safety window and an unexpired refresh credential. Expired-token bootstrap still
follows refresh policy. Opaque tokens require a canonical account hint. Offline
visibility uses the last accepted server scope until rejection can be learned.

Targeted tests cover cached offline reads, transient token failures, repeated
socket closure, accepted forced refresh, definitive rejection, scope isolation,
explicit sign-out, stale hydration, actual React rendering and durable replay.
