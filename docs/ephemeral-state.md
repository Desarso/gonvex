# Ephemeral state in app functions

Gonvex app functions can store short-lived JSON state through
`ctx.Ephemeral`. The runtime scopes every operation to the executing project
and tenant; app code never supplies either namespace.

```go
type lease struct {
    UserID string `json:"userId"`
}

if err := ctx.Ephemeral.Set("presence/lease/"+userID, lease{UserID: userID}, 2*time.Minute); err != nil {
    return nil, err
}

var current lease
found, err := ctx.Ephemeral.Get("presence/lease/"+userID, &current)

entries, err := ctx.Ephemeral.List("presence/lease/")
for _, entry := range entries {
    var live lease
    if err := entry.Decode(&live); err != nil {
        return nil, err
    }
}

err = ctx.Ephemeral.Delete("presence/lease/" + userID)
```

`Set` always requires a positive TTL. Values must be JSON-serializable. Keys
and list prefixes must be valid UTF-8, are length-limited, reject control
characters, and are escaped before they become Valkey value keys.

Queries whose results include ephemeral state declare
`gonvex.ReadsEphemeral()`. This prevents the durable query-result cache from
retaining a result after its ephemeral inputs expire. Mutations that write
only ephemeral state declare `gonvex.WritesEphemeral()`; the runtime does not
open a Postgres transaction and does not publish reactive or query-cache
invalidations for those calls.

## Listing design

Each project/tenant namespace has one sorted set whose members are logical app
keys and whose scores are expiry timestamps. `Set` updates the expiring value
and the sorted-set member together. `List(prefix)` prunes expired members,
reads live members from that tenant's set, filters the logical prefix, and
fetches values with `MGET`. `Delete` removes both records.

This deliberately avoids Redis `SCAN`: listing cost is bounded by entries in
the current tenant rather than by the deployment's entire Valkey keyspace.
The individual value key TTL remains authoritative, so an entry that expires
between the sorted-set read and `MGET` is omitted and cleaned from the index.

## Required backend

Ephemeral state is shared by all runtime instances through Valkey.
`VALKEY_URL` (or `REDIS_URL`) is mandatory; the runtime pings it at startup and
fails before serving traffic if the variable is unset, invalid, or unreachable.
There is deliberately no in-process fallback because it would break TTL and
cross-instance presence semantics.

Ephemeral operations do not write Postgres, advance the tenant sync clock, or
invalidate reactive queries. Applications that display TTL-driven state over
long-lived subscriptions should refresh that query on a suitable cadence so
expiry becomes visible without turning high-frequency keepalives into global
invalidations.
