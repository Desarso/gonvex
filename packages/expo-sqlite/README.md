# @gonvex/expo-sqlite

Transactional Expo SQLite persistence for the Gonvex Local Replica.

```ts
import { openDatabaseAsync } from "expo-sqlite";
import { expoSQLite } from "@gonvex/expo-sqlite";

const database = await openDatabaseAsync("gonvex.db");
const client = new GonvexClient(url, {
  localReplica: { storage: expoSQLite(database) },
});
```
