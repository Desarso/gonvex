import { StrictMode, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { GonvexClient, IndexedDBLocalReplicaStorage } from "@gonvex/client";
import { GonvexProvider, useLiveQuery } from "@gonvex/react";

declare global {
  interface Window {
    __gonvexValues: string[];
    __gonvexClient: GonvexClient;
  }
}

const pageURL = new URL(window.location.href);
const socketURL = new URL("ws://127.0.0.1:4180/ws");
socketURL.searchParams.set("channel", pageURL.searchParams.get("channel") ?? "default");
socketURL.searchParams.set("scope", pageURL.searchParams.get("scope") ?? "a".repeat(64));

window.__gonvexValues = [];
const client = new GonvexClient(socketURL.toString(), {
  identity: { sub: `browser-${pageURL.searchParams.get("scope") ?? "default"}` },
  localReplica: { storage: new IndexedDBLocalReplicaStorage("gonvex-local-replica-e2e") },
});
window.__gonvexClient = client;

const demoQuery = {
  kind: "query",
  path: "replica.demo",
  delivery: "live",
  live: { entity: "demo", key: "id", resultPath: [], plan: { table: "demo", key: "id", columns: ["id", "value"] } },
} as const;

function ReplicaDemo() {
  const result = useLiveQuery<Array<{ id: string; value: string }>>(demoQuery, { view: "main" });
  const value = result?.[0]?.value;

  useEffect(() => {
    if (value && window.__gonvexValues.at(-1) !== value) {
      window.__gonvexValues.push(value);
    }
  }, [value]);

  return (
    <main>
      <h1>Gonvex Local Replica</h1>
      <output data-testid="query-value">{value ?? "loading"}</output>
      <pre data-testid="query-history">{JSON.stringify(window.__gonvexValues)}</pre>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <GonvexProvider client={client}>
      <ReplicaDemo />
    </GonvexProvider>
  </StrictMode>,
);
