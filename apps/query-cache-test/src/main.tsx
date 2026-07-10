import { StrictMode, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { GonvexClient } from "@gonvex/client";
import { GonvexProvider, useQuery } from "@gonvex/react";

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
const client = new GonvexClient(socketURL.toString());
window.__gonvexClient = client;

const demoQuery = { kind: "query", path: "cache.demo" } as const;

function CacheDemo() {
  const result = useQuery<{ value: string }>(demoQuery, { view: "main" });
  const value = result?.value;

  useEffect(() => {
    if (value && window.__gonvexValues.at(-1) !== value) {
      window.__gonvexValues.push(value);
    }
  }, [value]);

  return (
    <main>
      <h1>Gonvex query cache</h1>
      <output data-testid="query-value">{value ?? "loading"}</output>
      <pre data-testid="query-history">{JSON.stringify(window.__gonvexValues)}</pre>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <GonvexProvider client={client}>
      <CacheDemo />
    </GonvexProvider>
  </StrictMode>,
);
