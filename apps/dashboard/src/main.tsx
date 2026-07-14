import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "@heroui/styles/css";
import "@xyflow/react/dist/style.css";
import "./App.css";
import { App } from "./App";
import { GonvexClient } from "@gonvex/client";
import { GonvexAuthProvider, useGonvexAuth } from "@gonvex/react";

const authProjectID = String(import.meta.env.VITE_GONVEX_DASHBOARD_AUTH_PROJECT_ID ?? "").trim();
const authRuntimeURL = String(import.meta.env.VITE_GONVEX_URL ?? import.meta.env.VITE_GONVEX_RUNTIME_URL ?? "http://localhost:8080").trim().replace(/\/+$/, "");
const authClient = authProjectID ? new GonvexClient(authWebSocketURL(authRuntimeURL), { project: authProjectID, telemetry: false }) : null;

function authWebSocketURL(runtimeURL: string): string {
  const url = new URL("/ws", runtimeURL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

function NativeDashboardApp() {
  return <App nativeAuth={useGonvexAuth()} />;
}

function DashboardRoot() {
  if (!authProjectID || !authClient) return <App />;
  return (
    <GonvexAuthProvider callbackPath="/login" client={authClient} projectId={authProjectID} runtimeUrl={authRuntimeURL}>
      <NativeDashboardApp />
    </GonvexAuthProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <DashboardRoot />
  </StrictMode>,
);
