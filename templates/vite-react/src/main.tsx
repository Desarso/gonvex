import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { GonvexClient } from "../gonvex/_generated/client";
import { GonvexProvider } from "../gonvex/_generated/react";
import App from "./App";
import "./styles.css";

const gonvexURL = import.meta.env.VITE_GONVEX_WS_URL ?? "ws://localhost:8080/ws";
// The runtime resolves functions per project, so the client must announce the
// same project id that `gonvex dev` syncs to. Without it the runtime answers
// every query with "is not implemented by the runtime".
const projectID = import.meta.env.VITE_GONVEX_PROJECT_ID;
const gonvex = new GonvexClient(gonvexURL, projectID ? { project: projectID } : {});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <GonvexProvider client={gonvex}>
      <App runtimeURL={gonvexURL} />
    </GonvexProvider>
  </StrictMode>,
);
