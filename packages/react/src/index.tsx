import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import type { FunctionReference, GonvexClient } from "@gonvex/client";
import type { JsonValue } from "@gonvex/protocol";

const GonvexContext = createContext<GonvexClient | null>(null);

export function GonvexProvider(props: { client: GonvexClient; children: ReactNode }) {
  return <GonvexContext.Provider value={props.client}>{props.children}</GonvexContext.Provider>;
}

export function useQuery<T = JsonValue>(ref: FunctionReference, args: JsonValue): T | undefined {
  const client = useGonvexClient();
  const [result, setResult] = useState<T>();

  useEffect(() => {
    return client.subscribeQuery(ref, args, (message) => {
      if (message.type === "query.result") {
        setResult(message.result as T);
      }
    });
  }, [client, ref, JSON.stringify(args)]);

  return result;
}

export function useMutation(ref: FunctionReference) {
  const client = useGonvexClient();
  return (args: JsonValue) => client.mutation(ref, args);
}

function useGonvexClient() {
  const client = useContext(GonvexContext);
  if (!client) throw new Error("GonvexProvider is required");
  return client;
}
