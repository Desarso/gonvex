import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import type { FunctionReference, GonvexClient } from "@gonvex/client";
import type { JsonValue } from "@gonvex/protocol";

const GonvexContext = createContext<GonvexClient | null>(null);

export const ConvexProvider = GonvexProvider;

export function GonvexProvider(props: { client: GonvexClient; children: ReactNode }) {
  return <GonvexContext.Provider value={props.client}>{props.children}</GonvexContext.Provider>;
}

export function useQuery<T = JsonValue>(ref: FunctionReference, args: JsonValue | "skip" = {}): T | undefined {
  const client = useGonvexClient();
  const [result, setResult] = useState<T>();

  useEffect(() => {
    if (args === "skip") {
      setResult(undefined);
      return;
    }
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
  return (args: JsonValue = {}) => client.mutation(ref, args);
}

export function useAction(ref: FunctionReference) {
  const client = useGonvexClient();
  return (args: JsonValue = {}) => client.action(ref, args);
}

export function useConvex() {
  return useGonvexClient();
}

export function useConvexAuth() {
  return { isLoading: false, isAuthenticated: true };
}

export function useConvexConnectionState() {
  return { hasInflightRequests: false, isWebSocketConnected: true };
}

export function usePaginatedQuery<T = JsonValue>(ref: FunctionReference, args: JsonValue | "skip" = {}, options: { initialNumItems?: number } = {}) {
  const pageArgs = args === "skip" ? "skip" : { ...(isRecord(args) ? args : { args }), paginationOpts: { numItems: options.initialNumItems ?? 25, cursor: null } };
  const result = useQuery<any>(ref, pageArgs as JsonValue | "skip");
  const page = Array.isArray(result) ? { page: result, isDone: true, continueCursor: null } : result;
  return {
    results: (page?.page ?? []) as T[],
    status: args === "skip" ? "Exhausted" : result === undefined ? "LoadingFirstPage" : page?.isDone ? "Exhausted" : "CanLoadMore",
    isLoading: args !== "skip" && result === undefined,
    loadMore: (_numItems: number) => undefined,
  };
}

function useGonvexClient() {
  const client = useContext(GonvexContext);
  if (!client) throw new Error("GonvexProvider is required");
  return client;
}

function isRecord(value: JsonValue): value is { [key: string]: JsonValue } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
