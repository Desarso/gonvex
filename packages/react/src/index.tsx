import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { ConvexReactClient, type FunctionReference, type GonvexClient } from "@gonvex/client";
import type { JsonValue } from "@gonvex/protocol";

const GonvexContext = createContext<GonvexClient | null>(null);
const GonvexAuthContext = createContext<AuthState>({ isLoading: false, isAuthenticated: true });

export const ConvexProvider = GonvexProvider;

export function GonvexProvider(props: { client: GonvexClient; children: ReactNode }) {
  return <GonvexContext.Provider value={props.client}>{props.children}</GonvexContext.Provider>;
}

export { ConvexReactClient };

type AuthState = {
  isLoading: boolean;
  isAuthenticated: boolean;
  fetchAccessToken?: (args: { forceRefreshToken: boolean }) => Promise<string | null>;
};

export function ConvexProviderWithAuth(props: {
  client: ConvexReactClient;
  children: ReactNode;
  useAuth: () => AuthState;
}) {
  const auth = props.useAuth();
  const [tokenReady, setTokenReady] = useState(false);

  useEffect(() => {
    setTokenReady(false);
    if (auth.isLoading || !auth.isAuthenticated || !auth.fetchAccessToken) return;
    let cancelled = false;
    void auth.fetchAccessToken({ forceRefreshToken: false }).then((token) => {
      if (!cancelled) {
        props.client.setAuth({ token: token ?? undefined });
        setTokenReady(Boolean(token));
      }
    });
    return () => {
      cancelled = true;
    };
  }, [auth.isLoading, auth.isAuthenticated, auth.fetchAccessToken, props.client]);

  const authValue = useMemo<AuthState>(
    () => ({
      ...auth,
      isLoading: auth.isLoading || (auth.isAuthenticated && !tokenReady),
      isAuthenticated: auth.isAuthenticated && tokenReady,
    }),
    [auth, tokenReady],
  );

  const shouldHoldChildren = auth.isLoading || (auth.isAuthenticated && !tokenReady);

  return (
    <GonvexAuthContext.Provider value={authValue}>
      <GonvexProvider client={props.client}>{shouldHoldChildren ? null : props.children}</GonvexProvider>
    </GonvexAuthContext.Provider>
  );
}

export function useQuery<T = JsonValue>(ref: FunctionReference, args: JsonValue | "skip" = {}): T | undefined {
  const client = useGonvexClient();
  const [result, setResult] = useState<T>();
  const path = ref.path;
  const kind = ref.kind;
  const argsKey = JSON.stringify(args);

  useEffect(() => {
    if (args === "skip") {
      setResult(undefined);
      return;
    }
    return client.subscribeQuery({ kind, path }, args, (message) => {
      if (message.type === "query.result") {
        setResult(message.result as T);
      }
    });
  }, [client, kind, path, argsKey]);

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
  return useContext(GonvexAuthContext);
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
