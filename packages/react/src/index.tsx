import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ButtonHTMLAttributes, type ReactNode } from "react";
import { ConvexReactClient, type FunctionReference, type GonvexClient } from "@gonvex/client";
import type { JsonValue } from "@gonvex/protocol";

const GonvexContext = createContext<GonvexClient | null>(null);
const GonvexAuthContext = createContext<AuthState>({ isLoading: false, isAuthenticated: true });

export const ConvexProvider = GonvexProvider;

export function GonvexProvider(props: { client: GonvexClient; children: ReactNode }) {
  return <GonvexContext.Provider value={props.client}>{props.children}</GonvexContext.Provider>;
}

export { ConvexReactClient };

export type AuthState = {
  isLoading: boolean;
  isAuthenticated: boolean;
  fetchAccessToken?: (args: { forceRefreshToken: boolean }) => Promise<string | null>;
};

export type GonvexAuthUser = {
  id: string;
  email?: string;
  emailVerified: boolean;
  name?: string;
  picture?: string;
  provider: "google" | string;
};

type GonvexAuthSession = {
  accessToken: string;
  expiresAt: number;
  user: GonvexAuthUser;
};

type PKCEState = { state: string; verifier: string; redirectUri: string; returnTo: string; createdAt: number };

export type GonvexAuthValue = AuthState & {
  user: GonvexAuthUser | null;
  error: string | null;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
};

export type GonvexAuthConfig = {
  runtimeUrl: string;
  projectId: string;
  callbackPath?: string;
};

const ManagedAuthContext = createContext<GonvexAuthValue | null>(null);

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

/**
 * Native Gonvex authentication. The runtime performs the one centrally
 * configured Google OAuth flow, while each app uses PKCE and receives a
 * project-scoped Gonvex session. No Firebase or Google SDK is loaded in the
 * browser.
 */
export function GonvexAuthProvider(props: GonvexAuthConfig & { client: GonvexClient; children: ReactNode }) {
  const runtimeUrl = props.runtimeUrl.replace(/\/+$/, "");
  const callbackPath = normalizeCallbackPath(props.callbackPath ?? "/auth/callback");
  const storageKey = `gonvex-auth:${props.projectId}`;
  const pkceStorageKey = `${storageKey}:pkce`;
  const [session, setSession] = useState<GonvexAuthSession | null>(() => readAuthSession(storageKey));
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const callbackStarted = useRef(false);

  const installSession = useCallback((next: GonvexAuthSession | null) => {
    if (next) {
      localStorage.setItem(storageKey, JSON.stringify(next));
      props.client.setAuth({ project: props.projectId, token: next.accessToken });
    } else {
      localStorage.removeItem(storageKey);
      props.client.setAuth({ project: props.projectId, token: undefined });
    }
    setSession(next);
  }, [props.client, props.projectId, storageKey]);

  useEffect(() => {
    let cancelled = false;
    const current = readAuthSession(storageKey);
    if (current) props.client.setAuth({ project: props.projectId, token: current.accessToken });
    else props.client.setAuth({ project: props.projectId, token: undefined });

    const url = new URL(window.location.href);
    const onCallbackPath = url.pathname === callbackPath;
    const code = onCallbackPath ? url.searchParams.get("code") : null;
    const returnedState = onCallbackPath ? url.searchParams.get("state") : null;
    const callbackError = onCallbackPath ? url.searchParams.get("error") : null;
    if ((!code && !callbackError) || callbackStarted.current) {
      setSession(current);
      setIsLoading(false);
      return () => { cancelled = true; };
    }
    callbackStarted.current = true;
    const pkce = readPKCE(pkceStorageKey);
    clearAuthCallbackParams(url, pkce?.returnTo);
    if (!pkce || !returnedState || returnedState !== pkce.state || Date.now() - pkce.createdAt > 10 * 60 * 1000) {
      sessionStorage.removeItem(pkceStorageKey);
      setError("The Google sign-in response could not be verified. Please try again.");
      setIsLoading(false);
      return () => { cancelled = true; };
    }
    if (callbackError) {
      sessionStorage.removeItem(pkceStorageKey);
      setError(callbackError === "access_denied" ? "Google sign-in was cancelled." : "Google sign-in failed. Please try again.");
      setIsLoading(false);
      return () => { cancelled = true; };
    }

    void fetch(`${runtimeUrl}/auth/token`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        grantType: "authorization_code",
        project: props.projectId,
        code,
        codeVerifier: pkce.verifier,
        redirectUri: pkce.redirectUri,
      }),
    }).then(async (response) => {
      const payload = await response.json().catch(() => ({})) as Partial<GonvexAuthSession> & { error?: string };
      if (!response.ok || !payload.accessToken || !payload.expiresAt || !payload.user) {
        throw new Error(payload.error ?? "Gonvex could not finish Google sign-in.");
      }
      if (!cancelled) installSession(payload as GonvexAuthSession);
    }).catch((cause) => {
      if (!cancelled) setError(cause instanceof Error ? cause.message : "Google sign-in failed.");
    }).finally(() => {
      sessionStorage.removeItem(pkceStorageKey);
      if (!cancelled) setIsLoading(false);
    });
    return () => { cancelled = true; };
  }, [callbackPath, installSession, pkceStorageKey, props.client, props.projectId, runtimeUrl, storageKey]);

  const signIn = useCallback(async () => {
    setError(null);
    const verifier = randomBase64Url(64);
    const state = randomBase64Url(32);
    const challengeBytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
    const challenge = bytesToBase64Url(new Uint8Array(challengeBytes));
    const redirectUri = new URL(callbackPath, window.location.origin).toString();
    const returnTo = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    sessionStorage.setItem(pkceStorageKey, JSON.stringify({ state, verifier, redirectUri, returnTo, createdAt: Date.now() }));
    const authorizeUrl = new URL(`${runtimeUrl}/auth/google/authorize`);
    authorizeUrl.searchParams.set("project", props.projectId);
    authorizeUrl.searchParams.set("redirect_uri", redirectUri);
    authorizeUrl.searchParams.set("state", state);
    authorizeUrl.searchParams.set("code_challenge", challenge);
    authorizeUrl.searchParams.set("code_challenge_method", "S256");
    window.location.assign(authorizeUrl.toString());
  }, [callbackPath, pkceStorageKey, props.projectId, runtimeUrl]);

  const signOut = useCallback(async () => {
    const token = session?.accessToken;
    installSession(null);
    setError(null);
    if (!token) return;
    await fetch(`${runtimeUrl}/auth/logout`, {
      method: "POST",
      headers: { authorization: `Bearer ${token}` },
    }).catch(() => undefined);
  }, [installSession, runtimeUrl, session?.accessToken]);

  const fetchAccessToken = useCallback(async () => {
    if (!session || session.expiresAt <= Date.now()) return null;
    return session.accessToken;
  }, [session]);

  const authValue = useMemo<GonvexAuthValue>(() => ({
    isLoading,
    isAuthenticated: Boolean(session && session.expiresAt > Date.now()),
    fetchAccessToken,
    user: session?.user ?? null,
    error,
    signIn,
    signOut,
  }), [error, fetchAccessToken, isLoading, session, signIn, signOut]);

  return (
    <ManagedAuthContext.Provider value={authValue}>
      <GonvexAuthContext.Provider value={authValue}>
        <GonvexProvider client={props.client}>{isLoading ? null : props.children}</GonvexProvider>
      </GonvexAuthContext.Provider>
    </ManagedAuthContext.Provider>
  );
}

export function useGonvexAuth(): GonvexAuthValue {
  const value = useContext(ManagedAuthContext);
  if (!value) throw new Error("GonvexAuthProvider is required");
  return value;
}

export function GonvexGoogleAuthButton(props: ButtonHTMLAttributes<HTMLButtonElement> & { signOutLabel?: string }) {
  const { signOutLabel = "Sign out", children, disabled, onClick, ...buttonProps } = props;
  const auth = useGonvexAuth();
  const label = auth.isLoading ? "Loading…" : auth.isAuthenticated ? signOutLabel : children ?? "Continue with Google";
  return (
    <button
      {...buttonProps}
      disabled={disabled || auth.isLoading}
      onClick={(event) => {
        onClick?.(event);
        if (event.defaultPrevented) return;
        void (auth.isAuthenticated ? auth.signOut() : auth.signIn());
      }}
      type={buttonProps.type ?? "button"}
    >
      {!auth.isAuthenticated && !auth.isLoading ? <GoogleMark /> : null}
      <span>{label}</span>
    </button>
  );
}

export function createGonvexAuth(config: GonvexAuthConfig) {
  function ConfiguredGonvexAuthProvider(props: { client: GonvexClient; children: ReactNode }) {
    return <GonvexAuthProvider {...config} {...props} />;
  }
  return {
    GonvexAuthProvider: ConfiguredGonvexAuthProvider,
    GoogleSignInButton: GonvexGoogleAuthButton,
    useGonvexAuth,
  };
}

function GoogleMark() {
  return (
    <svg aria-hidden="true" height="18" viewBox="0 0 18 18" width="18">
      <path fill="#4285F4" d="M17.64 9.2c0-.64-.06-1.25-.16-1.84H9v3.48h4.84a4.14 4.14 0 0 1-1.8 2.72v2.26h2.9c1.7-1.56 2.7-3.86 2.7-6.62Z" />
      <path fill="#34A853" d="M9 18c2.43 0 4.47-.8 5.96-2.18l-2.9-2.26c-.8.54-1.84.86-3.06.86-2.35 0-4.34-1.58-5.05-3.72H.96v2.34A9 9 0 0 0 9 18Z" />
      <path fill="#FBBC05" d="M3.95 10.7a5.41 5.41 0 0 1 0-3.4V4.96H.96a9 9 0 0 0 0 8.08l2.99-2.34Z" />
      <path fill="#EA4335" d="M9 3.58c1.32 0 2.5.45 3.44 1.35l2.58-2.58A8.62 8.62 0 0 0 9 0 9 9 0 0 0 .96 4.96L3.95 7.3C4.66 5.16 6.65 3.58 9 3.58Z" />
    </svg>
  );
}

function normalizeCallbackPath(value: string) {
  const path = value.trim();
  if (!path.startsWith("/") || path.startsWith("//") || path.includes("?") || path.includes("#")) {
    throw new Error("Gonvex auth callbackPath must be an absolute pathname");
  }
  return path;
}

function readAuthSession(key: string): GonvexAuthSession | null {
  if (typeof window === "undefined") return null;
  try {
    const parsed = JSON.parse(localStorage.getItem(key) ?? "null") as GonvexAuthSession | null;
    if (!parsed?.accessToken || !parsed.user?.id || parsed.expiresAt <= Date.now()) {
      localStorage.removeItem(key);
      return null;
    }
    return parsed;
  } catch {
    localStorage.removeItem(key);
    return null;
  }
}

function readPKCE(key: string): PKCEState | null {
  try {
    const parsed = JSON.parse(sessionStorage.getItem(key) ?? "null") as PKCEState | null;
    if (!parsed?.state || !parsed.verifier || !parsed.redirectUri || !parsed.returnTo || !parsed.createdAt) return null;
    return parsed;
  } catch {
    return null;
  }
}

function clearAuthCallbackParams(url: URL, returnTo?: string) {
  url.searchParams.delete("code");
  url.searchParams.delete("state");
  url.searchParams.delete("error");
  const safeReturnTo = returnTo?.startsWith("/") && !returnTo.startsWith("//") ? returnTo : url.toString();
  history.replaceState({}, "", safeReturnTo);
}

function randomBase64Url(byteLength: number) {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return bytesToBase64Url(bytes);
}

function bytesToBase64Url(bytes: Uint8Array) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
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
    setResult(undefined);
    const unsubscribeScope = client.onSessionScopeChange(() => setResult(undefined));
    const unsubscribeQuery = client.subscribeQuery({ kind, path }, args, (message) => {
      if (message.type === "query.result") {
        setResult(message.result as T);
      }
      if (message.type === "query.error") {
        setResult(undefined);
      }
    });
    return () => {
      unsubscribeScope();
      unsubscribeQuery();
    };
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
