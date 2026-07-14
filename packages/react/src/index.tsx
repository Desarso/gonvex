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

export type GonvexAuthTenant = {
  id: string;
  name: string;
  role: "owner" | "admin" | "member" | "viewer" | string;
  permissions?: Record<string, unknown>;
};

type GonvexAuthSession = {
  accessToken: string;
  expiresAt: number;
  refreshToken: string;
  refreshExpiresAt: number;
  user: GonvexAuthUser;
  tenants: GonvexAuthTenant[];
  activeTenantId?: string;
};

type PKCEState = { state: string; verifier: string; redirectUri: string; returnTo: string; createdAt: number };

class GonvexAuthRequestError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "GonvexAuthRequestError";
    this.status = status;
  }
}

export type GonvexAuthValue = AuthState & {
  user: GonvexAuthUser | null;
  tenants: GonvexAuthTenant[];
  activeTenant: GonvexAuthTenant | null;
  error: string | null;
  signIn: () => Promise<void>;
  signOut: (options?: { allDevices?: boolean }) => Promise<void>;
  setActiveTenant: (tenantId: string) => Promise<void>;
  refreshMemberships: () => Promise<GonvexAuthTenant[]>;
  createTenant: (name: string) => Promise<GonvexAuthTenant>;
  inviteMember: (tenantId: string, email: string, options?: { role?: GonvexAuthTenant["role"]; permissions?: Record<string, unknown> }) => Promise<void>;
  revokeInvitation: (tenantId: string, email: string) => Promise<void>;
  removeMember: (tenantId: string, userId: string) => Promise<void>;
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
  const callbackPath = normalizeCallbackPath(props.callbackPath ?? "/");
  const storageKey = `gonvex-auth:${encodeURIComponent(runtimeUrl)}:${props.projectId}`;
  const pkceStorageKey = `${storageKey}:pkce`;
  const [session, setSession] = useState<GonvexAuthSession | null>(() => readAuthSession(storageKey));
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshRetryAt, setRefreshRetryAt] = useState(0);
  const sessionRef = useRef(session);
  const bootstrapRef = useRef<Promise<GonvexAuthSession | null> | null>(null);
  const refreshRef = useRef<Promise<GonvexAuthSession | null> | null>(null);

  const installSession = useCallback((next: GonvexAuthSession | null, persist = true) => {
    sessionRef.current = next;
    if (next) {
      if (persist) safeLocalStorageSet(storageKey, JSON.stringify(next));
      props.client.setAuth({ project: props.projectId, tenant: next.activeTenantId, token: next.accessToken });
    } else {
      if (persist) safeLocalStorageRemove(storageKey);
      props.client.setAuth({ project: props.projectId, tenant: undefined, token: undefined });
    }
    setSession(next);
  }, [props.client, props.projectId, storageKey]);

  useEffect(() => {
    let cancelled = false;
    if (!bootstrapRef.current) {
      bootstrapRef.current = bootstrapGonvexAuth({ callbackPath, pkceStorageKey, projectId: props.projectId, runtimeUrl, storageKey });
    }
    void bootstrapRef.current.then((next) => {
      if (!cancelled) installSession(next);
    }).catch((cause) => {
      if (!cancelled) {
        installSession(null);
        setError(cause instanceof Error ? cause.message : "Google sign-in failed.");
      }
    }).finally(() => {
      safeSessionStorageRemove(pkceStorageKey);
      if (!cancelled) setIsLoading(false);
    });
    return () => { cancelled = true; };
  }, [callbackPath, installSession, pkceStorageKey, props.projectId, runtimeUrl, storageKey]);

  const refreshSession = useCallback(async (force = false) => {
    if (refreshRef.current) return refreshRef.current;
    let attemptedRefreshToken = "";
    const request = withBrowserAuthLock(`${storageKey}:refresh`, async () => {
      const current = readAuthSession(storageKey) ?? sessionRef.current;
      if (!current || current.refreshExpiresAt <= Date.now()) return null;
      if (!force && current.expiresAt > Date.now() + 60_000) return current;
      attemptedRefreshToken = current.refreshToken;
      const next = await requestGonvexAuthToken(runtimeUrl, {
        grantType: "refresh_token",
        project: props.projectId,
        refreshToken: current.refreshToken,
        tenant: current.activeTenantId,
      });
      // Persist the rotated token before releasing the cross-tab lock. The
      // next waiter must never read and reuse the just-consumed refresh token.
      safeLocalStorageSet(storageKey, JSON.stringify(next));
      setRefreshRetryAt(0);
      setError(null);
      return next;
    }).then((next) => {
      if (next) {
        setRefreshRetryAt(0);
        setError(null);
      }
      installSession(next);
      return next;
    }).catch((cause) => {
      const latest = readAuthSession(storageKey);
      if (latest && attemptedRefreshToken && latest.refreshToken !== attemptedRefreshToken) {
        // Another tab completed the one permitted rotation while this request
        // was in flight. Adopt its winner instead of erasing shared auth state.
        installSession(latest);
        setRefreshRetryAt(0);
        setError(null);
        return latest;
      }
      if (isFatalRefreshError(cause)) {
        installSession(null);
        setRefreshRetryAt(0);
        setError(cause instanceof Error ? cause.message : "Your session expired. Please sign in again.");
        return null;
      }
      // Network failures, timeouts, rate limits, and server outages must not
      // destroy a valid refresh credential. Keep it and retry shortly.
      if (sessionRef.current) setRefreshRetryAt(Date.now() + 5_000);
      setError("Gonvex could not refresh your session. Retrying shortly…");
      return null;
    }).finally(() => {
      refreshRef.current = null;
    });
    refreshRef.current = request;
    return request;
  }, [installSession, props.projectId, runtimeUrl, storageKey]);

  useEffect(() => {
    if (!session) return;
    const scheduledAt = Math.max(session.expiresAt - 60_000, refreshRetryAt);
    const delay = Math.max(0, scheduledAt - Date.now());
    const timeout = window.setTimeout(() => { void refreshSession(); }, delay);
    return () => window.clearTimeout(timeout);
  }, [refreshRetryAt, refreshSession, session]);

  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key !== storageKey) return;
      installSession(readAuthSession(storageKey), false);
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, [installSession, storageKey]);

  const signIn = useCallback(async () => {
    setError(null);
    const verifier = randomBase64Url(64);
    const state = randomBase64Url(32);
    const challengeBytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
    const challenge = bytesToBase64Url(new Uint8Array(challengeBytes));
    const redirectUri = new URL(callbackPath, window.location.origin).toString();
    const returnTo = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    safeSessionStorageSet(pkceStorageKey, JSON.stringify({ state, verifier, redirectUri, returnTo, createdAt: Date.now() }));
    const authorizeUrl = new URL(`${runtimeUrl}/auth/google/authorize`);
    authorizeUrl.searchParams.set("project", props.projectId);
    authorizeUrl.searchParams.set("redirect_uri", redirectUri);
    authorizeUrl.searchParams.set("state", state);
    authorizeUrl.searchParams.set("code_challenge", challenge);
    authorizeUrl.searchParams.set("code_challenge_method", "S256");
    window.location.assign(authorizeUrl.toString());
  }, [callbackPath, pkceStorageKey, props.projectId, runtimeUrl]);

  const signOut = useCallback(async (options?: { allDevices?: boolean }) => {
    const current = sessionRef.current;
    installSession(null);
    setError(null);
    if (!current) return;
    await fetch(`${runtimeUrl}/auth/logout`, {
      method: "POST",
      headers: { authorization: `Bearer ${current.accessToken}`, "content-type": "application/json" },
      body: JSON.stringify({ refreshToken: current.refreshToken, all: options?.allDevices === true }),
    }).catch(() => undefined);
  }, [installSession, runtimeUrl]);

  const fetchAccessToken = useCallback(async (args: { forceRefreshToken: boolean }) => {
    const current = sessionRef.current;
    if (!current) return null;
    if (!args.forceRefreshToken && current.expiresAt > Date.now() + 60_000) return current.accessToken;
    return (await refreshSession(args.forceRefreshToken))?.accessToken ?? null;
  }, [refreshSession]);

  const setActiveTenant = useCallback(async (tenantId: string) => {
    const current = sessionRef.current;
    if (!current || !current.tenants.some((tenant) => tenant.id === tenantId)) {
      throw new Error(`Your account does not have access to tenant ${tenantId}.`);
    }
    installSession({ ...current, activeTenantId: tenantId });
  }, [installSession]);

  const refreshMemberships = useCallback(async () => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before loading tenant memberships.");
    const current = sessionRef.current!;
    const response = await fetch(`${runtimeUrl}/auth/me`, {
      headers: { authorization: `Bearer ${token}`, ...(current.activeTenantId ? { "x-gonvex-tenant-id": current.activeTenantId } : {}) },
    });
    const payload = await response.json().catch(() => ({})) as { error?: string; user?: GonvexAuthUser; tenants?: GonvexAuthTenant[]; activeTenantId?: string };
    if (!response.ok || !payload.user || !payload.tenants) throw new Error(payload.error ?? "Could not load tenant memberships.");
    installSession({ ...current, user: payload.user, tenants: payload.tenants, activeTenantId: payload.activeTenantId });
    return payload.tenants;
  }, [fetchAccessToken, installSession, runtimeUrl]);

  const createTenant = useCallback(async (name: string) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before creating a tenant.");
    const response = await fetch(`${runtimeUrl}/auth/tenants`, {
      method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({ name }),
    });
    const payload = await response.json().catch(() => ({})) as { error?: string; tenant?: GonvexAuthTenant };
    if (!response.ok || !payload.tenant) throw new Error(payload.error ?? "Could not create the tenant.");
    const current = sessionRef.current!;
    installSession({ ...current, tenants: [...current.tenants.filter((tenant) => tenant.id !== payload.tenant!.id), payload.tenant], activeTenantId: payload.tenant.id });
    return payload.tenant;
  }, [fetchAccessToken, installSession, runtimeUrl]);

  const inviteMember = useCallback(async (tenantId: string, email: string, options?: { role?: GonvexAuthTenant["role"]; permissions?: Record<string, unknown> }) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before inviting a member.");
    const response = await fetch(`${runtimeUrl}/auth/tenants/${encodeURIComponent(tenantId)}/members`, {
      method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({ email, role: options?.role ?? "member", permissions: options?.permissions ?? {} }),
    });
    const payload = await response.json().catch(() => ({})) as { error?: string };
    if (!response.ok) throw new Error(payload.error ?? "Could not invite the member.");
  }, [fetchAccessToken, runtimeUrl]);

  const removeMember = useCallback(async (tenantId: string, userId: string) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before removing a member.");
    const response = await fetch(`${runtimeUrl}/auth/tenants/${encodeURIComponent(tenantId)}/members/${encodeURIComponent(userId)}`, {
      method: "DELETE", headers: { authorization: `Bearer ${token}` },
    });
    const payload = await response.json().catch(() => ({})) as { error?: string };
    if (!response.ok) throw new Error(payload.error ?? "Could not remove the member.");
  }, [fetchAccessToken, runtimeUrl]);

  const revokeInvitation = useCallback(async (tenantId: string, email: string) => {
    const token = await fetchAccessToken({ forceRefreshToken: false });
    if (!token) throw new Error("Sign in before revoking an invitation.");
    const response = await fetch(`${runtimeUrl}/auth/tenants/${encodeURIComponent(tenantId)}/invitations/${encodeURIComponent(email)}`, {
      method: "DELETE", headers: { authorization: `Bearer ${token}` },
    });
    const payload = await response.json().catch(() => ({})) as { error?: string };
    if (!response.ok) throw new Error(payload.error ?? "Could not revoke the invitation.");
  }, [fetchAccessToken, runtimeUrl]);

  const activeTenant = session?.tenants.find((tenant) => tenant.id === session.activeTenantId) ?? null;

  const authValue = useMemo<GonvexAuthValue>(() => ({
    isLoading,
    isAuthenticated: Boolean(session && session.refreshExpiresAt > Date.now()),
    fetchAccessToken,
    user: session?.user ?? null,
    tenants: session?.tenants ?? [],
    activeTenant,
    error,
    signIn,
    signOut,
    setActiveTenant,
    refreshMemberships,
    createTenant,
    inviteMember,
    revokeInvitation,
    removeMember,
  }), [activeTenant, createTenant, error, fetchAccessToken, inviteMember, isLoading, refreshMemberships, removeMember, revokeInvitation, session, setActiveTenant, signIn, signOut]);

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

async function bootstrapGonvexAuth(options: {
  callbackPath: string;
  pkceStorageKey: string;
  projectId: string;
  runtimeUrl: string;
  storageKey: string;
}): Promise<GonvexAuthSession | null> {
  const current = readAuthSession(options.storageKey);
  const url = new URL(window.location.href);
  const onCallbackPath = url.pathname === options.callbackPath;
  const code = onCallbackPath ? url.searchParams.get("code") : null;
  const returnedState = onCallbackPath ? url.searchParams.get("state") : null;
  const callbackError = onCallbackPath ? url.searchParams.get("error") : null;
  if (!code && !callbackError) {
    if (!current) return null;
    if (current.expiresAt > Date.now() + 30_000) return current;
    try {
      return await withBrowserAuthLock(`${options.storageKey}:refresh`, async () => {
        const latest = readAuthSession(options.storageKey) ?? current;
        if (latest.expiresAt > Date.now() + 30_000) return latest;
        if (latest.refreshExpiresAt <= Date.now()) throw new GonvexAuthRequestError("Your session expired. Please sign in again.", 401);
        const next = await requestGonvexAuthToken(options.runtimeUrl, {
          grantType: "refresh_token", project: options.projectId,
          refreshToken: latest.refreshToken, tenant: latest.activeTenantId,
        });
        safeLocalStorageSet(options.storageKey, JSON.stringify(next));
        return next;
      });
    } catch (cause) {
      if (!isFatalRefreshError(cause)) return readAuthSession(options.storageKey) ?? current;
      throw cause;
    }
  }

  const pkce = readPKCE(options.pkceStorageKey);
  clearAuthCallbackParams(url, pkce?.returnTo);
  if (!pkce || !returnedState || returnedState !== pkce.state || Date.now() - pkce.createdAt > 10 * 60 * 1000) {
    throw new Error("The Google sign-in response could not be verified. Please try again.");
  }
  if (callbackError) {
    const messages: Record<string, string> = {
      access_denied: "Google sign-in was cancelled.",
      invitation_required: "This app is invite-only. Ask an administrator to invite your verified Google email.",
      verified_google_email_required: "Google must provide a verified email address for this app.",
      membership_setup_failed: "Your account was verified, but its workspace could not be prepared. Please try again.",
    };
    throw new Error(messages[callbackError] ?? "Google sign-in failed. Please try again.");
  }
  return requestGonvexAuthToken(options.runtimeUrl, {
    grantType: "authorization_code", project: options.projectId, code,
    codeVerifier: pkce.verifier, redirectUri: pkce.redirectUri,
  });
}

async function requestGonvexAuthToken(runtimeUrl: string, body: Record<string, unknown>): Promise<GonvexAuthSession> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 15_000);
  try {
    const response = await fetch(`${runtimeUrl}/auth/token`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({})) as Partial<GonvexAuthSession> & { error?: string };
    if (!response.ok || !isGonvexAuthSession(payload)) {
      throw new GonvexAuthRequestError(payload.error ?? "Gonvex could not finish sign-in.", response.ok ? 502 : response.status);
    }
    return payload;
  } catch (cause) {
    if (controller.signal.aborted) throw new Error("Gonvex did not finish sign-in in time. Please try again.");
    throw cause;
  } finally {
    window.clearTimeout(timeout);
  }
}

function isFatalRefreshError(cause: unknown) {
  return cause instanceof GonvexAuthRequestError && (cause.status === 400 || cause.status === 401 || cause.status === 403);
}

function isGonvexAuthSession(value: Partial<GonvexAuthSession>): value is GonvexAuthSession {
  return Boolean(
    value.accessToken && value.expiresAt && value.refreshToken && value.refreshExpiresAt
    && value.user?.id && Array.isArray(value.tenants),
  );
}

async function withBrowserAuthLock<T>(name: string, action: () => Promise<T>): Promise<T> {
  const locks = typeof navigator === "undefined"
    ? undefined
    : (navigator as Navigator & { locks?: { request: <R>(name: string, callback: () => Promise<R>) => Promise<R> } }).locks;
  return locks ? locks.request(name, action) : withLocalStorageAuthLock(name, action);
}

async function withLocalStorageAuthLock<T>(name: string, action: () => Promise<T>): Promise<T> {
  const key = `${name}:lease`;
  const owner = randomBase64Url(16);
  const deadline = Date.now() + 25_000;
  while (Date.now() < deadline) {
    try {
      const current = JSON.parse(localStorage.getItem(key) ?? "null") as { owner?: string; expiresAt?: number } | null;
      if (!current?.owner || Number(current.expiresAt) <= Date.now()) {
        localStorage.setItem(key, JSON.stringify({ owner, expiresAt: Date.now() + 20_000 }));
        const claimed = JSON.parse(localStorage.getItem(key) ?? "null") as { owner?: string } | null;
        if (claimed?.owner === owner) {
          try {
            return await action();
          } finally {
            const latest = JSON.parse(localStorage.getItem(key) ?? "null") as { owner?: string } | null;
            if (latest?.owner === owner) localStorage.removeItem(key);
          }
        }
      }
    } catch {
      return action();
    }
    await new Promise((resolve) => setTimeout(resolve, 30 + Math.floor(Math.random() * 40)));
  }
  throw new Error("Another tab is refreshing this session. Please try again.");
}

function safeLocalStorageSet(key: string, value: string) {
  try { localStorage.setItem(key, value); } catch { /* storage can be unavailable in hardened browsers */ }
}

function safeLocalStorageRemove(key: string) {
  try { localStorage.removeItem(key); } catch { /* storage can be unavailable in hardened browsers */ }
}

function safeSessionStorageSet(key: string, value: string) {
  try { sessionStorage.setItem(key, value); } catch { /* reported by the missing-state check after redirect */ }
}

function safeSessionStorageRemove(key: string) {
  try { sessionStorage.removeItem(key); } catch { /* nothing else to clean */ }
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
    if (!parsed?.accessToken || !parsed.refreshToken || !parsed.user?.id || !Array.isArray(parsed.tenants) || parsed.refreshExpiresAt <= Date.now()) {
      safeLocalStorageRemove(key);
      return null;
    }
    return parsed;
  } catch {
    safeLocalStorageRemove(key);
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
