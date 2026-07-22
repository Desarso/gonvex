import { act, render, renderHook } from "@testing-library/react";
import { Component, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionState, FunctionReference, GonvexClient } from "@gonvex/client";
import type { ServerMessage } from "@gonvex/protocol";
import { GonvexProvider, useConvexConnectionState, useMutation, useQuery, useQueryResult } from "./index";

const ref: FunctionReference = { kind: "query", path: "tasks.list" };

class FakeGonvexClient {
  readonly queryListeners = new Set<(message: ServerMessage) => void>();
  readonly scopeHandlers = new Set<() => void>();
  readonly connectionHandlers = new Set<(state: ConnectionState) => void>();
  readonly retryQuery = vi.fn();
  readonly mutation = vi.fn(() => Promise.resolve(null));
  readonly action = vi.fn(() => Promise.resolve(null));
  subscribedArgs: unknown[] = [];
  state: ConnectionState = {
    isWebSocketConnected: true,
    hasEverConnected: true,
    connectionCount: 1,
    connectionRetries: 0,
    hasInflightRequests: false,
    inflightMutations: 0,
    inflightActions: 0,
    inflightOneShotQueries: 0,
  };

  subscribeQuery(_ref: FunctionReference, args: unknown, handler: (message: ServerMessage) => void) {
    this.subscribedArgs.push(args);
    this.queryListeners.add(handler);
    return () => {
      this.queryListeners.delete(handler);
    };
  }

  onSessionScopeChange(handler: () => void) {
    this.scopeHandlers.add(handler);
    return () => {
      this.scopeHandlers.delete(handler);
    };
  }

  connectionState(): ConnectionState {
    return this.state;
  }

  subscribeToConnectionState(handler: (state: ConnectionState) => void) {
    this.connectionHandlers.add(handler);
    return () => {
      this.connectionHandlers.delete(handler);
    };
  }

  emitQuery(message: ServerMessage) {
    for (const handler of Array.from(this.queryListeners)) handler(message);
  }

  setConnected(isWebSocketConnected: boolean) {
    this.state = { ...this.state, isWebSocketConnected };
    for (const handler of Array.from(this.connectionHandlers)) handler(this.state);
  }
}

function wrapperFor(client: FakeGonvexClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <GonvexProvider client={client as unknown as GonvexClient}>{children}</GonvexProvider>;
  };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("useQueryResult", () => {
  it("moves from loading to success when a result arrives", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, { status: "open" }), { wrapper: wrapperFor(client) });

    expect(result.current).toMatchObject({ status: "loading", isLoading: true, data: undefined });

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));

    expect(result.current).toMatchObject({ status: "success", isSuccess: true, data: ["task"], error: null, isStale: false });
  });

  it("reports skip status for skipped queries", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult(ref, "skip"), { wrapper: wrapperFor(client) });

    expect(result.current.status).toBe("skip");
    expect(client.queryListeners.size).toBe(0);
  });

  it("surfaces server query errors and recovers through retry", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, { status: "open" }), { wrapper: wrapperFor(client) });

    act(() => client.emitQuery({ type: "query.error", id: "q1", error: "permission denied" }));

    expect(result.current).toMatchObject({ status: "error", isError: true });
    expect(result.current.error?.message).toBe("permission denied");

    act(() => result.current.retry());

    expect(result.current).toMatchObject({ status: "loading", error: null });
    expect(client.retryQuery).toHaveBeenCalledWith({ kind: "query", path: "tasks.list" }, { status: "open" });

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["recovered"] }));
    expect(result.current).toMatchObject({ status: "success", data: ["recovered"] });
  });

  it("keeps last good data while erroring by default", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    act(() => client.emitQuery({ type: "query.error", id: "q1", error: "boom" }));

    expect(result.current).toMatchObject({ status: "error", data: ["task"], isStale: true });
  });

  it("drops last good data on error when keepPreviousData is false", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(
      () => useQueryResult<string[]>(ref, {}, { keepPreviousData: false }),
      { wrapper: wrapperFor(client) },
    );

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    act(() => client.emitQuery({ type: "query.error", id: "q1", error: "boom" }));

    expect(result.current).toMatchObject({ status: "error", data: undefined, isStale: false });
  });

  it("marks disconnected with stale data on socket drop and reloads on reconnect", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    act(() => client.setConnected(false));

    expect(result.current).toMatchObject({ status: "disconnected", isError: true, data: ["task"], isStale: true });

    act(() => client.setConnected(true));
    expect(result.current).toMatchObject({ status: "loading", data: ["task"], isStale: true });

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["fresh"] }));
    expect(result.current).toMatchObject({ status: "success", data: ["fresh"], isStale: false });
  });

  it("reports a soft timeout when no result arrives, without dropping the subscription", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    act(() => {
      vi.advanceTimersByTime(15_000);
    });

    expect(result.current).toMatchObject({ status: "timeout", isError: true });
    expect(client.queryListeners.size).toBe(1);

    // A late result still recovers the query.
    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["late"] }));
    expect(result.current).toMatchObject({ status: "success", data: ["late"] });
  });

  it("resets to loading when the session scope changes", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQueryResult<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    act(() => {
      for (const handler of Array.from(client.scopeHandlers)) handler();
    });

    expect(result.current).toMatchObject({ status: "loading", data: undefined });
  });
});

describe("useQuery", () => {
  it("returns undefined while loading and the result once it arrives", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useQuery<string[]>(ref, {}), { wrapper: wrapperFor(client) });

    expect(result.current).toBeUndefined();

    act(() => client.emitQuery({ type: "query.result", id: "q1", result: ["task"] }));
    expect(result.current).toEqual(["task"]);
  });

  it("throws server query errors so error boundaries can catch them", () => {
    const client = new FakeGonvexClient();
    const caught: Error[] = [];

    class Boundary extends Component<{ children: ReactNode }, { failed: boolean }> {
      state = { failed: false };

      static getDerivedStateFromError() {
        return { failed: true };
      }

      componentDidCatch(error: Error) {
        caught.push(error);
      }

      render() {
        return this.state.failed ? <div data-testid="failed">failed</div> : this.props.children;
      }
    }

    function QueryConsumer() {
      const value = useQuery<string[]>(ref, {});
      return <div>{JSON.stringify(value ?? null)}</div>;
    }

    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      const Wrapper = wrapperFor(client);
      const view = render(
        <Wrapper>
          <Boundary>
            <QueryConsumer />
          </Boundary>
        </Wrapper>,
      );

      act(() => client.emitQuery({ type: "query.error", id: "q1", error: "permission denied" }));

      expect(view.getByTestId("failed")).toBeTruthy();
      expect(caught[0]?.message).toBe("permission denied");
    } finally {
      consoleError.mockRestore();
    }
  });
});

describe("useConvexConnectionState", () => {
  it("reflects the real client connection state and updates on changes", () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(() => useConvexConnectionState(), { wrapper: wrapperFor(client) });

    expect(result.current).toMatchObject({ isWebSocketConnected: true, hasEverConnected: true });

    act(() => client.setConnected(false));
    expect(result.current.isWebSocketConnected).toBe(false);

    act(() => client.setConnected(true));
    expect(result.current.isWebSocketConnected).toBe(true);
  });
});

describe("useMutation", () => {
  it("forwards per-call timeout options to the client", async () => {
    const client = new FakeGonvexClient();
    const { result } = renderHook(
      () => useMutation({ kind: "mutation", path: "tasks.create" }, { timeoutMs: 5_000 }),
      { wrapper: wrapperFor(client) },
    );

    await act(async () => {
      await result.current({ title: "Ship" });
    });

    expect(client.mutation).toHaveBeenCalledWith({ kind: "mutation", path: "tasks.create" }, { title: "Ship" }, { timeoutMs: 5_000 });
  });
});
