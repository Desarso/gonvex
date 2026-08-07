import type { ReadonlySignal, Signal } from "../../client/dist/signals.js";
import { useCallback, useSyncExternalStore } from "react";

/** Subscribe a React component to a Gonvex signal's current value. */
export function useSignalValue<T>(sig: Signal<T> | ReadonlySignal<T>): T {
  const subscribe = useCallback(
    (onStoreChange: () => void) => sig.subscribe(onStoreChange),
    [sig],
  );
  const getSnapshot = useCallback(() => sig.peek(), [sig]);

  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
