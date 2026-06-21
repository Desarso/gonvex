export type BrowserCacheDisableReason =
  | "shared-worker-unavailable"
  | "web-locks-unavailable"
  | "dedicated-worker-unavailable"
  | "persistent-storage-unavailable";

export type BrowserCacheCapabilities = {
  sharedWorker: boolean;
  webLocks: boolean;
  dedicatedWorker: boolean;
  opfs: boolean;
  supported: boolean;
  disableReason?: BrowserCacheDisableReason;
};

type CapabilityGlobal = {
  SharedWorker?: unknown;
  Worker?: unknown;
  navigator?: {
    locks?: unknown;
    storage?: {
      getDirectory?: unknown;
    };
  };
};

export function detectBrowserCacheCapabilities(globalValue: CapabilityGlobal = globalThis): BrowserCacheCapabilities {
  const sharedWorker = typeof globalValue.SharedWorker === "function";
  const webLocks = typeof globalValue.navigator?.locks === "object" && globalValue.navigator?.locks !== null;
  const dedicatedWorker = typeof globalValue.Worker === "function";
  const opfs = typeof globalValue.navigator?.storage?.getDirectory === "function";
  const disableReason = firstDisableReason({ sharedWorker, webLocks, dedicatedWorker, opfs });

  return {
    sharedWorker,
    webLocks,
    dedicatedWorker,
    opfs,
    supported: disableReason === undefined,
    disableReason,
  };
}

function firstDisableReason(
  capabilities: Pick<BrowserCacheCapabilities, "sharedWorker" | "webLocks" | "dedicatedWorker" | "opfs">,
): BrowserCacheDisableReason | undefined {
  if (!capabilities.sharedWorker) {
    return "shared-worker-unavailable";
  }
  if (!capabilities.webLocks) {
    return "web-locks-unavailable";
  }
  if (!capabilities.dedicatedWorker) {
    return "dedicated-worker-unavailable";
  }
  if (!capabilities.opfs) {
    return "persistent-storage-unavailable";
  }
  return undefined;
}
