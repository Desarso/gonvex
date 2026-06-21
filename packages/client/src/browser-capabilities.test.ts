import { describe, expect, it } from "vitest";
import { detectBrowserCacheCapabilities } from "./browser-capabilities";

const supportedGlobal = {
  SharedWorker: function SharedWorker() {},
  Worker: function Worker() {},
  navigator: {
    locks: {},
    storage: {
      getDirectory() {},
    },
  },
};

describe("detectBrowserCacheCapabilities", () => {
  it("requires SharedWorker, Web Locks, dedicated Worker, and OPFS before enabling persistent cache", () => {
    expect(detectBrowserCacheCapabilities(supportedGlobal)).toEqual({
      sharedWorker: true,
      webLocks: true,
      dedicatedWorker: true,
      opfs: true,
      supported: true,
      disableReason: undefined,
    });
  });

  it("disables cache when SharedWorker is unavailable", () => {
    expect(detectBrowserCacheCapabilities({ ...supportedGlobal, SharedWorker: undefined })).toMatchObject({
      supported: false,
      disableReason: "shared-worker-unavailable",
    });
  });

  it("disables cache when Web Locks cannot prove ownership liveness", () => {
    expect(detectBrowserCacheCapabilities({
      ...supportedGlobal,
      navigator: { ...supportedGlobal.navigator, locks: undefined },
    })).toMatchObject({
      supported: false,
      disableReason: "web-locks-unavailable",
    });
  });

  it("disables cache when OPFS persistence is unavailable", () => {
    expect(detectBrowserCacheCapabilities({
      ...supportedGlobal,
      navigator: { ...supportedGlobal.navigator, storage: {} },
    })).toMatchObject({
      supported: false,
      disableReason: "persistent-storage-unavailable",
    });
  });
});
