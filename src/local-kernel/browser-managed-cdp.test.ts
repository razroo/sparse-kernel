import { describe, expect, it, vi } from "vitest";
import { resolveSparseKernelBrowserCdpEndpoint } from "./browser-managed-cdp.js";
import type {
  NativeBrowserProcessAcquireInput,
  NativeBrowserProcessLease,
} from "./browser-process-pool.js";

describe("resolveSparseKernelBrowserCdpEndpoint", () => {
  it("keeps explicit CDP endpoints ahead of native browser pools", async () => {
    const nativeAcquire = vi.fn(async (): Promise<NativeBrowserProcessLease> => {
      throw new Error("native pool should not be used");
    });

    await expect(
      resolveSparseKernelBrowserCdpEndpoint({
        env: {
          OPENCLAW_RUNTIME_BROWSER_BROKER: "native",
          OPENCLAW_SPARSEKERNEL_BROWSER_CDP_ENDPOINT: "http://127.0.0.1:9222",
        } as NodeJS.ProcessEnv,
        nativeAcquire,
      }),
    ).resolves.toEqual({
      cdpEndpoint: "http://127.0.0.1:9222",
      source: "static",
    });
    expect(nativeAcquire).not.toHaveBeenCalled();
  });

  it("resolves native browser pool endpoints with trust-zone context", async () => {
    const release = vi.fn(async () => {});
    const nativeAcquire = vi.fn(
      async (input: NativeBrowserProcessAcquireInput): Promise<NativeBrowserProcessLease> => {
        expect(input).toMatchObject({
          profile: "openclaw",
          trustZoneId: "authenticated_web",
        });
        return {
          cdpEndpoint: "http://127.0.0.1:19222",
          trustZoneId: "authenticated_web",
          poolKey: "authenticated_web:openclaw",
          userDataDir: "/tmp/openclaw-browser-pool",
          release,
        };
      },
    );

    const endpoint = await resolveSparseKernelBrowserCdpEndpoint({
      env: { OPENCLAW_RUNTIME_BROWSER_BROKER: "native" } as NodeJS.ProcessEnv,
      profile: "openclaw",
      trustZoneId: "authenticated_web",
      nativeAcquire,
    });

    expect(endpoint).toMatchObject({
      cdpEndpoint: "http://127.0.0.1:19222",
      source: "native-pool",
    });
    await endpoint?.release?.();
    expect(release).toHaveBeenCalledTimes(1);
  });
});
