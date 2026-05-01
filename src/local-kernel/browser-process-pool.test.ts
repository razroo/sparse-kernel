import { type ChildProcess, type SpawnOptions, spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  acquireNativeBrowserProcess,
  inspectNativeBrowserPoolStats,
  inspectNativeBrowserPools,
  stopAllNativeBrowserProcesses,
  sweepNativeBrowserProcesses,
} from "./browser-process-pool.js";

class FakeBrowserProcess extends EventEmitter {
  readonly kill = vi.fn(() => {
    this.emit("exit", 0, "SIGTERM");
    return true;
  });
  readonly unref = vi.fn();
}

function fetchInputUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") {
    return input;
  }
  if (input instanceof URL) {
    return input.href;
  }
  return input.url;
}

describe("native browser process pool", () => {
  afterEach(async () => {
    await stopAllNativeBrowserProcesses();
  });

  it("reuses one browser process for leases in the same trust-zone pool", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-browser-pool-"));
    const fakeExecutable = path.join(root, "chrome");
    fs.writeFileSync(fakeExecutable, "#!/bin/sh\nexit 0\n");
    fs.chmodSync(fakeExecutable, 0o700);

    const fakeProcess = new FakeBrowserProcess();
    const spawnCalls: Array<{ command: string; args: string[]; options: SpawnOptions }> = [];
    const spawnImpl = vi.fn((command: string, args?: readonly string[], options?: SpawnOptions) => {
      spawnCalls.push({
        command,
        args: [...(args ?? [])],
        options: options ?? {},
      });
      return fakeProcess as unknown as ChildProcess;
    }) as unknown as typeof spawn;
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      expect(fetchInputUrl(input)).toBe("http://127.0.0.1:19222/json/version");
      return Response.json({ Browser: "Chrome/123" });
    }) as unknown as typeof fetch;

    const env = {
      OPENCLAW_STATE_DIR: root,
      OPENCLAW_SPARSEKERNEL_BROWSER_EXECUTABLE: fakeExecutable,
    } as NodeJS.ProcessEnv;
    const commonInput = {
      trustZoneId: "public_web",
      profile: "default",
      env,
      fetchImpl,
      spawnImpl,
      allocatePort: async () => 19222,
      idleTimeoutMs: 0,
    };

    const first = await acquireNativeBrowserProcess(commonInput);
    const second = await acquireNativeBrowserProcess(commonInput);

    expect(first.cdpEndpoint).toBe("http://127.0.0.1:19222");
    expect(second.poolKey).toBe(first.poolKey);
    expect(inspectNativeBrowserPools()).toEqual([
      expect.objectContaining({ key: "public_web:default", refs: 2, exited: false }),
    ]);
    expect(spawnCalls).toHaveLength(1);
    expect(spawnCalls[0]).toMatchObject({ command: fakeExecutable });
    expect(spawnCalls[0]?.args).toEqual(
      expect.arrayContaining([
        "--remote-debugging-port=19222",
        "--remote-debugging-address=127.0.0.1",
        "--headless=new",
        "--disable-gpu",
      ]),
    );
    expect(spawnCalls[0]?.args.some((arg) => arg.startsWith("--user-data-dir="))).toBe(true);
    expect(spawnCalls[0]?.args).not.toContain("--no-sandbox");
    expect(spawnCalls[0]?.args).toContain("--no-proxy-server");
    expect(fakeProcess.unref).toHaveBeenCalledTimes(1);

    await first.release();
    expect(inspectNativeBrowserPools()).toEqual([
      expect.objectContaining({ key: "public_web:default", refs: 1 }),
    ]);
    expect(fakeProcess.kill).not.toHaveBeenCalled();
    await second.release();
    expect(fakeProcess.kill).toHaveBeenCalledTimes(1);
    expect(inspectNativeBrowserPoolStats()).toEqual([
      expect.objectContaining({
        key: "public_web:default",
        starts: 1,
        cleanStops: 1,
        crashes: 0,
      }),
    ]);

    fs.rmSync(root, { recursive: true, force: true });
  });

  it("enforces a max context count per native pool", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-browser-pool-"));
    const fakeExecutable = path.join(root, "chrome");
    fs.writeFileSync(fakeExecutable, "#!/bin/sh\nexit 0\n");
    fs.chmodSync(fakeExecutable, 0o700);

    const fakeProcess = new FakeBrowserProcess();
    const spawnImpl = vi.fn(
      () => fakeProcess as unknown as ChildProcess,
    ) as unknown as typeof spawn;
    const fetchImpl = vi.fn(async () =>
      Response.json({ Browser: "Chrome/123" }),
    ) as unknown as typeof fetch;
    const env = {
      OPENCLAW_STATE_DIR: root,
      OPENCLAW_SPARSEKERNEL_BROWSER_EXECUTABLE: fakeExecutable,
    } as NodeJS.ProcessEnv;

    const first = await acquireNativeBrowserProcess({
      trustZoneId: "public_web",
      env,
      fetchImpl,
      spawnImpl,
      allocatePort: async () => 19223,
      idleTimeoutMs: 0,
      maxContexts: 1,
    });

    await expect(
      acquireNativeBrowserProcess({
        trustZoneId: "public_web",
        env,
        fetchImpl,
        spawnImpl,
        allocatePort: async () => 19223,
        idleTimeoutMs: 0,
        maxContexts: 1,
      }),
    ).rejects.toThrow(/no available context slots/);
    await first.release();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("can launch a native pool through an explicit proxy server", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-browser-pool-"));
    const fakeExecutable = path.join(root, "chrome");
    fs.writeFileSync(fakeExecutable, "#!/bin/sh\nexit 0\n");
    fs.chmodSync(fakeExecutable, 0o700);

    const fakeProcess = new FakeBrowserProcess();
    const spawnCalls: Array<{ command: string; args: string[]; options: SpawnOptions }> = [];
    const spawnImpl = vi.fn((command: string, args?: readonly string[], options?: SpawnOptions) => {
      spawnCalls.push({ command, args: [...(args ?? [])], options: options ?? {} });
      return fakeProcess as unknown as ChildProcess;
    }) as unknown as typeof spawn;
    const fetchImpl = vi.fn(async () =>
      Response.json({ Browser: "Chrome/123" }),
    ) as unknown as typeof fetch;
    const env = {
      OPENCLAW_STATE_DIR: root,
      OPENCLAW_SPARSEKERNEL_BROWSER_EXECUTABLE: fakeExecutable,
    } as NodeJS.ProcessEnv;

    const lease = await acquireNativeBrowserProcess({
      trustZoneId: "public_web",
      env,
      fetchImpl,
      spawnImpl,
      allocatePort: async () => 19224,
      idleTimeoutMs: 0,
      proxyServer: "http://127.0.0.1:18080/",
    });

    expect(spawnCalls[0]?.args).toContain("--proxy-server=http://127.0.0.1:18080/");
    expect(spawnCalls[0]?.args).not.toContain("--no-proxy-server");
    await lease.release();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("sweeps idle native pools whose CDP endpoint is stale", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-browser-pool-"));
    const fakeExecutable = path.join(root, "chrome");
    fs.writeFileSync(fakeExecutable, "#!/bin/sh\nexit 0\n");
    fs.chmodSync(fakeExecutable, 0o700);

    const fakeProcess = new FakeBrowserProcess();
    const spawnImpl = vi.fn(
      () => fakeProcess as unknown as ChildProcess,
    ) as unknown as typeof spawn;
    const fetchReady = vi.fn(async () =>
      Response.json({ Browser: "Chrome/123" }),
    ) as unknown as typeof fetch;
    const fetchStale = vi.fn(
      async () => new Response("stale", { status: 503 }),
    ) as unknown as typeof fetch;
    const env = {
      OPENCLAW_STATE_DIR: root,
      OPENCLAW_SPARSEKERNEL_BROWSER_EXECUTABLE: fakeExecutable,
    } as NodeJS.ProcessEnv;

    const lease = await acquireNativeBrowserProcess({
      trustZoneId: "public_web",
      env,
      fetchImpl: fetchReady,
      spawnImpl,
      allocatePort: async () => 19225,
      idleTimeoutMs: 60_000,
    });

    expect(await sweepNativeBrowserProcesses({ fetchImpl: fetchStale })).toEqual({
      stopped: 0,
      stalePools: [],
    });
    await lease.release();
    expect(inspectNativeBrowserPools()).toEqual([
      expect.objectContaining({ key: "public_web:default", refs: 0 }),
    ]);
    expect(await sweepNativeBrowserProcesses({ fetchImpl: fetchStale })).toEqual({
      stopped: 1,
      stalePools: ["public_web:default"],
    });
    expect(fakeProcess.kill).toHaveBeenCalledTimes(1);
    expect(inspectNativeBrowserPools()).toEqual([]);

    fs.rmSync(root, { recursive: true, force: true });
  });
});
