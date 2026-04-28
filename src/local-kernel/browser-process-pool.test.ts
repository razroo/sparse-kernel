import { type ChildProcess, type SpawnOptions, spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  acquireNativeBrowserProcess,
  stopAllNativeBrowserProcesses,
} from "./browser-process-pool.js";

class FakeBrowserProcess extends EventEmitter {
  readonly kill = vi.fn(() => {
    this.emit("exit", 0, "SIGTERM");
    return true;
  });
  readonly unref = vi.fn();
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
      expect(input.toString()).toBe("http://127.0.0.1:19222/json/version");
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
    expect(fakeProcess.unref).toHaveBeenCalledTimes(1);

    await first.release();
    expect(fakeProcess.kill).not.toHaveBeenCalled();
    await second.release();
    expect(fakeProcess.kill).toHaveBeenCalledTimes(1);

    fs.rmSync(root, { recursive: true, force: true });
  });
});
