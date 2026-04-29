import { type ChildProcess, spawn } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { resolveRuntimeBrowserPoolRoot } from "./paths.js";

export type NativeBrowserProcessLease = {
  cdpEndpoint: string;
  trustZoneId: string;
  poolKey: string;
  pid?: number;
  userDataDir: string;
  release: () => Promise<void>;
};

export type NativeBrowserProcessAcquireInput = {
  trustZoneId: string;
  profile?: string;
  env?: NodeJS.ProcessEnv;
  fetchImpl?: typeof fetch;
  spawnImpl?: typeof spawn;
  allocatePort?: () => Promise<number>;
  readyTimeoutMs?: number;
  idleTimeoutMs?: number;
  executablePath?: string;
  maxContexts?: number;
};

type NativeBrowserPool = {
  key: string;
  trustZoneId: string;
  profile: string;
  cdpEndpoint: string;
  cdpPort: number;
  userDataDir: string;
  proc: ChildProcess;
  refs: number;
  idleTimer?: NodeJS.Timeout;
  exited: boolean;
};

export type NativeBrowserPoolSnapshot = {
  key: string;
  trustZoneId: string;
  profile: string;
  cdpEndpoint: string;
  refs: number;
  exited: boolean;
  pid?: number;
  userDataDir: string;
};

const pools = new Map<string, NativeBrowserPool>();
let exitHookInstalled = false;

export async function acquireNativeBrowserProcess(
  input: NativeBrowserProcessAcquireInput,
): Promise<NativeBrowserProcessLease> {
  const env = input.env ?? process.env;
  const trustZoneId = sanitizePoolPart(input.trustZoneId || "public_web");
  const profile = sanitizePoolPart(input.profile || "default");
  const key = `${trustZoneId}:${profile}`;
  const fetchImpl = input.fetchImpl ?? fetch;
  const idleTimeoutMs =
    input.idleTimeoutMs ?? readIntEnv(env.OPENCLAW_SPARSEKERNEL_BROWSER_IDLE_MS, 30_000);
  const maxContexts = Math.max(
    1,
    input.maxContexts ?? readIntEnv(env.OPENCLAW_SPARSEKERNEL_BROWSER_MAX_CONTEXTS, 8),
  );
  const existing = pools.get(key);
  if (existing && !existing.exited && (await isCdpReady(existing.cdpEndpoint, fetchImpl, 500))) {
    if (existing.refs >= maxContexts) {
      throw new Error(
        `SparseKernel native browser pool ${key} has no available context slots (${existing.refs}/${maxContexts}).`,
      );
    }
    clearIdleTimer(existing);
    existing.refs += 1;
    return leaseFor(existing, idleTimeoutMs);
  }
  if (existing) {
    await stopPool(existing);
    pools.delete(key);
  }

  const executablePath =
    input.executablePath ?? resolveNativeBrowserExecutable(env, process.platform);
  if (!executablePath) {
    throw new Error(
      "SparseKernel native browser pool could not find a Chromium-compatible executable. Set OPENCLAW_SPARSEKERNEL_BROWSER_EXECUTABLE.",
    );
  }

  const cdpPort = await (input.allocatePort ?? allocateLoopbackPort)();
  const userDataDir = path.join(resolveRuntimeBrowserPoolRoot(env), trustZoneId, profile);
  fs.mkdirSync(userDataDir, { recursive: true, mode: 0o700 });
  const cdpEndpoint = `http://127.0.0.1:${cdpPort}`;
  const spawnImpl = input.spawnImpl ?? spawn;
  const args = buildNativeBrowserArgs({
    cdpPort,
    userDataDir,
    env,
  });
  const proc = spawnImpl(executablePath, args, {
    detached: process.platform !== "win32",
    stdio: "ignore",
    env: sanitizeBrowserEnv(env),
  });
  proc.unref?.();
  const pool: NativeBrowserPool = {
    key,
    trustZoneId,
    profile,
    cdpEndpoint,
    cdpPort,
    userDataDir,
    proc,
    refs: 1,
    exited: false,
  };
  proc.once("exit", () => {
    pool.exited = true;
    pools.delete(key);
  });
  proc.once("error", () => {
    pool.exited = true;
    pools.delete(key);
  });
  pools.set(key, pool);
  installExitHook();

  try {
    await waitForCdpReady({
      cdpEndpoint,
      fetchImpl,
      timeoutMs:
        input.readyTimeoutMs ?? readIntEnv(env.OPENCLAW_SPARSEKERNEL_BROWSER_READY_MS, 10_000),
      proc: pool,
    });
  } catch (error) {
    await stopPool(pool);
    pools.delete(key);
    throw error;
  }

  return leaseFor(pool, idleTimeoutMs);
}

export async function stopAllNativeBrowserProcesses(): Promise<void> {
  const active = [...pools.values()];
  pools.clear();
  await Promise.all(active.map((pool) => stopPool(pool)));
}

export function inspectNativeBrowserPools(): NativeBrowserPoolSnapshot[] {
  return [...pools.values()]
    .map((pool) => ({
      key: pool.key,
      trustZoneId: pool.trustZoneId,
      profile: pool.profile,
      cdpEndpoint: pool.cdpEndpoint,
      refs: pool.refs,
      exited: pool.exited,
      ...(pool.proc.pid ? { pid: pool.proc.pid } : {}),
      userDataDir: pool.userDataDir,
    }))
    .toSorted((left, right) => left.key.localeCompare(right.key));
}

export function resolveNativeBrowserExecutable(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): string | null {
  const explicit = env.OPENCLAW_SPARSEKERNEL_BROWSER_EXECUTABLE?.trim();
  if (explicit) {
    return explicit;
  }
  const candidates =
    platform === "darwin"
      ? [
          "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
          "/Applications/Chromium.app/Contents/MacOS/Chromium",
          "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
          "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        ]
      : platform === "win32"
        ? ["chrome.exe", "msedge.exe", "brave.exe"]
        : [
            "google-chrome",
            "google-chrome-stable",
            "chromium",
            "chromium-browser",
            "brave-browser",
            "microsoft-edge",
          ];
  for (const candidate of candidates) {
    if (path.isAbsolute(candidate)) {
      if (isExecutable(candidate)) {
        return candidate;
      }
      continue;
    }
    const resolved = findExecutableOnPath(candidate, env);
    if (resolved) {
      return resolved;
    }
  }
  return null;
}

function leaseFor(pool: NativeBrowserPool, idleTimeoutMs?: number): NativeBrowserProcessLease {
  let released = false;
  return {
    cdpEndpoint: pool.cdpEndpoint,
    trustZoneId: pool.trustZoneId,
    poolKey: pool.key,
    pid: pool.proc.pid,
    userDataDir: pool.userDataDir,
    release: async () => {
      if (released) {
        return;
      }
      released = true;
      pool.refs = Math.max(0, pool.refs - 1);
      if (pool.refs === 0) {
        scheduleIdleStop(pool, idleTimeoutMs);
      }
    },
  };
}

function buildNativeBrowserArgs(params: {
  cdpPort: number;
  userDataDir: string;
  env: NodeJS.ProcessEnv;
}): string[] {
  const args = [
    `--remote-debugging-port=${params.cdpPort}`,
    "--remote-debugging-address=127.0.0.1",
    `--user-data-dir=${params.userDataDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-sync",
    "--disable-background-networking",
    "--disable-component-update",
    "--disable-features=Translate,MediaRouter",
    "--disable-session-crashed-bubble",
    "--hide-crash-restore-bubble",
    "--password-store=basic",
    "--no-proxy-server",
  ];
  if (readBooleanEnv(params.env.OPENCLAW_SPARSEKERNEL_BROWSER_HEADLESS, true)) {
    args.push("--headless=new", "--disable-gpu");
  }
  if (readBooleanEnv(params.env.OPENCLAW_SPARSEKERNEL_BROWSER_NO_SANDBOX, false)) {
    args.push("--no-sandbox");
  }
  if (process.platform === "linux") {
    args.push("--disable-dev-shm-usage");
  }
  const extraArgs =
    params.env.OPENCLAW_SPARSEKERNEL_BROWSER_EXTRA_ARGS?.trim() ??
    params.env.OPENCLAW_SPARSEKERNEL_BROWSER_ARGS?.trim();
  if (extraArgs) {
    args.push(...splitShellWords(extraArgs));
  }
  return args;
}

async function waitForCdpReady(params: {
  cdpEndpoint: string;
  fetchImpl: typeof fetch;
  timeoutMs: number;
  proc: NativeBrowserPool;
}): Promise<void> {
  const deadline = Date.now() + Math.max(1, params.timeoutMs);
  let lastError = "";
  while (Date.now() < deadline) {
    if (params.proc.exited) {
      throw new Error("SparseKernel native browser process exited before CDP became ready.");
    }
    try {
      if (await isCdpReady(params.cdpEndpoint, params.fetchImpl, 500)) {
        return;
      }
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await delay(100);
  }
  throw new Error(
    `SparseKernel native browser CDP did not become ready at ${params.cdpEndpoint}${lastError ? `: ${lastError}` : ""}`,
  );
}

async function isCdpReady(
  cdpEndpoint: string,
  fetchImpl: typeof fetch,
  timeoutMs: number,
): Promise<boolean> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new Error("timed out")), timeoutMs);
  try {
    const response = await fetchImpl(`${cdpEndpoint}/json/version`, {
      signal: ctrl.signal,
    });
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

async function allocateLoopbackPort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => {
        if (typeof address === "object" && address?.port) {
          resolve(address.port);
        } else {
          reject(new Error("Failed to allocate SparseKernel native browser port."));
        }
      });
    });
  });
}

function scheduleIdleStop(pool: NativeBrowserPool, idleTimeoutMs: number | undefined): void {
  clearIdleTimer(pool);
  const timeoutMs = Math.max(
    0,
    idleTimeoutMs ?? readIntEnv(process.env.OPENCLAW_SPARSEKERNEL_BROWSER_IDLE_MS, 30_000),
  );
  const stop = () => {
    if (pool.refs === 0) {
      pools.delete(pool.key);
      void stopPool(pool);
    }
  };
  if (timeoutMs === 0) {
    stop();
    return;
  }
  pool.idleTimer = setTimeout(stop, timeoutMs);
  pool.idleTimer.unref?.();
}

function clearIdleTimer(pool: NativeBrowserPool): void {
  if (pool.idleTimer) {
    clearTimeout(pool.idleTimer);
    pool.idleTimer = undefined;
  }
}

async function stopPool(pool: NativeBrowserPool): Promise<void> {
  clearIdleTimer(pool);
  if (pool.exited) {
    return;
  }
  pool.exited = true;
  try {
    if (process.platform !== "win32" && pool.proc.pid) {
      process.kill(-pool.proc.pid, "SIGTERM");
    } else {
      pool.proc.kill("SIGTERM");
    }
  } catch {
    try {
      pool.proc.kill("SIGTERM");
    } catch {
      // Best-effort cleanup; Chrome may already have exited.
    }
  }
}

function installExitHook(): void {
  if (exitHookInstalled) {
    return;
  }
  exitHookInstalled = true;
  process.once("exit", () => {
    for (const pool of pools.values()) {
      try {
        pool.proc.kill("SIGTERM");
      } catch {
        // ignore
      }
    }
  });
}

function sanitizeBrowserEnv(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const copy = { ...env };
  delete copy.HTTP_PROXY;
  delete copy.HTTPS_PROXY;
  delete copy.ALL_PROXY;
  delete copy.http_proxy;
  delete copy.https_proxy;
  delete copy.all_proxy;
  return copy;
}

function sanitizePoolPart(value: string): string {
  return (
    value
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_.-]+/g, "_")
      .slice(0, 80) || "default"
  );
}

function readBooleanEnv(raw: string | undefined, fallback: boolean): boolean {
  const normalized = raw?.trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }
  if (normalized === "1" || normalized === "true" || normalized === "on") {
    return true;
  }
  if (normalized === "0" || normalized === "false" || normalized === "off") {
    return false;
  }
  return fallback;
}

function readIntEnv(raw: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(raw ?? "", 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function findExecutableOnPath(command: string, env: NodeJS.ProcessEnv): string | null {
  const pathValue = env.PATH ?? process.env.PATH ?? "";
  for (const part of pathValue.split(path.delimiter)) {
    if (!part) {
      continue;
    }
    const candidate = path.join(part, command);
    if (isExecutable(candidate)) {
      return candidate;
    }
  }
  return null;
}

function isExecutable(filePath: string): boolean {
  try {
    fs.accessSync(filePath, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function splitShellWords(raw: string): string[] {
  return (
    raw.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g)?.map((part) => part.replace(/^["']|["']$/g, "")) ??
    []
  );
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
