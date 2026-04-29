import { spawn, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import type { LocalKernelDatabase } from "./database.js";
import { resolveNetworkPolicyProxyRef } from "./network-policy.js";
import type { SandboxAllocationRecord } from "./types.js";

export type SandboxBackendKind =
  | "local/no_isolation"
  | "docker"
  | "bwrap"
  | "minijail"
  | "ssh"
  | "openshell"
  | "vm"
  | "other";

export type SandboxAllocationRequest = {
  taskId: string;
  agentId?: string;
  trustZoneId: string;
  requirements?: {
    backend?: SandboxBackendKind;
    dockerImage?: string;
    maxRuntimeMs?: number;
    maxBytesOut?: number;
    maxTokens?: number;
    leaseUntil?: string;
  };
};

export type SandboxCommandRequest = {
  allocationId: string;
  backend?: SandboxBackendKind;
  dockerImage?: string;
  command: string;
  args?: string[];
  cwd?: string;
  env?: Record<string, string>;
  stdin?: string | Buffer | Uint8Array;
  signal?: AbortSignal;
  timeoutMs?: number;
  maxOutputBytes?: number;
};

export type DockerSandboxPolicy = {
  networkMode: "none" | "bridge";
  proxyServer?: string;
  memoryMb?: number;
  pidsLimit?: number;
  readOnlyRoot?: boolean;
  tmpfs?: string[];
};

export type SandboxPolicySnapshot = {
  trustZoneId: string;
  backend: SandboxBackendKind;
  filesystemPolicy?: unknown;
  maxProcesses?: number;
  maxMemoryMb?: number;
  maxRuntimeSeconds?: number;
  networkPolicy?: {
    id: string;
    defaultAction: "allow" | "deny";
    allowPrivateNetwork: boolean;
    proxyRef?: string;
  };
  docker?: DockerSandboxPolicy;
};

export type HardEgressEnforcementSnapshot = {
  helper: string;
  enforcementId: string;
  boundary: "host_firewall" | "egress_proxy" | "vm_firewall" | "platform_enforcer";
  description?: string;
};

export type SandboxCommandResult = {
  allocationId: string;
  exitCode: number | null;
  signal?: NodeJS.Signals;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  durationMs: number;
};

export type SandboxSpawnCommandRequest = {
  allocationId: string;
  backend: SandboxBackendKind;
  spawnPlan: SandboxSpawnPlan;
  cwd?: string;
  env?: Record<string, string>;
  stdin?: string | Buffer | Uint8Array;
  signal?: AbortSignal;
  timeoutMs: number;
  maxOutputBytes: number;
};

export interface SandboxBroker {
  allocateSandbox(request: SandboxAllocationRequest): SandboxAllocationRecord;
  releaseSandbox(allocationId: string): boolean;
  runCommand?(request: SandboxCommandRequest): Promise<SandboxCommandResult>;
}

function commandAvailable(command: string): boolean {
  const result = spawnSync(command, ["--version"], { stdio: "ignore" });
  if (result.error && (result.error as NodeJS.ErrnoException).code === "ENOENT") {
    return false;
  }
  return result.status === 0 || result.status === 1;
}

function firstAvailableCommand(commands: string[]): string | undefined {
  return commands.find((command) => commandAvailable(command));
}

export function isSandboxBackendAvailable(backend: SandboxBackendKind): boolean {
  switch (backend) {
    case "bwrap":
      return commandAvailable("bwrap") || commandAvailable("bubblewrap");
    case "minijail":
      return commandAvailable("minijail0") || commandAvailable("minijail");
    case "docker":
      return commandAvailable("docker");
    case "local/no_isolation":
    case "ssh":
    case "openshell":
    case "vm":
    case "other":
      return true;
  }
}

export type SandboxSpawnPlan = {
  command: string;
  args: string[];
  isolation: string;
};

type SandboxLeaseMetadata = {
  backend?: SandboxBackendKind;
  dockerImage?: string;
  isolation?: string;
  policy?: SandboxPolicySnapshot;
  hardEgress?: HardEgressEnforcementSnapshot;
};

type CommandResolver = (commands: string[]) => string | undefined;

function isTruthyRuntimeFlag(value: string | undefined): boolean {
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function readSandboxLeaseMetadata(value: unknown): SandboxLeaseMetadata {
  if (!isRecord(value)) {
    return {};
  }
  return {
    ...(typeof value.backend === "string" ? { backend: value.backend as SandboxBackendKind } : {}),
    ...(typeof value.dockerImage === "string" && value.dockerImage.trim()
      ? { dockerImage: value.dockerImage.trim() }
      : {}),
    ...(typeof value.isolation === "string" && value.isolation.trim()
      ? { isolation: value.isolation.trim() }
      : {}),
    ...(isRecord(value.policy) ? { policy: value.policy as SandboxPolicySnapshot } : {}),
    ...(isHardEgressEnforcementSnapshot(value.hardEgress) ? { hardEgress: value.hardEgress } : {}),
  };
}

function isHardEgressEnforcementSnapshot(value: unknown): value is HardEgressEnforcementSnapshot {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.helper === "string" &&
    typeof value.enforcementId === "string" &&
    isHardEgressBoundary(value.boundary)
  );
}

function resolveSandboxPolicySnapshot(params: {
  db: LocalKernelDatabase;
  trustZoneId: string;
  backend: SandboxBackendKind;
}): SandboxPolicySnapshot {
  const zone = params.db.listTrustZones().find((entry) => entry.id === params.trustZoneId);
  const networkPolicy = params.db.getNetworkPolicyForTrustZone(params.trustZoneId);
  const proxyDecision = resolveNetworkPolicyProxyRef(networkPolicy?.proxyRef);
  const dockerNetworkMode =
    networkPolicy?.defaultAction === "allow" && proxyDecision.ok ? "bridge" : "none";
  return {
    trustZoneId: params.trustZoneId,
    backend: params.backend,
    ...(zone?.filesystemPolicy !== undefined ? { filesystemPolicy: zone.filesystemPolicy } : {}),
    ...(zone?.maxProcesses !== undefined ? { maxProcesses: zone.maxProcesses } : {}),
    ...(zone?.maxMemoryMb !== undefined ? { maxMemoryMb: zone.maxMemoryMb } : {}),
    ...(zone?.maxRuntimeSeconds !== undefined ? { maxRuntimeSeconds: zone.maxRuntimeSeconds } : {}),
    ...(networkPolicy
      ? {
          networkPolicy: {
            id: networkPolicy.id,
            defaultAction: networkPolicy.defaultAction,
            allowPrivateNetwork: networkPolicy.allowPrivateNetwork,
            ...(networkPolicy.proxyRef ? { proxyRef: networkPolicy.proxyRef } : {}),
          },
        }
      : {}),
    docker: {
      networkMode: dockerNetworkMode,
      ...(proxyDecision.ok ? { proxyServer: proxyDecision.proxyServer } : {}),
      ...(zone?.maxMemoryMb !== undefined ? { memoryMb: zone.maxMemoryMb } : {}),
      ...(zone?.maxProcesses !== undefined ? { pidsLimit: zone.maxProcesses } : {}),
      readOnlyRoot: zone?.filesystemPolicy
        ? readFilesystemMode(zone.filesystemPolicy) !== "readwrite"
        : true,
      tmpfs: ["/tmp:rw,nosuid,nodev,noexec,size=64m"],
    },
  };
}

function readFilesystemMode(policy: unknown): string {
  if (!isRecord(policy)) {
    return "";
  }
  return typeof policy.mode === "string" ? policy.mode : "";
}

function describeSandboxBackend(backend: SandboxBackendKind): string {
  switch (backend) {
    case "local/no_isolation":
      return "local/no_isolation command runner; trusted execution only";
    case "bwrap":
      return "bubblewrap process namespace with broker-selected binds";
    case "minijail":
      return "minijail process jail with backend-defined limits";
    case "docker":
      return "Docker container process with broker-selected image, no pull, and network disabled by default";
    case "ssh":
      return "remote SSH backend placeholder; no v0 command execution";
    case "openshell":
      return "OpenShell backend placeholder; no v0 command execution";
    case "vm":
      return "VM backend placeholder; no v0 command execution";
    case "other":
      return "unknown sandbox backend placeholder; no v0 command execution";
  }
}

function validDockerEnvName(name: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}

function readHostEnv(name: string): string | undefined {
  const value = process.env[name];
  return value?.trim() ? value : undefined;
}

function isHardEgressBoundary(value: unknown): value is HardEgressEnforcementSnapshot["boundary"] {
  return (
    value === "host_firewall" ||
    value === "egress_proxy" ||
    value === "vm_firewall" ||
    value === "platform_enforcer"
  );
}

function readJsonOrWhitespaceArgs(raw: string | undefined, name: string): string[] {
  const value = raw?.trim();
  if (!value) {
    return [];
  }
  if (value.startsWith("[")) {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed) || parsed.some((entry) => typeof entry !== "string")) {
      throw new Error(`${name} must be a JSON string array`);
    }
    return parsed;
  }
  return value.split(/\s+/).filter(Boolean);
}

function readPositiveIntegerEnv(raw: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(raw?.trim() ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function resolveHardEgressHelper(
  env: NodeJS.ProcessEnv,
  enforcement?: HardEgressEnforcementSnapshot,
): { command: string; args: string[] } {
  const command =
    env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER?.trim() || enforcement?.helper?.trim();
  if (!command) {
    throw new Error("missing hard egress helper");
  }
  return {
    command,
    args: readJsonOrWhitespaceArgs(
      env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS,
      "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS",
    ),
  };
}

function isBuiltinHardEgressHelper(command: string): boolean {
  const normalized = command.trim().toLowerCase();
  return (
    normalized === "builtin" ||
    normalized === "sparsekernel:builtin" ||
    normalized === "openclaw:sparsekernel:builtin"
  );
}

function builtinHardEgressDescription(params: {
  backend: SandboxBackendKind;
  policy?: SandboxPolicySnapshot;
}): string {
  if (params.backend === "docker" && params.policy?.docker?.networkMode === "none") {
    return "Docker --network none selected by the SparseKernel sandbox policy";
  }
  if (params.backend === "bwrap") {
    return "bubblewrap --unshare-all selected by the SparseKernel sandbox backend";
  }
  throw new Error(
    `builtin hard egress only supports bwrap or Docker with networkMode=none, got ${params.backend}`,
  );
}

function runBuiltinHardEgressHelper(params: {
  action: "allocate" | "release";
  allocationId: string;
  backend: SandboxBackendKind;
  policy?: SandboxPolicySnapshot;
}): HardEgressEnforcementSnapshot | undefined {
  if (params.action === "release") {
    return undefined;
  }
  return {
    helper: "builtin",
    enforcementId: `builtin:${params.backend}:${params.allocationId}`,
    boundary: "platform_enforcer",
    description: builtinHardEgressDescription(params),
  };
}

function dockerContainerProxyServer(proxyServer: string): {
  proxyServer: string;
  addHostGateway: boolean;
} {
  try {
    const parsed = new URL(proxyServer);
    const host = parsed.hostname.toLowerCase();
    if (host === "127.0.0.1" || host === "localhost" || host === "::1") {
      parsed.hostname = "host.docker.internal";
      return { proxyServer: parsed.toString(), addHostGateway: true };
    }
  } catch {
    return { proxyServer, addHostGateway: false };
  }
  return { proxyServer, addHostGateway: false };
}

function runHardEgressHelper(params: {
  action: "allocate" | "release";
  allocationId: string;
  backend: SandboxBackendKind;
  trustZoneId?: string;
  agentId?: string;
  taskId?: string;
  policy?: SandboxPolicySnapshot;
  enforcement?: HardEgressEnforcementSnapshot;
  env: NodeJS.ProcessEnv;
}): HardEgressEnforcementSnapshot | undefined {
  const helper = resolveHardEgressHelper(params.env, params.enforcement);
  if (isBuiltinHardEgressHelper(helper.command)) {
    return runBuiltinHardEgressHelper({
      action: params.action,
      allocationId: params.allocationId,
      backend: params.backend,
      policy: params.policy,
    });
  }
  const result = spawnSync(helper.command, helper.args, {
    input: JSON.stringify({
      protocol: "openclaw.sparsekernel.sandbox-egress.v1",
      action: params.action,
      allocationId: params.allocationId,
      backend: params.backend,
      trustZoneId: params.trustZoneId,
      agentId: params.agentId,
      taskId: params.taskId,
      policy: params.policy,
      enforcement: params.enforcement,
    }),
    encoding: "utf8",
    maxBuffer: 256 * 1024,
    timeout: readPositiveIntegerEnv(
      params.env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_TIMEOUT_MS,
      30_000,
    ),
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(
      `hard egress helper exited ${result.status ?? result.signal ?? "unknown"}: ${
        result.stderr?.trim() || "no stderr"
      }`,
    );
  }
  if (params.action === "release") {
    return undefined;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(result.stdout || "{}") as unknown;
  } catch (error) {
    throw new Error(
      `hard egress helper returned invalid JSON: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
  if (!isRecord(parsed) || parsed.ok !== true) {
    throw new Error("hard egress helper did not confirm enforcement");
  }
  const enforcementId =
    typeof parsed.enforcementId === "string" && parsed.enforcementId.trim()
      ? parsed.enforcementId.trim()
      : undefined;
  if (!enforcementId) {
    throw new Error("hard egress helper response missing enforcementId");
  }
  const boundary = parsed.boundary;
  if (!isHardEgressBoundary(boundary)) {
    throw new Error("hard egress helper response missing supported boundary");
  }
  return {
    helper: helper.command,
    enforcementId,
    boundary,
    ...(typeof parsed.description === "string" && parsed.description.trim()
      ? { description: parsed.description.trim() }
      : {}),
  };
}

function sandboxNetworkProxyValidationFailure(params: {
  policy: SandboxPolicySnapshot;
  backend: SandboxBackendKind;
  env?: NodeJS.ProcessEnv;
}): string | undefined {
  if (!isTruthyRuntimeFlag(params.env?.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_PROXY)) {
    return undefined;
  }
  if (params.policy.networkPolicy?.defaultAction !== "allow") {
    return undefined;
  }
  const proxyDecision = resolveNetworkPolicyProxyRef(params.policy.networkPolicy.proxyRef);
  if (!proxyDecision.ok) {
    return proxyDecision.reason;
  }
  if (params.backend !== "docker") {
    return `backend cannot carry proxy-required sandbox egress in v0: ${params.backend}`;
  }
  return undefined;
}

export function buildSandboxProcessEnv(params: {
  backend: SandboxBackendKind;
  env?: Record<string, string>;
}): NodeJS.ProcessEnv {
  if (params.backend === "local/no_isolation") {
    return params.env ? { ...process.env, ...params.env } : process.env;
  }
  const base: NodeJS.ProcessEnv = {
    PATH: readHostEnv("PATH") ?? "/usr/local/bin:/usr/bin:/bin",
    HOME: "/tmp",
    TMPDIR: "/tmp",
    LANG: readHostEnv("LANG") ?? "C.UTF-8",
  };
  if (params.backend === "docker") {
    for (const name of ["DOCKER_HOST", "DOCKER_CONTEXT", "XDG_RUNTIME_DIR"]) {
      const value = readHostEnv(name);
      if (value) {
        base[name] = value;
      }
    }
  }
  return { ...base, ...(params.env ?? {}) };
}

export function buildSandboxSpawnPlan(params: {
  backend: SandboxBackendKind;
  command: string;
  args: string[];
  cwd?: string;
  env?: Record<string, string>;
  stdin?: boolean;
  dockerImage?: string;
  dockerPolicy?: DockerSandboxPolicy;
  resolveCommand?: CommandResolver;
}): SandboxSpawnPlan {
  const resolveCommand = params.resolveCommand ?? firstAvailableCommand;
  switch (params.backend) {
    case "local/no_isolation":
      return {
        command: params.command,
        args: params.args,
        isolation: describeSandboxBackend(params.backend),
      };
    case "bwrap": {
      const binary = resolveCommand(["bwrap", "bubblewrap"]);
      if (!binary) {
        throw new Error("Sandbox backend unavailable: bwrap");
      }
      const args = [
        "--die-with-parent",
        "--unshare-all",
        "--new-session",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
      ];
      for (const path of ["/usr", "/bin", "/lib", "/lib64", "/etc"]) {
        if (fs.existsSync(path)) {
          args.push("--ro-bind", path, path);
        }
      }
      if (params.cwd) {
        args.push("--bind", params.cwd, params.cwd, "--chdir", params.cwd);
      }
      args.push("--", params.command, ...params.args);
      return {
        command: binary,
        args,
        isolation: describeSandboxBackend(params.backend),
      };
    }
    case "minijail": {
      const binary = resolveCommand(["minijail0", "minijail"]);
      if (!binary) {
        throw new Error("Sandbox backend unavailable: minijail");
      }
      return {
        command: binary,
        args: ["-p", "-v", "--", params.command, ...params.args],
        isolation: describeSandboxBackend(params.backend),
      };
    }
    case "docker": {
      const binary = resolveCommand(["docker"]);
      if (!binary) {
        throw new Error("Sandbox backend unavailable: docker");
      }
      const image = params.dockerImage?.trim();
      if (!image) {
        throw new Error(
          "Sandbox backend docker requires an explicit dockerImage or OPENCLAW_SPARSEKERNEL_DOCKER_IMAGE.",
        );
      }
      const dockerPolicy = params.dockerPolicy ?? { networkMode: "none" as const };
      const args = [
        "run",
        "--rm",
        ...(params.stdin ? ["-i"] : []),
        "--pull",
        "never",
        "--network",
        dockerPolicy.networkMode,
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
      ];
      if (dockerPolicy.readOnlyRoot !== false) {
        args.push("--read-only");
      }
      for (const tmpfs of dockerPolicy.tmpfs ?? ["/tmp:rw,nosuid,nodev,noexec,size=64m"]) {
        args.push("--tmpfs", tmpfs);
      }
      if (dockerPolicy.memoryMb) {
        args.push("--memory", `${Math.max(1, Math.trunc(dockerPolicy.memoryMb))}m`);
      }
      if (dockerPolicy.pidsLimit) {
        args.push("--pids-limit", String(Math.max(1, Math.trunc(dockerPolicy.pidsLimit))));
      }
      const env = { ...(params.env ?? {}) };
      if (dockerPolicy.proxyServer) {
        const dockerProxy = dockerContainerProxyServer(dockerPolicy.proxyServer);
        if (dockerProxy.addHostGateway) {
          args.push("--add-host", "host.docker.internal:host-gateway");
        }
        env.HTTP_PROXY ??= dockerProxy.proxyServer;
        env.HTTPS_PROXY ??= dockerProxy.proxyServer;
        env.ALL_PROXY ??= dockerProxy.proxyServer;
        env.NO_PROXY ??= "127.0.0.1,localhost,::1";
      }
      for (const [name, value] of Object.entries(env)) {
        if (validDockerEnvName(name)) {
          args.push("--env", `${name}=${value}`);
        }
      }
      if (params.cwd) {
        args.push("-v", `${params.cwd}:/workspace:rw`, "-w", "/workspace");
      }
      args.push(image, params.command, ...params.args);
      return {
        command: binary,
        args,
        isolation: describeSandboxBackend(params.backend),
      };
    }
    case "ssh":
    case "openshell":
    case "vm":
    case "other":
      throw new Error(
        `Sandbox backend does not support brokered command execution yet: ${params.backend}`,
      );
  }
}

export async function runSandboxSpawnPlan(
  request: SandboxSpawnCommandRequest,
): Promise<SandboxCommandResult> {
  const started = Date.now();
  const timeoutMs = Math.max(1, request.timeoutMs);
  const maxOutputBytes = Math.max(1, request.maxOutputBytes);
  return await new Promise<SandboxCommandResult>((resolve, reject) => {
    const child = spawn(request.spawnPlan.command, request.spawnPlan.args, {
      cwd: request.backend === "local/no_isolation" ? request.cwd : undefined,
      env: buildSandboxProcessEnv({ backend: request.backend, env: request.env }),
      stdio: [request.stdin === undefined ? "ignore" : "pipe", "pipe", "pipe"],
    });
    let timedOut = false;
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    const abort = () => {
      child.kill("SIGTERM");
    };
    const append = (current: Buffer, chunk: Buffer) =>
      Buffer.concat([current, chunk]).subarray(0, maxOutputBytes);
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
    }, timeoutMs);
    timer.unref?.();
    if (request.signal?.aborted) {
      abort();
    } else {
      request.signal?.addEventListener("abort", abort, { once: true });
    }
    if (request.stdin !== undefined) {
      child.stdin?.end(request.stdin);
    }
    child.stdout?.on("data", (chunk: Buffer) => {
      stdout = append(stdout, chunk);
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr = append(stderr, chunk);
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      request.signal?.removeEventListener("abort", abort);
      reject(error);
    });
    child.on("close", (exitCode, signal) => {
      clearTimeout(timer);
      request.signal?.removeEventListener("abort", abort);
      resolve({
        allocationId: request.allocationId,
        exitCode,
        ...(signal ? { signal } : {}),
        stdout: stdout.toString("utf8"),
        stderr: stderr.toString("utf8"),
        timedOut,
        durationMs: Date.now() - started,
      });
    });
  });
}

export class LocalSandboxBroker implements SandboxBroker {
  private readonly allocationBackends = new Map<string, SandboxBackendKind>();

  constructor(private readonly db: LocalKernelDatabase) {}

  allocateSandbox(request: SandboxAllocationRequest): SandboxAllocationRecord {
    if (request.agentId) {
      const allowed = this.db.checkCapability({
        subjectType: "agent",
        subjectId: request.agentId,
        resourceType: "sandbox",
        resourceId: request.trustZoneId,
        action: "allocate",
        context: { taskId: request.taskId, requirements: request.requirements },
      });
      if (!allowed) {
        throw new Error(`Agent ${request.agentId} lacks sandbox allocate capability`);
      }
    }

    const backend = request.requirements?.backend ?? "local/no_isolation";
    if (!isSandboxBackendAvailable(backend)) {
      this.db.recordAudit({
        actor: request.agentId ? { type: "agent", id: request.agentId } : { type: "runtime" },
        action: "sandbox.allocation_denied_backend_unavailable",
        objectType: "trust_zone",
        objectId: request.trustZoneId,
        payload: { backend },
      });
      throw new Error(`Sandbox backend unavailable: ${backend}`);
    }
    const allocationId = `sandbox_${crypto.randomUUID()}`;
    const policy = resolveSandboxPolicySnapshot({
      db: this.db,
      trustZoneId: request.trustZoneId,
      backend,
    });
    let hardEgress: HardEgressEnforcementSnapshot | undefined;
    if (
      isTruthyRuntimeFlag(process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS) &&
      policy.networkPolicy?.defaultAction === "allow"
    ) {
      try {
        hardEgress = runHardEgressHelper({
          action: "allocate",
          allocationId,
          backend,
          trustZoneId: request.trustZoneId,
          agentId: request.agentId,
          taskId: request.taskId,
          policy,
          env: process.env,
        });
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        this.db.recordAudit({
          actor: request.agentId ? { type: "agent", id: request.agentId } : { type: "runtime" },
          action: "network_policy.hard_egress_unavailable",
          objectType: "trust_zone",
          objectId: request.trustZoneId,
          payload: {
            backend,
            reason,
          },
        });
        throw new Error(`Sandbox requires host-level egress enforcement: ${reason}`);
      }
      this.db.recordAudit({
        actor: request.agentId ? { type: "agent", id: request.agentId } : { type: "runtime" },
        action: "network_policy.hard_egress_enforced",
        objectType: "trust_zone",
        objectId: request.trustZoneId,
        payload: {
          backend,
          allocationId,
          enforcement: hardEgress,
        },
      });
    }
    const proxyFailure = sandboxNetworkProxyValidationFailure({
      policy,
      backend,
      env: process.env,
    });
    if (proxyFailure) {
      const hardEgressRequired = isTruthyRuntimeFlag(
        process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS,
      );
      this.db.recordAudit({
        actor: request.agentId ? { type: "agent", id: request.agentId } : { type: "runtime" },
        action: hardEgressRequired
          ? "network_policy.hard_egress_unavailable"
          : "network_policy.proxy_required_missing",
        objectType: "trust_zone",
        objectId: request.trustZoneId,
        payload: {
          backend,
          reason: proxyFailure,
        },
      });
      throw new Error(
        hardEgressRequired
          ? `Sandbox requires host-level egress enforcement: ${proxyFailure}`
          : `Sandbox requires a proxy-backed network policy: ${proxyFailure}`,
      );
    }
    const ownerTaskId =
      request.taskId && this.db.getTask(request.taskId) ? request.taskId : undefined;
    try {
      this.db.createResourceLease({
        id: allocationId,
        resourceType: "sandbox",
        resourceId: allocationId,
        ownerTaskId,
        ownerAgentId: request.agentId,
        trustZoneId: request.trustZoneId,
        leaseUntil: request.requirements?.leaseUntil,
        maxRuntimeMs: request.requirements?.maxRuntimeMs,
        maxBytesOut: request.requirements?.maxBytesOut,
        maxTokens: request.requirements?.maxTokens,
        metadata: {
          backend,
          dockerImage: request.requirements?.dockerImage,
          isolation: describeSandboxBackend(backend),
          policy,
          ...(hardEgress ? { hardEgress } : {}),
        },
      });
    } catch (error) {
      if (hardEgress) {
        try {
          runHardEgressHelper({
            action: "release",
            allocationId,
            backend,
            trustZoneId: request.trustZoneId,
            agentId: request.agentId,
            taskId: request.taskId,
            policy,
            enforcement: hardEgress,
            env: process.env,
          });
        } catch {
          // Preserve the original lease error; operators can reconcile by allocation id.
        }
      }
      throw error;
    }
    const lease = this.db.getResourceLease(allocationId);
    this.db.recordAudit({
      actor: request.agentId ? { type: "agent", id: request.agentId } : { type: "runtime" },
      action: "sandbox.allocated",
      objectType: "resource_lease",
      objectId: allocationId,
      payload: {
        taskId: request.taskId,
        trustZoneId: request.trustZoneId,
        backend,
        isolation: describeSandboxBackend(backend),
        policy,
        ...(hardEgress ? { hardEgress } : {}),
      },
    });
    this.allocationBackends.set(allocationId, backend);
    return {
      id: allocationId,
      taskId: request.taskId,
      trustZoneId: request.trustZoneId,
      backend,
      status: "active",
      createdAt: new Date().toISOString(),
      leaseUntil: lease?.leaseUntil ?? request.requirements?.leaseUntil,
    };
  }

  releaseSandbox(allocationId: string): boolean {
    const lease = this.db.getResourceLease(allocationId);
    const metadata = readSandboxLeaseMetadata(lease?.metadata);
    if (lease?.status === "active" && metadata.hardEgress && metadata.backend) {
      try {
        runHardEgressHelper({
          action: "release",
          allocationId,
          backend: metadata.backend,
          trustZoneId: lease.trustZoneId,
          agentId: lease.ownerAgentId,
          taskId: lease.ownerTaskId,
          policy: metadata.policy,
          enforcement: metadata.hardEgress,
          env: process.env,
        });
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        this.db.recordAudit({
          actor: { type: "runtime" },
          action: "network_policy.hard_egress_release_failed",
          objectType: "resource_lease",
          objectId: allocationId,
          payload: {
            reason,
            enforcement: metadata.hardEgress,
          },
        });
        throw new Error(`Sandbox hard egress release failed: ${reason}`);
      }
    }
    const released = this.db.releaseResourceLease(allocationId);
    if (released) {
      this.allocationBackends.delete(allocationId);
      this.db.recordAudit({
        actor: { type: "runtime" },
        action: "sandbox.released",
        objectType: "resource_lease",
        objectId: allocationId,
      });
    }
    return released;
  }

  async runCommand(request: SandboxCommandRequest): Promise<SandboxCommandResult> {
    const lease = this.db.getResourceLease(request.allocationId);
    if (!lease || lease.resourceType !== "sandbox" || lease.status !== "active") {
      this.db.recordAudit({
        actor: { type: "runtime" },
        action: "sandbox.command_denied_inactive_allocation",
        objectType: "resource_lease",
        objectId: request.allocationId,
      });
      throw new Error(`Sandbox allocation is not active: ${request.allocationId}`);
    }
    const command = request.command.trim();
    if (!command) {
      throw new Error("Sandbox command is required");
    }
    const metadata = readSandboxLeaseMetadata(lease.metadata);
    const backend =
      request.backend ?? this.allocationBackends.get(request.allocationId) ?? metadata.backend;
    if (!backend) {
      this.db.recordAudit({
        actor: { type: "runtime" },
        action: "sandbox.command_failed",
        objectType: "resource_lease",
        objectId: request.allocationId,
        payload: { error: "allocation backend unavailable" },
      });
      throw new Error(
        `Sandbox allocation backend is not available for command execution: ${request.allocationId}`,
      );
    }
    let spawnPlan: SandboxSpawnPlan;
    try {
      spawnPlan = buildSandboxSpawnPlan({
        backend,
        command,
        args: request.args ?? [],
        cwd: request.cwd,
        env: request.env,
        stdin: request.stdin !== undefined,
        dockerImage:
          request.dockerImage ??
          metadata.dockerImage ??
          process.env.OPENCLAW_SPARSEKERNEL_DOCKER_IMAGE,
        dockerPolicy: metadata.policy?.docker,
      });
    } catch (error) {
      this.db.recordAudit({
        actor: { type: "runtime" },
        action: "sandbox.command_failed",
        objectType: "resource_lease",
        objectId: request.allocationId,
        payload: {
          backend,
          error: error instanceof Error ? error.message : String(error),
        },
      });
      throw error;
    }
    const requestedTimeoutMs = request.timeoutMs ?? Number(lease.maxRuntimeMs ?? 30_000);
    const leaseTimeoutMs = Number(lease.maxRuntimeMs ?? requestedTimeoutMs);
    const timeoutMs = Math.max(1, Math.min(requestedTimeoutMs || 30_000, leaseTimeoutMs || 30_000));
    const requestedOutputBytes = request.maxOutputBytes ?? Number(lease.maxBytesOut ?? 256 * 1024);
    const leaseOutputBytes = Number(lease.maxBytesOut ?? requestedOutputBytes);
    const maxOutputBytes = Math.max(
      1,
      Math.min(requestedOutputBytes || 256 * 1024, leaseOutputBytes || 256 * 1024),
    );
    this.db.recordAudit({
      actor: { type: "runtime" },
      action: "sandbox.command_started",
      objectType: "resource_lease",
      objectId: request.allocationId,
      payload: {
        command,
        args: request.args ?? [],
        trustZoneId: lease.trustZoneId,
        backend,
        isolation: spawnPlan.isolation,
      },
    });
    try {
      const result = await runSandboxSpawnPlan({
        allocationId: request.allocationId,
        backend,
        spawnPlan,
        cwd: request.cwd,
        env: request.env,
        stdin: request.stdin,
        signal: request.signal,
        timeoutMs,
        maxOutputBytes,
      });
      this.db.recordUsage({
        resourceType: "sandbox_runtime",
        amount: result.durationMs,
        unit: "ms",
      });
      this.db.recordUsage({
        resourceType: "sandbox_output",
        amount: Buffer.byteLength(result.stdout) + Buffer.byteLength(result.stderr),
        unit: "byte",
      });
      this.db.recordAudit({
        actor: { type: "runtime" },
        action:
          result.exitCode === 0 && !result.timedOut
            ? "sandbox.command_completed"
            : "sandbox.command_failed",
        objectType: "resource_lease",
        objectId: request.allocationId,
        payload: {
          exitCode: result.exitCode,
          signal: result.signal,
          timedOut: result.timedOut,
          durationMs: result.durationMs,
          stdoutBytes: Buffer.byteLength(result.stdout),
          stderrBytes: Buffer.byteLength(result.stderr),
        },
      });
      return result;
    } catch (error) {
      this.db.recordAudit({
        actor: { type: "runtime" },
        action: "sandbox.command_failed",
        objectType: "resource_lease",
        objectId: request.allocationId,
        payload: { error: error instanceof Error ? error.message : String(error) },
      });
      throw error;
    }
  }
}
