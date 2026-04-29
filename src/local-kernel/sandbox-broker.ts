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

export type SandboxCommandResult = {
  allocationId: string;
  exitCode: number | null;
  signal?: NodeJS.Signals;
  stdout: string;
  stderr: string;
  timedOut: boolean;
  durationMs: number;
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
};

type CommandResolver = (commands: string[]) => string | undefined;

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
  };
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

export function buildSandboxSpawnPlan(params: {
  backend: SandboxBackendKind;
  command: string;
  args: string[];
  cwd?: string;
  env?: Record<string, string>;
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
        env.HTTP_PROXY ??= dockerPolicy.proxyServer;
        env.HTTPS_PROXY ??= dockerPolicy.proxyServer;
        env.ALL_PROXY ??= dockerPolicy.proxyServer;
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
    const policy = resolveSandboxPolicySnapshot({
      db: this.db,
      trustZoneId: request.trustZoneId,
      backend,
    });
    const allocationId = `sandbox_${crypto.randomUUID()}`;
    const ownerTaskId =
      request.taskId && this.db.getTask(request.taskId) ? request.taskId : undefined;
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
      },
    });
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
      leaseUntil: request.requirements?.leaseUntil,
    };
  }

  releaseSandbox(allocationId: string): boolean {
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
    const started = Date.now();
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
    return await new Promise<SandboxCommandResult>((resolve, reject) => {
      const child = spawn(spawnPlan.command, spawnPlan.args, {
        cwd: backend === "local/no_isolation" ? request.cwd : undefined,
        env: request.env ? { ...process.env, ...request.env } : process.env,
        stdio: ["ignore", "pipe", "pipe"],
      });
      let timedOut = false;
      let stdout = Buffer.alloc(0);
      let stderr = Buffer.alloc(0);
      const append = (current: Buffer, chunk: Buffer) =>
        Buffer.concat([current, chunk]).subarray(0, maxOutputBytes);
      const timer = setTimeout(() => {
        timedOut = true;
        child.kill("SIGTERM");
      }, timeoutMs);
      child.stdout.on("data", (chunk: Buffer) => {
        stdout = append(stdout, chunk);
      });
      child.stderr.on("data", (chunk: Buffer) => {
        stderr = append(stderr, chunk);
      });
      child.on("error", (error) => {
        clearTimeout(timer);
        this.db.recordAudit({
          actor: { type: "runtime" },
          action: "sandbox.command_failed",
          objectType: "resource_lease",
          objectId: request.allocationId,
          payload: { error: error.message },
        });
        reject(error);
      });
      child.on("close", (exitCode, signal) => {
        clearTimeout(timer);
        const durationMs = Date.now() - started;
        const result: SandboxCommandResult = {
          allocationId: request.allocationId,
          exitCode,
          ...(signal ? { signal } : {}),
          stdout: stdout.toString("utf8"),
          stderr: stderr.toString("utf8"),
          timedOut,
          durationMs,
        };
        this.db.recordUsage({
          resourceType: "sandbox_runtime",
          amount: durationMs,
          unit: "ms",
        });
        this.db.recordUsage({
          resourceType: "sandbox_output",
          amount: stdout.byteLength + stderr.byteLength,
          unit: "byte",
        });
        this.db.recordAudit({
          actor: { type: "runtime" },
          action:
            exitCode === 0 && !timedOut ? "sandbox.command_completed" : "sandbox.command_failed",
          objectType: "resource_lease",
          objectId: request.allocationId,
          payload: {
            exitCode,
            signal,
            timedOut,
            durationMs,
            stdoutBytes: stdout.byteLength,
            stderrBytes: stderr.byteLength,
          },
        });
        resolve(result);
      });
    });
  }
}
