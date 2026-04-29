import { spawn, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import type { LocalKernelDatabase } from "./database.js";
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
    maxRuntimeMs?: number;
    maxBytesOut?: number;
    maxTokens?: number;
    leaseUntil?: string;
  };
};

export type SandboxCommandRequest = {
  allocationId: string;
  backend?: SandboxBackendKind;
  command: string;
  args?: string[];
  cwd?: string;
  env?: Record<string, string>;
  timeoutMs?: number;
  maxOutputBytes?: number;
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

type SandboxSpawnPlan = {
  command: string;
  args: string[];
  isolation: string;
};

function buildSandboxSpawnPlan(params: {
  backend: SandboxBackendKind;
  command: string;
  args: string[];
  cwd?: string;
}): SandboxSpawnPlan {
  switch (params.backend) {
    case "local/no_isolation":
      return {
        command: params.command,
        args: params.args,
        isolation: "local/no_isolation command runner; trusted execution only",
      };
    case "bwrap": {
      const binary = firstAvailableCommand(["bwrap", "bubblewrap"]);
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
        isolation: "bubblewrap process namespace with broker-selected binds",
      };
    }
    case "minijail": {
      const binary = firstAvailableCommand(["minijail0", "minijail"]);
      if (!binary) {
        throw new Error("Sandbox backend unavailable: minijail");
      }
      return {
        command: binary,
        args: ["-p", "-v", "--", params.command, ...params.args],
        isolation: "minijail process jail with backend-defined limits",
      };
    }
    case "docker":
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
        isolation:
          backend === "local/no_isolation"
            ? "accounting only; no hard process, filesystem, or network isolation"
            : "backend-defined",
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
    const lease = this.db.db
      .prepare(
        `SELECT status, trust_zone_id, max_runtime_ms, max_bytes_out
         FROM resource_leases
         WHERE id = ? AND resource_type = 'sandbox'`,
      )
      .get(request.allocationId) as
      | {
          status: string;
          trust_zone_id: string | null;
          max_runtime_ms: number | bigint | null;
          max_bytes_out: number | bigint | null;
        }
      | undefined;
    if (!lease || lease.status !== "active") {
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
    const backend = request.backend ?? this.allocationBackends.get(request.allocationId);
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
    const requestedTimeoutMs = request.timeoutMs ?? Number(lease.max_runtime_ms ?? 30_000);
    const leaseTimeoutMs = Number(lease.max_runtime_ms ?? requestedTimeoutMs);
    const timeoutMs = Math.max(1, Math.min(requestedTimeoutMs || 30_000, leaseTimeoutMs || 30_000));
    const requestedOutputBytes =
      request.maxOutputBytes ?? Number(lease.max_bytes_out ?? 256 * 1024);
    const leaseOutputBytes = Number(lease.max_bytes_out ?? requestedOutputBytes);
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
        trustZoneId: lease.trust_zone_id,
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
