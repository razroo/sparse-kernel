import { Buffer } from "node:buffer";
import { spawn, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import type { LocalKernelDatabase } from "./database.js";
import {
  isBuiltinFirewallHardEgressHelper,
  runBuiltinFirewallHardEgressHelper,
} from "./hard-egress-firewall.js";
import { resolveNetworkPolicyProxyRef } from "./network-policy.js";
import type {
  DockerSandboxPolicy,
  HardEgressEnforcementSnapshot,
  SandboxBackendKind,
  SandboxIsolationProfileId,
  SandboxPolicySnapshot,
  SandboxWorkerIdentitySnapshot,
} from "./sandbox-contracts.js";
import type { SandboxAllocationRecord } from "./types.js";

export type {
  DockerSandboxPolicy,
  HardEgressEnforcementSnapshot,
  SandboxBackendKind,
  SandboxIsolationProfileId,
  SandboxPolicySnapshot,
} from "./sandbox-contracts.js";

export type SandboxAllocationRequest = {
  taskId: string;
  agentId?: string;
  trustZoneId: string;
  requirements?: {
    backend?: SandboxBackendKind;
    dockerImage?: string;
    isolationProfile?: SandboxIsolationProfileId;
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
  workerIdentity?: SandboxWorkerIdentitySnapshot;
};

export interface SandboxBroker {
  allocateSandbox(request: SandboxAllocationRequest): SandboxAllocationRecord;
  releaseSandbox(allocationId: string): boolean;
  runCommand?(request: SandboxCommandRequest): Promise<SandboxCommandResult>;
}

export type SandboxBackendProbe = {
  backend: SandboxBackendKind;
  available: boolean;
  command?: string;
  hardBoundary: boolean;
  isolation: string;
  notes: string[];
};

function commandAvailable(command: string): boolean {
  const result = spawnSync(command, ["--version"], { stdio: "ignore" });
  if (result.error && (result.error as NodeJS.ErrnoException).code === "ENOENT") {
    return false;
  }
  return result.status === 0 || result.status === 1;
}

function firstAvailableCommand(
  commands: string[],
  available: (command: string) => boolean = commandAvailable,
): string | undefined {
  return commands.find((command) => available(command));
}

function isExternalSandboxBackend(
  backend: SandboxBackendKind,
): backend is "ssh" | "openshell" | "vm" | "other" {
  return backend === "ssh" || backend === "openshell" || backend === "vm" || backend === "other";
}

function externalSandboxEnvKey(backend: SandboxBackendKind): string {
  return backend.toUpperCase().replace(/[^A-Z0-9]+/g, "_");
}

function resolveExternalSandboxWrapper(
  backend: SandboxBackendKind,
  env: NodeJS.ProcessEnv = process.env,
):
  | { command: string; args: string[]; hardBoundary: boolean; boundaryDescription?: string }
  | undefined {
  if (!isExternalSandboxBackend(backend)) {
    return undefined;
  }
  const key = externalSandboxEnvKey(backend);
  const command =
    env[`OPENCLAW_RUNTIME_SANDBOX_${key}_COMMAND`]?.trim() ||
    env.OPENCLAW_RUNTIME_SANDBOX_REMOTE_COMMAND?.trim();
  if (!command) {
    return undefined;
  }
  const args = readJsonOrWhitespaceArgs(
    env[`OPENCLAW_RUNTIME_SANDBOX_${key}_ARGS`] ?? env.OPENCLAW_RUNTIME_SANDBOX_REMOTE_ARGS,
    `OPENCLAW_RUNTIME_SANDBOX_${key}_ARGS`,
  );
  const boundary =
    env[`OPENCLAW_RUNTIME_SANDBOX_${key}_BOUNDARY`]?.trim() ||
    env.OPENCLAW_RUNTIME_SANDBOX_REMOTE_BOUNDARY?.trim();
  const hardBoundary =
    boundary === "host_firewall" ||
    boundary === "egress_proxy" ||
    boundary === "vm_firewall" ||
    boundary === "platform_enforcer";
  return {
    command,
    args,
    hardBoundary,
    ...(boundary ? { boundaryDescription: boundary } : {}),
  };
}

function sandboxBackendProvidesRequiredIsolation(
  backend: SandboxBackendKind,
  env: NodeJS.ProcessEnv,
): boolean {
  if (backend === "local/no_isolation") {
    return false;
  }
  if (isExternalSandboxBackend(backend)) {
    return resolveExternalSandboxWrapper(backend, env)?.hardBoundary === true;
  }
  return true;
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
      return true;
    case "ssh":
    case "openshell":
    case "vm":
    case "other":
      return Boolean(resolveExternalSandboxWrapper(backend));
  }
}

export function probeSandboxBackends(
  input: {
    commandAvailable?: (command: string) => boolean;
    env?: NodeJS.ProcessEnv;
  } = {},
): SandboxBackendProbe[] {
  const available = input.commandAvailable ?? commandAvailable;
  const env = input.env ?? process.env;
  const bwrap = firstAvailableCommand(["bwrap", "bubblewrap"], available);
  const minijail = firstAvailableCommand(["minijail0", "minijail"], available);
  const docker = firstAvailableCommand(["docker"], available);
  const vmWrapper = resolveExternalSandboxWrapper("vm", env);
  const sshWrapper = resolveExternalSandboxWrapper("ssh", env);
  const openshellWrapper = resolveExternalSandboxWrapper("openshell", env);
  return [
    {
      backend: "local/no_isolation",
      available: true,
      hardBoundary: false,
      isolation: describeSandboxBackend("local/no_isolation"),
      notes: ["Trusted operations only; no host isolation is provided."],
    },
    {
      backend: "bwrap",
      available: Boolean(bwrap),
      ...(bwrap ? { command: bwrap } : {}),
      hardBoundary: Boolean(bwrap),
      isolation: describeSandboxBackend("bwrap"),
      notes: bwrap
        ? ["Requires kernel namespace support and broker-selected bind policy."]
        : ["Install bubblewrap to enable this backend."],
    },
    {
      backend: "minijail",
      available: Boolean(minijail),
      ...(minijail ? { command: minijail } : {}),
      hardBoundary: Boolean(minijail),
      isolation: describeSandboxBackend("minijail"),
      notes: minijail
        ? ["Requires host minijail policy support for the selected trust zone."]
        : ["Install minijail to enable this backend."],
    },
    {
      backend: "docker",
      available: Boolean(docker),
      ...(docker ? { command: docker } : {}),
      hardBoundary: Boolean(docker),
      isolation: describeSandboxBackend("docker"),
      notes: docker
        ? ["Container isolation depends on daemon policy; not a per-agent default."]
        : ["Install/configure Docker and an explicit image to enable this backend."],
    },
    {
      backend: "vm",
      available: Boolean(vmWrapper),
      ...(vmWrapper ? { command: vmWrapper.command } : {}),
      hardBoundary: Boolean(vmWrapper?.hardBoundary),
      isolation: describeSandboxBackend("vm"),
      notes: vmWrapper
        ? [
            vmWrapper.hardBoundary
              ? `Operator VM wrapper declares boundary ${vmWrapper.boundaryDescription}.`
              : "Operator VM wrapper is configured; SparseKernel does not assume a hard boundary unless the wrapper declares one.",
          ]
        : ["Set OPENCLAW_RUNTIME_SANDBOX_VM_COMMAND to enable the VM wrapper backend."],
    },
    {
      backend: "ssh",
      available: Boolean(sshWrapper),
      ...(sshWrapper ? { command: sshWrapper.command } : {}),
      hardBoundary: Boolean(sshWrapper?.hardBoundary),
      isolation: describeSandboxBackend("ssh"),
      notes: sshWrapper
        ? [
            sshWrapper.hardBoundary
              ? `Operator SSH wrapper declares boundary ${sshWrapper.boundaryDescription}.`
              : "Operator SSH wrapper is configured; SSH transport is not itself a sandbox boundary.",
          ]
        : ["Set OPENCLAW_RUNTIME_SANDBOX_SSH_COMMAND to enable the SSH wrapper backend."],
    },
    {
      backend: "openshell",
      available: Boolean(openshellWrapper),
      ...(openshellWrapper ? { command: openshellWrapper.command } : {}),
      hardBoundary: Boolean(openshellWrapper?.hardBoundary),
      isolation: describeSandboxBackend("openshell"),
      notes: openshellWrapper
        ? [
            openshellWrapper.hardBoundary
              ? `Operator OpenShell wrapper declares boundary ${openshellWrapper.boundaryDescription}.`
              : "Operator OpenShell wrapper is configured; SparseKernel does not assume host isolation.",
          ]
        : [
            "Set OPENCLAW_RUNTIME_SANDBOX_OPENSHELL_COMMAND to enable the OpenShell wrapper backend.",
          ],
    },
  ];
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
  workerIdentity?: SandboxWorkerIdentitySnapshot;
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
    ...(isSandboxWorkerIdentitySnapshot(value.workerIdentity)
      ? { workerIdentity: value.workerIdentity }
      : {}),
  };
}

function isSandboxWorkerIdentitySnapshot(value: unknown): value is SandboxWorkerIdentitySnapshot {
  if (!isRecord(value)) {
    return false;
  }
  const source = value.source;
  const scope = value.scope;
  const uid = value.uid;
  const gid = value.gid;
  const sid = value.sid;
  return (
    typeof value.id === "string" &&
    source === "managed_pool" &&
    isRecord(scope) &&
    typeof scope.kind === "string" &&
    typeof scope.value === "string" &&
    (uid === undefined || typeof uid === "number") &&
    (gid === undefined || typeof gid === "number") &&
    (sid === undefined || typeof sid === "string")
  );
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
  requestedProfile?: SandboxIsolationProfileId;
}): SandboxPolicySnapshot {
  const zone = params.db.listTrustZones().find((entry) => entry.id === params.trustZoneId);
  const networkPolicy = params.db.getNetworkPolicyForTrustZone(params.trustZoneId);
  const proxyDecision = resolveNetworkPolicyProxyRef(networkPolicy?.proxyRef);
  const dockerNetworkMode =
    networkPolicy?.defaultAction === "allow" && proxyDecision.ok ? "bridge" : "none";
  const isolationProfile =
    params.requestedProfile ??
    resolveSandboxIsolationProfile({
      trustZoneId: params.trustZoneId,
      filesystemPolicy: zone?.filesystemPolicy,
    });
  return {
    trustZoneId: params.trustZoneId,
    backend: params.backend,
    isolationProfile,
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
            ...(networkPolicy.allowedHosts ? { allowedHosts: networkPolicy.allowedHosts } : {}),
            ...(networkPolicy.deniedCidrs ? { deniedCidrs: networkPolicy.deniedCidrs } : {}),
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

function resolveSandboxIsolationProfile(params: {
  trustZoneId: string;
  filesystemPolicy?: unknown;
}): SandboxIsolationProfileId {
  switch (params.trustZoneId) {
    case "public_web":
    case "authenticated_web":
    case "user_browser_profile":
      return "web_brokered";
    case "local_files_readonly":
      return "readonly_workspace";
    case "local_files_rw":
      return "rw_workspace";
    case "code_execution":
      return "code_execution";
    case "plugin_untrusted":
      return "plugin_untrusted";
    default:
      return readFilesystemMode(params.filesystemPolicy) === "readwrite"
        ? "rw_workspace"
        : "trusted_local";
  }
}

function profileAllowsWorkspaceWrite(profile: SandboxIsolationProfileId): boolean {
  return profile === "rw_workspace" || profile === "code_execution";
}

function profileRequiresIsolatedBackend(
  profile: SandboxIsolationProfileId,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  if (profile === "plugin_untrusted") {
    return (
      !isTruthyRuntimeFlag(env.OPENCLAW_RUNTIME_SANDBOX_ALLOW_LOCAL_UNTRUSTED) &&
      !isTruthyRuntimeFlag(env.OPENCLAW_RUNTIME_PLUGIN_ALLOW_NO_ISOLATION)
    );
  }
  if (profile === "code_execution") {
    return isTruthyRuntimeFlag(env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_ISOLATED_CODE_EXECUTION);
  }
  return false;
}

function selectSandboxBackendForProfile(params: {
  requestedBackend?: SandboxBackendKind;
  profile: SandboxIsolationProfileId;
  env?: NodeJS.ProcessEnv;
}): SandboxBackendKind {
  const env = params.env ?? process.env;
  const requested = params.requestedBackend;
  const requiresIsolation = profileRequiresIsolatedBackend(params.profile, env);
  if (requested) {
    if (requiresIsolation && !sandboxBackendProvidesRequiredIsolation(requested, env)) {
      throw new Error(
        `Sandbox isolation profile ${params.profile} requires an isolated backend; ${requested} is not an asserted isolation boundary`,
      );
    }
    return requested;
  }
  if (!requiresIsolation) {
    return "local/no_isolation";
  }
  const selected = (["bwrap", "minijail", "docker", "vm"] as SandboxBackendKind[]).find(
    (backend) =>
      isSandboxBackendAvailable(backend) && sandboxBackendProvidesRequiredIsolation(backend, env),
  );
  if (!selected) {
    throw new Error(
      `Sandbox isolation profile ${params.profile} requires bwrap, minijail, Docker, or a configured VM backend wrapper`,
    );
  }
  return selected;
}

function minijailProfileArgs(profile: SandboxIsolationProfileId): string[] {
  const profileEnvName = `OPENCLAW_RUNTIME_MINIJAIL_${profile
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")}_FLAGS`;
  return [
    "-p",
    "-v",
    ...readJsonOrWhitespaceArgs(
      process.env.OPENCLAW_RUNTIME_MINIJAIL_HARDENED_FLAGS,
      "OPENCLAW_RUNTIME_MINIJAIL_HARDENED_FLAGS",
    ),
    ...readJsonOrWhitespaceArgs(process.env[profileEnvName], profileEnvName),
  ];
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
      return "operator-supplied SSH command wrapper; SSH transport is not host isolation";
    case "openshell":
      return "operator-supplied OpenShell command wrapper";
    case "vm":
      return "operator-supplied VM command wrapper";
    case "other":
      return "operator-supplied custom sandbox command wrapper";
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
  workerIdentity?: SandboxWorkerIdentitySnapshot;
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
  if (isBuiltinFirewallHardEgressHelper(helper.command)) {
    return runBuiltinFirewallHardEgressHelper({
      action: params.action,
      allocationId: params.allocationId,
      backend: params.backend,
      policy: params.policy,
      enforcement: params.enforcement,
      workerIdentity: params.workerIdentity,
      env: params.env,
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
      { cause: error },
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

function readInteger(value: unknown, name: string): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const parsed = typeof value === "number" ? value : Number.parseInt(String(value).trim(), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function splitListEnv(raw: string | undefined): string[] {
  return (raw ?? "")
    .split(/[,\s]+/)
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function managedIdentityFromRecord(
  value: Record<string, unknown>,
  index: number,
): SandboxWorkerIdentitySnapshot {
  const uid = readInteger(value.uid, "worker identity uid");
  const gid = readInteger(value.gid, "worker identity gid");
  const sid = typeof value.sid === "string" && value.sid.trim() ? value.sid.trim() : undefined;
  const id =
    typeof value.id === "string" && value.id.trim()
      ? value.id.trim()
      : sid
        ? `sid:${sid}`
        : uid !== undefined
          ? `uid:${uid}${gid !== undefined ? `:gid:${gid}` : ""}`
          : `worker:${index}`;
  if (sid) {
    return {
      id,
      source: "managed_pool",
      sid,
      scope: { kind: "sid", value: sid },
    };
  }
  if (uid !== undefined) {
    return {
      id,
      source: "managed_pool",
      uid,
      ...(gid !== undefined ? { gid } : {}),
      scope: { kind: "uid", value: String(uid) },
    };
  }
  if (gid !== undefined) {
    return {
      id,
      source: "managed_pool",
      gid,
      scope: { kind: "gid", value: String(gid) },
    };
  }
  throw new Error("worker identity requires uid, gid, or sid");
}

function readManagedWorkerIdentityPool(env: NodeJS.ProcessEnv): SandboxWorkerIdentitySnapshot[] {
  const rawJson = env.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES?.trim();
  if (rawJson) {
    const parsed = JSON.parse(rawJson) as unknown;
    if (!Array.isArray(parsed)) {
      throw new Error("OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES must be a JSON array");
    }
    return parsed.map((entry, index) => {
      if (!isRecord(entry)) {
        throw new Error("worker identity entries must be objects");
      }
      return managedIdentityFromRecord(entry, index);
    });
  }
  const uids = splitListEnv(env.OPENCLAW_RUNTIME_SANDBOX_WORKER_UIDS);
  const gids = splitListEnv(env.OPENCLAW_RUNTIME_SANDBOX_WORKER_GIDS);
  if (uids.length === 0) {
    return [];
  }
  return uids.map((uid, index) =>
    managedIdentityFromRecord(
      {
        uid,
        ...(gids[index] ? { gid: gids[index] } : {}),
      },
      index,
    ),
  );
}

function shouldUseManagedWorkerIdentity(env: NodeJS.ProcessEnv): boolean {
  const mode = env.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITY_MODE?.trim().toLowerCase();
  return (
    mode === "managed" ||
    Boolean(env.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES?.trim()) ||
    Boolean(env.OPENCLAW_RUNTIME_SANDBOX_WORKER_UIDS?.trim())
  );
}

function claimManagedWorkerIdentity(params: {
  db: LocalKernelDatabase;
  env: NodeJS.ProcessEnv;
  backend: SandboxBackendKind;
}): SandboxWorkerIdentitySnapshot | undefined {
  if (!shouldUseManagedWorkerIdentity(params.env)) {
    return undefined;
  }
  if (params.backend !== "local/no_isolation") {
    throw new Error(
      `managed worker identities are only supported for local/no_isolation command workers, got ${params.backend}`,
    );
  }
  const pool = readManagedWorkerIdentityPool(params.env);
  if (pool.length === 0) {
    throw new Error("managed worker identity mode requires at least one configured identity");
  }
  const active = new Set(
    params.db
      .listResourceLeases({ resourceType: "sandbox", status: "active", limit: 1000 })
      .map((lease) => readSandboxLeaseMetadata(lease.metadata).workerIdentity?.id)
      .filter((id): id is string => Boolean(id)),
  );
  const identity = pool.find((entry) => !active.has(entry.id));
  if (!identity) {
    throw new Error(`managed worker identity pool exhausted: ${pool.length} active`);
  }
  return identity;
}

function readProgramFirewallScope(metadata: SandboxLeaseMetadata): string | undefined {
  const scope = metadata.hardEgress?.firewall?.scope;
  if (!scope?.startsWith("program:")) {
    return undefined;
  }
  const program = scope.slice("program:".length).trim();
  return program || undefined;
}

export function buildSandboxProcessEnv(params: {
  backend: SandboxBackendKind;
  env?: Record<string, string>;
  proxyServer?: string;
}): NodeJS.ProcessEnv {
  const proxyEnv =
    params.proxyServer && params.backend === "local/no_isolation"
      ? {
          HTTP_PROXY: params.proxyServer,
          HTTPS_PROXY: params.proxyServer,
          ALL_PROXY: params.proxyServer,
          NO_PROXY: "127.0.0.1,localhost,::1",
        }
      : {};
  if (params.backend === "local/no_isolation") {
    return { ...process.env, ...proxyEnv, ...(params.env ?? {}) };
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
  isolationProfile?: SandboxIsolationProfileId;
  filesystemPolicy?: unknown;
  resolveCommand?: CommandResolver;
}): SandboxSpawnPlan {
  const resolveCommand = params.resolveCommand ?? firstAvailableCommand;
  const isolationProfile = params.isolationProfile ?? "trusted_local";
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
      const env = buildSandboxProcessEnv({ backend: "bwrap", env: params.env });
      const args = [
        "--die-with-parent",
        "--unshare-all",
        "--new-session",
        "--clearenv",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/home",
        "--dir",
        "/home/openclaw",
      ];
      env.HOME = "/home/openclaw";
      env.TMPDIR = "/tmp";
      for (const [name, value] of Object.entries(env)) {
        if (value !== undefined && validDockerEnvName(name)) {
          args.push("--setenv", name, value);
        }
      }
      for (const path of ["/usr", "/bin", "/lib", "/lib64", "/etc"]) {
        if (fs.existsSync(path)) {
          args.push("--ro-bind", path, path);
        }
      }
      if (params.cwd) {
        const bindFlag = profileAllowsWorkspaceWrite(isolationProfile) ? "--bind" : "--ro-bind";
        args.push(bindFlag, params.cwd, params.cwd, "--chdir", params.cwd);
      }
      args.push("--", params.command, ...params.args);
      return {
        command: binary,
        args,
        isolation: `${describeSandboxBackend(params.backend)}; profile=${isolationProfile}`,
      };
    }
    case "minijail": {
      const binary = resolveCommand(["minijail0", "minijail"]);
      if (!binary) {
        throw new Error("Sandbox backend unavailable: minijail");
      }
      const profileArgs = minijailProfileArgs(isolationProfile);
      return {
        command: binary,
        args: [...profileArgs, "--", params.command, ...params.args],
        isolation: `${describeSandboxBackend(params.backend)}; profile=${isolationProfile}`,
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
        isolation: `${describeSandboxBackend(params.backend)}; profile=${isolationProfile}`,
      };
    }
    case "ssh":
    case "openshell":
    case "vm":
    case "other": {
      const wrapper = resolveExternalSandboxWrapper(params.backend);
      if (!wrapper) {
        throw new Error(
          `Sandbox backend ${params.backend} requires OPENCLAW_RUNTIME_SANDBOX_${externalSandboxEnvKey(
            params.backend,
          )}_COMMAND or OPENCLAW_RUNTIME_SANDBOX_REMOTE_COMMAND.`,
        );
      }
      const request = {
        protocol: "openclaw.sparsekernel.sandbox-command.v1",
        backend: params.backend,
        isolationProfile,
        command: params.command,
        args: params.args,
        ...(params.cwd ? { cwd: params.cwd } : {}),
        env: params.env ?? {},
        stdin: params.stdin === true,
      };
      return {
        command: wrapper.command,
        args: [
          ...wrapper.args,
          "--sparsekernel-request-base64",
          Buffer.from(JSON.stringify(request), "utf8").toString("base64url"),
          "--",
          params.command,
          ...params.args,
        ],
        isolation: `${describeSandboxBackend(params.backend)}; profile=${isolationProfile}${
          wrapper.hardBoundary ? `; declaredBoundary=${wrapper.boundaryDescription}` : ""
        }`,
      };
    }
  }
}

export async function runSandboxSpawnPlan(
  request: SandboxSpawnCommandRequest,
): Promise<SandboxCommandResult> {
  const started = Date.now();
  const timeoutMs = Math.max(1, request.timeoutMs);
  const maxOutputBytes = Math.max(1, request.maxOutputBytes);
  return await new Promise<SandboxCommandResult>((resolve, reject) => {
    const workerIdentity =
      request.backend === "local/no_isolation" ? request.workerIdentity : undefined;
    const child = spawn(request.spawnPlan.command, request.spawnPlan.args, {
      cwd: request.backend === "local/no_isolation" ? request.cwd : undefined,
      env: buildSandboxProcessEnv({ backend: request.backend, env: request.env }),
      stdio: [request.stdin === undefined ? "ignore" : "pipe", "pipe", "pipe"],
      ...(workerIdentity?.uid !== undefined ? { uid: workerIdentity.uid } : {}),
      ...(workerIdentity?.gid !== undefined ? { gid: workerIdentity.gid } : {}),
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

  constructor(
    private readonly db: LocalKernelDatabase,
    private readonly options: { env?: NodeJS.ProcessEnv } = {},
  ) {}

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

    const requestedProfile = resolveSandboxIsolationProfile({
      trustZoneId: request.trustZoneId,
      filesystemPolicy: this.db.listTrustZones().find((entry) => entry.id === request.trustZoneId)
        ?.filesystemPolicy,
    });
    let backend: SandboxBackendKind;
    try {
      backend = selectSandboxBackendForProfile({
        requestedBackend: request.requirements?.backend,
        profile: request.requirements?.isolationProfile ?? requestedProfile,
        env: this.options.env,
      });
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.db.recordAudit({
        actor: request.agentId ? { type: "agent", id: request.agentId } : { type: "runtime" },
        action: "sandbox.allocation_denied_isolation_required",
        objectType: "trust_zone",
        objectId: request.trustZoneId,
        payload: {
          requestedBackend: request.requirements?.backend,
          isolationProfile: request.requirements?.isolationProfile ?? requestedProfile,
          reason,
        },
      });
      throw error;
    }
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
    if (!request.requirements?.backend && backend !== "local/no_isolation") {
      this.db.recordAudit({
        actor: request.agentId ? { type: "agent", id: request.agentId } : { type: "runtime" },
        action: "sandbox.backend_auto_selected",
        objectType: "trust_zone",
        objectId: request.trustZoneId,
        payload: {
          backend,
          isolationProfile: request.requirements?.isolationProfile ?? requestedProfile,
        },
      });
    }
    const allocationId = `sandbox_${crypto.randomUUID()}`;
    const policy = resolveSandboxPolicySnapshot({
      db: this.db,
      trustZoneId: request.trustZoneId,
      backend,
      requestedProfile: request.requirements?.isolationProfile ?? requestedProfile,
    });
    let hardEgress: HardEgressEnforcementSnapshot | undefined;
    let workerIdentity: SandboxWorkerIdentitySnapshot | undefined;
    if (
      isTruthyRuntimeFlag(process.env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS) &&
      policy.networkPolicy?.defaultAction === "allow"
    ) {
      try {
        const helper = resolveHardEgressHelper(process.env);
        if (isBuiltinFirewallHardEgressHelper(helper.command)) {
          workerIdentity = claimManagedWorkerIdentity({
            db: this.db,
            env: process.env,
            backend,
          });
        }
        hardEgress = runHardEgressHelper({
          action: "allocate",
          allocationId,
          backend,
          trustZoneId: request.trustZoneId,
          agentId: request.agentId,
          taskId: request.taskId,
          policy,
          workerIdentity,
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
            ...(workerIdentity ? { workerIdentity } : {}),
          },
        });
        throw new Error(`Sandbox requires host-level egress enforcement: ${reason}`, {
          cause: error,
        });
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
          ...(workerIdentity ? { workerIdentity } : {}),
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
          ...(workerIdentity ? { workerIdentity } : {}),
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
            workerIdentity,
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
        ...(workerIdentity ? { workerIdentity } : {}),
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
          workerIdentity: metadata.workerIdentity,
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
    const programScope = readProgramFirewallScope(metadata);
    if (programScope && command !== programScope) {
      this.db.recordAudit({
        actor: { type: "runtime" },
        action: "sandbox.command_denied_firewall_scope",
        objectType: "resource_lease",
        objectId: request.allocationId,
        payload: { command, requiredProgram: programScope },
      });
      throw new Error(
        `Sandbox command does not match the scoped firewall program: ${programScope}`,
      );
    }
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
        isolationProfile: metadata.policy?.isolationProfile,
        filesystemPolicy: metadata.policy?.filesystemPolicy,
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
        env:
          backend === "local/no_isolation" && metadata.policy?.docker?.proxyServer
            ? {
                HTTP_PROXY: metadata.policy.docker.proxyServer,
                HTTPS_PROXY: metadata.policy.docker.proxyServer,
                ALL_PROXY: metadata.policy.docker.proxyServer,
                NO_PROXY: "127.0.0.1,localhost,::1",
                ...(request.env ?? {}),
              }
            : request.env,
        stdin: request.stdin,
        signal: request.signal,
        timeoutMs,
        maxOutputBytes,
        workerIdentity: backend === "local/no_isolation" ? metadata.workerIdentity : undefined,
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
