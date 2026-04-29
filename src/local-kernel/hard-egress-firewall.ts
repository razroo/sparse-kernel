import { spawnSync } from "node:child_process";
import net from "node:net";
import { resolveNetworkPolicyProxyRef } from "./network-policy.js";
import type {
  HardEgressEnforcementSnapshot,
  SandboxBackendKind,
  SandboxPolicySnapshot,
} from "./sandbox-broker.js";

export type BuiltinFirewallPlatform = "linux_iptables" | "darwin_pf" | "windows_advfirewall";

export type BuiltinFirewallScope =
  | { kind: "uid"; value: string }
  | { kind: "gid"; value: string }
  | { kind: "program"; value: string };

export type BuiltinFirewallCommand = {
  command: string;
  args: string[];
};

export type BuiltinFirewallEgressPlan = {
  platform: BuiltinFirewallPlatform;
  enforcementId: string;
  scope: BuiltinFirewallScope;
  allowedCidrs: string[];
  commands: BuiltinFirewallCommand[];
  releaseCommands: BuiltinFirewallCommand[];
  description: string;
  limitations: string[];
};

type BuiltinFirewallRunParams = {
  action: "allocate" | "release";
  allocationId: string;
  backend: SandboxBackendKind;
  policy?: SandboxPolicySnapshot;
  enforcement?: HardEgressEnforcementSnapshot;
  env: NodeJS.ProcessEnv;
};

function truthy(value: string | undefined): boolean {
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

function normalizePlatform(raw: string | undefined): BuiltinFirewallPlatform {
  const normalized = raw?.trim().toLowerCase();
  if (normalized) {
    switch (normalized) {
      case "linux":
      case "linux-iptables":
      case "linux_iptables":
      case "iptables":
        return "linux_iptables";
      case "darwin":
      case "macos":
      case "macos-pf":
      case "darwin-pf":
      case "darwin_pf":
      case "pf":
        return "darwin_pf";
      case "win32":
      case "windows":
      case "windows-advfirewall":
      case "windows_advfirewall":
      case "advfirewall":
        return "windows_advfirewall";
      default:
        throw new Error(`unsupported builtin firewall platform: ${raw}`);
    }
  }
  switch (process.platform) {
    case "linux":
      return "linux_iptables";
    case "darwin":
      return "darwin_pf";
    case "win32":
      return "windows_advfirewall";
    default:
      throw new Error(`unsupported builtin firewall platform: ${process.platform}`);
  }
}

function parseScopeValue(raw: string): BuiltinFirewallScope {
  const trimmed = raw.trim();
  const separator = trimmed.includes(":") ? ":" : trimmed.includes("=") ? "=" : "";
  if (!separator) {
    throw new Error(
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE must be uid:<id>, gid:<id>, or program:<path>",
    );
  }
  const [kindRaw = "", ...valueParts] = trimmed.split(separator);
  const value = valueParts.join(separator).trim();
  const kind = kindRaw.trim().toLowerCase();
  if (!value) {
    throw new Error("builtin firewall scope is missing a value");
  }
  if (kind === "uid" || kind === "gid") {
    if (!/^\d+$/.test(value)) {
      throw new Error(`builtin firewall ${kind} scope must be numeric`);
    }
    return { kind, value };
  }
  if (kind === "program") {
    if (!value.startsWith("/") && !/^[A-Za-z]:[\\/]/.test(value)) {
      throw new Error("builtin firewall program scope must be an absolute path");
    }
    return { kind, value };
  }
  throw new Error(`unsupported builtin firewall scope kind: ${kind}`);
}

function resolveFirewallScope(env: NodeJS.ProcessEnv): BuiltinFirewallScope {
  const raw = env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE?.trim();
  if (raw) {
    return parseScopeValue(raw);
  }
  const uid = env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_UID?.trim();
  if (uid) {
    return parseScopeValue(`uid:${uid}`);
  }
  const gid = env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_GID?.trim();
  if (gid) {
    return parseScopeValue(`gid:${gid}`);
  }
  const program = env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PROGRAM?.trim();
  if (program) {
    return parseScopeValue(`program:${program}`);
  }
  throw new Error("builtin firewall manager requires OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE");
}

function normalizeIpOrCidr(value: string): string | undefined {
  const raw = value.trim();
  if (!raw || raw.includes("*")) {
    return undefined;
  }
  const unwrapped = raw.startsWith("[") && raw.endsWith("]") ? raw.slice(1, -1) : raw;
  const ipVersion = net.isIP(unwrapped);
  if (ipVersion === 4) {
    return `${unwrapped}/32`;
  }
  if (ipVersion === 6) {
    return `${unwrapped}/128`;
  }
  const [address, prefixRaw] = unwrapped.split("/");
  if (!address || prefixRaw === undefined) {
    return undefined;
  }
  const cidrVersion = net.isIP(address);
  if (cidrVersion !== 4 && cidrVersion !== 6) {
    return undefined;
  }
  const prefix = Number.parseInt(prefixRaw, 10);
  const maxPrefix = cidrVersion === 4 ? 32 : 128;
  if (!Number.isFinite(prefix) || prefix < 0 || prefix > maxPrefix) {
    return undefined;
  }
  return `${address}/${prefix}`;
}

function cidrFamily(cidr: string): 4 | 6 {
  const [address = ""] = cidr.split("/");
  return net.isIP(address) === 6 ? 6 : 4;
}

function collectFirewallAllowedCidrs(policy: SandboxPolicySnapshot | undefined): string[] {
  const networkPolicy = policy?.networkPolicy;
  if (!networkPolicy) {
    throw new Error("builtin firewall manager requires a network policy snapshot");
  }
  const allowed = new Set<string>();
  const proxyDecision = resolveNetworkPolicyProxyRef(networkPolicy.proxyRef);
  if (proxyDecision.ok) {
    allowed.add("127.0.0.0/8");
    allowed.add("::1/128");
  } else if (networkPolicy.proxyRef) {
    throw new Error(`builtin firewall manager cannot use proxy_ref: ${proxyDecision.reason}`);
  }
  for (const host of networkPolicy.allowedHosts ?? []) {
    const cidr = normalizeIpOrCidr(host);
    if (!cidr) {
      throw new Error(
        "builtin firewall manager accepts only IP/CIDR allowed_hosts; route hostnames through a loopback proxy_ref",
      );
    }
    allowed.add(cidr);
  }
  if (allowed.size === 0) {
    throw new Error(
      "builtin firewall manager requires a loopback proxy_ref or IP/CIDR allowed_hosts",
    );
  }
  return [...allowed].sort();
}

function linuxOwnerArgs(scope: BuiltinFirewallScope): string[] {
  if (scope.kind === "uid") {
    return ["-m", "owner", "--uid-owner", scope.value];
  }
  if (scope.kind === "gid") {
    return ["-m", "owner", "--gid-owner", scope.value];
  }
  throw new Error("linux iptables firewall manager supports only uid/gid scopes");
}

function firewallCommand(command: string, args: string[]): BuiltinFirewallCommand {
  return { command, args };
}

function buildLinuxIptablesPlan(params: {
  allocationId: string;
  scope: BuiltinFirewallScope;
  allowedCidrs: string[];
  env: NodeJS.ProcessEnv;
}): Pick<BuiltinFirewallEgressPlan, "commands" | "releaseCommands" | "limitations"> {
  const iptables = params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_IPTABLES?.trim() || "iptables";
  const ip6tables = params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_IP6TABLES?.trim() || "ip6tables";
  const ownerArgs = linuxOwnerArgs(params.scope);
  const comment = `openclaw-sk:${params.allocationId}`;
  const addDrop4 = firewallCommand(iptables, [
    "-I",
    "OUTPUT",
    "1",
    ...ownerArgs,
    "-m",
    "comment",
    "--comment",
    comment,
    "-j",
    "REJECT",
  ]);
  const addDrop6 = firewallCommand(ip6tables, [
    "-I",
    "OUTPUT",
    "1",
    ...ownerArgs,
    "-m",
    "comment",
    "--comment",
    comment,
    "-j",
    "REJECT",
  ]);
  const deleteDrop4 = firewallCommand(iptables, [
    "-D",
    "OUTPUT",
    ...ownerArgs,
    "-m",
    "comment",
    "--comment",
    comment,
    "-j",
    "REJECT",
  ]);
  const deleteDrop6 = firewallCommand(ip6tables, [
    "-D",
    "OUTPUT",
    ...ownerArgs,
    "-m",
    "comment",
    "--comment",
    comment,
    "-j",
    "REJECT",
  ]);
  const addAllows = params.allowedCidrs.map((cidr) => {
    const command = cidrFamily(cidr) === 6 ? ip6tables : iptables;
    return firewallCommand(command, [
      "-I",
      "OUTPUT",
      "1",
      ...ownerArgs,
      "-d",
      cidr,
      "-m",
      "comment",
      "--comment",
      comment,
      "-j",
      "ACCEPT",
    ]);
  });
  const deleteAllows = params.allowedCidrs.map((cidr) => {
    const command = cidrFamily(cidr) === 6 ? ip6tables : iptables;
    return firewallCommand(command, [
      "-D",
      "OUTPUT",
      ...ownerArgs,
      "-d",
      cidr,
      "-m",
      "comment",
      "--comment",
      comment,
      "-j",
      "ACCEPT",
    ]);
  });
  return {
    commands: [addDrop4, addDrop6, ...addAllows],
    releaseCommands: [...deleteAllows, deleteDrop4, deleteDrop6],
    limitations: [
      "Requires iptables/ip6tables owner match support and administrative privileges.",
      "The UID/GID scope must be dedicated to this SparseKernel allocation or policy can leak between processes sharing the scope.",
      "Hostname allowlists must be enforced by a proxy; firewall rules accept only IP/CIDR destinations.",
    ],
  };
}

export function isBuiltinFirewallHardEgressHelper(command: string): boolean {
  const normalized = command.trim().toLowerCase();
  return (
    normalized === "builtin-firewall" ||
    normalized === "builtin:firewall" ||
    normalized === "sparsekernel:builtin-firewall" ||
    normalized === "openclaw:sparsekernel:builtin-firewall"
  );
}

export function buildBuiltinFirewallEgressPlan(params: {
  allocationId: string;
  backend: SandboxBackendKind;
  policy?: SandboxPolicySnapshot;
  env: NodeJS.ProcessEnv;
}): BuiltinFirewallEgressPlan {
  if (!truthy(params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED)) {
    throw new Error(
      "builtin firewall manager requires OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED=1",
    );
  }
  const platform = normalizePlatform(params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM);
  const scope = resolveFirewallScope(params.env);
  const allowedCidrs = collectFirewallAllowedCidrs(params.policy);
  const enforcementId = `builtin-firewall:${platform}:${params.allocationId}`;
  if (platform === "linux_iptables") {
    const linux = buildLinuxIptablesPlan({
      allocationId: params.allocationId,
      scope,
      allowedCidrs,
      env: params.env,
    });
    return {
      platform,
      enforcementId,
      scope,
      allowedCidrs,
      commands: linux.commands,
      releaseCommands: linux.releaseCommands,
      description: `Linux iptables owner-match allowlist for ${scope.kind}:${scope.value}`,
      limitations: linux.limitations,
    };
  }
  if (platform === "darwin_pf") {
    throw new Error(
      "builtin firewall manager does not install macOS pf anchors yet; use an external helper, VM, bwrap, or Docker no-network backend",
    );
  }
  throw new Error(
    "builtin firewall manager does not provide Windows allowlist semantics yet; use an external helper, VM, bwrap, or Docker no-network backend",
  );
}

function executeFirewallCommands(params: {
  commands: BuiltinFirewallCommand[];
  env: NodeJS.ProcessEnv;
  phase: string;
}): void {
  const timeout = Number.parseInt(
    params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_COMMAND_TIMEOUT_MS?.trim() || "30000",
    10,
  );
  const timeoutMs = Number.isFinite(timeout) && timeout > 0 ? timeout : 30_000;
  for (const command of params.commands) {
    const result = spawnSync(command.command, command.args, {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: timeoutMs,
    });
    if (result.error) {
      throw result.error;
    }
    if (result.status !== 0) {
      throw new Error(
        `builtin firewall ${params.phase} command failed (${command.command} ${command.args.join(
          " ",
        )}): ${result.stderr?.trim() || result.stdout?.trim() || "no output"}`,
      );
    }
  }
}

function bestEffortFirewallRelease(params: {
  commands: BuiltinFirewallCommand[];
  env: NodeJS.ProcessEnv;
}): void {
  for (const command of params.commands) {
    spawnSync(command.command, command.args, {
      encoding: "utf8",
      stdio: ["ignore", "ignore", "ignore"],
      timeout: 10_000,
    });
  }
}

function assertFirewallScopeMatchesSandboxRunner(params: {
  backend: SandboxBackendKind;
  scope: BuiltinFirewallScope;
}): void {
  if (params.backend !== "local/no_isolation") {
    throw new Error(
      `builtin firewall manager cannot bind firewall scope to sandbox backend ${params.backend}; use builtin no-network verification or an external helper`,
    );
  }
  if (params.scope.kind === "uid") {
    const getuid = process.getuid;
    if (typeof getuid !== "function") {
      throw new Error("builtin firewall manager cannot verify local runner uid on this platform");
    }
    if (String(getuid()) !== params.scope.value) {
      throw new Error(
        "builtin firewall uid scope does not match the local sandbox runner uid; SparseKernel cannot claim egress enforcement for this allocation",
      );
    }
    return;
  }
  if (params.scope.kind === "gid") {
    const getgid = process.getgid;
    if (typeof getgid !== "function") {
      throw new Error("builtin firewall manager cannot verify local runner gid on this platform");
    }
    if (String(getgid()) !== params.scope.value) {
      throw new Error(
        "builtin firewall gid scope does not match the local sandbox runner gid; SparseKernel cannot claim egress enforcement for this allocation",
      );
    }
    return;
  }
  throw new Error(
    "builtin firewall program scope is not tied to the local sandbox runner in v0; use an external helper",
  );
}

function readReleaseCommands(
  enforcement: HardEgressEnforcementSnapshot | undefined,
): BuiltinFirewallCommand[] {
  const releaseCommands = enforcement?.firewall?.releaseCommands;
  if (!Array.isArray(releaseCommands)) {
    return [];
  }
  return releaseCommands.filter((entry): entry is BuiltinFirewallCommand =>
    Boolean(
      entry &&
      typeof entry.command === "string" &&
      Array.isArray(entry.args) &&
      entry.args.every((arg) => typeof arg === "string"),
    ),
  );
}

export function runBuiltinFirewallHardEgressHelper(
  params: BuiltinFirewallRunParams,
): HardEgressEnforcementSnapshot | undefined {
  if (params.action === "release") {
    const releaseCommands = readReleaseCommands(params.enforcement);
    if (releaseCommands.length > 0) {
      executeFirewallCommands({
        commands: releaseCommands,
        env: params.env,
        phase: "release",
      });
    }
    return undefined;
  }
  const plan = buildBuiltinFirewallEgressPlan({
    allocationId: params.allocationId,
    backend: params.backend,
    policy: params.policy,
    env: params.env,
  });
  if (!truthy(params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY)) {
    throw new Error(
      `builtin firewall manager generated a ${plan.platform} plan but will not mutate the host firewall without OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1`,
    );
  }
  assertFirewallScopeMatchesSandboxRunner({ backend: params.backend, scope: plan.scope });
  try {
    executeFirewallCommands({
      commands: plan.commands,
      env: params.env,
      phase: "apply",
    });
  } catch (error) {
    bestEffortFirewallRelease({ commands: plan.releaseCommands, env: params.env });
    throw error;
  }
  return {
    helper: "builtin-firewall",
    enforcementId: plan.enforcementId,
    boundary: "host_firewall",
    description: plan.description,
    firewall: {
      platform: plan.platform,
      scope: `${plan.scope.kind}:${plan.scope.value}`,
      allowedCidrs: plan.allowedCidrs,
      releaseCommands: plan.releaseCommands,
      applied: true,
      limitations: plan.limitations,
    },
  };
}
