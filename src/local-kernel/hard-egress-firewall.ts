import { spawnSync } from "node:child_process";
import net from "node:net";
import { resolveNetworkPolicyProxyRef } from "./network-policy.js";
import type {
  BuiltinFirewallCommand,
  BuiltinFirewallPlatform,
  BuiltinFirewallScope,
  HardEgressEnforcementSnapshot,
  SandboxBackendKind,
  SandboxPolicySnapshot,
  SandboxWorkerIdentitySnapshot,
} from "./sandbox-contracts.js";
export type {
  BuiltinFirewallCommand,
  BuiltinFirewallPlatform,
  BuiltinFirewallScope,
  SandboxWorkerIdentitySnapshot,
} from "./sandbox-contracts.js";

export type BuiltinFirewallEgressPlan = {
  platform: BuiltinFirewallPlatform;
  enforcementId: string;
  scope: BuiltinFirewallScope;
  allowedCidrs: string[];
  proxyDelegatedHosts: string[];
  protocolCoverage: "all_ip" | "tcp_udp";
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
  workerIdentity?: SandboxWorkerIdentitySnapshot;
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
      "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE must be uid:<id>, gid:<id>, sid:<sid>, or program:<path>",
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
  if (kind === "sid") {
    if (!/^S-\d-\d+(?:-\d+)+$/i.test(value)) {
      throw new Error("builtin firewall sid scope must be a Windows SID");
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

function resolveFirewallScope(params: {
  env: NodeJS.ProcessEnv;
  workerIdentity?: SandboxWorkerIdentitySnapshot;
}): BuiltinFirewallScope {
  if (params.workerIdentity) {
    return params.workerIdentity.scope;
  }
  const env = params.env;
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
  const sid = env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SID?.trim();
  if (sid) {
    return parseScopeValue(`sid:${sid}`);
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

type FirewallDestinationPlan = {
  allowedCidrs: string[];
  proxyDelegatedHosts: string[];
};

function collectFirewallDestinations(
  policy: SandboxPolicySnapshot | undefined,
): FirewallDestinationPlan {
  const networkPolicy = policy?.networkPolicy;
  if (!networkPolicy) {
    throw new Error("builtin firewall manager requires a network policy snapshot");
  }
  const allowed = new Set<string>();
  const proxyDelegatedHosts = new Set<string>();
  const proxyDecision = resolveNetworkPolicyProxyRef(networkPolicy.proxyRef);
  if (proxyDecision.ok) {
    allowed.add("127.0.0.0/8");
    allowed.add("::1/128");
  } else if (networkPolicy.proxyRef) {
    throw new Error(`builtin firewall manager cannot use proxy_ref: ${proxyDecision.reason}`);
  }
  for (const host of networkPolicy.allowedHosts ?? []) {
    const cidr = normalizeIpOrCidr(host);
    if (cidr) {
      allowed.add(cidr);
    } else if (proxyDecision.ok) {
      proxyDelegatedHosts.add(host.trim().toLowerCase());
    } else {
      throw new Error(
        "builtin firewall manager accepts only IP/CIDR allowed_hosts; route hostnames through a loopback proxy_ref",
      );
    }
  }
  if (allowed.size === 0) {
    throw new Error(
      "builtin firewall manager requires a loopback proxy_ref or IP/CIDR allowed_hosts",
    );
  }
  return {
    allowedCidrs: [...allowed].sort(),
    proxyDelegatedHosts: [...proxyDelegatedHosts].sort(),
  };
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

function shellSingleQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function sanitizeFirewallToken(value: string): string {
  const token = value.replace(/[^A-Za-z0-9_.-]/g, "_");
  return token || "sandbox";
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
      "Hostname allowlists are enforced only when the trust zone also uses a loopback proxy_ref; firewall rules accept only loopback and IP/CIDR destinations.",
    ],
  };
}

function buildDarwinPfPlan(params: {
  allocationId: string;
  scope: BuiltinFirewallScope;
  allowedCidrs: string[];
  env: NodeJS.ProcessEnv;
}): Pick<BuiltinFirewallEgressPlan, "commands" | "releaseCommands" | "limitations"> {
  if (params.scope.kind !== "uid" && params.scope.kind !== "gid") {
    throw new Error("macOS pf firewall manager supports only uid/gid scopes");
  }
  const pfctl = params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PFCTL?.trim() || "pfctl";
  const anchorRoot =
    params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PF_ANCHOR_ROOT?.trim() ||
    "com.apple/openclaw_sparsekernel";
  const anchor = `${anchorRoot}/${sanitizeFirewallToken(params.allocationId)}`;
  const scopeRule =
    params.scope.kind === "uid" ? `user ${params.scope.value}` : `group ${params.scope.value}`;
  const cidrSet = `{ ${params.allowedCidrs.join(" ")} }`;
  const rules = [
    `pass out quick proto { tcp udp } ${scopeRule} to ${cidrSet} keep state`,
    `block drop out quick proto { tcp udp } ${scopeRule}`,
    "",
  ].join("\n");
  const loadScript = `printf %s ${shellSingleQuote(rules)} | ${shellSingleQuote(
    pfctl,
  )} -a ${shellSingleQuote(anchor)} -f - && ${shellSingleQuote(pfctl)} -E >/dev/null`;
  return {
    commands: [firewallCommand("/bin/sh", ["-c", loadScript])],
    releaseCommands: [firewallCommand(pfctl, ["-a", anchor, "-F", "all"])],
    limitations: [
      "Requires macOS pf administrative privileges and the default com.apple/* anchor point.",
      "pf user/group matching applies to TCP and UDP socket traffic; use a VM backend for whole-protocol host isolation.",
      "The UID/GID scope must be dedicated to this SparseKernel allocation or policy can leak between processes sharing the scope.",
    ],
  };
}

function windowsLocalUserSddl(scope: BuiltinFirewallScope): string | undefined {
  return scope.kind === "sid" ? `D:(A;;CC;;;${scope.value})` : undefined;
}

function buildWindowsAdvfirewallPlan(params: {
  allocationId: string;
  scope: BuiltinFirewallScope;
  allowedCidrs: string[];
  env: NodeJS.ProcessEnv;
}): Pick<BuiltinFirewallEgressPlan, "commands" | "releaseCommands" | "limitations"> {
  if (params.scope.kind !== "sid" && params.scope.kind !== "program") {
    throw new Error("Windows builtin firewall manager supports only sid/program scopes");
  }
  const powershell =
    params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_POWERSHELL?.trim() || "powershell.exe";
  const baseName = `OpenClaw-SparseKernel-${sanitizeFirewallToken(params.allocationId)}`;
  const preflight = firewallCommand(powershell, [
    "-NoProfile",
    "-NonInteractive",
    "-Command",
    "$bad = Get-NetFirewallProfile -Profile Domain,Private,Public | Where-Object { $_.Enabled -ne $true -or $_.DefaultOutboundAction -ne 'Block' }; if ($bad) { Write-Error 'SparseKernel requires all Windows Firewall profiles enabled with DefaultOutboundAction Block before scoped allow rules are meaningful.'; exit 42 }",
  ]);
  const commands = params.allowedCidrs.map((cidr, index) => {
    const name = `${baseName}-${index}`;
    const args = [
      "-NoProfile",
      "-NonInteractive",
      "-Command",
      "New-NetFirewallRule",
      "-Name",
      name,
      "-DisplayName",
      name,
      "-Direction",
      "Outbound",
      "-Action",
      "Allow",
      "-RemoteAddress",
      cidr,
      "-Protocol",
      "Any",
      "-Profile",
      "Any",
    ];
    const localUser = windowsLocalUserSddl(params.scope);
    if (localUser) {
      args.push("-LocalUser", localUser);
    } else if (params.scope.kind === "program") {
      args.push("-Program", params.scope.value);
    }
    return firewallCommand(powershell, args);
  });
  return {
    commands: [preflight, ...commands],
    releaseCommands: params.allowedCidrs.map((_, index) =>
      firewallCommand(powershell, [
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "Remove-NetFirewallRule",
        "-Name",
        `${baseName}-${index}`,
      ]),
    ),
    limitations: [
      "Verifies that Windows Firewall profiles are enabled with DefaultOutboundAction Block before installing scoped allow rules.",
      "Explicit Windows block rules take precedence over allow rules, so this manager only installs scoped allow rules under a confirmed default-block profile.",
      "SID scopes require the command worker to run as the corresponding Windows principal; use an external launcher/helper to materialize that identity.",
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
  workerIdentity?: SandboxWorkerIdentitySnapshot;
  env: NodeJS.ProcessEnv;
}): BuiltinFirewallEgressPlan {
  if (
    !params.workerIdentity &&
    !truthy(params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED)
  ) {
    throw new Error(
      "builtin firewall manager requires OPENCLAW_RUNTIME_SANDBOX_FIREWALL_SCOPE_DEDICATED=1",
    );
  }
  const platform = normalizePlatform(params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_PLATFORM);
  const scope = resolveFirewallScope({ env: params.env, workerIdentity: params.workerIdentity });
  const destinations = collectFirewallDestinations(params.policy);
  const enforcementId = `builtin-firewall:${platform}:${params.allocationId}`;
  const managedPrefix = params.workerIdentity ? "managed " : "";
  if (platform === "linux_iptables") {
    const linux = buildLinuxIptablesPlan({
      allocationId: params.allocationId,
      scope,
      allowedCidrs: destinations.allowedCidrs,
      env: params.env,
    });
    return {
      platform,
      enforcementId,
      scope,
      allowedCidrs: destinations.allowedCidrs,
      proxyDelegatedHosts: destinations.proxyDelegatedHosts,
      protocolCoverage: "all_ip",
      commands: linux.commands,
      releaseCommands: linux.releaseCommands,
      description: `Linux iptables owner-match allowlist for ${managedPrefix}${scope.kind}:${scope.value}`,
      limitations: linux.limitations,
    };
  }
  if (platform === "darwin_pf") {
    const darwin = buildDarwinPfPlan({
      allocationId: params.allocationId,
      scope,
      allowedCidrs: destinations.allowedCidrs,
      env: params.env,
    });
    return {
      platform,
      enforcementId,
      scope,
      allowedCidrs: destinations.allowedCidrs,
      proxyDelegatedHosts: destinations.proxyDelegatedHosts,
      protocolCoverage: "tcp_udp",
      commands: darwin.commands,
      releaseCommands: darwin.releaseCommands,
      description: `macOS pf scoped allowlist for ${managedPrefix}${scope.kind}:${scope.value}`,
      limitations: darwin.limitations,
    };
  }
  const windows = buildWindowsAdvfirewallPlan({
    allocationId: params.allocationId,
    scope,
    allowedCidrs: destinations.allowedCidrs,
    env: params.env,
  });
  return {
    platform,
    enforcementId,
    scope,
    allowedCidrs: destinations.allowedCidrs,
    proxyDelegatedHosts: destinations.proxyDelegatedHosts,
    protocolCoverage: "all_ip",
    commands: windows.commands,
    releaseCommands: windows.releaseCommands,
    description: `Windows Firewall default-block allowlist for ${scope.kind}:${scope.value}`,
    limitations: windows.limitations,
  };
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
  workerIdentity?: SandboxWorkerIdentitySnapshot;
}): void {
  if (params.backend !== "local/no_isolation") {
    throw new Error(
      `builtin firewall manager cannot bind firewall scope to sandbox backend ${params.backend}; use builtin no-network verification or an external helper`,
    );
  }
  if (params.scope.kind === "uid") {
    if (params.workerIdentity?.uid !== undefined) {
      const getuid = process.getuid;
      if (
        typeof getuid === "function" &&
        getuid() !== 0 &&
        getuid() !== params.workerIdentity.uid
      ) {
        throw new Error(
          "managed worker uid requires the broker to run as root or as the target uid",
        );
      }
      const getgid = process.getgid;
      if (
        params.workerIdentity.gid !== undefined &&
        typeof getgid === "function" &&
        typeof getuid === "function" &&
        getuid() !== 0 &&
        getgid() !== params.workerIdentity.gid
      ) {
        throw new Error(
          "managed worker gid requires the broker to run as root or as the target gid",
        );
      }
      return;
    }
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
    if (params.workerIdentity?.gid !== undefined) {
      const getgid = process.getgid;
      const getuid = process.getuid;
      if (
        typeof getgid === "function" &&
        typeof getuid === "function" &&
        getuid() !== 0 &&
        getgid() !== params.workerIdentity.gid
      ) {
        throw new Error(
          "managed worker gid requires the broker to run as root or as the target gid",
        );
      }
      return;
    }
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
  if (params.scope.kind === "program") {
    return;
  }
  throw new Error(
    "builtin firewall scope is not tied to the local sandbox runner in v0; use a managed UID/GID identity, program scope, or an external helper",
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
    workerIdentity: params.workerIdentity,
    env: params.env,
  });
  if (!truthy(params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY)) {
    throw new Error(
      `builtin firewall manager generated a ${plan.platform} plan but will not mutate the host firewall without OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1`,
    );
  }
  if (
    truthy(params.env.OPENCLAW_RUNTIME_SANDBOX_FIREWALL_REQUIRE_FULL_PROTOCOL) &&
    plan.protocolCoverage !== "all_ip"
  ) {
    throw new Error(
      `builtin firewall manager generated a ${plan.protocolCoverage} plan for ${plan.platform}; use a full-protocol backend, VM, or external helper for this trust zone`,
    );
  }
  assertFirewallScopeMatchesSandboxRunner({
    backend: params.backend,
    scope: plan.scope,
    workerIdentity: params.workerIdentity,
  });
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
      proxyDelegatedHosts: plan.proxyDelegatedHosts,
      protocolCoverage: plan.protocolCoverage,
      releaseCommands: plan.releaseCommands,
      applied: true,
      limitations: plan.limitations,
    },
    ...(params.workerIdentity ? { workerIdentity: params.workerIdentity } : {}),
  };
}
