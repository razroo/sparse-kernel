import { spawnSync } from "node:child_process";
import type {
  BuiltinFirewallCommand,
  SandboxWorkerIdentitySnapshot,
} from "./hard-egress-firewall.js";

export type WorkerIdentityProvisionPlatform = "linux" | "darwin" | "windows";

export type WorkerIdentityProvisionPlan = {
  platform: WorkerIdentityProvisionPlatform;
  count: number;
  commands: BuiltinFirewallCommand[];
  identities: SandboxWorkerIdentitySnapshot[];
  environment: Record<string, string>;
  notes: string[];
};

export type WorkerIdentityProvisionOptions = {
  platform?: string;
  count?: number;
  prefix?: string;
  uidStart?: number;
  gid?: number;
  group?: string;
};

export type WorkerIdentityProvisionApplyResult = {
  command: BuiltinFirewallCommand;
  status: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
};

const DEFAULT_PREFIX = "openclaw-sk-worker";
const DEFAULT_GROUP = "openclaw-sparsekernel-workers";
const DEFAULT_UID_START = 62_000;
const DEFAULT_GID = 62_000;

function normalizePlatform(
  raw: WorkerIdentityProvisionOptions["platform"],
): WorkerIdentityProvisionPlatform {
  const value = (raw ?? process.platform).trim().toLowerCase();
  if (value === "linux") {
    return "linux";
  }
  if (value === "darwin" || value === "macos") {
    return "darwin";
  }
  if (value === "win32" || value === "windows") {
    return "windows";
  }
  throw new Error(`Unsupported worker identity platform: ${raw ?? process.platform}`);
}

function readPositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const resolved = value ?? fallback;
  if (!Number.isInteger(resolved) || resolved < 1) {
    throw new Error(`${label} must be a positive integer`);
  }
  return resolved;
}

function safeName(value: string, label: string): string {
  const trimmed = value.trim();
  if (!/^[A-Za-z_][A-Za-z0-9_-]*$/.test(trimmed)) {
    throw new Error(
      `${label} must start with a letter or underscore and contain only letters, numbers, underscore, or dash`,
    );
  }
  return trimmed;
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function command(command: string, args: string[]): BuiltinFirewallCommand {
  return { command, args };
}

function makeUnixIdentity(id: string, uid: number, gid: number): SandboxWorkerIdentitySnapshot {
  return {
    id,
    source: "managed_pool",
    uid,
    gid,
    scope: { kind: "uid", value: String(uid) },
  };
}

function makeWindowsIdentity(id: string): SandboxWorkerIdentitySnapshot {
  const sidPlaceholder = `S-1-5-21-REPLACE-WITH-${id.toUpperCase().replace(/[^A-Z0-9]/g, "-")}-SID`;
  return {
    id,
    source: "managed_pool",
    sid: sidPlaceholder,
    scope: { kind: "sid", value: sidPlaceholder },
  };
}

function buildLinuxCommands(params: {
  identities: SandboxWorkerIdentitySnapshot[];
  group: string;
}): BuiltinFirewallCommand[] {
  const commands = [
    `getent group ${shellQuote(params.group)} >/dev/null || groupadd --system ${shellQuote(params.group)}`,
  ];
  for (const identity of params.identities) {
    if (identity.uid === undefined || identity.gid === undefined) {
      continue;
    }
    commands.push(
      `id -u ${shellQuote(identity.id)} >/dev/null 2>&1 || useradd --system --no-create-home --home-dir /var/empty --shell /usr/sbin/nologin --uid ${identity.uid} --gid ${shellQuote(params.group)} ${shellQuote(identity.id)}`,
    );
  }
  return commands.map((entry) => command("/bin/sh", ["-c", entry]));
}

function buildDarwinCommands(params: {
  identities: SandboxWorkerIdentitySnapshot[];
  group: string;
  gid: number;
}): BuiltinFirewallCommand[] {
  const commands = [
    `dscl . -read /Groups/${shellQuote(params.group)} >/dev/null 2>&1 || { dscl . -create /Groups/${shellQuote(params.group)} && dscl . -create /Groups/${shellQuote(params.group)} PrimaryGroupID ${params.gid} && dscl . -create /Groups/${shellQuote(params.group)} Password '*'; }`,
  ];
  for (const identity of params.identities) {
    if (identity.uid === undefined || identity.gid === undefined) {
      continue;
    }
    const name = identity.id;
    commands.push(
      [
        `dscl . -read /Users/${shellQuote(name)} >/dev/null 2>&1 || {`,
        `dscl . -create /Users/${shellQuote(name)}`,
        `&& dscl . -create /Users/${shellQuote(name)} UniqueID ${identity.uid}`,
        `&& dscl . -create /Users/${shellQuote(name)} PrimaryGroupID ${identity.gid}`,
        `&& dscl . -create /Users/${shellQuote(name)} NFSHomeDirectory /var/empty`,
        `&& dscl . -create /Users/${shellQuote(name)} UserShell /usr/bin/false`,
        `&& dscl . -create /Users/${shellQuote(name)} IsHidden 1`,
        `&& dscl . -create /Users/${shellQuote(name)} Password '*';`,
        `}`,
        `&& dseditgroup -o edit -a ${shellQuote(name)} -t user ${shellQuote(params.group)}`,
      ].join(" "),
    );
  }
  return commands.map((entry) => command("/bin/sh", ["-c", entry]));
}

function buildWindowsCommands(params: {
  identities: SandboxWorkerIdentitySnapshot[];
  group: string;
}): BuiltinFirewallCommand[] {
  const scripts = [
    `if (-not (Get-LocalGroup -Name ${JSON.stringify(params.group)} -ErrorAction SilentlyContinue)) { New-LocalGroup -Name ${JSON.stringify(params.group)} | Out-Null }`,
  ];
  for (const identity of params.identities) {
    scripts.push(
      [
        `$name = ${JSON.stringify(identity.id)};`,
        `if (-not (Get-LocalUser -Name $name -ErrorAction SilentlyContinue)) { New-LocalUser -Name $name -NoPassword -AccountNeverExpires -UserMayNotChangePassword | Out-Null }`,
        `Add-LocalGroupMember -Group ${JSON.stringify(params.group)} -Member $name -ErrorAction SilentlyContinue;`,
      ].join(" "),
    );
  }
  scripts.push(
    `$ids = @(${params.identities
      .map((identity) => JSON.stringify(identity.id))
      .join(
        ",",
      )}) | ForEach-Object { $user = Get-LocalUser -Name $_; @{ id = $user.Name; sid = $user.SID.Value } }; $ids | ConvertTo-Json -Compress`,
  );
  return scripts.map((script) =>
    command("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", script]),
  );
}

export function buildWorkerIdentityProvisionPlan(
  options: WorkerIdentityProvisionOptions = {},
): WorkerIdentityProvisionPlan {
  const platform = normalizePlatform(options.platform);
  const count = readPositiveInteger(options.count, 2, "count");
  const prefix = safeName(options.prefix ?? DEFAULT_PREFIX, "prefix");
  const group = safeName(options.group ?? DEFAULT_GROUP, "group");
  const uidStart = readPositiveInteger(options.uidStart, DEFAULT_UID_START, "uidStart");
  const gid = readPositiveInteger(options.gid, DEFAULT_GID, "gid");
  const identities = Array.from({ length: count }, (_, index) => {
    const id = `${prefix}-${index}`;
    if (platform === "windows") {
      return makeWindowsIdentity(id);
    }
    return makeUnixIdentity(id, uidStart + index, gid);
  });
  const commands =
    platform === "linux"
      ? buildLinuxCommands({ identities, group })
      : platform === "darwin"
        ? buildDarwinCommands({ identities, group, gid })
        : buildWindowsCommands({ identities, group });
  const environment: Record<string, string> = {
    OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITY_MODE: "managed",
    OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES: JSON.stringify(identities),
  };
  const notes =
    platform === "windows"
      ? [
          "Run the commands from an elevated PowerShell prompt.",
          "Replace the placeholder SIDs in OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES with the JSON emitted by the final command.",
          "Windows hard egress still requires firewall profiles to be enabled with DefaultOutboundAction Block; SparseKernel verifies that before installing scoped allow rules.",
        ]
      : [
          "Run the commands with administrative privileges.",
          "Export the environment values for the SparseKernel broker process.",
          "These identities should be reserved for SparseKernel worker leases and not shared with unrelated host processes.",
        ];
  return { platform, count, commands, identities, environment, notes };
}

export function applyWorkerIdentityProvisionPlan(
  plan: WorkerIdentityProvisionPlan,
): WorkerIdentityProvisionApplyResult[] {
  return plan.commands.map((entry) => {
    const result = spawnSync(entry.command, entry.args, {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    if (result.error) {
      throw result.error;
    }
    return {
      command: entry,
      status: result.status,
      signal: result.signal,
      stdout: result.stdout ?? "",
      stderr: result.stderr ?? "",
    };
  });
}
