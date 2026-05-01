export type SandboxBackendKind =
  | "local/no_isolation"
  | "docker"
  | "bwrap"
  | "minijail"
  | "ssh"
  | "openshell"
  | "vm"
  | "other";

export type SandboxIsolationProfileId =
  | "trusted_local"
  | "web_brokered"
  | "readonly_workspace"
  | "rw_workspace"
  | "code_execution"
  | "plugin_untrusted";

export type BuiltinFirewallPlatform = "linux_iptables" | "darwin_pf" | "windows_advfirewall";

export type BuiltinFirewallScope =
  | { kind: "uid"; value: string }
  | { kind: "gid"; value: string }
  | { kind: "program"; value: string }
  | { kind: "sid"; value: string };

export type SandboxWorkerIdentitySnapshot = {
  id: string;
  source: "managed_pool";
  uid?: number;
  gid?: number;
  sid?: string;
  scope: BuiltinFirewallScope;
};

export type BuiltinFirewallCommand = {
  command: string;
  args: string[];
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
  isolationProfile?: SandboxIsolationProfileId;
  filesystemPolicy?: unknown;
  maxProcesses?: number;
  maxMemoryMb?: number;
  maxRuntimeSeconds?: number;
  networkPolicy?: {
    id: string;
    defaultAction: "allow" | "deny";
    allowPrivateNetwork: boolean;
    allowedHosts?: string[];
    deniedCidrs?: string[];
    proxyRef?: string;
  };
  docker?: DockerSandboxPolicy;
};

export type HardEgressEnforcementSnapshot = {
  helper: string;
  enforcementId: string;
  boundary: "host_firewall" | "egress_proxy" | "vm_firewall" | "platform_enforcer";
  description?: string;
  firewall?: {
    platform: BuiltinFirewallPlatform | (string & {});
    scope: string;
    allowedCidrs?: string[];
    proxyDelegatedHosts?: string[];
    protocolCoverage?: string;
    releaseCommands?: BuiltinFirewallCommand[];
    applied?: boolean;
    limitations?: string[];
  };
  workerIdentity?: SandboxWorkerIdentitySnapshot;
};
