import {
  resolveRuntimeSessionStoreMode,
  resolveRuntimeTranscriptCompatMode,
} from "../config/sessions/runtime-ledger.js";
import {
  inspectNativeBrowserPoolStats,
  inspectNativeBrowserPools,
} from "./browser-process-pool.js";
import type { LocalKernelDatabase } from "./database.js";
import { isBuiltinFirewallHardEgressHelper } from "./hard-egress-firewall.js";
import { resolveNetworkPolicyProxyRef } from "./network-policy.js";
import { probeSandboxBackends } from "./sandbox-broker.js";
import { resolveRuntimeToolBrokerMode } from "./tool-broker-runtime.js";

export type SparseKernelDoctorStatus = "pass" | "warn" | "fail" | "info";

export type SparseKernelDoctorCheck = {
  id: string;
  status: SparseKernelDoctorStatus;
  summary: string;
  detail?: string;
  remediation?: string;
};

export type SparseKernelAcceptanceLane = {
  id: string;
  platform: "all" | "linux" | "darwin" | "windows";
  command: string;
  purpose: string;
  status: "required" | "recommended" | "optional";
};

export type SparseKernelDoctorReport = {
  ok: boolean;
  generatedAt: string;
  schemaVersion: number;
  tableCounts: Record<string, number>;
  toolBrokerMode: string;
  sessionStoreMode: string;
  transcriptCompatMode: string;
  resourceBudgets: Record<string, number>;
  checks: SparseKernelDoctorCheck[];
  acceptanceLanes: SparseKernelAcceptanceLane[];
};

export function inspectSparseKernelRuntime(params: {
  db: LocalKernelDatabase;
  env?: NodeJS.ProcessEnv;
}): SparseKernelDoctorReport {
  const env = params.env ?? process.env;
  const inspect = params.db.inspect();
  const checks = [
    inspectLedger(params.db),
    inspectSessionStore(env),
    inspectTranscriptMode(env),
    inspectToolBroker(env),
    inspectSandboxBackends(env),
    inspectResourceBudgets(params.db),
    inspectHardEgress(env),
    inspectEgressProxy(env),
    inspectWorkerIdentities(env),
    inspectPluginSubprocess(env),
    inspectBrowserBroker(env),
  ];
  return {
    ok: checks.every((check) => check.status !== "fail"),
    generatedAt: new Date().toISOString(),
    schemaVersion: inspect.schemaVersion,
    tableCounts: inspect.counts,
    toolBrokerMode: resolveRuntimeToolBrokerMode(env),
    sessionStoreMode: resolveRuntimeSessionStoreMode(env),
    transcriptCompatMode: resolveRuntimeTranscriptCompatMode(env),
    resourceBudgets: params.db.getResourceBudgetSnapshot(),
    checks,
    acceptanceLanes: sparseKernelAcceptanceLanes(),
  };
}

export function sparseKernelAcceptanceLanes(): SparseKernelAcceptanceLane[] {
  return [
    {
      id: "ledger-and-leases",
      platform: "all",
      command: "pnpm test src/local-kernel/database.test.ts",
      purpose: "SQLite schema, task leases, artifacts, capabilities, browser and sandbox ledgers.",
      status: "required",
    },
    {
      id: "tool-broker",
      platform: "all",
      command: "pnpm test src/local-kernel/tool-broker.test.ts",
      purpose:
        "Capability mediated tools, plugin subprocess routing, sandboxed exec, browser proxy injection.",
      status: "required",
    },
    {
      id: "runtime-cli",
      platform: "all",
      command: "pnpm test src/commands/runtime.sparsekernel.test.ts",
      purpose:
        "Runtime CLI inspection, maintenance, strict acceptance, cutover plan, and operator-facing surfaces.",
      status: "required",
    },
    {
      id: "egress-proxy",
      platform: "all",
      command: "pnpm test src/local-kernel/egress-proxy.test.ts",
      purpose: "Loopback egress proxy allow and deny behavior against trust-zone network policy.",
      status: "required",
    },
    {
      id: "browser-cdp",
      platform: "all",
      command: "pnpm test packages/browser-broker/src/index.test.ts",
      purpose:
        "CDP browser context lifecycle, artifacts, tabs, console, upload, PDF, actionability.",
      status: "recommended",
    },
    {
      id: "linux-hard-egress",
      platform: "linux",
      command:
        "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1 pnpm test src/local-kernel/hard-egress-firewall.test.ts",
      purpose: "Linux operator helper plans and fail-closed hard-egress setup.",
      status: "recommended",
    },
    {
      id: "darwin-hard-egress",
      platform: "darwin",
      command:
        "OPENCLAW_RUNTIME_SANDBOX_FIREWALL_APPLY=1 pnpm test src/local-kernel/hard-egress-firewall.test.ts",
      purpose: "macOS pf helper planning, limits, and fail-closed checks.",
      status: "recommended",
    },
    {
      id: "windows-hard-egress",
      platform: "windows",
      command: "pnpm test src/local-kernel/hard-egress-firewall.test.ts",
      purpose: "Windows firewall rule planning and default-outbound-block preflight.",
      status: "recommended",
    },
  ];
}

function inspectLedger(db: LocalKernelDatabase): SparseKernelDoctorCheck {
  const version = db.schemaVersion();
  return {
    id: "ledger.schema",
    status: version > 0 ? "pass" : "fail",
    summary: `SQLite ledger schema version ${version}.`,
    remediation: version > 0 ? undefined : "Run openclaw runtime migrate.",
  };
}

function inspectSessionStore(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const mode = resolveRuntimeSessionStoreMode(env);
  if (mode === "sqlite" || mode === "sqlite-strict") {
    return {
      id: "sessions.ledger_primary",
      status: "pass",
      summary: `Session transcript appends are ledger-primary in ${mode} mode.`,
    };
  }
  return {
    id: "sessions.ledger_primary",
    status: "warn",
    summary: `Session store mode is ${mode}; legacy files remain primary.`,
    remediation: "Set OPENCLAW_RUNTIME_SESSION_STORE=sqlite after importing existing sessions.",
  };
}

function inspectTranscriptMode(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const mode = resolveRuntimeTranscriptCompatMode(env);
  return {
    id: "sessions.transcript_compat",
    status: mode === "ledger-only" ? "pass" : "warn",
    summary:
      mode === "ledger-only"
        ? "Transcript writes use the ledger-primary path without legacy JSONL compatibility writes."
        : "Transcript writes still keep legacy JSONL compatibility enabled.",
    remediation:
      mode === "ledger-only"
        ? undefined
        : "Set OPENCLAW_RUNTIME_SESSION_STORE=sqlite-strict or OPENCLAW_RUNTIME_TRANSCRIPT_COMPAT=ledger-only after import/export coverage is green.",
  };
}

function inspectToolBroker(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const mode = resolveRuntimeToolBrokerMode(env);
  if (mode === "off") {
    return {
      id: "tools.broker",
      status: "fail",
      summary: "SparseKernel tool broker is disabled.",
      remediation: "Unset OPENCLAW_RUNTIME_TOOL_BROKER=off or set it to local/daemon.",
    };
  }
  return {
    id: "tools.broker",
    status: "pass",
    summary: `Tool calls route through the ${mode} SparseKernel broker.`,
  };
}

function inspectSandboxBackends(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const probes = probeSandboxBackends({ env });
  const available = probes
    .filter((probe) => probe.hardBoundary && probe.available)
    .map((probe) => probe.backend);
  if (available.length > 0) {
    return {
      id: "sandbox.backends",
      status: "pass",
      summary: `Available isolated sandbox backend(s): ${available.join(", ")}.`,
      detail: probes
        .map((probe) => `${probe.backend}: ${probe.available ? "available" : "missing"}`)
        .join("; "),
    };
  }
  const allowNoIsolation = env.OPENCLAW_RUNTIME_PLUGIN_ALLOW_NO_ISOLATION === "1";
  return {
    id: "sandbox.backends",
    status: allowNoIsolation ? "warn" : "fail",
    summary: "No isolated command sandbox backend was found.",
    remediation:
      "Install bwrap/minijail, configure Docker, configure an operator VM wrapper, or only use local/no_isolation for trusted operations.",
  };
}

function inspectResourceBudgets(db: LocalKernelDatabase): SparseKernelDoctorCheck {
  const budgets = db.getResourceBudgetSnapshot();
  return {
    id: "scheduler.resource_budgets",
    status: "pass",
    summary: `Small-VM resource budgets: active steps ${budgets.activeAgentStepsMax}, model calls ${budgets.modelCallsInFlightMax}, file patch jobs ${budgets.filePatchJobsMax}, test jobs ${budgets.testJobsMax}, browser contexts ${budgets.browserContextsMax}, heavy sandboxes ${budgets.heavySandboxesMax}.`,
  };
}

function inspectHardEgress(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const helper = env.OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER?.trim();
  const required =
    env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS === "1" ||
    env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS === "true";
  if (!helper) {
    return {
      id: "egress.hard",
      status: required ? "fail" : "warn",
      summary: "Hard egress helper is not configured.",
      remediation:
        "Use the built-in helper where supported or provide an operator helper for platform enforcement.",
    };
  }
  if (isBuiltinFirewallHardEgressHelper(helper)) {
    return {
      id: "egress.hard",
      status: "pass",
      summary: "Built-in hard-egress helper is configured for supported backend plans.",
    };
  }
  return {
    id: "egress.hard",
    status: "info",
    summary: "Operator-supplied hard-egress helper is configured.",
  };
}

function inspectEgressProxy(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const proxy = env.OPENCLAW_RUNTIME_EGRESS_PROXY_URL?.trim();
  if (!proxy) {
    return {
      id: "egress.proxy",
      status: "warn",
      summary: "Loopback egress proxy URL is not configured.",
      remediation:
        "Run openclaw runtime egress-proxy and attach its loopback URL as the trust-zone proxy_ref.",
    };
  }
  const decision = resolveNetworkPolicyProxyRef(proxy);
  return {
    id: "egress.proxy",
    status: decision.ok ? "pass" : "fail",
    summary: decision.ok
      ? "Loopback egress proxy URL is valid."
      : `Loopback egress proxy URL is invalid: ${decision.reason}.`,
  };
}

function inspectWorkerIdentities(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const mode = env.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITY_MODE?.trim().toLowerCase();
  if (mode !== "managed") {
    return {
      id: "sandbox.worker_identity",
      status: "warn",
      summary: "Managed worker identities are not enabled.",
      remediation: "Run openclaw runtime worker-identities and apply the generated environment.",
    };
  }
  const configured =
    Boolean(env.OPENCLAW_RUNTIME_SANDBOX_WORKER_IDENTITIES?.trim()) ||
    Boolean(env.OPENCLAW_RUNTIME_SANDBOX_WORKER_UIDS?.trim()) ||
    Boolean(env.OPENCLAW_RUNTIME_SANDBOX_WORKER_SIDS?.trim());
  return {
    id: "sandbox.worker_identity",
    status: configured ? "pass" : "fail",
    summary: configured
      ? "Managed worker identity pool is configured."
      : "Managed worker identity mode is enabled without identities.",
  };
}

function inspectPluginSubprocess(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const boundary =
    env.OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY?.trim().toLowerCase() ??
    env.OPENCLAW_RUNTIME_PLUGIN_PROCESS?.trim().toLowerCase() ??
    (env.OPENCLAW_SPARSEKERNEL_STRICT === "1" || env.OPENCLAW_SPARSEKERNEL_STRICT === "true"
      ? "strict"
      : undefined);
  const defaultWorker = env.OPENCLAW_RUNTIME_PLUGIN_SUBPROCESS_COMMAND?.trim();
  if (boundary === "subprocess" || boundary === "strict") {
    return {
      id: "plugins.subprocess",
      status: defaultWorker ? "pass" : "fail",
      summary: defaultWorker
        ? "Plugin subprocess policy has a default worker command."
        : "Plugin subprocess policy is strict but no default worker command is configured.",
      remediation: defaultWorker
        ? undefined
        : "Set OPENCLAW_RUNTIME_PLUGIN_SUBPROCESS_COMMAND or plugin subprocess metadata.",
    };
  }
  return {
    id: "plugins.subprocess",
    status: defaultWorker ? "pass" : "warn",
    summary: defaultWorker
      ? "Default plugin subprocess worker is configured."
      : "Non-bundled plugins are subprocess-first; bundled/native plugins may still run in process by default.",
    remediation: defaultWorker
      ? undefined
      : "Set OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY=strict to require subprocess workers.",
  };
}

function inspectBrowserBroker(env: NodeJS.ProcessEnv): SparseKernelDoctorCheck {
  const mode = env.OPENCLAW_RUNTIME_BROWSER_BROKER?.trim().toLowerCase() || "auto";
  const pools = inspectNativeBrowserPools();
  const stats = inspectNativeBrowserPoolStats();
  return {
    id: "browser.broker",
    status: mode === "off" || mode === "0" || mode === "false" ? "warn" : "pass",
    summary:
      mode === "off" || mode === "0" || mode === "false"
        ? "SparseKernel browser broker is disabled."
        : `Browser broker mode ${mode}; native pools=${pools.length}, stats=${stats.length}.`,
    remediation:
      mode === "off" || mode === "0" || mode === "false"
        ? "Unset OPENCLAW_RUNTIME_BROWSER_BROKER=off for brokered contexts."
        : undefined,
  };
}
