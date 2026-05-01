import { spawnSync } from "node:child_process";
import { parseDurationMs } from "../cli/parse-duration.js";
import { getRuntimeConfig } from "../config/config.js";
import {
  resolveAllAgentSessionStoreTargets,
  resolveSessionStoreTargets,
} from "../config/sessions.js";
import { formatErrorMessage } from "../infra/errors.js";
import {
  ContentAddressedArtifactStore,
  applyWorkerIdentityProvisionPlan,
  buildWorkerIdentityProvisionPlan,
  exportSessionAsJsonl,
  importLegacySessionStore,
  inspectSparseKernelRuntime,
  inspectNativeBrowserPoolStats,
  inspectNativeBrowserPools,
  ensureSupervisedEgressProxy,
  listSupervisedEgressProxies,
  openLocalKernelDatabase,
  recoverEmbeddedRunTasks,
  resolveNetworkPolicyProxyRef,
  startLoopbackEgressProxy,
  stopSupervisedEgressProxy,
  sweepNativeBrowserProcesses,
} from "../local-kernel/index.js";
import type { ResourceBudgetUpdateInput, RuntimeRetentionPolicy } from "../local-kernel/index.js";
import type { WorkerIdentityProvisionPlatform } from "../local-kernel/index.js";
import type { SparseKernelAcceptanceLane } from "../local-kernel/runtime-doctor.js";
import type { OutputRuntimeEnv, RuntimeEnv } from "../runtime.js";
import { writeRuntimeJson } from "../runtime.js";

function hasStdout(runtime: RuntimeEnv): runtime is OutputRuntimeEnv {
  return typeof (runtime as Partial<OutputRuntimeEnv>).writeStdout === "function";
}

function writeText(runtime: RuntimeEnv, text: string): void {
  if (hasStdout(runtime)) {
    runtime.writeStdout(text.endsWith("\n") ? text.slice(0, -1) : text);
    return;
  }
  runtime.log(text);
}

export async function runtimeMigrateCommand(
  opts: { json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    const inspect = db.inspect();
    if (opts.json) {
      writeRuntimeJson(runtime, inspect);
      return;
    }
    runtime.log(`Runtime DB ready: ${inspect.path}`);
    runtime.log(`Schema version: ${inspect.schemaVersion}`);
  } finally {
    db.close();
  }
}

export async function runtimeInspectCommand(
  opts: { json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    const inspect = db.inspect();
    if (opts.json) {
      writeRuntimeJson(runtime, inspect);
      return;
    }
    runtime.log(`Runtime DB: ${inspect.path}`);
    runtime.log(`Schema version: ${inspect.schemaVersion}`);
    for (const [table, count] of Object.entries(inspect.counts)) {
      runtime.log(`${table}: ${count}`);
    }
  } finally {
    db.close();
  }
}

export async function runtimeDoctorCommand(
  opts: { json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    const report = inspectSparseKernelRuntime({ db });
    if (opts.json) {
      writeRuntimeJson(runtime, report);
      return;
    }
    runtime.log(`SparseKernel runtime doctor: ${report.ok ? "ok" : "attention needed"}`);
    runtime.log(`Schema version: ${report.schemaVersion}`);
    runtime.log(`Tool broker: ${report.toolBrokerMode}`);
    runtime.log(`Session store: ${report.sessionStoreMode}`);
    for (const check of report.checks) {
      runtime.log(`${check.status.toUpperCase()} ${check.id}: ${check.summary}`);
      if (check.remediation) {
        runtime.log(`  ${check.remediation}`);
      }
    }
    runtime.log("Acceptance lanes:");
    for (const lane of report.acceptanceLanes) {
      runtime.log(`  ${lane.id} [${lane.platform}/${lane.status}]: ${lane.command}`);
    }
  } finally {
    db.close();
  }
}

type SparseKernelStrictFinding = {
  id: string;
  status: "pass" | "fail";
  summary: string;
  remediation?: string;
};

type SparseKernelAcceptanceLaneRun = {
  id: string;
  command: string;
  status: "passed" | "failed";
  exitCode: number | null;
  signal?: NodeJS.Signals | null;
  durationMs: number;
  stdout?: string;
  stderr?: string;
};

function isTruthyRuntimeFlag(value: string | undefined): boolean {
  const normalized = value?.trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function currentAcceptancePlatform(): "linux" | "darwin" | "windows" {
  return process.platform === "win32"
    ? "windows"
    : process.platform === "darwin"
      ? "darwin"
      : "linux";
}

function resolvePluginBoundaryMode(env: NodeJS.ProcessEnv): string | undefined {
  const explicit =
    env.OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY?.trim().toLowerCase() ??
    env.OPENCLAW_RUNTIME_PLUGIN_PROCESS?.trim().toLowerCase();
  if (explicit) {
    return explicit;
  }
  return isTruthyRuntimeFlag(env.OPENCLAW_SPARSEKERNEL_STRICT) ? "strict" : undefined;
}

function buildStrictAcceptanceFindings(params: {
  report: ReturnType<typeof inspectSparseKernelRuntime>;
  env?: NodeJS.ProcessEnv;
}): SparseKernelStrictFinding[] {
  const env = params.env ?? process.env;
  const checkStatus = (id: string) =>
    params.report.checks.find((check) => check.id === id)?.status ?? "fail";
  const pluginBoundary = resolvePluginBoundaryMode(env);
  const pluginWorker = env.OPENCLAW_RUNTIME_PLUGIN_SUBPROCESS_COMMAND?.trim();
  const browserMode = env.OPENCLAW_RUNTIME_BROWSER_BROKER?.trim().toLowerCase() || "auto";
  const hardEgressRequired = isTruthyRuntimeFlag(env.OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS);
  return [
    {
      id: "sessions.sqlite_strict",
      status: params.report.sessionStoreMode === "sqlite-strict" ? "pass" : "fail",
      summary:
        params.report.sessionStoreMode === "sqlite-strict"
          ? "Session metadata reads and writes are ledger-authoritative."
          : `Session store mode is ${params.report.sessionStoreMode}, not sqlite-strict.`,
      remediation:
        params.report.sessionStoreMode === "sqlite-strict"
          ? undefined
          : "Import existing sessions, then set OPENCLAW_SPARSEKERNEL_STRICT=1 or OPENCLAW_RUNTIME_SESSION_STORE=sqlite-strict.",
    },
    {
      id: "transcripts.ledger_only",
      status: params.report.transcriptCompatMode === "ledger-only" ? "pass" : "fail",
      summary:
        params.report.transcriptCompatMode === "ledger-only"
          ? "Transcript appends are ledger-only; JSONL is export/compat."
          : "Transcript compatibility mode still writes legacy JSONL.",
      remediation:
        params.report.transcriptCompatMode === "ledger-only"
          ? undefined
          : "Set OPENCLAW_RUNTIME_TRANSCRIPT_COMPAT=ledger-only after import/export coverage is green.",
    },
    {
      id: "tools.broker_required",
      status: params.report.toolBrokerMode === "off" ? "fail" : "pass",
      summary:
        params.report.toolBrokerMode === "off"
          ? "Tool broker is disabled."
          : `Tool broker mode is ${params.report.toolBrokerMode}.`,
      remediation:
        params.report.toolBrokerMode === "off"
          ? "Unset OPENCLAW_RUNTIME_TOOL_BROKER=off or set it to local/daemon."
          : undefined,
    },
    {
      id: "sandbox.isolated_backend",
      status: checkStatus("sandbox.backends") === "pass" ? "pass" : "fail",
      summary:
        checkStatus("sandbox.backends") === "pass"
          ? "At least one isolated sandbox backend is available."
          : "No isolated sandbox backend is available.",
      remediation:
        checkStatus("sandbox.backends") === "pass"
          ? undefined
          : "Install bwrap/minijail or configure an explicit Docker/VM backend before accepting untrusted work.",
    },
    {
      id: "egress.proxy",
      status: checkStatus("egress.proxy") === "pass" ? "pass" : "fail",
      summary:
        checkStatus("egress.proxy") === "pass"
          ? "Loopback egress proxy is configured."
          : "No valid loopback egress proxy is configured.",
      remediation:
        checkStatus("egress.proxy") === "pass"
          ? undefined
          : "Run openclaw runtime egress-proxy --trust-zone public_web --attach and require broker proxy mode where needed.",
    },
    {
      id: "egress.hard_when_required",
      status: !hardEgressRequired || checkStatus("egress.hard") === "pass" ? "pass" : "fail",
      summary: hardEgressRequired
        ? checkStatus("egress.hard") === "pass"
          ? "Hard-egress mode has a supported helper."
          : "Hard-egress mode is required but not enforced."
        : "Hard-egress mode is not required for this profile.",
      remediation:
        hardEgressRequired && checkStatus("egress.hard") !== "pass"
          ? "Configure OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER=builtin, builtin-firewall, or an operator helper."
          : undefined,
    },
    {
      id: "plugins.subprocess_default",
      status:
        (pluginBoundary === "subprocess" || pluginBoundary === "strict") && pluginWorker
          ? "pass"
          : "fail",
      summary:
        (pluginBoundary === "subprocess" || pluginBoundary === "strict") && pluginWorker
          ? "Plugin subprocess policy and default worker are configured."
          : "Bundled/native plugin tools can still run in process.",
      remediation:
        (pluginBoundary === "subprocess" || pluginBoundary === "strict") && pluginWorker
          ? undefined
          : "Set OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY=strict and OPENCLAW_RUNTIME_PLUGIN_SUBPROCESS_COMMAND.",
    },
    {
      id: "browser.brokered_mode",
      status:
        browserMode === "cdp" || browserMode === "managed" || browserMode === "native"
          ? "pass"
          : "fail",
      summary:
        browserMode === "cdp" || browserMode === "managed" || browserMode === "native"
          ? `Browser broker mode is ${browserMode}.`
          : `Browser broker mode is ${browserMode}; strict acceptance requires an explicit brokered mode.`,
      remediation:
        browserMode === "cdp" || browserMode === "managed" || browserMode === "native"
          ? undefined
          : "Set OPENCLAW_RUNTIME_BROWSER_BROKER=native, managed, or cdp.",
    },
    {
      id: "scheduler.resource_budgets",
      status: checkStatus("scheduler.resource_budgets") === "pass" ? "pass" : "fail",
      summary: "Small-VM task, browser, and sandbox resource budgets are available.",
    },
  ];
}

function acceptanceLaneRunnable(
  lane: SparseKernelAcceptanceLane,
  includeRecommended: boolean,
): boolean {
  return lane.status === "required" || (includeRecommended && lane.status === "recommended");
}

function tailOutput(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  return value.length <= 4_000 ? value : value.slice(-4_000);
}

function runAcceptanceLaneCommand(
  lane: SparseKernelAcceptanceLane,
  env: NodeJS.ProcessEnv,
): SparseKernelAcceptanceLaneRun {
  const started = Date.now();
  const result = spawnSync(lane.command, {
    shell: true,
    encoding: "utf8",
    env,
    maxBuffer: 8 * 1024 * 1024,
  });
  const exitCode = result.status;
  const status = exitCode === 0 ? "passed" : "failed";
  return {
    id: lane.id,
    command: lane.command,
    status,
    exitCode,
    signal: result.signal,
    durationMs: Date.now() - started,
    stdout: tailOutput(result.stdout),
    stderr: tailOutput(
      result.stderr || (result.error ? `${result.error.name}: ${result.error.message}` : ""),
    ),
  };
}

export async function runtimeAcceptanceCommand(
  opts: {
    strict?: boolean;
    currentPlatform?: boolean;
    includeRecommended?: boolean;
    run?: boolean;
    json?: boolean;
    env?: NodeJS.ProcessEnv;
    runLaneCommand?: (
      lane: SparseKernelAcceptanceLane,
      env: NodeJS.ProcessEnv,
    ) => SparseKernelAcceptanceLaneRun;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  const env = opts.env ?? process.env;
  const db = openLocalKernelDatabase();
  try {
    const report = inspectSparseKernelRuntime({ db, env });
    const platform = currentAcceptancePlatform();
    const lanes = opts.currentPlatform
      ? report.acceptanceLanes.filter(
          (lane) => lane.platform === "all" || lane.platform === platform,
        )
      : report.acceptanceLanes;
    const strictFindings = opts.strict ? buildStrictAcceptanceFindings({ report, env }) : undefined;
    const readinessOk = opts.strict
      ? strictFindings?.every((finding) => finding.status === "pass") === true
      : report.ok;
    const checks = opts.strict ? (strictFindings ?? []) : report.checks;
    const runResults = opts.run
      ? lanes
          .filter((lane) => acceptanceLaneRunnable(lane, Boolean(opts.includeRecommended)))
          .map((lane) => (opts.runLaneCommand ?? runAcceptanceLaneCommand)(lane, env))
      : undefined;
    const runOk = runResults ? runResults.every((result) => result.status === "passed") : true;
    const ok = readinessOk && runOk;
    const payload = {
      ok,
      strict: Boolean(opts.strict),
      platform,
      checks,
      lanes,
      ...(opts.run
        ? {
            ran: runResults ?? [],
            includeRecommended: Boolean(opts.includeRecommended),
          }
        : {}),
    };
    if (opts.json) {
      writeRuntimeJson(runtime, payload);
    } else {
      runtime.log(`SparseKernel acceptance: ${ok ? "ok" : "attention needed"}`);
      for (const check of payload.checks) {
        runtime.log(`${check.status.toUpperCase()} ${check.id}: ${check.summary}`);
        if (check.remediation) {
          runtime.log(`  ${check.remediation}`);
        }
      }
      runtime.log("Acceptance lanes:");
      for (const lane of lanes) {
        runtime.log(`  ${lane.id} [${lane.platform}/${lane.status}]: ${lane.command}`);
      }
      if (runResults) {
        runtime.log("Executed lanes:");
        for (const result of runResults) {
          runtime.log(
            `  ${result.status.toUpperCase()} ${result.id}: ${result.command} (${result.durationMs}ms)`,
          );
          if (result.stderr && result.status === "failed") {
            runtime.log(`  stderr: ${result.stderr}`);
          }
        }
      }
    }
    if (!ok) {
      runtime.exit(1);
    }
  } finally {
    db.close();
  }
}

export async function runtimeCutoverPlanCommand(
  opts: { json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    const report = inspectSparseKernelRuntime({ db });
    const strictFindings = buildStrictAcceptanceFindings({ report });
    const payload = {
      ok: strictFindings.every((finding) => finding.status === "pass"),
      environment: {
        OPENCLAW_SPARSEKERNEL_STRICT: "1",
        OPENCLAW_RUNTIME_SESSION_STORE: "sqlite-strict",
        OPENCLAW_RUNTIME_TRANSCRIPT_COMPAT: "ledger-only",
        OPENCLAW_RUNTIME_PLUGIN_PROCESS_BOUNDARY: "strict",
        OPENCLAW_RUNTIME_BROWSER_BROKER: "native",
      },
      commands: [
        "openclaw sessions import --from-existing",
        "openclaw runtime acceptance --strict --current-platform --run --include-recommended",
        "openclaw sessions export --session <session-id> --format jsonl",
      ],
      checks: strictFindings,
    };
    if (opts.json) {
      writeRuntimeJson(runtime, payload);
      return;
    }
    runtime.log("SparseKernel cutover plan:");
    runtime.log("1. Import existing file-backed sessions:");
    runtime.log("   openclaw sessions import --from-existing");
    runtime.log("2. Set strict runtime environment:");
    for (const [key, value] of Object.entries(payload.environment)) {
      runtime.log(`   ${key}=${value}`);
    }
    runtime.log("3. Run strict acceptance:");
    runtime.log(
      "   openclaw runtime acceptance --strict --current-platform --run --include-recommended",
    );
    runtime.log("4. Keep JSONL as explicit export/rollback:");
    runtime.log("   openclaw sessions export --session <session-id> --format jsonl");
    runtime.log(`Current readiness: ${payload.ok ? "ok" : "attention needed"}`);
    for (const check of strictFindings) {
      runtime.log(`${check.status.toUpperCase()} ${check.id}: ${check.summary}`);
      if (check.remediation) {
        runtime.log(`  ${check.remediation}`);
      }
    }
  } finally {
    db.close();
  }
}

export async function runtimeEgressProxyCommand(
  opts: {
    trustZone?: string;
    host?: string;
    port?: string;
    attach?: boolean;
    supervised?: boolean;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  const trustZoneId = opts.trustZone?.trim() || "public_web";
  const port = opts.port ? Number.parseInt(opts.port, 10) : 0;
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    runtime.error("--port must be an integer from 0 to 65535");
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  let proxy: Awaited<ReturnType<typeof startLoopbackEgressProxy>> | undefined;
  try {
    if (opts.supervised) {
      const record = await ensureSupervisedEgressProxy({
        db,
        trustZoneId,
        host: opts.host,
        port,
      });
      if (opts.json) {
        writeRuntimeJson(runtime, { ok: true, supervised: true, proxy: record });
      } else {
        runtime.log(`SparseKernel supervised egress proxy for ${trustZoneId}: ${record.proxyRef}`);
      }
      return;
    }
    proxy = await startLoopbackEgressProxy({
      db,
      trustZoneId,
      host: opts.host,
      port,
      actor: { type: "operator" },
    });
    db.recordAudit({
      actor: { type: "operator" },
      action: "egress_proxy.started",
      objectType: "trust_zone",
      objectId: trustZoneId,
      payload: { url: proxy.url },
    });
    if (opts.attach) {
      db.attachNetworkPolicyProxyToTrustZone({
        trustZoneId,
        proxyRef: proxy.url,
        actor: { type: "operator" },
      });
    }
    if (opts.json) {
      writeRuntimeJson(runtime, {
        ok: true,
        trustZoneId,
        proxyUrl: proxy.url,
        attached: opts.attach === true,
      });
    } else {
      runtime.log(`SparseKernel egress proxy listening for ${trustZoneId}: ${proxy.url}`);
      if (opts.attach) {
        runtime.log(`Attached proxy_ref to trust zone ${trustZoneId}.`);
      }
    }
    await new Promise<void>((resolve) => {
      const stop = () => {
        process.off("SIGINT", stop);
        process.off("SIGTERM", stop);
        resolve();
      };
      process.on("SIGINT", stop);
      process.on("SIGTERM", stop);
    });
  } finally {
    if (proxy) {
      await proxy.close();
      db.recordAudit({
        actor: { type: "operator" },
        action: "egress_proxy.stopped",
        objectType: "trust_zone",
        objectId: trustZoneId,
      });
    }
    db.close();
  }
}

export async function runtimeEgressProxyListCommand(
  opts: { json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const proxies = listSupervisedEgressProxies();
  if (opts.json) {
    writeRuntimeJson(runtime, { proxies });
    return;
  }
  if (proxies.length === 0) {
    runtime.log("No supervised SparseKernel egress proxies are running in this process.");
    return;
  }
  for (const proxy of proxies) {
    runtime.log(`${proxy.trustZoneId} proxy=${proxy.proxyRef} started=${proxy.startedAt}`);
  }
}

export async function runtimeEgressProxyStopCommand(
  opts: { trustZone?: string; clear?: boolean; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const trustZoneId = opts.trustZone?.trim() || "public_web";
  const db = openLocalKernelDatabase();
  try {
    const stopped = await stopSupervisedEgressProxy({
      db,
      trustZoneId,
      clearProxyRef: opts.clear,
    });
    if (opts.json) {
      writeRuntimeJson(runtime, { ok: true, trustZoneId, stopped });
      return;
    }
    runtime.log(
      stopped
        ? `Stopped supervised SparseKernel egress proxy for ${trustZoneId}.`
        : `No supervised SparseKernel egress proxy was running for ${trustZoneId}.`,
    );
  } finally {
    db.close();
  }
}

export async function runtimeNetworkProxySetCommand(
  opts: { trustZone?: string; proxyRef?: string; clear?: boolean; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const trustZoneId = opts.trustZone?.trim() || "public_web";
  const rawProxyRef = opts.clear ? null : opts.proxyRef?.trim();
  if (!opts.clear) {
    const decision = resolveNetworkPolicyProxyRef(rawProxyRef ?? undefined);
    if (!decision.ok) {
      runtime.error(`Invalid --proxy-ref: ${decision.reason}`);
      runtime.exit(1);
      return;
    }
  }
  const db = openLocalKernelDatabase();
  try {
    let result;
    try {
      result = db.attachNetworkPolicyProxyToTrustZone({
        trustZoneId,
        proxyRef: rawProxyRef,
        actor: { type: "operator" },
      });
    } catch (err) {
      runtime.error(formatErrorMessage(err));
      runtime.exit(1);
      return;
    }
    if (opts.json) {
      writeRuntimeJson(runtime, { ok: true, ...result });
      return;
    }
    runtime.log(
      rawProxyRef
        ? `Attached proxy_ref to trust zone ${trustZoneId}: ${rawProxyRef}`
        : `Cleared proxy_ref for trust zone ${trustZoneId}.`,
    );
  } finally {
    db.close();
  }
}

export async function runtimeNetworkProxyShowCommand(
  opts: { trustZone?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const trustZoneId = opts.trustZone?.trim() || "public_web";
  const db = openLocalKernelDatabase();
  try {
    const trustZone = db.getTrustZone(trustZoneId);
    const networkPolicy = db.getNetworkPolicyForTrustZone(trustZoneId);
    if (opts.json) {
      writeRuntimeJson(runtime, { trustZone, networkPolicy });
      return;
    }
    if (!trustZone) {
      runtime.error(`Unknown trust zone: ${trustZoneId}`);
      runtime.exit(1);
      return;
    }
    runtime.log(
      `${trustZoneId} networkPolicy=${trustZone.networkPolicyId ?? "-"} proxy_ref=${
        networkPolicy?.proxyRef ?? "-"
      }`,
    );
  } finally {
    db.close();
  }
}

export async function runtimeVacuumCommand(
  opts: { json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    db.vacuum();
    if (opts.json) {
      writeRuntimeJson(runtime, { ok: true, path: db.dbPath });
      return;
    }
    runtime.log(`Vacuumed runtime DB: ${db.dbPath}`);
  } finally {
    db.close();
  }
}

export async function runtimePruneCommand(
  opts: { olderThan?: string; retention?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const olderThanRaw = opts.olderThan ?? "7d";
  let olderThanMs: number;
  try {
    olderThanMs = parseDurationMs(olderThanRaw, { defaultUnit: "d" });
  } catch (err) {
    runtime.error(`--older-than ${formatErrorMessage(err)}`);
    runtime.exit(1);
    return;
  }
  const olderThan = new Date(Date.now() - olderThanMs).toISOString();
  let retentionPolicies: RuntimeRetentionPolicy[] | undefined;
  try {
    retentionPolicies = parseRetentionPolicies(opts.retention);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    const store = new ContentAddressedArtifactStore(db);
    const result = store.prune({ olderThan, retentionPolicies });
    const prunedBrowserObservations = db.pruneBrowserObservations({ olderThan });
    if (opts.json) {
      writeRuntimeJson(runtime, {
        olderThan,
        retentionPolicies,
        prunedArtifacts: result.artifacts.length,
        deletedFiles: result.deletedFiles,
        prunedBrowserObservations,
      });
      return;
    }
    runtime.log(
      `Pruned ${result.artifacts.length} runtime artifact record(s), deleted ${result.deletedFiles} file(s).`,
    );
    if (prunedBrowserObservations > 0) {
      runtime.log(`Pruned ${prunedBrowserObservations} browser observation record(s).`);
    }
  } finally {
    db.close();
  }
}

function parseLimit(value: string | undefined, defaultValue: number): number {
  if (value === undefined) {
    return defaultValue;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error("limit must be a positive integer");
  }
  return Math.min(1000, parsed);
}

function parseRetentionPolicies(raw: string | undefined): RuntimeRetentionPolicy[] | undefined {
  if (!raw?.trim()) {
    return undefined;
  }
  const policies = raw
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  const valid = new Set(["ephemeral", "session", "durable", "debug"]);
  const invalid = policies.find((policy) => !valid.has(policy));
  if (invalid) {
    throw new Error(`Unsupported retention policy: ${invalid}`);
  }
  return policies as RuntimeRetentionPolicy[];
}

function readRuntimeInfoNumber(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

export async function runtimeSessionsCommand(
  opts: { agent?: string; limit?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  let limit: number;
  try {
    limit = parseLimit(opts.limit, 50);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    const sessions = db.listSessions({ agentId: opts.agent, limit });
    if (opts.json) {
      writeRuntimeJson(runtime, { sessions });
      return;
    }
    if (sessions.length === 0) {
      runtime.log("No SparseKernel sessions found.");
      return;
    }
    for (const session of sessions) {
      runtime.log(
        `${session.id} agent=${session.agentId} status=${session.status} updated=${session.updatedAt}`,
      );
    }
  } finally {
    db.close();
  }
}

export async function runtimeTasksCommand(
  opts: { status?: string; kind?: string; limit?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  let limit: number;
  try {
    limit = parseLimit(opts.limit, 50);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    const tasks = db.listTasks({ status: opts.status, kind: opts.kind, limit });
    if (opts.json) {
      writeRuntimeJson(runtime, { tasks });
      return;
    }
    if (tasks.length === 0) {
      runtime.log("No SparseKernel tasks found.");
      return;
    }
    for (const task of tasks) {
      runtime.log(
        `${task.id} kind=${task.kind} status=${task.status} priority=${task.priority} updated=${task.updatedAt}`,
      );
    }
  } finally {
    db.close();
  }
}

export async function runtimeTranscriptCommand(
  opts: { session?: string; limit?: string; json?: boolean; format?: string },
  runtime: RuntimeEnv,
): Promise<void> {
  const sessionId = opts.session?.trim();
  if (!sessionId) {
    runtime.error("--session <id> is required");
    runtime.exit(1);
    return;
  }
  if (opts.format && opts.format !== "events" && opts.format !== "jsonl") {
    runtime.error("--format must be events or jsonl");
    runtime.exit(1);
    return;
  }
  let limit: number;
  try {
    limit = parseLimit(opts.limit, 100);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    if (opts.format === "jsonl") {
      writeText(runtime, exportSessionAsJsonl({ db, sessionId }));
      return;
    }
    const events = db.listTranscriptEvents(sessionId).slice(-limit);
    if (opts.json) {
      writeRuntimeJson(runtime, { sessionId, events });
      return;
    }
    if (events.length === 0) {
      runtime.log(`No transcript events found for ${sessionId}.`);
      return;
    }
    for (const event of events) {
      runtime.log(`#${event.seq} ${event.role}/${event.eventType} ${event.createdAt}`);
      if (event.content !== undefined) {
        runtime.log(JSON.stringify(event.content));
      }
    }
  } finally {
    db.close();
  }
}

export async function runtimeBrowserTargetsCommand(
  opts: {
    context?: string;
    session?: string;
    task?: string;
    status?: string;
    limit?: string;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  let limit: number;
  try {
    limit = parseLimit(opts.limit, 100);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    const targets = db.listBrowserTargets({
      contextId: opts.context,
      sessionId: opts.session,
      taskId: opts.task,
      status: opts.status,
      limit,
    });
    if (opts.json) {
      writeRuntimeJson(runtime, { targets });
      return;
    }
    if (targets.length === 0) {
      runtime.log("No SparseKernel browser targets found.");
      return;
    }
    for (const target of targets) {
      runtime.log(
        `${target.targetId} context=${target.contextId} status=${target.status} url=${target.url ?? "-"} console=${target.consoleCount} network=${target.networkCount} artifacts=${target.artifactCount}`,
      );
    }
  } finally {
    db.close();
  }
}

export async function runtimeBrowserObservationsCommand(
  opts: {
    context?: string;
    target?: string;
    type?: string;
    since?: string;
    limit?: string;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  let limit: number;
  let since: string | undefined;
  try {
    limit = parseLimit(opts.limit, 100);
    since = opts.since
      ? new Date(Date.now() - parseDurationMs(opts.since, { defaultUnit: "d" })).toISOString()
      : undefined;
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    const observations = db.listBrowserObservations({
      contextId: opts.context,
      targetId: opts.target,
      observationType: opts.type,
      since,
      limit,
    });
    if (opts.json) {
      writeRuntimeJson(runtime, { observations });
      return;
    }
    if (observations.length === 0) {
      runtime.log("No SparseKernel browser observations found.");
      return;
    }
    for (const observation of observations) {
      runtime.log(
        `#${observation.id} ${observation.observationType} context=${observation.contextId} target=${observation.targetId ?? "-"} ${observation.createdAt}`,
      );
    }
  } finally {
    db.close();
  }
}

export async function runtimeBrowserPoolsCommand(
  opts: { trustZone?: string; status?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    const ledgerPools = db.listBrowserPools({
      trustZoneId: opts.trustZone,
      status: opts.status,
    });
    const nativePools = inspectNativeBrowserPools().filter(
      (pool) => !opts.trustZone || pool.trustZoneId === opts.trustZone,
    );
    const nativeStats = inspectNativeBrowserPoolStats().filter(
      (pool) => !opts.trustZone || pool.trustZoneId === opts.trustZone,
    );
    const result = { ledgerPools, nativePools, nativeStats };
    if (opts.json) {
      writeRuntimeJson(runtime, result);
      return;
    }
    if (ledgerPools.length === 0 && nativePools.length === 0 && nativeStats.length === 0) {
      runtime.log("No SparseKernel browser pools found.");
      return;
    }
    for (const pool of ledgerPools) {
      runtime.log(
        `${pool.id} trustZone=${pool.trustZoneId} kind=${pool.browserKind} status=${pool.status} contexts=${pool.activeContexts}/${pool.maxContexts} cdp=${pool.cdpEndpoint ?? "-"}`,
      );
    }
    for (const pool of nativePools) {
      runtime.log(
        `native:${pool.key} trustZone=${pool.trustZoneId} profile=${pool.profile} refs=${pool.refs}/${pool.maxContexts} exited=${pool.exited} endpoint=${pool.cdpEndpoint}`,
      );
    }
    for (const stats of nativeStats) {
      runtime.log(
        `native-stats:${stats.key} starts=${stats.starts} cleanStops=${stats.cleanStops} crashes=${stats.crashes} lastExit=${stats.lastExitAt ?? "-"}`,
      );
    }
  } finally {
    db.close();
  }
}

export async function runtimeLeasesCommand(
  opts: {
    resourceType?: string;
    status?: string;
    trustZone?: string;
    agent?: string;
    limit?: string;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  let limit: number;
  try {
    limit = parseLimit(opts.limit, 100);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    const leases = db.listResourceLeases({
      resourceType: opts.resourceType,
      status: opts.status,
      trustZoneId: opts.trustZone,
      ownerAgentId: opts.agent,
      limit,
    });
    if (opts.json) {
      writeRuntimeJson(runtime, { leases });
      return;
    }
    if (leases.length === 0) {
      runtime.log("No SparseKernel resource leases found.");
      return;
    }
    for (const lease of leases) {
      runtime.log(
        `${lease.id} type=${lease.resourceType} resource=${lease.resourceId} status=${lease.status} trustZone=${lease.trustZoneId ?? "-"} owner=${lease.ownerAgentId ?? "-"} leaseUntil=${lease.leaseUntil ?? "-"}`,
      );
    }
  } finally {
    db.close();
  }
}

export async function runtimeArtifactSummaryCommand(
  opts: { json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    const artifacts = db.summarizeArtifactRetention();
    if (opts.json) {
      writeRuntimeJson(runtime, { artifacts });
      return;
    }
    if (artifacts.length === 0) {
      runtime.log("No SparseKernel artifacts found.");
      return;
    }
    for (const row of artifacts) {
      runtime.log(`${row.retentionPolicy}: ${row.count} artifact(s), ${row.sizeBytes} byte(s)`);
    }
  } finally {
    db.close();
  }
}

export async function runtimeArtifactAccessCommand(
  opts: {
    artifact?: string;
    subjectType?: string;
    subject?: string;
    permission?: string;
    limit?: string;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  let limit: number;
  try {
    limit = parseLimit(opts.limit, 100);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    const access = db.listArtifactAccess({
      artifactId: opts.artifact,
      subjectType: opts.subjectType,
      subjectId: opts.subject,
      permission: opts.permission,
      limit,
    });
    if (opts.json) {
      writeRuntimeJson(runtime, { access });
      return;
    }
    if (access.length === 0) {
      runtime.log("No SparseKernel artifact access records found.");
      return;
    }
    for (const row of access) {
      runtime.log(
        `${row.artifactId} ${row.permission} subject=${row.subjectType}:${row.subjectId} created=${row.createdAt} expires=${row.expiresAt ?? "-"}`,
      );
    }
  } finally {
    db.close();
  }
}

export async function runtimeRecoverCommand(
  opts: { task?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    const released = db.releaseExpiredLeases();
    const embeddedRuns = recoverEmbeddedRunTasks({ db, taskId: opts.task });
    const result = { releasedExpiredLeases: released, embeddedRuns };
    if (opts.json) {
      writeRuntimeJson(runtime, result);
      return;
    }
    runtime.log(
      `Recovered ${embeddedRuns.recovered} embedded run task(s); released ${released} expired lease(s).`,
    );
  } finally {
    db.close();
  }
}

export async function runtimeMaintainCommand(
  opts: {
    olderThan?: string;
    retention?: string;
    task?: string;
    scheduleEvery?: string;
    runDue?: boolean;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  const olderThanRaw = opts.olderThan ?? "7d";
  let olderThanMs: number;
  let scheduleEveryMs: number | undefined;
  let retentionPolicies: RuntimeRetentionPolicy[] | undefined;
  try {
    olderThanMs = parseDurationMs(olderThanRaw, { defaultUnit: "d" });
    scheduleEveryMs = opts.scheduleEvery
      ? parseDurationMs(opts.scheduleEvery, { defaultUnit: "m" })
      : undefined;
    retentionPolicies = parseRetentionPolicies(opts.retention);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const olderThan = new Date(Date.now() - olderThanMs).toISOString();
  const db = openLocalKernelDatabase();
  try {
    if (scheduleEveryMs !== undefined) {
      db.setRuntimeInfo("maintenance.schedule_every_ms", String(scheduleEveryMs));
      db.recordAudit({
        actor: { type: "operator" },
        action: "maintenance.schedule_updated",
        objectType: "runtime_info",
        objectId: "maintenance.schedule_every_ms",
        payload: { scheduleEveryMs },
      });
    } else {
      scheduleEveryMs = readRuntimeInfoNumber(
        db.getRuntimeInfo("maintenance.schedule_every_ms")?.value,
      );
    }
    if (opts.runDue && scheduleEveryMs) {
      const lastRunRaw = db.getRuntimeInfo("maintenance.last_run_at")?.value;
      const lastRunMs = lastRunRaw ? Date.parse(lastRunRaw) : Number.NaN;
      const nowMs = Date.now();
      if (Number.isFinite(lastRunMs) && nowMs - lastRunMs < scheduleEveryMs) {
        const nextRunAt = new Date(lastRunMs + scheduleEveryMs).toISOString();
        const result = {
          skipped: true,
          reason: "not due",
          scheduleEveryMs,
          lastRunAt: lastRunRaw,
          nextRunAt,
        };
        db.setRuntimeInfo("maintenance.last_due_check_at", new Date(nowMs).toISOString());
        if (opts.json) {
          writeRuntimeJson(runtime, result);
          return;
        }
        runtime.log(`Runtime maintenance not due until ${nextRunAt}.`);
        return;
      }
    }
    const releasedExpiredLeases = db.releaseExpiredLeases();
    const embeddedRuns = recoverEmbeddedRunTasks({ db, taskId: opts.task });
    const store = new ContentAddressedArtifactStore(db);
    const prunedArtifacts = store.prune({ olderThan, retentionPolicies });
    const prunedBrowserObservations = db.pruneBrowserObservations({ olderThan });
    const sweptBrowserPools = await sweepNativeBrowserProcesses();
    const result = {
      releasedExpiredLeases,
      embeddedRuns,
      olderThan,
      retentionPolicies,
      prunedArtifacts: prunedArtifacts.artifacts.length,
      deletedFiles: prunedArtifacts.deletedFiles,
      prunedBrowserObservations,
      sweptBrowserPools,
      scheduleEveryMs,
    };
    const completedAt = new Date().toISOString();
    db.setRuntimeInfo("maintenance.last_run_at", completedAt);
    db.setRuntimeInfo("maintenance.last_result_json", JSON.stringify(result));
    if (opts.json) {
      writeRuntimeJson(runtime, result);
      return;
    }
    runtime.log(
      `Maintained runtime: released ${releasedExpiredLeases} lease(s), recovered ${embeddedRuns.recovered} embedded run(s), pruned ${prunedArtifacts.artifacts.length} artifact record(s), deleted ${prunedArtifacts.deletedFiles} file(s), pruned ${prunedBrowserObservations} browser observation(s), swept ${sweptBrowserPools.stopped} browser pool(s).`,
    );
  } finally {
    db.close();
  }
}

export async function runtimeBudgetCommand(
  opts: { since?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const db = openLocalKernelDatabase();
  try {
    let since: string | undefined;
    if (opts.since) {
      try {
        since = new Date(
          Date.now() - parseDurationMs(opts.since, { defaultUnit: "d" }),
        ).toISOString();
      } catch (err) {
        runtime.error(`--since ${formatErrorMessage(err)}`);
        runtime.exit(1);
        return;
      }
    }
    const result = {
      resourceBudgets: db.getResourceBudgetSnapshot(),
      trustZones: db.listTrustZones(),
      usage: db.summarizeUsage({ since }),
    };
    if (opts.json) {
      writeRuntimeJson(runtime, result);
      return;
    }
    runtime.log("Trust-zone budgets:");
    runtime.log(
      `Small-VM defaults: activeSteps=${result.resourceBudgets.activeAgentStepsMax} modelCalls=${result.resourceBudgets.modelCallsInFlightMax} filePatchJobs=${result.resourceBudgets.filePatchJobsMax} testJobs=${result.resourceBudgets.testJobsMax} browserContexts=${result.resourceBudgets.browserContextsMax} heavySandboxes=${result.resourceBudgets.heavySandboxesMax}`,
    );
    for (const zone of result.trustZones) {
      runtime.log(
        `${zone.id}: backend=${zone.sandboxBackend} maxProcesses=${zone.maxProcesses ?? "-"} maxMemoryMb=${zone.maxMemoryMb ?? "-"} maxRuntimeSeconds=${zone.maxRuntimeSeconds ?? "-"}`,
      );
    }
    if (result.usage.length > 0) {
      runtime.log("Usage:");
      for (const row of result.usage) {
        runtime.log(`${row.resourceType}: ${row.amount} ${row.unit}`);
      }
    }
  } finally {
    db.close();
  }
}

function parseOptionalInteger(value: string | undefined, label: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${label} must be a non-negative integer`);
  }
  return parsed;
}

export async function runtimeBudgetSetCommand(
  opts: {
    trustZone?: string;
    maxProcesses?: string;
    maxMemoryMb?: string;
    maxRuntimeSeconds?: string;
    logicalAgentsMax?: string;
    activeAgentStepsMax?: string;
    modelCallsInFlightMax?: string;
    filePatchJobsMax?: string;
    testJobsMax?: string;
    browserContextsMax?: string;
    heavySandboxesMax?: string;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  const trustZone = opts.trustZone?.trim();
  let maxProcesses: number | undefined;
  let maxMemoryMb: number | undefined;
  let maxRuntimeSeconds: number | undefined;
  const resourceBudgetUpdates: ResourceBudgetUpdateInput = {};
  try {
    maxProcesses = parseOptionalInteger(opts.maxProcesses, "--max-processes");
    maxMemoryMb = parseOptionalInteger(opts.maxMemoryMb, "--max-memory-mb");
    maxRuntimeSeconds = parseOptionalInteger(opts.maxRuntimeSeconds, "--max-runtime-seconds");
    resourceBudgetUpdates.logicalAgentsMax = parseOptionalInteger(
      opts.logicalAgentsMax,
      "--logical-agents-max",
    );
    resourceBudgetUpdates.activeAgentStepsMax = parseOptionalInteger(
      opts.activeAgentStepsMax,
      "--active-agent-steps-max",
    );
    resourceBudgetUpdates.modelCallsInFlightMax = parseOptionalInteger(
      opts.modelCallsInFlightMax,
      "--model-calls-in-flight-max",
    );
    resourceBudgetUpdates.filePatchJobsMax = parseOptionalInteger(
      opts.filePatchJobsMax,
      "--file-patch-jobs-max",
    );
    resourceBudgetUpdates.testJobsMax = parseOptionalInteger(opts.testJobsMax, "--test-jobs-max");
    resourceBudgetUpdates.browserContextsMax = parseOptionalInteger(
      opts.browserContextsMax,
      "--browser-contexts-max",
    );
    resourceBudgetUpdates.heavySandboxesMax = parseOptionalInteger(
      opts.heavySandboxesMax,
      "--heavy-sandboxes-max",
    );
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const hasTrustZoneUpdates =
    maxProcesses !== undefined || maxMemoryMb !== undefined || maxRuntimeSeconds !== undefined;
  const hasResourceBudgetUpdates = Object.values(resourceBudgetUpdates).some(
    (value) => value !== undefined,
  );
  if (hasTrustZoneUpdates && !trustZone) {
    runtime.error("--trust-zone <id> is required when updating trust-zone limits.");
    runtime.exit(1);
    return;
  }
  if (!hasTrustZoneUpdates && !hasResourceBudgetUpdates) {
    runtime.error("Pass at least one budget limit to update.");
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    let zone;
    if (hasTrustZoneUpdates && trustZone) {
      const ok = db.updateTrustZoneLimits({
        id: trustZone,
        maxProcesses,
        maxMemoryMb,
        maxRuntimeSeconds,
      });
      if (!ok) {
        runtime.error(`Unknown trust zone: ${trustZone}`);
        runtime.exit(1);
        return;
      }
      zone = db.listTrustZones().find((entry) => entry.id === trustZone);
    }
    const resourceBudgets = hasResourceBudgetUpdates
      ? db.updateResourceBudgets(resourceBudgetUpdates)
      : db.getResourceBudgetSnapshot();
    if (opts.json) {
      writeRuntimeJson(runtime, {
        ok: true,
        resourceBudgets,
        ...(zone ? { trustZone: zone } : {}),
      });
      return;
    }
    if (zone) {
      runtime.log(`Updated runtime budget for trust zone ${trustZone}.`);
    }
    if (hasResourceBudgetUpdates) {
      runtime.log("Updated SparseKernel small-VM resource budgets.");
    }
  } finally {
    db.close();
  }
}

export async function runtimeWorkerIdentitiesCommand(
  opts: {
    count?: string;
    prefix?: string;
    uidStart?: string;
    gid?: string;
    group?: string;
    platform?: string;
    apply?: boolean;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  let count: number | undefined;
  let uidStart: number | undefined;
  let gid: number | undefined;
  try {
    count = parseOptionalInteger(opts.count, "--count");
    uidStart = parseOptionalInteger(opts.uidStart, "--uid-start");
    gid = parseOptionalInteger(opts.gid, "--gid");
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  let plan;
  try {
    plan = buildWorkerIdentityProvisionPlan({
      ...(opts.platform ? { platform: opts.platform as WorkerIdentityProvisionPlatform } : {}),
      ...(count !== undefined ? { count } : {}),
      ...(opts.prefix ? { prefix: opts.prefix } : {}),
      ...(uidStart !== undefined ? { uidStart } : {}),
      ...(gid !== undefined ? { gid } : {}),
      ...(opts.group ? { group: opts.group } : {}),
    });
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  let applyResults;
  if (opts.apply) {
    applyResults = applyWorkerIdentityProvisionPlan(plan);
    const failed = applyResults.find((result) => result.status !== 0);
    if (failed) {
      runtime.error(
        `Worker identity provision command failed: ${failed.command.command} ${failed.command.args.join(" ")}\n${failed.stderr || failed.stdout}`,
      );
      runtime.exit(1);
      return;
    }
  }
  if (opts.json) {
    writeRuntimeJson(runtime, { plan, ...(applyResults ? { applyResults } : {}) });
    return;
  }
  runtime.log(`SparseKernel worker identity plan (${plan.platform}, ${plan.count} worker(s))`);
  runtime.log("Commands:");
  for (const entry of plan.commands) {
    runtime.log(`  ${entry.command} ${entry.args.map((arg) => JSON.stringify(arg)).join(" ")}`);
  }
  runtime.log("Environment:");
  for (const [name, value] of Object.entries(plan.environment)) {
    runtime.log(`  ${name}=${value}`);
  }
  for (const note of plan.notes) {
    runtime.log(`Note: ${note}`);
  }
  if (applyResults) {
    runtime.log(`Applied ${applyResults.length} worker identity command(s).`);
  }
}

export async function sessionsImportToRuntimeCommand(
  opts: {
    fromExisting?: boolean;
    store?: string;
    agent?: string;
    allAgents?: boolean;
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  if (!opts.fromExisting && !opts.store) {
    runtime.error("Pass --from-existing or --store <path> to import sessions into the runtime DB.");
    runtime.exit(1);
    return;
  }
  const cfg = getRuntimeConfig();
  let targets;
  try {
    targets = opts.fromExisting
      ? await resolveAllAgentSessionStoreTargets(cfg)
      : resolveSessionStoreTargets(cfg, {
          store: opts.store,
          agent: opts.agent,
          allAgents: opts.allAgents,
        });
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }

  const db = openLocalKernelDatabase();
  try {
    const results = [];
    for (const target of targets) {
      results.push(await importLegacySessionStore({ db, target }));
    }
    const summary = {
      stores: results.length,
      sessions: results.reduce((sum, result) => sum + result.sessions, 0),
      importedEvents: results.reduce((sum, result) => sum + result.importedEvents, 0),
      skippedExistingTranscripts: results.reduce(
        (sum, result) => sum + result.skippedExistingTranscripts,
        0,
      ),
      missingTranscripts: results.reduce((sum, result) => sum + result.missingTranscripts, 0),
      results,
    };
    if (opts.json) {
      writeRuntimeJson(runtime, summary);
      return;
    }
    runtime.log(
      `Imported ${summary.sessions} session row(s), ${summary.importedEvents} transcript event(s) from ${summary.stores} store(s).`,
    );
    if (summary.missingTranscripts > 0) {
      runtime.log(`Missing transcript files: ${summary.missingTranscripts}`);
    }
  } finally {
    db.close();
  }
}

export async function sessionsExportFromRuntimeCommand(
  opts: { session?: string; format?: string },
  runtime: RuntimeEnv,
): Promise<void> {
  const sessionId = opts.session?.trim();
  if (!sessionId) {
    runtime.error("--session <id> is required");
    runtime.exit(1);
    return;
  }
  if (opts.format && opts.format !== "jsonl") {
    runtime.error("Only --format jsonl is supported for runtime session export.");
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
    writeText(runtime, exportSessionAsJsonl({ db, sessionId }));
  } finally {
    db.close();
  }
}
