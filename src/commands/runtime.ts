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
import type { RuntimeRetentionPolicy } from "../local-kernel/index.js";
import type { WorkerIdentityProvisionPlatform } from "../local-kernel/index.js";
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
    json?: boolean;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  const trustZone = opts.trustZone?.trim();
  if (!trustZone) {
    runtime.error("--trust-zone <id> is required");
    runtime.exit(1);
    return;
  }
  let maxProcesses: number | undefined;
  let maxMemoryMb: number | undefined;
  let maxRuntimeSeconds: number | undefined;
  try {
    maxProcesses = parseOptionalInteger(opts.maxProcesses, "--max-processes");
    maxMemoryMb = parseOptionalInteger(opts.maxMemoryMb, "--max-memory-mb");
    maxRuntimeSeconds = parseOptionalInteger(opts.maxRuntimeSeconds, "--max-runtime-seconds");
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  if (maxProcesses === undefined && maxMemoryMb === undefined && maxRuntimeSeconds === undefined) {
    runtime.error("Pass at least one budget limit to update.");
    runtime.exit(1);
    return;
  }
  const db = openLocalKernelDatabase();
  try {
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
    const zone = db.listTrustZones().find((entry) => entry.id === trustZone);
    if (opts.json) {
      writeRuntimeJson(runtime, { ok: true, trustZone: zone });
      return;
    }
    runtime.log(`Updated runtime budget for trust zone ${trustZone}.`);
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
