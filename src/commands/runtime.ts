import { parseDurationMs } from "../cli/parse-duration.js";
import { getRuntimeConfig } from "../config/config.js";
import {
  resolveAllAgentSessionStoreTargets,
  resolveSessionStoreTargets,
} from "../config/sessions.js";
import { formatErrorMessage } from "../infra/errors.js";
import {
  ContentAddressedArtifactStore,
  exportSessionAsJsonl,
  importLegacySessionStore,
  openLocalKernelDatabase,
  recoverEmbeddedRunTasks,
} from "../local-kernel/index.js";
import type { RuntimeRetentionPolicy } from "../local-kernel/index.js";
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
  opts: { olderThan?: string; retention?: string; task?: string; json?: boolean },
  runtime: RuntimeEnv,
): Promise<void> {
  const olderThanRaw = opts.olderThan ?? "7d";
  let olderThanMs: number;
  let retentionPolicies: RuntimeRetentionPolicy[] | undefined;
  try {
    olderThanMs = parseDurationMs(olderThanRaw, { defaultUnit: "d" });
    retentionPolicies = parseRetentionPolicies(opts.retention);
  } catch (err) {
    runtime.error(formatErrorMessage(err));
    runtime.exit(1);
    return;
  }
  const olderThan = new Date(Date.now() - olderThanMs).toISOString();
  const db = openLocalKernelDatabase();
  try {
    const releasedExpiredLeases = db.releaseExpiredLeases();
    const embeddedRuns = recoverEmbeddedRunTasks({ db, taskId: opts.task });
    const store = new ContentAddressedArtifactStore(db);
    const prunedArtifacts = store.prune({ olderThan, retentionPolicies });
    const prunedBrowserObservations = db.pruneBrowserObservations({ olderThan });
    const result = {
      releasedExpiredLeases,
      embeddedRuns,
      olderThan,
      retentionPolicies,
      prunedArtifacts: prunedArtifacts.artifacts.length,
      deletedFiles: prunedArtifacts.deletedFiles,
      prunedBrowserObservations,
    };
    if (opts.json) {
      writeRuntimeJson(runtime, result);
      return;
    }
    runtime.log(
      `Maintained runtime: released ${releasedExpiredLeases} lease(s), recovered ${embeddedRuns.recovered} embedded run(s), pruned ${prunedArtifacts.artifacts.length} artifact record(s), deleted ${prunedArtifacts.deletedFiles} file(s), pruned ${prunedBrowserObservations} browser observation(s).`,
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
      trustZones: db.listTrustZones(),
      usage: db.summarizeUsage({ since }),
    };
    if (opts.json) {
      writeRuntimeJson(runtime, result);
      return;
    }
    runtime.log("Trust-zone budgets:");
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
