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
} from "../local-kernel/index.js";
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
  opts: { olderThan?: string; json?: boolean },
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
  const db = openLocalKernelDatabase();
  try {
    const store = new ContentAddressedArtifactStore(db);
    const result = store.prune({ olderThan });
    if (opts.json) {
      writeRuntimeJson(runtime, {
        olderThan,
        prunedArtifacts: result.artifacts.length,
        deletedFiles: result.deletedFiles,
      });
      return;
    }
    runtime.log(
      `Pruned ${result.artifacts.length} runtime artifact record(s), deleted ${result.deletedFiles} file(s).`,
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
