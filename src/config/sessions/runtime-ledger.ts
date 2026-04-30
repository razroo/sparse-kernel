import path from "node:path";
import { openLocalKernelDatabase } from "../../local-kernel/database.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { DEFAULT_AGENT_ID, normalizeAgentId } from "../../routing/session-key.js";
import type { SessionEntry } from "./types.js";

export type RuntimeSessionStoreMode = "off" | "dual" | "sqlite" | "sqlite-strict";

const SESSION_STORE_MODE_ENV = "OPENCLAW_RUNTIME_SESSION_STORE";
const log = createSubsystemLogger("sessions/runtime-ledger");
const warnedMirrorFailures = new Set<string>();

function dateFromEpochMs(value: number | undefined): string | undefined {
  return typeof value === "number" && Number.isFinite(value)
    ? new Date(value).toISOString()
    : undefined;
}

function inferSessionStatus(entry: SessionEntry): string {
  if (entry.status === "running") {
    return "running";
  }
  if (entry.status === "failed" || entry.status === "killed" || entry.status === "timeout") {
    return entry.status;
  }
  return "active";
}

function inferAgentIdFromStorePath(storePath: string): string {
  const resolved = path.resolve(storePath);
  if (path.basename(resolved) !== "sessions.json") {
    return DEFAULT_AGENT_ID;
  }
  const sessionsDir = path.dirname(resolved);
  if (path.basename(sessionsDir) !== "sessions") {
    return DEFAULT_AGENT_ID;
  }
  const agentDir = path.dirname(sessionsDir);
  if (path.basename(path.dirname(agentDir)) !== "agents") {
    return DEFAULT_AGENT_ID;
  }
  return normalizeAgentId(path.basename(agentDir));
}

export function resolveRuntimeSessionStoreMode(
  env: NodeJS.ProcessEnv = process.env,
): RuntimeSessionStoreMode {
  const raw = env[SESSION_STORE_MODE_ENV]?.trim().toLowerCase();
  if (!raw && (env.VITEST || env.NODE_ENV === "test")) {
    return "off";
  }
  if (raw === "off" || raw === "0" || raw === "false") {
    return "off";
  }
  if (raw === "sqlite-strict" || raw === "strict-sqlite" || raw === "strict") {
    return "sqlite-strict";
  }
  if (raw === "sqlite" || raw === "on" || raw === "1" || raw === "true") {
    return "sqlite";
  }
  return "dual";
}

export function isRuntimeSessionStorePrimary(env: NodeJS.ProcessEnv = process.env): boolean {
  const mode = resolveRuntimeSessionStoreMode(env);
  return mode === "sqlite" || mode === "sqlite-strict";
}

function buildSessionStoreLedgerEntries(params: {
  storePath: string;
  store: Record<string, SessionEntry>;
}) {
  const storePath = path.resolve(params.storePath);
  const defaultAgentId = inferAgentIdFromStorePath(storePath);
  return Object.entries(params.store).flatMap(([sessionKey, entry]) => {
    if (!entry?.sessionId) {
      return [];
    }
    const agentId = defaultAgentId;
    return [
      {
        sessionKey,
        sessionId: entry.sessionId,
        agentId,
        entry,
        channel: entry.lastChannel ?? entry.channel,
        status: inferSessionStatus(entry),
        currentTokenCount: entry.totalTokens ?? entry.contextTokens ?? 0,
        lastActivityAt: dateFromEpochMs(entry.lastInteractionAt ?? entry.updatedAt),
        createdAt: dateFromEpochMs(entry.sessionStartedAt ?? entry.startedAt),
        updatedAt: dateFromEpochMs(entry.updatedAt),
      },
    ];
  });
}

export function persistSessionStoreToRuntimeLedger(params: {
  storePath: string;
  store: Record<string, SessionEntry>;
  env?: NodeJS.ProcessEnv;
}): void {
  const env = params.env ?? process.env;
  const storePath = path.resolve(params.storePath);
  const entries = buildSessionStoreLedgerEntries({
    storePath,
    store: params.store,
  });
  const db = openLocalKernelDatabase({ env });
  try {
    db.replaceSessionEntriesForStore({ storePath, entries });
  } finally {
    db.close();
  }
}

export function loadSessionStoreFromRuntimeLedger(
  storePath: string,
  env: NodeJS.ProcessEnv = process.env,
): Record<string, SessionEntry> | undefined {
  const mode = resolveRuntimeSessionStoreMode(env);
  if (mode !== "sqlite" && mode !== "sqlite-strict") {
    return undefined;
  }
  const db = openLocalKernelDatabase({ env });
  try {
    const store = db.loadSessionEntriesForStore(path.resolve(storePath));
    if (!store && mode === "sqlite-strict") {
      return {};
    }
    return store as Record<string, SessionEntry> | undefined;
  } finally {
    db.close();
  }
}

export function mirrorSessionStoreToRuntimeLedger(params: {
  storePath: string;
  store: Record<string, SessionEntry>;
  env?: NodeJS.ProcessEnv;
}): void {
  const env = params.env ?? process.env;
  const mode = resolveRuntimeSessionStoreMode(env);
  if (mode === "off") {
    return;
  }
  try {
    persistSessionStoreToRuntimeLedger({
      storePath: params.storePath,
      store: params.store,
      env,
    });
  } catch (err) {
    if (mode === "sqlite" || mode === "sqlite-strict") {
      throw err;
    }
    const storePath = path.resolve(params.storePath);
    const warningKey = `${storePath}:${err instanceof Error ? err.message : String(err)}`;
    if (!warnedMirrorFailures.has(warningKey)) {
      warnedMirrorFailures.add(warningKey);
      log.warn("runtime session ledger mirror failed; legacy session file remains authoritative", {
        storePath,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
}

export function appendTranscriptMessageToRuntimeLedger(params: {
  storePath: string;
  sessionKey: string;
  agentId?: string;
  entry: SessionEntry;
  messageId: string;
  message: { role: string; timestamp?: number };
  env?: NodeJS.ProcessEnv;
}): void {
  const env = params.env ?? process.env;
  const mode = resolveRuntimeSessionStoreMode(env);
  if (mode === "off") {
    return;
  }
  const storePath = path.resolve(params.storePath);
  const agentId = normalizeAgentId(params.agentId ?? inferAgentIdFromStorePath(storePath));
  let db: ReturnType<typeof openLocalKernelDatabase> | undefined;
  try {
    db = openLocalKernelDatabase({ env });
    db.upsertSessionEntry({
      storePath,
      sessionKey: params.sessionKey,
      sessionId: params.entry.sessionId,
      agentId,
      entry: params.entry,
      channel: params.entry.lastChannel ?? params.entry.channel,
      status: inferSessionStatus(params.entry),
      currentTokenCount: params.entry.totalTokens ?? params.entry.contextTokens ?? 0,
      lastActivityAt: dateFromEpochMs(params.entry.lastInteractionAt ?? params.entry.updatedAt),
      createdAt: dateFromEpochMs(params.entry.sessionStartedAt ?? params.entry.startedAt),
      updatedAt: dateFromEpochMs(params.entry.updatedAt),
    });
    db.appendTranscriptEvent({
      sessionId: params.entry.sessionId,
      role: params.message.role,
      eventType: "message",
      content: {
        type: "message",
        id: params.messageId,
        message: params.message,
      },
      createdAt: dateFromEpochMs(params.message.timestamp),
    });
  } catch (err) {
    if (mode === "sqlite" || mode === "sqlite-strict") {
      throw err;
    }
    const warningKey = `${storePath}:${params.messageId}:${
      err instanceof Error ? err.message : String(err)
    }`;
    if (!warnedMirrorFailures.has(warningKey)) {
      warnedMirrorFailures.add(warningKey);
      log.warn("runtime transcript ledger append failed; legacy transcript file remains readable", {
        storePath,
        sessionId: params.entry.sessionId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  } finally {
    db?.close();
  }
}
