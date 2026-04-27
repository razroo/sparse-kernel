import fs from "node:fs/promises";
import path from "node:path";
import { loadSessionStore, resolveSessionFilePath } from "../config/sessions.js";
import type { SessionEntry } from "../config/sessions/types.js";
import type { LocalKernelDatabase } from "./database.js";
import type { TranscriptEventRecord } from "./types.js";

export type SessionStoreImportTarget = {
  agentId: string;
  storePath: string;
};

export type SessionImportResult = {
  storePath: string;
  agentId: string;
  sessions: number;
  importedEvents: number;
  skippedExistingTranscripts: number;
  missingTranscripts: number;
};

function dateFromEpochMs(value: number | undefined): string | undefined {
  return typeof value === "number" && Number.isFinite(value)
    ? new Date(value).toISOString()
    : undefined;
}

function readLegacyEventRole(parsed: Record<string, unknown>): string {
  const message = parsed.message as { role?: unknown } | undefined;
  if (typeof message?.role === "string" && message.role.trim()) {
    return message.role.trim();
  }
  if (typeof parsed.type === "string" && parsed.type.trim()) {
    return parsed.type.trim();
  }
  return "unknown";
}

function readLegacyEventType(parsed: Record<string, unknown>): string {
  return typeof parsed.type === "string" && parsed.type.trim() ? parsed.type.trim() : "entry";
}

function readLegacyCreatedAt(parsed: Record<string, unknown>): string | undefined {
  const message = parsed.message as { timestamp?: unknown } | undefined;
  if (typeof message?.timestamp === "number" && Number.isFinite(message.timestamp)) {
    return new Date(message.timestamp).toISOString();
  }
  if (typeof parsed.timestamp === "string" && parsed.timestamp.trim()) {
    return parsed.timestamp.trim();
  }
  return undefined;
}

function shouldSkipLegacyLine(parsed: Record<string, unknown>): boolean {
  return parsed.type === "session";
}

async function importTranscript(params: {
  db: LocalKernelDatabase;
  sessionId: string;
  transcriptPath: string;
}): Promise<{ importedEvents: number; missing: boolean; skippedExisting: boolean }> {
  const existingEvents = params.db.listTranscriptEvents(params.sessionId);
  const existingLegacyIds = new Set<string>();
  const importedIds = new Map<string, number>();
  for (const event of existingEvents) {
    const content = event.content;
    if (!content || typeof content !== "object") {
      continue;
    }
    const legacyId = (content as { id?: unknown }).id;
    if (typeof legacyId === "string" && legacyId.trim()) {
      existingLegacyIds.add(legacyId);
      importedIds.set(legacyId, event.id);
    }
  }
  let raw: string;
  try {
    raw = await fs.readFile(params.transcriptPath, "utf-8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      return { importedEvents: 0, missing: true, skippedExisting: false };
    }
    throw err;
  }
  let importedEvents = 0;
  for (const line of raw.split(/\r?\n/)) {
    if (!line.trim()) {
      continue;
    }
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(line) as Record<string, unknown>;
    } catch {
      params.db.recordAudit({
        actor: { type: "runtime" },
        action: "session_import.skipped_malformed_line",
        objectType: "session",
        objectId: params.sessionId,
      });
      continue;
    }
    if (shouldSkipLegacyLine(parsed)) {
      continue;
    }
    const legacyId = typeof parsed.id === "string" && parsed.id.trim() ? parsed.id : undefined;
    if (legacyId && existingLegacyIds.has(legacyId)) {
      continue;
    }
    const parentId =
      typeof parsed.parentId === "string" ? importedIds.get(parsed.parentId) : undefined;
    const event = params.db.appendTranscriptEvent({
      sessionId: params.sessionId,
      parentEventId: parentId,
      role: readLegacyEventRole(parsed),
      eventType: readLegacyEventType(parsed),
      content: parsed,
      createdAt: readLegacyCreatedAt(parsed),
    });
    if (legacyId) {
      importedIds.set(legacyId, event.id);
    }
    importedEvents += 1;
  }
  return { importedEvents, missing: false, skippedExisting: existingEvents.length > 0 };
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

export async function importLegacySessionStore(params: {
  db: LocalKernelDatabase;
  target: SessionStoreImportTarget;
}): Promise<SessionImportResult> {
  const store = loadSessionStore(params.target.storePath, {
    skipCache: true,
    skipRuntimeLedger: true,
  });
  let importedEvents = 0;
  let skippedExistingTranscripts = 0;
  let missingTranscripts = 0;
  for (const [sessionKey, entry] of Object.entries(store)) {
    if (!entry?.sessionId) {
      continue;
    }
    params.db.upsertSessionEntry({
      storePath: path.resolve(params.target.storePath),
      sessionKey,
      sessionId: entry.sessionId,
      agentId: params.target.agentId,
      entry,
      channel: entry.lastChannel ?? entry.channel,
      status: inferSessionStatus(entry),
      currentTokenCount: entry.totalTokens ?? entry.contextTokens ?? 0,
      lastActivityAt: dateFromEpochMs(entry.lastInteractionAt ?? entry.updatedAt),
      createdAt: dateFromEpochMs(entry.sessionStartedAt ?? entry.startedAt),
      updatedAt: dateFromEpochMs(entry.updatedAt),
    });
    const transcriptPath = resolveSessionFilePath(entry.sessionId, entry, {
      agentId: params.target.agentId,
      sessionsDir: path.dirname(params.target.storePath),
    });
    const result = await importTranscript({
      db: params.db,
      sessionId: entry.sessionId,
      transcriptPath,
    });
    importedEvents += result.importedEvents;
    skippedExistingTranscripts += result.skippedExisting ? 1 : 0;
    missingTranscripts += result.missing ? 1 : 0;
  }
  return {
    storePath: params.target.storePath,
    agentId: params.target.agentId,
    sessions: Object.keys(store).length,
    importedEvents,
    skippedExistingTranscripts,
    missingTranscripts,
  };
}

function legacyLineForEvent(event: TranscriptEventRecord): unknown {
  if (event.content && typeof event.content === "object") {
    return event.content;
  }
  return {
    type: event.eventType,
    id: `kernel-${event.id}`,
    message: {
      role: event.role,
      content: event.content ?? "",
      timestamp: Date.parse(event.createdAt),
    },
  };
}

export function exportSessionAsJsonl(params: {
  db: LocalKernelDatabase;
  sessionId: string;
}): string {
  const session = params.db.getSession(params.sessionId);
  if (!session) {
    throw new Error(`Session not found in runtime ledger: ${params.sessionId}`);
  }
  const lines = [
    JSON.stringify({
      type: "session",
      version: 1,
      id: session.id,
      timestamp: session.createdAt,
    }),
  ];
  for (const event of params.db.listTranscriptEvents(params.sessionId)) {
    lines.push(JSON.stringify(legacyLineForEvent(event)));
  }
  return `${lines.join("\n")}\n`;
}
