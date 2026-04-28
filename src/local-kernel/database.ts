import crypto from "node:crypto";
import { chmodSync, existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import type { DatabaseSync, SQLInputValue } from "node:sqlite";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { configureSqliteWalMaintenance, type SqliteWalMaintenance } from "../infra/sqlite-wal.js";
import { resolveRuntimeKernelDbPath, resolveRuntimeKernelDir } from "./paths.js";
import { LOCAL_KERNEL_MIGRATIONS, LOCAL_KERNEL_SCHEMA_VERSION } from "./schema.js";
import type {
  ArtifactRecord,
  ArtifactRecordInput,
  BrowserContextRecord,
  ClaimTaskByIdInput,
  CapabilityCheckInput,
  EnqueueTaskInput,
  GrantCapabilityInput,
  KernelActor,
  KernelAgentInput,
  KernelAuditInput,
  KernelSessionInput,
  KernelSessionRecord,
  KernelTaskRecord,
  NetworkPolicyRecord,
  TranscriptEventInput,
  TranscriptEventRecord,
  TrustZoneRecord,
  UsageSummaryRecord,
} from "./types.js";

const KERNEL_DIR_MODE = 0o700;
const KERNEL_FILE_MODE = 0o600;
const KERNEL_SIDECAR_SUFFIXES = ["", "-shm", "-wal"] as const;

type MigrationRow = { version: number | bigint };
type CountRow = { count: number | bigint };
type MaxSeqRow = { seq: number | bigint | null };

type SessionRow = {
  id: string;
  agent_id: string;
  session_key: string | null;
  channel: string | null;
  status: string;
  current_token_count: number | bigint;
  compacted_until_event_id: number | bigint | null;
  last_activity_at: string | null;
  created_at: string;
  updated_at: string;
};

type TranscriptEventRow = {
  id: number | bigint;
  session_id: string;
  parent_event_id: number | bigint | null;
  seq: number | bigint;
  role: string;
  event_type: string;
  content_json: string | null;
  tool_call_id: string | null;
  token_count: number | bigint | null;
  created_at: string;
};

type TaskRow = {
  id: string;
  agent_id: string | null;
  session_id: string | null;
  kind: string;
  priority: number | bigint;
  status: KernelTaskRecord["status"];
  idempotency_key: string | null;
  lease_owner: string | null;
  lease_until: string | null;
  attempts: number | bigint;
  input_json: string | null;
  result_artifact_id: string | null;
  created_at: string;
  updated_at: string;
};

type AuditRow = {
  id: number | bigint;
  actor_type: string | null;
  actor_id: string | null;
  action: string;
  object_type: string | null;
  object_id: string | null;
  payload_json: string | null;
  created_at: string;
};

type ArtifactRow = {
  id: string;
  sha256: string;
  mime_type: string | null;
  size_bytes: number | bigint;
  storage_ref: string;
  created_by_task_id: string | null;
  created_by_tool_call_id: string | null;
  classification: string | null;
  retention_policy: ArtifactRecord["retentionPolicy"] | null;
  created_at: string;
};

type BrowserContextRow = {
  id: string;
  pool_id: string;
  agent_id: string | null;
  session_id: string | null;
  task_id: string | null;
  profile_mode: string;
  allowed_origins_json: string | null;
  status: string;
  created_at: string;
  expires_at: string | null;
};

type SessionEntryRow = {
  store_path: string;
  session_key: string;
  session_id: string;
  agent_id: string;
  entry_json: string;
  created_at: string;
  updated_at: string;
};

type TrustZoneRow = {
  id: string;
  description: string | null;
  sandbox_backend: string;
  network_policy_id: string | null;
  filesystem_policy_json: string | null;
  max_processes: number | bigint | null;
  max_memory_mb: number | bigint | null;
  max_runtime_seconds: number | bigint | null;
  created_at: string;
};

type UsageSummaryRow = {
  resource_type: string;
  unit: string;
  amount: number | bigint;
};

type NetworkPolicyRow = {
  id: string;
  default_action: "allow" | "deny";
  allow_private_network: number | bigint;
  allowed_hosts_json: string | null;
  denied_cidrs_json: string | null;
  proxy_ref: string | null;
  created_at: string;
};

export type OpenLocalKernelDatabaseOptions = {
  dbPath?: string;
  env?: NodeJS.ProcessEnv;
  migrate?: boolean;
};

function nowIso(): string {
  return new Date().toISOString();
}

function futureIso(now: string, ms: number): string {
  return new Date(Date.parse(now) + ms).toISOString();
}

function jsonToText(value: unknown): string | null {
  return value === undefined ? null : JSON.stringify(value);
}

function parseJsonText(raw: string | null): unknown {
  if (!raw) {
    return undefined;
  }
  return JSON.parse(raw) as unknown;
}

function numberFromSql(value: number | bigint | null | undefined): number | undefined {
  if (typeof value === "bigint") {
    return Number(value);
  }
  return typeof value === "number" ? value : undefined;
}

function optionalText(value: string | null | undefined): string | undefined {
  return value?.trim() ? value : undefined;
}

function changes(result: { changes?: number | bigint }): number {
  return numberFromSql(result.changes) ?? 0;
}

function ensureRuntimeDbPermissions(dbPath: string, env: NodeJS.ProcessEnv): void {
  if (dbPath === ":memory:") {
    return;
  }
  const dir = dbPath === resolveRuntimeKernelDbPath(env) ? resolveRuntimeKernelDir(env) : undefined;
  mkdirSync(dir ?? path.dirname(dbPath), {
    recursive: true,
    mode: KERNEL_DIR_MODE,
  });
  if (dir) {
    chmodSync(dir, KERNEL_DIR_MODE);
  }
  for (const suffix of KERNEL_SIDECAR_SUFFIXES) {
    const candidate = `${dbPath}${suffix}`;
    if (existsSync(candidate)) {
      chmodSync(candidate, KERNEL_FILE_MODE);
    }
  }
}

function toSession(row: SessionRow): KernelSessionRecord {
  return {
    id: row.id,
    agentId: row.agent_id,
    ...(optionalText(row.session_key) ? { sessionKey: row.session_key! } : {}),
    ...(optionalText(row.channel) ? { channel: row.channel! } : {}),
    status: row.status,
    currentTokenCount: numberFromSql(row.current_token_count) ?? 0,
    ...(numberFromSql(row.compacted_until_event_id) !== undefined
      ? { compactedUntilEventId: numberFromSql(row.compacted_until_event_id) }
      : {}),
    ...(optionalText(row.last_activity_at) ? { lastActivityAt: row.last_activity_at! } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toTranscriptEvent(row: TranscriptEventRow): TranscriptEventRecord {
  return {
    id: numberFromSql(row.id) ?? 0,
    sessionId: row.session_id,
    ...(numberFromSql(row.parent_event_id) !== undefined
      ? { parentEventId: numberFromSql(row.parent_event_id) }
      : {}),
    seq: numberFromSql(row.seq) ?? 0,
    role: row.role,
    eventType: row.event_type,
    ...(row.content_json ? { content: parseJsonText(row.content_json) } : {}),
    ...(optionalText(row.tool_call_id) ? { toolCallId: row.tool_call_id! } : {}),
    ...(numberFromSql(row.token_count) !== undefined
      ? { tokenCount: numberFromSql(row.token_count) }
      : {}),
    createdAt: row.created_at,
  };
}

function toTask(row: TaskRow): KernelTaskRecord {
  return {
    id: row.id,
    ...(optionalText(row.agent_id) ? { agentId: row.agent_id! } : {}),
    ...(optionalText(row.session_id) ? { sessionId: row.session_id! } : {}),
    kind: row.kind,
    priority: numberFromSql(row.priority) ?? 0,
    status: row.status,
    ...(optionalText(row.idempotency_key) ? { idempotencyKey: row.idempotency_key! } : {}),
    ...(optionalText(row.lease_owner) ? { leaseOwner: row.lease_owner! } : {}),
    ...(optionalText(row.lease_until) ? { leaseUntil: row.lease_until! } : {}),
    attempts: numberFromSql(row.attempts) ?? 0,
    ...(row.input_json ? { input: parseJsonText(row.input_json) } : {}),
    ...(optionalText(row.result_artifact_id) ? { resultArtifactId: row.result_artifact_id! } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toAudit(row: AuditRow) {
  return {
    id: numberFromSql(row.id) ?? 0,
    ...(optionalText(row.actor_type) ? { actorType: row.actor_type! } : {}),
    ...(optionalText(row.actor_id) ? { actorId: row.actor_id! } : {}),
    action: row.action,
    ...(optionalText(row.object_type) ? { objectType: row.object_type! } : {}),
    ...(optionalText(row.object_id) ? { objectId: row.object_id! } : {}),
    ...(row.payload_json ? { payload: parseJsonText(row.payload_json) } : {}),
    createdAt: row.created_at,
  };
}

function toArtifact(row: ArtifactRow): ArtifactRecord {
  return {
    id: row.id,
    sha256: row.sha256,
    ...(optionalText(row.mime_type) ? { mimeType: row.mime_type! } : {}),
    sizeBytes: numberFromSql(row.size_bytes) ?? 0,
    storageRef: row.storage_ref,
    ...(optionalText(row.created_by_task_id) ? { createdByTaskId: row.created_by_task_id! } : {}),
    ...(optionalText(row.created_by_tool_call_id)
      ? { createdByToolCallId: row.created_by_tool_call_id! }
      : {}),
    ...(optionalText(row.classification) ? { classification: row.classification! } : {}),
    ...(optionalText(row.retention_policy) ? { retentionPolicy: row.retention_policy! } : {}),
    createdAt: row.created_at,
  };
}

function toBrowserContext(row: BrowserContextRow): BrowserContextRecord {
  return {
    id: row.id,
    poolId: row.pool_id,
    ...(optionalText(row.agent_id) ? { agentId: row.agent_id! } : {}),
    ...(optionalText(row.session_id) ? { sessionId: row.session_id! } : {}),
    ...(optionalText(row.task_id) ? { taskId: row.task_id! } : {}),
    profileMode: row.profile_mode,
    ...(row.allowed_origins_json
      ? { allowedOrigins: parseJsonText(row.allowed_origins_json) }
      : {}),
    status: row.status,
    createdAt: row.created_at,
    ...(optionalText(row.expires_at) ? { expiresAt: row.expires_at! } : {}),
  };
}

function toTrustZone(row: TrustZoneRow): TrustZoneRecord {
  return {
    id: row.id,
    ...(optionalText(row.description) ? { description: row.description! } : {}),
    sandboxBackend: row.sandbox_backend,
    ...(optionalText(row.network_policy_id) ? { networkPolicyId: row.network_policy_id! } : {}),
    ...(row.filesystem_policy_json
      ? { filesystemPolicy: parseJsonText(row.filesystem_policy_json) }
      : {}),
    ...(numberFromSql(row.max_processes) !== undefined
      ? { maxProcesses: numberFromSql(row.max_processes) }
      : {}),
    ...(numberFromSql(row.max_memory_mb) !== undefined
      ? { maxMemoryMb: numberFromSql(row.max_memory_mb) }
      : {}),
    ...(numberFromSql(row.max_runtime_seconds) !== undefined
      ? { maxRuntimeSeconds: numberFromSql(row.max_runtime_seconds) }
      : {}),
    createdAt: row.created_at,
  };
}

function toNetworkPolicy(row: NetworkPolicyRow): NetworkPolicyRecord {
  const allowedHosts = row.allowed_hosts_json
    ? (parseJsonText(row.allowed_hosts_json) as string[])
    : undefined;
  const deniedCidrs = row.denied_cidrs_json
    ? (parseJsonText(row.denied_cidrs_json) as string[])
    : undefined;
  return {
    id: row.id,
    defaultAction: row.default_action,
    allowPrivateNetwork: Number(row.allow_private_network) !== 0,
    ...(Array.isArray(allowedHosts) ? { allowedHosts } : {}),
    ...(Array.isArray(deniedCidrs) ? { deniedCidrs } : {}),
    ...(optionalText(row.proxy_ref) ? { proxyRef: row.proxy_ref! } : {}),
    createdAt: row.created_at,
  };
}

export class LocalKernelDatabase {
  readonly db: DatabaseSync;
  readonly dbPath: string;
  private readonly walMaintenance: SqliteWalMaintenance;
  private closed = false;

  constructor(options: OpenLocalKernelDatabaseOptions = {}) {
    const env = options.env ?? process.env;
    this.dbPath = options.dbPath ?? resolveRuntimeKernelDbPath(env);
    ensureRuntimeDbPermissions(this.dbPath, env);
    const { DatabaseSync } = requireNodeSqlite();
    this.db = new DatabaseSync(this.dbPath);
    this.walMaintenance = configureSqliteWalMaintenance(this.db);
    this.db.exec("PRAGMA foreign_keys = ON;");
    this.db.exec("PRAGMA synchronous = NORMAL;");
    this.db.exec("PRAGMA busy_timeout = 5000;");
    if (options.migrate !== false) {
      this.migrate();
    }
    ensureRuntimeDbPermissions(this.dbPath, env);
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.walMaintenance.close();
    this.db.close();
    this.closed = true;
  }

  migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS schema_migrations(
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
      );
    `);
    const applied = new Set(
      (this.db.prepare("SELECT version FROM schema_migrations").all() as MigrationRow[]).map(
        (row) => Number(row.version),
      ),
    );
    for (const migration of LOCAL_KERNEL_MIGRATIONS) {
      if (applied.has(migration.version)) {
        continue;
      }
      this.withTransaction(() => {
        for (const statement of migration.statements) {
          this.db.exec(statement);
        }
        this.db
          .prepare("INSERT INTO schema_migrations(version, applied_at) VALUES(?, ?)")
          .run(migration.version, nowIso());
      });
    }
  }

  schemaVersion(): number {
    const row = this.db.prepare("SELECT MAX(version) AS version FROM schema_migrations").get() as
      | MigrationRow
      | undefined;
    return row ? Number(row.version) : 0;
  }

  inspect(): { path: string; schemaVersion: number; counts: Record<string, number> } {
    const tables = [
      "agents",
      "sessions",
      "transcript_events",
      "tasks",
      "tool_calls",
      "resource_leases",
      "browser_contexts",
      "artifacts",
      "capabilities",
      "audit_log",
    ];
    const countsOut: Record<string, number> = {};
    for (const table of tables) {
      const row = this.db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as CountRow;
      countsOut[table] = Number(row.count);
    }
    return { path: this.dbPath, schemaVersion: this.schemaVersion(), counts: countsOut };
  }

  vacuum(): void {
    this.db.exec("VACUUM;");
  }

  listTrustZones(): TrustZoneRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM trust_zones ORDER BY id ASC")
      .all() as TrustZoneRow[];
    return rows.map(toTrustZone);
  }

  getNetworkPolicyForTrustZone(trustZoneId: string): NetworkPolicyRecord | undefined {
    const row = this.db
      .prepare(
        `SELECT np.*
         FROM trust_zones tz
         JOIN network_policies np ON np.id = tz.network_policy_id
         WHERE tz.id = ?`,
      )
      .get(trustZoneId) as NetworkPolicyRow | undefined;
    return row ? toNetworkPolicy(row) : undefined;
  }

  updateTrustZoneLimits(input: {
    id: string;
    maxProcesses?: number | null;
    maxMemoryMb?: number | null;
    maxRuntimeSeconds?: number | null;
  }): boolean {
    const updated = changes(
      this.db
        .prepare(
          `UPDATE trust_zones
           SET max_processes = COALESCE(?, max_processes),
               max_memory_mb = COALESCE(?, max_memory_mb),
               max_runtime_seconds = COALESCE(?, max_runtime_seconds)
           WHERE id = ?`,
        )
        .run(
          input.maxProcesses ?? null,
          input.maxMemoryMb ?? null,
          input.maxRuntimeSeconds ?? null,
          input.id,
        ),
    );
    if (updated > 0) {
      this.recordAudit({
        actor: { type: "operator" },
        action: "trust_zone.limits_updated",
        objectType: "trust_zone",
        objectId: input.id,
        payload: {
          maxProcesses: input.maxProcesses,
          maxMemoryMb: input.maxMemoryMb,
          maxRuntimeSeconds: input.maxRuntimeSeconds,
        },
      });
    }
    return updated > 0;
  }

  recordUsage(input: {
    agentId?: string;
    sessionId?: string;
    taskId?: string;
    resourceType: string;
    amount: number;
    unit: string;
    createdAt?: string;
  }): void {
    this.db
      .prepare(
        `INSERT INTO usage_records(
          agent_id, session_id, task_id, resource_type, amount, unit, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        input.agentId ?? null,
        input.sessionId ?? null,
        input.taskId ?? null,
        input.resourceType,
        Math.trunc(input.amount),
        input.unit,
        input.createdAt ?? nowIso(),
      );
  }

  summarizeUsage(input: { since?: string } = {}): UsageSummaryRecord[] {
    const where = input.since ? "WHERE created_at >= ?" : "";
    const rows = this.db
      .prepare(
        `SELECT resource_type, unit, SUM(amount) AS amount
         FROM usage_records
         ${where}
         GROUP BY resource_type, unit
         ORDER BY resource_type ASC, unit ASC`,
      )
      .all(...(input.since ? [input.since] : [])) as UsageSummaryRow[];
    return rows.map((row) => ({
      resourceType: row.resource_type,
      unit: row.unit,
      amount: numberFromSql(row.amount) ?? 0,
    }));
  }

  withTransaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      throw error;
    }
  }

  recordAudit(input: KernelAuditInput): void {
    this.db
      .prepare(
        `INSERT INTO audit_log(actor_type, actor_id, action, object_type, object_id, payload_json, created_at)
         VALUES(?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        input.actor?.type ?? null,
        input.actor?.id ?? null,
        input.action,
        input.objectType ?? null,
        input.objectId ?? null,
        jsonToText(input.payload),
        input.createdAt ?? nowIso(),
      );
  }

  listAudit(input: { limit?: number } = {}) {
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit ?? 100)));
    const rows = this.db
      .prepare(
        `SELECT id, actor_type, actor_id, action, object_type, object_id, payload_json, created_at
         FROM audit_log
         ORDER BY id DESC
         LIMIT ?`,
      )
      .all(limit) as AuditRow[];
    return rows.map(toAudit);
  }

  ensureAgent(input: KernelAgentInput): void {
    const now = input.now ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO agents(id, name, role, status, created_at, updated_at)
         VALUES(?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
           name=COALESCE(excluded.name, agents.name),
           role=COALESCE(excluded.role, agents.role),
           status=excluded.status,
           updated_at=excluded.updated_at`,
      )
      .run(input.id, input.name ?? null, input.role ?? null, input.status ?? "active", now, now);
  }

  upsertSession(input: KernelSessionInput): KernelSessionRecord {
    this.ensureAgent({ id: input.agentId });
    const now = input.updatedAt ?? input.createdAt ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO sessions(
          id, agent_id, session_key, channel, status, current_token_count, last_activity_at, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          agent_id=excluded.agent_id,
          session_key=excluded.session_key,
          channel=excluded.channel,
          status=excluded.status,
          current_token_count=excluded.current_token_count,
          last_activity_at=excluded.last_activity_at,
          updated_at=excluded.updated_at`,
      )
      .run(
        input.id,
        input.agentId,
        input.sessionKey ?? null,
        input.channel ?? null,
        input.status ?? "active",
        input.currentTokenCount ?? 0,
        input.lastActivityAt ?? null,
        input.createdAt ?? now,
        now,
      );
    const session = this.getSession(input.id);
    if (!session) {
      throw new Error(`Failed to upsert kernel session ${input.id}`);
    }
    return session;
  }

  getSession(id: string): KernelSessionRecord | undefined {
    const row = this.db.prepare("SELECT * FROM sessions WHERE id = ?").get(id) as
      | SessionRow
      | undefined;
    return row ? toSession(row) : undefined;
  }

  listSessions(input: { agentId?: string; limit?: number } = {}): KernelSessionRecord[] {
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit ?? 50)));
    const rows = input.agentId?.trim()
      ? (this.db
          .prepare(
            `SELECT * FROM sessions
             WHERE agent_id = ?
             ORDER BY COALESCE(last_activity_at, updated_at, created_at) DESC, id ASC
             LIMIT ?`,
          )
          .all(input.agentId.trim(), limit) as SessionRow[])
      : (this.db
          .prepare(
            `SELECT * FROM sessions
             ORDER BY COALESCE(last_activity_at, updated_at, created_at) DESC, id ASC
             LIMIT ?`,
          )
          .all(limit) as SessionRow[]);
    return rows.map(toSession);
  }

  appendTranscriptEvent(input: TranscriptEventInput): TranscriptEventRecord {
    const now = input.createdAt ?? nowIso();
    return this.withTransaction(() => {
      const maxRow = this.db
        .prepare("SELECT MAX(seq) AS seq FROM transcript_events WHERE session_id = ?")
        .get(input.sessionId) as MaxSeqRow;
      const seq = (numberFromSql(maxRow.seq) ?? 0) + 1;
      const result = this.db
        .prepare(
          `INSERT INTO transcript_events(
            session_id, parent_event_id, seq, role, event_type, content_json, tool_call_id, token_count, created_at
          ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          input.sessionId,
          input.parentEventId ?? null,
          seq,
          input.role,
          input.eventType,
          jsonToText(input.content),
          input.toolCallId ?? null,
          input.tokenCount ?? null,
          now,
        );
      const id = numberFromSql(result.lastInsertRowid) ?? 0;
      this.recordAudit({
        actor: { type: "runtime" },
        action: "transcript_event.appended",
        objectType: "transcript_event",
        objectId: String(id),
        payload: { sessionId: input.sessionId, seq, role: input.role, eventType: input.eventType },
        createdAt: now,
      });
      return this.getTranscriptEvent(id);
    });
  }

  getTranscriptEvent(id: number): TranscriptEventRecord {
    const row = this.db.prepare("SELECT * FROM transcript_events WHERE id = ?").get(id) as
      | TranscriptEventRow
      | undefined;
    if (!row) {
      throw new Error(`Transcript event not found: ${id}`);
    }
    return toTranscriptEvent(row);
  }

  listTranscriptEvents(sessionId: string): TranscriptEventRecord[] {
    const rows = this.db
      .prepare("SELECT * FROM transcript_events WHERE session_id = ? ORDER BY seq ASC")
      .all(sessionId) as TranscriptEventRow[];
    return rows.map(toTranscriptEvent);
  }

  upsertSessionEntry(input: {
    storePath: string;
    sessionKey: string;
    sessionId: string;
    agentId: string;
    entry: unknown;
    channel?: string;
    status?: string;
    currentTokenCount?: number;
    lastActivityAt?: string;
    createdAt?: string;
    updatedAt?: string;
  }): void {
    const now = input.updatedAt ?? input.createdAt ?? nowIso();
    this.upsertSession({
      id: input.sessionId,
      agentId: input.agentId,
      sessionKey: input.sessionKey,
      channel: input.channel,
      status: input.status ?? "active",
      currentTokenCount: input.currentTokenCount,
      lastActivityAt: input.lastActivityAt,
      createdAt: input.createdAt,
      updatedAt: now,
    });
    this.db
      .prepare(
        `INSERT INTO session_entries(
          store_path, session_key, session_id, agent_id, entry_json, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(store_path, session_key) DO UPDATE SET
          session_id=excluded.session_id,
          agent_id=excluded.agent_id,
          entry_json=excluded.entry_json,
          updated_at=excluded.updated_at`,
      )
      .run(
        input.storePath,
        input.sessionKey,
        input.sessionId,
        input.agentId,
        JSON.stringify(input.entry),
        input.createdAt ?? now,
        now,
      );
  }

  replaceSessionEntriesForStore(input: {
    storePath: string;
    entries: Array<{
      sessionKey: string;
      sessionId: string;
      agentId: string;
      entry: unknown;
      channel?: string;
      status?: string;
      currentTokenCount?: number;
      lastActivityAt?: string;
      createdAt?: string;
      updatedAt?: string;
    }>;
    now?: string;
  }): void {
    const now = input.now ?? nowIso();
    this.withTransaction(() => {
      const retainedKeys = new Set<string>();
      for (const entry of input.entries) {
        retainedKeys.add(entry.sessionKey);
        this.upsertSessionEntry({
          storePath: input.storePath,
          ...entry,
          updatedAt: entry.updatedAt ?? now,
        });
      }
      const existing = this.db
        .prepare("SELECT session_key FROM session_entries WHERE store_path = ?")
        .all(input.storePath) as Array<{ session_key: string }>;
      for (const row of existing) {
        if (retainedKeys.has(row.session_key)) {
          continue;
        }
        this.db
          .prepare("DELETE FROM session_entries WHERE store_path = ? AND session_key = ?")
          .run(input.storePath, row.session_key);
      }
      this.recordAudit({
        actor: { type: "runtime" },
        action: "session_store.mirrored",
        objectType: "session_store",
        objectId: input.storePath,
        payload: { entries: input.entries.length },
        createdAt: now,
      });
    });
  }

  loadSessionEntriesForStore(storePath: string): Record<string, unknown> | undefined {
    const rows = this.db
      .prepare(
        `SELECT * FROM session_entries
         WHERE store_path = ?
         ORDER BY session_key ASC`,
      )
      .all(storePath) as SessionEntryRow[];
    if (rows.length === 0) {
      return undefined;
    }
    const store: Record<string, unknown> = {};
    for (const row of rows) {
      store[row.session_key] = JSON.parse(row.entry_json) as unknown;
    }
    return store;
  }

  enqueueTask(input: EnqueueTaskInput): KernelTaskRecord {
    const id = input.id ?? crypto.randomUUID();
    const now = input.createdAt ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO tasks(
          id, agent_id, session_id, kind, priority, status, idempotency_key, input_json, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING`,
      )
      .run(
        id,
        input.agentId ?? null,
        input.sessionId ?? null,
        input.kind,
        input.priority ?? 0,
        input.idempotencyKey ?? null,
        jsonToText(input.input),
        now,
        now,
      );
    this.recordTaskEvent(id, "queued", { kind: input.kind, priority: input.priority ?? 0 }, now);
    this.recordAudit({
      actor: { type: "runtime" },
      action: "task.enqueued",
      objectType: "task",
      objectId: id,
      payload: { kind: input.kind },
      createdAt: now,
    });
    const task = this.getTask(id);
    if (!task) {
      throw new Error(`Failed to enqueue task ${id}`);
    }
    return task;
  }

  getTask(id: string): KernelTaskRecord | undefined {
    const row = this.db.prepare("SELECT * FROM tasks WHERE id = ?").get(id) as TaskRow | undefined;
    return row ? toTask(row) : undefined;
  }

  listTasks(input: { status?: string; kind?: string; limit?: number } = {}): KernelTaskRecord[] {
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit ?? 50)));
    const predicates: string[] = [];
    const args: SQLInputValue[] = [];
    if (input.status?.trim()) {
      predicates.push("status = ?");
      args.push(input.status.trim());
    }
    if (input.kind?.trim()) {
      predicates.push("kind = ?");
      args.push(input.kind.trim());
    }
    const where = predicates.length > 0 ? `WHERE ${predicates.join(" AND ")}` : "";
    const rows = this.db
      .prepare(
        `SELECT * FROM tasks
         ${where}
         ORDER BY priority DESC, created_at DESC, id ASC
         LIMIT ?`,
      )
      .all(...args, limit) as TaskRow[];
    return rows.map(toTask);
  }

  recoverTaskLease(input: {
    taskId: string;
    reason: string;
    actor?: KernelActor;
    now?: string;
  }): boolean {
    const now = input.now ?? nowIso();
    const updated = changes(
      this.db
        .prepare(
          `UPDATE tasks
           SET status = 'queued', lease_owner = NULL, lease_until = NULL, updated_at = ?
           WHERE id = ? AND status = 'running'`,
        )
        .run(now, input.taskId),
    );
    if (updated === 1) {
      this.recordTaskEvent(input.taskId, "recovered", { reason: input.reason }, now);
      this.recordAudit({
        actor: input.actor ?? { type: "runtime" },
        action: "task.recovered",
        objectType: "task",
        objectId: input.taskId,
        payload: { reason: input.reason },
        createdAt: now,
      });
    }
    return updated === 1;
  }

  claimNextTask(input: {
    workerId: string;
    kinds?: string[];
    leaseMs?: number;
    now?: string;
  }): KernelTaskRecord | null {
    const now = input.now ?? nowIso();
    const leaseUntil = futureIso(now, input.leaseMs ?? 60_000);
    return this.withTransaction(() => {
      const kinds = (input.kinds ?? []).map((kind) => kind.trim()).filter(Boolean);
      const whereKind = kinds.length > 0 ? ` AND kind IN (${kinds.map(() => "?").join(", ")})` : "";
      const row = this.db
        .prepare(
          `SELECT * FROM tasks
           WHERE status = 'queued'${whereKind}
           ORDER BY priority DESC, created_at ASC, id ASC
           LIMIT 1`,
        )
        .get(...(kinds as SQLInputValue[])) as TaskRow | undefined;
      if (!row) {
        return null;
      }
      const updated = changes(
        this.db
          .prepare(
            `UPDATE tasks
             SET status = 'running', lease_owner = ?, lease_until = ?, attempts = attempts + 1, updated_at = ?
             WHERE id = ? AND status = 'queued'`,
          )
          .run(input.workerId, leaseUntil, now, row.id),
      );
      if (updated !== 1) {
        return null;
      }
      this.recordTaskEvent(row.id, "claimed", { workerId: input.workerId, leaseUntil }, now);
      this.recordAudit({
        actor: { type: "worker", id: input.workerId },
        action: "task.claimed",
        objectType: "task",
        objectId: row.id,
        payload: { leaseUntil },
        createdAt: now,
      });
      return this.getTask(row.id) ?? null;
    });
  }

  claimTask(input: ClaimTaskByIdInput): KernelTaskRecord | null {
    const now = input.now ?? nowIso();
    const leaseUntil = futureIso(now, input.leaseMs ?? 60_000);
    return this.withTransaction(() => {
      const updated = changes(
        this.db
          .prepare(
            `UPDATE tasks
             SET status = 'running', lease_owner = ?, lease_until = ?, attempts = attempts + 1, updated_at = ?
             WHERE id = ? AND status = 'queued'`,
          )
          .run(input.workerId, leaseUntil, now, input.taskId),
      );
      if (updated !== 1) {
        return null;
      }
      this.recordTaskEvent(input.taskId, "claimed", { workerId: input.workerId, leaseUntil }, now);
      this.recordAudit({
        actor: { type: "worker", id: input.workerId },
        action: "task.claimed",
        objectType: "task",
        objectId: input.taskId,
        payload: { leaseUntil },
        createdAt: now,
      });
      return this.getTask(input.taskId) ?? null;
    });
  }

  heartbeatTask(taskId: string, workerId: string, leaseMs = 60_000, now = nowIso()): boolean {
    const task = this.getTask(taskId);
    if (!task || task.status !== "running" || task.leaseOwner !== workerId) {
      return false;
    }
    const leaseUntil = futureIso(now, leaseMs);
    const updated = changes(
      this.db
        .prepare(
          `UPDATE tasks SET lease_until = ?, updated_at = ?
           WHERE id = ? AND status = 'running' AND lease_owner = ?`,
        )
        .run(leaseUntil, now, taskId, workerId),
    );
    if (updated === 1) {
      this.recordTaskEvent(taskId, "heartbeat", { workerId, leaseUntil }, now);
    }
    return updated === 1;
  }

  completeTask(
    taskId: string,
    workerId: string,
    result: { artifactId?: string; output?: unknown },
    now = nowIso(),
  ): boolean {
    const updated = changes(
      this.db
        .prepare(
          `UPDATE tasks
           SET status = 'succeeded', lease_owner = NULL, lease_until = NULL, result_artifact_id = ?, updated_at = ?
           WHERE id = ? AND status = 'running' AND lease_owner = ?`,
        )
        .run(result.artifactId ?? null, now, taskId, workerId),
    );
    if (updated === 1) {
      this.recordTaskEvent(taskId, "completed", result, now);
      this.recordAudit({
        actor: { type: "worker", id: workerId },
        action: "task.completed",
        objectType: "task",
        objectId: taskId,
        payload: result,
        createdAt: now,
      });
    }
    return updated === 1;
  }

  failTask(taskId: string, workerId: string, error: unknown, now = nowIso()): boolean {
    const payload = { error: error instanceof Error ? error.message : String(error) };
    const updated = changes(
      this.db
        .prepare(
          `UPDATE tasks
           SET status = 'failed', lease_owner = NULL, lease_until = NULL, updated_at = ?
           WHERE id = ? AND status = 'running' AND lease_owner = ?`,
        )
        .run(now, taskId, workerId),
    );
    if (updated === 1) {
      this.recordTaskEvent(taskId, "failed", payload, now);
      this.recordAudit({
        actor: { type: "worker", id: workerId },
        action: "task.failed",
        objectType: "task",
        objectId: taskId,
        payload,
        createdAt: now,
      });
    }
    return updated === 1;
  }

  releaseExpiredLeases(now = nowIso()): number {
    return this.withTransaction(() => {
      const taskRows = this.db
        .prepare(
          `SELECT id FROM tasks
           WHERE status = 'running' AND lease_until IS NOT NULL AND lease_until <= ?`,
        )
        .all(now) as Array<{ id: string }>;
      const updatedTasks = changes(
        this.db
          .prepare(
            `UPDATE tasks
             SET status = 'queued', lease_owner = NULL, lease_until = NULL, updated_at = ?
             WHERE status = 'running' AND lease_until IS NOT NULL AND lease_until <= ?`,
          )
          .run(now, now),
      );
      for (const row of taskRows) {
        this.recordTaskEvent(row.id, "lease_expired", {}, now);
      }
      const expiredResourceLeases = changes(
        this.db
          .prepare(
            `UPDATE resource_leases
             SET status = 'expired', updated_at = ?
             WHERE status = 'active' AND lease_until IS NOT NULL AND lease_until <= ?`,
          )
          .run(now, now),
      );
      if (updatedTasks > 0 || expiredResourceLeases > 0) {
        this.recordAudit({
          actor: { type: "runtime" },
          action: "leases.expired_released",
          payload: { tasks: updatedTasks, resourceLeases: expiredResourceLeases },
          createdAt: now,
        });
      }
      return updatedTasks + expiredResourceLeases;
    });
  }

  recordArtifact(input: ArtifactRecordInput): ArtifactRecord {
    const id = input.id ?? `artifact_${crypto.randomUUID()}`;
    const now = input.createdAt ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO artifacts(
          id, sha256, mime_type, size_bytes, storage_ref, created_by_task_id,
          created_by_tool_call_id, classification, retention_policy, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sha256) DO UPDATE SET
          mime_type=COALESCE(excluded.mime_type, artifacts.mime_type),
          classification=COALESCE(excluded.classification, artifacts.classification),
          retention_policy=COALESCE(excluded.retention_policy, artifacts.retention_policy)`,
      )
      .run(
        id,
        input.sha256,
        input.mimeType ?? null,
        input.sizeBytes,
        input.storageRef,
        input.createdByTaskId ?? null,
        input.createdByToolCallId ?? null,
        input.classification ?? null,
        input.retentionPolicy ?? "session",
        now,
      );
    const artifact = this.getArtifactBySha256(input.sha256);
    if (!artifact) {
      throw new Error(`Failed to record artifact ${input.sha256}`);
    }
    this.recordAudit({
      actor: { type: "runtime" },
      action: "artifact.created",
      objectType: "artifact",
      objectId: artifact.id,
      payload: { sha256: artifact.sha256, sizeBytes: artifact.sizeBytes },
      createdAt: now,
    });
    return artifact;
  }

  getArtifact(id: string): ArtifactRecord | undefined {
    const row = this.db.prepare("SELECT * FROM artifacts WHERE id = ?").get(id) as
      | ArtifactRow
      | undefined;
    return row ? toArtifact(row) : undefined;
  }

  getArtifactBySha256(sha256: string): ArtifactRecord | undefined {
    const row = this.db.prepare("SELECT * FROM artifacts WHERE sha256 = ?").get(sha256) as
      | ArtifactRow
      | undefined;
    return row ? toArtifact(row) : undefined;
  }

  grantArtifactAccess(input: {
    artifactId: string;
    subjectType: string;
    subjectId: string;
    permission: string;
    expiresAt?: string;
    createdAt?: string;
  }): void {
    const now = input.createdAt ?? nowIso();
    this.db
      .prepare(
        `INSERT OR REPLACE INTO artifact_access(
          artifact_id, subject_type, subject_id, permission, expires_at, created_at
        ) VALUES(?, ?, ?, ?, ?, ?)`,
      )
      .run(
        input.artifactId,
        input.subjectType,
        input.subjectId,
        input.permission,
        input.expiresAt ?? null,
        now,
      );
    this.recordAudit({
      actor: { type: "runtime" },
      action: "artifact_access.granted",
      objectType: "artifact",
      objectId: input.artifactId,
      payload: {
        subjectType: input.subjectType,
        subjectId: input.subjectId,
        permission: input.permission,
      },
      createdAt: now,
    });
  }

  hasArtifactAccess(input: {
    artifactId: string;
    subjectType: string;
    subjectId: string;
    permission: string;
    now?: string;
  }): boolean {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS count FROM artifact_access
         WHERE artifact_id = ?
           AND subject_type = ?
           AND subject_id = ?
           AND permission = ?
           AND (expires_at IS NULL OR expires_at > ?)`,
      )
      .get(
        input.artifactId,
        input.subjectType,
        input.subjectId,
        input.permission,
        input.now ?? nowIso(),
      ) as CountRow;
    return Number(row.count) > 0;
  }

  grantCapability(input: GrantCapabilityInput): string {
    const id = input.id ?? `cap_${crypto.randomUUID()}`;
    const now = input.createdAt ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO capabilities(
          id, subject_type, subject_id, resource_type, resource_id, action, constraints_json, expires_at, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.subjectType,
        input.subjectId,
        input.resourceType,
        input.resourceId ?? null,
        input.action,
        jsonToText(input.constraints),
        input.expiresAt ?? null,
        now,
      );
    this.recordAudit({
      actor: input.actor ?? { type: "runtime" },
      action: "capability.granted",
      objectType: "capability",
      objectId: id,
      payload: {
        subjectType: input.subjectType,
        subjectId: input.subjectId,
        resourceType: input.resourceType,
        resourceId: input.resourceId,
        action: input.action,
      },
      createdAt: now,
    });
    return id;
  }

  revokeCapability(
    capabilityId: string,
    actor: { type: string; id?: string } = { type: "runtime" },
  ): boolean {
    const removed = changes(
      this.db.prepare("DELETE FROM capabilities WHERE id = ?").run(capabilityId),
    );
    if (removed > 0) {
      this.recordAudit({
        actor,
        action: "capability.revoked",
        objectType: "capability",
        objectId: capabilityId,
      });
    }
    return removed > 0;
  }

  checkCapability(input: CapabilityCheckInput): boolean {
    const now = input.now ?? nowIso();
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS count FROM capabilities
         WHERE subject_type = ?
           AND subject_id = ?
           AND resource_type = ?
           AND (resource_id IS NULL OR resource_id = ?)
           AND action = ?
           AND (expires_at IS NULL OR expires_at > ?)`,
      )
      .get(
        input.subjectType,
        input.subjectId,
        input.resourceType,
        input.resourceId ?? null,
        input.action,
        now,
      ) as CountRow;
    const allowed = Number(row.count) > 0;
    if (!allowed && input.auditDenied !== false) {
      this.recordAudit({
        actor: input.actor ?? { type: input.subjectType, id: input.subjectId },
        action: "capability.denied",
        objectType: input.resourceType,
        objectId: input.resourceId,
        payload: { action: input.action, context: input.context },
        createdAt: now,
      });
    }
    return allowed;
  }

  ensureBrowserPool(input: {
    id?: string;
    trustZoneId: string;
    browserKind?: string;
    maxContexts?: number;
    cdpEndpoint?: string;
    now?: string;
  }): string {
    const id = input.id ?? `browser_pool:${input.trustZoneId}:${input.browserKind ?? "chromium"}`;
    const now = input.now ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO browser_pools(
          id, trust_zone_id, browser_kind, status, max_contexts, cdp_endpoint, created_at, updated_at
        ) VALUES(?, ?, ?, 'active', ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          status = 'active',
          max_contexts = excluded.max_contexts,
          cdp_endpoint = excluded.cdp_endpoint,
          updated_at = excluded.updated_at`,
      )
      .run(
        id,
        input.trustZoneId,
        input.browserKind ?? "chromium",
        input.maxContexts ?? 8,
        input.cdpEndpoint ?? null,
        now,
        now,
      );
    return id;
  }

  countActiveBrowserContexts(poolId: string): number {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS count FROM browser_contexts
         WHERE pool_id = ? AND status = 'active'`,
      )
      .get(poolId) as CountRow;
    return Number(row.count);
  }

  getBrowserPoolMaxContexts(poolId: string): number {
    const row = this.db
      .prepare("SELECT max_contexts AS count FROM browser_pools WHERE id = ?")
      .get(poolId) as CountRow | undefined;
    return row ? Number(row.count) : 0;
  }

  createBrowserContext(input: {
    poolId: string;
    agentId?: string;
    sessionId?: string;
    taskId?: string;
    profileMode?: string;
    allowedOrigins?: unknown;
    expiresAt?: string;
    now?: string;
  }): BrowserContextRecord {
    const id = `browser_context_${crypto.randomUUID()}`;
    const now = input.now ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO browser_contexts(
          id, pool_id, agent_id, session_id, task_id, profile_mode, allowed_origins_json, status, created_at, expires_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)`,
      )
      .run(
        id,
        input.poolId,
        input.agentId ?? null,
        input.sessionId ?? null,
        input.taskId ?? null,
        input.profileMode ?? "ephemeral",
        jsonToText(input.allowedOrigins),
        now,
        input.expiresAt ?? null,
      );
    const context = this.getBrowserContext(id);
    if (!context) {
      throw new Error(`Failed to create browser context ${id}`);
    }
    return context;
  }

  getBrowserContext(id: string): BrowserContextRecord | undefined {
    const row = this.db.prepare("SELECT * FROM browser_contexts WHERE id = ?").get(id) as
      | BrowserContextRow
      | undefined;
    return row ? toBrowserContext(row) : undefined;
  }

  releaseBrowserContext(id: string, now = nowIso()): boolean {
    const updated = changes(
      this.db
        .prepare(
          "UPDATE browser_contexts SET status = 'released' WHERE id = ? AND status = 'active'",
        )
        .run(id),
    );
    if (updated > 0) {
      this.recordAudit({
        actor: { type: "runtime" },
        action: "browser_context.released",
        objectType: "browser_context",
        objectId: id,
        createdAt: now,
      });
    }
    return updated > 0;
  }

  createResourceLease(input: {
    id?: string;
    resourceType: string;
    resourceId: string;
    ownerTaskId?: string;
    ownerAgentId?: string;
    trustZoneId?: string;
    leaseUntil?: string;
    maxRuntimeMs?: number;
    maxBytesOut?: number;
    maxTokens?: number;
    now?: string;
  }): string {
    const id = input.id ?? `lease_${crypto.randomUUID()}`;
    const now = input.now ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO resource_leases(
          id, resource_type, resource_id, owner_task_id, owner_agent_id, trust_zone_id,
          status, lease_until, max_runtime_ms, max_bytes_out, max_tokens, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.resourceType,
        input.resourceId,
        input.ownerTaskId ?? null,
        input.ownerAgentId ?? null,
        input.trustZoneId ?? null,
        input.leaseUntil ?? null,
        input.maxRuntimeMs ?? null,
        input.maxBytesOut ?? null,
        input.maxTokens ?? null,
        now,
        now,
      );
    this.recordAudit({
      actor: { type: "runtime" },
      action: "resource_lease.created",
      objectType: "resource_lease",
      objectId: id,
      payload: { resourceType: input.resourceType, resourceId: input.resourceId },
      createdAt: now,
    });
    return id;
  }

  releaseResourceLease(id: string, now = nowIso()): boolean {
    const updated = changes(
      this.db
        .prepare(
          "UPDATE resource_leases SET status = 'released', updated_at = ? WHERE id = ? AND status = 'active'",
        )
        .run(now, id),
    );
    if (updated > 0) {
      this.recordAudit({
        actor: { type: "runtime" },
        action: "resource_lease.released",
        objectType: "resource_lease",
        objectId: id,
        createdAt: now,
      });
    }
    return updated > 0;
  }

  releaseResourceLeasesByResource(params: {
    resourceType: string;
    resourceId: string;
    now?: string;
  }): number {
    const now = params.now ?? nowIso();
    const updated = changes(
      this.db
        .prepare(
          `UPDATE resource_leases
           SET status = 'released', updated_at = ?
           WHERE resource_type = ? AND resource_id = ? AND status = 'active'`,
        )
        .run(now, params.resourceType, params.resourceId),
    );
    if (updated > 0) {
      this.recordAudit({
        actor: { type: "runtime" },
        action: "resource_lease.released_by_resource",
        objectType: params.resourceType,
        objectId: params.resourceId,
        payload: { count: updated },
        createdAt: now,
      });
    }
    return updated;
  }

  insertToolCall(input: {
    id?: string;
    taskId?: string;
    sessionId?: string;
    agentId?: string;
    toolName: string;
    input?: unknown;
    now?: string;
  }): string {
    const id = input.id ?? `tool_${crypto.randomUUID()}`;
    const now = input.now ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO tool_calls(
          id, task_id, session_id, agent_id, tool_name, status, input_json, created_at
        ) VALUES(?, ?, ?, ?, ?, 'created', ?, ?)`,
      )
      .run(
        id,
        input.taskId ?? null,
        input.sessionId ?? null,
        input.agentId ?? null,
        input.toolName,
        jsonToText(input.input),
        now,
      );
    this.recordAudit({
      actor: input.agentId ? { type: "agent", id: input.agentId } : { type: "runtime" },
      action: "tool_call.created",
      objectType: "tool_call",
      objectId: id,
      payload: { toolName: input.toolName, taskId: input.taskId, sessionId: input.sessionId },
      createdAt: now,
    });
    return id;
  }

  startToolCall(id: string, now = nowIso()): void {
    this.db
      .prepare("UPDATE tool_calls SET status = 'running', started_at = ? WHERE id = ?")
      .run(now, id);
    this.recordAudit({
      actor: { type: "runtime" },
      action: "tool_call.started",
      objectType: "tool_call",
      objectId: id,
      createdAt: now,
    });
  }

  finishToolCall(id: string, output: unknown, now = nowIso()): void {
    this.db
      .prepare(
        "UPDATE tool_calls SET status = 'succeeded', output_json = ?, ended_at = ? WHERE id = ?",
      )
      .run(jsonToText(output), now, id);
    this.recordAudit({
      actor: { type: "runtime" },
      action: "tool_call.completed",
      objectType: "tool_call",
      objectId: id,
      createdAt: now,
    });
  }

  failToolCall(id: string, error: unknown, now = nowIso()): void {
    const message = error instanceof Error ? error.message : String(error);
    this.db
      .prepare("UPDATE tool_calls SET status = 'failed', error = ?, ended_at = ? WHERE id = ?")
      .run(message, now, id);
    this.recordAudit({
      actor: { type: "runtime" },
      action: "tool_call.failed",
      objectType: "tool_call",
      objectId: id,
      payload: { error: message },
      createdAt: now,
    });
  }

  pruneArtifacts(params: { olderThan: string; retentionPolicies?: string[] }): ArtifactRecord[] {
    const policies = params.retentionPolicies ?? ["ephemeral", "debug"];
    const placeholders = policies.map(() => "?").join(", ");
    const rows = this.db
      .prepare(
        `SELECT * FROM artifacts
         WHERE created_at < ? AND retention_policy IN (${placeholders})`,
      )
      .all(params.olderThan, ...(policies as SQLInputValue[])) as ArtifactRow[];
    const artifacts = rows.map(toArtifact);
    this.withTransaction(() => {
      for (const artifact of artifacts) {
        this.db.prepare("DELETE FROM artifact_access WHERE artifact_id = ?").run(artifact.id);
        this.db.prepare("DELETE FROM artifacts WHERE id = ?").run(artifact.id);
        this.recordAudit({
          actor: { type: "runtime" },
          action: "artifact.pruned",
          objectType: "artifact",
          objectId: artifact.id,
          payload: { sha256: artifact.sha256, retentionPolicy: artifact.retentionPolicy },
        });
      }
    });
    return artifacts;
  }

  private recordTaskEvent(
    taskId: string,
    eventType: string,
    payload: unknown,
    createdAt: string,
  ): void {
    this.db
      .prepare(
        "INSERT INTO task_events(task_id, event_type, payload_json, created_at) VALUES(?, ?, ?, ?)",
      )
      .run(taskId, eventType, jsonToText(payload), createdAt);
  }
}

export function openLocalKernelDatabase(
  options: OpenLocalKernelDatabaseOptions = {},
): LocalKernelDatabase {
  return new LocalKernelDatabase(options);
}

export { LOCAL_KERNEL_SCHEMA_VERSION };
