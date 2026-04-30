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
  ArtifactAccessRecord,
  ArtifactRetentionSummaryRecord,
  BrowserContextRecord,
  BrowserObservationInput,
  BrowserObservationRecord,
  BrowserPoolRecord,
  BrowserTargetInput,
  BrowserTargetRecord,
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
  NetworkPolicyInput,
  NetworkPolicyRecord,
  ResourceLeaseRecord,
  RuntimeRetentionPolicy,
  RuntimeInfoRecord,
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

type ArtifactAccessRow = {
  artifact_id: string;
  subject_type: string;
  subject_id: string;
  permission: string;
  expires_at: string | null;
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

type BrowserPoolRow = {
  id: string;
  trust_zone_id: string;
  browser_kind: string;
  status: string;
  max_contexts: number | bigint;
  active_contexts?: number | bigint | null;
  cdp_endpoint: string | null;
  created_at: string;
  updated_at: string;
};

type BrowserTargetRow = {
  id: string;
  context_id: string;
  target_id: string;
  opener_target_id: string | null;
  url: string | null;
  title: string | null;
  status: string;
  close_reason: string | null;
  console_count: number | bigint;
  network_count: number | bigint;
  artifact_count: number | bigint;
  created_at: string;
  updated_at: string;
  closed_at: string | null;
};

type BrowserObservationRow = {
  id: number | bigint;
  context_id: string;
  target_id: string | null;
  observation_type: string;
  payload_json: string | null;
  created_at: string;
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

type ResourceLeaseRow = {
  id: string;
  resource_type: string;
  resource_id: string;
  owner_task_id: string | null;
  owner_agent_id: string | null;
  trust_zone_id: string | null;
  status: string;
  lease_until: string | null;
  max_runtime_ms: number | bigint | null;
  max_bytes_out: number | bigint | null;
  max_tokens: number | bigint | null;
  metadata_json: string | null;
  created_at: string;
  updated_at: string;
};

type ArtifactRetentionSummaryRow = {
  retention_policy: string | null;
  count: number | bigint;
  size_bytes: number | bigint | null;
};

type RuntimeInfoRow = {
  key: string;
  value: string;
  updated_at: string;
};

type TaskBudgetKind =
  | "active_agent_steps"
  | "model_calls_in_flight"
  | "file_patch_jobs"
  | "test_jobs";

class ResourceLeaseBudgetError extends Error {
  constructor(
    message: string,
    readonly payload: Record<string, unknown>,
  ) {
    super(message);
    this.name = "ResourceLeaseBudgetError";
  }
}

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

function readIntegerText(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value?.trim() ?? "", 10);
  return Number.isFinite(parsed) ? Math.max(0, Math.trunc(parsed)) : fallback;
}

function changes(result: { changes?: number | bigint }): number {
  return numberFromSql(result.changes) ?? 0;
}

function taskBudgetKind(kind: string): TaskBudgetKind {
  const normalized = kind.trim().toLowerCase();
  if (
    normalized.includes("model") ||
    normalized.includes("llm") ||
    normalized.includes("completion")
  ) {
    return "model_calls_in_flight";
  }
  if (
    normalized.includes("file_patch") ||
    normalized.includes("patch") ||
    normalized.includes("write_file")
  ) {
    return "file_patch_jobs";
  }
  if (
    normalized.includes("test") ||
    normalized.includes("vitest") ||
    normalized.includes("check")
  ) {
    return "test_jobs";
  }
  return "active_agent_steps";
}

function taskBudgetRuntimeInfoKey(kind: TaskBudgetKind): string {
  return `resource_budget.${kind}_max`;
}

function taskBudgetDefault(kind: TaskBudgetKind): number {
  switch (kind) {
    case "active_agent_steps":
      return 100;
    case "model_calls_in_flight":
      return 50;
    case "file_patch_jobs":
      return 16;
    case "test_jobs":
      return 4;
  }
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

function toArtifactAccess(row: ArtifactAccessRow): ArtifactAccessRecord {
  return {
    artifactId: row.artifact_id,
    subjectType: row.subject_type,
    subjectId: row.subject_id,
    permission: row.permission,
    ...(optionalText(row.expires_at) ? { expiresAt: row.expires_at! } : {}),
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

function toBrowserPool(row: BrowserPoolRow): BrowserPoolRecord {
  return {
    id: row.id,
    trustZoneId: row.trust_zone_id,
    browserKind: row.browser_kind,
    status: row.status,
    maxContexts: numberFromSql(row.max_contexts) ?? 0,
    activeContexts: numberFromSql(row.active_contexts) ?? 0,
    ...(optionalText(row.cdp_endpoint) ? { cdpEndpoint: row.cdp_endpoint! } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function toBrowserTarget(row: BrowserTargetRow): BrowserTargetRecord {
  return {
    id: row.id,
    contextId: row.context_id,
    targetId: row.target_id,
    ...(optionalText(row.opener_target_id) ? { openerTargetId: row.opener_target_id! } : {}),
    ...(optionalText(row.url) ? { url: row.url! } : {}),
    ...(optionalText(row.title) ? { title: row.title! } : {}),
    status: row.status,
    ...(optionalText(row.close_reason) ? { closeReason: row.close_reason! } : {}),
    consoleCount: numberFromSql(row.console_count) ?? 0,
    networkCount: numberFromSql(row.network_count) ?? 0,
    artifactCount: numberFromSql(row.artifact_count) ?? 0,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...(optionalText(row.closed_at) ? { closedAt: row.closed_at! } : {}),
  };
}

function toBrowserObservation(row: BrowserObservationRow): BrowserObservationRecord {
  return {
    id: numberFromSql(row.id) ?? 0,
    contextId: row.context_id,
    ...(optionalText(row.target_id) ? { targetId: row.target_id! } : {}),
    observationType: row.observation_type,
    ...(row.payload_json ? { payload: parseJsonText(row.payload_json) } : {}),
    createdAt: row.created_at,
  };
}

function toResourceLease(row: ResourceLeaseRow): ResourceLeaseRecord {
  return {
    id: row.id,
    resourceType: row.resource_type,
    resourceId: row.resource_id,
    ...(optionalText(row.owner_task_id) ? { ownerTaskId: row.owner_task_id! } : {}),
    ...(optionalText(row.owner_agent_id) ? { ownerAgentId: row.owner_agent_id! } : {}),
    ...(optionalText(row.trust_zone_id) ? { trustZoneId: row.trust_zone_id! } : {}),
    status: row.status,
    ...(optionalText(row.lease_until) ? { leaseUntil: row.lease_until! } : {}),
    ...(numberFromSql(row.max_runtime_ms) !== undefined
      ? { maxRuntimeMs: numberFromSql(row.max_runtime_ms) }
      : {}),
    ...(numberFromSql(row.max_bytes_out) !== undefined
      ? { maxBytesOut: numberFromSql(row.max_bytes_out) }
      : {}),
    ...(numberFromSql(row.max_tokens) !== undefined
      ? { maxTokens: numberFromSql(row.max_tokens) }
      : {}),
    ...(row.metadata_json ? { metadata: parseJsonText(row.metadata_json) } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
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
  private transactionDepth = 0;
  private savepointCounter = 0;

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
      "runtime_info",
      "agents",
      "sessions",
      "transcript_events",
      "tasks",
      "tool_calls",
      "resource_leases",
      "browser_pools",
      "browser_contexts",
      "browser_targets",
      "browser_observations",
      "artifacts",
      "artifact_access",
      "capabilities",
      "audit_log",
      "usage_records",
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

  setRuntimeInfo(key: string, value: string, updatedAt = nowIso()): RuntimeInfoRecord {
    this.db
      .prepare(
        `INSERT INTO runtime_info(key, value, updated_at)
         VALUES(?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           value=excluded.value,
           updated_at=excluded.updated_at`,
      )
      .run(key, value, updatedAt);
    return { key, value, updatedAt };
  }

  getRuntimeInfo(key: string): RuntimeInfoRecord | undefined {
    const row = this.db.prepare("SELECT * FROM runtime_info WHERE key = ?").get(key) as
      | RuntimeInfoRow
      | undefined;
    return row ? { key: row.key, value: row.value, updatedAt: row.updated_at } : undefined;
  }

  listRuntimeInfo(prefix?: string): RuntimeInfoRecord[] {
    const rows = prefix
      ? (this.db
          .prepare("SELECT * FROM runtime_info WHERE key LIKE ? ORDER BY key ASC")
          .all(`${prefix}%`) as RuntimeInfoRow[])
      : (this.db.prepare("SELECT * FROM runtime_info ORDER BY key ASC").all() as RuntimeInfoRow[]);
    return rows.map((row) => ({ key: row.key, value: row.value, updatedAt: row.updated_at }));
  }

  getResourceBudgetSnapshot(): Record<string, number> {
    return {
      logicalAgentsMax: this.readRuntimeInfoInteger("resource_budget.logical_agents_max", 500),
      activeAgentStepsMax: this.readRuntimeInfoInteger(
        "resource_budget.active_agent_steps_max",
        100,
      ),
      modelCallsInFlightMax: this.readRuntimeInfoInteger(
        "resource_budget.model_calls_in_flight_max",
        50,
      ),
      filePatchJobsMax: this.readRuntimeInfoInteger("resource_budget.file_patch_jobs_max", 16),
      testJobsMax: this.readRuntimeInfoInteger("resource_budget.test_jobs_max", 4),
      browserContextsMax: this.readRuntimeInfoInteger("resource_budget.browser_contexts_max", 2),
      heavySandboxesMax: this.readRuntimeInfoInteger("resource_budget.heavy_sandboxes_max", 1),
    };
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

  getTrustZone(id: string): TrustZoneRecord | undefined {
    const row = this.db.prepare("SELECT * FROM trust_zones WHERE id = ?").get(id) as
      | TrustZoneRow
      | undefined;
    return row ? toTrustZone(row) : undefined;
  }

  upsertNetworkPolicy(input: NetworkPolicyInput): NetworkPolicyRecord {
    const now = input.createdAt ?? nowIso();
    this.db
      .prepare(
        `INSERT INTO network_policies(
          id, default_action, allow_private_network, allowed_hosts_json, denied_cidrs_json, proxy_ref, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          default_action=excluded.default_action,
          allow_private_network=excluded.allow_private_network,
          allowed_hosts_json=excluded.allowed_hosts_json,
          denied_cidrs_json=excluded.denied_cidrs_json,
          proxy_ref=excluded.proxy_ref`,
      )
      .run(
        input.id,
        input.defaultAction,
        input.allowPrivateNetwork ? 1 : 0,
        jsonToText(input.allowedHosts ?? []),
        jsonToText(input.deniedCidrs),
        input.proxyRef ?? null,
        now,
      );
    this.recordAudit({
      actor: input.actor ?? { type: "runtime" },
      action: "network_policy.upserted",
      objectType: "network_policy",
      objectId: input.id,
      payload: {
        defaultAction: input.defaultAction,
        allowPrivateNetwork: input.allowPrivateNetwork ?? false,
        allowedHosts: input.allowedHosts ?? [],
        deniedCidrs: input.deniedCidrs,
        proxyRef: input.proxyRef,
      },
      createdAt: now,
    });
    const row = this.db.prepare("SELECT * FROM network_policies WHERE id = ?").get(input.id) as
      | NetworkPolicyRow
      | undefined;
    if (!row) {
      throw new Error(`Failed to upsert network policy ${input.id}`);
    }
    return toNetworkPolicy(row);
  }

  attachNetworkPolicyProxyToTrustZone(input: {
    trustZoneId: string;
    proxyRef?: string | null;
    actor?: KernelActor;
    createdAt?: string;
  }): { trustZone: TrustZoneRecord; networkPolicy: NetworkPolicyRecord } {
    const now = input.createdAt ?? nowIso();
    return this.withTransaction(() => {
      const zoneRow = this.db
        .prepare("SELECT * FROM trust_zones WHERE id = ?")
        .get(input.trustZoneId) as TrustZoneRow | undefined;
      if (!zoneRow) {
        throw new Error(`Unknown trust zone: ${input.trustZoneId}`);
      }
      const policyId = optionalText(zoneRow.network_policy_id) ?? `${input.trustZoneId}_policy`;
      if (!optionalText(zoneRow.network_policy_id)) {
        this.db
          .prepare("UPDATE trust_zones SET network_policy_id = ? WHERE id = ?")
          .run(policyId, input.trustZoneId);
      }
      this.db
        .prepare(
          `INSERT INTO network_policies(
            id, default_action, allow_private_network, allowed_hosts_json, denied_cidrs_json, proxy_ref, created_at
          ) VALUES(?, 'deny', 0, ?, NULL, ?, ?)
          ON CONFLICT(id) DO UPDATE SET
            proxy_ref=excluded.proxy_ref`,
        )
        .run(policyId, jsonToText([]), input.proxyRef ?? null, now);
      this.recordAudit({
        actor: input.actor ?? { type: "operator" },
        action: input.proxyRef
          ? "network_policy.proxy_ref_attached"
          : "network_policy.proxy_ref_cleared",
        objectType: "trust_zone",
        objectId: input.trustZoneId,
        payload: { networkPolicyId: policyId, proxyRef: input.proxyRef ?? null },
        createdAt: now,
      });
      const trustZone = this.getTrustZone(input.trustZoneId);
      const networkPolicy = this.getNetworkPolicyForTrustZone(input.trustZoneId);
      if (!trustZone || !networkPolicy) {
        throw new Error(`Failed to attach network policy proxy for ${input.trustZoneId}`);
      }
      return { trustZone, networkPolicy };
    });
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
    if (this.transactionDepth > 0) {
      const savepoint = `local_kernel_sp_${++this.savepointCounter}`;
      this.db.exec(`SAVEPOINT ${savepoint}`);
      this.transactionDepth += 1;
      try {
        const result = fn();
        this.db.exec(`RELEASE SAVEPOINT ${savepoint}`);
        return result;
      } catch (error) {
        try {
          this.db.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`);
          this.db.exec(`RELEASE SAVEPOINT ${savepoint}`);
        } catch {}
        throw error;
      } finally {
        this.transactionDepth -= 1;
      }
    }
    this.db.exec("BEGIN IMMEDIATE");
    this.transactionDepth += 1;
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      throw error;
    } finally {
      this.transactionDepth -= 1;
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
      const budgetDenial = this.resolveTaskClaimBudgetDenial({
        kind: row.kind,
        workerId: input.workerId,
      });
      if (budgetDenial) {
        this.recordAudit({
          actor: { type: "worker", id: input.workerId },
          action: "task.claim_denied_resource_budget",
          objectType: "task",
          objectId: row.id,
          payload: budgetDenial,
          createdAt: now,
        });
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
      const row = this.db
        .prepare("SELECT id, kind, status FROM tasks WHERE id = ?")
        .get(input.taskId) as Pick<TaskRow, "id" | "kind" | "status"> | undefined;
      if (!row || row.status !== "queued") {
        return null;
      }
      const budgetDenial = this.resolveTaskClaimBudgetDenial({
        kind: row.kind,
        workerId: input.workerId,
      });
      if (budgetDenial) {
        this.recordAudit({
          actor: { type: "worker", id: input.workerId },
          action: "task.claim_denied_resource_budget",
          objectType: "task",
          objectId: input.taskId,
          payload: budgetDenial,
          createdAt: now,
        });
        return null;
      }
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

  private resolveTaskClaimBudgetDenial(input: {
    kind: string;
    workerId: string;
  }): Record<string, unknown> | undefined {
    const globalLimit = this.readRuntimeInfoInteger(
      taskBudgetRuntimeInfoKey("active_agent_steps"),
      taskBudgetDefault("active_agent_steps"),
    );
    const globalActive = this.countRunningTasksForBudget("active_agent_steps");
    if (globalActive >= globalLimit) {
      return {
        kind: input.kind,
        workerId: input.workerId,
        budget: "active_agent_steps",
        active: globalActive,
        limit: globalLimit,
      };
    }
    const budgetKind = taskBudgetKind(input.kind);
    if (budgetKind === "active_agent_steps") {
      return undefined;
    }
    const limit = this.readRuntimeInfoInteger(
      taskBudgetRuntimeInfoKey(budgetKind),
      taskBudgetDefault(budgetKind),
    );
    const active = this.countRunningTasksForBudget(budgetKind);
    if (active < limit) {
      return undefined;
    }
    return {
      kind: input.kind,
      workerId: input.workerId,
      budget: budgetKind,
      active,
      limit,
    };
  }

  private countRunningTasksForBudget(kind: TaskBudgetKind): number {
    if (kind === "active_agent_steps") {
      const row = this.db
        .prepare("SELECT COUNT(*) AS count FROM tasks WHERE status = 'running'")
        .get() as CountRow;
      return Number(row.count);
    }
    const rows = this.db.prepare("SELECT kind FROM tasks WHERE status = 'running'").all() as Array<
      Pick<TaskRow, "kind">
    >;
    return rows.filter((row) => taskBudgetKind(row.kind) === kind).length;
  }

  private readRuntimeInfoInteger(key: string, fallback: number): number {
    const row = this.db.prepare("SELECT value FROM runtime_info WHERE key = ?").get(key) as
      | Pick<RuntimeInfoRow, "value">
      | undefined;
    return readIntegerText(row?.value, fallback);
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

  listArtifactAccess(
    input: {
      artifactId?: string;
      subjectType?: string;
      subjectId?: string;
      permission?: string;
      limit?: number;
    } = {},
  ): ArtifactAccessRecord[] {
    const conditions: string[] = [];
    const params: SQLInputValue[] = [];
    if (input.artifactId) {
      conditions.push("artifact_id = ?");
      params.push(input.artifactId);
    }
    if (input.subjectType) {
      conditions.push("subject_type = ?");
      params.push(input.subjectType);
    }
    if (input.subjectId) {
      conditions.push("subject_id = ?");
      params.push(input.subjectId);
    }
    if (input.permission) {
      conditions.push("permission = ?");
      params.push(input.permission);
    }
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit ?? 100)));
    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const rows = this.db
      .prepare(
        `SELECT artifact_id, subject_type, subject_id, permission, expires_at, created_at
         FROM artifact_access
         ${where}
         ORDER BY created_at DESC, artifact_id ASC
         LIMIT ?`,
      )
      .all(...params, limit) as ArtifactAccessRow[];
    return rows.map(toArtifactAccess);
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

  listBrowserPools(input: { trustZoneId?: string; status?: string } = {}): BrowserPoolRecord[] {
    const conditions: string[] = [];
    const params: SQLInputValue[] = [];
    if (input.trustZoneId) {
      conditions.push("bp.trust_zone_id = ?");
      params.push(input.trustZoneId);
    }
    if (input.status) {
      conditions.push("bp.status = ?");
      params.push(input.status);
    }
    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const rows = this.db
      .prepare(
        `SELECT bp.*,
                COALESCE(SUM(CASE WHEN bc.status = 'active' THEN 1 ELSE 0 END), 0) AS active_contexts
         FROM browser_pools bp
         LEFT JOIN browser_contexts bc ON bc.pool_id = bp.id
         ${where}
         GROUP BY bp.id
         ORDER BY bp.trust_zone_id ASC, bp.browser_kind ASC, bp.id ASC`,
      )
      .all(...params) as BrowserPoolRow[];
    return rows.map(toBrowserPool);
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
      this.db
        .prepare(
          `UPDATE browser_targets
           SET status = 'closed',
               close_reason = COALESCE(close_reason, 'context_released'),
               updated_at = ?,
               closed_at = COALESCE(closed_at, ?)
           WHERE context_id = ? AND status = 'active'`,
        )
        .run(now, now, id);
    }
    return updated > 0;
  }

  recordBrowserTarget(input: BrowserTargetInput): BrowserTargetRecord {
    const now = input.updatedAt ?? input.createdAt ?? nowIso();
    const id = `${input.contextId}:${input.targetId}`;
    this.db
      .prepare(
        `INSERT INTO browser_targets(
          id, context_id, target_id, opener_target_id, url, title, status, close_reason,
          created_at, updated_at, closed_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(context_id, target_id) DO UPDATE SET
          opener_target_id = COALESCE(excluded.opener_target_id, browser_targets.opener_target_id),
          url = COALESCE(excluded.url, browser_targets.url),
          title = COALESCE(excluded.title, browser_targets.title),
          status = excluded.status,
          close_reason = COALESCE(excluded.close_reason, browser_targets.close_reason),
          updated_at = excluded.updated_at,
          closed_at = COALESCE(excluded.closed_at, browser_targets.closed_at)`,
      )
      .run(
        id,
        input.contextId,
        input.targetId,
        input.openerTargetId ?? null,
        input.url ?? null,
        input.title ?? null,
        input.status ?? "active",
        input.closeReason ?? null,
        input.createdAt ?? now,
        now,
        input.closedAt ?? null,
      );
    this.recordAudit({
      actor: { type: "runtime" },
      action: "browser_target.recorded",
      objectType: "browser_target",
      objectId: id,
      payload: {
        contextId: input.contextId,
        targetId: input.targetId,
        status: input.status ?? "active",
        url: input.url,
      },
      createdAt: now,
    });
    const target = this.getBrowserTarget(input.contextId, input.targetId);
    if (!target) {
      throw new Error(`Failed to record browser target ${input.targetId}`);
    }
    return target;
  }

  getBrowserTarget(contextId: string, targetId: string): BrowserTargetRecord | undefined {
    const row = this.db
      .prepare("SELECT * FROM browser_targets WHERE context_id = ? AND target_id = ?")
      .get(contextId, targetId) as BrowserTargetRow | undefined;
    return row ? toBrowserTarget(row) : undefined;
  }

  closeBrowserTarget(input: {
    contextId: string;
    targetId: string;
    reason?: string;
    closedAt?: string;
  }): BrowserTargetRecord {
    const now = input.closedAt ?? nowIso();
    this.recordBrowserTarget({
      contextId: input.contextId,
      targetId: input.targetId,
      status: "closed",
      closeReason: input.reason ?? "closed",
      updatedAt: now,
      closedAt: now,
    });
    this.recordAudit({
      actor: { type: "runtime" },
      action: "browser_target.closed",
      objectType: "browser_target",
      objectId: `${input.contextId}:${input.targetId}`,
      payload: { contextId: input.contextId, targetId: input.targetId, reason: input.reason },
      createdAt: now,
    });
    const target = this.getBrowserTarget(input.contextId, input.targetId);
    if (!target) {
      throw new Error(`Failed to close browser target ${input.targetId}`);
    }
    return target;
  }

  recordBrowserObservation(input: BrowserObservationInput): BrowserObservationRecord {
    const now = input.createdAt ?? nowIso();
    if (input.targetId && !this.getBrowserTarget(input.contextId, input.targetId)) {
      this.recordBrowserTarget({
        contextId: input.contextId,
        targetId: input.targetId,
        status: "active",
        createdAt: now,
        updatedAt: now,
      });
    }
    const result = this.db
      .prepare(
        `INSERT INTO browser_observations(context_id, target_id, observation_type, payload_json, created_at)
         VALUES(?, ?, ?, ?, ?)`,
      )
      .run(
        input.contextId,
        input.targetId ?? null,
        input.observationType,
        jsonToText(input.payload),
        now,
      );
    if (input.targetId) {
      const counter =
        input.observationType === "browser_console" ||
        input.observationType.startsWith("browser_console.")
          ? "console_count"
          : input.observationType.startsWith("browser_artifact")
            ? "artifact_count"
            : input.observationType.startsWith("browser_network")
              ? "network_count"
              : undefined;
      if (counter) {
        this.db
          .prepare(
            `UPDATE browser_targets
             SET ${counter} = ${counter} + 1, updated_at = ?
             WHERE context_id = ? AND target_id = ?`,
          )
          .run(now, input.contextId, input.targetId);
      }
    }
    const id = numberFromSql(result.lastInsertRowid) ?? 0;
    this.recordAudit({
      actor: { type: "runtime" },
      action: "browser_context.observation",
      objectType: "browser_context",
      objectId: input.contextId,
      payload: {
        targetId: input.targetId,
        observationType: input.observationType,
        payload: input.payload,
      },
      createdAt: now,
    });
    const row = this.db.prepare("SELECT * FROM browser_observations WHERE id = ?").get(id) as
      | BrowserObservationRow
      | undefined;
    if (!row) {
      throw new Error(`Failed to record browser observation ${id}`);
    }
    return toBrowserObservation(row);
  }

  listBrowserTargets(
    input: {
      contextId?: string;
      sessionId?: string;
      taskId?: string;
      status?: string;
      limit?: number;
    } = {},
  ): BrowserTargetRecord[] {
    const conditions: string[] = [];
    const params: SQLInputValue[] = [];
    if (input.contextId) {
      conditions.push("bt.context_id = ?");
      params.push(input.contextId);
    }
    if (input.sessionId) {
      conditions.push("bc.session_id = ?");
      params.push(input.sessionId);
    }
    if (input.taskId) {
      conditions.push("bc.task_id = ?");
      params.push(input.taskId);
    }
    if (input.status) {
      conditions.push("bt.status = ?");
      params.push(input.status);
    }
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit ?? 100)));
    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const rows = this.db
      .prepare(
        `SELECT bt.*
         FROM browser_targets bt
         JOIN browser_contexts bc ON bc.id = bt.context_id
         ${where}
         ORDER BY bt.updated_at DESC, bt.id ASC
         LIMIT ?`,
      )
      .all(...params, limit) as BrowserTargetRow[];
    return rows.map(toBrowserTarget);
  }

  listBrowserObservations(
    input: {
      contextId?: string;
      targetId?: string;
      observationType?: string;
      since?: string;
      limit?: number;
    } = {},
  ): BrowserObservationRecord[] {
    const conditions: string[] = [];
    const params: SQLInputValue[] = [];
    if (input.contextId) {
      conditions.push("context_id = ?");
      params.push(input.contextId);
    }
    if (input.targetId) {
      conditions.push("target_id = ?");
      params.push(input.targetId);
    }
    if (input.observationType) {
      conditions.push("observation_type = ?");
      params.push(input.observationType);
    }
    if (input.since) {
      conditions.push("created_at >= ?");
      params.push(input.since);
    }
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit ?? 100)));
    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const rows = this.db
      .prepare(
        `SELECT *
         FROM browser_observations
         ${where}
         ORDER BY id DESC
         LIMIT ?`,
      )
      .all(...params, limit) as BrowserObservationRow[];
    return rows.map(toBrowserObservation);
  }

  pruneBrowserObservations(params: { olderThan: string; observationTypes?: string[] }): number {
    const types = params.observationTypes?.filter((type) => type.trim());
    const typeWhere = types?.length
      ? `AND observation_type IN (${types.map(() => "?").join(", ")})`
      : "";
    const deleted = changes(
      this.db
        .prepare(`DELETE FROM browser_observations WHERE created_at < ? ${typeWhere}`)
        .run(params.olderThan, ...((types ?? []) as SQLInputValue[])),
    );
    if (deleted > 0) {
      this.recordAudit({
        actor: { type: "runtime" },
        action: "browser_observations.pruned",
        objectType: "browser_observation",
        payload: { olderThan: params.olderThan, observationTypes: types, count: deleted },
      });
    }
    return deleted;
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
    metadata?: unknown;
    now?: string;
  }): string {
    const id = input.id ?? `lease_${crypto.randomUUID()}`;
    const now = input.now ?? nowIso();
    try {
      return this.withTransaction(() => {
        const budget = this.resolveResourceLeaseBudget(input, now);
        this.db
          .prepare(
            `INSERT INTO resource_leases(
              id, resource_type, resource_id, owner_task_id, owner_agent_id, trust_zone_id,
              status, lease_until, max_runtime_ms, max_bytes_out, max_tokens, metadata_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)`,
          )
          .run(
            id,
            input.resourceType,
            input.resourceId,
            input.ownerTaskId ?? null,
            input.ownerAgentId ?? null,
            input.trustZoneId ?? null,
            budget.leaseUntil ?? null,
            budget.maxRuntimeMs ?? null,
            input.maxBytesOut ?? null,
            input.maxTokens ?? null,
            jsonToText(input.metadata),
            now,
            now,
          );
        this.recordAudit({
          actor: { type: "runtime" },
          action: "resource_lease.created",
          objectType: "resource_lease",
          objectId: id,
          payload: {
            resourceType: input.resourceType,
            resourceId: input.resourceId,
            ...(input.trustZoneId ? { trustZoneId: input.trustZoneId } : {}),
            ...(budget.maxRuntimeMs !== input.maxRuntimeMs
              ? { maxRuntimeMs: budget.maxRuntimeMs }
              : {}),
          },
          createdAt: now,
        });
        return id;
      });
    } catch (error) {
      if (error instanceof ResourceLeaseBudgetError) {
        this.recordAudit({
          actor: { type: "runtime" },
          action: "resource_lease.denied_budget_exhausted",
          objectType: "trust_zone",
          objectId: input.trustZoneId,
          payload: error.payload,
          createdAt: now,
        });
      }
      throw error;
    }
  }

  private resolveResourceLeaseBudget(
    input: {
      resourceType: string;
      trustZoneId?: string;
      leaseUntil?: string;
      maxRuntimeMs?: number;
    },
    now: string,
  ): { leaseUntil?: string; maxRuntimeMs?: number } {
    const zone = input.trustZoneId
      ? this.listTrustZones().find((entry) => entry.id === input.trustZoneId)
      : undefined;
    if (
      zone?.maxProcesses !== undefined &&
      input.resourceType === "sandbox" &&
      zone.maxProcesses >= 0
    ) {
      const row = this.db
        .prepare(
          `SELECT COUNT(*) AS count
           FROM resource_leases
           WHERE trust_zone_id = ? AND resource_type = 'sandbox' AND status = 'active'`,
        )
        .get(zone.id) as CountRow;
      const active = Number(row.count);
      if (active >= zone.maxProcesses) {
        throw new ResourceLeaseBudgetError(
          `Trust zone ${zone.id} sandbox budget exhausted: ${active}/${zone.maxProcesses} active`,
          {
            trustZoneId: zone.id,
            resourceType: input.resourceType,
            active,
            limit: zone.maxProcesses,
          },
        );
      }
    }

    let maxRuntimeMs = input.maxRuntimeMs;
    if (zone?.maxRuntimeSeconds !== undefined) {
      const trustZoneMaxMs = Math.max(1, Math.trunc(zone.maxRuntimeSeconds * 1000));
      maxRuntimeMs =
        maxRuntimeMs === undefined
          ? trustZoneMaxMs
          : Math.min(Math.max(1, Math.trunc(maxRuntimeMs)), trustZoneMaxMs);
    } else if (maxRuntimeMs !== undefined) {
      maxRuntimeMs = Math.max(1, Math.trunc(maxRuntimeMs));
    }

    let leaseUntil = input.leaseUntil;
    if (maxRuntimeMs !== undefined) {
      const runtimeLeaseUntil = futureIso(now, maxRuntimeMs);
      if (!leaseUntil || Date.parse(leaseUntil) > Date.parse(runtimeLeaseUntil)) {
        leaseUntil = runtimeLeaseUntil;
      }
    }
    return { leaseUntil, maxRuntimeMs };
  }

  getResourceLease(id: string): ResourceLeaseRecord | undefined {
    const row = this.db.prepare("SELECT * FROM resource_leases WHERE id = ?").get(id) as
      | ResourceLeaseRow
      | undefined;
    return row ? toResourceLease(row) : undefined;
  }

  listResourceLeases(
    input: {
      resourceType?: string;
      status?: string;
      trustZoneId?: string;
      ownerAgentId?: string;
      limit?: number;
    } = {},
  ): ResourceLeaseRecord[] {
    const conditions: string[] = [];
    const params: SQLInputValue[] = [];
    if (input.resourceType) {
      conditions.push("resource_type = ?");
      params.push(input.resourceType);
    }
    if (input.status) {
      conditions.push("status = ?");
      params.push(input.status);
    }
    if (input.trustZoneId) {
      conditions.push("trust_zone_id = ?");
      params.push(input.trustZoneId);
    }
    if (input.ownerAgentId) {
      conditions.push("owner_agent_id = ?");
      params.push(input.ownerAgentId);
    }
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit ?? 100)));
    const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
    const rows = this.db
      .prepare(
        `SELECT *
         FROM resource_leases
         ${where}
         ORDER BY updated_at DESC, id ASC
         LIMIT ?`,
      )
      .all(...params, limit) as ResourceLeaseRow[];
    return rows.map(toResourceLease);
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

  summarizeArtifactRetention(): ArtifactRetentionSummaryRecord[] {
    const rows = this.db
      .prepare(
        `SELECT COALESCE(retention_policy, 'unknown') AS retention_policy,
                COUNT(*) AS count,
                COALESCE(SUM(size_bytes), 0) AS size_bytes
         FROM artifacts
         GROUP BY COALESCE(retention_policy, 'unknown')
         ORDER BY retention_policy ASC`,
      )
      .all() as ArtifactRetentionSummaryRow[];
    return rows.map((row) => ({
      retentionPolicy: (row.retention_policy ?? "unknown") as RuntimeRetentionPolicy | "unknown",
      count: numberFromSql(row.count) ?? 0,
      sizeBytes: numberFromSql(row.size_bytes) ?? 0,
    }));
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
