use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use uuid::Uuid;

pub const SPARSEKERNEL_SCHEMA_VERSION: i64 = 3;
pub const SPARSEKERNEL_PROTOCOL_VERSION: &str = "2026-04-29.v1";
const MIGRATION_0001: &str = include_str!("../../../migrations/0001_initial.sql");
const MIGRATION_0002: &str =
    include_str!("../../../migrations/0002_browser_targets_observations.sql");
const MIGRATION_0003: &str = include_str!("../../../migrations/0003_resource_lease_metadata.sql");

pub type Result<T> = std::result::Result<T, SparseKernelError>;

#[derive(Debug)]
pub enum SparseKernelError {
    Sqlite(rusqlite::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
    NotFound(String),
    Denied(String),
    Invalid(String),
}

impl fmt::Display for SparseKernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SparseKernelError::Sqlite(err) => write!(f, "sqlite error: {err}"),
            SparseKernelError::Io(err) => write!(f, "io error: {err}"),
            SparseKernelError::Json(err) => write!(f, "json error: {err}"),
            SparseKernelError::NotFound(message) => write!(f, "not found: {message}"),
            SparseKernelError::Denied(message) => write!(f, "denied: {message}"),
            SparseKernelError::Invalid(message) => write!(f, "invalid: {message}"),
        }
    }
}

impl std::error::Error for SparseKernelError {}

impl From<rusqlite::Error> for SparseKernelError {
    fn from(value: rusqlite::Error) -> Self {
        SparseKernelError::Sqlite(value)
    }
}

impl From<std::io::Error> for SparseKernelError {
    fn from(value: std::io::Error) -> Self {
        SparseKernelError::Io(value)
    }
}

impl From<serde_json::Error> for SparseKernelError {
    fn from(value: serde_json::Error) -> Self {
        SparseKernelError::Json(value)
    }
}

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn future_iso(seconds: i64) -> String {
    (Utc::now() + ChronoDuration::seconds(seconds)).to_rfc3339()
}

fn json_text(value: Option<&Value>) -> Option<String> {
    value.map(Value::to_string)
}

fn parse_json(raw: Option<String>) -> Option<Value> {
    raw.and_then(|text| serde_json::from_str(&text).ok())
}

fn truthy_env_flag(name: &str) -> bool {
    matches!(
        env::var(name).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SparseKernelPaths {
    pub home_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub db_path: PathBuf,
    pub artifact_root: PathBuf,
}

impl SparseKernelPaths {
    pub fn from_env() -> Self {
        let home_dir = env::var_os("SPARSEKERNEL_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                env::var_os("OPENCLAW_STATE_DIR")
                    .map(|state| PathBuf::from(state).join("sparsekernel"))
            })
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".sparsekernel")))
            .unwrap_or_else(|| PathBuf::from(".sparsekernel"));
        let runtime_dir = home_dir.join("runtime");
        let db_path = runtime_dir.join("sparsekernel.sqlite");
        let artifact_root = home_dir.join("artifacts");
        Self {
            home_dir,
            runtime_dir,
            db_path,
            artifact_root,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbInspect {
    pub path: PathBuf,
    pub schema_version: i64,
    pub counts: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: i64,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub action: String,
    pub object_type: Option<String>,
    pub object_id: Option<String>,
    pub payload: Option<Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditInput {
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub action: String,
    pub object_type: Option<String>,
    pub object_id: Option<String>,
    pub payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: String,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    pub kind: String,
    pub priority: i64,
    pub status: String,
    pub lease_owner: Option<String>,
    pub lease_until: Option<String>,
    pub attempts: i64,
    pub result_artifact_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRecord {
    pub id: String,
    pub agent_id: String,
    pub session_key: Option<String>,
    pub channel: Option<String>,
    pub status: String,
    pub current_token_count: i64,
    pub last_activity_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TranscriptEventRecord {
    pub id: i64,
    pub session_id: String,
    pub parent_event_id: Option<i64>,
    pub seq: i64,
    pub role: String,
    pub event_type: String,
    pub content: Option<Value>,
    pub tool_call_id: Option<String>,
    pub token_count: Option<i64>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertSessionInput {
    pub id: String,
    pub agent_id: String,
    pub session_key: Option<String>,
    pub channel: Option<String>,
    pub status: Option<String>,
    pub current_token_count: Option<i64>,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendTranscriptEventInput {
    pub session_id: String,
    pub parent_event_id: Option<i64>,
    pub role: String,
    pub event_type: String,
    pub content: Option<Value>,
    pub tool_call_id: Option<String>,
    pub token_count: Option<i64>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueTaskInput {
    pub id: Option<String>,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    pub kind: String,
    pub priority: i64,
    pub idempotency_key: Option<String>,
    pub input: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub id: String,
    pub task_id: Option<String>,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub tool_name: String,
    pub status: String,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateToolCallInput {
    pub id: Option<String>,
    pub task_id: Option<String>,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub tool_name: String,
    pub input: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteToolCallInput {
    pub output: Option<Value>,
    #[serde(default)]
    pub artifact_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub id: String,
    pub sha256: String,
    pub mime_type: Option<String>,
    pub size_bytes: i64,
    pub storage_ref: String,
    pub classification: Option<String>,
    pub retention_policy: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityRecord {
    pub id: String,
    pub subject_type: String,
    pub subject_id: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub action: String,
    pub constraints: Option<Value>,
    pub expires_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantCapabilityInput {
    pub subject_type: String,
    pub subject_id: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub action: String,
    pub constraints: Option<Value>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityCheck {
    pub subject_type: String,
    pub subject_id: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub action: String,
    pub context: Option<Value>,
    pub audit_denied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserContextRecord {
    pub id: String,
    pub pool_id: String,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub profile_mode: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserPoolRecord {
    pub id: String,
    pub trust_zone_id: String,
    pub browser_kind: String,
    pub status: String,
    pub max_contexts: i64,
    pub cdp_endpoint: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserTargetRecord {
    pub id: String,
    pub context_id: String,
    pub target_id: String,
    pub opener_target_id: Option<String>,
    pub url: Option<String>,
    pub title: Option<String>,
    pub status: String,
    pub close_reason: Option<String>,
    pub console_count: i64,
    pub network_count: i64,
    pub artifact_count: i64,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserObservationRecord {
    pub id: i64,
    pub context_id: String,
    pub target_id: Option<String>,
    pub observation_type: String,
    pub payload: Option<Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordBrowserTargetInput {
    pub context_id: String,
    pub target_id: String,
    pub opener_target_id: Option<String>,
    pub url: Option<String>,
    pub title: Option<String>,
    pub status: Option<String>,
    pub close_reason: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub closed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordBrowserObservationInput {
    pub context_id: String,
    pub target_id: Option<String>,
    pub observation_type: String,
    pub payload: Option<Value>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListBrowserTargetsInput {
    pub context_id: Option<String>,
    pub session_id: Option<String>,
    pub task_id: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListBrowserObservationsInput {
    pub context_id: Option<String>,
    pub target_id: Option<String>,
    pub observation_type: Option<String>,
    pub since: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrowserEndpointProbe {
    pub endpoint: String,
    pub reachable: bool,
    pub status_code: Option<u16>,
    pub browser: Option<String>,
    pub web_socket_debugger_url: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxAllocationRecord {
    pub id: String,
    pub task_id: Option<String>,
    pub trust_zone_id: String,
    pub backend: String,
    pub status: String,
    pub created_at: String,
}

pub struct SparseKernelDb {
    conn: Connection,
    path: PathBuf,
}

impl SparseKernelDb {
    pub fn open_default() -> Result<Self> {
        Self::open(SparseKernelPaths::from_env().db_path)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&path)?;
        conn.busy_timeout(Duration::from_millis(5_000))?;
        conn.execute_batch(
            "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;",
        )?;
        let db = Self { conn, path };
        db.migrate()?;
        Ok(db)
    }

    pub fn migrate(&self) -> Result<()> {
        let current = self.schema_version()?;
        self.conn.execute_batch("BEGIN IMMEDIATE;")?;
        let result = (|| -> Result<()> {
            if current < 1 {
                self.conn.execute_batch(MIGRATION_0001)?;
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(?, ?)",
                    params![1, now_iso()],
                )?;
            }
            if current < 2 {
                self.conn.execute_batch(MIGRATION_0002)?;
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(?, ?)",
                    params![2, now_iso()],
                )?;
            }
            if current < 3 {
                self.conn.execute_batch(MIGRATION_0003)?;
                self.conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(?, ?)",
                    params![3, now_iso()],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT;")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK;");
                Err(err)
            }
        }
    }

    pub fn schema_version(&self) -> Result<i64> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Ok(0);
        }
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn inspect(&self) -> Result<DbInspect> {
        let tables = [
            "agents",
            "sessions",
            "transcript_events",
            "messages",
            "tasks",
            "task_events",
            "tool_calls",
            "trust_zones",
            "network_policies",
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
        let mut counts = BTreeMap::new();
        for table in tables {
            let count =
                self.conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })?;
            counts.insert(table.to_string(), count);
        }
        Ok(DbInspect {
            path: self.path.clone(),
            schema_version: self.schema_version()?,
            counts,
        })
    }

    pub fn record_audit(&self, input: AuditInput) -> Result<()> {
        self.conn.execute(
            "INSERT INTO audit_log(actor_type, actor_id, action, object_type, object_id, payload_json, created_at)
             VALUES(?, ?, ?, ?, ?, ?, ?)",
            params![
                input.actor_type,
                input.actor_id,
                input.action,
                input.object_type,
                input.object_id,
                json_text(input.payload.as_ref()),
                now_iso(),
            ],
        )?;
        Ok(())
    }

    pub fn list_audit(&self, limit: i64) -> Result<Vec<AuditEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, actor_type, actor_id, action, object_type, object_id, payload_json, created_at
             FROM audit_log ORDER BY id DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![limit.max(0)], |row| {
            Ok(AuditEvent {
                id: row.get(0)?,
                actor_type: row.get(1)?,
                actor_id: row.get(2)?,
                action: row.get(3)?,
                object_type: row.get(4)?,
                object_id: row.get(5)?,
                payload: parse_json(row.get(6)?),
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn ensure_agent(&self, id: &str) -> Result<()> {
        let now = now_iso();
        self.conn.execute(
            "INSERT INTO agents(id, status, created_at, updated_at)
             VALUES(?, 'active', ?, ?)
             ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at",
            params![id, now, now],
        )?;
        Ok(())
    }

    pub fn upsert_session(&self, input: UpsertSessionInput) -> Result<SessionRecord> {
        let id = input.id.trim();
        if id.is_empty() {
            return Err(SparseKernelError::Invalid(
                "session id is required".to_string(),
            ));
        }
        let agent_id = input.agent_id.trim();
        if agent_id.is_empty() {
            return Err(SparseKernelError::Invalid(
                "agent_id is required".to_string(),
            ));
        }
        self.ensure_agent(agent_id)?;
        let now = now_iso();
        self.conn.execute(
            "INSERT INTO sessions(
                id, agent_id, session_key, channel, status, current_token_count, last_activity_at, created_at, updated_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                agent_id = excluded.agent_id,
                session_key = excluded.session_key,
                channel = excluded.channel,
                status = excluded.status,
                current_token_count = excluded.current_token_count,
                last_activity_at = excluded.last_activity_at,
                updated_at = excluded.updated_at",
            params![
                id,
                agent_id,
                input.session_key,
                input.channel,
                input.status.unwrap_or_else(|| "active".to_string()),
                input.current_token_count.unwrap_or(0),
                input.last_activity_at,
                now,
                now,
            ],
        )?;
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "session.upserted".to_string(),
            object_type: Some("session".to_string()),
            object_id: Some(id.to_string()),
            payload: Some(json!({ "agentId": agent_id })),
        })?;
        self.get_session(id)
    }

    pub fn get_session(&self, id: &str) -> Result<SessionRecord> {
        self.conn
            .query_row(
                "SELECT id, agent_id, session_key, channel, status, current_token_count, last_activity_at, created_at, updated_at
                 FROM sessions WHERE id = ?",
                params![id],
                session_from_row,
            )
            .optional()?
            .ok_or_else(|| SparseKernelError::NotFound(format!("session {id}")))
    }

    pub fn list_sessions(&self, limit: i64) -> Result<Vec<SessionRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, agent_id, session_key, channel, status, current_token_count, last_activity_at, created_at, updated_at
             FROM sessions ORDER BY updated_at DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![limit.max(0)], session_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn append_transcript_event(
        &mut self,
        input: AppendTranscriptEventInput,
    ) -> Result<TranscriptEventRecord> {
        let session_id = input.session_id.trim();
        if session_id.is_empty() {
            return Err(SparseKernelError::Invalid(
                "session_id is required".to_string(),
            ));
        }
        if input.role.trim().is_empty() {
            return Err(SparseKernelError::Invalid("role is required".to_string()));
        }
        if input.event_type.trim().is_empty() {
            return Err(SparseKernelError::Invalid(
                "event_type is required".to_string(),
            ));
        }
        self.get_session(session_id)?;
        let now = input.created_at.unwrap_or_else(now_iso);
        let tx = self.conn.transaction()?;
        let seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM transcript_events WHERE session_id = ?",
            params![session_id],
            |row| row.get(0),
        )?;
        tx.execute(
            "INSERT INTO transcript_events(
                session_id, parent_event_id, seq, role, event_type, content_json, tool_call_id, token_count, created_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                session_id,
                input.parent_event_id,
                seq,
                input.role.trim(),
                input.event_type.trim(),
                json_text(input.content.as_ref()),
                input.tool_call_id,
                input.token_count,
                now,
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO audit_log(actor_type, actor_id, action, object_type, object_id, payload_json, created_at)
             VALUES('runtime', NULL, 'transcript_event.appended', 'transcript_event', ?, ?, ?)",
            params![
                id.to_string(),
                json!({
                    "sessionId": session_id,
                    "seq": seq,
                    "role": input.role.trim(),
                    "eventType": input.event_type.trim()
                })
                .to_string(),
                now,
            ],
        )?;
        tx.commit()?;
        self.get_transcript_event(id)
    }

    pub fn get_transcript_event(&self, id: i64) -> Result<TranscriptEventRecord> {
        self.conn
            .query_row(
                "SELECT id, session_id, parent_event_id, seq, role, event_type, content_json, tool_call_id, token_count, created_at
                 FROM transcript_events WHERE id = ?",
                params![id],
                transcript_event_from_row,
            )
            .optional()?
            .ok_or_else(|| SparseKernelError::NotFound(format!("transcript event {id}")))
    }

    pub fn list_transcript_events(
        &self,
        session_id: &str,
        limit: i64,
    ) -> Result<Vec<TranscriptEventRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, parent_event_id, seq, role, event_type, content_json, tool_call_id, token_count, created_at
             FROM transcript_events WHERE session_id = ? ORDER BY seq ASC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![session_id, limit.max(0)], transcript_event_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn enqueue_task(&self, input: EnqueueTaskInput) -> Result<TaskRecord> {
        let id = input.id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let now = now_iso();
        self.conn.execute(
            "INSERT INTO tasks(id, agent_id, session_id, kind, priority, status, idempotency_key, input_json, created_at, updated_at)
             VALUES(?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
            params![
                id,
                input.agent_id,
                input.session_id,
                input.kind,
                input.priority,
                input.idempotency_key,
                json_text(input.input.as_ref()),
                now,
                now,
            ],
        )?;
        self.record_task_event(&id, "queued", json!({ "priority": input.priority }))?;
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "task.created".to_string(),
            object_type: Some("task".to_string()),
            object_id: Some(id.clone()),
            payload: Some(json!({ "kind": input.kind })),
        })?;
        self.get_task(&id)
    }

    pub fn get_task(&self, id: &str) -> Result<TaskRecord> {
        self.conn
            .query_row(
                "SELECT id, agent_id, session_id, kind, priority, status, lease_owner, lease_until, attempts, result_artifact_id, created_at, updated_at
                 FROM tasks WHERE id = ?",
                params![id],
                task_from_row,
            )
            .optional()?
            .ok_or_else(|| SparseKernelError::NotFound(format!("task {id}")))
    }

    pub fn list_tasks(&self, limit: i64) -> Result<Vec<TaskRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, agent_id, session_id, kind, priority, status, lease_owner, lease_until, attempts, result_artifact_id, created_at, updated_at
             FROM tasks ORDER BY created_at DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![limit.max(0)], task_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn claim_next_task(
        &mut self,
        worker_id: &str,
        kinds: &[String],
        lease_seconds: i64,
    ) -> Result<Option<TaskRecord>> {
        let tx = self.conn.transaction()?;
        let selected: Option<(String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT id, kind FROM tasks WHERE status = 'queued' ORDER BY priority DESC, created_at ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut found = None;
            for row in rows {
                let (id, kind) = row?;
                if kinds.is_empty() || kinds.iter().any(|entry| entry == &kind) {
                    found = Some((id, kind));
                    break;
                }
            }
            found
        };
        let Some((id, kind)) = selected else {
            tx.commit()?;
            return Ok(None);
        };
        let now = now_iso();
        let lease_until = future_iso(lease_seconds.max(1));
        let updated = tx.execute(
            "UPDATE tasks
             SET status = 'running', lease_owner = ?, lease_until = ?, attempts = attempts + 1, updated_at = ?
             WHERE id = ? AND status = 'queued'",
            params![worker_id, lease_until, now, id],
        )?;
        if updated == 0 {
            tx.commit()?;
            return Ok(None);
        }
        tx.execute(
            "INSERT INTO task_events(task_id, event_type, payload_json, created_at) VALUES(?, 'claimed', ?, ?)",
            params![id, json!({ "workerId": worker_id }).to_string(), now],
        )?;
        tx.execute(
            "INSERT INTO audit_log(actor_type, actor_id, action, object_type, object_id, payload_json, created_at)
             VALUES('worker', ?, 'task.claimed', 'task', ?, ?, ?)",
            params![worker_id, id, json!({ "kind": kind }).to_string(), now],
        )?;
        tx.commit()?;
        self.get_task(&id).map(Some)
    }

    pub fn claim_task(
        &self,
        task_id: &str,
        worker_id: &str,
        lease_seconds: i64,
    ) -> Result<Option<TaskRecord>> {
        let now = now_iso();
        let lease_until = future_iso(lease_seconds.max(1));
        let updated = self.conn.execute(
            "UPDATE tasks
             SET status = 'running', lease_owner = ?, lease_until = ?, attempts = attempts + 1, updated_at = ?
             WHERE id = ? AND status = 'queued'",
            params![worker_id, lease_until, now, task_id],
        )?;
        if updated == 0 {
            return Ok(None);
        }
        self.record_task_event(
            task_id,
            "claimed",
            json!({ "workerId": worker_id, "leaseUntil": lease_until }),
        )?;
        self.record_audit(AuditInput {
            actor_type: Some("worker".to_string()),
            actor_id: Some(worker_id.to_string()),
            action: "task.claimed".to_string(),
            object_type: Some("task".to_string()),
            object_id: Some(task_id.to_string()),
            payload: Some(json!({ "leaseUntil": lease_until })),
        })?;
        self.get_task(task_id).map(Some)
    }

    pub fn heartbeat_task(
        &self,
        task_id: &str,
        worker_id: &str,
        lease_seconds: i64,
    ) -> Result<bool> {
        let lease_until = future_iso(lease_seconds.max(1));
        let updated = self.conn.execute(
            "UPDATE tasks SET lease_until = ?, updated_at = ? WHERE id = ? AND status = 'running' AND lease_owner = ?",
            params![lease_until, now_iso(), task_id, worker_id],
        )?;
        Ok(updated > 0)
    }

    pub fn complete_task(
        &self,
        task_id: &str,
        worker_id: &str,
        result_artifact_id: Option<&str>,
    ) -> Result<bool> {
        let now = now_iso();
        let updated = self.conn.execute(
            "UPDATE tasks
             SET status = 'completed', lease_owner = NULL, lease_until = NULL, result_artifact_id = ?, updated_at = ?
             WHERE id = ? AND status = 'running' AND lease_owner = ?",
            params![result_artifact_id, now, task_id, worker_id],
        )?;
        if updated > 0 {
            self.record_task_event(task_id, "completed", json!({ "workerId": worker_id }))?;
            self.record_audit(AuditInput {
                actor_type: Some("worker".to_string()),
                actor_id: Some(worker_id.to_string()),
                action: "task.completed".to_string(),
                object_type: Some("task".to_string()),
                object_id: Some(task_id.to_string()),
                payload: result_artifact_id.map(|id| json!({ "resultArtifactId": id })),
            })?;
        }
        Ok(updated > 0)
    }

    pub fn fail_task(&self, task_id: &str, worker_id: &str, error: &str) -> Result<bool> {
        let now = now_iso();
        let updated = self.conn.execute(
            "UPDATE tasks
             SET status = 'failed', lease_owner = NULL, lease_until = NULL, updated_at = ?
             WHERE id = ? AND status = 'running' AND lease_owner = ?",
            params![now, task_id, worker_id],
        )?;
        if updated > 0 {
            self.record_task_event(task_id, "failed", json!({ "error": error }))?;
            self.record_audit(AuditInput {
                actor_type: Some("worker".to_string()),
                actor_id: Some(worker_id.to_string()),
                action: "task.failed".to_string(),
                object_type: Some("task".to_string()),
                object_id: Some(task_id.to_string()),
                payload: Some(json!({ "error": error })),
            })?;
        }
        Ok(updated > 0)
    }

    pub fn release_expired_leases(&self, now: &str) -> Result<(usize, usize)> {
        let task_ids: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare("SELECT id FROM tasks WHERE status = 'running' AND lease_until IS NOT NULL AND lease_until < ?")?;
            let rows = stmt.query_map(params![now], |row| row.get(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for id in &task_ids {
            self.conn.execute(
                "UPDATE tasks SET status = 'queued', lease_owner = NULL, lease_until = NULL, updated_at = ? WHERE id = ?",
                params![now, id],
            )?;
            self.record_task_event(id, "lease_expired", json!({}))?;
        }
        let resource_count = self.conn.execute(
            "UPDATE resource_leases SET status = 'expired', updated_at = ? WHERE status = 'active' AND lease_until IS NOT NULL AND lease_until < ?",
            params![now, now],
        )?;
        Ok((task_ids.len(), resource_count))
    }

    pub fn record_artifact(
        &self,
        sha256: &str,
        size_bytes: i64,
        storage_ref: &str,
        mime_type: Option<&str>,
        classification: Option<&str>,
        retention_policy: Option<&str>,
    ) -> Result<ArtifactRecord> {
        let id = format!("artifact_{}", Uuid::new_v4());
        let now = now_iso();
        self.conn.execute(
            "INSERT OR IGNORE INTO artifacts(id, sha256, mime_type, size_bytes, storage_ref, classification, retention_policy, created_at)
             VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            params![id, sha256, mime_type, size_bytes, storage_ref, classification, retention_policy, now],
        )?;
        let artifact = self.get_artifact_by_sha256(sha256)?;
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "artifact.created".to_string(),
            object_type: Some("artifact".to_string()),
            object_id: Some(artifact.id.clone()),
            payload: Some(json!({ "sha256": sha256, "sizeBytes": size_bytes })),
        })?;
        Ok(artifact)
    }

    pub fn get_artifact(&self, id: &str) -> Result<ArtifactRecord> {
        self.query_artifact("id", id)?
            .ok_or_else(|| SparseKernelError::NotFound(format!("artifact {id}")))
    }

    pub fn get_artifact_by_sha256(&self, sha256: &str) -> Result<ArtifactRecord> {
        self.query_artifact("sha256", sha256)?
            .ok_or_else(|| SparseKernelError::NotFound(format!("artifact sha256 {sha256}")))
    }

    fn query_artifact(&self, column: &str, value: &str) -> Result<Option<ArtifactRecord>> {
        self.conn
            .query_row(
                &format!("SELECT id, sha256, mime_type, size_bytes, storage_ref, classification, retention_policy, created_at FROM artifacts WHERE {column} = ?"),
                params![value],
                |row| {
                    Ok(ArtifactRecord {
                        id: row.get(0)?,
                        sha256: row.get(1)?,
                        mime_type: row.get(2)?,
                        size_bytes: row.get(3)?,
                        storage_ref: row.get(4)?,
                        classification: row.get(5)?,
                        retention_policy: row.get(6)?,
                        created_at: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(SparseKernelError::from)
    }

    pub fn grant_artifact_access(
        &self,
        artifact_id: &str,
        subject_type: &str,
        subject_id: &str,
        permission: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO artifact_access(artifact_id, subject_type, subject_id, permission, created_at)
             VALUES(?, ?, ?, ?, ?)",
            params![artifact_id, subject_type, subject_id, permission, now_iso()],
        )?;
        Ok(())
    }

    pub fn has_artifact_access(
        &self,
        artifact_id: &str,
        subject_type: &str,
        subject_id: &str,
        permission: &str,
    ) -> Result<bool> {
        let found: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM artifact_access
             WHERE artifact_id = ? AND subject_type = ? AND subject_id = ? AND permission = ?
               AND (expires_at IS NULL OR expires_at > ?)",
                params![artifact_id, subject_type, subject_id, permission, now_iso()],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    pub fn grant_capability(&self, input: GrantCapabilityInput) -> Result<CapabilityRecord> {
        let id = format!("cap_{}", Uuid::new_v4());
        let now = now_iso();
        self.conn.execute(
            "INSERT INTO capabilities(id, subject_type, subject_id, resource_type, resource_id, action, constraints_json, expires_at, created_at)
             VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                id,
                input.subject_type,
                input.subject_id,
                input.resource_type,
                input.resource_id,
                input.action,
                json_text(input.constraints.as_ref()),
                input.expires_at,
                now,
            ],
        )?;
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "capability.granted".to_string(),
            object_type: Some("capability".to_string()),
            object_id: Some(id.clone()),
            payload: None,
        })?;
        self.get_capability(&id)
    }

    pub fn get_capability(&self, id: &str) -> Result<CapabilityRecord> {
        self.conn
            .query_row(
                "SELECT id, subject_type, subject_id, resource_type, resource_id, action, constraints_json, expires_at, created_at
                 FROM capabilities WHERE id = ?",
                params![id],
                capability_from_row,
            )
            .optional()?
            .ok_or_else(|| SparseKernelError::NotFound(format!("capability {id}")))
    }

    pub fn revoke_capability(&self, id: &str) -> Result<bool> {
        let updated = self
            .conn
            .execute("DELETE FROM capabilities WHERE id = ?", params![id])?;
        if updated > 0 {
            self.record_audit(AuditInput {
                actor_type: Some("runtime".to_string()),
                actor_id: None,
                action: "capability.revoked".to_string(),
                object_type: Some("capability".to_string()),
                object_id: Some(id.to_string()),
                payload: None,
            })?;
        }
        Ok(updated > 0)
    }

    pub fn check_capability(&self, check: CapabilityCheck) -> Result<bool> {
        let found: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM capabilities
             WHERE subject_type = ? AND subject_id = ? AND resource_type = ?
               AND (resource_id = ? OR resource_id IS NULL)
               AND action = ? AND (expires_at IS NULL OR expires_at > ?)
             LIMIT 1",
                params![
                    check.subject_type,
                    check.subject_id,
                    check.resource_type,
                    check.resource_id,
                    check.action,
                    now_iso(),
                ],
                |row| row.get(0),
            )
            .optional()?;
        let allowed = found.is_some();
        if !allowed && check.audit_denied {
            self.record_audit(AuditInput {
                actor_type: Some(check.subject_type),
                actor_id: Some(check.subject_id),
                action: "capability.denied".to_string(),
                object_type: Some(check.resource_type),
                object_id: check.resource_id,
                payload: Some(json!({ "action": check.action, "context": check.context })),
            })?;
        }
        Ok(allowed)
    }

    pub fn list_capabilities(
        &self,
        subject_type: &str,
        subject_id: &str,
    ) -> Result<Vec<CapabilityRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, subject_type, subject_id, resource_type, resource_id, action, constraints_json, expires_at, created_at
             FROM capabilities WHERE subject_type = ? AND subject_id = ? ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![subject_type, subject_id], capability_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn create_tool_call(&self, input: CreateToolCallInput) -> Result<ToolCallRecord> {
        let tool_name = input.tool_name.trim();
        if tool_name.is_empty() {
            return Err(SparseKernelError::Invalid(
                "tool_name is required".to_string(),
            ));
        }
        if let Some(agent_id) = &input.agent_id {
            self.ensure_agent(agent_id)?;
            let allowed = self.check_capability(CapabilityCheck {
                subject_type: "agent".to_string(),
                subject_id: agent_id.clone(),
                resource_type: "tool".to_string(),
                resource_id: Some(tool_name.to_string()),
                action: "invoke".to_string(),
                context: Some(json!({
                    "taskId": input.task_id.clone(),
                    "sessionId": input.session_id.clone(),
                })),
                audit_denied: true,
            })?;
            if !allowed {
                self.record_audit(AuditInput {
                    actor_type: Some("agent".to_string()),
                    actor_id: Some(agent_id.clone()),
                    action: "tool_call.denied".to_string(),
                    object_type: Some("tool".to_string()),
                    object_id: Some(tool_name.to_string()),
                    payload: None,
                })?;
                return Err(SparseKernelError::Denied(format!(
                    "agent {agent_id} cannot invoke tool {tool_name}"
                )));
            }
        }

        let id = input
            .id
            .unwrap_or_else(|| format!("tool_call_{}", Uuid::new_v4()));
        let now = now_iso();
        self.conn.execute(
            "INSERT INTO tool_calls(id, task_id, session_id, agent_id, tool_name, status, input_json, created_at)
             VALUES(?, ?, ?, ?, ?, 'created', ?, ?)",
            params![
                id,
                input.task_id,
                input.session_id,
                input.agent_id,
                tool_name,
                json_text(input.input.as_ref()),
                now,
            ],
        )?;
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "tool_call.created".to_string(),
            object_type: Some("tool_call".to_string()),
            object_id: Some(id.clone()),
            payload: Some(json!({ "toolName": tool_name })),
        })?;
        self.get_tool_call(&id)
    }

    pub fn get_tool_call(&self, id: &str) -> Result<ToolCallRecord> {
        self.conn
            .query_row(
                "SELECT id, task_id, session_id, agent_id, tool_name, status, input_json, output_json, error, started_at, ended_at, created_at
                 FROM tool_calls WHERE id = ?",
                params![id],
                tool_call_from_row,
            )
            .optional()?
            .ok_or_else(|| SparseKernelError::NotFound(format!("tool call {id}")))
    }

    pub fn list_tool_calls(&self, limit: i64) -> Result<Vec<ToolCallRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, task_id, session_id, agent_id, tool_name, status, input_json, output_json, error, started_at, ended_at, created_at
             FROM tool_calls ORDER BY created_at DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![limit.max(0)], tool_call_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn start_tool_call(&self, id: &str) -> Result<ToolCallRecord> {
        let now = now_iso();
        let updated = self.conn.execute(
            "UPDATE tool_calls SET status = 'running', started_at = ? WHERE id = ? AND status = 'created'",
            params![now, id],
        )?;
        if updated == 0 {
            return Err(SparseKernelError::Invalid(format!(
                "tool call {id} cannot be started"
            )));
        }
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "tool_call.started".to_string(),
            object_type: Some("tool_call".to_string()),
            object_id: Some(id.to_string()),
            payload: None,
        })?;
        self.get_tool_call(id)
    }

    pub fn complete_tool_call(
        &self,
        id: &str,
        input: CompleteToolCallInput,
    ) -> Result<ToolCallRecord> {
        for artifact_id in &input.artifact_ids {
            self.get_artifact(artifact_id)?;
        }
        let now = now_iso();
        let output = json!({
            "output": input.output,
            "artifact_ids": input.artifact_ids,
        });
        let updated = self.conn.execute(
            "UPDATE tool_calls SET status = 'completed', output_json = ?, error = NULL, ended_at = ? WHERE id = ? AND status = 'running'",
            params![output.to_string(), now, id],
        )?;
        if updated == 0 {
            return Err(SparseKernelError::Invalid(format!(
                "tool call {id} cannot be completed"
            )));
        }
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "tool_call.completed".to_string(),
            object_type: Some("tool_call".to_string()),
            object_id: Some(id.to_string()),
            payload: Some(json!({ "artifactIds": output["artifact_ids"] })),
        })?;
        self.get_tool_call(id)
    }

    pub fn fail_tool_call(&self, id: &str, error: &str) -> Result<ToolCallRecord> {
        if error.trim().is_empty() {
            return Err(SparseKernelError::Invalid("error is required".to_string()));
        }
        let now = now_iso();
        let updated = self.conn.execute(
            "UPDATE tool_calls SET status = 'failed', error = ?, ended_at = ? WHERE id = ? AND status IN ('created', 'running')",
            params![error, now, id],
        )?;
        if updated == 0 {
            return Err(SparseKernelError::Invalid(format!(
                "tool call {id} cannot be failed"
            )));
        }
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "tool_call.failed".to_string(),
            object_type: Some("tool_call".to_string()),
            object_id: Some(id.to_string()),
            payload: Some(json!({ "error": error })),
        })?;
        self.get_tool_call(id)
    }

    pub fn list_browser_contexts(&self, limit: i64) -> Result<Vec<BrowserContextRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, pool_id, agent_id, session_id, task_id, profile_mode, status, created_at
             FROM browser_contexts ORDER BY created_at DESC LIMIT ?",
        )?;
        let rows = stmt.query_map(params![limit.max(0)], browser_context_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn list_browser_pools(&self) -> Result<Vec<BrowserPoolRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, trust_zone_id, browser_kind, status, max_contexts, cdp_endpoint, created_at, updated_at
             FROM browser_pools ORDER BY trust_zone_id ASC, browser_kind ASC",
        )?;
        let rows = stmt.query_map([], browser_pool_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn record_browser_target(
        &self,
        input: RecordBrowserTargetInput,
    ) -> Result<BrowserTargetRecord> {
        let now = input
            .updated_at
            .clone()
            .or_else(|| input.created_at.clone())
            .unwrap_or_else(now_iso);
        let id = format!("{}:{}", input.context_id, input.target_id);
        self.conn.execute(
            "INSERT INTO browser_targets(
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
                closed_at = COALESCE(excluded.closed_at, browser_targets.closed_at)",
            params![
                id,
                input.context_id,
                input.target_id,
                input.opener_target_id,
                input.url,
                input.title,
                input.status.unwrap_or_else(|| "active".to_string()),
                input.close_reason,
                input.created_at.unwrap_or_else(|| now.clone()),
                now,
                input.closed_at,
            ],
        )?;
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "browser_target.recorded".to_string(),
            object_type: Some("browser_target".to_string()),
            object_id: Some(id),
            payload: None,
        })?;
        self.get_browser_target(&input.context_id, &input.target_id)
    }

    pub fn get_browser_target(
        &self,
        context_id: &str,
        target_id: &str,
    ) -> Result<BrowserTargetRecord> {
        self.conn
            .query_row(
                "SELECT id, context_id, target_id, opener_target_id, url, title, status, close_reason,
                        console_count, network_count, artifact_count, created_at, updated_at, closed_at
                 FROM browser_targets WHERE context_id = ? AND target_id = ?",
                params![context_id, target_id],
                browser_target_from_row,
            )
            .map_err(SparseKernelError::from)
    }

    pub fn close_browser_target(
        &self,
        context_id: &str,
        target_id: &str,
        reason: Option<&str>,
        closed_at: Option<&str>,
    ) -> Result<BrowserTargetRecord> {
        let now = closed_at.map(str::to_string).unwrap_or_else(now_iso);
        let target = self.record_browser_target(RecordBrowserTargetInput {
            context_id: context_id.to_string(),
            target_id: target_id.to_string(),
            opener_target_id: None,
            url: None,
            title: None,
            status: Some("closed".to_string()),
            close_reason: Some(reason.unwrap_or("closed").to_string()),
            created_at: None,
            updated_at: Some(now.clone()),
            closed_at: Some(now),
        })?;
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "browser_target.closed".to_string(),
            object_type: Some("browser_target".to_string()),
            object_id: Some(target.id.clone()),
            payload: Some(json!({ "reason": reason })),
        })?;
        Ok(target)
    }

    pub fn record_browser_observation(
        &self,
        input: RecordBrowserObservationInput,
    ) -> Result<BrowserObservationRecord> {
        let now = input.created_at.unwrap_or_else(now_iso);
        let context_id = input.context_id;
        let target_id = input.target_id;
        let observation_type = input.observation_type;
        let payload = input.payload;
        if let Some(target_id) = &target_id {
            let _ = self.record_browser_target(RecordBrowserTargetInput {
                context_id: context_id.clone(),
                target_id: target_id.clone(),
                opener_target_id: None,
                url: None,
                title: None,
                status: Some("active".to_string()),
                close_reason: None,
                created_at: Some(now.clone()),
                updated_at: Some(now.clone()),
                closed_at: None,
            });
        }
        self.conn.execute(
            "INSERT INTO browser_observations(context_id, target_id, observation_type, payload_json, created_at)
             VALUES(?, ?, ?, ?, ?)",
            params![
                context_id.as_str(),
                target_id.as_deref(),
                observation_type.as_str(),
                json_text(payload.as_ref()),
                now.as_str(),
            ],
        )?;
        let id = self.conn.last_insert_rowid();
        if let Some(target_id) = &target_id {
            let counter = if observation_type.starts_with("browser_console") {
                Some("console_count")
            } else if observation_type.starts_with("browser_artifact") {
                Some("artifact_count")
            } else if observation_type.starts_with("browser_network") {
                Some("network_count")
            } else {
                None
            };
            if let Some(counter) = counter {
                self.conn.execute(
                    &format!(
                        "UPDATE browser_targets SET {counter} = {counter} + 1, updated_at = ? WHERE context_id = ? AND target_id = ?"
                    ),
                    params![now.as_str(), context_id.as_str(), target_id.as_str()],
                )?;
            }
        }
        self.record_audit(AuditInput {
            actor_type: Some("runtime".to_string()),
            actor_id: None,
            action: "browser_context.observation".to_string(),
            object_type: Some("browser_context".to_string()),
            object_id: Some(context_id.clone()),
            payload: Some(json!({
                "targetId": target_id,
                "observationType": observation_type,
                "payload": payload,
            })),
        })?;
        self.conn
            .query_row(
                "SELECT id, context_id, target_id, observation_type, payload_json, created_at
                 FROM browser_observations WHERE id = ?",
                params![id],
                browser_observation_from_row,
            )
            .map_err(SparseKernelError::from)
    }

    pub fn list_browser_targets(
        &self,
        input: ListBrowserTargetsInput,
    ) -> Result<Vec<BrowserTargetRecord>> {
        let mut conditions = Vec::new();
        let mut values: Vec<String> = Vec::new();
        if let Some(value) = input.context_id {
            conditions.push("bt.context_id = ?");
            values.push(value);
        }
        if let Some(value) = input.session_id {
            conditions.push("bc.session_id = ?");
            values.push(value);
        }
        if let Some(value) = input.task_id {
            conditions.push("bc.task_id = ?");
            values.push(value);
        }
        if let Some(value) = input.status {
            conditions.push("bt.status = ?");
            values.push(value);
        }
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let limit = input.limit.unwrap_or(100).clamp(1, 1000);
        let sql = format!(
            "SELECT bt.id, bt.context_id, bt.target_id, bt.opener_target_id, bt.url, bt.title,
                    bt.status, bt.close_reason, bt.console_count, bt.network_count,
                    bt.artifact_count, bt.created_at, bt.updated_at, bt.closed_at
             FROM browser_targets bt
             JOIN browser_contexts bc ON bc.id = bt.context_id
             {where_clause}
             ORDER BY bt.updated_at DESC, bt.id ASC
             LIMIT ?"
        );
        let mut params = values;
        params.push(limit.to_string());
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(params), browser_target_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    pub fn list_browser_observations(
        &self,
        input: ListBrowserObservationsInput,
    ) -> Result<Vec<BrowserObservationRecord>> {
        let mut conditions = Vec::new();
        let mut values: Vec<String> = Vec::new();
        if let Some(value) = input.context_id {
            conditions.push("context_id = ?");
            values.push(value);
        }
        if let Some(value) = input.target_id {
            conditions.push("target_id = ?");
            values.push(value);
        }
        if let Some(value) = input.observation_type {
            conditions.push("observation_type = ?");
            values.push(value);
        }
        if let Some(value) = input.since {
            conditions.push("created_at >= ?");
            values.push(value);
        }
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let limit = input.limit.unwrap_or(100).clamp(1, 1000);
        let sql = format!(
            "SELECT id, context_id, target_id, observation_type, payload_json, created_at
             FROM browser_observations
             {where_clause}
             ORDER BY id DESC
             LIMIT ?"
        );
        let mut params = values;
        params.push(limit.to_string());
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(params), browser_observation_from_row)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(SparseKernelError::from)
    }

    fn record_task_event(&self, task_id: &str, event_type: &str, payload: Value) -> Result<()> {
        self.conn.execute(
            "INSERT INTO task_events(task_id, event_type, payload_json, created_at) VALUES(?, ?, ?, ?)",
            params![task_id, event_type, payload.to_string(), now_iso()],
        )?;
        Ok(())
    }
}

fn task_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
    Ok(TaskRecord {
        id: row.get(0)?,
        agent_id: row.get(1)?,
        session_id: row.get(2)?,
        kind: row.get(3)?,
        priority: row.get(4)?,
        status: row.get(5)?,
        lease_owner: row.get(6)?,
        lease_until: row.get(7)?,
        attempts: row.get(8)?,
        result_artifact_id: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn session_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        id: row.get(0)?,
        agent_id: row.get(1)?,
        session_key: row.get(2)?,
        channel: row.get(3)?,
        status: row.get(4)?,
        current_token_count: row.get(5)?,
        last_activity_at: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn transcript_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TranscriptEventRecord> {
    Ok(TranscriptEventRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        parent_event_id: row.get(2)?,
        seq: row.get(3)?,
        role: row.get(4)?,
        event_type: row.get(5)?,
        content: parse_json(row.get(6)?),
        tool_call_id: row.get(7)?,
        token_count: row.get(8)?,
        created_at: row.get(9)?,
    })
}

fn tool_call_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ToolCallRecord> {
    Ok(ToolCallRecord {
        id: row.get(0)?,
        task_id: row.get(1)?,
        session_id: row.get(2)?,
        agent_id: row.get(3)?,
        tool_name: row.get(4)?,
        status: row.get(5)?,
        input: parse_json(row.get(6)?),
        output: parse_json(row.get(7)?),
        error: row.get(8)?,
        started_at: row.get(9)?,
        ended_at: row.get(10)?,
        created_at: row.get(11)?,
    })
}

fn capability_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CapabilityRecord> {
    Ok(CapabilityRecord {
        id: row.get(0)?,
        subject_type: row.get(1)?,
        subject_id: row.get(2)?,
        resource_type: row.get(3)?,
        resource_id: row.get(4)?,
        action: row.get(5)?,
        constraints: parse_json(row.get(6)?),
        expires_at: row.get(7)?,
        created_at: row.get(8)?,
    })
}

fn browser_context_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BrowserContextRecord> {
    Ok(BrowserContextRecord {
        id: row.get(0)?,
        pool_id: row.get(1)?,
        agent_id: row.get(2)?,
        session_id: row.get(3)?,
        task_id: row.get(4)?,
        profile_mode: row.get(5)?,
        status: row.get(6)?,
        created_at: row.get(7)?,
    })
}

fn browser_pool_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BrowserPoolRecord> {
    Ok(BrowserPoolRecord {
        id: row.get(0)?,
        trust_zone_id: row.get(1)?,
        browser_kind: row.get(2)?,
        status: row.get(3)?,
        max_contexts: row.get(4)?,
        cdp_endpoint: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn browser_target_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BrowserTargetRecord> {
    Ok(BrowserTargetRecord {
        id: row.get(0)?,
        context_id: row.get(1)?,
        target_id: row.get(2)?,
        opener_target_id: row.get(3)?,
        url: row.get(4)?,
        title: row.get(5)?,
        status: row.get(6)?,
        close_reason: row.get(7)?,
        console_count: row.get(8)?,
        network_count: row.get(9)?,
        artifact_count: row.get(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
        closed_at: row.get(13)?,
    })
}

fn browser_observation_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BrowserObservationRecord> {
    Ok(BrowserObservationRecord {
        id: row.get(0)?,
        context_id: row.get(1)?,
        target_id: row.get(2)?,
        observation_type: row.get(3)?,
        payload: parse_json(row.get(4)?),
        created_at: row.get(5)?,
    })
}

pub struct ArtifactStore<'a> {
    db: &'a SparseKernelDb,
    root: PathBuf,
}

struct TempArtifactBlob {
    path: PathBuf,
    sha256: String,
    size_bytes: i64,
}

impl<'a> ArtifactStore<'a> {
    pub fn new(db: &'a SparseKernelDb, root: impl AsRef<Path>) -> Self {
        Self {
            db,
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn write(
        &self,
        bytes: &[u8],
        mime_type: Option<&str>,
        retention_policy: Option<&str>,
        subject: Option<(&str, &str, &str)>,
    ) -> Result<ArtifactRecord> {
        let sha256 = hex_sha256(bytes);
        let storage_ref = artifact_storage_ref(&sha256)?;
        let storage_path = self.root.join(&storage_ref);
        if let Some(parent) = storage_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !storage_path.exists() {
            fs::write(&storage_path, bytes)?;
        }
        self.record_stored_artifact(
            &sha256,
            bytes.len() as i64,
            &storage_ref,
            mime_type,
            retention_policy,
            subject,
        )
    }

    pub fn import_file(
        &self,
        file_path: impl AsRef<Path>,
        mime_type: Option<&str>,
        retention_policy: Option<&str>,
        subject: Option<(&str, &str, &str)>,
    ) -> Result<ArtifactRecord> {
        let file = fs::File::open(file_path.as_ref())?;
        let temp = self.write_reader_to_temp(file)?;
        let storage_ref = artifact_storage_ref(&temp.sha256)?;
        self.move_temp_to_storage(&temp.path, &storage_ref)?;
        self.record_stored_artifact(
            &temp.sha256,
            temp.size_bytes,
            &storage_ref,
            mime_type,
            retention_policy,
            subject,
        )
    }

    pub fn read(&self, artifact_id: &str, subject: Option<(&str, &str, &str)>) -> Result<Vec<u8>> {
        let artifact = self.checked_artifact(artifact_id, subject)?;
        Ok(fs::read(self.root.join(artifact.storage_ref))?)
    }

    pub fn export_file(
        &self,
        artifact_id: &str,
        destination_path: impl AsRef<Path>,
        subject: Option<(&str, &str, &str)>,
    ) -> Result<ArtifactRecord> {
        let artifact = self.checked_artifact(artifact_id, subject)?;
        let destination_path = destination_path.as_ref();
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(self.root.join(&artifact.storage_ref), destination_path)?;
        Ok(artifact)
    }

    fn checked_artifact(
        &self,
        artifact_id: &str,
        subject: Option<(&str, &str, &str)>,
    ) -> Result<ArtifactRecord> {
        if let Some((subject_type, subject_id, permission)) = subject {
            if !self
                .db
                .has_artifact_access(artifact_id, subject_type, subject_id, permission)?
            {
                self.db.record_audit(AuditInput {
                    actor_type: Some(subject_type.to_string()),
                    actor_id: Some(subject_id.to_string()),
                    action: "artifact_access.denied".to_string(),
                    object_type: Some("artifact".to_string()),
                    object_id: Some(artifact_id.to_string()),
                    payload: Some(json!({ "permission": permission })),
                })?;
                return Err(SparseKernelError::Denied(format!(
                    "artifact access denied: {artifact_id}"
                )));
            }
        }
        let artifact = self.db.get_artifact(artifact_id)?;
        Ok(artifact)
    }

    fn record_stored_artifact(
        &self,
        sha256: &str,
        size_bytes: i64,
        storage_ref: &str,
        mime_type: Option<&str>,
        retention_policy: Option<&str>,
        subject: Option<(&str, &str, &str)>,
    ) -> Result<ArtifactRecord> {
        let artifact = self.db.record_artifact(
            sha256,
            size_bytes,
            storage_ref,
            mime_type,
            None,
            retention_policy,
        )?;
        if let Some((subject_type, subject_id, permission)) = subject {
            self.db
                .grant_artifact_access(&artifact.id, subject_type, subject_id, permission)?;
        }
        Ok(artifact)
    }

    fn write_reader_to_temp<R: Read>(&self, mut reader: R) -> Result<TempArtifactBlob> {
        let tmp_dir = self.root.join(".tmp");
        fs::create_dir_all(&tmp_dir)?;
        let tmp_path = tmp_dir.join(format!(
            "artifact-{}-{}.tmp",
            std::process::id(),
            Uuid::new_v4()
        ));
        let mut writer = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)?;
        let mut hasher = Sha256::new();
        let mut size_bytes = 0_i64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => count,
                Err(err) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(err.into());
                }
            };
            let chunk = &buffer[..count];
            hasher.update(chunk);
            size_bytes += count as i64;
            if let Err(err) = writer.write_all(chunk) {
                let _ = fs::remove_file(&tmp_path);
                return Err(err.into());
            }
        }
        if let Err(err) = writer.flush() {
            let _ = fs::remove_file(&tmp_path);
            return Err(err.into());
        }
        Ok(TempArtifactBlob {
            path: tmp_path,
            sha256: format!("{:x}", hasher.finalize()),
            size_bytes,
        })
    }

    fn move_temp_to_storage(&self, tmp_path: &Path, storage_ref: &str) -> Result<()> {
        let storage_path = self.root.join(storage_ref);
        if let Some(parent) = storage_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if storage_path.exists() {
            fs::remove_file(tmp_path)?;
            return Ok(());
        }
        match fs::rename(tmp_path, &storage_path) {
            Ok(()) => Ok(()),
            Err(_) if storage_path.exists() => {
                let _ = fs::remove_file(tmp_path);
                Ok(())
            }
            Err(err) => {
                let _ = fs::remove_file(tmp_path);
                Err(err.into())
            }
        }
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn artifact_storage_ref(sha256: &str) -> Result<String> {
    if sha256.len() != 64 || !sha256.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(SparseKernelError::Invalid(format!(
            "invalid sha256 {sha256}"
        )));
    }
    Ok(format!(
        "sha256/{}/{}/{}",
        &sha256[0..2],
        &sha256[2..4],
        sha256
    ))
}

#[derive(Debug, Clone)]
struct LoopbackHttpEndpoint {
    authority: String,
    host: String,
    port: u16,
}

fn parse_loopback_http_endpoint(
    endpoint: &str,
) -> std::result::Result<LoopbackHttpEndpoint, String> {
    let endpoint = endpoint.trim();
    let Some(rest) = endpoint.strip_prefix("http://") else {
        return Err("only http:// loopback CDP endpoints are supported".to_string());
    };
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.is_empty() {
        return Err("CDP endpoint host is required".to_string());
    }
    if authority.contains('@') {
        return Err("credentials in CDP endpoint URLs are not supported".to_string());
    }

    let (host, port) = if let Some(stripped) = authority.strip_prefix('[') {
        let (host, suffix) = stripped
            .split_once(']')
            .ok_or_else(|| "invalid IPv6 CDP endpoint".to_string())?;
        let port = suffix
            .strip_prefix(':')
            .map(parse_port)
            .transpose()?
            .unwrap_or(80);
        (host.to_string(), port)
    } else if let Some((host, port)) = authority.rsplit_once(':') {
        if host.is_empty() || port.is_empty() {
            return Err("CDP endpoint host and port are required".to_string());
        }
        (host.to_string(), parse_port(port)?)
    } else {
        (authority.to_string(), 80)
    };

    let normalized = host.to_ascii_lowercase();
    if normalized != "localhost" && normalized != "127.0.0.1" && normalized != "::1" {
        return Err("CDP endpoint must be loopback (localhost, 127.0.0.1, or [::1])".to_string());
    }

    Ok(LoopbackHttpEndpoint {
        authority: authority.to_string(),
        host,
        port,
    })
}

fn parse_port(raw: &str) -> std::result::Result<u16, String> {
    let port = raw
        .parse::<u16>()
        .map_err(|_| format!("invalid CDP endpoint port: {raw}"))?;
    if port == 0 {
        return Err("CDP endpoint port must be nonzero".to_string());
    }
    Ok(port)
}

fn validate_browser_cdp_endpoint(endpoint: &str) -> Result<()> {
    parse_loopback_http_endpoint(endpoint)
        .map(|_| ())
        .map_err(SparseKernelError::Invalid)
}

pub fn probe_browser_endpoint(endpoint: &str) -> BrowserEndpointProbe {
    let parsed = match parse_loopback_http_endpoint(endpoint) {
        Ok(parsed) => parsed,
        Err(error) => {
            return BrowserEndpointProbe {
                endpoint: endpoint.to_string(),
                reachable: false,
                status_code: None,
                browser: None,
                web_socket_debugger_url: None,
                error: Some(error),
            };
        }
    };

    match probe_loopback_http_endpoint(&parsed) {
        Ok((status_code, body)) => {
            let mut probe = BrowserEndpointProbe {
                endpoint: endpoint.to_string(),
                reachable: (200..300).contains(&status_code),
                status_code: Some(status_code),
                browser: None,
                web_socket_debugger_url: None,
                error: None,
            };
            if !probe.reachable {
                probe.error = Some(format!("CDP endpoint returned HTTP {status_code}"));
                return probe;
            }
            match serde_json::from_str::<Value>(&body) {
                Ok(value) => {
                    probe.browser = value
                        .get("Browser")
                        .or_else(|| value.get("browser"))
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    probe.web_socket_debugger_url = value
                        .get("webSocketDebuggerUrl")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    probe
                }
                Err(err) => BrowserEndpointProbe {
                    endpoint: endpoint.to_string(),
                    reachable: false,
                    status_code: Some(status_code),
                    browser: None,
                    web_socket_debugger_url: None,
                    error: Some(format!("invalid CDP version JSON: {err}")),
                },
            }
        }
        Err(error) => BrowserEndpointProbe {
            endpoint: endpoint.to_string(),
            reachable: false,
            status_code: None,
            browser: None,
            web_socket_debugger_url: None,
            error: Some(error),
        },
    }
}

fn probe_loopback_http_endpoint(
    endpoint: &LoopbackHttpEndpoint,
) -> std::result::Result<(u16, String), String> {
    let timeout = Duration::from_millis(1_500);
    let addrs = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .map_err(|err| format!("failed to resolve CDP endpoint: {err}"))?;
    let mut last_error = None;
    for addr in addrs {
        if !addr.ip().is_loopback() {
            continue;
        }
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(timeout))
                    .map_err(|err| format!("failed to set read timeout: {err}"))?;
                stream
                    .set_write_timeout(Some(timeout))
                    .map_err(|err| format!("failed to set write timeout: {err}"))?;
                let request = format!(
                    "GET /json/version HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
                    endpoint.authority
                );
                stream
                    .write_all(request.as_bytes())
                    .map_err(|err| format!("failed to write CDP probe: {err}"))?;
                let mut response = String::new();
                stream
                    .read_to_string(&mut response)
                    .map_err(|err| format!("failed to read CDP probe: {err}"))?;
                return parse_http_response(&response);
            }
            Err(err) => last_error = Some(err.to_string()),
        }
    }
    Err(last_error.unwrap_or_else(|| "no loopback address resolved for CDP endpoint".to_string()))
}

fn parse_http_response(response: &str) -> std::result::Result<(u16, String), String> {
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "invalid HTTP response from CDP endpoint".to_string())?;
    let status_line = headers
        .lines()
        .next()
        .ok_or_else(|| "missing HTTP status line from CDP endpoint".to_string())?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "missing HTTP status code from CDP endpoint".to_string())?
        .parse::<u16>()
        .map_err(|_| "invalid HTTP status code from CDP endpoint".to_string())?;
    Ok((status_code, body.to_string()))
}

pub trait ToolBroker {
    fn create_call(&self, input: CreateToolCallInput) -> Result<ToolCallRecord>;
    fn start_call(&self, id: &str) -> Result<ToolCallRecord>;
    fn complete_call(&self, id: &str, input: CompleteToolCallInput) -> Result<ToolCallRecord>;
    fn fail_call(&self, id: &str, error: &str) -> Result<ToolCallRecord>;
}

pub struct LedgerToolBroker<'a> {
    pub db: &'a SparseKernelDb,
}

impl ToolBroker for LedgerToolBroker<'_> {
    fn create_call(&self, input: CreateToolCallInput) -> Result<ToolCallRecord> {
        self.db.create_tool_call(input)
    }

    fn start_call(&self, id: &str) -> Result<ToolCallRecord> {
        self.db.start_tool_call(id)
    }

    fn complete_call(&self, id: &str, input: CompleteToolCallInput) -> Result<ToolCallRecord> {
        self.db.complete_tool_call(id, input)
    }

    fn fail_call(&self, id: &str, error: &str) -> Result<ToolCallRecord> {
        self.db.fail_tool_call(id, error)
    }
}

pub trait BrowserBroker {
    fn acquire_context(
        &self,
        agent_id: Option<&str>,
        session_id: Option<&str>,
        task_id: Option<&str>,
        trust_zone_id: &str,
        max_contexts: i64,
        cdp_endpoint: Option<&str>,
    ) -> Result<BrowserContextRecord>;
    fn release_context(&self, context_id: &str) -> Result<bool>;
}

pub struct MockBrowserBroker<'a> {
    pub db: &'a SparseKernelDb,
}

impl BrowserBroker for MockBrowserBroker<'_> {
    fn acquire_context(
        &self,
        agent_id: Option<&str>,
        session_id: Option<&str>,
        task_id: Option<&str>,
        trust_zone_id: &str,
        max_contexts: i64,
        cdp_endpoint: Option<&str>,
    ) -> Result<BrowserContextRecord> {
        if let Some(endpoint) = cdp_endpoint {
            validate_browser_cdp_endpoint(endpoint)?;
        }
        if let Some(agent_id) = agent_id {
            self.db.ensure_agent(agent_id)?;
            let allowed = self.db.check_capability(CapabilityCheck {
                subject_type: "agent".to_string(),
                subject_id: agent_id.to_string(),
                resource_type: "browser_context".to_string(),
                resource_id: Some(trust_zone_id.to_string()),
                action: "allocate".to_string(),
                context: None,
                audit_denied: true,
            })?;
            if !allowed {
                return Err(SparseKernelError::Denied(format!(
                    "agent {agent_id} cannot allocate browser context"
                )));
            }
        }
        let pool_id = format!("browser_pool_{trust_zone_id}");
        let now = now_iso();
        let browser_kind = if cdp_endpoint.is_some() {
            "cdp"
        } else {
            "mock"
        };
        self.db.conn.execute(
            "INSERT INTO browser_pools(id, trust_zone_id, browser_kind, status, max_contexts, cdp_endpoint, created_at, updated_at)
             VALUES(?, ?, ?, 'active', ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
               browser_kind = CASE
                 WHEN excluded.cdp_endpoint IS NOT NULL THEN excluded.browser_kind
                 ELSE browser_pools.browser_kind
               END,
               max_contexts = excluded.max_contexts,
               cdp_endpoint = COALESCE(excluded.cdp_endpoint, browser_pools.cdp_endpoint),
               updated_at = excluded.updated_at",
            params![pool_id, trust_zone_id, browser_kind, max_contexts, cdp_endpoint, now, now],
        )?;
        let active: i64 = self.db.conn.query_row(
            "SELECT COUNT(*) FROM browser_contexts WHERE pool_id = ? AND status = 'active'",
            params![pool_id],
            |row| row.get(0),
        )?;
        if active >= max_contexts {
            return Err(SparseKernelError::Denied(
                "no browser contexts available".to_string(),
            ));
        }
        let context_id = format!("browser_ctx_{}", Uuid::new_v4());
        self.db.conn.execute(
            "INSERT INTO browser_contexts(id, pool_id, agent_id, session_id, task_id, profile_mode, status, created_at)
             VALUES(?, ?, ?, ?, ?, 'ephemeral', 'active', ?)",
            params![context_id, pool_id, agent_id, session_id, task_id, now],
        )?;
        self.db.conn.execute(
            "INSERT INTO resource_leases(id, resource_type, resource_id, owner_task_id, owner_agent_id, trust_zone_id, status, created_at, updated_at)
             VALUES(?, 'browser_context', ?, ?, ?, ?, 'active', ?, ?)",
            params![context_id, context_id, task_id, agent_id, trust_zone_id, now, now],
        )?;
        self.db.record_audit(AuditInput {
            actor_type: agent_id.map(|_| "agent".to_string()),
            actor_id: agent_id.map(str::to_string),
            action: "browser_context.acquired".to_string(),
            object_type: Some("browser_context".to_string()),
            object_id: Some(context_id.clone()),
            payload: Some(json!({
                "trustZoneId": trust_zone_id,
                "browserKind": browser_kind,
                "cdpEndpointConfigured": cdp_endpoint.is_some()
            })),
        })?;
        Ok(BrowserContextRecord {
            id: context_id,
            pool_id,
            agent_id: agent_id.map(str::to_string),
            session_id: session_id.map(str::to_string),
            task_id: task_id.map(str::to_string),
            profile_mode: "ephemeral".to_string(),
            status: "active".to_string(),
            created_at: now,
        })
    }

    fn release_context(&self, context_id: &str) -> Result<bool> {
        let now = now_iso();
        let updated = self.db.conn.execute(
            "UPDATE browser_contexts SET status = 'released' WHERE id = ? AND status = 'active'",
            params![context_id],
        )?;
        if updated > 0 {
            self.db.conn.execute(
                "UPDATE resource_leases SET status = 'released', updated_at = ? WHERE resource_type = 'browser_context' AND resource_id = ?",
                params![now, context_id],
            )?;
            self.db.record_audit(AuditInput {
                actor_type: Some("runtime".to_string()),
                actor_id: None,
                action: "browser_context.released".to_string(),
                object_type: Some("browser_context".to_string()),
                object_id: Some(context_id.to_string()),
                payload: None,
            })?;
        }
        Ok(updated > 0)
    }
}

pub trait SandboxBroker {
    fn allocate_sandbox(
        &self,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        trust_zone_id: &str,
        backend: Option<&str>,
    ) -> Result<SandboxAllocationRecord>;
    fn release_sandbox(&self, allocation_id: &str) -> Result<bool>;
}

pub struct LocalSandboxBroker<'a> {
    pub db: &'a SparseKernelDb,
}

fn trust_zone_allows_network(db: &SparseKernelDb, trust_zone_id: &str) -> Result<bool> {
    let action = db
        .conn
        .query_row(
            "SELECT np.default_action
             FROM trust_zones tz
             JOIN network_policies np ON np.id = tz.network_policy_id
             WHERE tz.id = ?",
            params![trust_zone_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(matches!(action.as_deref(), Some("allow")))
}

fn hard_egress_helper_args() -> Result<Vec<String>> {
    let raw = env::var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS").unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        return Ok(serde_json::from_str(trimmed)?);
    }
    Ok(trimmed
        .split_whitespace()
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect())
}

fn run_hard_egress_helper(
    action: &str,
    allocation_id: &str,
    backend: &str,
    trust_zone_id: &str,
    agent_id: Option<&str>,
    task_id: Option<&str>,
    enforcement: Option<&Value>,
) -> Result<Option<Value>> {
    let helper = env::var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SparseKernelError::Denied("missing hard egress helper".to_string()))?;
    let payload = json!({
        "protocol": "openclaw.sparsekernel.sandbox-egress.v1",
        "action": action,
        "allocationId": allocation_id,
        "backend": backend,
        "trustZoneId": trust_zone_id,
        "agentId": agent_id,
        "taskId": task_id,
        "enforcement": enforcement,
    });
    let mut child = Command::new(&helper)
        .args(hard_egress_helper_args()?)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(payload.to_string().as_bytes())?;
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(SparseKernelError::Denied(format!(
            "hard egress helper exited {}: {}",
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            if stderr.is_empty() {
                "no stderr".to_string()
            } else {
                stderr
            }
        )));
    }
    if action == "release" {
        return Ok(None);
    }
    let response: Value = serde_json::from_slice(&output.stdout)?;
    if response.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(SparseKernelError::Denied(
            "hard egress helper did not confirm enforcement".to_string(),
        ));
    }
    let enforcement_id = response
        .get("enforcementId")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            SparseKernelError::Denied(
                "hard egress helper response missing enforcementId".to_string(),
            )
        })?;
    let boundary = response
        .get("boundary")
        .and_then(Value::as_str)
        .filter(|value| {
            matches!(
                *value,
                "host_firewall" | "egress_proxy" | "vm_firewall" | "platform_enforcer"
            )
        })
        .ok_or_else(|| {
            SparseKernelError::Denied(
                "hard egress helper response missing supported boundary".to_string(),
            )
        })?;
    Ok(Some(json!({
        "helper": helper,
        "enforcementId": enforcement_id,
        "boundary": boundary,
        "description": response.get("description").and_then(Value::as_str),
    })))
}

impl SandboxBroker for LocalSandboxBroker<'_> {
    fn allocate_sandbox(
        &self,
        agent_id: Option<&str>,
        task_id: Option<&str>,
        trust_zone_id: &str,
        backend: Option<&str>,
    ) -> Result<SandboxAllocationRecord> {
        if let Some(agent_id) = agent_id {
            self.db.ensure_agent(agent_id)?;
            let allowed = self.db.check_capability(CapabilityCheck {
                subject_type: "agent".to_string(),
                subject_id: agent_id.to_string(),
                resource_type: "sandbox".to_string(),
                resource_id: Some(trust_zone_id.to_string()),
                action: "allocate".to_string(),
                context: None,
                audit_denied: true,
            })?;
            if !allowed {
                return Err(SparseKernelError::Denied(format!(
                    "agent {agent_id} cannot allocate sandbox"
                )));
            }
        }
        let allocation_id = format!("sandbox_{}", Uuid::new_v4());
        let now = now_iso();
        let backend = backend.unwrap_or("local/no_isolation").to_string();
        let mut metadata = json!({ "backend": backend.clone() });
        if truthy_env_flag("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS")
            && trust_zone_allows_network(self.db, trust_zone_id)?
        {
            match run_hard_egress_helper(
                "allocate",
                &allocation_id,
                &backend,
                trust_zone_id,
                agent_id,
                task_id,
                None,
            ) {
                Ok(Some(enforcement)) => {
                    metadata["hardEgress"] = enforcement.clone();
                    self.db.record_audit(AuditInput {
                        actor_type: agent_id.map(|_| "agent".to_string()),
                        actor_id: agent_id.map(str::to_string),
                        action: "network_policy.hard_egress_enforced".to_string(),
                        object_type: Some("trust_zone".to_string()),
                        object_id: Some(trust_zone_id.to_string()),
                        payload: Some(json!({
                            "backend": backend.clone(),
                            "allocationId": allocation_id,
                            "enforcement": enforcement,
                        })),
                    })?;
                }
                Ok(None) => {
                    return Err(SparseKernelError::Denied(
                        "sandbox requires host-level egress enforcement: helper returned no enforcement"
                            .to_string(),
                    ));
                }
                Err(err) => {
                    let reason = err.to_string();
                    self.db.record_audit(AuditInput {
                        actor_type: agent_id.map(|_| "agent".to_string()),
                        actor_id: agent_id.map(str::to_string),
                        action: "network_policy.hard_egress_unavailable".to_string(),
                        object_type: Some("trust_zone".to_string()),
                        object_id: Some(trust_zone_id.to_string()),
                        payload: Some(json!({ "backend": backend.clone(), "reason": reason })),
                    })?;
                    return Err(SparseKernelError::Denied(format!(
                        "sandbox requires host-level egress enforcement: {reason}"
                    )));
                }
            }
        }
        self.db.conn.execute(
            "INSERT INTO resource_leases(id, resource_type, resource_id, owner_task_id, owner_agent_id, trust_zone_id, status, metadata_json, created_at, updated_at)
             VALUES(?, 'sandbox', ?, ?, ?, ?, 'active', ?, ?, ?)",
            params![
                allocation_id,
                allocation_id,
                task_id,
                agent_id,
                trust_zone_id,
                metadata.to_string(),
                now,
                now
            ],
        )?;
        self.db.record_audit(AuditInput {
            actor_type: agent_id.map(|_| "agent".to_string()),
            actor_id: agent_id.map(str::to_string),
            action: "sandbox.allocated".to_string(),
            object_type: Some("resource_lease".to_string()),
            object_id: Some(allocation_id.clone()),
            payload: Some(json!({ "trustZoneId": trust_zone_id, "backend": backend.clone() })),
        })?;
        Ok(SandboxAllocationRecord {
            id: allocation_id,
            task_id: task_id.map(str::to_string),
            trust_zone_id: trust_zone_id.to_string(),
            backend,
            status: "active".to_string(),
            created_at: now,
        })
    }

    fn release_sandbox(&self, allocation_id: &str) -> Result<bool> {
        let now = now_iso();
        let lease: Option<(Option<String>, Option<String>, Option<String>, Option<String>)> = self
            .db
            .conn
            .query_row(
                "SELECT trust_zone_id, owner_agent_id, owner_task_id, metadata_json FROM resource_leases WHERE id = ? AND resource_type = 'sandbox' AND status = 'active'",
                params![allocation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        if let Some((trust_zone_id, owner_agent_id, owner_task_id, Some(raw_metadata))) =
            lease.as_ref()
        {
            let parsed: Value = serde_json::from_str(raw_metadata)?;
            if let Some(enforcement) = parsed.get("hardEgress") {
                let backend = parsed
                    .get("backend")
                    .and_then(Value::as_str)
                    .unwrap_or("local/no_isolation");
                if let Err(err) = run_hard_egress_helper(
                    "release",
                    allocation_id,
                    backend,
                    trust_zone_id.as_deref().unwrap_or(""),
                    owner_agent_id.as_deref(),
                    owner_task_id.as_deref(),
                    Some(enforcement),
                ) {
                    self.db.record_audit(AuditInput {
                        actor_type: Some("runtime".to_string()),
                        actor_id: None,
                        action: "network_policy.hard_egress_release_failed".to_string(),
                        object_type: Some("resource_lease".to_string()),
                        object_id: Some(allocation_id.to_string()),
                        payload: Some(json!({
                            "reason": err.to_string(),
                            "enforcement": enforcement,
                        })),
                    })?;
                    return Err(err);
                }
            }
        }
        let updated = self.db.conn.execute(
            "UPDATE resource_leases SET status = 'released', updated_at = ? WHERE id = ? AND resource_type = 'sandbox' AND status = 'active'",
            params![now, allocation_id],
        )?;
        if updated > 0 {
            self.db.record_audit(AuditInput {
                actor_type: Some("runtime".to_string()),
                actor_id: None,
                action: "sandbox.released".to_string(),
                object_type: Some("resource_lease".to_string()),
                object_id: Some(allocation_id.to_string()),
                payload: None,
            })?;
        }
        Ok(updated > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use tempfile::tempdir;

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn temp_db() -> (tempfile::TempDir, SparseKernelDb) {
        let dir = tempdir().expect("temp dir");
        let db = SparseKernelDb::open(dir.path().join("runtime.sqlite")).expect("db");
        (dir, db)
    }

    #[test]
    fn migrates_empty_db_idempotently() {
        let (_dir, db) = temp_db();
        assert_eq!(db.schema_version().unwrap(), SPARSEKERNEL_SCHEMA_VERSION);
        db.migrate().unwrap();
        assert_eq!(db.schema_version().unwrap(), SPARSEKERNEL_SCHEMA_VERSION);
        assert_eq!(db.inspect().unwrap().counts["trust_zones"], 7);
    }

    #[test]
    fn session_upsert_creates_agent_and_supports_tool_call_foreign_keys() {
        let (_dir, mut db) = temp_db();
        let session = db
            .upsert_session(UpsertSessionInput {
                id: "session-a".to_string(),
                agent_id: "agent-a".to_string(),
                session_key: Some("agent:agent-a:main".to_string()),
                channel: Some("telegram".to_string()),
                status: Some("active".to_string()),
                current_token_count: Some(42),
                last_activity_at: Some("2026-04-27T00:00:00Z".to_string()),
            })
            .unwrap();
        assert_eq!(session.agent_id, "agent-a");
        assert_eq!(session.current_token_count, 42);
        assert_eq!(db.list_sessions(10).unwrap().len(), 1);
        let first_event = db
            .append_transcript_event(AppendTranscriptEventInput {
                session_id: "session-a".to_string(),
                parent_event_id: None,
                role: "user".to_string(),
                event_type: "message".to_string(),
                content: Some(json!({ "text": "hello" })),
                tool_call_id: None,
                token_count: Some(3),
                created_at: Some("2026-04-27T00:00:00Z".to_string()),
            })
            .unwrap();
        let second_event = db
            .append_transcript_event(AppendTranscriptEventInput {
                session_id: "session-a".to_string(),
                parent_event_id: Some(first_event.id),
                role: "assistant".to_string(),
                event_type: "message".to_string(),
                content: Some(json!({ "text": "hi" })),
                tool_call_id: None,
                token_count: None,
                created_at: None,
            })
            .unwrap();
        assert_eq!(first_event.seq, 1);
        assert_eq!(second_event.seq, 2);
        assert_eq!(db.list_transcript_events("session-a", 10).unwrap().len(), 2);

        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "agent-a".to_string(),
            resource_type: "tool".to_string(),
            resource_id: Some("demo".to_string()),
            action: "invoke".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();
        let call = db
            .create_tool_call(CreateToolCallInput {
                id: Some("tool-call-session".to_string()),
                task_id: None,
                session_id: Some("session-a".to_string()),
                agent_id: Some("agent-a".to_string()),
                tool_name: "demo".to_string(),
                input: None,
            })
            .unwrap();
        assert_eq!(call.session_id.as_deref(), Some("session-a"));
        let actions: Vec<String> = db
            .list_audit(10)
            .unwrap()
            .into_iter()
            .map(|event| event.action)
            .collect();
        assert!(actions.contains(&"session.upserted".to_string()));
    }

    #[test]
    fn task_claiming_is_atomic_and_expired_leases_recover() {
        let (_dir, mut db) = temp_db();
        db.enqueue_task(EnqueueTaskInput {
            id: Some("task-a".to_string()),
            agent_id: None,
            session_id: None,
            kind: "demo".to_string(),
            priority: 1,
            idempotency_key: None,
            input: Some(json!({ "hello": true })),
        })
        .unwrap();
        db.enqueue_task(EnqueueTaskInput {
            id: Some("task-b".to_string()),
            agent_id: None,
            session_id: None,
            kind: "demo".to_string(),
            priority: 10,
            idempotency_key: None,
            input: Some(json!({ "hello": "second" })),
        })
        .unwrap();
        let claimed_by_id = db
            .claim_task("task-a", "worker-id", 60)
            .unwrap()
            .expect("claimed by id");
        assert_eq!(claimed_by_id.id, "task-a");
        assert_eq!(claimed_by_id.lease_owner.as_deref(), Some("worker-id"));
        assert!(db
            .claim_task("task-a", "worker-other", 60)
            .unwrap()
            .is_none());
        assert!(db.complete_task("task-a", "worker-id", None).unwrap());
        let claimed = db
            .claim_next_task("worker-a", &[], 60)
            .unwrap()
            .expect("claimed");
        assert_eq!(claimed.id, "task-b");
        assert_eq!(claimed.status, "running");
        assert!(db.claim_next_task("worker-b", &[], 60).unwrap().is_none());
        let future = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
        assert_eq!(db.release_expired_leases(&future).unwrap().0, 1);
        let reclaimed = db.claim_next_task("worker-b", &[], 60).unwrap().unwrap();
        assert_eq!(reclaimed.lease_owner.as_deref(), Some("worker-b"));
        assert!(db.complete_task("task-b", "worker-b", None).unwrap());
        assert_eq!(db.get_task("task-b").unwrap().status, "completed");
    }

    #[test]
    fn artifact_store_dedupes_and_enforces_access() {
        let (dir, db) = temp_db();
        let store = ArtifactStore::new(&db, dir.path().join("artifacts"));
        let first = store
            .write(
                b"hello",
                Some("text/plain"),
                Some("session"),
                Some(("agent", "main", "read")),
            )
            .unwrap();
        let second = store
            .write(b"hello", Some("text/plain"), Some("session"), None)
            .unwrap();
        let import_path = dir.path().join("import.txt");
        fs::write(&import_path, b"hello").unwrap();
        let imported = store
            .import_file(&import_path, Some("text/plain"), Some("session"), None)
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(first.id, imported.id);
        assert_eq!(
            store
                .read(&first.id, Some(("agent", "main", "read")))
                .unwrap(),
            b"hello"
        );
        assert!(store
            .read(&first.id, Some(("agent", "other", "read")))
            .is_err());
        let export_path = dir.path().join("export.txt");
        store
            .export_file(&first.id, &export_path, Some(("agent", "main", "read")))
            .unwrap();
        assert_eq!(fs::read(export_path).unwrap(), b"hello");
    }

    #[test]
    fn capabilities_allow_deny_revoke_and_audit() {
        let (_dir, db) = temp_db();
        let check = CapabilityCheck {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "tool".to_string(),
            resource_id: Some("exec".to_string()),
            action: "invoke".to_string(),
            context: None,
            audit_denied: true,
        };
        assert!(!db.check_capability(check.clone()).unwrap());
        let cap = db
            .grant_capability(GrantCapabilityInput {
                subject_type: "agent".to_string(),
                subject_id: "main".to_string(),
                resource_type: "tool".to_string(),
                resource_id: Some("exec".to_string()),
                action: "invoke".to_string(),
                constraints: None,
                expires_at: None,
            })
            .unwrap();
        assert!(db.check_capability(check.clone()).unwrap());
        assert_eq!(db.list_capabilities("agent", "main").unwrap().len(), 1);
        assert!(db.revoke_capability(&cap.id).unwrap());
        assert!(!db.check_capability(check).unwrap());
        let actions: Vec<String> = db
            .list_audit(10)
            .unwrap()
            .into_iter()
            .map(|event| event.action)
            .collect();
        assert!(actions.contains(&"capability.denied".to_string()));
        assert!(actions.contains(&"capability.granted".to_string()));
        assert!(actions.contains(&"capability.revoked".to_string()));
    }

    #[test]
    fn tool_broker_checks_capability_tracks_artifacts_and_audits() {
        let (dir, db) = temp_db();
        db.enqueue_task(EnqueueTaskInput {
            id: Some("task-a".to_string()),
            agent_id: None,
            session_id: None,
            kind: "browser_capture".to_string(),
            priority: 0,
            idempotency_key: None,
            input: None,
        })
        .unwrap();
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "tool".to_string(),
            resource_id: Some("browser.capture".to_string()),
            action: "invoke".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();

        let broker = LedgerToolBroker { db: &db };
        let call = broker
            .create_call(CreateToolCallInput {
                id: Some("tool-call-a".to_string()),
                task_id: Some("task-a".to_string()),
                session_id: None,
                agent_id: Some("main".to_string()),
                tool_name: "browser.capture".to_string(),
                input: Some(json!({ "url": "https://example.com" })),
            })
            .unwrap();
        assert_eq!(call.status, "created");

        let denied = broker.create_call(CreateToolCallInput {
            id: Some("tool-call-denied".to_string()),
            task_id: Some("task-a".to_string()),
            session_id: None,
            agent_id: Some("main".to_string()),
            tool_name: "exec".to_string(),
            input: None,
        });
        assert!(denied.is_err());

        let running = broker.start_call(&call.id).unwrap();
        assert_eq!(running.status, "running");
        let artifact = ArtifactStore::new(&db, dir.path().join("artifacts"))
            .write(b"pixels", Some("image/png"), Some("debug"), None)
            .unwrap();
        let completed = broker
            .complete_call(
                &call.id,
                CompleteToolCallInput {
                    output: Some(json!({ "ok": true })),
                    artifact_ids: vec![artifact.id.clone()],
                },
            )
            .unwrap();
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.output.unwrap()["artifact_ids"][0], artifact.id);

        let actions: Vec<String> = db
            .list_audit(20)
            .unwrap()
            .into_iter()
            .map(|event| event.action)
            .collect();
        assert!(actions.contains(&"tool_call.created".to_string()));
        assert!(actions.contains(&"tool_call.denied".to_string()));
        assert!(actions.contains(&"tool_call.started".to_string()));
        assert!(actions.contains(&"tool_call.completed".to_string()));
    }

    #[test]
    fn browser_endpoint_probe_reads_loopback_cdp_version() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).unwrap();
            let body = r#"{"Browser":"Chrome/123.0","webSocketDebuggerUrl":"ws://127.0.0.1/devtools/browser/test"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let probe = probe_browser_endpoint(&format!("http://{addr}"));
        server.join().unwrap();

        assert!(probe.reachable);
        assert_eq!(probe.status_code, Some(200));
        assert_eq!(probe.browser.as_deref(), Some("Chrome/123.0"));
        assert_eq!(
            probe.web_socket_debugger_url.as_deref(),
            Some("ws://127.0.0.1/devtools/browser/test")
        );
    }

    #[test]
    fn browser_endpoint_probe_rejects_non_loopback_cdp() {
        let probe = probe_browser_endpoint("http://10.0.0.8:9222");
        assert!(!probe.reachable);
        assert!(probe.error.unwrap().contains("loopback"));
    }

    #[test]
    fn mock_browser_and_sandbox_brokers_allocate_and_release() {
        let (_dir, db) = temp_db();
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "browser_context".to_string(),
            resource_id: Some("public_web".to_string()),
            action: "allocate".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();
        db.grant_capability(GrantCapabilityInput {
            subject_type: "agent".to_string(),
            subject_id: "main".to_string(),
            resource_type: "sandbox".to_string(),
            resource_id: Some("code_execution".to_string()),
            action: "allocate".to_string(),
            constraints: None,
            expires_at: None,
        })
        .unwrap();
        let browser = MockBrowserBroker { db: &db };
        let context = browser
            .acquire_context(
                Some("main"),
                None,
                None,
                "public_web",
                1,
                Some("http://127.0.0.1:9222"),
            )
            .unwrap();
        let pools = db.list_browser_pools().unwrap();
        assert_eq!(pools[0].browser_kind, "cdp");
        assert_eq!(
            pools[0].cdp_endpoint.as_deref(),
            Some("http://127.0.0.1:9222")
        );
        assert!(browser
            .acquire_context(Some("main"), None, None, "public_web", 1, None)
            .is_err());
        assert!(browser.release_context(&context.id).unwrap());
        let sandbox = LocalSandboxBroker { db: &db };
        let allocation = sandbox
            .allocate_sandbox(Some("main"), None, "code_execution", None)
            .unwrap();
        assert_eq!(allocation.backend, "local/no_isolation");
        assert!(sandbox.release_sandbox(&allocation.id).unwrap());
    }

    #[test]
    fn sandbox_hard_egress_mode_fails_closed_for_network_allowing_zones() {
        let _guard = env_lock();
        let (_dir, db) = temp_db();
        let previous = env::var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS").ok();
        let previous_helper = env::var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER").ok();
        let previous_helper_args =
            env::var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS").ok();
        env::set_var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS", "1");
        env::remove_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER");
        env::remove_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS");
        let result = LocalSandboxBroker { db: &db }.allocate_sandbox(
            None,
            None,
            "public_web",
            Some("local/no_isolation"),
        );
        match previous {
            Some(value) => env::set_var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS", value),
            None => env::remove_var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS"),
        }
        match previous_helper {
            Some(value) => env::set_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER", value),
            None => env::remove_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER"),
        }
        match previous_helper_args {
            Some(value) => env::set_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS", value),
            None => env::remove_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS"),
        }
        assert!(result.is_err());
        let audit = db.list_audit(1).unwrap();
        assert_eq!(audit[0].action, "network_policy.hard_egress_unavailable");
        assert_eq!(audit[0].object_id.as_deref(), Some("public_web"));
    }

    #[test]
    fn sandbox_hard_egress_helper_allows_network_allocations() {
        if !Path::new("/bin/sh").exists() {
            return;
        }
        let _guard = env_lock();
        let (_dir, db) = temp_db();
        let previous = env::var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS").ok();
        let previous_helper = env::var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER").ok();
        let previous_helper_args =
            env::var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS").ok();
        env::set_var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS", "1");
        env::set_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER", "/bin/sh");
        env::set_var(
            "OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS",
            serde_json::to_string(&vec![
                "-c",
                "cat >/dev/null; printf '{\"ok\":true,\"enforcementId\":\"fw-test\",\"boundary\":\"host_firewall\"}'",
            ])
            .unwrap(),
        );
        let sandbox = LocalSandboxBroker { db: &db };
        let allocation =
            sandbox.allocate_sandbox(None, None, "public_web", Some("local/no_isolation"));
        let release_result = allocation
            .as_ref()
            .ok()
            .map(|allocation| sandbox.release_sandbox(&allocation.id));
        let audit = db.list_audit(5).unwrap();
        match previous {
            Some(value) => env::set_var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS", value),
            None => env::remove_var("OPENCLAW_RUNTIME_SANDBOX_REQUIRE_HARD_EGRESS"),
        }
        match previous_helper {
            Some(value) => env::set_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER", value),
            None => env::remove_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER"),
        }
        match previous_helper_args {
            Some(value) => env::set_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS", value),
            None => env::remove_var("OPENCLAW_RUNTIME_SANDBOX_HARD_EGRESS_HELPER_ARGS"),
        }
        let allocation = allocation.unwrap();
        assert_eq!(allocation.backend, "local/no_isolation");
        assert!(audit
            .iter()
            .any(|entry| entry.action == "network_policy.hard_egress_enforced"));
        assert!(release_result.unwrap().unwrap());
    }
}
