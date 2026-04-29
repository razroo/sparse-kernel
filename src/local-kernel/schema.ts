export type KernelMigration = {
  version: number;
  statements: readonly string[];
};

export const LOCAL_KERNEL_SCHEMA_VERSION = 4;

const createBaseSchema = `
CREATE TABLE IF NOT EXISTS schema_migrations(
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents(
  id TEXT PRIMARY KEY,
  name TEXT,
  role TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions(
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  session_key TEXT,
  channel TEXT,
  status TEXT NOT NULL,
  current_token_count INTEGER NOT NULL DEFAULT 0,
  compacted_until_event_id INTEGER,
  last_activity_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);

CREATE TABLE IF NOT EXISTS transcript_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  parent_event_id INTEGER,
  seq INTEGER NOT NULL,
  role TEXT NOT NULL,
  event_type TEXT NOT NULL,
  content_json TEXT,
  tool_call_id TEXT,
  token_count INTEGER,
  created_at TEXT NOT NULL,
  UNIQUE(session_id, seq),
  FOREIGN KEY(session_id) REFERENCES sessions(id),
  FOREIGN KEY(parent_event_id) REFERENCES transcript_events(id)
);

CREATE TABLE IF NOT EXISTS session_summaries(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  from_event_id INTEGER NOT NULL,
  to_event_id INTEGER NOT NULL,
  summary_text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS messages(
  id TEXT PRIMARY KEY,
  from_agent_id TEXT,
  to_agent_id TEXT,
  session_id TEXT,
  topic TEXT,
  payload_json TEXT NOT NULL,
  classification TEXT,
  created_at TEXT NOT NULL,
  read_at TEXT,
  FOREIGN KEY(from_agent_id) REFERENCES agents(id),
  FOREIGN KEY(to_agent_id) REFERENCES agents(id),
  FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS tasks(
  id TEXT PRIMARY KEY,
  agent_id TEXT,
  session_id TEXT,
  kind TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  idempotency_key TEXT,
  lease_owner TEXT,
  lease_until TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  input_json TEXT,
  result_artifact_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(agent_id) REFERENCES agents(id),
  FOREIGN KEY(session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS task_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(id)
);

CREATE TABLE IF NOT EXISTS tool_calls(
  id TEXT PRIMARY KEY,
  task_id TEXT,
  session_id TEXT,
  agent_id TEXT,
  tool_name TEXT NOT NULL,
  status TEXT NOT NULL,
  input_json TEXT,
  output_json TEXT,
  error TEXT,
  started_at TEXT,
  ended_at TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(task_id) REFERENCES tasks(id),
  FOREIGN KEY(session_id) REFERENCES sessions(id),
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);

CREATE TABLE IF NOT EXISTS network_policies(
  id TEXT PRIMARY KEY,
  default_action TEXT NOT NULL,
  allow_private_network INTEGER NOT NULL DEFAULT 0,
  allowed_hosts_json TEXT,
  denied_cidrs_json TEXT,
  proxy_ref TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trust_zones(
  id TEXT PRIMARY KEY,
  description TEXT,
  sandbox_backend TEXT NOT NULL,
  network_policy_id TEXT,
  filesystem_policy_json TEXT,
  max_processes INTEGER,
  max_memory_mb INTEGER,
  max_runtime_seconds INTEGER,
  created_at TEXT NOT NULL,
  FOREIGN KEY(network_policy_id) REFERENCES network_policies(id)
);

CREATE TABLE IF NOT EXISTS resource_leases(
  id TEXT PRIMARY KEY,
  resource_type TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  owner_task_id TEXT,
  owner_agent_id TEXT,
  trust_zone_id TEXT,
  status TEXT NOT NULL,
  lease_until TEXT,
  max_runtime_ms INTEGER,
  max_bytes_out INTEGER,
  max_tokens INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(owner_task_id) REFERENCES tasks(id),
  FOREIGN KEY(owner_agent_id) REFERENCES agents(id),
  FOREIGN KEY(trust_zone_id) REFERENCES trust_zones(id)
);

CREATE TABLE IF NOT EXISTS browser_pools(
  id TEXT PRIMARY KEY,
  trust_zone_id TEXT NOT NULL,
  browser_kind TEXT NOT NULL,
  status TEXT NOT NULL,
  max_contexts INTEGER NOT NULL,
  cdp_endpoint TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(trust_zone_id) REFERENCES trust_zones(id)
);

CREATE TABLE IF NOT EXISTS browser_contexts(
  id TEXT PRIMARY KEY,
  pool_id TEXT NOT NULL,
  agent_id TEXT,
  session_id TEXT,
  task_id TEXT,
  profile_mode TEXT NOT NULL,
  allowed_origins_json TEXT,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT,
  FOREIGN KEY(pool_id) REFERENCES browser_pools(id),
  FOREIGN KEY(agent_id) REFERENCES agents(id),
  FOREIGN KEY(session_id) REFERENCES sessions(id),
  FOREIGN KEY(task_id) REFERENCES tasks(id)
);

CREATE TABLE IF NOT EXISTS artifacts(
  id TEXT PRIMARY KEY,
  sha256 TEXT NOT NULL UNIQUE,
  mime_type TEXT,
  size_bytes INTEGER NOT NULL,
  storage_ref TEXT NOT NULL,
  created_by_task_id TEXT,
  created_by_tool_call_id TEXT,
  classification TEXT,
  retention_policy TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(created_by_task_id) REFERENCES tasks(id),
  FOREIGN KEY(created_by_tool_call_id) REFERENCES tool_calls(id)
);

CREATE TABLE IF NOT EXISTS artifact_access(
  artifact_id TEXT NOT NULL,
  subject_type TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  permission TEXT NOT NULL,
  expires_at TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(artifact_id, subject_type, subject_id, permission),
  FOREIGN KEY(artifact_id) REFERENCES artifacts(id)
);

CREATE TABLE IF NOT EXISTS capabilities(
  id TEXT PRIMARY KEY,
  subject_type TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id TEXT,
  action TEXT NOT NULL,
  constraints_json TEXT,
  expires_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor_type TEXT,
  actor_id TEXT,
  action TEXT NOT NULL,
  object_type TEXT,
  object_id TEXT,
  payload_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usage_records(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id TEXT,
  session_id TEXT,
  task_id TEXT,
  resource_type TEXT NOT NULL,
  amount INTEGER NOT NULL,
  unit TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(agent_id) REFERENCES agents(id),
  FOREIGN KEY(session_id) REFERENCES sessions(id),
  FOREIGN KEY(task_id) REFERENCES tasks(id)
);

CREATE INDEX IF NOT EXISTS transcript_events_session_seq_idx ON transcript_events(session_id, seq);
CREATE INDEX IF NOT EXISTS sessions_agent_activity_idx ON sessions(agent_id, last_activity_at);
CREATE INDEX IF NOT EXISTS tasks_status_priority_created_idx ON tasks(status, priority, created_at);
CREATE INDEX IF NOT EXISTS tasks_lease_until_idx ON tasks(lease_until);
CREATE INDEX IF NOT EXISTS tool_calls_session_idx ON tool_calls(session_id);
CREATE INDEX IF NOT EXISTS messages_to_read_idx ON messages(to_agent_id, read_at);
CREATE INDEX IF NOT EXISTS audit_log_created_idx ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS artifacts_sha256_idx ON artifacts(sha256);
CREATE INDEX IF NOT EXISTS resource_leases_resource_status_lease_idx ON resource_leases(resource_type, status, lease_until);
CREATE INDEX IF NOT EXISTS browser_contexts_pool_status_idx ON browser_contexts(pool_id, status);
CREATE INDEX IF NOT EXISTS capabilities_subject_resource_action_idx ON capabilities(subject_type, subject_id, resource_type, resource_id, action, expires_at);
`;

const seedDefaultTrustZones = `
INSERT OR IGNORE INTO network_policies(id, default_action, allow_private_network, allowed_hosts_json, denied_cidrs_json, proxy_ref, created_at)
VALUES
  ('deny_all', 'deny', 0, '[]', NULL, NULL, datetime('now')),
  ('public_web_default', 'allow', 0, '[]', NULL, NULL, datetime('now')),
  ('authenticated_web_default', 'allow', 0, '[]', NULL, NULL, datetime('now')),
  ('local_files_default', 'deny', 0, '[]', NULL, NULL, datetime('now')),
  ('code_execution_default', 'deny', 0, '[]', NULL, NULL, datetime('now'));

INSERT OR IGNORE INTO trust_zones(id, description, sandbox_backend, network_policy_id, filesystem_policy_json, max_processes, max_memory_mb, max_runtime_seconds, created_at)
VALUES
  ('public_web', 'Unauthenticated public web browsing and fetches. Route blocking is not a hard security boundary.', 'browser_context', 'public_web_default', '{"mode":"none"}', NULL, NULL, NULL, datetime('now')),
  ('authenticated_web', 'Authenticated browser work isolated by context/profile policy.', 'browser_context', 'authenticated_web_default', '{"mode":"none"}', NULL, NULL, NULL, datetime('now')),
  ('local_files_readonly', 'Read-only local file access mediated by tools.', 'local/no_isolation', 'local_files_default', '{"mode":"readonly"}', NULL, NULL, NULL, datetime('now')),
  ('local_files_rw', 'Read-write local file access mediated by tools.', 'local/no_isolation', 'local_files_default', '{"mode":"readwrite"}', NULL, NULL, NULL, datetime('now')),
  ('code_execution', 'Code execution requiring a backend with explicit isolation semantics.', 'local/no_isolation', 'code_execution_default', '{"mode":"workspace"}', NULL, NULL, NULL, datetime('now')),
  ('plugin_untrusted', 'Untrusted plugin execution target; local/no_isolation is accounting only, not isolation.', 'local/no_isolation', 'deny_all', '{"mode":"none"}', NULL, NULL, NULL, datetime('now')),
  ('user_browser_profile', 'User browser profile access; requires explicit capability and user trust.', 'browser_context', 'authenticated_web_default', '{"mode":"none"}', NULL, NULL, NULL, datetime('now'));
`;

const createSessionEntryMirror = `
CREATE TABLE IF NOT EXISTS session_entries(
  store_path TEXT NOT NULL,
  session_key TEXT NOT NULL,
  session_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  entry_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(store_path, session_key),
  FOREIGN KEY(session_id) REFERENCES sessions(id),
  FOREIGN KEY(agent_id) REFERENCES agents(id)
);

CREATE INDEX IF NOT EXISTS session_entries_session_idx ON session_entries(session_id);
CREATE INDEX IF NOT EXISTS session_entries_agent_updated_idx ON session_entries(agent_id, updated_at);
`;

const createBrowserTargetLedger = `
CREATE TABLE IF NOT EXISTS browser_targets(
  id TEXT PRIMARY KEY,
  context_id TEXT NOT NULL,
  target_id TEXT NOT NULL,
  opener_target_id TEXT,
  url TEXT,
  title TEXT,
  status TEXT NOT NULL,
  close_reason TEXT,
  console_count INTEGER NOT NULL DEFAULT 0,
  network_count INTEGER NOT NULL DEFAULT 0,
  artifact_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  closed_at TEXT,
  UNIQUE(context_id, target_id),
  FOREIGN KEY(context_id) REFERENCES browser_contexts(id)
);

CREATE TABLE IF NOT EXISTS browser_observations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  context_id TEXT NOT NULL,
  target_id TEXT,
  observation_type TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(context_id) REFERENCES browser_contexts(id)
);

CREATE INDEX IF NOT EXISTS browser_targets_context_status_idx ON browser_targets(context_id, status);
CREATE INDEX IF NOT EXISTS browser_targets_session_task_idx ON browser_targets(context_id, updated_at);
CREATE INDEX IF NOT EXISTS browser_observations_context_created_idx ON browser_observations(context_id, created_at);
CREATE INDEX IF NOT EXISTS browser_observations_target_created_idx ON browser_observations(context_id, target_id, created_at);
CREATE INDEX IF NOT EXISTS browser_observations_type_created_idx ON browser_observations(observation_type, created_at);
`;

const createRuntimeInfoAndLeaseMetadata = `
CREATE TABLE IF NOT EXISTS runtime_info(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

ALTER TABLE resource_leases ADD COLUMN metadata_json TEXT;

CREATE INDEX IF NOT EXISTS resource_leases_owner_status_updated_idx ON resource_leases(owner_agent_id, status, updated_at);
CREATE INDEX IF NOT EXISTS runtime_info_updated_idx ON runtime_info(updated_at);
`;

export const LOCAL_KERNEL_MIGRATIONS: readonly KernelMigration[] = [
  {
    version: 1,
    statements: [createBaseSchema, seedDefaultTrustZones],
  },
  {
    version: 2,
    statements: [createSessionEntryMirror],
  },
  {
    version: 3,
    statements: [createBrowserTargetLedger],
  },
  {
    version: 4,
    statements: [createRuntimeInfoAndLeaseMetadata],
  },
];
