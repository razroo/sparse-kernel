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
