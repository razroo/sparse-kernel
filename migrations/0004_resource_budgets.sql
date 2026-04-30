INSERT OR IGNORE INTO runtime_info(key, value, updated_at)
VALUES
  ('resource_budget.logical_agents_max', '500', datetime('now')),
  ('resource_budget.active_agent_steps_max', '100', datetime('now')),
  ('resource_budget.model_calls_in_flight_max', '50', datetime('now')),
  ('resource_budget.file_patch_jobs_max', '16', datetime('now')),
  ('resource_budget.test_jobs_max', '4', datetime('now')),
  ('resource_budget.browser_contexts_max', '2', datetime('now')),
  ('resource_budget.heavy_sandboxes_max', '1', datetime('now'));
