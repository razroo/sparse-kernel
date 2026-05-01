ALTER TABLE resource_leases ADD COLUMN metadata_json TEXT;

CREATE INDEX IF NOT EXISTS resource_leases_owner_status_updated_idx ON resource_leases(owner_agent_id, status, updated_at);
