-- PostgreSQL is the coordination authority. SQLite on a Google Drive mount
-- cannot provide reliable distributed locking across independent Colab hosts.
CREATE TABLE IF NOT EXISTS pipeline_runs (
 run_id text PRIMARY KEY, query_hash text NOT NULL, manifest jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS pipeline_instances (
 instance_id text PRIMARY KEY, run_id text NOT NULL REFERENCES pipeline_runs,
 shard_id integer NOT NULL, started_at timestamptz NOT NULL DEFAULT now(),
 last_heartbeat timestamptz NOT NULL DEFAULT now(), metrics jsonb NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS documents (
 document_id text PRIMARY KEY, canonical_title text NOT NULL DEFAULT '', source_url text NOT NULL DEFAULT '',
 source_metadata jsonb NOT NULL DEFAULT '{}', filename text NOT NULL DEFAULT '',
 download_status text NOT NULL DEFAULT 'READY', downloaded_bytes bigint NOT NULL DEFAULT 0,
 final_size bigint, sha256 text, storage_location text, document_attempt integer NOT NULL DEFAULT 0,
 fast_handoff_count integer NOT NULL DEFAULT 0, consecutive_failures integer NOT NULL DEFAULT 0,
 last_error text, last_route text, not_before timestamptz NOT NULL DEFAULT 'epoch',
 claimed_by text, claim_token bigint NOT NULL DEFAULT 0, lease_expires_at timestamptz,
 identity_sha text, ashrae_identity boolean, identity_result jsonb,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS query_document_results (
 query_hash text NOT NULL, document_id text NOT NULL REFERENCES documents,
 prompt_version text NOT NULL DEFAULT '', stage1_status text, stage2_query_relevant boolean,
 final_query_status text, validation_sha text, result jsonb,
 validation_owner text, validation_token bigint NOT NULL DEFAULT 0, validation_lease timestamptz,
 PRIMARY KEY(query_hash, document_id));
CREATE TABLE IF NOT EXISTS search_pages (
 query_hash text NOT NULL, page_number integer NOT NULL, assigned_shard integer NOT NULL,
 status text NOT NULL DEFAULT 'READY', claimed_by text, claim_token bigint NOT NULL DEFAULT 0,
 lease_expires_at timestamptz, started_at timestamptz, completed_at timestamptz,
 documents_found integer NOT NULL DEFAULT 0, last_error text,
 PRIMARY KEY(query_hash, page_number));
CREATE TABLE IF NOT EXISTS source_limits (
 host text PRIMARY KEY, not_before timestamptz NOT NULL DEFAULT '-infinity',
 next_start timestamptz NOT NULL DEFAULT '-infinity');
CREATE INDEX IF NOT EXISTS documents_recovery ON documents(download_status, not_before, lease_expires_at);
ALTER TABLE documents ALTER COLUMN not_before SET DEFAULT 'epoch';
UPDATE documents SET not_before='epoch' WHERE not_before='-infinity';
