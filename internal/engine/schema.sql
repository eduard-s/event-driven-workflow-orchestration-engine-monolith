CREATE TABLE IF NOT EXISTS engine_control (
 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), owner uuid NOT NULL
);
CREATE TABLE IF NOT EXISTS executions (
 id uuid PRIMARY KEY, definition jsonb NOT NULL, policy jsonb NOT NULL,
 state text NOT NULL CHECK (state IN ('PENDING','RUNNING','COMPLETED','FAILED','EXPIRED')),
 trigger jsonb, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 accepted_at timestamptz, deadline_at timestamptz, terminal_at timestamptz,
 pending_until timestamptz NOT NULL, retention_until timestamptz,
 input_bytes bigint NOT NULL DEFAULT 0, output_bytes bigint NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS executions_active ON executions(state,created_at) WHERE state IN ('PENDING','RUNNING');
CREATE INDEX IF NOT EXISTS executions_pending_deadline ON executions(pending_until) WHERE state='PENDING';
CREATE INDEX IF NOT EXISTS executions_running_deadline ON executions(deadline_at) WHERE state='RUNNING';
CREATE INDEX IF NOT EXISTS executions_retention ON executions(retention_until) WHERE retention_until IS NOT NULL;
CREATE TABLE IF NOT EXISTS nodes (
 execution_id uuid NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
 node_id text NOT NULL, handler text NOT NULL, dependencies text[] NOT NULL,
 state text NOT NULL CHECK (state IN ('PENDING','RUNNING','COMPLETED','RETRY_WAIT','FAILED','BLOCKED','EXPIRED')),
 reason text NOT NULL DEFAULT 'WAITING_DEPENDENCIES', try_number integer NOT NULL DEFAULT 0,
 resolved_input jsonb, output jsonb, has_output boolean NOT NULL DEFAULT false,
 ready_at timestamptz NOT NULL DEFAULT clock_timestamp(), error jsonb,
 PRIMARY KEY(execution_id,node_id)
);
CREATE INDEX IF NOT EXISTS nodes_ready ON nodes(state,ready_at,execution_id) WHERE state IN ('PENDING','RETRY_WAIT');
CREATE TABLE IF NOT EXISTS tries (
 execution_id uuid NOT NULL, node_id text NOT NULL, try_number integer NOT NULL,
 owner uuid NOT NULL, started_at timestamptz NOT NULL, lease_until timestamptz NOT NULL,
 finished_at timestamptz, success boolean, error jsonb, runtime jsonb,
 PRIMARY KEY(execution_id,node_id,try_number),
 FOREIGN KEY(execution_id,node_id) REFERENCES nodes(execution_id,node_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS submission_keys (
 key text PRIMARY KEY, execution_id uuid NOT NULL REFERENCES executions(id) ON DELETE CASCADE
);
