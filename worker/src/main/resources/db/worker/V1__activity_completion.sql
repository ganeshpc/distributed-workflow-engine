-- Worker-owned record that an attempt already finished.
-- The engine database does not contain this table.
-- Redelivery of the same (workflow_id, step_name, attempt) republishes this row
-- and does not run the activity again.

CREATE TABLE activity_completion (
    workflow_id  UUID         NOT NULL,
    step_name    VARCHAR(128) NOT NULL,
    attempt      INTEGER      NOT NULL,
    success      BOOLEAN      NOT NULL,
    output_json  TEXT,
    error        TEXT,
    completed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, step_name, attempt)
);
