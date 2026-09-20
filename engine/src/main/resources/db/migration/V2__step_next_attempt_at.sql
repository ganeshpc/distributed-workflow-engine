ALTER TABLE workflow_step
    ADD COLUMN next_attempt_at TIMESTAMPTZ;

CREATE INDEX idx_workflow_step_next_attempt_at
    ON workflow_step (next_attempt_at)
    WHERE next_attempt_at IS NOT NULL;
