ALTER TABLE workflow_step
    ADD COLUMN deadline_at TIMESTAMPTZ;

CREATE INDEX idx_workflow_step_deadline_at
    ON workflow_step (deadline_at)
    WHERE deadline_at IS NOT NULL;
