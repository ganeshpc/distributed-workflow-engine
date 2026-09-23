-- Phase 7: instance COMPENSATING / COMPENSATED, and a forward step marked
-- COMPENSATED after its compensation activity succeeds.

ALTER TABLE workflow_instance DROP CONSTRAINT chk_workflow_instance_status;
ALTER TABLE workflow_instance ADD CONSTRAINT chk_workflow_instance_status
    CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'COMPENSATING', 'COMPENSATED'));

ALTER TABLE workflow_step DROP CONSTRAINT chk_workflow_step_status;
ALTER TABLE workflow_step ADD CONSTRAINT chk_workflow_step_status
    CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'COMPENSATED'));
