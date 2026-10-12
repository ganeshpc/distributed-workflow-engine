-- Phase 13: append-only history for one execution. The current rows stay the
-- projection that GET reads. Event identity is (workflow_instance_id, event_id).

CREATE TABLE workflow_event (
    workflow_instance_id  UUID         NOT NULL
        REFERENCES workflow_instance (id),
    event_id              BIGINT       NOT NULL,
    event_type            VARCHAR(64)  NOT NULL,
    step_name             VARCHAR(64),
    step_position         INT,
    attempt               INT,
    instance_status       VARCHAR(32)  NOT NULL,
    step_status           VARCHAR(32),
    error                 TEXT,
    detail                TEXT,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_instance_id, event_id),
    CONSTRAINT chk_workflow_event_id CHECK (event_id >= 1)
);
