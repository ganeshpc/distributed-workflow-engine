CREATE TABLE workflow_instance (
    id                  UUID PRIMARY KEY,
    type                VARCHAR(64)  NOT NULL,
    definition_version  INT          NOT NULL,
    status              VARCHAR(32)  NOT NULL,
    input_json          JSONB        NOT NULL,
    output_json         JSONB,
    current_step        VARCHAR(64),
    idempotency_key     VARCHAR(128) NOT NULL,
    version             INT          NOT NULL DEFAULT 0,
    error               TEXT,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_workflow_instance_status
      CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    CONSTRAINT chk_workflow_instance_def_ver
      CHECK (definition_version >= 1)
);

CREATE UNIQUE INDEX uq_workflow_instance_idempotency_key
    ON workflow_instance (idempotency_key);

CREATE INDEX idx_workflow_instance_status
    ON workflow_instance (status);

CREATE TABLE workflow_step (
    id                    UUID PRIMARY KEY,
    workflow_instance_id  UUID         NOT NULL
        REFERENCES workflow_instance (id),
    name                  VARCHAR(64)  NOT NULL,
    position              INT          NOT NULL,
    status                VARCHAR(32)  NOT NULL,
    attempt               INT          NOT NULL DEFAULT 0,
    input_json            JSONB,
    output_json           JSONB,
    error                 TEXT,
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    CONSTRAINT uq_workflow_step_instance_name
      UNIQUE (workflow_instance_id, name),
    CONSTRAINT uq_workflow_step_instance_position
      UNIQUE (workflow_instance_id, position),
    CONSTRAINT chk_workflow_step_status
      CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    CONSTRAINT chk_workflow_step_attempt
      CHECK (attempt >= 0),
    CONSTRAINT chk_workflow_step_position
      CHECK (position >= 0)
);

CREATE FUNCTION workflow_instance_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_workflow_instance_updated_at
    BEFORE UPDATE ON workflow_instance
    FOR EACH ROW
    EXECUTE FUNCTION workflow_instance_set_updated_at();
