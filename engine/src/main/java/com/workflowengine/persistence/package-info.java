/**
 * JPA mapping of {@code workflow_instance} and {@code workflow_step}. Flyway
 * owns the schema; {@code ddl-auto} is {@code none}.
 *
 * <p>PostgreSQL is the source of truth for orchestration metadata. JSON is
 * {@code jsonb}. Timestamps are {@code timestamptz} assigned by the database.
 * Optimistic concurrency uses JPA {@code @Version} only.
 */
package com.workflowengine.persistence;
