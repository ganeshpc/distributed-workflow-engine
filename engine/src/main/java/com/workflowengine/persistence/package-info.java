/**
 * JPA mapping of {@code workflow_instance} and {@code workflow_step}. Flyway
 * owns the schema; {@code ddl-auto} is {@code none}.
 *
 * <p>PostgreSQL is the source of truth for orchestration metadata. JSON is
 * {@code jsonb}. {@code started_at}, {@code completed_at}, {@code created_at},
 * and {@code updated_at} are {@code timestamptz} values assigned by the database.
 * {@code next_attempt_at} and {@code deadline_at} are instants written from the
 * engine {@code Clock}. Optimistic concurrency uses JPA {@code @Version} only.
 */
package com.workflowengine.persistence;
