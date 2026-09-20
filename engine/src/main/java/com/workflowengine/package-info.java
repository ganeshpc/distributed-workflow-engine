/**
 * Phase 1 Spring Boot process for the durable current-state orchestrator.
 *
 * <p>The engine owns workflow and step lifecycle in PostgreSQL. It does not
 * own orders, inventory, payments, shipments, or notifications. Business
 * operations run through the {@link com.workflowengine.api.activity.Activity}
 * port; Phase 1 implementations are in-process stubs.
 *
 * <p>Engine metadata is transactionally consistent. The business saga those
 * activities represent is only eventually consistent with the systems they
 * will one day call.
 */
package com.workflowengine;
