/**
 * JDK-only public contracts for the workflow engine: statuses, snapshots, start
 * commands, and the activity port.
 *
 * <p>This module has no Spring, JPA, Jackson, or Lombok dependencies. JSON
 * travels as UTF-8 {@link java.lang.String} text so a future worker process can
 * depend on this jar without the Boot application. HTTP JSON records stay in
 * {@code com.workflowengine.web}.
 *
 * <p>Orchestration metadata is eventually consistent with business systems;
 * these types describe engine state only, not orders or payments.
 */
package com.workflowengine.api;
