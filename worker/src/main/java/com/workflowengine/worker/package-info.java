/**
 * Out-of-process activity worker. One JVM runs all five ORDER stubs.
 *
 * <p>Kafka is transport. This process does not own workflow rows. Phase 6
 * stores a finished attempt in this process's own database, keyed by
 * {@code (workflowId, stepName, attempt)}. A redelivery publishes that result
 * and does not execute again. The stubs still return canned JSON.
 */
package com.workflowengine.worker;
