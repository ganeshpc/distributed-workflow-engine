/**
 * Out-of-process activity worker. One JVM runs all five ORDER stubs.
 *
 * <p>Kafka is transport. This process does not own workflow rows. Phase 6
 * will add an idempotency store so a redelivered task does not repeat a
 * real side effect. The stubs here still have no side effects.
 */
package com.workflowengine.worker;
