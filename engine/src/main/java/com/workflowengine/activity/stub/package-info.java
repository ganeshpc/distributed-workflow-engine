/**
 * Demo activity adapters. They are not business services and must not grow
 * order, inventory, or payment tables.
 *
 * <p><strong>{@code failAt}</strong> is a demo failure-injection backdoor:
 * if workflow input JSON contains {@code "failAt":"<STEP_NAME>"} the matching
 * stub returns {@code STUB_FORCED_FAILURE}. Unknown {@code failAt} values are
 * ignored and every step succeeds. Future workers ignore {@code failAt}
 * unless {@code spring.profiles.active=test}.
 *
 * <p>{@link com.workflowengine.activity.stub.ActivityBlockHook} and
 * {@link com.workflowengine.activity.stub.StubInvocationCounters} exist so
 * durability tests can prove commit-before-invoke without mocking Postgres.
 */
package com.workflowengine.activity.stub;
