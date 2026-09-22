/**
 * Demo activity adapters that run in the worker process. They are not business
 * services and must not grow order, inventory, or payment tables.
 *
 * <p><strong>{@code failAt}</strong> is a demo failure-injection backdoor.
 * The worker honors {@code "failAt":"<STEP_NAME>"} only when
 * {@code spring.profiles.active=test}. Otherwise the field is ignored and the
 * stub succeeds. Unknown values are ignored even in the test profile.
 *
 * <p>{@link com.workflowengine.worker.activity.ActivityBlockHook} and
 * {@link com.workflowengine.worker.activity.StubInvocationCounters} exist so
 * tests can prove the engine committed {@code RUNNING} before the worker
 * finished {@code execute}.
 */
package com.workflowengine.worker.activity;
