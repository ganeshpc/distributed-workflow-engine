/**
 * Spring beans that are not domain types: the workflow {@code TaskExecutor},
 * recovery and timeout scheduler, and OpenAPI / Scalar UI.
 *
 * <p>Keep this package small. A new {@code @Bean} needs a reason the
 * auto-configuration does not already cover.
 */
package com.workflowengine.config;
