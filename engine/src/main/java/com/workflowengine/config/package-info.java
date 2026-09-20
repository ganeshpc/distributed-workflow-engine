/**
 * Spring beans that are not domain types: the workflow {@code TaskExecutor},
 * recovery scheduler, and OpenAPI / Swagger UI.
 *
 * <p>Keep this package small. A new {@code @Bean} needs a reason the
 * auto-configuration does not already cover.
 */
package com.workflowengine.config;
