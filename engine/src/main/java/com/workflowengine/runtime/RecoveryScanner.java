package com.workflowengine.runtime;

import com.workflowengine.api.StepStatus;
import com.workflowengine.api.WorkflowStatus;
import com.workflowengine.persistence.WorkflowInstanceEntity;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import com.workflowengine.persistence.WorkflowStepEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * Phase 2 leftover scanner. Resumes never-started {@code PENDING} admissions
 * and due {@code RUNNING} steps. Does not touch {@code FAILED} or
 * {@code COMPLETED}.
 *
 * <p>Uses {@code idx_workflow_instance_status}. Idempotent POST still must
 * not submit the executor; this scanner is what continues a leftover after
 * crash. Same-process duplicates are filtered by {@link WorkflowDispatcher}.
 *
 * <p>Runs on the scheduler thread (and once at {@link ApplicationReadyEvent}).
 * Submit is asynchronous. {@link WorkflowExecutor} publishes a task; it does
 * not call the activity. A {@code RUNNING} step is republished at the same
 * attempt once {@code next_attempt_at} is due. A due {@code deadline_at} is
 * not submitted; {@link TimeoutPoller} fails it. An instance {@code RUNNING}
 * with no {@code RUNNING} step and a later {@code PENDING} step is submitted
 * so a crash between complete and the next step-start can continue.
 */
@Slf4j
@Component
public class RecoveryScanner {

    private final WorkflowInstanceRepository instances;
    private final WorkflowDispatcher dispatcher;
    private final Clock clock;
    private final boolean enabled;

    /**
     * @param instances leftover query
     * @param dispatcher submit-once per inflight id
     * @param clock backoff due-time
     * @param enabled when false, scans are no-ops (tests that plant leftovers)
     */
    public RecoveryScanner(
            WorkflowInstanceRepository instances,
            WorkflowDispatcher dispatcher,
            Clock clock,
            @Value("${workflow.recovery.enabled:true}") boolean enabled
    ) {
        this.instances = instances;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.enabled = enabled;
    }

    /** Startup pass so crash leftovers resume without waiting for the interval. */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        scan();
    }

    /** Periodic pass for backoff due times and lost submits in a live process. */
    @Scheduled(fixedDelayString = "${workflow.recovery.interval-ms:2000}")
    public void scheduled() {
        scan();
    }

    /**
     * Loads {@code PENDING} and {@code RUNNING} instances and submits those
     * that are due. {@code FAILED} is never in this query.
     */
    public void scan() {
        if (!enabled) {
            return;
        }
        Instant now = clock.instant();
        List<WorkflowInstanceEntity> leftovers = instances.findByStatusIn(
                List.of(WorkflowStatus.PENDING, WorkflowStatus.RUNNING, WorkflowStatus.COMPENSATING));
        for (WorkflowInstanceEntity instance : leftovers) {
            if (due(instance, now)) {
                log.info("recovery submit workflowId={} status={} version={}",
                        instance.getId(), instance.getStatus(), instance.getVersion());
                dispatcher.submit(instance.getId());
            }
        }
    }

    /**
     * Whether this leftover should be submitted. A due {@code deadline_at}
     * is excluded so the timeout poller is the writer for that row.
     *
     * @param instance aggregate with steps loaded
     * @param now engine clock instant for this pass
     * @return true when the scanner should submit
     */
    private static boolean due(WorkflowInstanceEntity instance, Instant now) {
        if (instance.getStatus() == WorkflowStatus.PENDING) {
            return instance.getSteps().stream().allMatch(step -> step.getStatus() == StepStatus.PENDING);
        }
        if (instance.getStatus() != WorkflowStatus.RUNNING
                && instance.getStatus() != WorkflowStatus.COMPENSATING) {
            return false;
        }
        boolean anyRunning = instance.getSteps().stream()
                .anyMatch(step -> step.getStatus() == StepStatus.RUNNING);
        if (!anyRunning) {
            return instance.getSteps().stream().anyMatch(step -> step.getStatus() == StepStatus.PENDING);
        }
        return instance.getSteps().stream()
                .filter(step -> step.getStatus() == StepStatus.RUNNING)
                .anyMatch(step -> isDue(step, now));
    }

    /**
     * Resume is due when the attempt has not timed out and backoff has elapsed.
     *
     * @param step a {@code RUNNING} step
     * @param now engine clock instant for this pass
     * @return false when {@code deadline_at} is due or {@code next_attempt_at} is still in the future
     */
    private static boolean isDue(WorkflowStepEntity step, Instant now) {
        if (WorkflowExecutor.deadlineDue(step.getDeadlineAt(), now)) {
            return false;
        }
        Instant next = step.getNextAttemptAt();
        return next == null || !next.isAfter(now);
    }
}
