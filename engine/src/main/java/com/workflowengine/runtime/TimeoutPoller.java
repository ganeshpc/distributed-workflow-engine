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
 * Phase 3 step-timeout poller. Marks a {@code RUNNING} step {@code FAILED}
 * when {@code deadline_at} is due, with error {@code TIMED_OUT}.
 *
 * <p>Depends on {@link WorkflowExecutor#timeoutIfDue} for the step-fail
 * transaction and on {@link WorkflowInstanceRepository} for the scan. It does
 * not call {@link WorkflowDispatcher}. The dispatcher's inflight set would
 * hide a workflow whose activity is still running on a {@code workflow-*}
 * thread, which is the case this poller exists to close. The invoke is not
 * interrupted. A completion that arrives afterward is discarded because the
 * step is no longer {@code RUNNING}.
 *
 * <p>Runs on the scheduler thread and once at {@link ApplicationReadyEvent}.
 * One process. Singleton, no per-workflow state. {@code FAILED} and
 * {@code COMPLETED} are never selected. A null {@code deadline_at} is not due.
 * This is not a timer step; it only expires an attempt that is already
 * {@code RUNNING}.
 *
 * <p>Failure handling: {@link WorkflowExecutor#timeoutIfDue} logs
 * infrastructure failures and does not invent a second terminal status.
 * Disabling {@code workflow.timeout.enabled} stops this scan only. A later
 * running-resume of an already-due leftover still writes {@code TIMED_OUT}
 * instead of invoking.
 */
@Slf4j
@Component
public class TimeoutPoller {

    private final WorkflowInstanceRepository instances;
    private final WorkflowExecutor executor;
    private final Clock clock;
    private final boolean enabled;

    /**
     * @param instances {@code RUNNING} instance query with steps loaded
     * @param executor writer of the timeout step-fail transaction
     * @param clock same clock that stamped {@code deadline_at}
     * @param enabled when false, scans are no-ops
     */
    public TimeoutPoller(
            WorkflowInstanceRepository instances,
            WorkflowExecutor executor,
            Clock clock,
            @Value("${workflow.timeout.enabled:true}") boolean enabled
    ) {
        this.instances = instances;
        this.executor = executor;
        this.clock = clock;
        this.enabled = enabled;
    }

    /** Startup pass so a deadline that elapsed while the process was down is applied immediately. */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        scan();
    }

    /** Periodic pass. The delay is {@code workflow.timeout.interval-ms}, not the step timeout itself. */
    @Scheduled(fixedDelayString = "${workflow.timeout.interval-ms:2000}")
    public void scheduled() {
        scan();
    }

    /**
     * Loads {@code RUNNING} instances and times out each one whose current
     * attempt is due. Does not invoke an activity and does not increment
     * {@code attempt}.
     */
    public void scan() {
        if (!enabled) {
            return;
        }
        Instant now = clock.instant();
        List<WorkflowInstanceEntity> running = instances.findByStatusIn(List.of(WorkflowStatus.RUNNING));
        for (WorkflowInstanceEntity instance : running) {
            if (expired(instance, now)) {
                log.info("timeout due workflowId={} type={} status={} version={}",
                        instance.getId(), instance.getType(), instance.getStatus(), instance.getVersion());
                executor.timeoutIfDue(instance.getId());
            }
        }
    }

    /**
     * True when the linear walk has a {@code RUNNING} step whose deadline is due.
     *
     * @param instance aggregate with steps loaded
     * @param now engine clock instant for this pass
     * @return false when every running step has a null or future deadline
     */
    private static boolean expired(WorkflowInstanceEntity instance, Instant now) {
        return instance.getSteps().stream()
                .filter(step -> step.getStatus() == StepStatus.RUNNING)
                .map(WorkflowStepEntity::getDeadlineAt)
                .anyMatch(deadline -> WorkflowExecutor.deadlineDue(deadline, now));
    }
}
