package com.workflowengine.worker.idempotency;

import com.workflowengine.api.activity.ActivityCompletion;
import com.workflowengine.api.activity.ActivityContext;
import com.workflowengine.api.activity.ActivityResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

/**
 * Remembers that one attempt already produced a result.
 *
 * <p>Phase 6 worker store. {@link com.workflowengine.worker.TaskListener}
 * asks this type before it calls the activity. A hit is the result to
 * publish again. A miss means execute, then {@link #record} commits the row,
 * then the listener publishes. Execute is never inside this transaction.
 *
 * <p>The correlation id is {@code (workflowId, stepName, attempt)}. Two
 * listener threads can both miss and both execute; the primary key keeps one
 * row, and the loser publishes the stored winner. A crash after commit and
 * before publish is recovered by the next delivery, which finds the row and
 * does not execute. A crash during execute, before commit, still runs the
 * activity again. That window is the remaining at-least-once gap.
 *
 * <p>Spring singleton. Uses its own {@link TransactionTemplate}. A missing
 * database fails the insert and the listener does not acknowledge the task.
 */
@Component
public class ActivityIdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(ActivityIdempotencyStore.class);

    private final ActivityCompletionRepository completions;
    private final TransactionTemplate tx;

    /**
     * @param completions completion rows
     * @param transactionManager worker database transactions; not the engine's
     */
    public ActivityIdempotencyStore(
            ActivityCompletionRepository completions,
            PlatformTransactionManager transactionManager
    ) {
        this.completions = completions;
        this.tx = new TransactionTemplate(transactionManager);
    }

    /**
     * Loads a finished attempt.
     *
     * @param workflowId instance id; not null
     * @param stepName activity name; not null
     * @param attempt attempt from the task
     * @return the stored result, or empty when this attempt has not finished
     */
    public Optional<ActivityCompletion> find(UUID workflowId, String stepName, int attempt) {
        ActivityCompletionKey key = new ActivityCompletionKey(workflowId, stepName, attempt);
        ActivityCompletion found = tx.execute(status ->
                completions.findById(key).map(ActivityIdempotencyStore::toCompletion).orElse(null));
        return Optional.ofNullable(found);
    }

    /**
     * Commits the first result for this attempt.
     *
     * <p>Runs on the listener thread, after execute has returned, with no
     * activity call inside the transaction. A concurrent insert reloads the
     * winner and returns that row. The caller publishes the returned value.
     *
     * @param task task that ran; not null
     * @param result outcome of that run; not null
     * @return the row that is now durable, which may be an earlier winner
     */
    public ActivityCompletion record(ActivityContext task, ActivityResult result) {
        ActivityCompletionKey key = new ActivityCompletionKey(
                task.workflowId(), task.stepName(), task.attempt());
        try {
            ActivityCompletion stored = tx.execute(status -> {
                ActivityCompletionEntity entity = new ActivityCompletionEntity();
                entity.setWorkflowId(task.workflowId());
                entity.setStepName(task.stepName());
                entity.setAttempt(task.attempt());
                entity.setSuccess(result.success());
                entity.setOutputJson(result.outputJson());
                entity.setError(result.error());
                completions.saveAndFlush(entity);
                return toCompletion(entity);
            });
            log.info("completion stored workflowId={} step={} attempt={} success={}",
                    task.workflowId(), task.stepName(), task.attempt(), result.success());
            return stored;
        } catch (DataIntegrityViolationException ex) {
            ActivityCompletion winner = tx.execute(status ->
                    completions.findById(key).map(ActivityIdempotencyStore::toCompletion).orElse(null));
            if (winner == null) {
                throw ex;
            }
            log.info("completion already stored workflowId={} step={} attempt={}",
                    task.workflowId(), task.stepName(), task.attempt());
            return winner;
        }
    }

    private static ActivityCompletion toCompletion(ActivityCompletionEntity entity) {
        return new ActivityCompletion(
                entity.getWorkflowId(),
                entity.getStepName(),
                entity.getAttempt(),
                entity.isSuccess(),
                entity.getOutputJson(),
                entity.getError()
        );
    }
}
