package com.workflowengine.web;

import com.workflowengine.application.InvalidStartWorkflowException;
import com.workflowengine.application.WorkflowNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Maps request-thread exceptions to generic HTTP bodies. Executor-thread
 * failures never reach this type.
 *
 * <p>SQLSTATE, SQL text, and stack traces stay in logs. 503 is only for
 * database unavailability on the request thread. Payload-too-large is
 * unwrapped even when nested under a parse exception.
 *
 * <p>Spring singleton, request thread only.
 */
@Slf4j
@RestControllerAdvice
public class RestExceptionHandler {

    /**
     * @param ex validation failure
     * @return 400 {@code Bad Request}
     */
    @ExceptionHandler(InvalidStartWorkflowException.class)
    public ResponseEntity<ErrorResponse> badRequest(InvalidStartWorkflowException ex) {
        return ResponseEntity.badRequest().body(new ErrorResponse("Bad Request"));
    }

    @ExceptionHandler({
            HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class
    })
    /**
     * @param ex unreadable JSON or path type mismatch
     * @return 413 if nested too-large, otherwise 400
     */
    public ResponseEntity<ErrorResponse> malformed(Exception ex) {
        if (isPayloadTooLarge(ex)) {
            return tooLarge();
        }
        return ResponseEntity.badRequest().body(new ErrorResponse("Bad Request"));
    }

    @ExceptionHandler({WorkflowNotFoundException.class, NoResourceFoundException.class})
    /**
     * @param ex missing workflow or unmatched path
     * @return 404 {@code Not Found}
     */
    public ResponseEntity<ErrorResponse> notFound(Exception ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse("Not Found"));
    }

    @ExceptionHandler(PayloadTooLargeException.class)
    /**
     * @param ex body over 64 KB
     * @return 413 {@code Payload Too Large}
     */
    public ResponseEntity<ErrorResponse> payloadTooLarge(PayloadTooLargeException ex) {
        return tooLarge();
    }

    @ExceptionHandler({
            DataAccessResourceFailureException.class,
            CannotGetJdbcConnectionException.class,
            CannotCreateTransactionException.class
    })
    /**
     * @param ex database unreachable on the request thread
     * @return 503 {@code Service Unavailable}
     */
    public ResponseEntity<ErrorResponse> unavailable(Exception ex) {
        log.warn("database unreachable on request thread", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse("Service Unavailable"));
    }

    @ExceptionHandler(Exception.class)
    /**
     * @param ex any other request-thread error
     * @return 413 if nested too-large, otherwise 500
     */
    public ResponseEntity<ErrorResponse> unexpected(Exception ex) {
        if (isPayloadTooLarge(ex)) {
            return tooLarge();
        }
        log.error("unexpected request-thread error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("Internal Server Error"));
    }

    /** @return 413 with the generic payload-too-large body */
    private static ResponseEntity<ErrorResponse> tooLarge() {
        return ResponseEntity.status(HttpStatus.CONTENT_TOO_LARGE)
                .body(new ErrorResponse("Payload Too Large"));
    }

    /**
     * Walks the cause chain for {@link PayloadTooLargeException}.
     *
     * @param ex root throwable
     * @return true if 413 should win over 400/500
     */
    private static boolean isPayloadTooLarge(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof PayloadTooLargeException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
