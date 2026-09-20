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

@Slf4j
@RestControllerAdvice
public class RestExceptionHandler {

    @ExceptionHandler(InvalidStartWorkflowException.class)
    public ResponseEntity<ErrorResponse> badRequest(InvalidStartWorkflowException ex) {
        return ResponseEntity.badRequest().body(new ErrorResponse("Bad Request"));
    }

    @ExceptionHandler({
            HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class
    })
    public ResponseEntity<ErrorResponse> malformed(Exception ex) {
        if (isPayloadTooLarge(ex)) {
            return tooLarge();
        }
        return ResponseEntity.badRequest().body(new ErrorResponse("Bad Request"));
    }

    @ExceptionHandler({WorkflowNotFoundException.class, NoResourceFoundException.class})
    public ResponseEntity<ErrorResponse> notFound(Exception ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse("Not Found"));
    }

    @ExceptionHandler(PayloadTooLargeException.class)
    public ResponseEntity<ErrorResponse> payloadTooLarge(PayloadTooLargeException ex) {
        return tooLarge();
    }

    @ExceptionHandler({
            DataAccessResourceFailureException.class,
            CannotGetJdbcConnectionException.class,
            CannotCreateTransactionException.class
    })
    public ResponseEntity<ErrorResponse> unavailable(Exception ex) {
        log.warn("database unreachable on request thread", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse("Service Unavailable"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> unexpected(Exception ex) {
        if (isPayloadTooLarge(ex)) {
            return tooLarge();
        }
        log.error("unexpected request-thread error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse("Internal Server Error"));
    }

    private static ResponseEntity<ErrorResponse> tooLarge() {
        return ResponseEntity.status(HttpStatus.CONTENT_TOO_LARGE)
                .body(new ErrorResponse("Payload Too Large"));
    }

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
