package com.workflowengine.web;

import com.workflowengine.api.StartWorkflowCommand;
import com.workflowengine.application.AdmissionResult;
import com.workflowengine.application.GetWorkflowService;
import com.workflowengine.application.InvalidStartWorkflowException;
import com.workflowengine.application.StartWorkflowService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.UUID;

/**
 * REST adapter for admit-then-run. Maps HTTP JSON to
 * {@link StartWorkflowCommand} and snapshots back to Jackson 3 JSON.
 *
 * <p>POST returns the TX1 snapshot ({@code PENDING}, {@code version = 0}) with
 * {@code 201} and {@code Location}, or {@code 200} for an idempotent retry.
 * The request thread does not run the executor. GET is how clients wait for
 * {@code COMPLETED} or {@code FAILED}.
 *
 * <p>Request-thread singleton. Validation failures become 400; missing ids
 * 404. Executor leftovers remain 200 on GET.
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/workflows")
public class WorkflowController {

    private final StartWorkflowService startWorkflowService;
    private final GetWorkflowService getWorkflowService;
    private final ObjectMapper objectMapper;

    /**
     * Admits an ORDER workflow. {@code 201} is not terminal.
     *
     * @param request JSON body; {@code type}, {@code idempotencyKey}, object {@code input}
     * @return TX1 body on create, existing snapshot on idempotent retry
     * @throws InvalidStartWorkflowException mapped to 400
     */
    @PostMapping
    public ResponseEntity<WorkflowResponse> start(@RequestBody StartWorkflowRequest request) {
        AdmissionResult admission = startWorkflowService.start(toCommand(request));
        WorkflowResponse body = WorkflowResponses.from(admission.snapshot(), objectMapper);
        if (admission.created()) {
            return ResponseEntity.created(URI.create("/api/v1/workflows/" + body.id())).body(body);
        }
        return ResponseEntity.ok(body);
    }

    /**
     * Returns the committed snapshot, including leftovers.
     *
     * @param id workflow id
     * @return current snapshot
     * @throws com.workflowengine.application.WorkflowNotFoundException mapped to 404
     */
    @GetMapping("/{id}")
    public WorkflowResponse get(@PathVariable("id") UUID id) {
        return WorkflowResponses.from(getWorkflowService.get(id), objectMapper);
    }

    /**
     * Serializes {@code input} to JSON text. Non-object input is rejected.
     *
     * @param request HTTP body
     * @return engine command
     * @throws InvalidStartWorkflowException when the body is missing or input is not an object
     */
    private StartWorkflowCommand toCommand(StartWorkflowRequest request) {
        if (request == null) {
            throw new InvalidStartWorkflowException("command is required");
        }
        if (request.input() != null && !request.input().isObject()) {
            throw new InvalidStartWorkflowException("input is required");
        }
        String inputJson = null;
        if (request.input() != null) {
            try {
                inputJson = objectMapper.writeValueAsString(request.input());
            } catch (JacksonException ex) {
                throw new InvalidStartWorkflowException("input is required");
            }
        }
        return new StartWorkflowCommand(request.type(), request.idempotencyKey(), inputJson);
    }
}
