package com.workflowengine.web;

import com.workflowengine.api.StartWorkflowCommand;
import com.workflowengine.application.AdmissionResult;
import com.workflowengine.application.GetWorkflowService;
import com.workflowengine.application.InvalidStartWorkflowException;
import com.workflowengine.application.StartWorkflowService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
 * <p>POST returns the admit snapshot ({@code PENDING}, {@code version = 0}) with
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
@Tag(name = "Workflows", description = "Admit and query ORDER workflows")
public class WorkflowController {

    private final StartWorkflowService startWorkflowService;
    private final GetWorkflowService getWorkflowService;
    private final ObjectMapper objectMapper;

    /**
     * Admits an ORDER workflow. {@code 201} is not terminal.
     *
     * @param request JSON body; {@code type}, {@code idempotencyKey}, object {@code input}
     * @return admit body on create, existing snapshot on idempotent retry
     * @throws InvalidStartWorkflowException mapped to 400
     */
    @PostMapping
    @Operation(
            summary = "Admit an ORDER workflow",
            description = "Commits instance PENDING plus five PENDING steps, then submits "
                    + "the executor on another thread. The 201 body is that admit snapshot "
                    + "(version 0), not a terminal status. Poll GET for COMPLETED or FAILED. "
                    + "The same idempotencyKey returns 200 and does not start a second run."
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "201",
                    description = "Admitted. Location points at GET by id.",
                    headers = @Header(name = "Location", description = "/api/v1/workflows/{id}"),
                    content = @Content(schema = @Schema(implementation = WorkflowResponse.class))
            ),
            @ApiResponse(
                    responseCode = "200",
                    description = "Idempotent retry; existing snapshot at current status.",
                    content = @Content(schema = @Schema(implementation = WorkflowResponse.class))
            ),
            @ApiResponse(responseCode = "400", description = "Unknown type, missing key, or invalid input"),
            @ApiResponse(responseCode = "413", description = "Body larger than 64 KB"),
            @ApiResponse(responseCode = "503", description = "Database unreachable on this request thread")
    })
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
    @Operation(
            summary = "Get workflow snapshot",
            description = "Committed state including leftovers (PENDING never-started, "
                    + "RUNNING mid-step, FAILED). Steps are ordered by position."
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Current snapshot",
                    content = @Content(schema = @Schema(implementation = WorkflowResponse.class))
            ),
            @ApiResponse(responseCode = "404", description = "Unknown id")
    })
    public WorkflowResponse get(
            @Parameter(description = "Server-generated workflow id", required = true)
            @PathVariable("id") UUID id
    ) {
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
