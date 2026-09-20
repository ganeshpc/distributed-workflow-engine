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

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/workflows")
public class WorkflowController {

    private final StartWorkflowService startWorkflowService;
    private final GetWorkflowService getWorkflowService;
    private final ObjectMapper objectMapper;

    @PostMapping
    public ResponseEntity<WorkflowResponse> start(@RequestBody StartWorkflowRequest request) {
        AdmissionResult admission = startWorkflowService.start(toCommand(request));
        WorkflowResponse body = WorkflowResponses.from(admission.snapshot(), objectMapper);
        if (admission.created()) {
            return ResponseEntity.created(URI.create("/api/v1/workflows/" + body.id())).body(body);
        }
        return ResponseEntity.ok(body);
    }

    @GetMapping("/{id}")
    public WorkflowResponse get(@PathVariable("id") UUID id) {
        return WorkflowResponses.from(getWorkflowService.get(id), objectMapper);
    }

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
