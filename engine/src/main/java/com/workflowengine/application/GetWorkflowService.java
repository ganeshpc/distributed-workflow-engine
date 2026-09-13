package com.workflowengine.application;

import com.workflowengine.api.WorkflowSnapshot;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class GetWorkflowService {

    private final WorkflowInstanceRepository instances;

    public GetWorkflowService(WorkflowInstanceRepository instances) {
        this.instances = instances;
    }

    public WorkflowSnapshot get(UUID id) {
        return instances.findById(id)
                .map(WorkflowSnapshots::from)
                .orElseThrow(() -> new WorkflowNotFoundException(id));
    }
}
