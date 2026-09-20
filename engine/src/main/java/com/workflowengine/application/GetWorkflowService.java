package com.workflowengine.application;

import com.workflowengine.api.WorkflowSnapshot;
import com.workflowengine.persistence.WorkflowInstanceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class GetWorkflowService {

    private final WorkflowInstanceRepository instances;

    public WorkflowSnapshot get(UUID id) {
        return instances.findById(id)
                .map(WorkflowSnapshots::from)
                .orElseThrow(() -> new WorkflowNotFoundException(id));
    }
}
