package com.workflowengine.domain;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class WorkflowDefinitionRegistry {

    private final Map<String, WorkflowDefinition> byType;

    public WorkflowDefinitionRegistry(List<WorkflowDefinition> definitions) {
        Map<String, WorkflowDefinition> map = new HashMap<>();
        for (WorkflowDefinition definition : definitions) {
            WorkflowDefinition previous = map.put(definition.type(), definition);
            if (previous != null) {
                throw new IllegalStateException("duplicate workflow type: " + definition.type());
            }
        }
        this.byType = Map.copyOf(map);
    }

    public Optional<WorkflowDefinition> findByType(String type) {
        return Optional.ofNullable(byType.get(type));
    }
}
