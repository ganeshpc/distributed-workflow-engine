package com.workflowengine.domain;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Indexes {@link WorkflowDefinition} beans by {@link WorkflowDefinition#type()}.
 *
 * <p>Built once at startup. After construction the map is immutable. Duplicate
 * types fail the context. Lookups are safe from any thread.
 *
 * <p>Unknown type is not an exception here; admission maps a miss to HTTP 400.
 */
@Component
public class WorkflowDefinitionRegistry {

    private final Map<String, WorkflowDefinition> byType;

    /**
     * Indexes every definition in the context.
     *
     * @param definitions Spring-discovered graphs; must not contain duplicate types
     * @throws IllegalStateException if two beans share a type
     */
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

    /**
     * Finds a definition by the client-supplied type.
     *
     * @param type workflow type; may be null
     * @return empty if unknown
     */
    public Optional<WorkflowDefinition> findByType(String type) {
        return Optional.ofNullable(byType.get(type));
    }
}
