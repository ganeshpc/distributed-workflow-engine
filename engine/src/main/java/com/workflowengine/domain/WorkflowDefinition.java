package com.workflowengine.domain;

import java.util.List;

/**
 * In-process description of a workflow graph. Phase 1 graphs are linear lists.
 *
 * <p>Not part of {@code engine-api}: workers do not load definitions. The
 * {@link #version()} that ran is stored on the instance at admit.
 *
 * <p>Implementations are Spring singletons and must be immutable after
 * construction. Duplicate {@link #type()} values fail registry startup.
 */
public interface WorkflowDefinition {

    /**
     * Returns the type clients send on POST, for example {@code ORDER}.
     *
     * @return non-blank type key
     */
    String type();

    /**
     * Returns the definition version copied onto {@code definition_version}.
     *
     * @return integer {@code >= 1}
     */
    int version();

    /**
     * Returns steps in execution order. Index is {@code position}.
     *
     * @return non-empty immutable list
     */
    List<StepDefinition> steps();
}
