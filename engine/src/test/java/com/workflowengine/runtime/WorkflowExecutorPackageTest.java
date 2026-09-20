package com.workflowengine.runtime;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the package boundary: {@link WorkflowExecutor} must not import
 * {@code com.workflowengine.activity.stub}.
 */
class WorkflowExecutorPackageTest {

    /** Fails if the executor class file names the stub package. */
    @Test
    void executorBytecodeDoesNotReferenceStubPackage() throws IOException {
        try (InputStream in = WorkflowExecutor.class.getResourceAsStream("WorkflowExecutor.class")) {
            assertThat(in).isNotNull();
            String bytecode = new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
            assertThat(bytecode).doesNotContain("activity/stub");
            assertThat(bytecode).doesNotContain("activity.stub");
        }

        Path source = Path.of("src/main/java/com/workflowengine/runtime/WorkflowExecutor.java");
        if (Files.exists(source)) {
            String text = Files.readString(source);
            assertThat(text).doesNotContain("activity.stub");
        }
    }
}
