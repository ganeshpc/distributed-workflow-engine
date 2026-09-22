package com.workflowengine.web;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import com.workflowengine.support.KafkaWorkers;
import com.workflowengine.worker.activity.ActivityBlockHook;
import com.workflowengine.worker.activity.CreateOrderActivity;
import com.workflowengine.worker.activity.StubInvocationCounters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP Phase 1 acceptance: admit {@code 201} body, poll GET to terminal,
 * idempotent {@code 200}, {@code failAt}, 400/404/413, health-only actuator,
 * Scalar UI and OpenAPI spec.
 *
 * <p>Uses Testcontainers Postgres. Stubs are reset per test. The blocked-first
 * stub case proves the request thread does not wait for the saga.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class WorkflowApiTest {

    private static final Duration TERMINAL_TIMEOUT = Duration.ofSeconds(5);
    private static final List<String> ORDER_STEPS = List.of(
            "CREATE_ORDER",
            "RESERVE_INVENTORY",
            "PROCESS_PAYMENT",
            "CREATE_SHIPMENT",
            "SEND_NOTIFICATION"
    );

    @Container
    static KafkaContainer kafka = KafkaWorkers.newContainer();

    private static ConfigurableApplicationContext worker;

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16")
            .waitingFor(new WaitAllStrategy()
                    .withStrategy(Wait.forLogMessage(
                            ".*database system is ready to accept connections.*\\s", 2))
                    .withStrategy(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofSeconds(60)));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void startWorker() {
        worker = KafkaWorkers.startWorker(kafka);
    }

    @AfterAll
    static void stopWorker() {
        if (worker != null) {
            worker.close();
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void resetStubs() {
        ActivityBlockHook.clear();
        StubInvocationCounters.reset();
    }

    @AfterEach
    void releaseStubs() {
        ActivityBlockHook.clear();
    }

    @Test
    void postReturnsAdmitSnapshotThenGetReachesCompleted() throws Exception {
        String key = "http-happy-" + UUID.randomUUID();
        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderBody(key, "{\"customerId\":\"cust-9\",\"amountCents\":4999}")))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", startsWith("/api/v1/workflows/")))
                .andReturn();

        JsonNode admit = read(created);
        assertAdmitSnapshot(admit);
        assertThat(admit.path("input").path("customerId").asString()).isEqualTo("cust-9");
        assertThat(admit.path("input").path("amountCents").asInt()).isEqualTo(4999);
        UUID id = UUID.fromString(admit.path("id").asString());
        assertThat(created.getResponse().getHeader("Location")).isEqualTo("/api/v1/workflows/" + id);

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
        assertThat(done.path("definitionVersion").asInt()).isEqualTo(1);
        assertThat(done.path("currentStep").asString()).isEqualTo("SEND_NOTIFICATION");
        assertThat(done.path("output")).isEqualTo(done.path("steps").get(4).path("output"));
        assertThat(done.path("output").path("activity").asString()).isEqualTo("SEND_NOTIFICATION");
        assertCompletedSteps(done);
    }

    @Test
    void postReturnsAdmitSnapshotWhileFirstStubIsBlocked() throws Exception {
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        String key = "http-block-" + UUID.randomUUID();

        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderBody(key, "{\"customerId\":\"cust-9\"}")))
                .andExpect(status().isCreated())
                .andReturn();

        JsonNode admit = read(created);
        assertAdmitSnapshot(admit);
        UUID id = UUID.fromString(admit.path("id").asString());

        assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5))).isTrue();
        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);

        mockMvc.perform(get("/api/v1/workflows/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RUNNING"))
                .andExpect(jsonPath("$.currentStep").value("CREATE_ORDER"))
                .andExpect(jsonPath("$.steps[0].status").value("RUNNING"));

        ActivityBlockHook.release(CreateOrderActivity.NAME);
        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
    }

    @Test
    void validationAndNotFoundAndPayloadTooLarge() throws Exception {
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"UNKNOWN","idempotencyKey":"k","input":{}}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Bad Request"));

        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"ORDER","input":{"customerId":"cust-9"}}
                                """))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"ORDER","idempotencyKey":"","input":{}}
                                """))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"ORDER","idempotencyKey":"%s","input":{}}
                                """.formatted("k".repeat(129))))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get("/api/v1/workflows/" + UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Not Found"));

        String oversized = "{\"type\":\"ORDER\",\"idempotencyKey\":\"big\",\"input\":{\"pad\":\""
                + "x".repeat(70_000) + "\"}}";
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(oversized))
                .andExpect(status().isContentTooLarge())
                .andExpect(jsonPath("$.error").value("Payload Too Large"));
    }

    @Test
    void secondPostSameKeyReturnsExistingAndDoesNotRerun() throws Exception {
        String key = "http-idem-" + UUID.randomUUID();
        String body = orderBody(key, "{\"customerId\":\"cust-9\"}");

        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn();
        UUID id = UUID.fromString(read(created).path("id").asString());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        int createCount = CreateOrderActivity.invocationCount();
        assertThat(createCount).isEqualTo(1);

        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.version").value(10));

        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(createCount);
    }

    @Test
    void failAtProcessPaymentViaHttp() throws Exception {
        String key = "http-fail-pay-" + UUID.randomUUID();
        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderBody(key, "{\"customerId\":\"cust-9\",\"failAt\":\"PROCESS_PAYMENT\"}")))
                .andExpect(status().isCreated())
                .andReturn();
        UUID id = UUID.fromString(read(created).path("id").asString());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asString()).isEqualTo("FAILED");
        assertThat(done.path("currentStep").asString()).isEqualTo("PROCESS_PAYMENT");
        assertThat(done.path("version").asInt()).isEqualTo(6);
        assertThat(done.path("output").isNull()).isTrue();
        assertThat(done.path("error").asString()).isEqualTo("STUB_FORCED_FAILURE");

        JsonNode steps = done.path("steps");
        assertThat(steps.get(0).path("status").asString()).isEqualTo("COMPLETED");
        assertThat(steps.get(1).path("status").asString()).isEqualTo("COMPLETED");
        assertThat(steps.get(2).path("name").asString()).isEqualTo("PROCESS_PAYMENT");
        assertThat(steps.get(2).path("status").asString()).isEqualTo("FAILED");
        assertThat(steps.get(2).path("completedAt").isNull()).isFalse();
        assertThat(steps.get(3).path("name").asString()).isEqualTo("CREATE_SHIPMENT");
        assertThat(steps.get(3).path("status").asString()).isEqualTo("PENDING");
        assertThat(steps.get(3).path("startedAt").isNull()).isTrue();
        assertThat(steps.get(4).path("name").asString()).isEqualTo("SEND_NOTIFICATION");
        assertThat(steps.get(4).path("status").asString()).isEqualTo("PENDING");
        assertThat(steps.get(4).path("startedAt").isNull()).isTrue();
    }

    @Test
    void failAtCreateOrderViaHttp() throws Exception {
        String key = "http-fail-create-" + UUID.randomUUID();
        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderBody(key, "{\"failAt\":\"CREATE_ORDER\"}")))
                .andExpect(status().isCreated())
                .andReturn();
        UUID id = UUID.fromString(read(created).path("id").asString());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asString()).isEqualTo("FAILED");
        assertThat(done.path("currentStep").asString()).isEqualTo("CREATE_ORDER");
        assertThat(done.path("version").asInt()).isEqualTo(2);

        JsonNode steps = done.path("steps");
        assertThat(steps.get(0).path("status").asString()).isEqualTo("FAILED");
        assertThat(steps.get(0).path("completedAt").isNull()).isFalse();
        for (int i = 1; i < 5; i++) {
            assertThat(steps.get(i).path("status").asString()).isEqualTo("PENDING");
            assertThat(steps.get(i).path("startedAt").isNull()).isTrue();
            assertThat(steps.get(i).path("attempt").asInt()).isZero();
        }
    }

    @Test
    void unknownFailAtCompletesViaHttp() throws Exception {
        String key = "http-fail-unknown-" + UUID.randomUUID();
        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderBody(key, "{\"failAt\":\"NOT_A_REAL_STEP\"}")))
                .andExpect(status().isCreated())
                .andReturn();
        UUID id = UUID.fromString(read(created).path("id").asString());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
        assertCompletedSteps(done);
    }

    @Test
    void actuatorExposesHealthOnly() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
        mockMvc.perform(get("/actuator/env")).andExpect(status().isNotFound());
        mockMvc.perform(get("/actuator/heapdump")).andExpect(status().isNotFound());
    }

    @Test
    void scalarUiIsServed() throws Exception {
        mockMvc.perform(get("/scalar"))
                .andExpect(status().isOk());
    }

    @Test
    void openApiDocsDescribeWorkflows() throws Exception {
        mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.info.title").value("Distributed Workflow Engine"))
                .andExpect(jsonPath("$.paths['/api/v1/workflows']").exists())
                .andExpect(jsonPath("$.paths['/api/v1/workflows/{id}']").exists());
    }

    private JsonNode pollUntilTerminal(UUID id) throws Exception {
        long deadline = System.nanoTime() + TERMINAL_TIMEOUT.toNanos();
        JsonNode latest = null;
        while (System.nanoTime() < deadline) {
            MvcResult result = mockMvc.perform(get("/api/v1/workflows/" + id))
                    .andExpect(status().isOk())
                    .andReturn();
            latest = read(result);
            String status = latest.path("status").asString();
            if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
                return latest;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("workflow " + id + " did not reach terminal state; last=" + latest);
    }

    private static void assertAdmitSnapshot(JsonNode body) {
        assertThat(body.path("status").asString()).isEqualTo("PENDING");
        assertThat(body.path("version").asInt()).isZero();
        assertThat(body.path("currentStep").isNull()).isTrue();
        assertThat(body.path("output").isNull()).isTrue();
        assertThat(body.path("definitionVersion").asInt()).isEqualTo(1);
        assertThat(body.path("type").asString()).isEqualTo("ORDER");
        assertThat(body.path("createdAt").isNull()).isFalse();
        assertThat(body.path("updatedAt").isNull()).isFalse();
        JsonNode steps = body.path("steps");
        assertThat(steps.size()).isEqualTo(5);
        for (int i = 0; i < ORDER_STEPS.size(); i++) {
            JsonNode step = steps.get(i);
            assertThat(step.path("name").asString()).isEqualTo(ORDER_STEPS.get(i));
            assertThat(step.path("position").asInt()).isEqualTo(i);
            assertThat(step.path("status").asString()).isEqualTo("PENDING");
            assertThat(step.path("attempt").asInt()).isZero();
            assertThat(step.path("output").isNull()).isTrue();
            assertThat(step.path("startedAt").isNull()).isTrue();
            assertThat(step.path("completedAt").isNull()).isTrue();
        }
    }

    private static void assertCompletedSteps(JsonNode body) {
        JsonNode steps = body.path("steps");
        assertThat(steps.size()).isEqualTo(5);
        for (int i = 0; i < ORDER_STEPS.size(); i++) {
            JsonNode step = steps.get(i);
            assertThat(step.path("name").asString()).isEqualTo(ORDER_STEPS.get(i));
            assertThat(step.path("position").asInt()).isEqualTo(i);
            assertThat(step.path("status").asString()).isEqualTo("COMPLETED");
            assertThat(step.path("attempt").asInt()).isEqualTo(1);
            assertThat(step.path("startedAt").isNull()).isFalse();
            assertThat(step.path("completedAt").isNull()).isFalse();
            assertThat(step.path("output").path("activity").asString()).isEqualTo(ORDER_STEPS.get(i));
        }
    }

    private JsonNode read(MvcResult result) throws Exception {
        return objectMapper.readTree(result.getResponse().getContentAsByteArray());
    }

    private static String orderBody(String idempotencyKey, String inputJson) {
        return "{\"type\":\"ORDER\",\"idempotencyKey\":\"" + idempotencyKey + "\",\"input\":" + inputJson + "}";
    }
}
