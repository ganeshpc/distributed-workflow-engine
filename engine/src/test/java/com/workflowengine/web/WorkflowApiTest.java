package com.workflowengine.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.workflowengine.activity.stub.ActivityBlockHook;
import com.workflowengine.activity.stub.CreateOrderActivity;
import com.workflowengine.activity.stub.StubInvocationCounters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .waitingFor(new WaitAllStrategy()
                    .withStrategy(Wait.forLogMessage(
                            ".*database system is ready to accept connections.*\\s", 2))
                    .withStrategy(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofSeconds(60)));

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
    void postReturnsTx1ThenGetReachesCompleted() throws Exception {
        String key = "http-happy-" + UUID.randomUUID();
        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderBody(key, "{\"customerId\":\"cust-9\",\"amountCents\":4999}")))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", startsWith("/api/v1/workflows/")))
                .andReturn();

        JsonNode tx1 = read(created);
        assertTx1Body(tx1);
        assertThat(tx1.path("input").path("customerId").asText()).isEqualTo("cust-9");
        assertThat(tx1.path("input").path("amountCents").asInt()).isEqualTo(4999);
        UUID id = UUID.fromString(tx1.path("id").asText());
        assertThat(created.getResponse().getHeader("Location")).isEqualTo("/api/v1/workflows/" + id);

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asText()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
        assertThat(done.path("definitionVersion").asInt()).isEqualTo(1);
        assertThat(done.path("currentStep").asText()).isEqualTo("SEND_NOTIFICATION");
        assertThat(done.path("output")).isEqualTo(done.path("steps").get(4).path("output"));
        assertThat(done.path("output").path("activity").asText()).isEqualTo("SEND_NOTIFICATION");
        assertCompletedSteps(done);
    }

    @Test
    void postReturnsTx1WhileFirstStubIsBlocked() throws Exception {
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        String key = "http-block-" + UUID.randomUUID();

        MvcResult created = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(orderBody(key, "{\"customerId\":\"cust-9\"}")))
                .andExpect(status().isCreated())
                .andReturn();

        JsonNode tx1 = read(created);
        assertTx1Body(tx1);
        UUID id = UUID.fromString(tx1.path("id").asText());

        assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5))).isTrue();
        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);

        mockMvc.perform(get("/api/v1/workflows/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RUNNING"))
                .andExpect(jsonPath("$.currentStep").value("CREATE_ORDER"))
                .andExpect(jsonPath("$.steps[0].status").value("RUNNING"));

        ActivityBlockHook.release(CreateOrderActivity.NAME);
        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asText()).isEqualTo("COMPLETED");
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
                .andExpect(status().isPayloadTooLarge())
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
        UUID id = UUID.fromString(read(created).path("id").asText());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asText()).isEqualTo("COMPLETED");
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
        UUID id = UUID.fromString(read(created).path("id").asText());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asText()).isEqualTo("FAILED");
        assertThat(done.path("currentStep").asText()).isEqualTo("PROCESS_PAYMENT");
        assertThat(done.path("version").asInt()).isEqualTo(6);
        assertThat(done.path("output").isNull()).isTrue();
        assertThat(done.path("error").asText()).isEqualTo("STUB_FORCED_FAILURE");

        JsonNode steps = done.path("steps");
        assertThat(steps.get(0).path("status").asText()).isEqualTo("COMPLETED");
        assertThat(steps.get(1).path("status").asText()).isEqualTo("COMPLETED");
        assertThat(steps.get(2).path("name").asText()).isEqualTo("PROCESS_PAYMENT");
        assertThat(steps.get(2).path("status").asText()).isEqualTo("FAILED");
        assertThat(steps.get(2).path("completedAt").isNull()).isFalse();
        assertThat(steps.get(3).path("name").asText()).isEqualTo("CREATE_SHIPMENT");
        assertThat(steps.get(3).path("status").asText()).isEqualTo("PENDING");
        assertThat(steps.get(3).path("startedAt").isNull()).isTrue();
        assertThat(steps.get(4).path("name").asText()).isEqualTo("SEND_NOTIFICATION");
        assertThat(steps.get(4).path("status").asText()).isEqualTo("PENDING");
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
        UUID id = UUID.fromString(read(created).path("id").asText());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asText()).isEqualTo("FAILED");
        assertThat(done.path("currentStep").asText()).isEqualTo("CREATE_ORDER");
        assertThat(done.path("version").asInt()).isEqualTo(2);

        JsonNode steps = done.path("steps");
        assertThat(steps.get(0).path("status").asText()).isEqualTo("FAILED");
        assertThat(steps.get(0).path("completedAt").isNull()).isFalse();
        for (int i = 1; i < 5; i++) {
            assertThat(steps.get(i).path("status").asText()).isEqualTo("PENDING");
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
        UUID id = UUID.fromString(read(created).path("id").asText());

        JsonNode done = pollUntilTerminal(id);
        assertThat(done.path("status").asText()).isEqualTo("COMPLETED");
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

    private JsonNode pollUntilTerminal(UUID id) throws Exception {
        long deadline = System.nanoTime() + TERMINAL_TIMEOUT.toNanos();
        JsonNode latest = null;
        while (System.nanoTime() < deadline) {
            MvcResult result = mockMvc.perform(get("/api/v1/workflows/" + id))
                    .andExpect(status().isOk())
                    .andReturn();
            latest = read(result);
            String status = latest.path("status").asText();
            if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
                return latest;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("workflow " + id + " did not reach terminal state; last=" + latest);
    }

    private static void assertTx1Body(JsonNode body) {
        assertThat(body.path("status").asText()).isEqualTo("PENDING");
        assertThat(body.path("version").asInt()).isZero();
        assertThat(body.path("currentStep").isNull()).isTrue();
        assertThat(body.path("output").isNull()).isTrue();
        assertThat(body.path("definitionVersion").asInt()).isEqualTo(1);
        assertThat(body.path("type").asText()).isEqualTo("ORDER");
        assertThat(body.path("createdAt").isNull()).isFalse();
        assertThat(body.path("updatedAt").isNull()).isFalse();
        JsonNode steps = body.path("steps");
        assertThat(steps.size()).isEqualTo(5);
        for (int i = 0; i < ORDER_STEPS.size(); i++) {
            JsonNode step = steps.get(i);
            assertThat(step.path("name").asText()).isEqualTo(ORDER_STEPS.get(i));
            assertThat(step.path("position").asInt()).isEqualTo(i);
            assertThat(step.path("status").asText()).isEqualTo("PENDING");
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
            assertThat(step.path("name").asText()).isEqualTo(ORDER_STEPS.get(i));
            assertThat(step.path("position").asInt()).isEqualTo(i);
            assertThat(step.path("status").asText()).isEqualTo("COMPLETED");
            assertThat(step.path("attempt").asInt()).isEqualTo(1);
            assertThat(step.path("startedAt").isNull()).isFalse();
            assertThat(step.path("completedAt").isNull()).isFalse();
            assertThat(step.path("output").path("activity").asText()).isEqualTo(ORDER_STEPS.get(i));
        }
    }

    private JsonNode read(MvcResult result) throws Exception {
        return objectMapper.readTree(result.getResponse().getContentAsByteArray());
    }

    private static String orderBody(String idempotencyKey, String inputJson) {
        return "{\"type\":\"ORDER\",\"idempotencyKey\":\"" + idempotencyKey + "\",\"input\":" + inputJson + "}";
    }
}
