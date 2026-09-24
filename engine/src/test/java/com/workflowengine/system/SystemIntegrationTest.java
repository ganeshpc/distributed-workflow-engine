package com.workflowengine.system;

import com.workflowengine.WorkflowEngineApplication;
import com.workflowengine.definition.OrderWorkflowDefinition;
import com.workflowengine.runtime.TimeoutPoller;
import com.workflowengine.support.AdjustableClock;
import com.workflowengine.support.KafkaWorkers;
import com.workflowengine.worker.activity.ActivityBlockHook;
import com.workflowengine.worker.activity.CreateOrderActivity;
import com.workflowengine.worker.activity.ReserveInventoryActivity;
import com.workflowengine.worker.activity.StubInvocationCounters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end checks for the behavior implemented through Phase 7.
 *
 * <p>The client is {@link HttpClient}. The engine is a real web process.
 * Postgres and Kafka are Testcontainers. The worker is the production
 * process. Tests poll {@code GET}; they do not pass {@code ?wait=}.
 */
class SystemIntegrationTest {

    private static final Duration TERMINAL_TIMEOUT = Duration.ofSeconds(8);
    private static final List<String> ORDER_STEPS = List.of(
            "CREATE_ORDER",
            "RESERVE_INVENTORY",
            "PROCESS_PAYMENT",
            "CREATE_SHIPMENT",
            "SEND_NOTIFICATION"
    );
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private static PostgreSQLContainer postgres;
    private static KafkaContainer kafka;
    private static ConfigurableApplicationContext worker;
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private ConfigurableApplicationContext engine;
    private String baseUrl;

    @BeforeAll
    static void startInfra() {
        kafka = KafkaWorkers.newContainer();
        kafka.start();
        worker = KafkaWorkers.startWorker(kafka);
        postgres = new PostgreSQLContainer("postgres:16")
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(
                                ".*database system is ready to accept connections.*\\s", 2))
                        .withStrategy(Wait.forListeningPort())
                        .withStartupTimeout(Duration.ofSeconds(60)));
        postgres.start();
    }

    @AfterAll
    static void stopInfra() {
        if (worker != null) {
            worker.close();
        }
        if (kafka != null) {
            kafka.close();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @BeforeEach
    void resetStubs() {
        ActivityBlockHook.clear();
        StubInvocationCounters.reset();
    }

    @AfterEach
    void stopEngine() {
        ActivityBlockHook.clear();
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    void happyPathCompletesAllFiveSteps() throws Exception {
        startEngine(null);
        String key = "sys-happy-" + UUID.randomUUID();
        HttpResponse<String> created = post(orderBody(key, "{\"customerId\":\"cust-9\",\"amountCents\":4999}"));
        assertThat(created.statusCode()).isEqualTo(201);
        JsonNode admit = JSON.readTree(created.body());
        assertAdmit(admit);
        assertThat(admit.path("input").path("customerId").asString()).isEqualTo("cust-9");
        UUID id = UUID.fromString(admit.path("id").asString());
        assertThat(created.headers().firstValue("Location").orElseThrow())
                .isEqualTo("/api/v1/workflows/" + id);

        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
        assertThat(done.path("currentStep").asString()).isEqualTo("SEND_NOTIFICATION");
        assertThat(done.path("output").path("activity").asString()).isEqualTo("SEND_NOTIFICATION");
        assertThat(done.path("error").isNull()).isTrue();
        JsonNode steps = done.path("steps");
        assertThat(steps.size()).isEqualTo(5);
        for (int i = 0; i < ORDER_STEPS.size(); i++) {
            assertThat(steps.get(i).path("name").asString()).isEqualTo(ORDER_STEPS.get(i));
            assertThat(steps.get(i).path("status").asString()).isEqualTo("COMPLETED");
            assertThat(steps.get(i).path("attempt").asInt()).isEqualTo(1);
            assertThat(steps.get(i).path("output").path("activity").asString()).isEqualTo(ORDER_STEPS.get(i));
        }
    }

    @Test
    void admitReturnsPendingWhileTheWorkerIsStillInsideTheFirstStep() throws Exception {
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        startEngine(null);
        HttpResponse<String> created = post(orderBody("sys-block-" + UUID.randomUUID(), "{\"customerId\":\"cust-9\"}"));
        assertThat(created.statusCode()).isEqualTo(201);
        JsonNode admit = JSON.readTree(created.body());
        assertAdmit(admit);
        UUID id = UUID.fromString(admit.path("id").asString());

        assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5))).isTrue();
        JsonNode running = get(id);
        assertThat(running.path("status").asString()).isEqualTo("RUNNING");
        assertThat(running.path("currentStep").asString()).isEqualTo("CREATE_ORDER");
        assertThat(running.path("steps").get(0).path("status").asString()).isEqualTo("RUNNING");
        assertThat(running.path("steps").get(0).path("attempt").asInt()).isEqualTo(1);

        ActivityBlockHook.release(CreateOrderActivity.NAME);
        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
    }

    @Test
    void secondPostWithTheSameKeyDoesNotRunTheSagaAgain() throws Exception {
        startEngine(null);
        String body = orderBody("sys-idem-" + UUID.randomUUID(), "{\"customerId\":\"cust-9\"}");
        HttpResponse<String> created = post(body);
        UUID id = UUID.fromString(JSON.readTree(created.body()).path("id").asString());
        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);

        HttpResponse<String> again = post(body);
        assertThat(again.statusCode()).isEqualTo(200);
        JsonNode existing = JSON.readTree(again.body());
        assertThat(existing.path("id").asString()).isEqualTo(id.toString());
        assertThat(existing.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(existing.path("version").asInt()).isEqualTo(10);
        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
    }

    @Test
    void createOrderFailureStaysFailedWithLaterStepsPending() throws Exception {
        startEngine(null);
        HttpResponse<String> created = post(orderBody(
                "sys-fail-create-" + UUID.randomUUID(),
                "{\"failAt\":\"CREATE_ORDER\"}"));
        UUID id = UUID.fromString(JSON.readTree(created.body()).path("id").asString());
        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("FAILED");
        assertThat(done.path("currentStep").asString()).isEqualTo("CREATE_ORDER");
        assertThat(done.path("version").asInt()).isEqualTo(2);
        assertThat(done.path("error").asString()).isEqualTo("STUB_FORCED_FAILURE");
        assertThat(done.path("output").isNull()).isTrue();
        assertThat(done.path("steps").size()).isEqualTo(5);
        assertThat(done.path("steps").get(0).path("status").asString()).isEqualTo("FAILED");
        for (int i = 1; i < 5; i++) {
            assertThat(done.path("steps").get(i).path("status").asString()).isEqualTo("PENDING");
        }
    }

    @Test
    void paymentFailureCompensatesCompletedStepsInReverse() throws Exception {
        startEngine(null);
        HttpResponse<String> created = post(orderBody(
                "sys-fail-pay-" + UUID.randomUUID(),
                "{\"customerId\":\"cust-9\",\"failAt\":\"PROCESS_PAYMENT\"}"));
        UUID id = UUID.fromString(JSON.readTree(created.body()).path("id").asString());
        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPENSATED");
        assertThat(done.path("currentStep").asString()).isEqualTo("COMPENSATE_CREATE_ORDER");
        assertThat(done.path("version").asInt()).isEqualTo(10);
        assertThat(done.path("error").asString()).isEqualTo("STUB_FORCED_FAILURE");
        assertThat(done.path("output").isNull()).isTrue();
        JsonNode steps = done.path("steps");
        assertThat(steps.get(0).path("status").asString()).isEqualTo("COMPENSATED");
        assertThat(steps.get(1).path("status").asString()).isEqualTo("COMPENSATED");
        assertThat(steps.get(2).path("name").asString()).isEqualTo("PROCESS_PAYMENT");
        assertThat(steps.get(2).path("status").asString()).isEqualTo("FAILED");
        assertThat(steps.get(3).path("status").asString()).isEqualTo("PENDING");
        assertThat(steps.get(4).path("status").asString()).isEqualTo("PENDING");
        assertThat(steps.get(5).path("name").asString()).isEqualTo("COMPENSATE_RESERVE_INVENTORY");
        assertThat(steps.get(5).path("status").asString()).isEqualTo("COMPLETED");
        assertThat(steps.get(6).path("name").asString()).isEqualTo("COMPENSATE_CREATE_ORDER");
        assertThat(steps.get(6).path("status").asString()).isEqualTo("COMPLETED");
    }

    @Test
    void compensationFailureLeavesTheInstanceFailed() throws Exception {
        AdjustableClock clock = new AdjustableClock(START);
        ActivityBlockHook.install(ReserveInventoryActivity.NAME);
        startEngine(clock);
        HttpResponse<String> created = post(orderBody(
                "sys-fail-comp-" + UUID.randomUUID(),
                "{\"failAt\":\"COMPENSATE_CREATE_ORDER\"}"));
        UUID id = UUID.fromString(JSON.readTree(created.body()).path("id").asString());
        assertThat(ActivityBlockHook.awaitBlocked(ReserveInventoryActivity.NAME, Duration.ofSeconds(5))).isTrue();

        clock.advance(OrderWorkflowDefinition.STEP_TIMEOUT.plusMinutes(1));
        engine.getBean(TimeoutPoller.class).scan();
        ActivityBlockHook.release(ReserveInventoryActivity.NAME);

        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("FAILED");
        assertThat(done.path("error").asString()).isEqualTo("STUB_FORCED_FAILURE");
        assertThat(done.path("output").isNull()).isTrue();
        JsonNode steps = done.path("steps");
        assertThat(steps.get(0).path("name").asString()).isEqualTo("CREATE_ORDER");
        assertThat(steps.get(0).path("status").asString()).isEqualTo("COMPLETED");
        assertThat(steps.get(1).path("name").asString()).isEqualTo("RESERVE_INVENTORY");
        assertThat(steps.get(1).path("status").asString()).isEqualTo("FAILED");
        assertThat(steps.get(1).path("error").asString()).isEqualTo("TIMED_OUT");
        assertThat(steps.get(5).path("name").asString()).isEqualTo("COMPENSATE_CREATE_ORDER");
        assertThat(steps.get(5).path("status").asString()).isEqualTo("FAILED");
    }

    @Test
    void unknownFailAtStillCompletes() throws Exception {
        startEngine(null);
        HttpResponse<String> created = post(orderBody(
                "sys-fail-unknown-" + UUID.randomUUID(),
                "{\"failAt\":\"NOT_A_STEP\"}"));
        UUID id = UUID.fromString(JSON.readTree(created.body()).path("id").asString());
        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
    }

    @Test
    void rejectedRequestsUseGenericBodies() throws Exception {
        startEngine(null);
        assertThat(post("{\"type\":\"UNKNOWN\",\"idempotencyKey\":\"k\",\"input\":{}}").statusCode()).isEqualTo(400);
        assertThat(post("{\"type\":\"ORDER\",\"input\":{}}").statusCode()).isEqualTo(400);
        assertThat(post("{\"type\":\"ORDER\",\"idempotencyKey\":\"\",\"input\":{}}").statusCode()).isEqualTo(400);
        assertThat(post("{").statusCode()).isEqualTo(400);

        HttpResponse<String> missing = getRaw("/api/v1/workflows/" + UUID.randomUUID());
        assertThat(missing.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(missing.body()).path("error").asString()).isEqualTo("Not Found");

        String oversized = "{\"type\":\"ORDER\",\"idempotencyKey\":\"big\",\"input\":{\"pad\":\""
                + "x".repeat(70_000) + "\"}}";
        HttpResponse<String> tooLarge = post(oversized);
        assertThat(tooLarge.statusCode()).isEqualTo(413);
        assertThat(JSON.readTree(tooLarge.body()).path("error").asString()).isEqualTo("Payload Too Large");

        HttpResponse<String> health = getRaw("/actuator/health");
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(health.body()).path("status").asString()).isEqualTo("UP");
    }

    @Test
    void restartedEngineFinishesABlockedStepWithoutRunningItTwice() throws Exception {
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        startEngine(null);
        HttpResponse<String> created = post(orderBody("sys-recover-" + UUID.randomUUID(), "{\"customerId\":\"cust-9\"}"));
        UUID id = UUID.fromString(JSON.readTree(created.body()).path("id").asString());
        assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5))).isTrue();
        assertThat(get(id).path("status").asString()).isEqualTo("RUNNING");
        int invocations = CreateOrderActivity.invocationCount();
        engine.close();
        engine = null;

        startEngine(null, true);
        ActivityBlockHook.release(CreateOrderActivity.NAME);
        JsonNode done = poll(id);
        assertThat(done.path("status").asString()).isEqualTo("COMPLETED");
        assertThat(done.path("version").asInt()).isEqualTo(10);
        assertThat(done.path("steps").get(0).path("attempt").asInt()).isEqualTo(1);
        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(invocations);
    }

    @Test
    void dueDeadlineFailsTheRunningStepAndIgnoresTheLateSuccess() throws Exception {
        AdjustableClock clock = new AdjustableClock(START);
        ActivityBlockHook.install(CreateOrderActivity.NAME);
        startEngine(clock);
        HttpResponse<String> created = post(orderBody("sys-timeout-" + UUID.randomUUID(), "{\"customerId\":\"cust-9\"}"));
        UUID id = UUID.fromString(JSON.readTree(created.body()).path("id").asString());
        assertThat(ActivityBlockHook.awaitBlocked(CreateOrderActivity.NAME, Duration.ofSeconds(5))).isTrue();
        assertThat(get(id).path("status").asString()).isEqualTo("RUNNING");

        clock.advance(OrderWorkflowDefinition.STEP_TIMEOUT.plusMinutes(1));
        engine.getBean(TimeoutPoller.class).scan();

        JsonNode failed = get(id);
        assertThat(failed.path("status").asString()).isEqualTo("FAILED");
        assertThat(failed.path("error").asString()).isEqualTo("TIMED_OUT");
        assertThat(failed.path("currentStep").asString()).isEqualTo("CREATE_ORDER");
        assertThat(failed.path("steps").get(0).path("status").asString()).isEqualTo("FAILED");
        assertThat(failed.path("steps").get(0).path("error").asString()).isEqualTo("TIMED_OUT");
        assertThat(failed.path("steps").size()).isEqualTo(5);

        ActivityBlockHook.release(CreateOrderActivity.NAME);
        Thread.sleep(300);
        JsonNode still = get(id);
        assertThat(still.path("status").asString()).isEqualTo("FAILED");
        assertThat(still.path("error").asString()).isEqualTo("TIMED_OUT");
        assertThat(CreateOrderActivity.invocationCount()).isEqualTo(1);
    }

    private void startEngine(Clock clock) {
        startEngine(clock, false);
    }

    private void startEngine(Clock clock, boolean fastRecovery) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(WorkflowEngineApplication.class);
        if (clock != null) {
            builder.initializers(applicationContext -> {
                GenericApplicationContext generic = (GenericApplicationContext) applicationContext;
                generic.registerBean("clock", Clock.class, () -> clock);
            });
        }
        engine = builder.run(
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.datasource.driver-class-name=org.postgresql.Driver",
                "--spring.jpa.hibernate.ddl-auto=none",
                "--spring.flyway.enabled=true",
                "--server.port=0",
                "--workflow.recovery.interval-ms=" + (fastRecovery ? "200" : "600000"),
                "--workflow.timeout.interval-ms=600000",
                KafkaWorkers.bootstrapArg(kafka)
        );
        String port = engine.getEnvironment().getProperty("local.server.port");
        baseUrl = "http://127.0.0.1:" + port;
    }

    private HttpResponse<String> post(String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/workflows"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode get(UUID id) throws Exception {
        HttpResponse<String> response = getRaw("/api/v1/workflows/" + id);
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private HttpResponse<String> getRaw(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode poll(UUID id) throws Exception {
        long deadline = System.nanoTime() + TERMINAL_TIMEOUT.toNanos();
        JsonNode latest = null;
        while (System.nanoTime() < deadline) {
            latest = get(id);
            String status = latest.path("status").asString();
            if ("COMPLETED".equals(status) || "FAILED".equals(status) || "COMPENSATED".equals(status)) {
                return latest;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("workflow " + id + " did not finish; last=" + latest);
    }

    private static void assertAdmit(JsonNode body) {
        assertThat(body.path("status").asString()).isEqualTo("PENDING");
        assertThat(body.path("version").asInt()).isZero();
        assertThat(body.path("currentStep").isNull()).isTrue();
        assertThat(body.path("output").isNull()).isTrue();
        assertThat(body.path("steps").size()).isEqualTo(5);
        for (int i = 0; i < ORDER_STEPS.size(); i++) {
            assertThat(body.path("steps").get(i).path("name").asString()).isEqualTo(ORDER_STEPS.get(i));
            assertThat(body.path("steps").get(i).path("status").asString()).isEqualTo("PENDING");
        }
    }

    private static String orderBody(String idempotencyKey, String inputJson) {
        return "{\"type\":\"ORDER\",\"idempotencyKey\":\"" + idempotencyKey + "\",\"input\":" + inputJson + "}";
    }
}
