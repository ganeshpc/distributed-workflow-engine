# Distributed Workflow Engine

Java 21 / Maven multi-module workflow orchestrator. Phase 1 is a single Spring Boot process plus PostgreSQL.

## Prerequisites

- Java 21
- Maven 3.9+
- Docker (for PostgreSQL)

## Local Postgres

```text
docker compose up -d
```

Wait until the `postgres` service is healthy (`docker compose ps`). Credentials:

| Setting  | Value      |
|----------|------------|
| Host     | localhost  |
| Port     | 5432       |
| Database | workflow   |
| User     | workflow   |
| Password | workflow   |

## Run the engine

```text
mvn -pl engine -am spring-boot:run
```

The app listens on port 8080. Health:

```text
curl -s http://localhost:8080/actuator/health
```

Expect `{"status":"UP"}`.

## Start an ORDER workflow

```text
docker compose up -d
# wait for postgres healthy
mvn -pl engine -am spring-boot:run

curl -sD - -X POST http://localhost:8080/api/v1/workflows \
  -H 'Content-Type: application/json' \
  -d '{"type":"ORDER","idempotencyKey":"order-1001","input":{"customerId":"cust-9","amountCents":4999}}'
# 201 + Location: /api/v1/workflows/<id>

curl -s http://localhost:8080/api/v1/workflows/<id>
# poll until status=COMPLETED
```
