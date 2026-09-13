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
