FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -pl engine,worker -am package -Dmaven.test.skip=true \
    && cp engine/target/engine-*.jar /engine.jar \
    && cp worker/target/worker-*.jar /worker.jar

FROM eclipse-temurin:21-jre-jammy AS engine
WORKDIR /app
COPY --from=build /engine.jar /app/engine.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/engine.jar"]

FROM eclipse-temurin:21-jre-jammy AS worker
WORKDIR /app
COPY --from=build /worker.jar /app/worker.jar
ENTRYPOINT ["java", "-jar", "/app/worker.jar"]
