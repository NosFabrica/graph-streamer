# -------- Build stage --------
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app

# Wrapper and build scripts first, so dependency downloads cache apart from source.
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
RUN ./gradlew --no-daemon dependencies > /dev/null

COPY src ./src
RUN ./gradlew --no-daemon installDist -x test


# -------- Runtime stage --------
FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /app/build/install/graph-streamer ./

# Long-running and stateless: one replica (one writer to the graph). Its state is in Neo4j and Redis.
ENTRYPOINT ["/app/bin/graph-streamer"]
