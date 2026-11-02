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

# Long-running: one replica, its SQLite mirror on a volume at /data (STORE_PATH).
VOLUME ["/data"]
ENTRYPOINT ["/app/bin/graph-streamer"]
