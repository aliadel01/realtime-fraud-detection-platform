# --- Build stage: compile the shaded/fat jar ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Step 1: copy ONLY pom.xml first, and download dependencies here.
# This step only re-runs if pom.xml itself changes. Editing Java
# source code later will NOT invalidate this layer.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline

# Step 2: now copy the source code and compile.
# This step re-runs on every code change, but it no longer needs to
# re-download anything -- dependencies are already cached from Step 1
# (both in the build-cache mount AND, since dependency:go-offline
# already ran, in Maven's normal resolution).
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests package

# --- Runtime stage: Flink client image, used only to submit the job ---
FROM flink:1.18-java17
COPY --from=build /build/target/bronze-ingestion-job-1.0.0.jar /opt/job/bronze-ingestion-job.jar