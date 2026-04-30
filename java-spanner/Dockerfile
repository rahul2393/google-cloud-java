FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates file \
 && rm -rf /var/lib/apt/lists/*
USER 1000

# Build with the google-cloud-java repository root as the Docker context.
# Example from java-spanner/: docker build -f Dockerfile -t image:latest ..
COPY --chown=1000:1000 sdk-platform-java /workspace/sdk-platform-java
COPY --chown=1000:1000 java-common-protos /workspace/java-common-protos
COPY --chown=1000:1000 java-iam /workspace/java-iam
COPY --chown=1000:1000 java-monitoring /workspace/java-monitoring
COPY --chown=1000:1000 java-trace /workspace/java-trace
COPY --chown=1000:1000 google-auth-library-java /workspace/google-auth-library-java
COPY --chown=1000:1000 google-cloud-pom-parent /workspace/google-cloud-pom-parent
COPY --chown=1000:1000 google-cloud-jar-parent /workspace/google-cloud-jar-parent

# Bootstrap the sdk-platform-java artifacts required by java-spanner.
RUN mvn -f /workspace/sdk-platform-java/pom.xml \
    -pl gapic-generator-java-pom-parent,java-shared-dependencies/third-party-dependencies \
    -am clean install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/java-common-protos/pom.xml clean install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/pom.xml \
    -pl api-common-java \
    -am install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/google-auth-library-java/pom.xml \
    -pl credentials,oauth2_http,bom \
    -am clean install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/pom.xml \
    -pl gax-java/gax-bom \
    -am install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/java-iam/pom.xml \
    -pl proto-google-iam-v1 \
    -am clean install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/pom.xml \
    -pl java-shared-dependencies,api-common-java,gax-java/gax-bom,gax-java/gax,gax-java/gax-grpc,gax-java/gax-httpjson,java-core/google-cloud-core-grpc \
    -am install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/pom.xml \
    -pl gapic-generator-java-bom,java-core/google-cloud-core-bom,java-shared-dependencies/first-party-dependencies,java-shared-dependencies \
    -am install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/java-monitoring/pom.xml \
    -pl google-cloud-monitoring,proto-google-cloud-monitoring-v3,grpc-google-cloud-monitoring-v3 \
    -am clean install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/java-trace/pom.xml \
    -pl google-cloud-trace,proto-google-cloud-trace-v1 \
    -am clean install \
    -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip

# Build the java-spanner library and then the bypass-performance jar.
COPY --chown=1000:1000 java-spanner /workspace/java-spanner
RUN mvn -f /workspace/java-spanner/pom.xml install -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip -pl google-cloud-spanner -am
RUN mvn -f /workspace/java-spanner/bypass-performance/pom.xml package -DskipTests -Dmaven.test.skip=true -Dclirr.skip -Dmaven.javadoc.skip=true -Dcheckstyle.skip

RUN JAR_PATH="$(find /workspace/java-spanner/bypass-performance/target -maxdepth 1 -type f -name 'google-cloud-spanner-bypass-performance-*.jar' ! -name '*-tests.jar' ! -name 'original-*' | head -n 1)" \
 && test -n "$JAR_PATH" \
 && cp "$JAR_PATH" /workspace/java-spanner/bypass-performance/target/main.jar \
 && file /workspace/java-spanner/bypass-performance/target/main.jar

FROM eclipse-temurin:17-jre

WORKDIR /app

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/java-spanner/bypass-performance/target/main.jar /app/main.jar

ENTRYPOINT ["java", "-jar", "/app/main.jar"]
