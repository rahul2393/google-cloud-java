FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates file \
 && rm -rf /var/lib/apt/lists/*
USER 1000

# Build with the google-cloud-java repository root as the Docker context.
# Example from java-spanner/: docker build -f Dockerfile -t java-spanner-bypass:test ..
COPY --chown=1000:1000 sdk-platform-java /workspace/sdk-platform-java
COPY --chown=1000:1000 google-cloud-pom-parent /workspace/google-cloud-pom-parent
COPY --chown=1000:1000 google-cloud-jar-parent /workspace/google-cloud-jar-parent
COPY --chown=1000:1000 java-spanner /workspace/java-spanner

# Bootstrap the local SNAPSHOT artifacts required by java-spanner.
RUN mvn -f /workspace/sdk-platform-java/pom.xml -pl gapic-generator-java-bom,java-core/google-cloud-core-bom -am clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/java-shared-dependencies/third-party-dependencies/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/api-common-java/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/java-common-protos/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/gax-java/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/java-shared-dependencies/first-party-dependencies/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/java-shared-dependencies/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/java-iam/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip
RUN mvn -f /workspace/sdk-platform-java/java-core/pom.xml clean install -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip

# Build the java-spanner library and then the bypass-performance shaded jar.
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
