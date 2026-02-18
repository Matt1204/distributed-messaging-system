# Stage 1: Build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Copy the parent POM and all module POMs first to cache dependencies
COPY pom.xml .
COPY chat-proto/pom.xml chat-proto/
COPY chat-server/pom.xml chat-server/
COPY chat-client/pom.xml chat-client/

# Download dependencies (this layer is cached unless POMs change)
RUN mvn dependency:go-offline -B

# Copy source code and build
COPY . .
RUN mvn clean package -DskipTests -pl chat-server -am

# Stage 2: Runtime
FROM amazoncorretto:17

WORKDIR /app

# Create a non-root user for security
RUN yum install -y shadow-utils \
    && groupadd -r spring \
    && useradd -r -g spring spring \
    && yum clean all
USER spring:spring

# Copy only the built JAR from the build stage
# We use the specific name to avoid ambiguity
COPY --from=build /app/chat-server/target/chat-server-1.0-SNAPSHOT.jar app.jar

ENV SPRING_PROFILES_ACTIVE=prod
ENV PORT=50051
EXPOSE 50051

ENTRYPOINT ["java", "-Dserver.port=${PORT}", "-jar", "app.jar"]
