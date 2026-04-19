# Local Development Guide

This document explains how to run the project locally for development, including:

- What modules are included
- What dependencies are required
- How to configure environment variables
- How to run server and client step by step
- What cloud services this project expects (without cloud provisioning tutorials)

Assumption: required cloud services (Cosmos DB, Redis, etc.) are already deployed and reachable.

## 1) Overview

### Project modules

- `chat-proto`: shared gRPC/Protobuf contracts used by both server and client
- `chat-server`: Spring Boot + gRPC backend, connected to Cosmos DB and Redis
- `chat-client`: Java Swing desktop client, gRPC connection, local SQLite storage

### Dependencies

- Java 17
- Maven 3.x
- Network access to your deployed Redis and Cosmos DB

### High-level local workflow

1. Prepare local development environment (JDK + Maven)
2. Build modules from repository root
3. Configure server env vars
4. Run server
5. Configure client env vars
6. Run client and connect to server target

## 2) Local setup and startup (server + client)

### Step 1: Prepare local environment

- Install Java 17 and make sure `java -version` shows 17
- Install Maven and make sure `mvn -version` works

### Step 2: Build from repo root

Run from repository root:

```bash
mvn install
```

This ensures `chat-proto` is built and available to `chat-server` and `chat-client`.

### Step 3: Important working-directory rule

Both launcher classes load dotenv files using directory names:

- server reads from `chat-server`
- client reads from `chat-client`

So for local runs, use repository root as current working directory.  
If you run from IDE, set working directory to repository root, or provide env vars directly in the run configuration.

### Step 4: Configure and run server

#### 4.1 Create server env file

Use `chat-server/.env.example` as template and create `chat-server/.env`.

Required server variables:

- `COSMOS_ENDPOINT`
- `COSMOS_KEY`
- `COSMOS_DATABASE` (used by `dev` profile)
- `REDIS_HOST`
- `REDIS_PORT`
- `REDIS_PASSWORD`
- `CONTAINER_APP_REPLICA_NAME`

Optional server variables:

- `CHAT_GRPC_PORT` (default `50051` in dev profile)
- `SEND_WORKER_THREADS` (default `8`)
- `SPRING_PROFILES_ACTIVE` (default `dev`)

#### 4.2 Run server

From repository root:

```bash
mvn -pl chat-server spring-boot:run
```

### Step 5: Configure and run client

#### 5.1 Client env variables

Create `chat-client/.env` (or set system env vars).  
Required:

- `TARGET`

Environment consistency rule:

- `IS_PROD` must match `TARGET`
- local-style target (`localhost`, `127.0.0.1`, etc.) -> set `IS_PROD=false`
- cloud-style target (for example an Azure Container Apps domain) -> set `IS_PROD=true`

Optional client variables actually used by code:

- `CHAT_CLIENT_DEBUG_SIDEBAR`
- `CHAT_CLIENT_CATCHUP_LIMIT`
- `CHAT_CLIENT_HISTORY_PAGE_SIZE`

Note: `CHAT_CLIENT_CONVERSATION_INITIAL_SIZE` is not used by current client code.

#### 5.2 Client env examples

Local server example:

```dotenv
TARGET=localhost:50051
IS_PROD=false
CHAT_CLIENT_DEBUG_SIDEBAR=true
CHAT_CLIENT_CATCHUP_LIMIT=10
CHAT_CLIENT_HISTORY_PAGE_SIZE=5
```

Deployed server example (placeholder):

```dotenv
TARGET=<your-app>.<region>.azurecontainerapps.io:443
IS_PROD=true
CHAT_CLIENT_DEBUG_SIDEBAR=true
CHAT_CLIENT_CATCHUP_LIMIT=10
CHAT_CLIENT_HISTORY_PAGE_SIZE=5
```

#### 5.3 Run client

From repository root:

```bash
mvn -pl chat-client compile exec:java -Dexec.mainClass=com.coen6731.chat.client.ChatClient
```

### Step 6: TLS behavior

Client channel behavior is target-based:

- if `TARGET` ends with `:443`, client uses TLS transport
- otherwise client uses plaintext transport (typical for local `localhost:50051`)

## 3) Expected cloud service dependencies

This section explains what the application expects from cloud services, not how to provision them.

### Azure Cosmos DB expectations

- Endpoint + key based connection
- Database name comes from:
  - `COSMOS_DATABASE` in `dev`
  - fixed `chat-server-prod` in `prod` profile config
- Existing containers are expected with these names:
  - `users`
  - `messages`
  - `conversations`
- Expected partition key strategy based on server access pattern:
  - `users` container: partitioned by `userId`
  - `messages` and `conversations` containers: partitioned by `conversationId`

The server opens existing database/containers; it does not create full Cosmos infrastructure automatically.

### Redis expectations

- Redis connection is configured with TLS enabled in both dev/prod profiles
- Redis instance must support:
  - string keys for online presence and sequence tracking
  - Redis Streams and consumer groups for cross-replica relay
- `CONTAINER_APP_REPLICA_NAME` should be unique per running server process when sharing one Redis instance

## 4) Quick reference links

- Core architecture and behavior: [`specs/output_specs.md`](output_specs.md)
- Server env template: [`chat-server/.env.example`](../chat-server/.env.example)
- Server config:
  - [`chat-server/src/main/resources/application.yml`](../chat-server/src/main/resources/application.yml)
  - [`chat-server/src/main/resources/application-dev.yml`](../chat-server/src/main/resources/application-dev.yml)
  - [`chat-server/src/main/resources/application-prod.yml`](../chat-server/src/main/resources/application-prod.yml)

## 5) Environment variables summary

### Server

| Variable | Required | Purpose |
|---|---|---|
| `COSMOS_ENDPOINT` | Yes | Cosmos DB endpoint |
| `COSMOS_KEY` | Yes | Cosmos DB key |
| `COSMOS_DATABASE` | Yes (dev) | Cosmos database name in dev profile |
| `REDIS_HOST` | Yes | Redis host |
| `REDIS_PORT` | Yes | Redis port |
| `REDIS_PASSWORD` | Yes | Redis password |
| `CONTAINER_APP_REPLICA_NAME` | Yes | Replica identity used in routing/relay |
| `CHAT_GRPC_PORT` | No | Server gRPC port in dev (default `50051`) |
| `SEND_WORKER_THREADS` | No | Async send worker thread count |
| `SPRING_PROFILES_ACTIVE` | No | Spring profile, default `dev` |

### Client

| Variable | Required | Purpose |
|---|---|---|
| `TARGET` | Yes | gRPC target endpoint (`host:port`) |
| `IS_PROD` | Strongly recommended | Must match target environment classification |
| `CHAT_CLIENT_DEBUG_SIDEBAR` | No | Enable debug sidebar |
| `CHAT_CLIENT_CATCHUP_LIMIT` | No | Per-conversation catch-up upper bound |
| `CHAT_CLIENT_HISTORY_PAGE_SIZE` | No | Conversation history page size |

