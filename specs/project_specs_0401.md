# Architectural Specification (Code-Verified Merge)

**Spec version:** v2-merged  
**Source of truth:** repository implementation (`chat-proto`, `chat-server`, `chat-client`)  
**Scope:** current behavior only; no aspirational architecture unless labeled **(inferred)**.

---

## 0. Critical Corrections Applied

This merged spec intentionally fixes inaccuracies or ambiguity from prior specs.

- `CatchupRequest` is **not** a map payload; it is `cursorHints[]` + `perConversationLimit`.
- `OutboundMessage` includes `toEmail`, `text`, `clientMsgId`, optional `conversationId`; sender info is server-derived.
- `GetMsgHistoryRequest` field is `retriveMsgQuantity` (typo in proto but real contract).
- Session bootstrap supports `x-user-id` metadata on stream open; this bypasses password challenge if user exists.
- Catchup message query is newest-first per conversation (`ORDER BY sequenceId DESC`) and bounded by per-conversation limit.
- History query is descending from an inclusive cursor (`sequenceId <= beforeSequenceId`), capped to 200 server-side.
- Duplicate login behavior is explicit: prior local session receives `DUPLICATE_LOGIN` and is closed.
- Redis stream relay is per target instance (`stream:instance:{instanceId}`) using consumer groups.
- Message idempotency relies on deterministic `serverMsgId = UUID.nameUUIDFromBytes(senderUserId + "::" + clientMsgId)`.
- Conversation model is effectively two-party, enforced by creation flow and membership checks.

---

## 1. System Boundary and Context

The system is a real-time 1:1 messaging demo with:

- server-authoritative persistence in Cosmos DB,
- Redis-based ephemeral coordination (presence, per-conversation sequence, cross-node relay),
- Java Swing desktop client with per-user SQLite local cache.

### In-boundaries

- `chat-proto` RPC contract and generated classes.
- `chat-server` stream handling, auth, persistence orchestration, delivery routing.
- `chat-client` transport lifecycle, heartbeat/reconnect, local DB reconciliation, UI callbacks.

### Out-of-boundaries

- OAuth/OIDC, token issuance, external notification systems, moderation, full-text search, group chat, mobile/web clients.

### Trust boundaries

- Client input is untrusted.
- Server accepts `x-user-id` header on stream establishment and treats matching user as authenticated.
- Transport security depends on target (`:443` enables TLS from client; non-443 uses plaintext).

Security implication: header-based resume is acceptable only in trusted deployment paths; otherwise account impersonation risk exists.

---

## 2. Architecture Overview

### Style

- Server: modular monolith (single Spring Boot process exposing one gRPC service).
- Client: thick desktop app (UI + persistence + transport state).
- Contract-first comms via protobuf/gRPC bidi stream.

### Runtime topology

- One or more server replicas behind Container Apps ingress (`transport: Http2`, sticky affinity shown in `containerapp.yaml`).
- Shared Cosmos DB containers: `users`, `conversations`, `messages`.
- Shared Redis for routing and sequence state.
- Client instances run independently with local SQLite files.

### High-level flow

1. Client opens stream and optionally supplies `x-user-id` if previously authenticated.
2. User authenticates by register/login or is treated authenticated from header path.
3. Outbound send path: validate -> resolve/create conversation -> allocate sequence via Redis -> persist in Cosmos -> sender ack -> best-effort live delivery.
4. Reconnect path: heartbeat detects failure -> reconnect with backoff -> catchup with local cursor hints.

---

## 3. Module Breakdown

## `chat-proto`

- Owns service and message schema for `MessagingService.Chat(stream ClientEvent) returns (stream ServerEvent)`.

## `chat-server`

### `MessagingServiceImpl`

- Single event dispatcher for login/register/send/catchup/history/heartbeat.
- Enforces auth gates for non-auth events.
- Performs idempotent send semantics via deterministic `serverMsgId`.

### `CosmosDBHandler`

- Encapsulates users/conversations/messages CRUD and query operations.
- `messages` writes partition by `conversationId`.
- `createMessageIfAbsent` handles 409 conflicts as dedupe signals.

### `RedisHandler`

- Presence key: `user:online:{userId}` -> `{instanceId}:{sessionId}` with 30s TTL.
- Sequence key: `conversation:latest_msg_sequenceId:{conversationId}` via atomic `INCR`.
- Relay stream: `stream:instance:{targetInstanceId}` with per-instance consumer group.

### `ConnectionRegistry`

- Local `userId -> UserSession` map.
- Duplicate login replacement semantics.
- Heartbeat cleanup (30s timeout, 5s cleanup interval).
- Cross-node relay publish and local delivery of remote relay records.

### `GrpcServerLifecycle` + `UserIdInterceptor`

- gRPC server startup/shutdown under Spring lifecycle.
- Interceptor injects `x-user-id` metadata into context for session bootstrap.

## `chat-client`

### `ChatClientSession`

- Channel/stream lifecycle, reconnect backoff, auth wait-latch, catchup triggering.
- Uses TLS only for targets ending `:443`, otherwise plaintext.
- Adds `x-user-id` header on reconnect when already authenticated.

### `ServerResponseHandler`

- Reconciles all server events into local DB and UI callbacks.
- Applies ack handling, catchup/history ingestion, cursor updates.

### `DatabaseManager`

- SQLite schema management and migration-safe creation.
- Provisional outbound writes, canonical idempotent writes, conversation summaries, local cursors.

### `HeartbeatManager`

- Ping every 10s, pong timeout window 5s, reconnect after 3 misses.

---

## 4. Protocol and Contract (Verified)

### RPC

- `MessagingService.Chat` is a single bidirectional stream.

### ClientEvent payloads

- `LoginUser { email, password }`
- `RegisterUser { email, password }`
- `OutboundMessage { toEmail, text, clientMsgId, conversationId }`
- `HeartbeatPing { ts }`
- `CatchupRequest { cursorHints[], perConversationLimit }`
- `GetMsgHistoryRequest { conversationId, beforeSequenceId, retriveMsgQuantity }`

### ServerEvent payloads

- `InboundMessage`
- `SendMessageAck`
- `CatchupResult`
- `MsgHistoryResult`
- `AuthSuccess`
- `HeartbeatPong`
- `ServerError`

### Error signaling model

- Send path: `SendMessageAck` with `status=FAILED` and structured `errorCode/errorReason`.
- Other paths: `ServerError { code, reason }`.
- Observed server error codes include:
  `BAD_REQUEST`, `AUTH_NOT_AUTHENTICATED`, `AUTH_INVALID_CREDENTIALS`, `AUTH_EMAIL_ALREADY_EXISTS`,
  `RECIPIENT_NOT_FOUND`, `CONVERSATION_INVALID`, `PERSISTENCE_FAILED`, `INTERNAL`.

---

## 5. Core Domain and Rules

### Entities

- User: `userId`, normalized `email`, `passwordHash`.
- Conversation: `conversationId`, `memberUserIds`, timestamps.
- Message: canonical ids, conversation linkage, sequence, sender/recipient ids, content, timestamp, status.

### Business rules

- Send requires authenticated session.
- Conversation access requires membership.
- New conversations are created with exactly two members in send flow.
- Message text must be non-empty and <= 4096 chars.

### Invariants

- `sequenceId` monotonic per conversation (Redis key based).
- `serverMsgId` deterministic per `(senderUserId, clientMsgId)`.
- Duplicate send retries converge to existing persisted message when conflict occurs.

---

## 6. Data Architecture

## Cosmos DB

- Containers: `users`, `conversations`, `messages`.
- Users keyed by `userId`; lookup by email uses query.
- Messages written with partition key `conversationId`.
- `findMaxSequenceId(conversationId)` used to seed Redis sequence key when absent.

## Redis

- Presence markers with TTL for route lookup.
- Sequence cursor keys per conversation.
- Per-instance relay streams plus consumer group/consumer naming conventions.

## SQLite (client)

- `messages` with unique constraints on `(conversation_id, server_msg_id)` and `(conversation_id, sequence_id)` when present.
- `conversations` summary table for UI list.
- `local_conversation_cursor` per user+conversation for catchup hints.
- `user_state` for active identity snapshot.

---

## 7. End-to-End Behavior

### Send message path

1. Validate auth and payload.
2. Resolve recipient by email.
3. Resolve/create conversation with membership checks.
4. Allocate next `sequenceId` via Redis (`initializeIfAbsent` from Cosmos max, then `INCR`).
5. Persist message to Cosmos (`createMessageIfAbsent`).
6. Touch conversation timestamps.
7. Send ack to sender with `PERSISTED_PENDING_DELIVERY`.
8. Attempt live delivery:
   - local session direct send, or
   - Redis stream relay to target instance/session.

### Reconnect + catchup

1. Heartbeat failures trigger reconnect backoff.
2. Reconnect attaches `x-user-id` if already authenticated.
3. Client sends `CatchupRequest` with local cursors.
4. Server enumerates authorized conversations and returns newest missing window per conversation.
5. Client idempotently persists canonical messages and advances local cursors.

### History paging

- Client requests older page before a sequence.
- Server enforces auth + membership and returns descending page bounded to max 200.
- Client writes results without forcing cursor rewind.

---

## 8. Security Posture (As Implemented)

### Implemented

- Passwords hashed via BCrypt.
- Optional transport security based on endpoint conventions.
- Server-side membership checks for history/send.

### Gaps / risks

- `x-user-id` resume path has no cryptographic proof of possession.
- No explicit rate limiting or abuse controls in app code.
- No application-level audit trail beyond logs.
- `containerapp.yaml` includes plaintext secrets in repository snapshot; operationally unsafe.

---

## 9. Non-Functional Characteristics

- Horizontal scale possible at server tier; current sample manifest sets `maxReplicas: 1`.
- Catchup complexity grows with number of user conversations.
- Redis consumer is single-threaded per instance (simple, predictable, potential throughput bottleneck at high fan-in).
- No distributed tracing/metrics pipeline implemented in code; logging is primary observability channel.

---

## 10. Configuration and Environment

### Server

- `application.yml` activates profile from `SPRING_PROFILES_ACTIVE` (default `dev`).
- `application-dev.yml` and `application-prod.yml` configure Cosmos, Redis, gRPC port, replica name.
- gRPC port from `CHAT_GRPC_PORT` (dev) or `PORT` (prod default 50051).

### Client

- Reads `.env` under `chat-client` then falls back to process env.
- Key variables: `TARGET`, `IS_PROD`, `CHAT_CLIENT_DEBUG_SIDEBAR`, `CHAT_CLIENT_HISTORY_PAGE_SIZE`, `CHAT_CLIENT_CATCHUP_LIMIT`.
- Startup enforces consistency between `TARGET` and `IS_PROD`.

---

## 11. Dependency and Build Facts

- Java 17.
- Spring Boot 3.3.4 (server).
- gRPC Java 1.64.0 / Protobuf 3.25.3 (via parent).
- Azure Cosmos SDK 4.65.0.
- Spring Data Redis.
- SQLite JDBC 3.45.1.0.
- Maven multi-module: `chat-proto`, `chat-server`, `chat-client`.

---

## 12. Failure Analysis

- Cosmos unavailable: auth/send/catchup/history may fail; errors returned to client paths.
- Redis unavailable: sequence allocation and cross-node live relay can fail; send may fail before persist.
- Replica crash: clients disconnect and reconnect; durable recovery via catchup.
- Stream relay lag/errors: local consumer retries loop; delivery can be delayed.
- Header spoofing on untrusted boundary: potential session impersonation.

---

## 13. Evolution Guidance (Grounded)

- Maintain additive protobuf evolution; do not repurpose/remove numeric tags without coordinated migration.
- Keep `serverMsgId` derivation stable to preserve idempotency semantics across client retries.
- If moving beyond trusted networks, replace bare `x-user-id` with signed token/session proof.
- If scaling catchup, optimize per-user conversation scan and message query fan-out.

---

## 14. Implementation Anchors

- Proto: `chat-proto/src/main/proto/chat.proto`
- Server service: `chat-server/src/main/java/com/coen6731/chat/server/MessagingServiceImpl.java`
- Server persistence: `chat-server/src/main/java/com/coen6731/chat/server/CosmosDBHandler.java`
- Server redis/routing: `chat-server/src/main/java/com/coen6731/chat/server/RedisHandler.java`
- Session registry: `chat-server/src/main/java/com/coen6731/chat/server/ConnectionRegistry.java`
- gRPC lifecycle/interceptor: `chat-server/src/main/java/com/coen6731/chat/server/GrpcServerLifecycle.java`, `UserIdInterceptor.java`
- Client lifecycle: `chat-client/src/main/java/com/coen6731/chat/client/ChatClientSession.java`
- Client event handling: `chat-client/src/main/java/com/coen6731/chat/client/ServerResponseHandler.java`
- Client local DB: `chat-client/src/main/java/com/coen6731/chat/client/DatabaseManager.java`
- Client heartbeat: `chat-client/src/main/java/com/coen6731/chat/client/HeartbeatManager.java`

---

*End of merged code-verified specification.*
