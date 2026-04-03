# Architectural Specification - grpc-chat-demo (Code Truth, v2)

This specification is based only on repository code. No existing document is treated as the source of truth.
Labeling convention:

- `Code`: directly verifiable from source code
- `Inference`: reasonable inference from implemented behavior
- `Gap`: not implemented or not clearly defined in code

---

## 1. System Boundary and Context Analysis

### 1.1 System goals

`Code`
This system is a 1:1 real-time chat system with these core capabilities:

1. Account registration and login (email/password)
2. Single bidirectional gRPC long-lived stream
3. Persist-first message ack behavior
4. Cross-replica online routing and forwarding
5. Catchup and paged history after disconnect
6. Client-side local SQLite cache and session recovery

### 1.2 In-boundary modules

`Code`

1. `chat-proto`: Protobuf contracts and gRPC service definitions
2. `chat-server`: Spring Boot + gRPC + Cosmos + Redis
3. `chat-client`: Swing UI + gRPC client + SQLite

### 1.3 Out-of-boundary dependencies

`Code`

1. Azure Cosmos DB (durability)
2. Redis (presence, cross-replica relay, sequence allocation)
3. Runtime environment (container platform, networking, TLS termination)

### 1.4 Boundary data flow

`Code`

1. Client -> Server: `ClientEvent`
2. Server -> Client: `ServerEvent`
3. Server -> Cosmos: users / conversations / messages
4. Server <-> Redis: presence keys, conversation sequence keys, instance streams

---

## 2. Overall Architecture Design

### 2.1 Architecture style

`Code`
Modular architecture within a single process, with multi-replica horizontal deployment support.
Each replica maintains local connection sessions; cross-replica coordination is Redis-based.

### 2.2 Topology and routing model

`Code`

1. A client connects to one server replica
2. If recipient is online on the same replica, deliver locally
3. If recipient is online on another replica, publish to target replica Redis Stream
4. Target replica consumes stream and delivers to local session

### 2.3 Key trade-offs

`Inference`

1. Single gRPC bidi stream reduces connection complexity but couples multiple features in one stream
2. Redis handles online/relay concerns; Cosmos handles durable state
3. Async send pipeline protects gRPC callback threads but introduces queue/backpressure semantics

---

## 3. Module Breakdown

### 3.1 `chat-proto`

`Code`
Defines `MessagingService.Chat(stream ClientEvent) returns (stream ServerEvent)`.
`ClientEvent` includes `LoginUser`, `RegisterUser`, `OutboundMessage`, `HeartbeatPing`, `CatchupRequest`, `GetMsgHistoryRequest`.
`ServerEvent` includes `InboundMessage`, `ServerError`, `CatchupResult`, `HeartbeatPong`, `AuthSuccess`, `SendMessageAck`, `MsgHistoryResult`.

### 3.2 `chat-server/GrpcServerLifecycle`

`Code`

1. Starts gRPC via `ServerBuilder.forPort(chat.grpc.port)`
2. Registers `MessagingServiceImpl`
3. Registers interceptor `UserIdInterceptor`
4. Uses `SmartLifecycle` for lifecycle control
5. Keeps non-daemon `awaitThread`
6. Graceful shutdown waits 30s, then `shutdownNow`

### 3.3 `chat-server/UserIdInterceptor`

`Code`
Reads `x-user-id` from metadata into gRPC `Context`, while allowing anonymous stream establishment.

### 3.4 `chat-server/MessagingServiceImpl`

`Code`
Core orchestration layer responsible for:

1. Stream-level auth state management
2. Login and registration
3. Async send pipeline
4. Heartbeat response
5. Catchup and history
6. Error output model

Key constants: `MAX_TEXT_LENGTH=4096`, default catchup limit=50, page cap=200.

### 3.5 `chat-server/ConnectionRegistry`

`Code`

1. Local `ConcurrentHashMap<userId, UserSession>`
2. Writes Redis presence on user online
3. Kicks old session on duplicate login (`DUPLICATE_LOGIN`)
4. Periodic timeout cleanup (scan every 5s, timeout 30s)
5. Handles local delivery of remotely relayed messages

### 3.6 `chat-server/RedisHandler`

`Code`

1. Presence key: `user:online:{userId}`, value `{instanceId}:{sessionId}`, TTL=30s
2. Sequence key: `conversation:latest_msg_sequenceId:{conversationId}`
3. Relay stream: `stream:instance:{instanceId}`
4. Consumer group: `cg:{instanceId}`
5. Background loop: `count(10)` + `block(2s)` + `ack`

### 3.7 `chat-server/CosmosDBHandler`

`Code`
Encapsulates users/conversations/messages container access:

1. User query and creation
2. Conversation lookup and idempotent creation
3. Idempotent message write
4. Sequence-based queries (catchup/history)
5. Conversation touch (`patch` + fallback `upsert`)

### 3.8 `chat-server/SendAsyncExecutor`

`Code`

1. Fixed thread pool (`chat.send.worker-threads`, min 1)
2. Queue capacity 30000 (`ArrayBlockingQueue`)
3. Returns false on rejection
4. Exposes snapshot metrics (submitted/completed/rejected)

### 3.9 `chat-client/ChatClient` + `ChatClientSession`

`Code`

1. Reads `TARGET`, `IS_PROD`, and optional tuning envs
2. Validates consistency between `IS_PROD` and `TARGET`
3. Establishes gRPC stream
4. Login/register blocking wait (15s)
5. Auto reconnect (exponential backoff + jitter, max 5s)
6. Attaches `x-user-id` on reconnect when already authenticated

### 3.10 `chat-client/ServerResponseHandler`

`Code`
Demultiplexes `ServerEvent`, updates SQLite, triggers UI refresh, coordinates reconnect.

### 3.11 `chat-client/HeartbeatManager`

`Code`

1. Sends ping every 10s
2. Schedules 5s pong timeout after each ping
3. Triggers reconnect after 3 consecutive misses

### 3.12 `chat-client/DatabaseManager`

`Code`
Manages local SQLite schema and CRUD:

1. `messages` (composite PK: `client_msg_id + direction`)
2. `conversations`
3. `user_state`
4. `local_conversation_cursor`

### 3.13 `chat-client/ChatWindow`

`Code`
Swing UI containing:

1. Login/register view
2. Conversation list and message bubbles
3. Top-scroll-triggered history backfill
4. Optional debug sidebar

---

## 4. Protocol and Contract Design

### 4.1 gRPC service

`Code`

1. Only one RPC: `Chat`
2. The bidi stream multiplexes auth/send/heartbeat/catchup/history

### 4.2 Key message contracts

`Code`

1. `OutboundMessage`: `toEmail`, `text`, `clientMsgId`, `conversationId`
2. `SendMessageAck`: `status`, `errorCode`, `errorReason`, `sequenceId`, `ackTs`
3. `CatchupRequest`: per-conversation cursor hints + per-conversation limit
4. `GetMsgHistoryRequest`: `conversationId`, `beforeSequenceId`, `retriveMsgQuantity`

Note: `retriveMsgQuantity` is a typo in the protocol but is part of the real contract.

### 4.3 Error model

`Code`
Two error channels:

1. Generic error: `ServerError(code, reason)`
2. Send error: `SendMessageAck(status=FAILED, errorCode, errorReason)`

Common codes: `BAD_REQUEST`, `AUTH_NOT_AUTHENTICATED`, `AUTH_INVALID_CREDENTIALS`, `AUTH_EMAIL_ALREADY_EXISTS`, `INTERNAL`, `RECIPIENT_NOT_FOUND`, `CONVERSATION_INVALID`, `PERSISTENCE_FAILED`, `OVERLOADED`.

### 4.4 Ack status semantics

`Code`
Current server send path emits only:

1. Success: `PERSISTED_PENDING_DELIVERY`
2. Failure: `FAILED`

`Gap`
`DELIVERED_LIVE` exists in proto but is not emitted by current server implementation.

---

## 5. Core Behavior Flows

### 5.1 Stream establishment and authentication

`Code`

1. On stream start, check `x-user-id`
2. If header userId resolves in Cosmos, stream enters authenticated state directly
3. Otherwise stream remains unauthenticated and waits for `LoginUser` / `RegisterUser`
4. On auth success, server emits `AuthSuccess` and registers online session

### 5.2 Login

`Code`

1. Normalize email (trim + lowercase)
2. Validate non-empty inputs
3. Find user by email
4. Verify password using BCrypt
5. Enter `activateAuthenticatedSession` on success

### 5.3 Registration

`Code`

1. Validate email/password
2. Enforce email uniqueness
3. BCrypt hash
4. Create user record
5. Enter `activateAuthenticatedSession` on success

### 5.4 Send pipeline (most critical)

`Code`

1. Validate sender authenticated
2. Validate content: non-empty `toEmail`, `clientMsgId`, `text`, and `text<=4096`
3. Submit send task to `SendAsyncExecutor`
4. If queue full, immediate `FAILED + OVERLOADED`
5. In async worker:
6. Resolve recipient userId by email
7. Resolve/create conversation with membership constraints
8. Allocate sequenceId (Redis, with Cosmos max init fallback)
9. Derive deterministic `serverMsgId = UUID.nameUUIDFromBytes(senderUserId + "::" + clientMsgId)`
10. Persist via Cosmos `createMessageIfAbsent`
11. On success/idempotent hit, send success ack (`PERSISTED_PENDING_DELIVERY`)
12. Build `InboundMessage` and attempt live delivery (local or cross-replica)
13. Emit send latency metrics log

### 5.5 Local/cross-replica live delivery

`Code`

1. Check local `ConnectionRegistry.getSession(toUserId)` first
2. If local session exists, push directly
3. Else query Redis routing info
4. Parse `instanceId:sessionId`
5. Publish to target instance stream
6. Target instance consumes and sessionId-matches before final local delivery

### 5.6 Catchup

`Code`

1. Authenticated users only
2. Limit normalization: default 50, cap 200
3. Load all authorized conversations and sort by `lastMessageAtMs desc`
4. For each conversation, compute server latest sequence from Cosmos
5. If client cursor < server cursor, fetch newest missing window
6. Return `CatchupResult` (with per-conversation `conversationLatestSequenceId`)

### 5.7 History

`Code`

1. Authenticated users only
2. Validate non-empty `conversationId`, existence, and membership authorization
3. `beforeSequenceId` must be > 0
4. `retriveMsgQuantity` must be > 0 and capped at 200
5. Query `sequenceId <= beforeSequenceId` ordered by `sequenceId desc`
6. Return `MsgHistoryResult`

---

## 6. Data Architecture

### 6.1 Cosmos data model

`Code`

1. `users`: `id/userId/email/passwordHash/createdAt/updatedAt`
2. `conversations`: `id/conversationId/memberUserIds/createdAtMs/updatedAtMs/lastMessageAtMs`
3. `messages`: `id=serverMsgId` plus `clientMsgId/conversationId/sequenceId/sender/recipient/text/sentAtMs/status`

### 6.2 Cosmos partitioning and query behavior

`Code`

1. users: point-read by `userId` or SQL query by email
2. messages: writes use partition key `conversationId`
3. conversations: by id and by member query
4. server latest sequence uses `MAX(c.sequenceId)`

### 6.3 Redis data model

`Code`

1. Presence key: `user:online:{userId}`, TTL 30s
2. Sequence key: `conversation:latest_msg_sequenceId:{conversationId}`
3. Relay stream: `stream:instance:{instanceId}` with map fields

### 6.4 SQLite data model

`Code`

1. `messages` composite PK: `(client_msg_id, direction)`
2. Unique index: `(conversation_id, server_msg_id)` when server_msg_id non-empty
3. Unique index: `(conversation_id, sequence_id)` when sequence_id > 0
4. `conversations` stores conversation preview
5. `local_conversation_cursor` stores latest sequence per user/conversation

### 6.5 Client message-state persistence strategy

`Code`

1. Insert outbound provisional row before send (`PENDING_ACK`)
2. Update provisional row on success ack
3. Delete provisional row on failed ack
4. Inbound/catchup/history all use canonical idempotent upsert path

---

## 7. Frontend History Backfill Algorithm

### 7.1 Initial load

`Code`

1. Read `latestKnownSequenceId` (cursor) and `maxStoredSequenceId` (local messages)
2. Use `high=max(latestKnown,maxStored)`
3. Target range `[low, high]`, where `low=max(1, high-pageSize+1)`
4. Query local existing sequence set in range
5. Compute missing segments
6. For each missing segment, send history request and wait for completion
7. Re-render from local data in range

### 7.2 Upward load older messages

`Code`

1. Trigger only when scroll reaches top
2. Show separator gate first; require second upward action to fetch
3. Expand older range from `renderedLowSequenceId-1`
4. Mark exhausted when no older messages remain

### 7.3 Concurrency protection

`Code`

1. `pendingHistoryFetchByConversation` prevents concurrent history requests per conversation
2. `selectionGeneration` prevents stale async result from corrupting newly selected conversation

---

## 8. Concurrency and Threading Model

### 8.1 Server

`Code`

1. gRPC callback threads handle stream callbacks
2. send pipeline runs in `SendAsyncExecutor` worker threads
3. `UserSession.send` is `synchronized` for `StreamObserver` safety
4. `ConnectionRegistry` uses `ConcurrentHashMap` + scheduled cleanup thread
5. `RedisHandler` has single consumer-loop thread

### 8.2 Client

`Code`

1. UI runs on Swing EDT
2. network/heavy tasks run in `CompletableFuture` and scheduler threads
3. heartbeat/reconnect run via `ScheduledExecutorService`

---

## 9. Security Architecture

### 9.1 Implemented mechanisms

`Code`

1. BCrypt password hash storage and verification
2. Email normalization to reduce duplicate-account bypass
3. Membership checks before conversation history access
4. Sender identity derived from server-authenticated state, not client claims

### 9.2 Critical security risks

`Code`

1. `x-user-id` can restore auth state if mapped user exists
2. No signed token, expiry, or replay protection for this resume path
3. Client may use plaintext gRPC when target is not `:443`

`Gap`

1. No application-layer rate limiting
2. No account lockout strategy
3. No audit event model

---

## 10. Failure Analysis

| Scenario | Trigger | Current behavior | User impact |
|---|---|---|---|
| Send queue full | `SendAsyncExecutor` reject | Returns `FAILED + OVERLOADED` | Send fails, user can retry |
| Redis unavailable | sequence/presence/relay call fails | sequence allocation or cross-replica live delivery may fail | live delivery may fail, persisted messages recoverable via catchup |
| Cosmos unavailable | user lookup or message persist fails | returns `INTERNAL` / `PERSISTENCE_FAILED` | auth/send fails |
| Recipient not found | email lookup empty | `RECIPIENT_NOT_FOUND` ack | send fails |
| Session heartbeat timeout | no heartbeat for 30s | server cleans session and presence key | client reconnects |
| Stream disconnect | onError/onCompleted | client reconnect with backoff | temporary unavailable, then catchup |

Additional note:

- `Code`: relay consumer uses `read(lastConsumed)->process->ack`.
- `Gap`: no implemented pending-entry claim/replay strategy; therefore “no relay loss across restart” cannot be strictly guaranteed from current code.

---

## 11. Non-Functional Design

### 11.1 Scalability

`Code`

1. platform-level multi-replica scaling
2. send throughput constrained mainly by worker threads and queue capacity
3. data bottlenecks depend on Cosmos query patterns and RU provisioning

### 11.2 Resilience and recovery

`Code`

1. client heartbeat and auto reconnect
2. catchup on reconnect
3. server-side session timeout cleanup

### 11.3 Observability

`Code`

1. SLF4J logs across critical paths
2. detailed send latency logs in send pipeline
3. ACA sample config enables Java metrics agent

`Gap`
No standardized in-code metrics/tracing instrumentation layer.

---

## 12. Configuration and Environment Design

### 12.1 Server configuration

`Code`

1. default Spring profile in `application.yml`: `dev`
2. `chat.send.worker-threads` default `8`, override via `SEND_WORKER_THREADS`
3. `chat.grpc.port`: dev via `CHAT_GRPC_PORT` (default 50051), prod via `PORT` (default 50051)
4. Redis SSL enabled in both dev/prod profile configs
5. `container.app.replica.name` injected from env; routing depends on uniqueness

### 12.2 Client configuration

`Code`

1. required: `TARGET`
2. required consistency: `IS_PROD` must match `TARGET` environment characteristics
3. optional: `CHAT_CLIENT_DEBUG_SIDEBAR`, `CHAT_CLIENT_HISTORY_PAGE_SIZE`, `CHAT_CLIENT_CATCHUP_LIMIT`
4. local DB path uses env prefix: `dev_*.db` / `prod_*.db`

---

## 13. Dependency Graph and Stack

### 13.1 Top-level versions

`Code`

1. Java 17
2. gRPC 1.64.0
3. Protobuf 3.25.3

### 13.2 Server dependencies

`Code`

1. Spring Boot 3.3.4
2. `grpc-netty-shaded`
3. `azure-cosmos` 4.65.0
4. Spring Data Redis
5. Spring Security Crypto
6. `dotenv-java` 3.0.0

### 13.3 Client dependencies

`Code`

1. `grpc-netty-shaded`
2. `sqlite-jdbc` 3.45.1.0
3. `java-dotenv` 5.2.2
4. Swing (JDK)

---

## 14. Versioning and Evolution Strategy

### 14.1 Protocol evolution

`Code + Inference`

1. Protobuf `oneof` enables event-type extension
2. Existing typo field `retriveMsgQuantity` should be migrated via additive compatible strategy, not in-place rename

### 14.2 Data evolution

`Code`

1. SQLite startup performs `CREATE IF NOT EXISTS` plus selective `ALTER`
2. Cosmos has no schema migration framework; evolution relies on read/write compatibility

`Gap`
No explicit schema version gating or automated rollback migration process.

---

## 15. Formal Invariants

### 15.1 Must-hold invariants

`Code`

1. conversation sequence expected monotonic via Redis `INCR`
2. `serverMsgId` is deterministic for `(senderUserId, clientMsgId)`
3. catchup/history only allowed for authorized conversation members
4. local mapping is effectively one active session per user, with latest session replacing prior one

### 15.2 Important boundary conditions

`Code + Inference`

1. `DELIVERED_LIVE` not emitted in ack path; client semantics should treat persistence success as the primary success condition
2. history query uses `<= beforeSequenceId`; caller must avoid duplicate render
3. if Redis routing hits but target sessionId mismatches, remote node drops relay delivery attempt

---

## 16. Explicit Gaps and Recommended Backlog

Prioritized:

1. `P0`: replace weak `x-user-id` resume with signed token model (expiry/revocation/rotation)
2. `P1`: add relay pending recovery strategy (claim/replay on restart/consumer failure)
3. `P1`: add unified metrics/tracing (queue depth, send latency, catchup cost, relay lag)
4. `P1`: add server-side rate limiting and abuse protections
5. `P2`: implement real `DELIVERED_LIVE` ack semantics if required by product behavior
6. `P2`: add automated tests (auth, idempotent send, cross-replica relay, history edge cases)
7. `P2`: improve secret hygiene (avoid sensitive config in exported manifests)

---

## Appendix A - Repository Truth Scope

This spec is derived from:

1. `chat-server/src/main/java/...`
2. `chat-client/src/main/java/...`
3. `chat-proto/src/main/proto/chat.proto`
4. parent/module `pom.xml`
5. `chat-server/src/main/resources/*.yml`
6. `chat-server/Dockerfile`
7. `chat-client/db/init.sql`

`docs/` and `specs/` text files are not treated as the source of truth.

