# Distributed Messaging System — Architectural Specification

**Specification version:** v1
**Implementation baseline:** repository code at generation time
**Audience:** engineering teams evolving, reimplementing, or refactoring the system

This specification describes the implemented system as the primary source of truth. Statements marked **Inferred** describe intended operating policy where the repository does not encode a formal value or mechanism; they are not claims of existing implementation.

## 1. System Boundary and Context Analysis

### Purpose and objectives

The system is a distributed, one-to-one messaging application. It combines a Java Swing desktop client, a horizontally replicable Spring Boot/gRPC server, durable Azure Cosmos DB storage, and Redis coordination. Its primary objectives are to authenticate users, carry interactive chat traffic over one long-lived bidirectional stream, acknowledge messages after durable persistence, route live messages across server replicas, recover missed messages after reconnection, and retain a local offline-friendly message cache.

The persistence-first send model makes durable acceptance distinct from live delivery. A successful `PERSISTED_PENDING_DELIVERY` acknowledgement means the canonical message is stored; a recipient who is offline or temporarily unreachable can recover it through catchup. This separation is central to the design.

### Operating ecosystem and actors

The human actor uses `ChatWindow`, which owns login/register forms, conversation selection, message composition, timeline rendering, and history scrolling. The desktop process acts on the human's behalf through `ChatClientSession`, `ServerResponseHandler`, `HeartbeatManager`, and `DatabaseManager`.

The server runs one or more replicas. Each replica accepts gRPC streams, owns only its local live sessions, and coordinates with other replicas through Redis. Azure Cosmos DB is the system of record for users, conversations, and canonical messages. Redis owns ephemeral presence, per-conversation sequence allocation, and per-replica relay streams. The deployment platform supplies networking, environment variables, replica identity, and—in production—TLS reachability.

### Boundary ownership

Inside the repository boundary are the `chat-proto`, `chat-server`, and `chat-client` Maven modules; the desktop UI and SQLite cache; authentication, send, catchup, history, heartbeat, persistence, routing, and relay logic; runtime configuration; and the server container image.

Outside the boundary are Cosmos DB and Redis provisioning, DNS and load balancing, production TLS termination/certificates, secret distribution, user email ownership verification, host operating-system policy, and backup infrastructure. The server assumes the Cosmos database already exposes `users`, `conversations`, and `messages` containers with partition keys compatible with the calls in `CosmosDBHandler`. It assumes Redis supports string keys, atomic increments, TTLs, and Streams consumer groups.

### Boundary-crossing data and direction

Clients send `ClientEvent` envelopes containing login, registration, outbound message, heartbeat, catchup, or history payloads. Servers return `ServerEvent` envelopes containing authentication success, canonical inbound messages, send acknowledgements, heartbeat replies, catchup/history results, or structured errors. Server replicas write and query Cosmos records, write/read Redis presence and sequence keys, and publish/consume Redis Stream records. The client reads and writes a user-scoped SQLite database containing cached messages, conversation summaries, identity state, and sequence cursors.

Credentials cross the client/server boundary during login or registration and password hashes cross the server/Cosmos boundary. Canonical message content crosses client/server, server/Cosmos, and—during remote live delivery—server/Redis/server boundaries. Redis routing values expose replica and session identifiers but not credentials.

### Trust and security boundaries

The client is untrusted for sender identity, conversation membership, sequence allocation, timestamps, and server message IDs. The authenticated stream state in `MessagingServiceImpl` supplies sender identity; the service checks membership before history access and conversation reuse. Cosmos credentials and Redis credentials are server-side secrets supplied through configuration. SQLite is a local convenience store and is not authoritative.

`UserIdInterceptor` copies `x-user-id` metadata into gRPC context, and stream initialization may restore a session when that user exists. This is an implemented trust path; an evolution should replace it with verifiable credentials while preserving the stream state model. Passwords are hashed with BCrypt before storage. Transport security depends on deployment mode: the client selects TLS for the production target and plaintext for local development.

## 2. Overall Architecture Design

### Architectural style and rationale

The system is a hybrid distributed architecture: a modular server process is replicated horizontally; Redis supplies event-driven cross-node relay and shared ephemeral coordination; Cosmos supplies durable document persistence; and each desktop client maintains a local cache. It is not a microservice suite because authentication, messaging, routing orchestration, and persistence adapters execute in one Spring process. This arrangement keeps a side project operationally understandable while demonstrating distributed routing, asynchronous work, durable recovery, and clear module boundaries.

The structural view is:

```text
Human
  -> Swing ChatWindow
     -> ChatClientSession <-> bidirectional gRPC stream <-> Server Replica A
     -> ServerResponseHandler                            |-> local UserSession
     -> HeartbeatManager                                 |-> SendAsyncExecutor
     -> SQLite DatabaseManager                           |-> CosmosDBHandler -> Cosmos DB
                                                        |-> RedisHandler <-> Redis
Redis stream for Replica B -> Server Replica B -> local UserSession -> recipient client
```

### Runtime topology and deployment

The client is a Java 17 desktop process. It creates a Netty gRPC channel, one logical chat stream, scheduler threads for reconnect/heartbeat, and a SQLite database selected by environment and authenticated user. The server is a Java 17 Spring Boot process containing a separately managed gRPC server. `GrpcServerLifecycle` binds `chat.grpc.port`, registers `MessagingServiceImpl` and `UserIdInterceptor`, starts an await thread, and performs graceful shutdown followed by forced termination if necessary.

The multi-stage Dockerfile compiles the reactor with Maven 3.9/Temurin 17, copies only the repackaged server JAR into an Amazon Corretto 17 runtime, and runs it as a non-root `spring` user. The runtime defaults to the `prod` Spring profile and exposes port 50051. Configuration anticipates a container platform that provides a unique replica name.

### Scalability and fault tolerance model

Server replicas are stateless with respect to durable business data and can scale horizontally. Local live connections remain replica-affine, while Redis maps each online user to `{instanceId}:{sessionId}` and relays deliveries to the owning replica. Cosmos scales durable reads/writes according to its partition and RU configuration. Send work is isolated from gRPC callbacks by a fixed worker pool and a bounded queue of 30,000 tasks, providing explicit backpressure through `OVERLOADED` acknowledgements.

Durable messages permit recovery when live delivery fails. Heartbeats remove stale local sessions and allow presence TTLs to expire. Clients reconnect with bounded exponential backoff and jitter and issue catchup after restored authentication. Redis sequence initialization consults Cosmos's durable maximum before atomic increments, connecting ephemeral allocation to durable history.

### Multi-region posture and trade-offs

Multi-region operation is **Inferred, not explicitly configured**. Cosmos and Redis endpoints can be region-independent configuration values, but the code defines no regional routing, active-active conflict policy, or latency-aware replica selection. A v2 multi-region design should assign sequence authority per conversation and specify Redis/Cosmos failover behavior before active-active writes.

One multiplexed gRPC stream simplifies session and ordering context but couples all feature traffic to one connection. Redis enables fast routing and atomic counters but introduces an availability dependency for sequence allocation and cross-replica delivery. SQLite improves responsiveness and recovery but requires idempotent merge logic. A broker-per-message design could offer stronger relay retention; the present design instead relies on durable Cosmos messages plus catchup for product-level recovery.

## 3. Complete Module Breakdown

### `chat-proto`

This module owns the language-neutral transport contract and generated Java types. It exposes `MessagingService.Chat`, a bidirectional streaming RPC whose request and response envelopes use `oneof` payloads. It does not own authentication decisions, persistence, routing, or UI behavior. Maven invokes Protobuf 3.25.3 and the gRPC Java 1.64.0 generator. Compatibility depends on preserving field numbers and enum semantics.

### Server bootstrap and lifecycle

`ChatServer` loads optional dotenv values from `chat-server`, promotes values into system properties when the environment has not supplied them, and starts Spring Boot. `GrpcServerLifecycle` owns the native gRPC server, port binding, service registration, interceptor registration, await thread, and shutdown sequence. It uses Spring `SmartLifecycle`, so infrastructure adapters are injected before gRPC begins serving. Startup failure is surfaced as an illegal state; shutdown waits up to 30 seconds before forcing closure.

### `MessagingServiceImpl`

This component is the application orchestration boundary. Each `chat()` call creates stream-local mutable authentication/session state and returns a request observer that dispatches the six client event variants. It validates input, normalizes emails, verifies or creates accounts, activates sessions, responds to heartbeat, submits send work, authorizes catchup/history, maps persistence records to Protobuf, and translates failures into server events or failed acknowledgements.

It owns no database state directly. It delegates durable operations to `CosmosDBHandler`, ephemeral sequence/routing operations to `RedisHandler` through `ConnectionRegistry`, live stream synchronization to `UserSession`, and background send work to `SendAsyncExecutor`. Send text is bounded to 4,096 characters; catchup defaults to 50 messages per conversation and request pages cap at 200. Per-stream state is confined to its observer instance, while asynchronous send tasks capture identity and session snapshots explicitly.

### Authentication and interception

`UserIdInterceptor` extracts the ASCII `x-user-id` metadata header and stores it in gRPC `Context`. It permits anonymous stream creation; event handling enforces authentication. `MessagingServiceImpl` uses `BCryptPasswordEncoder` for registration hashes and login verification, and normalizes email by trimming and lowercasing. An authenticated stream registers exactly one current local `UserSession` for a user.

### `ConnectionRegistry` and `UserSession`

`ConnectionRegistry` owns the replica-local `ConcurrentHashMap<userId, UserSession>`. Online activation replaces a prior session and closes it with `DUPLICATE_LOGIN`. Offline removal uses compare-by-observer semantics so an older stream cannot remove a newer session. A daemon scheduler scans every five seconds and expires sessions whose heartbeat is older than 30 seconds. Presence changes are mirrored to Redis.

`UserSession` owns a UUID session ID, the outbound `StreamObserver`, and a volatile last-heartbeat timestamp. Its `send` and `close` methods synchronize access because gRPC `StreamObserver` is not thread-safe. The session module does not determine authorization or persistence; it is a concurrency-safe transport handle.

The registry also mediates local and remote delivery. A remote Redis record is converted into `InboundMessage` only if the current local session ID matches the record's target. This prevents delivery to a replacement login. Relay publication includes canonical IDs, participant IDs, content, timestamp, and sequence.

### `RedisHandler`

This adapter owns Redis key conventions and the single-threaded relay consumer loop. Presence keys are `user:online:{userId}`, values are `{instanceId}:{sessionId}`, and TTL is 30 seconds. Conversation counters are `conversation:latest_msg_sequenceId:{conversationId}` and use `SETNX` plus `INCR`. Relay streams are `stream:instance:{instanceId}` with group `cg:{instanceId}` and consumer `consumer:{instanceId}:main`.

The consumer blocks for up to two seconds, reads ten records at a time, validates routing fields, delegates local delivery, and acknowledges processed records. On operational exceptions it logs, waits one second, and resumes while running. Its state is the running flag and executor; durable message truth remains in Cosmos.

### `CosmosDBHandler`

This adapter owns all Cosmos access. At startup it constructs a synchronous `CosmosClient` and handles for `users`, `messages`, and `conversations`; at shutdown it closes the client. It maps untyped documents into `UserRecord`, `ConversationRecord`, and `MessageRecord` records.

User operations create normalized-email records and query by email or point-read by ID. Conversation operations find by ID, create if absent while enforcing both members, list conversations containing a member, and patch last-message timestamps. Message operations create canonical records idempotently, find by server ID, compute maximum sequence, count/fetch missing ranges, and fetch descending history. Cosmos exceptions are contained and converted into optional/list/result values for the application layer.

The adapter owns persistence transformations but not business identity derivation, authorization flow, live delivery, UI projection, or retry policy. Synchronous calls execute in the send worker for send operations and in gRPC callback context for authentication/catchup/history.

### `SendAsyncExecutor`

The executor provides a fixed number of configurable daemon workers, a 30,000-element `ArrayBlockingQueue`, non-blocking submission, and rejection signaling. It tracks submitted, completed, and rejected counts with low-contention counters and returns snapshots used in structured latency logs. Shutdown drains for eight seconds and then reports forced drops. This boundary prevents slow Cosmos/Redis calls in send processing from occupying gRPC inbound callbacks.

### Client bootstrap and session transport

`ChatClient` loads client dotenv configuration, validates `TARGET`/`IS_PROD` consistency, reads catchup/history/UI tuning, creates `ChatClientSession`, and launches Swing UI. `ChatClientSession` owns channel creation, stream lifecycle, authentication requests and their 15-second wait, outbound sends, catchup/history requests, user-scoped database attachment, reconnection, and orderly close.

The session uses atomic flags for connected, reconnecting, closing, authenticated, and pending-catchup state; an atomic latch reference coordinates authentication responses. Reconnect delay begins at one second and caps at five seconds with jitter. Production channels use transport security; local channels use plaintext. Client request sends are synchronized/guarded around the current observer to avoid writing through a stale stream.

### `HeartbeatManager`

The heartbeat module schedules a ping every ten seconds and a five-second pong deadline. It records consecutive misses and asks the session to reconnect after three misses. A pong resets health. It owns liveness timers, not the channel or authentication state, and it is stopped during session teardown.

### `ServerResponseHandler`

This response demultiplexer converts every `ServerEvent` variant into local state and UI callbacks. Authentication success updates session state and opens the proper user database. Send acknowledgements reconcile provisional outbound rows. Inbound, catchup, and history messages pass through canonical idempotent database writes before display. Stream error/completion signals initiate reconnect unless closing. It separates transport callbacks from durable/UI side effects.

### `DatabaseManager`

This module owns the SQLite connection, schema initialization/evolution, message CRUD, conversation summaries, authenticated user state, and local cursors. It serializes database access through synchronized methods/connection use and maps rows into client-facing records. The message table distinguishes `INBOUND` and `OUTBOUND` using a composite primary key, while additional indexes support canonical IDs, sequence ordering, and conversation timelines. It is a cache/projection; conflicts resolve in favor of canonical server fields.

### `ChatWindow` and `ClientUiListener`

`ChatWindow` is the presentation and interaction module. It renders authentication and chat views, sends commands through the session, renders conversation previews and message bubbles, and implements range-based history backfill. Swing state mutations run on the Event Dispatch Thread; longer database/network planning uses asynchronous work and generation checks. `ClientUiListener` is the callback boundary through which transport/database results trigger view updates without importing Swing into lower layers.

The history UI tracks the rendered sequence window, an in-flight request per conversation, selection generations, and a two-stage top-scroll gate. It computes missing sequence segments, requests only missing ranges, and re-renders from SQLite. This makes local persistence the immediate rendering source and network results incremental inputs.

## 4. Inter-Module Relationships and Communication

### Client-to-server stream

`ChatClientSession` and `MessagingServiceImpl` communicate asynchronously over one bidirectional gRPC/HTTP2 stream using generated Protobuf schemas. The stream itself has no per-event transaction. Authentication establishes stream-local authority; subsequent send, heartbeat, catchup, and history events share it. Client reconnect uses exponential backoff; individual application events do not have an automatic transport retry contract. The deterministic client message ID supports an explicit resend without duplicate canonical persistence.

The client heartbeat policy supplies a five-second response deadline and three-strike reconnect threshold. Other RPC-like events multiplexed on the stream have application-specific waits: authentication waits up to 15 seconds, and history completion is coordinated by client state. gRPC errors close the stream and propagate to reconnect logic.

### Application-to-Cosmos communication

`MessagingServiceImpl` calls `CosmosDBHandler` synchronously. Registration, conversation creation, and message creation each form separate Cosmos item operations; the end-to-end send is not a distributed transaction. The ordering is recipient resolution, conversation resolution, sequence allocation, message persistence, conversation timestamp touch, sender acknowledgement, then live delivery. Therefore durable persistence is the success boundary, while conversation preview update and live delivery are secondary side effects.

Idempotency is implemented by deterministic `serverMsgId` for `(senderUserId, clientMsgId)` and create-once semantics. A Cosmos 409 causes lookup of the existing record and reuse of its canonical fields. Conversation creation similarly resolves 409 races and verifies membership. Timeouts/retries are primarily Azure SDK defaults; no application circuit breaker overrides are encoded.

### Application-to-Redis communication

Sequence allocation is synchronous and strongly serialized per Redis key through atomic `INCR`. When the key is absent, the server reads Cosmos's durable maximum, attempts `SETNX`, then increments. Presence registration/renewal/removal is synchronous best effort in the session lifecycle. Routing reads return a single route token.

Cross-replica relay is asynchronous: the sending replica appends a map record to the recipient replica's stream. The target consumer reads in group order, attempts session-ID-guarded delivery, and acknowledges. Stream ordering is Redis record order per target stream; application-visible ordering is ultimately represented by the per-conversation `sequenceId`. Catchup from Cosmos repairs missed live presentation.

### Client-to-SQLite and UI communication

Outbound messages are inserted as provisional local rows before network send. Successful acknowledgements replace provisional identifiers/status with canonical fields; failed acknowledgements remove the provisional record. Inbound, catchup, and history paths upsert canonical rows. Database uniqueness and merge behavior make repeated server events idempotent. UI refreshes read SQLite rather than treating the network callback payload as the sole view model.

### Normal flow

A user authenticates; the server registers local and Redis presence. On send, the client generates a `clientMsgId`, stores a pending row, and emits `OutboundMessage`. A worker resolves recipient/conversation, allocates sequence, persists the message, acknowledges the sender, and delivers locally or via Redis. The receiver stores the canonical inbound row and updates its conversation view. The sender reconciles its pending row with the acknowledgement.

### Failure, retry, and partial outage flows

If the send queue rejects, the sender immediately receives `FAILED/OVERLOADED`; no durable write occurred and an explicit new attempt is safe. If Cosmos persistence fails, the sender receives `PERSISTENCE_FAILED`; no success acknowledgement is emitted. If live delivery or Redis routing fails after persistence, the durable success remains valid and the recipient obtains the message through later catchup. If the stream fails, the client tears down heartbeat/channel state, reconnects with backoff, restores authentication, and catches up.

If Redis is unavailable before sequence allocation, sending cannot produce a valid canonical sequence and fails. If it becomes unavailable only during presence/relay after persistence, real-time reachability degrades while durable history remains. If a recipient logs in again between route lookup and relay consumption, the relay's session-ID check prevents delivery to the replacement session; catchup supplies the durable message.

No application-level circuit breaker is encoded. Backpressure is the bounded executor; SDK and transport layers govern lower-level connection timeouts. Transaction consistency is hybrid: each Cosmos item write is atomic, Redis increments are atomic, and end-to-end send uses ordered compensating/recovery behavior rather than a cross-store transaction.

## 5. Domain Model and Behavior Design

### Users and authenticated sessions

`UserRecord` contains canonical `userId`, normalized email, and BCrypt password hash. Email is the login and recipient lookup key; user ID is the durable identity and server-side sender identity. A stream transitions from unauthenticated to authenticated once through login, registration, or the implemented resume metadata path. Authenticated state permits send, catchup, history, and heartbeat-associated presence. Duplicate login replaces the prior local session.

The invariant is that outbound sender identity comes from authenticated stream state, never from `OutboundMessage`. Registration requires nonblank email/password and an unused normalized email. Login requires an existing record and BCrypt match.

### Conversations

`ConversationRecord` owns `conversationId`, two member user IDs, and creation/update/last-message timestamps. An explicit conversation may be reused only when both sender and recipient are members. A blank requested ID creates a UUID conversation. History and catchup require the authenticated user to be in `memberUserIds`.

The conversation is the consistency and ordering scope. Its last-message time drives server catchup sorting and client conversation summaries. Group chat is outside the current behavior even though the member field is a list.

### Canonical messages

`MessageRecord` owns deterministic `serverMsgId`, caller-generated `clientMsgId`, conversation and participant IDs, per-conversation sequence, text, send time, persistence status, and audit timestamps. The deterministic server ID makes a repeated `(senderUserId, clientMsgId)` refer to the same logical message. The sequence ID establishes conversation order independently of wall-clock time.

The server-side state transition is:

```text
validated command
  -> queued
  -> conversation/sequence resolved
  -> durable PERSISTED_PENDING_DELIVERY
  -> optional live presentation
or -> FAILED before durable acceptance
```

`DELIVERED_LIVE` is defined in the protocol as an extensibility state; durable acceptance is the implemented success contract. A failed send cannot transition the provisional local row into canonical success without a later successful resend/ack.

### Cursors, catchup, and history

A conversation cursor is the greatest canonical sequence known locally for one user/conversation. Catchup compares client hints with Cosmos's durable maximum and returns the newest missing window per authorized conversation. History requests retrieve messages at or before a sequence boundary in descending order, allowing the UI to backfill older ranges.

Canonical database upsert protects idempotency when windows overlap. The client cursor advances from canonical sequences and must never be derived solely from arrival time. Messages shown in a conversation must belong to that conversation, and history cannot cross its membership boundary.

### Side effects and consistency boundaries

Message persistence is the primary transaction boundary. Conversation timestamp update, sender acknowledgement, and live delivery follow it. Redis presence is an expiring projection of session state. SQLite is a projection of server truth with provisional local state. Infrastructure concerns are contained behind `CosmosDBHandler`, `RedisHandler`, `DatabaseManager`, and gRPC-generated types; orchestration rules remain in the service/session layers.

## 6. Data Architecture

### Cosmos DB

Cosmos DB is selected for durable, horizontally scalable document storage and direct compatibility with cloud deployment. The `users` container stores `id=userId`, `userId`, normalized `email`, `passwordHash`, `createdAt`, and `updatedAt`. User point reads use `userId`; email login uses a parameterized SQL query.

The `conversations` container stores `id=conversationId`, `conversationId`, `memberUserIds`, `createdAtMs`, `updatedAtMs`, and `lastMessageAtMs`. Create and point operations use a conversation partition key; membership listing uses `ARRAY_CONTAINS` and may span partitions depending on provisioning.

The `messages` container stores `id=serverMsgId`, canonical IDs, conversation ID, sequence, sender/recipient IDs, text, send/status/audit fields. Writes use `conversationId` as partition key. Sequence-range reads order by `sequenceId`; implementation queries support maximum, count, ascending missing ranges, newest missing ranges, and descending history.

**Inferred indexing requirements:** production provisioning should index normalized user email, conversation membership, conversation timestamps, and message `(conversationId, sequenceId)`. A composite index supporting sequence ordering within conversation is appropriate. These are infrastructure requirements inferred from query shapes, not repository-managed IaC.

### Redis

Redis is the ephemeral coordination and routing layer. Presence keys expire after 30 seconds and are renewed on heartbeat. Sequence counters persist as string integers and are initialized from Cosmos maximum when absent. Per-instance streams carry live relay fields; consumer groups isolate processing per target replica. Presence can be rebuilt from connected sessions; sequence counters can be reinitialized from durable messages; relay loss is recoverable at the product level through catchup.

### SQLite

The `messages` table uses `(client_msg_id, direction)` as primary key and stores canonical identifiers, participants, text, send time, status, and sequence fields introduced through startup migration logic. Indexes support conversation/time retrieval, server-ID lookup, and canonical uniqueness by conversation/server ID or conversation/sequence. The `conversations` table stores peer identity, last-message time, and preview. `user_state` stores the current local identity. `local_conversation_cursor` keys the latest sequence by `(user_id, conversation_id)`.

The client creates missing tables/indexes and performs selective additive alterations at startup. Backward compatibility is therefore additive: new nullable/defaulted columns and indexes may be introduced without invalidating old databases. Cosmos has no repository-managed migration runner; readers tolerate mapped document values and writers emit the current shape.

### Lifecycle, caching, and consistency

No archival/retention deletion policy is encoded. **Inferred:** durable messages and conversations are retained indefinitely unless Cosmos lifecycle policy is configured externally; Redis presence expires automatically; relay stream retention is externally governed; SQLite persists until the local database is removed. Any v2 retention policy must preserve conversation cursor semantics and clearly distinguish deleted content from missing pages.

The overall consistency model is hybrid. Cosmos item operations and Redis increments are individually strong at their operation boundaries; presence and cross-replica delivery are eventually consistent; SQLite converges through acknowledgements, inbound events, catchup, and history. There is no read/write replica separation in application code.

## 7. API and Contract Design

### Public gRPC API

The sole public service is:

```protobuf
rpc Chat(stream ClientEvent) returns (stream ServerEvent);
```

`ClientEvent` payloads are `LoginUser(email,password)`, `RegisterUser(email,password)`, `OutboundMessage(toEmail,text,clientMsgId,conversationId)`, `HeartbeatPing(ts)`, `CatchupRequest(cursorHints,perConversationLimit)`, and `GetMsgHistoryRequest(conversationId,beforeSequenceId,retriveMsgQuantity)`.

`ServerEvent` payloads are `AuthSuccess`, `InboundMessage`, `SendMessageAck`, `HeartbeatPong`, `CatchupResult`, `MsgHistoryResult`, and `ServerError`. `CanonicalMessage` is the common durable representation used by synchronization results. `InboundMessage` repeats canonical fields for real-time delivery.

### Error and acknowledgement model

General failures use `ServerError(code,reason)`. Send-specific outcomes use `SendMessageAck`, preserving `clientMsgId` for local reconciliation. Implemented codes include `BAD_REQUEST`, `AUTH_NOT_AUTHENTICATED`, `AUTH_INVALID_CREDENTIALS`, `AUTH_EMAIL_ALREADY_EXISTS`, `INTERNAL`, `RECIPIENT_NOT_FOUND`, `CONVERSATION_INVALID`, `PERSISTENCE_FAILED`, and `OVERLOADED`. The sender treats `PERSISTED_PENDING_DELIVERY` as durable success and `FAILED` as unsuccessful acceptance.

### Internal contracts and event schemas

The Redis presence contract is `user:online:{userId} -> {instanceId}:{sessionId}`. The sequence contract is `conversation:latest_msg_sequenceId:{conversationId} -> decimal integer`. Relay topics are per-instance streams `stream:instance:{instanceId}`. Relay records contain `toUserId`, `targetSessionId`, `serverMsgId`, `clientMsgId`, `conversationId`, `fromUserId`, `fromEmail`, `text`, `sentAtMs`, and `sequenceId`.

Consumer isolation is by target instance consumer group. The target session ID is part of the contract to prevent stale-route delivery. Record evolution should be additive; consumers normalize absent text fields and numeric parsing defaults, while routing identifiers remain mandatory.

### Versioning, deprecation, auth, and rate policy

The current API has no explicit package version or deprecation schedule. Protobuf evolution must retain existing field numbers, add new payload fields/event variants with new numbers, reserve removed identifiers, and treat the misspelled `retriveMsgQuantity` as a wire-compatible public name until an additive replacement exists.

Authentication is stream-level rather than per-event token validation. Authorization is enforced by authenticated sender state and conversation membership. No application rate limiter is encoded; bounded send execution supplies resource backpressure but is not an identity-based quota. **Inferred:** production evolution should add authenticated principal tokens and per-user/IP limits without changing canonical message idempotency.

## 8. Security Architecture

Passwords are hashed and verified with BCrypt; plaintext passwords are only request inputs. Emails are normalized before lookup and storage. Server-side authenticated state owns sender identity, conversation membership gates history, and session replacement prevents two local streams from simultaneously representing the same user mapping. Parameterized Cosmos queries are used for user-controlled values.

The implemented session model is one authenticated identity per stream and one current `UserSession` per user per registry. There is no role hierarchy because all users have the same messaging role; permissions are object-scoped: send as self, read member conversations, and receive messages addressed to self. Multi-tenant organization boundaries are not part of the domain.

Production transport uses TLS from the client when configured for the production endpoint; Redis SSL is enabled in both server profiles. Cosmos/Redis encryption at rest and key rotation are platform responsibilities. Secrets enter through environment/Spring configuration, and the Docker image runs as a non-root user. No application key-management provider is embedded.

The principal threat surfaces are credential guessing, forged resume identity, unauthorized history access, message flooding, malicious content, secret leakage, and stale-session routing. Existing mitigations include BCrypt, membership validation, message length limit, deterministic idempotency, bounded work queue, Redis TTL, and session-ID relay checks. **Inferred evolution requirements:** replace the resume header with signed expiring tokens, add account throttling and audit events, validate endpoint certificates consistently, rotate platform secrets, and apply content/log redaction policies.

Logs provide operational traces of replica, user IDs, client message IDs, routing, and failures. They do not form a formal immutable audit ledger. A future audit model should record authentication, session replacement, authorization denial, and administrative security events without recording passwords or unnecessary message content.

## 9. Non-Functional Design

### Performance and scalability

The send path is designed for concurrency through a configurable fixed worker pool (default eight) and bounded queue. Per-session sends are serialized; separate users can be delivered concurrently. Redis routes in O(1) key lookup and consumes relay batches of ten. Cosmos message reads are bounded by normalized limits, with a maximum of 200 per requested page; catchup defaults to 50 per conversation.

No formal latency SLO is encoded. Implemented send logs split queue wait, worker execution, and total duration and include worker/queue/counter snapshots. **Inferred initial targets for v2 validation:** measure durable acknowledgement p50/p95/p99, queue utilization/rejection, catchup page latency, Redis relay lag, and reconnect recovery duration before establishing contractual SLOs.

### Load behavior, resilience, and availability

The queue rejects rather than growing without bound. Authentication/history currently use synchronous adapter calls in inbound processing, so scaling must consider both gRPC callback capacity and Cosmos RU/query performance. Per-conversation sequence keys serialize allocation for a hot conversation. Cross-partition user/member queries may become cost centers as data grows.

Availability depends on server reachability, Cosmos for authentication/durable messaging, and Redis for new sequence allocation and distributed live routing. Live-delivery degradation does not invalidate persisted messages; reconnect/catchup is the main recovery mechanism. Presence expires automatically, stale sessions are removed, and client reconnect uses jitter to reduce synchronized retries.

### Observability

SLF4J logging covers startup, authentication, routing, cleanup, Redis consumer behavior, persistence errors, reconnect state, and send latency. Executor snapshots expose worker count, active workers, queue depth, submitted, completed, and rejected values inside logs. The design does not embed a metrics registry or distributed tracer.

**Inferred observability model:** production should emit structured JSON with timestamp, level, replica ID, operation, correlation IDs (`clientMsgId`, `serverMsgId`, `conversationId`), latency, and outcome; message text and credentials must be excluded. Metrics should cover connection count, authentication outcomes, queue depth/rejection, Cosmos/Redis latency/errors, relay pending/lag, catchup volume, heartbeat timeouts, and reconnects. Traces should propagate a correlation context through gRPC, worker, Cosmos, and Redis relay boundaries. Alerts should focus on sustained error/rejection rates, queue saturation, storage dependency failures, and abnormal reconnect/timeout rates.

## 10. Configuration and Environment Design

The server uses Spring profiles. `application.yml` selects `${SPRING_PROFILES_ACTIVE:dev}` and configures `${SEND_WORKER_THREADS:8}`. Development takes Cosmos endpoint/key/database, replica name, gRPC port, and Redis host/port/password from environment. Production fixes the Cosmos database name to `chat-server-prod`, maps gRPC port from `PORT`, and retains environment-supplied infrastructure credentials. Redis TLS is enabled.

The client uses dotenv/environment values including `TARGET`, `IS_PROD`, catchup/history page tuning, and debug-sidebar configuration. It validates production/target alignment and separates local database filenames by environment and user. This prevents a development cache from being treated as production state.

Feature flags are limited to configuration toggles such as debug UI and numeric tuning; there is no centralized dynamic flag service. CI configuration and infrastructure-as-code are not present in the repository. Maven provides reproducible module compilation and Protobuf generation, while the Dockerfile provides image construction.

**Inferred rollout policy:** build and test all three modules, publish an immutable image, deploy a canary replica with backward-compatible Proto/data changes, observe dependency and queue metrics, then roll out gradually. Blue/green is viable because durable state is external, but clients with long-lived streams require connection draining and compatibility across adjacent versions. Rollback must never require reverting an already-written incompatible document or wire field.

## 11. Dependency Graph and Technology Stack

The reactor targets Java 17 and contains `chat-proto`, `chat-server`, and `chat-client`. Both runtime modules depend inward on generated `chat-proto`; neither imports the other. The server layers are bootstrap/lifecycle -> application orchestration -> session/executor and persistence/routing adapters -> external services. The client layers are bootstrap/UI -> session/response/liveness -> local database and generated transport. This direction should be preserved during refactoring.

Core managed versions are gRPC 1.64.0 and Protobuf 3.25.3. `chat-proto` uses `protobuf-maven-plugin` 0.6.1 and OS classifier plugin 1.7.1. The server uses Spring Boot 3.3.4, Azure Cosmos SDK 4.65.0, Spring Data Redis, Spring Security Crypto, shaded Netty gRPC, and dotenv-java 3.0.0. The client uses shaded Netty gRPC, SQLite JDBC 3.45.1.0, java-dotenv 5.2.2, and JDK Swing.

External services are Azure Cosmos DB, Redis, and the deployment/network platform. Maven and Java base images are build/runtime dependencies. Version upgrades should be staged: generated Protobuf/runtime versions must remain compatible; Spring Boot governs its dependency BOM; Cosmos and Redis client upgrades require integration tests against provisioned services; SQLite upgrades require migration and existing-cache tests.

Semantic breaking changes include reused Proto tags, changed acknowledgement meaning, incompatible Cosmos partition keys, destructive SQLite schema changes, and altered Redis key/record semantics. Such changes require additive migration or dual-read/write periods rather than in-place replacement.

## 12. Failure Analysis

### Server replica and connection lifecycle

A replica failure drops its local streams and stops renewing presence. Redis TTL removes stale routes within approximately 30 seconds; clients detect transport/pong failure and reconnect to an available replica. Durable messages remain in Cosmos. Replica recovery requires no local server data restore. **Inferred RTO:** bounded primarily by client detection plus reconnect backoff; **RPO:** zero for messages acknowledged after Cosmos persistence, while unacknowledged in-flight commands may require explicit retry.

### Cosmos DB

Cosmos unavailability prevents account lookup, conversation resolution, durable send, and synchronization. The service returns structured errors or empty outcomes and does not claim persistence. Cosmos is therefore the durable single-service dependency. Backup/restore, geo-replication, and point-in-time recovery are external responsibilities. **Inferred targets:** RPO and RTO should match configured Cosmos continuous backup/failover policy; the application must validate restored maximum sequences before Redis counters resume.

### Redis

Redis unavailability can prevent sequence allocation, presence discovery, and remote live relay. New sends that cannot allocate a positive sequence fail before persistence; already persisted messages remain recoverable. Presence and counters are reconstructible, but temporary loss increases catchup reliance. Redis persistence/replication and stream backup are externally managed. **Inferred RTO:** dependency failover plus client catchup; **RPO:** durable message RPO remains Cosmos-defined, while ephemeral live relay/presence may be lost without product data loss.

### Send executor

Queue saturation causes explicit `OVERLOADED` failure and prevents memory exhaustion. Worker shutdown waits eight seconds before dropping queued tasks; commands without success acknowledgement remain retryable. Monitoring queue age and rejection is essential to prevent latency cascades into Cosmos/Redis. Scaling worker count must be coordinated with storage capacity rather than increased independently.

### Relay stream and partial delivery

The consumer processes then acknowledges records. A delivery exception is logged, and canonical storage remains the recovery source. Session mismatch intentionally suppresses delivery to a new session. Consumer/stream health failure degrades live delivery for that replica but not history. Pending-entry recovery policy is not encoded; v2 can add claim/replay while retaining idempotent client storage.

### Client and SQLite

Client process failure loses only volatile UI/transport state and possibly unacknowledged commands; SQLite retains local projections. Database corruption or deletion causes cache loss, followed by reconstruction through authentication, catchup, and paged history subject to server retention. Backups are a user/host concern. UI background work uses generation and in-flight guards to avoid stale selection updates.

### Failure cascades and disaster recovery

Slow Cosmos calls can occupy all send workers and fill the queue; rejection then limits the cascade. Redis outages can convert normal messaging into failures before persistence and increase reconnect/catchup work afterward. A reconnect storm can amplify authentication and catchup queries; jitter and bounded pages partially control it. Disaster recovery order should restore Cosmos, validate containers/partitioning, restore Redis connectivity, initialize counters from durable maxima, start replicas with unique IDs, and then allow clients to reconnect gradually.

## 13. Versioning and Evolution Strategy

The Maven project currently identifies as `1.0-SNAPSHOT`; a release process should adopt semantic versioning for application artifacts. Patch releases preserve all contracts, minor releases add backward-compatible event fields/types, and major releases may alter behavior only through a migration window.

Protobuf evolution is additive. Existing field numbers and enum numeric values are immutable; removed tags/names must be reserved. Clients and servers from adjacent releases should interoperate, with unknown `oneof` variants ignored or surfaced safely. The `retriveMsgQuantity` field cannot simply be renamed on the wire; add a correctly named field with a new number, accept both, prefer the new field, and deprecate the old over multiple client release cycles.

Cosmos changes should use tolerant readers and additive writers, followed by backfill, validation, and only then removal of legacy reads. Partition-key changes require new containers and controlled migration. SQLite migrations should remain idempotent and transactional where supported, be tested from every supported prior schema, and preserve canonical uniqueness/cursors. Redis key or relay schema changes require versioned prefixes or dual consumers during rolling deployment.

Contract tests should compile old/new Proto clients against a compatibility matrix and exercise login, deterministic resend, catchup, history boundaries, and unknown fields. Integration tests should cover Cosmos conflicts, Redis counter reinitialization, cross-replica relay/session replacement, and client SQLite migration. Deprecation timelines are **Inferred** and should span at least the supported desktop client upgrade window because clients are not deployed atomically with servers.

Compatibility enforcement belongs in CI through Protobuf breaking-change checks, migration tests, and mixed-version integration suites. Rollouts should remain canary/gradual, with durable schema writes delayed until all rollback candidates can read them.

## 14. Formal Consistency and Invariants

### System-wide invariants

An acknowledged durable success corresponds to a canonical Cosmos message. Live delivery is never a prerequisite for that acknowledgement. Each active registry maps a user to at most one current local session, and relay delivery requires the route's session ID to equal that current session. Verification belongs in send integration tests, duplicate-login tests, and cross-replica race tests.

### Data invariants

`serverMsgId` is deterministic for `(senderUserId, clientMsgId)`, making retries converge on one canonical message. A canonical message belongs to exactly one conversation and contains its authenticated sender and resolved recipient. Sequence IDs are positive and monotonically allocated per conversation through Redis; durable maximum is the recovery baseline when a counter is missing. Conversation reuse requires both participants to be members. SQLite canonical uniqueness and upsert behavior prevent duplicate display across inbound, catchup, and history paths.

These invariants are enforced through deterministic UUID derivation, Cosmos create conflict handling, conversation membership checks, Redis atomic increment, and local unique indexes/merge logic. They should be verified under concurrent duplicate sends and counter initialization races.

### Transaction invariants

No success acknowledgement precedes message persistence. Conversation touch and live delivery occur only after a canonical record is obtained. Failure before persistence must produce a failed acknowledgement or stream failure, never a success. Because Cosmos, Redis, and SQLite do not share a transaction, recovery uses idempotency and ordered side effects. Tests should inject failure at every boundary and assert the allowed observable state.

### Security invariants

Client-provided payloads cannot choose sender user ID, sequence ID, server message ID, or canonical timestamp. History and catchup data are restricted to conversations containing the authenticated user. Stored passwords are BCrypt hashes. Credentials and message bodies must not be added to routine logs. A future token mechanism must cryptographically bind identity and expiry while retaining these authorization checks.

### Operational invariants

Every server replica requires a unique nonblank replica identity for Redis routing. Presence TTL and server heartbeat timeout remain aligned at 30 seconds, while the client ping/deadline/strike policy detects broken streams early enough to reconnect. Redis counter recovery must consult durable maximum before allocation. Queue capacity is finite and rejection is observable. Shutdown stops schedulers/consumers/executors and closes gRPC/Cosmos resources.

Operational verification should continuously compare persisted sequence maxima with allocated counters, alert on sustained queue rejection or relay errors, exercise restore/reconnect/catchup drills, and validate that mixed-version deployments preserve Protobuf, Redis, Cosmos, and SQLite contracts.
