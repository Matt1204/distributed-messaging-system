## 1. Overview (Spring Boot + ACA Replica + Redis Streams)

### 1.1 High-level architecture (logical, implemented)

**Client**
- Opens one gRPC channel and one long-lived bidi stream `Chat`.
- Sends auth/send/heartbeat/catchup/history events on that stream.

**Server (Spring Boot in Azure Container Apps replicas)**
- gRPC frontend + stream event handler (`MessagingServiceImpl`).
- Local session registry: `connectionsMap[userId] -> UserSession`.
- Distributed presence registry in Redis:
  - `user:online:{userId} -> {instanceId}:{sessionId}` with TTL.
- Cross-replica relay through Redis Streams.
- Durable state in Cosmos DB.

**Instance identity**
- server instance id comes from Spring property `container.app.replica.name`.
- in deployment this maps to env `CONTAINER_APP_REPLICA_NAME`.
- used in stream key/group naming and routing values.

---

## 2. Redis Streams model used in code

### 2.1 Core terms (as used)
- stream: append-only relay log per target instance
- entry: one relay record map
- consumer group: per-instance group
- consumer: one logical worker (`consumer:{instanceId}:main`)
- ack: `XACK` after processing

### 2.2 Key conventions
- Presence key: `user:online:{userId}`
- Presence value: `{instanceId}:{sessionId}`
- Sequence key: `conversation:latest_msg_sequenceId:{conversationId}`
- Instance inbound stream: `stream:instance:{instanceId}`
- Group: `cg:{instanceId}`

### 2.3 Consumption configuration (implemented)

Consumer loop behavior:

1. read with `count(10)` and `block(2s)`
2. offset mode: `ReadOffset.lastConsumed()`
3. process each record and then `ack`
4. on loop exception: log, sleep 1s, continue

---

## 3. Cross-instance delivery flow (implemented)

Assume sender on instance-A, recipient on instance-B.

1. **Routing lookup**
- sender instance checks local `connectionsMap` first
- if not local, reads `GET user:online:{toUserId}`
- parses `{targetInstanceId}:{targetSessionId}`

2. **Forwarding**
- sender publishes map payload to `stream:instance:{targetInstanceId}`
- payload fields currently include:
  - `toUserId`
  - `targetSessionId`
  - `serverMsgId`
  - `clientMsgId`
  - `conversationId`
  - `fromUserId`
  - `fromEmail`
  - `text`
  - `sentAtMs`
  - `sequenceId`

3. **Target consume + local delivery**
- target instance consumer reads record
- validates local session exists and `sessionId` matches `targetSessionId`
- converts map back to `InboundMessage`
- sends to local stream observer
- acknowledges stream entry

4. **Mismatch/offline behavior**
- if target session missing/mismatch, record is effectively dropped after processing/ack
- durability fallback is catchup/history from Cosmos

---

## 4. Presence and heartbeat integration

### 4.1 Online registration

On authenticated session activation:

1. create new `UserSession`
2. store in local `connectionsMap`
3. `SET user:online:{userId} {instanceId}:{sessionId} EX 30`
4. if previous local session existed, close old one with `DUPLICATE_LOGIN`

### 4.2 Online renewal

On authenticated `HeartbeatPing`:

1. update local session heartbeat timestamp
2. renew Redis TTL to 30s

### 4.3 Offline cleanup

Two cleanup paths:

1. active cleanup by server scheduler: timeout > 30s removes local map + Redis key
2. passive cleanup by Redis TTL expiry if process dies and renew stops

---

## 5. Sequence allocation and routing relationship

Routing and sequencing are separate concerns but both use Redis:

1. presence+relay use `user:online:*` and `stream:instance:*`
2. sequence allocation uses `conversation:latest_msg_sequenceId:*`

On Redis sequence-key miss:

1. server reads durable max sequence from Cosmos
2. initializes key with `setIfAbsent`
3. increments key to allocate next sequence

---

## 6. Failure behavior and boundaries

### 6.1 Implemented behavior

1. local delivery failure does not rollback persisted message.
2. relay delivery failure/mismatch does not rollback persisted message.
3. sender receives success ack after persistence, not after recipient consume.
4. reconnect catchup repairs missed live deliveries.

### 6.2 Known limitations

1. no pending-claim replay logic for consumer group pending entries.
2. no dead-letter queue policy in code.
3. no end-to-end exactly-once live delivery guarantee.

---

## 7. Operational checklist (current code alignment)

1. each replica has unique `container.app.replica.name`.
2. Redis connectivity and TLS config are valid.
3. consumer group exists (create-or-ignore behavior on startup).
4. heartbeat renewal path is active for authenticated users.
5. session timeout cleanup scheduler is running.
6. relay logs and routing logs are monitored.

---

## 8. Practical guidance for future hardening

1. add pending replay/claim strategy for unacked stream entries.
2. define explicit relay retry/DLQ policy.
3. harden auth resume mechanism (`x-user-id` replacement).
4. add structured metrics for relay lag, ack rate, and mismatch drops.

