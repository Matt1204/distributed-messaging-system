# Heartbeat and Connection Liveness Design (Implemented)

This document explains the current heartbeat/liveness implementation across client and server.

Source of truth:

- `chat-client/src/main/java/com/coen6731/chat/client/HeartbeatManager.java`
- `chat-client/src/main/java/com/coen6731/chat/client/ChatClientSession.java`
- `chat-client/src/main/java/com/coen6731/chat/client/ServerResponseHandler.java`
- `chat-server/src/main/java/com/coen6731/chat/server/MessagingServiceImpl.java`
- `chat-server/src/main/java/com/coen6731/chat/server/ConnectionRegistry.java`
- `chat-server/src/main/java/com/coen6731/chat/server/RedisHandler.java`

---

## 1. Core issue: dual online state consistency

Current system maintains two online-state layers:

1. local in-memory state: `ConnectionRegistry.connectionsMap`
2. distributed routing state: Redis `user:online:{userId}` with TTL

Both need to converge after disconnects or failures to avoid routing to stale sessions.

---

## 2. Implemented timing parameters

1. client heartbeat ping interval: `10s`
2. client pong timeout per ping: `5s`
3. client reconnect trigger threshold: `3` missed pongs
4. server local session inactivity timeout: `30s`
5. server cleanup scan interval: `5s`
6. Redis presence TTL: `30s`

---

## 3. Server-side heartbeat behavior

### 3.1 Ping handling

On every `HeartbeatPing`:

1. server sends `HeartbeatPong`
2. if authenticated stream state is active:
   - update local session heartbeat (`ConnectionRegistry.updateHeartbeat`)
   - renew Redis TTL (`RedisHandler.renewUserOnline`)

For unauthenticated streams:

- pong is still returned
- no user heartbeat update
- no Redis presence renewal

### 3.2 Local timeout cleanup

`ConnectionRegistry` runs scheduled cleanup:

1. every 5s, scan local sessions
2. if `now - lastHeartbeat > 30000ms`:
   - remove local mapping
   - remove Redis online key
   - close stream with `TIMEOUT`

### 3.3 Crash/dead-instance cleanup

If server process dies before explicit cleanup:

1. Redis online key stops renewing
2. key expires by TTL
3. other replicas stop routing to stale instance/session

---

## 4. Client-side failure detection and reconnect

### 4.1 3-strikes model

`HeartbeatManager` logic:

1. schedule ping every 10s
2. after each ping, schedule one 5s pong-timeout task
3. on pong: reset miss counter to 0 and cancel pending timeout
4. on timeout: increment miss counter
5. when miss counter >= 3: trigger reconnect callback

### 4.2 Teardown before reconnect

`ChatClientSession.triggerReconnect()` calls teardown path:

1. stop heartbeat tasks
2. mark connection state false
3. complete current stream observer
4. shutdown gRPC channel
5. schedule reconnect with delay

### 4.3 Backoff and jitter

Implemented reconnect delay:

1. base delay starts at 1000ms
2. per-attempt random jitter: 0..500ms
3. exponential backoff up to 5000ms cap
4. successful inbound event resets delay to 1000ms

---

## 5. Post-reconnect synchronization behavior

When reconnect becomes healthy:

1. if client had authenticated state, it may attach `x-user-id` header when creating new stub
2. client sends catchup once per reconnect cycle (guarded by `catchupPendingAfterReconnect`)
3. catchup uses local SQLite conversation cursors as hints

This is the implemented recovery path for missed persisted messages.

---

## 6. Critical boundaries and risks

1. Header-based auth resume (`x-user-id`) is implemented but weak from security perspective.
2. Heartbeat gives transport liveness, not end-to-end delivery guarantee.
3. Redis TTL expiry and local timeout values are equal (30s); this is simple but may be aggressive under unstable networks.
4. No adaptive heartbeat interval logic is implemented.

---

## 7. Implementation checklist

1. `HeartbeatPing` and `HeartbeatPong` exist in proto and are wired in stream handlers.
2. Server heartbeat path updates local + Redis state only when authenticated.
3. Client 3-strikes reconnect path is active.
4. Reconnect backoff + jitter is active and capped.
5. Reconnect catchup trigger is active.

