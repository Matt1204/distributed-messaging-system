### 3.2 Consistency and Heartbeat Mechanism

#### Core issue: dual-state consistency

The system maintains two online states:
1. local server state in `connectionsMap`
2. distributed routing state in Redis (`user:online:{userId}`)

If server instances crash or clients disconnect unexpectedly, both states must eventually converge. Otherwise, routing may target dead sessions.

#### Solution: TTL lease model

Redis online state is treated as a renewable lease, not permanent truth.

- **TTL**: online routing key expires after 30 seconds.
- **Renewal**: each valid authenticated heartbeat renews the TTL.

#### Timing parameters (implemented values)

1. **Client heartbeat interval**: 10s
2. **Server inactivity timeout**: 30s
3. **Redis TTL**: 30s

Note: server timeout and Redis TTL are equal in the current implementation.

#### Step-by-step lifecycle management

1. **Connect**
  - Client establishes a gRPC stream.
  - After authentication, server registers local session and writes Redis route:
    - `connectionsMap.put(userId, session)`
    - `SET user:online:{userId} {instanceId}:{sessionId} EX 30`
2. **Heartbeat (healthy path)**
  - Client sends `HeartbeatPing`.
  - Server always replies with `HeartbeatPong`.
  - If stream is authenticated, server updates local heartbeat and renews Redis TTL:
    - `updateHeartbeat(userId)`
    - `EXPIRE user:online:{userId} 30`
3. **Client timeout cleanup**
  - Client stops heartbeats.
  - Cleanup task removes sessions whose last heartbeat exceeds 30s.
  - Server performs active cleanup:
    1. remove session from local map
    2. delete Redis online key
4. **Server crash cleanup (passive)**
  - If a server crashes before cleanup, Redis key is no longer renewed.
  - Redis automatically expires and removes stale online entries.

---

## 3.3 Client-Side Resilience: Detection and Reconnection

> **Design rationale:**
> Server-side cleanup prevents stale routing state, but good user experience also requires client-side failure detection and reconnect behavior.

### 3.3.1 Failure detection: the 3-strikes rule

Client should not disconnect immediately on one transient failure.

- **Ping interval**: 10s
- **Pong timeout**: 5s
- **Failure threshold**: 3 consecutive misses

State logic:
1. **Normal**: send ping, receive pong, reset `missedPongs` to 0.
2. **Unstable**: ping timeout increments `missedPongs`.
3. **Dead**: when `missedPongs >= 3`, trigger teardown and reconnect.

### 3.3.2 Teardown phase

Before reconnecting, client cleans old transport resources:

1. stop heartbeat tasks
2. complete/close current stream observer
3. shutdown current gRPC channel
4. notify UI as disconnected/reconnecting

### 3.3.3 Reconnect strategy (exponential backoff + jitter)

Current implementation:
- **Base delay**: 1s
- **Max delay cap**: 5s
- **Jitter**: random 0~500ms

This prevents tight reconnect loops and reduces synchronized reconnect spikes.

### 3.3.4 Post-reconnect synchronization

A reconnected transport is not enough; message state may be stale.

1. reconnect becomes healthy
2. if authenticated state is retained, client reattaches `x-user-id`
3. client sends `CatchupRequest` with per-conversation cursor hints from SQLite:
   - `cursorHints[]` (`conversationId`, `clientLastReceivedSequenceId`)
   - `perConversationLimit`
4. server returns missing windows via `CatchupResult`
5. client writes results to local DB and refreshes UI

---

