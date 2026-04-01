## 1. Overview (Spring Boot + ACA Replica + Redis Streams)

### 1.1 High-level architecture (logical)

**Client**
* Establishes one gRPC channel to server (HTTP/2 long-lived connection)
* Opens bidirectional stream `Chat` for messaging and heartbeat

**Server (Spring Boot, deployed on Azure Container Apps)**
* **Gateway / gRPC Frontend**
  * Authentication, connection/session management, routing
  * Maintains local mapping: `connections[userId] -> StreamObserver<ServerEvent>`
  * Maintains Redis online registry with TTL: `user:online:{userId} -> {instanceId}:{sessionId}`
  * Uses Redis Streams for cross-replica relay
  * Reads/writes CosmosDB

* **Instance ID**
  * In Azure Container Apps, each replica has a unique name.
  * In code, server uses Spring property `container.app.replica.name`, populated from env `CONTAINER_APP_REPLICA_NAME`.
  * It is used as Redis stream identity (`stream:instance:{instanceId}`) and consumer-group naming.

---

## 2. Redis Streams quick primer

### 2.1 Core terms
* **Stream**: append-only message log
* **Entry**: one stream record (with id such as `1700000000000-0`)
* **Consumer Group**: shared consumption progress across consumers
* **Consumer**: specific member in a consumer group
* **ACK (`XACK`)**: confirms successful processing
* **Pending**: delivered but not acknowledged

### 2.2 Key conventions in this design
* Global Connection Registry: `user:online:{userId}` -> `{instanceId}:{sessionId}` (with TTL)
* Instance inbound stream: `stream:instance:{instanceId}`
* Consumer group name: `cg:{instanceId}` (one group per instance)
* Consumer name in current code: `consumer:{instanceId}:main`

### 2.3 Common commands (conceptual)
* `XADD stream:instance:inst-B * from inst-A to Bob payload ...`
  * Append one relay record into target instance stream
* `XGROUP CREATE stream:instance:inst-B cg:inst-B $ MKSTREAM`
  * Create consumer group for stream (`$` means start from new records)
* `XREADGROUP GROUP cg:inst-B consumer:inst-B-1 BLOCK 2000 COUNT 10 STREAMS stream:instance:inst-B >`
  * Read new group messages (`>` means undispatched new messages)
* `XACK stream:instance:inst-B cg:inst-B <entryId>`
  * Acknowledge after successful processing

---

## 3. Cross-instance message delivery flow (Streams)

Assume **Alice is on instance-A** and **Bob is on instance-B**.

1. **Routing lookup**
* instance-A receives `sendMessage(to=Bob, payload=...)`
* Check local `connectionsMap` first
* If not local, query Redis `GET user:online:Bob`
* If value is `instance-B:session-xyz`, continue relay

2. **Forwarding**
* instance-A executes `XADD stream:instance:instance-B ...`
* Payload includes current relay fields used by code:
  `toUserId`, `targetSessionId`, `serverMsgId`, `clientMsgId`, `conversationId`, `fromUserId`, `fromEmail`, `text`, `sentAtMs`, `sequenceId`

3. **Delivery**
* instance-B consumer loop reads from `XREADGROUP`
* Check local `connectionsMap[Bob]`
* If exists and `sessionId` matches, deliver to Bob stream
* Acknowledge with `XACK`

4. **Failure behavior (POC)**
* If target local session no longer matches or user is offline, server logs and still ACKs (to avoid indefinite stream buildup).
* Offline compensation is handled by catchup/history paths.

### 3.1 Core Redis interaction code (publish / consume / ACK)

The snippets below explain concepts with Spring Data Redis `StringRedisTemplate`.

```java
// Producer: instance-A forwards to instance-B stream
public RecordId forwardToInstance(String targetInstanceId, ChatEnvelope msg) {
    String streamKey = "stream:instance:" + targetInstanceId;

    Map<String, String> fields = new HashMap<>();
    fields.put("toUserId", msg.getToUserId());
    fields.put("fromUserId", msg.getFromUserId());
    fields.put("serverMsgId", msg.getServerMsgId());
    fields.put("clientMsgId", msg.getClientMsgId());
    fields.put("conversationId", msg.getConversationId());
    fields.put("fromEmail", msg.getFromEmail());
    fields.put("text", msg.getText());
    fields.put("sentAtMs", msg.getSentAtMs());
    fields.put("sequenceId", msg.getSequenceId());
    fields.put("targetSessionId", msg.getTargetSessionId());

    return stringRedisTemplate.opsForStream().add(
        StreamRecords.newRecord()
            .in(streamKey)
            .ofMap(fields)
    );
}
```

```java
// On instance-B startup, initialize consumer group (ignore already exists)
public void ensureGroup(String instanceId) {
    String streamKey = "stream:instance:" + instanceId;
    String group = "cg:" + instanceId;
    try {
        stringRedisTemplate.opsForStream()
            .createGroup(streamKey, ReadOffset.latest(), group);
    } catch (Exception e) {
        // BUSYGROUP (already exists) can be ignored in POC
    }
}
```

```java
// Consumer Loop: instance-B continuously reads and delivers
public void consumeLoop(String instanceId) {
    String streamKey = "stream:instance:" + instanceId;
    String group = "cg:" + instanceId;
    String consumer = "consumer:" + instanceId + ":main";

    // read message records
    List<MapRecord<String, Object, Object>> records =
        stringRedisTemplate.opsForStream().read(
            Consumer.from(group, consumer), // group + consumer
            // read up to 10 entries, block up to 2 seconds
            StreamReadOptions.empty().count(10).block(Duration.ofSeconds(2)),
            // read from last-consumed position
            StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        );

    if (records == null || records.isEmpty()) {
        continue;
    }

    for (MapRecord<String, Object, Object> r : records) {
        try {
            // 1) parse fields
            String toUserId = String.valueOf(r.getValue().get("toUserId"));
            String targetSessionId = String.valueOf(r.getValue().get("targetSessionId"));
            // 2) check local session and deliver gRPC
            deliverIfLocalSessionMatches(toUserId, targetSessionId, r.getValue());
            // 3) ACK after processing
            stringRedisTemplate.opsForStream().acknowledge(streamKey, group, r.getId());
        } catch (Exception ex) {
            // POC: log only, no retry/dead-letter in this snippet
        }
    }
}
```

### 3.2 Notes
* Maintain a background consumer thread (or scheduled/worker equivalent).

---

## 6. TTL + heartbeat renewal (client-server integration)

### 6.1 Design idea
* Client and server already exchange heartbeat on the long-lived gRPC stream.
* Server maps valid heartbeat activity to Redis TTL renewal.
* As long as heartbeat continues, `user:online:{userId}` remains valid.
* If heartbeat stops, TTL expires and other instances stop routing to stale sessions.

### 6.2 Parameter guidance (POC)
* Redis TTL: `30s`
* Client heartbeat interval: `10s`
* Server renewal action: `EXPIRE 30` on each valid authenticated heartbeat
* Heartbeat interval should remain significantly smaller than TTL

### 6.3 Step-by-step heartbeat flow (conceptual)

1. **Connection established**
* Client opens `Chat` stream and becomes authenticated
* Server creates `sessionId`
* Server writes online key: `SET user:online:{userId} {instanceId}:{sessionId} EX 30`

2. **Renewal during runtime**
* Client sends heartbeat every 10s
* After authenticated heartbeat, server:
  * validates current active session
  * renews `user:online:{userId}` to 30s

3. **Transient network jitter**
* If one or two heartbeats are lost but connection recovers quickly, later heartbeats renew TTL again.
* If TTL has not expired, online state remains valid.

4. **Disconnect or instance failure**
* If client disconnects or instance fails, heartbeat stops
* Redis key expires automatically at TTL
* Other instances no longer route to that stale target

---

## 7. Implementation checklist

1. Maintain Redis Streams producer/consumer components for relay.
2. Keep `instanceId` config and startup consumer-group initialization.
3. Keep connection lifecycle integration for `SET EX` and heartbeat-driven `EXPIRE`.
4. In `sendMessage` path:
* deliver directly when recipient is local
* otherwise query online key and `XADD` to target instance stream
5. Add baseline observability logs:
* `route_lookup_result`
* `stream_enqueue_success/fail`
* `stream_consume_success/fail`
* `heartbeat_renew_success/fail`
