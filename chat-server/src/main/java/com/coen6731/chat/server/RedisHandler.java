package com.coen6731.chat.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class RedisHandler {
    private static final Logger logger = LoggerFactory.getLogger(RedisHandler.class);
    private static final String ONLINE_USER_KEY_PREFIX = "user:online:";
    private static final String STREAM_KEY_PREFIX = "stream:instance:";
    private static final String GROUP_PREFIX = "cg:";
    private static final String CONSUMER_PREFIX = "consumer:";

    private final StringRedisTemplate redisTemplate;
    private final ConnectionRegistry connectionRegistry;
    private final String instanceId;
    private final ExecutorService consumerExecutor;
    private volatile boolean running = true;

    public RedisHandler(StringRedisTemplate redisTemplate,
                        @Lazy ConnectionRegistry connectionRegistry,
                        @Value("${container.app.replica.name}") String instanceId) {
        this.redisTemplate = redisTemplate;
        this.connectionRegistry = connectionRegistry;
        this.instanceId = instanceId;
        this.consumerExecutor = Executors.newSingleThreadExecutor();
    }

    @PostConstruct
    public void init() {
        logger.info("[RedisHandler] Initializing for instance: {}", instanceId);
        ensureConsumerGroup();
        startConsumerLoop();
    }

    @PreDestroy
    public void shutdown() {
        logger.info("[RedisHandler] Shutting down...");
        running = false;
        consumerExecutor.shutdownNow();
        try {
            if (!consumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("[RedisHandler] Consumer executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Online Status Management ---

    /**
     * Registers a user as online on this instance.
     * Key: user:online:{userId} -> {instanceId}:{sessionId}
     * TTL: 30 seconds
     * Query: SET user:online:{userId} {instanceId}:{sessionId} EX 30
     */
    public void registerUserOnline(String userId, String sessionId) {
        String key = ONLINE_USER_KEY_PREFIX + userId;
        String value = instanceId + ":" + sessionId;
        // Use SET with EXPIRE (TTL) to ensure the online status automatically expires if the instance crashes
        redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(30));
        logger.debug("[RedisHandler] Registered user {} online at {}", userId, value);
    }

    /**
     * Renews the online status TTL.
     * Query: EXPIRE user:online:{userId} 30
     */
    public void renewUserOnline(String userId) {
        String onlineUserKey = ONLINE_USER_KEY_PREFIX + userId;
        // Refresh the TTL to keep the user online as long as heartbeats are received
        redisTemplate.expire(onlineUserKey, Duration.ofSeconds(30));
        logger.debug("[RedisHandler] Renewed redis TTL for user {}", userId);
    }

    /**
     * Removes the online status.
     * Query: DEL user:online:{userId}
     */
    public void removeUserOnline(String userId) {
        String key = ONLINE_USER_KEY_PREFIX + userId;
        redisTemplate.delete(key);
        logger.debug("[RedisHandler] Removed online status for user {}", userId);
    }

    /**
     * Gets the instance and session ID for a user.
     * Returns "instanceId:sessionId" or null if offline.
     * Query: GET user:online:{userId}
     */
    public String getUserOnlineInfo(String userId) {
        return redisTemplate.opsForValue().get(ONLINE_USER_KEY_PREFIX + userId); // user:online:{userId}
    }

    // --- Stream Management ---

    /**
     * Publishes a message to a target instance's stream.
     * Query: XADD stream:instance:{targetInstanceId} * field1 value1 field2 value2 ...
     */
    public void publishRelayMessage(String targetInstanceId, Map<String, String> streamMsgRecord) {
        String streamKey = STREAM_KEY_PREFIX + targetInstanceId; // stream:instance:{targetInstanceId}
        // XADD appends a new entry to the stream. '*' means Redis generates the entry ID.
        redisTemplate.opsForStream().add(
            StreamRecords.newRecord()
                .in(streamKey)
                .ofMap(streamMsgRecord)
        );
        logger.debug("[RedisHandler] Published relay message to node: {}", streamKey);
    }

    private void ensureConsumerGroup() {
        String streamKey = STREAM_KEY_PREFIX + instanceId; // stream:instance:{instanceId}
        String group = GROUP_PREFIX + instanceId; // cg:{instanceId}
        try {
            // Query: XGROUP CREATE stream:instance:{instanceId} cg:{instanceId} $ MKSTREAM
            // Create a consumer group for this instance's specific stream.
            // ReadOffset.latest() ($) means only new messages arriving after group creation are consumed.
            redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.latest(), group);
            logger.info("[RedisHandler] Created consumer group {} for stream {}", group, streamKey);
        } catch (Exception e) {
            // Ignore if group already exists (BUSYGROUP)
            logger.info("[RedisHandler] Consumer group {} likely exists or stream issue: {}", group, e.getMessage());
        }
    }

    private void startConsumerLoop() {
        // Dedicated thread for blocking stream consumption
        consumerExecutor.submit(() -> {
            String streamKey = STREAM_KEY_PREFIX + instanceId;
            String consumerGroupKey = GROUP_PREFIX + instanceId;
            String consumerKey = CONSUMER_PREFIX + instanceId + ":main"; // consumer:{instanceId}:main

            logger.info("[RedisHandler] Starting consumer loop for stream {}", streamKey);

            while (running) {
                try {
                    // Query: XREADGROUP GROUP cg:{id} consumer:{id}:main BLOCK 2000 COUNT 10 STREAMS stream:instance:{id} >
                    // ReadOffset.lastConsumed() ('>') reads messages that haven't been delivered to any other consumer in the group.
                    // block(Duration.ofSeconds(2)) prevents busy-waiting by blocking for up to 2 seconds if no messages are available.
                    @SuppressWarnings("unchecked")
                    List<MapRecord<String, Object, Object>> streamRecordList = redisTemplate.opsForStream().read(
                        Consumer.from(consumerGroupKey, consumerKey),
                        StreamReadOptions.empty().count(10).block(Duration.ofSeconds(2)),
                        StreamOffset.create(streamKey, ReadOffset.lastConsumed())
                    );

                    if (streamRecordList != null && !streamRecordList.isEmpty()) {
                        for (MapRecord<String, Object, Object> streamRecord : streamRecordList) {
                            processStreamMsgRecord(streamRecord);
                            // Query: XACK stream:instance:{id} cg:{id} {recordId}
                            // Acknowledge the message so it's removed from the Pending Entires List (PEL).
                            // In this POC, we ACK immediately after processing to ensure at-most-once delivery.
                            redisTemplate.opsForStream().acknowledge(streamKey, consumerGroupKey, streamRecord.getId());
                        }
                    }
                } catch (Exception e) {
                    if (running) {
                        logger.error("[RedisHandler] Error in consumer loop", e);
                        try {
                            Thread.sleep(1000); // Backoff on error to avoid log flooding
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        });
    }

    private void processStreamMsgRecord(MapRecord<String, Object, Object> record) {
        try {
            Map<Object, Object> streamMsgRecord = record.getValue();
            String toUserId = (String) streamMsgRecord.get("toUserId");
            String fromEmail = (String) streamMsgRecord.get("fromEmail");
            String targetSessionId = (String) streamMsgRecord.get("targetSessionId");
            logger.info("[{}] [RedisHandler] Received message from user {} to user {} in target session {}", instanceId, fromEmail, toUserId, targetSessionId);
            // Other fields available: fromUserId, messageId, chatPayload (JSON)
            
            if (toUserId != null && targetSessionId != null) {
                connectionRegistry.deliverRemoteMessage(toUserId, targetSessionId, streamMsgRecord);
            } else {
                logger.warn("[RedisHandler] Received malformed message: {}", streamMsgRecord);
            }
        } catch (Exception e) {
            logger.error("[RedisHandler] Failed to process record {}", record.getId(), e);
        }
    }
}
