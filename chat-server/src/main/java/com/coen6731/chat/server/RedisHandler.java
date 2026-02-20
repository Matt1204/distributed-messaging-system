package com.coen6731.chat.server;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

/**
 * Responsibility: maintain Redis-based online registry and cross-node relay stream.
 * Input: online lifecycle events and relay payloads.
 * Output: Redis key updates and stream consume/publish side-effects.
 */
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

  /**
   * Responsibility: construct redis relay component for one server instance.
   * Input: redis template, registry callback, and current instance id.
   * Output: initialized redis handler.
   */
  public RedisHandler(
      StringRedisTemplate redisTemplate,
      @Lazy ConnectionRegistry connectionRegistry,
      @Value("${container.app.replica.name}") String instanceId) {
    this.redisTemplate = redisTemplate;
    this.connectionRegistry = connectionRegistry;
    this.instanceId = instanceId;
    this.consumerExecutor = Executors.newSingleThreadExecutor();
  }

  /**
   * Responsibility: initialize consumer group and start relay consumer loop.
   * Input: bean startup callback.
   * Output: active Redis stream consumer.
   */
  @PostConstruct
  public void init() {
    logger.info("[RedisHandler] Initializing for instance: {}", instanceId);
    ensureConsumerGroup();
    startConsumerLoop();
  }

  /**
   * Responsibility: stop consumer loop and thread during shutdown.
   * Input: bean shutdown callback.
   * Output: terminated consumer executor.
   */
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

  /**
   * Responsibility: mark user online with TTL so stale entries self-expire.
   * Input: user id and local session id.
   * Output: Redis SET key with 30s TTL.
   */
  public void registerUserOnline(String userId, String sessionId) {
    String key = ONLINE_USER_KEY_PREFIX + userId;
    String value = instanceId + ":" + sessionId;
    redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(30));
    logger.debug("[RedisHandler] Registered user {} online at {}", userId, value);
  }

  /**
   * Responsibility: refresh TTL for existing online user marker.
   * Input: user id.
   * Output: Redis EXPIRE call.
   */
  public void renewUserOnline(String userId) {
    String onlineUserKey = ONLINE_USER_KEY_PREFIX + userId;
    redisTemplate.expire(onlineUserKey, Duration.ofSeconds(30));
    logger.debug("[RedisHandler] Renewed redis TTL for user {}", userId);
  }

  /**
   * Responsibility: remove user online marker explicitly.
   * Input: user id.
   * Output: Redis DEL key.
   */
  public void removeUserOnline(String userId) {
    String key = ONLINE_USER_KEY_PREFIX + userId;
    redisTemplate.delete(key);
    logger.debug("[RedisHandler] Removed online status for user {}", userId);
  }

  /**
   * Responsibility: read global route token for user.
   * Input: user id.
   * Output: instance/session tuple string or null.
   */
  public String getUserOnlineInfo(String userId) {
    return redisTemplate.opsForValue().get(ONLINE_USER_KEY_PREFIX + userId);
  }

  /**
   * Responsibility: publish one relay payload to target instance stream.
   * Input: target instance id and relay fields.
   * Output: Redis XADD record.
   */
  public void publishRelayMessage(String targetInstanceId, Map<String, String> streamMsgRecord) {
    String streamKey = STREAM_KEY_PREFIX + targetInstanceId;
    redisTemplate.opsForStream().add(StreamRecords.newRecord().in(streamKey).ofMap(streamMsgRecord));
    logger.debug("[RedisHandler] Published relay message to node: {}", streamKey);
  }

  /**
   * Responsibility: create per-instance consumer group if missing.
   * Input: current instance id.
   * Output: existing or newly created Redis stream group.
   */
  private void ensureConsumerGroup() {
    String streamKey = STREAM_KEY_PREFIX + instanceId;
    String group = GROUP_PREFIX + instanceId;
    try {
      redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.latest(), group);
      logger.info("[RedisHandler] Created consumer group {} for stream {}", group, streamKey);
    } catch (Exception e) {
      logger.info(
          "[RedisHandler] Consumer group {} likely exists or stream issue: {}",
          group,
          e.getMessage());
    }
  }

  /**
   * Responsibility: continuously consume this instance stream and dispatch relay records.
   * Input: none (runs in background thread).
   * Output: delegated local delivery plus XACK.
   */
  private void startConsumerLoop() {
    consumerExecutor.submit(
        () -> {
          String streamKey = STREAM_KEY_PREFIX + instanceId;
          String consumerGroupKey = GROUP_PREFIX + instanceId;
          String consumerKey = CONSUMER_PREFIX + instanceId + ":main";

          logger.info("[RedisHandler] Starting consumer loop for stream {}", streamKey);

          while (running) {
            try {
              @SuppressWarnings("unchecked")
              List<MapRecord<String, Object, Object>> streamRecordList =
                  redisTemplate.opsForStream().read(
                      Consumer.from(consumerGroupKey, consumerKey),
                      StreamReadOptions.empty().count(10).block(Duration.ofSeconds(2)),
                      StreamOffset.create(streamKey, ReadOffset.lastConsumed()));

              if (streamRecordList != null && !streamRecordList.isEmpty()) {
                for (MapRecord<String, Object, Object> streamRecord : streamRecordList) {
                  processStreamMsgRecord(streamRecord);
                  redisTemplate
                      .opsForStream()
                      .acknowledge(streamKey, consumerGroupKey, streamRecord.getId());
                }
              }
            } catch (Exception e) {
              if (running) {
                logger.error("[RedisHandler] Error in consumer loop", e);
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                }
              }
            }
          }
        });
  }

  /**
   * Responsibility: validate relay fields and hand off to local connection registry.
   * Input: one stream record from Redis.
   * Output: local delivery attempt when record is valid.
   */
  private void processStreamMsgRecord(MapRecord<String, Object, Object> record) {
    try {
      Map<Object, Object> streamMsgRecord = record.getValue();
      String toUserId = asString(streamMsgRecord.get("toUserId"));
      String targetSessionId = asString(streamMsgRecord.get("targetSessionId"));
      String fromUserId = asString(streamMsgRecord.get("fromUserId"));
      logger.info(
          "[{}] [RedisHandler] relay message from {} to {} targetSession={}",
          instanceId,
          fromUserId,
          toUserId,
          targetSessionId);

      if (!toUserId.isBlank() && !targetSessionId.isBlank()) {
        connectionRegistry.deliverRemoteMessage(toUserId, targetSessionId, streamMsgRecord);
      } else {
        logger.warn("[RedisHandler] Received malformed message: {}", streamMsgRecord);
      }
    } catch (Exception e) {
      logger.error("[RedisHandler] Failed to process record {}", record.getId(), e);
    }
  }

  /**
   * Responsibility: normalize nullable Redis field values to strings.
   * Input: object value from Redis record.
   * Output: non-null string.
   */
  private String asString(Object value) {
    return value == null ? "" : String.valueOf(value);
  }
}
