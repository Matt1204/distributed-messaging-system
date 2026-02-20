package com.coen6731.chat.server;

import com.coen6731.chat.InboundMessage;
import com.coen6731.chat.ServerEvent;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Responsibility: keep local active user sessions and route local/remote deliveries.
 * Input: user online/offline/session updates and relay payloads.
 * Output: local session sends and Redis relay publication.
 */
@Component
public class ConnectionRegistry {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionRegistry.class);
  private static final long HEARTBEAT_TIMEOUT_MS = 30000;
  private static final long CLEANUP_INTERVAL_MS = 5000;

  private final ConcurrentHashMap<String, UserSession> connectionsMap = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler;
  private final RedisHandler redisHandler;

  /**
   * Responsibility: initialize session registry and periodic timeout cleanup job.
   * Input: redis handler used for cross-node online routing.
   * Output: live registry component.
   */
  public ConnectionRegistry(RedisHandler redisHandler) {
    this.redisHandler = redisHandler;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "connection-cleanup");
              t.setDaemon(true);
              return t;
            });

    this.scheduler.scheduleAtFixedRate(
        this::cleanupInactiveSessions, CLEANUP_INTERVAL_MS, CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  /**
   * Responsibility: register/replace a user's active stream session.
   * Input: userId and stream observer.
   * Output: current active UserSession for this user.
   */
  public UserSession handleUserOnline(String userId, StreamObserver<ServerEvent> stream) {
    UserSession newSession = new UserSession(stream);
    UserSession oldSession = connectionsMap.put(userId, newSession);

    redisHandler.registerUserOnline(userId, newSession.getSessionId());

    if (oldSession != null && !Objects.equals(oldSession.getResponseObserver(), stream)) {
      oldSession.sendErrorAndClose("DUPLICATE_LOGIN", "A new session has replaced this connection");
    }
    return newSession;
  }

  /**
   * Responsibility: remove a user's session only when the stream matches the current mapping.
   * Input: userId and stream observer being closed.
   * Output: mapping removed when stream is current.
   */
  public void handleUserOffline(String userId, StreamObserver<ServerEvent> stream) {
    connectionsMap.computeIfPresent(
        userId,
        (key, currentUserSession) -> {
          if (Objects.equals(currentUserSession.getResponseObserver(), stream)) {
            redisHandler.removeUserOnline(userId);
            return null;
          }
          return currentUserSession;
        });
  }

  /**
   * Responsibility: get active local session for a user.
   * Input: userId.
   * Output: local UserSession or null when user is not local-online.
   */
  public UserSession getSession(String userId) {
    return connectionsMap.get(userId);
  }

  /**
   * Responsibility: refresh heartbeat for active local session and Redis TTL.
   * Input: userId.
   * Output: updated last-heartbeat timestamp and Redis expiry.
   */
  public void updateHeartbeat(String userId) {
    UserSession userSession = connectionsMap.get(userId);
    if (userSession != null) {
      userSession.updateHeartbeat();
      redisHandler.renewUserOnline(userId);
    }
  }

  /**
   * Responsibility: remove expired sessions that stopped sending heartbeats.
   * Input: none (scheduled task).
   * Output: stale sessions removed and closed.
   */
  private void cleanupInactiveSessions() {
    long now = System.currentTimeMillis();
    connectionsMap.forEach(
        (userId, userSession) -> {
          if (now - userSession.getLastHeartbeat() > HEARTBEAT_TIMEOUT_MS) {
            logger.info(
                "[server] cleanup - removing inactive user: {}, last seen: {}ms ago",
                userId,
                now - userSession.getLastHeartbeat());
            cleanupTimeoutUserSession(userId, userSession);
          }
        });

    logger.debug("[server] cleanup done - active sessions after cleanup: {}", connectionsMap.keySet());
  }

  /**
   * Responsibility: atomically remove a specific stale user session and close stream.
   * Input: userId and exact UserSession reference to remove.
   * Output: timeout error pushed to removed session.
   */
  private void cleanupTimeoutUserSession(String userId, UserSession userSession) {
    if (connectionsMap.remove(userId, userSession)) {
      redisHandler.removeUserOnline(userId);
      userSession.sendErrorAndClose("TIMEOUT", "User " + userId + " session timed out");
    }
  }

  /**
   * Responsibility: convert relay stream fields back into InboundMessage and send locally.
   * Input: target user/session and relay message field map.
   * Output: one inbound server event if target session matches.
   */
  public void deliverRemoteMessage(
      String toUserId, String targetSessionId, Map<Object, Object> streamMessageRecord) {
    UserSession session = connectionsMap.get(toUserId);
    if (session != null && session.getSessionId().equals(targetSessionId)) {
      try {
        InboundMessage inboundMessage =
            InboundMessage.newBuilder()
                .setServerMsgId(asString(streamMessageRecord.get("serverMsgId")))
                .setClientMsgId(asString(streamMessageRecord.get("clientMsgId")))
                .setConversationId(asString(streamMessageRecord.get("conversationId")))
                .setFromUserId(asString(streamMessageRecord.get("fromUserId")))
                .setFromEmail(asString(streamMessageRecord.get("fromEmail")))
                .setToUserId(toUserId)
                .setText(asString(streamMessageRecord.get("text")))
                .setSentAtMs(asLong(streamMessageRecord.get("sentAtMs")))
                .build();

        session.send(ServerEvent.newBuilder().setInboundMessage(inboundMessage).build());
        logger.debug("[ConnectionRegistry] Delivered remote message to user {}", toUserId);
      } catch (Exception e) {
        logger.error("[ConnectionRegistry] Failed to deliver remote message to user {}", toUserId, e);
      }
    } else {
      logger.debug(
          "[ConnectionRegistry] Remote message target session mismatch or user offline: {}/{}",
          toUserId,
          targetSessionId);
    }
  }

  /**
   * Responsibility: look up global online route for a user via Redis.
   * Input: userId.
   * Output: route token "instanceId:sessionId" or null.
   */
  public String getRoutingInfo(String userId) {
    return redisHandler.getUserOnlineInfo(userId);
  }

  /**
   * Responsibility: publish canonical message metadata to remote node stream.
   * Input: remote instance/session target and canonical inbound payload.
   * Output: relay record appended to target Redis stream.
   */
  public void ReplayMessageToNode(
      String targetInstanceId, String toUserId, String targetSessionId, InboundMessage message) {
    Map<String, String> streamMessageRecord = new java.util.HashMap<>();
    streamMessageRecord.put("toUserId", toUserId);
    streamMessageRecord.put("targetSessionId", targetSessionId);
    streamMessageRecord.put("serverMsgId", message.getServerMsgId());
    streamMessageRecord.put("clientMsgId", message.getClientMsgId());
    streamMessageRecord.put("conversationId", message.getConversationId());
    streamMessageRecord.put("fromUserId", message.getFromUserId());
    streamMessageRecord.put("fromEmail", message.getFromEmail());
    streamMessageRecord.put("text", message.getText());
    streamMessageRecord.put("sentAtMs", String.valueOf(message.getSentAtMs()));

    redisHandler.publishRelayMessage(targetInstanceId, streamMessageRecord);
  }

  /**
   * Responsibility: normalize relay map field into non-null string.
   * Input: arbitrary Redis value object.
   * Output: string representation or empty string.
   */
  private String asString(Object value) {
    return value == null ? "" : String.valueOf(value);
  }

  /**
   * Responsibility: parse relay field into long timestamp value.
   * Input: numeric/string timestamp object from Redis map.
   * Output: parsed long value or 0 when invalid.
   */
  private long asLong(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String text) {
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException ignored) {
        return 0L;
      }
    }
    return 0L;
  }

  /**
   * Responsibility: stop cleanup scheduler during bean shutdown.
   * Input: none.
   * Output: scheduler terminated.
   */
  @PreDestroy
  public void shutdown() {
    scheduler.shutdownNow();
  }
}
