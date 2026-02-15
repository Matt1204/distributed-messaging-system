package com.coen6731.chat.server;

import com.coen6731.chat.ServerEvent;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Manages active user connections.
 * It stores the mapping between user IDs and their response observers (streams).
 */
@Component
public class ConnectionRegistry {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionRegistry.class);
  private static final long HEARTBEAT_TIMEOUT_MS = 30000; // 30 seconds
  private static final long CLEANUP_INTERVAL_MS = 5000;   // Check every 5 seconds

  // ConcurrentHashMap is used for thread-safe access since multiple threads (clients)
  // can register/unregister concurrently.
  private final ConcurrentHashMap<String, UserSession> connectionsMap =
      new ConcurrentHashMap<>();

  private final ScheduledExecutorService scheduler;

  public ConnectionRegistry() {
    // Create a daemon thread for cleanup so it doesn't prevent JVM shutdown
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "connection-cleanup");
      t.setDaemon(true);
      return t;
    });

    // Schedule the cleanup task
    this.scheduler.scheduleAtFixedRate(
        this::cleanupInactiveSessions,
        CLEANUP_INTERVAL_MS,
        CLEANUP_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Registers an active stream session for a user.
   * If the user is already connected, the old connection is closed with an error.
   * @return The newly created UserSession
   */
  public UserSession handleUserOnline(String userId, StreamObserver<ServerEvent> stream) {
    UserSession newSession = new UserSession(stream);
    UserSession oldSession = connectionsMap.put(userId, newSession);

    // If there was an existing connection and it's different from the new one, close the old one.
    if (oldSession != null && !Objects.equals(oldSession.getResponseObserver(), stream)) {
      oldSession.sendErrorAndClose("DUPLICATE_LOGIN", "A new session has replaced this connection");
    }
    return newSession;
  }

  /**
   * Removes a user's connection.
   * Only removes if the current stream matches the one stored (to avoid removing a new session).
   */
  public void handleUserOffline(String userId, StreamObserver<ServerEvent> stream) {
    connectionsMap.computeIfPresent(
        userId,
        (key, current) -> {
          // Only remove if the stored stream is the one requesting unregistration.
          if (Objects.equals(current.getResponseObserver(), stream)) {
            return null; // returning null removes the mapping
          }
          return current; // keep the current mapping
        });
  }

  public UserSession getSession(String userId) {
    return connectionsMap.get(userId);
  }

  public void updateHeartbeat(String userId) {
    UserSession userSession = connectionsMap.get(userId);
    if (userSession != null) {
      userSession.updateHeartbeat();
    }
  }

  // Periodic task to remove inactive sessions.
  private void cleanupInactiveSessions() {
    long now = System.currentTimeMillis();
    connectionsMap.forEach((userId, userSession) -> {
      if (now - userSession.getLastHeartbeat() > HEARTBEAT_TIMEOUT_MS) {
        logger.info(
            "[server] cleanup - removing inactive user: {}, last seen: {}ms ago",
            userId,
            now - userSession.getLastHeartbeat());

        // Atomically remove if the userSession hasn't changed
        cleanupTimeoutUserSession(userId, userSession);
      }
    });

    logger.debug(
        "[server] cleanup done - active sessions after cleanup: {}",
        connectionsMap.keySet());
  }

  private void cleanupTimeoutUserSession(String userId, UserSession userSession) {
    if (connectionsMap.remove(userId, userSession)) {
      userSession.sendErrorAndClose("TIMEOUT", "User " + userId + " session timed out");
    }

  }

  @PreDestroy
  public void shutdown() {
    scheduler.shutdownNow();
  }
}
