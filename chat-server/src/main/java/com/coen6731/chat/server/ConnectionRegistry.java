package com.coen6731.chat.server;

import com.coen6731.chat.ServerEvent;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages active user connections.
 * It stores the mapping between user IDs and their response observers (streams).
 */
public class ConnectionRegistry {
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
   * Registers a user with their response stream.
   * If the user is already connected, the old connection is closed with an error.
   */
  public void register(String userId, StreamObserver<ServerEvent> stream) {
    UserSession newSession = new UserSession(stream);
    UserSession oldSession = connectionsMap.put(userId, newSession);

    // If there was an existing connection and it's different from the new one, close the old one.
    if (oldSession != null && !Objects.equals(oldSession.getResponseObserver(), stream)) {
      oldSession.sendErrorAndClose("DUPLICATE_LOGIN", "A new session has replaced this connection");
    }
  }

  /**
   * Removes a user's connection.
   * Only removes if the current stream matches the one stored (to avoid removing a new session).
   */
  public void unregister(String userId, StreamObserver<ServerEvent> stream) {
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
        System.out.println("[server] cleanup - removing inactive user:" + userId + ", last seen: " + (now - userSession.getLastHeartbeat()) + "ms ago");

        // Atomically remove if the userSession hasn't changed
        if (connectionsMap.remove(userId, userSession)) {
          userSession.sendErrorAndClose("TIMEOUT", "User " + userId + " session timed out");
        }
      }
    });

    System.out.println("[server] cleanup done - active sessions after cleanup: \n" + connectionsMap.keySet());
  }
}
