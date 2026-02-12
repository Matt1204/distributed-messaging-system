package com.coen6731.chat.server;

import com.coen6731.chat.ServerEvent;
import com.coen6731.chat.ServerError;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an active user session on the server.
 * Encapsulates the gRPC stream and heartbeat state.
 */
public class UserSession {
  private static final Logger logger = LoggerFactory.getLogger(UserSession.class);
  private final StreamObserver<ServerEvent> responseObserver;
  private volatile long lastHeartbeat;

  public UserSession(StreamObserver<ServerEvent> responseObserver) {
    this.responseObserver = responseObserver;
    this.lastHeartbeat = System.currentTimeMillis();
  }

  public StreamObserver<ServerEvent> getResponseObserver() {
    return responseObserver;
  }

  public void updateHeartbeat() {
    this.lastHeartbeat = System.currentTimeMillis();
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  /**
   * Sends an event to this session's client.
   * Note: StreamObserver is not thread-safe. Synchronization is handled here.
   */
  public synchronized void send(ServerEvent event) {
    try {
      responseObserver.onNext(event);
    } catch (Exception e) {
      logger.warn("[server] error sending to session", e);
    }
  }

  /**
   * Sends an error and closes the connection.
   */
  public void sendErrorAndClose(String code, String reason) {
    // Send error
    try {
      send(ServerEvent.newBuilder()
          .setServerError(
              ServerError.newBuilder()
                  .setCode(code)
                  .setReason(reason)
                  .build())
          .build());
    } catch (Exception ignored) {
      // Ignore
    } finally {
      close();
    }
  }

  public synchronized void close() {
    try {
      responseObserver.onCompleted();
    } catch (Exception ignored) {
      // Ignore
    }
  }
}
