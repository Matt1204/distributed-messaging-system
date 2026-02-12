package com.coen6731.chat.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Spring-managed lifecycle wrapper for our gRPC server.
 * This keeps the exact same server behavior while letting Spring Boot own startup/shutdown.
 */
@Component
public class GrpcServerLifecycle implements SmartLifecycle {
  private static final Logger logger = LoggerFactory.getLogger(GrpcServerLifecycle.class);
  private final MessagingServiceImpl messagingService;
  private final int port;
  private volatile boolean running;
  private Server server;
  private Thread awaitThread;

  public GrpcServerLifecycle(
      MessagingServiceImpl messagingService,
      @Value("${chat.grpc.port:50051}") int port) {
    this.messagingService = messagingService;
    this.port = port;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    try {
      // Build the gRPC server with the same service wiring as before.
      this.server =
          ServerBuilder.forPort(port)
              .addService(messagingService)
              .intercept(new UserIdInterceptor())
              .build()
              .start();
      logger.info("[server] started on port {}", port);

      // Spring Boot will exit if only daemon threads are running.
      // gRPC's internal threads are daemon by default, so we keep a non-daemon
      // thread blocking on awaitTermination() to match the old "main blocks forever" behavior.
      this.awaitThread =
          new Thread(
              () -> {
                try {
                  server.awaitTermination();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              },
              "grpc-await");
      this.awaitThread.setDaemon(false);
      this.awaitThread.start();

      running = true;
    } catch (IOException e) {
      // If the server can't start, fail fast so Spring doesn't keep running silently.
      throw new IllegalStateException("Failed to start gRPC server on port " + port, e);
    }
  }

  @Override
  public void stop() {
    if (!running || server == null) {
      return;
    }
    logger.info("[server] shutting down gRPC server...");
    // 1. Stop accepting new calls
    server.shutdown();
    try {
      // 2. Wait for existing calls to complete, matching Azure's default terminationGracePeriodSeconds (30s)
      if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
          logger.warn("[server] gRPC server did not terminate in time, forcing shutdown.");
          server.shutdownNow();
      }
    } catch (InterruptedException e) {
      logger.warn("[server] interrupted during shutdown, forcing shutdown.");
      server.shutdownNow();
      Thread.currentThread().interrupt();
    } finally {
      logger.info("[server] gRPC server stopped.");
      if (awaitThread != null) {
        awaitThread.interrupt();
      }
      running = false;
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE;
  }
}
