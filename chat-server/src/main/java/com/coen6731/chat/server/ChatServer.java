package com.coen6731.chat.server;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for the Chat Server.
 * We bootstrap Spring Boot here and let Spring manage the gRPC server lifecycle.
 */
@SpringBootApplication
public class ChatServer {
  private static final Logger logger = LoggerFactory.getLogger(ChatServer.class);

  public static void main(String[] args) {
    Dotenv dotenv = Dotenv.configure()
        .directory("chat-server")
        .ignoreIfMissing()
        .load();
    dotenv.entries().forEach(entry -> {
      if (System.getProperty(entry.getKey()) == null && System.getenv(entry.getKey()) == null) {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    });

    // We now prefer environment variables for port configuration.
    // Use CHAT_GRPC_PORT or PORT, falling back to 50051.
    String envPort = System.getProperty("CHAT_GRPC_PORT");
    if (envPort == null || envPort.isBlank()) {
      envPort = System.getenv("CHAT_GRPC_PORT");
    }
    if (envPort == null || envPort.isBlank()) {
      envPort = System.getProperty("PORT");
    }
    if (envPort == null || envPort.isBlank()) {
      envPort = System.getenv("PORT");
    }

    String port = (envPort != null && envPort.matches("\\d+")) ? envPort : "50051";

    // Inject the port into Spring Boot arguments
    String[] normalizedArgs = new String[args.length + 1];
    System.arraycopy(args, 0, normalizedArgs, 0, args.length);
    normalizedArgs[args.length] = "--chat.grpc.port=" + port;

    logger.info("[server] starting on port {}", port);

    // Spring Boot becomes our "main loop", and the gRPC server is started by a bean.
    SpringApplication.run(ChatServer.class, normalizedArgs);
  }
}
