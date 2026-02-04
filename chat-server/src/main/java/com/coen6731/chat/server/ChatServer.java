package com.coen6731.chat.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

/**
 * The main entry point for the Chat Server.
 * This class is responsible for starting the gRPC server and registering services.
 */
public class ChatServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    // Default port is 50051, but can be overridden by command line arguments.
    int port = 50051;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    }

    // registry holds the active client connections.
    ConnectionRegistry registry = new ConnectionRegistry();

    // Build the gRPC server.
    Server server =
        ServerBuilder.forPort(port)
            // Register our MessagingServiceImpl to handle RPC calls.
            // We pass the registry so the service can manage connections.
            .addService(new MessagingServiceImpl(registry))
            .build()
            .start();

    System.out.println("[server] started on port " + port);

    // Add a shutdown hook to cleanly stop the server when the JVM shuts down (e.g., via Ctrl+C).
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("[server] shutting down");
                  server.shutdown();
                }));

    // Keep the main thread alive while the server is running.
    server.awaitTermination();
  }
}
