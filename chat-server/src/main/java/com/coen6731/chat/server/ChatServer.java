package com.coen6731.chat.server;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The main entry point for the Chat Server.
 * We bootstrap Spring Boot here and let Spring manage the gRPC server lifecycle.
 */
@SpringBootApplication
public class ChatServer {

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

    // Spring Boot becomes our "main loop", and the gRPC server is started by a bean.
    SpringApplication.run(ChatServer.class, args);
  }
}
