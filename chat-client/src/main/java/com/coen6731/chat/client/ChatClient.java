package com.coen6731.chat.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * A command-line chat client.
 * Connects to the gRPC server and allows users to register and exchange messages.
 */
public class ChatClient {
  public static void main(String[] args) throws IOException {
    String target = args.length > 0 ? args[0] : "localhost:50051";
    ChatClientSession session = new ChatClientSession(target);
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("[client] connected to " + target);
    printHelp();

    // Main loop to read user input.
    while (true) {
      System.out.print("> ");
      String line = reader.readLine();
      if (line == null) {
        break; // End of input
      }
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }

      if (line.equals("/help")) {
        printHelp();
        continue;
      }
      if (line.equals("/exit")) {
        break;
      }

      // Handle registration command
      if (line.startsWith("/register ")) {
        String userId = line.substring("/register ".length()).trim();
        if (userId.isEmpty()) {
          System.out.println("Usage: /register <userId>");
          continue;
        }
        session.register(userId);
        continue;
      }

      // Handle send command
      if (line.startsWith("/send ")) {
        String payload = line.substring("/send ".length()).trim();
        int firstSpace = payload.indexOf(' ');
        if (firstSpace <= 0) {
          System.out.println("Usage: /send <toUserId> <text>");
          continue;
        }
        String toUserId = payload.substring(0, firstSpace).trim();
        String text = payload.substring(firstSpace + 1).trim();
        if (text.isEmpty()) {
          System.out.println("Usage: /send <toUserId> <text>");
          continue;
        }
        session.sendMessage(toUserId, text);
        continue;
      }

      System.out.println("Unknown command. Type /help for usage.");
    }

    // Cleanup
    session.close();
  }

  private static void printHelp() {
    System.out.println("Commands:");
    System.out.println("  /register <userId>");
    System.out.println("  /send <toUserId> <text>");
    System.out.println("  /help");
    System.out.println("  /exit");
  }
}
