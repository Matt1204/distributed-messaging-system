package com.coen6731.chat.client;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.UUID;

public class ChatClient {
  public static void main(String[] args) throws IOException {
    Dotenv dotenv = Dotenv.configure()
        .directory("chat-client")
        .ignoreIfMissing()
        .load();
    String target = dotenv.get("TARGET");
    if (target == null) {
      target = System.getenv("TARGET");
    }
     
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    String dbPath = askDbPath(reader);
    if (DatabaseManager.databaseExists(dbPath)) {
      System.out.println("[client] database found: " + dbPath);
    } else {
      runNewUserRegistration(reader, dbPath);
    }

    ChatClientSession session = new ChatClientSession(target, dbPath);
    DatabaseManager dbManager = new DatabaseManager(dbPath);
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
      if (line.equals("/register")) {
        String userName = dbManager.getUserName();
        String userId = dbManager.getUserId();
        session.sendRegisterUser(userId, userName);
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

  private static String askDbPath(BufferedReader reader) throws IOException {
    while (true) {
      System.out.print("Enter sqlite database name (.db): ");
      String input = reader.readLine();
      if (input == null) {
        throw new IOException("Input stream closed before database name was provided.");
      }
      String dbPath = resolveDbPath(input.trim());
      if (!dbPath.isBlank()) {
        return dbPath;
      }
      System.out.println("[client] database name cannot be empty.");
    }
  }

  private static String resolveDbPath(String input) {
    if (input.isBlank()) {
      return "";
    }

    String normalized = input.endsWith(".db") ? input : input + ".db";
    Path path = Path.of(normalized);
    if (!path.isAbsolute() && path.getParent() == null) {
      return Path.of("chat-client", "db", normalized).toString();
    }
    return path.toString();
  }

  private static void runNewUserRegistration(BufferedReader reader, String dbPath) throws IOException {
    System.out.println("[client] database not found, starting new user registration.");
    DatabaseManager.initializeDatabase(dbPath, "chat-client/db/init.sql");
    DatabaseManager dbManager = new DatabaseManager(dbPath);

    String userId = generateUserSsid();
    String userName = askRequiredInput(reader, "Enter userName: ");
    dbManager.updateUserState(userId, userName, null);
    System.out.println("[client] generated user ssid: " + userId);
    System.out.println("[client] registration completed and saved to " + dbPath);
  }

  private static String generateUserSsid() {
    return "ssid-" + UUID.randomUUID();
  }

  private static String askRequiredInput(BufferedReader reader, String prompt) throws IOException {
    while (true) {
      System.out.print(prompt);
      String value = reader.readLine();
      if (value == null) {
        throw new IOException("Input stream closed unexpectedly.");
      }
      String trimmed = value.trim();
      if (!trimmed.isEmpty()) {
        return trimmed;
      }
      System.out.println("[client] value cannot be empty.");
    }
  }
}
