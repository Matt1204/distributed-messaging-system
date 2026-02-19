package com.coen6731.chat.client;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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
    ChatClientSession session = new ChatClientSession(target);
    System.out.println("[client] connected to " + target);

    runAuthGate(reader, session);
    printHelp();

    while (true) {
      System.out.print("> ");
      String line = reader.readLine();
      if (line == null) {
        break;
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
      if (line.equals("/login")) {
        runLogin(reader, session);
        continue;
      }
      if (line.equals("/register")) {
        runRegister(reader, session);
        continue;
      }

      if (line.startsWith("/send ")) {
        String payload = line.substring("/send ".length()).trim();
        int firstSpace = payload.indexOf(' ');
        if (firstSpace <= 0) {
          System.out.println("Usage: /send <toEmail> <text>");
          continue;
        }
        String toEmail = payload.substring(0, firstSpace).trim();
        String text = payload.substring(firstSpace + 1).trim();
        if (text.isEmpty()) {
          System.out.println("Usage: /send <toEmail> <text>");
          continue;
        }
        session.sendMessage(toEmail, text);
        continue;
      }

      System.out.println("Unknown command. Type /help for usage.");
    }

    session.close();
  }

  private static void runAuthGate(BufferedReader reader, ChatClientSession session) throws IOException {
    while (!session.isAuthenticated()) {
      System.out.println("Authenticate first:");
      System.out.println("  1) login");
      System.out.println("  2) register");
      System.out.print("Choose [1/2]: ");
      String option = reader.readLine();
      if (option == null) {
        throw new IOException("Input stream closed while waiting for authentication option.");
      }

      option = option.trim();
      if ("1".equals(option) || "login".equalsIgnoreCase(option)) {
        runLogin(reader, session);
      } else if ("2".equals(option) || "register".equalsIgnoreCase(option)) {
        runRegister(reader, session);
      } else {
        System.out.println("Invalid option. Please choose 1 or 2.");
      }

      if (!session.isAuthenticated()) {
        System.out.println("[client] authentication failed. " + session.getLastAuthError());
      }
    }
  }

  private static void runLogin(BufferedReader reader, ChatClientSession session) throws IOException {
    String email = askRequiredInput(reader, "Enter email: ");
    String password = askRequiredInput(reader, "Enter password: ");
    boolean ok = session.login(email, password);
    if (!ok) {
      System.out.println("[client] login failed. " + session.getLastAuthError());
    }
  }

  private static void runRegister(BufferedReader reader, ChatClientSession session) throws IOException {
    String email = askRequiredInput(reader, "Enter email: ");
    String password = askRequiredInput(reader, "Enter password: ");
    boolean ok = session.register(email, password);
    if (!ok) {
      System.out.println("[client] register failed. " + session.getLastAuthError());
    }
  }

  private static void printHelp() {
    System.out.println("Commands:");
    System.out.println("  /login");
    System.out.println("  /register");
    System.out.println("  /send <toUserId> <text>");
    System.out.println("  /help");
    System.out.println("  /exit");
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
