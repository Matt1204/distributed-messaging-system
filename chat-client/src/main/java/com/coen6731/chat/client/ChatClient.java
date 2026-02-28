package com.coen6731.chat.client;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import javax.swing.SwingUtilities;

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
    String isProdValue = dotenv.get("IS_PROD");
    if (isProdValue == null) {
      isProdValue = System.getenv("IS_PROD");
    }
    String debugSidebar = dotenv.get("CHAT_CLIENT_DEBUG_SIDEBAR");
    if (debugSidebar == null) {
      debugSidebar = System.getenv("CHAT_CLIENT_DEBUG_SIDEBAR");
    }
    String historyPageSize = dotenv.get("CHAT_CLIENT_HISTORY_PAGE_SIZE");
    if (historyPageSize == null) {
      historyPageSize = System.getenv("CHAT_CLIENT_HISTORY_PAGE_SIZE");
    }
    String catchupLimit = dotenv.get("CHAT_CLIENT_CATCHUP_LIMIT");
    if (catchupLimit == null) {
      catchupLimit = System.getenv("CHAT_CLIENT_CATCHUP_LIMIT");
    }

    boolean debugSidebarEnabled = isTruthy(debugSidebar);
    int historySize = parsePositiveInt(historyPageSize, 20, 200);
    int perConversationCatchupLimit = parsePositiveInt(catchupLimit, 50, 200);
    boolean isProd = isTruthy(isProdValue);

    validateTargetWithEnvironment(target, isProd);

    ChatClientSession session = new ChatClientSession(target, perConversationCatchupLimit, isProd);
    System.out.println("[client] connected to " + target);

    SwingUtilities.invokeLater(() -> {
      ChatWindow window = new ChatWindow(session, debugSidebarEnabled, historySize);
      window.setVisible(true);
    });
  }

  private static boolean isTruthy(String value) {
    if (value == null) {
      return false;
    }
    String normalized = value.trim().toLowerCase();
    return "1".equals(normalized)
        || "true".equals(normalized)
        || "yes".equals(normalized)
        || "on".equals(normalized);
  }

  private static int parsePositiveInt(String value, int fallback, int maxValue) {
    if (value == null || value.isBlank()) {
      return fallback;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      if (parsed <= 0) {
        return fallback;
      }
      return Math.min(parsed, maxValue);
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  private static void validateTargetWithEnvironment(String target, boolean isProd) {
    if (target == null || target.isBlank()) {
      throw new IllegalArgumentException("TARGET is required and cannot be empty.");
    }
    String normalized = target.trim().toLowerCase();
    boolean targetLooksProd = looksLikeProdTarget(normalized);
    if (isProd != targetLooksProd) {
      throw new IllegalStateException(
          "IS_PROD="
              + isProd
              + " mismatches TARGET="
              + target
              + ". Update TARGET or IS_PROD so they match.");
    }
  }

  private static boolean looksLikeProdTarget(String normalizedTarget) {
    if (normalizedTarget.contains("localhost")
        || normalizedTarget.contains("127.0.0.1")
        || normalizedTarget.contains("0.0.0.0")
        || normalizedTarget.contains(".local")
        || normalizedTarget.contains("dev")
        || normalizedTarget.contains("staging")
        || normalizedTarget.contains("test")) {
      return false;
    }
    return true;
  }
}
