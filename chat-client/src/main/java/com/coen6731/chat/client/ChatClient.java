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
    String debugSidebar = dotenv.get("CHAT_CLIENT_DEBUG_SIDEBAR");
    if (debugSidebar == null) {
      debugSidebar = System.getenv("CHAT_CLIENT_DEBUG_SIDEBAR");
    }
    boolean debugSidebarEnabled = isTruthy(debugSidebar);

    ChatClientSession session = new ChatClientSession(target);
    System.out.println("[client] connected to " + target);

    SwingUtilities.invokeLater(() -> {
      ChatWindow window = new ChatWindow(session, debugSidebarEnabled);
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
}
