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

    // TEST ONLY - TO BE REMOVED
    TestConfig testConfig = new TestConfig(
        isTruthy(dotenv.get("SKIP_LOGIN") != null ? dotenv.get("SKIP_LOGIN") : System.getenv("SKIP_LOGIN")),
        isTruthy(dotenv.get("SELECT_USER_A") != null ? dotenv.get("SELECT_USER_A") : System.getenv("SELECT_USER_A")),
        dotenv.get("USER_A_EMAIL") != null ? dotenv.get("USER_A_EMAIL") : System.getenv("USER_A_EMAIL"),
        dotenv.get("USER_A_PASSWORD") != null ? dotenv.get("USER_A_PASSWORD") : System.getenv("USER_A_PASSWORD"),
        isTruthy(dotenv.get("SELECT_USER_B") != null ? dotenv.get("SELECT_USER_B") : System.getenv("SELECT_USER_B")),
        dotenv.get("USER_B_EMAIL") != null ? dotenv.get("USER_B_EMAIL") : System.getenv("USER_B_EMAIL"),
        dotenv.get("USER_B_PASSWORD") != null ? dotenv.get("USER_B_PASSWORD") : System.getenv("USER_B_PASSWORD")
    );

    ChatClientSession session = new ChatClientSession(target);
    System.out.println("[client] connected to " + target);

    SwingUtilities.invokeLater(() -> {
      ChatWindow window = new ChatWindow(session, debugSidebarEnabled, testConfig);
      window.setVisible(true);
    });
  }

  // TEST ONLY - TO BE REMOVED
  public static class TestConfig {
    public final boolean skipLogin;
    public final boolean selectUserA;
    public final String userAEmail;
    public final String userAPassword;
    public final boolean selectUserB;
    public final String userBEmail;
    public final String userBPassword;

    public TestConfig(boolean skipLogin, boolean selectUserA, String userAEmail, String userAPassword,
                      boolean selectUserB, String userBEmail, String userBPassword) {
      this.skipLogin = skipLogin;
      this.selectUserA = selectUserA;
      this.userAEmail = userAEmail;
      this.userAPassword = userAPassword;
      this.selectUserB = selectUserB;
      this.userBEmail = userBEmail;
      this.userBPassword = userBPassword;
    }
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
