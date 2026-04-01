package com.coen6731.chat.client.perf;

import com.coen6731.chat.client.ChatClientSession;

/**
 * Responsibility: provision test identities by register-or-login fallback.
 * Input: session plus email/password credentials.
 * Output: authenticated session state for perf traffic generation.
 */
public final class AccountProvisioner {
  private AccountProvisioner() {}

  public static void ensureAuthenticated(
      ChatClientSession session,
      String email,
      String password,
      long connectTimeoutMs,
      String roleLabel)
      throws IllegalStateException {
    boolean connected = session.awaitConnected(connectTimeoutMs);
    if (!connected) {
      throw new IllegalStateException(roleLabel + " failed to connect within timeout.");
    }

    if (session.register(email, password)) {
      return;
    }

    String registerErr = safe(session.getLastAuthError());
    if (session.login(email, password)) {
      return;
    }

    String loginErr = safe(session.getLastAuthError());
    throw new IllegalStateException(
        roleLabel
            + " register/login failed. registerErr="
            + registerErr
            + " loginErr="
            + loginErr);
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }
}
