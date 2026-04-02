package com.coen6731.chat.client.perf;

import com.coen6731.chat.client.ChatClientSession;

/**
 * Responsibility: provision test identities by register-or-login fallback.
 * Input: session plus email/password credentials.
 * Output: authenticated session state for perf traffic generation.
 */
public final class AccountProvisioner {
  private static final long AUTH_RETRY_INTERVAL_MS = 300L;

  private AccountProvisioner() {}

  public static void ensureAuthenticated(
      ChatClientSession session,
      String email,
      String password,
      long connectTimeoutMs,
      String roleLabel)
      throws IllegalStateException {
    long boundedTimeoutMs = Math.max(1000L, connectTimeoutMs);
    long deadlineMs = System.currentTimeMillis() + boundedTimeoutMs;
    String registerErr = "";
    String loginErr = "";
    int attempts = 0;

    while (System.currentTimeMillis() < deadlineMs) {
      attempts++;
      if (session.isAuthenticated()) {
        return;
      }

      if (session.register(email, password)) {
        return;
      }
      registerErr = safe(session.getLastAuthError());

      if (session.login(email, password)) {
        return;
      }
      loginErr = safe(session.getLastAuthError());

      // Credentials mismatch is not recoverable by retry.
      if (isInvalidCredentials(registerErr) && isInvalidCredentials(loginErr)) {
        break;
      }

      sleepQuietly(Math.min(AUTH_RETRY_INTERVAL_MS, Math.max(50L, deadlineMs - System.currentTimeMillis())));
    }

    throw new IllegalStateException(
        roleLabel
            + " register/login failed within timeoutMs="
            + boundedTimeoutMs
            + " attempts="
            + attempts
            + ". registerErr="
            + registerErr
            + " loginErr="
            + loginErr);
  }

  private static boolean isInvalidCredentials(String error) {
    String normalized = safe(error).toUpperCase();
    return normalized.contains("AUTH_INVALID_CREDENTIALS");
  }

  private static void sleepQuietly(long millis) {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }
}
