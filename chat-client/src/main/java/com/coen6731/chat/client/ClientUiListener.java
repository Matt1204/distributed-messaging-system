package com.coen6731.chat.client;

/**
 * Responsibility: receive client session and server-stream updates for UI rendering.
 * Input: connection/auth/message lifecycle events.
 * Output: UI state refreshes or user-visible notifications.
 */
public interface ClientUiListener {
  void onInfo(String text);

  void onConnectionState(boolean connected);

  void onAuthState(boolean authenticated, String email, String error);

  void onChatMessage(String conversationId, String fromEmail, String text, String msgId, long sentAtMs);

  void onSendAck(String clientMsgId, boolean success, String code, String reason);

  void onConversationDataChanged();

  void onHistoryResultSummary(String conversationId, long startSequenceId, int messageCount);

  void onCatchupResultSummary(String conversationId, long startSequenceId, int messageCount);

  void onError(String code, String reason);
}
