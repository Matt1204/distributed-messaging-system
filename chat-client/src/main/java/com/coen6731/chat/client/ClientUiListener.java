package com.coen6731.chat.client;

public interface ClientUiListener {
  void onInfo(String text);

  void onConnectionState(boolean connected);

  void onAuthState(boolean authenticated, String email, String error);

  void onChatMessage(String fromEmail, String text, String msgId, long ts);

  void onError(String code, String reason);
}
