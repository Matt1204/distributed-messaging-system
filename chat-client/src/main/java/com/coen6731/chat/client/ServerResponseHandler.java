package com.coen6731.chat.client;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.CanonicalMessage;
import com.coen6731.chat.CatchupConversationResult;
import com.coen6731.chat.CatchupResult;
import com.coen6731.chat.InboundMessage;
import com.coen6731.chat.MsgHistoryResult;
import com.coen6731.chat.SendMessageAck;
import com.coen6731.chat.SendStatus;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.ServerEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Responsibility: consume server stream events and reconcile local persistence/UI state.
 * Input: ServerEvent payloads from grpc stream.
 * Output: local DB writes, callbacks, and reconnect/auth state changes.
 */
public class ServerResponseHandler implements StreamObserver<ServerEvent> {
  private final Supplier<DatabaseManager> dbManagerSupplier;
  private final HeartbeatManager heartbeatManager;
  private final Runnable reconnectCallback;
  private final Runnable onConnectionHealthy;
  private final Consumer<AuthSuccess> onAuthSuccess;
  private final BiConsumerString onAuthFailed;
  private final Supplier<String> currentUserIdSupplier;
  private final Supplier<String> currentEmailSupplier;
  private final Supplier<ClientUiListener> uiListenerSupplier;
  private final Runnable onReconnectCatchupReady;

  @FunctionalInterface
  public interface BiConsumerString {
    void accept(String code, String reason);
  }

  /**
   * Responsibility: construct event handler with dependencies from session.
   * Input: suppliers/callbacks for db, heartbeat, reconnect, auth, and UI.
   * Output: ready stream observer.
   */
  public ServerResponseHandler(
      Supplier<DatabaseManager> dbManagerSupplier,
      HeartbeatManager heartbeatManager,
      Runnable reconnectCallback,
      Runnable onConnectionHealthy,
      Consumer<AuthSuccess> onAuthSuccess,
      BiConsumerString onAuthFailed,
      Supplier<String> currentUserIdSupplier,
      Supplier<String> currentEmailSupplier,
      Supplier<ClientUiListener> uiListenerSupplier,
      Runnable onReconnectCatchupReady) {
    this.dbManagerSupplier = dbManagerSupplier;
    this.heartbeatManager = heartbeatManager;
    this.reconnectCallback = reconnectCallback;
    this.onConnectionHealthy = onConnectionHealthy;
    this.onAuthSuccess = onAuthSuccess;
    this.onAuthFailed = onAuthFailed;
    this.currentUserIdSupplier = currentUserIdSupplier;
    this.currentEmailSupplier = currentEmailSupplier;
    this.uiListenerSupplier = uiListenerSupplier;
    this.onReconnectCatchupReady = onReconnectCatchupReady;
  }

  /**
   * Responsibility: route each server event to the matching domain handler.
   * Input: one ServerEvent from stream.
   * Output: DB/UI/reconnect side-effects.
   */
  @Override
  public void onNext(ServerEvent value) {
    onConnectionHealthy.run();
    onReconnectCatchupReady.run();
    switch (value.getPayloadCase()) {
      case INBOUNDMESSAGE:
        handleInboundMessage(value.getInboundMessage());
        break;
      case SENDMESSAGEACK:
        handleSendMessageAck(value.getSendMessageAck());
        break;
      case CATCHUPRESULT:
        handleCatchupResult(value.getCatchupResult());
        break;
      case MSGHISTORYRESULT:
        handleMsgHistoryResult(value.getMsgHistoryResult());
        break;
      case SERVERERROR:
        handleServerError(value.getServerError());
        break;
      case HEARTBEATPONG:
        heartbeatManager.handlePong();
        break;
      case AUTHSUCCESS:
        handleAuthSuccess(value.getAuthSuccess());
        break;
      case PAYLOAD_NOT_SET:
      default:
        System.out.println("[client] received empty event");
        break;
    }
  }

  /**
   * Responsibility: handle stream errors and trigger reconnect when needed.
   * Input: throwable from grpc stream.
   * Output: debug notifications and reconnect signal.
   */
  @Override
  public void onError(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      StatusRuntimeException sre = (StatusRuntimeException) t;
      Status status = sre.getStatus();
      if (status.getCode() == Status.Code.UNAVAILABLE || status.getCode() == Status.Code.CANCELLED) {
        System.out.println("[client] OnError(), Server code: " + status.getCode());
        notifyInfo("[client] OnError(), Server code: " + status.getCode());
      } else {
        System.out.println("[client] OnError(), Stream error: " + status);
        notifyInfo("[client] OnError(), Stream error: " + status);
      }
    } else {
      System.out.println("[client] OnError " + t.getMessage());
      notifyInfo("[client] OnError " + t.getMessage());
    }

    if (heartbeatManager.isThreeStrikes()) {
      notifyInfo("[client] OnError(), already 3 strikes, calling reconnect immediately");
      reconnectCallback.run();
    } else {
      notifyInfo("[client] OnError(), not 3 strikes, wait next ping...");
    }
  }

  /**
   * Responsibility: reconnect when server closes stream.
   * Input: grpc completion callback.
   * Output: reconnect request.
   */
  @Override
  public void onCompleted() {
    System.out.println("[client] stream closed by server");
    notifyInfo("[client] stream closed by server");
    reconnectCallback.run();
  }

  /**
   * Responsibility: pass auth success to session layer.
   * Input: auth success payload.
   * Output: session auth state update.
   */
  private void handleAuthSuccess(AuthSuccess authSuccess) {
    onAuthSuccess.accept(authSuccess);
  }

  /**
   * Responsibility: persist one live inbound message and advance cursor by sequence.
   * Input: inbound message payload.
   * Output: local write + cursor update + UI refresh.
   */
  private void handleInboundMessage(InboundMessage msg) {
    DatabaseManager dbManager = dbManagerSupplier.get();
    if (dbManager == null) {
      notifyInfo("[client] received message before local user database is ready");
      return;
    }

    String currentUserId = safe(currentUserIdSupplier.get());
    String currentEmail = safe(currentEmailSupplier.get());
    String senderEmail = safe(msg.getFromEmail());

    dbManager.insertInboundMessage(
        msg.getServerMsgId(),
        safe(msg.getClientMsgId()),
        safe(msg.getConversationId()),
        msg.getSequenceId(),
        safe(msg.getFromUserId()),
        senderEmail,
        safe(msg.getToUserId()),
        currentEmail,
        safe(msg.getText()),
        msg.getSentAtMs(),
        SendStatus.PERSISTED_PENDING_DELIVERY.name());

    if (!currentUserId.isBlank()) {
      dbManager.advanceConversationCursorIfHigher(currentUserId, safe(msg.getConversationId()), msg.getSequenceId());
    }

    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onChatMessage(
          msg.getConversationId(),
          senderEmail,
          msg.getText(),
          msg.getServerMsgId(),
          msg.getSentAtMs());
      listener.onConversationDataChanged();
    }
  }

  /**
   * Responsibility: reconcile outbound provisional row with send ack result.
   * Input: send ack payload.
   * Output: DB row update/delete, cursor update, and user-visible ack feedback.
   */
  private void handleSendMessageAck(SendMessageAck ack) {
    DatabaseManager dbManager = dbManagerSupplier.get();
    if (dbManager == null) {
      notifyInfo("[client] received send ack before local user database is ready");
      return;
    }

    if (ack.getStatus() == SendStatus.PERSISTED_PENDING_DELIVERY
        || ack.getStatus() == SendStatus.DELIVERED_LIVE) {
      long effectiveSentAt = ack.getAckTs() > 0 ? ack.getAckTs() : Instant.now().toEpochMilli();
      dbManager.markOutboundAckSuccess(
          ack.getClientMsgId(),
          ack.getServerMsgId(),
          ack.getConversationId(),
          ack.getSequenceId(),
          effectiveSentAt,
          ack.getStatus().name());

      String currentUserId = safe(currentUserIdSupplier.get());
      if (!currentUserId.isBlank()) {
        dbManager.advanceConversationCursorIfHigher(currentUserId, ack.getConversationId(), ack.getSequenceId());
      }

      notifySendAck(ack.getClientMsgId(), true, "", "");
      notifySendAckDetailed(
          ack.getClientMsgId(),
          ack.getServerMsgId(),
          ack.getConversationId(),
          ack.getSequenceId(),
          true,
          "",
          "");
      notifyConversationRefresh();
      return;
    }

    dbManager.deleteOutboundByClientMsgId(ack.getClientMsgId());
    notifySendAck(ack.getClientMsgId(), false, ack.getErrorCode(), ack.getErrorReason());
    notifySendAckDetailed(
        ack.getClientMsgId(),
        ack.getServerMsgId(),
        ack.getConversationId(),
        ack.getSequenceId(),
        false,
        ack.getErrorCode(),
        ack.getErrorReason());
    notifyConversationRefresh();
  }

  /**
   * Responsibility: persist multi-conversation catchup payload and advance local cursors.
   * Input: catchup result with per-conversation newest-first messages.
   * Output: local idempotent writes, cursor updates, and one UI refresh.
   */
  private void handleCatchupResult(CatchupResult catchupResult) {
    DatabaseManager dbManager = dbManagerSupplier.get();
    if (dbManager == null) {
      return;
    }

    String currentUserId = safe(currentUserIdSupplier.get());
    String currentEmail = safe(currentEmailSupplier.get());
    for (CatchupConversationResult conversationResult : catchupResult.getConversationResultsList()) {
      List<CanonicalMessage> messages = conversationResult.getMessagesList();
      for (CanonicalMessage message : messages) {
        persistCanonicalMessage(dbManager, message, currentUserId, currentEmail);
      }
      if (!currentUserId.isBlank()) {
        long highestPersistedSequenceId = maxPositiveSequenceId(messages);
        if (highestPersistedSequenceId > 0) {
          dbManager.upsertConversationCursor(
              currentUserId,
              conversationResult.getConversationId(),
              highestPersistedSequenceId);
        }
      }
      notifyCatchupResultSummary(
          conversationResult.getConversationId(), minPositiveSequenceId(messages), messages.size());
    }
    notifyConversationRefresh();
  }

  /**
   * Responsibility: persist one history page without advancing sync cursor.
   * Input: history response for one conversation.
   * Output: local idempotent writes and UI refresh.
   */
  private void handleMsgHistoryResult(MsgHistoryResult result) {
    DatabaseManager dbManager = dbManagerSupplier.get();
    if (dbManager == null) {
      return;
    }

    String currentUserId = safe(currentUserIdSupplier.get());
    String currentEmail = safe(currentEmailSupplier.get());
    List<CanonicalMessage> messages = result.getMessagesList();
    for (CanonicalMessage message : messages) {
      persistCanonicalMessage(dbManager, message, currentUserId, currentEmail);
    }
    notifyHistoryResultSummary(result.getConversationId(), minPositiveSequenceId(messages), messages.size());
    notifyConversationRefresh();
  }

  private long minPositiveSequenceId(List<CanonicalMessage> messages) {
    long min = Long.MAX_VALUE;
    for (CanonicalMessage message : messages) {
      if (message.getSequenceId() > 0) {
        min = Math.min(min, message.getSequenceId());
      }
    }
    return min == Long.MAX_VALUE ? 0L : min;
  }

  private long maxPositiveSequenceId(List<CanonicalMessage> messages) {
    long max = 0L;
    for (CanonicalMessage message : messages) {
      if (message.getSequenceId() > 0) {
        max = Math.max(max, message.getSequenceId());
      }
    }
    return max;
  }

  /**
   * Responsibility: persist canonical message into a unified idempotent SQLite path.
   * Input: canonical message and current authenticated user identity.
   * Output: inserted-or-ignored local row.
   */
  private void persistCanonicalMessage(
      DatabaseManager dbManager,
      CanonicalMessage message,
      String currentUserId,
      String currentEmail) {
    boolean isOutbound = !currentUserId.isBlank() && currentUserId.equals(message.getFromUserId());
    String direction = isOutbound ? "OUTBOUND" : "INBOUND";
    String senderEmail = safe(message.getFromEmail());
    String recipientEmail = isOutbound ? "" : currentEmail;
    String peerUserId = isOutbound ? safe(message.getToUserId()) : safe(message.getFromUserId());
    String peerEmail = isOutbound ? "" : senderEmail;

    dbManager.upsertCanonicalMessage(
        safe(message.getClientMsgId()),
        direction,
        safe(message.getServerMsgId()),
        safe(message.getConversationId()),
        message.getSequenceId(),
        safe(message.getFromUserId()),
        senderEmail,
        safe(message.getToUserId()),
        recipientEmail,
        safe(message.getText()),
        message.getSentAtMs(),
        SendStatus.PERSISTED_PENDING_DELIVERY.name(),
        peerUserId,
        peerEmail);
  }

  /**
   * Responsibility: handle server error payloads (legacy auth + generic errors).
   * Input: ServerError code/reason.
   * Output: auth callback and UI error callback.
   */
  private void handleServerError(ServerError err) {
    if (err.getCode().startsWith("AUTH_")
        || "BAD_REQUEST".equals(err.getCode())
        || "INTERNAL".equals(err.getCode())) {
      onAuthFailed.accept(err.getCode(), err.getReason());
    }
    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onError(err.getCode(), err.getReason());
    }
  }

  /**
   * Responsibility: send info text to UI listener if available.
   * Input: info text.
   * Output: listener callback side-effect.
   */
  private void notifyInfo(String text) {
    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onInfo(text);
    }
  }

  /**
   * Responsibility: publish outbound send ack result to UI.
   * Input: client message id and success/error metadata.
   * Output: listener callback side-effect.
   */
  private void notifySendAck(String clientMsgId, boolean success, String code, String reason) {
    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onSendAck(clientMsgId, success, code, reason);
    }
  }

  private void notifySendAckDetailed(
      String clientMsgId,
      String serverMsgId,
      String conversationId,
      long sequenceId,
      boolean success,
      String code,
      String reason) {
    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onSendAckDetailed(
          clientMsgId, serverMsgId, conversationId, sequenceId, success, code, reason);
    }
  }

  private void notifyHistoryResultSummary(String conversationId, long startSequenceId, int messageCount) {
    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onHistoryResultSummary(conversationId, startSequenceId, messageCount);
    }
  }

  private void notifyCatchupResultSummary(String conversationId, long startSequenceId, int messageCount) {
    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onCatchupResultSummary(conversationId, startSequenceId, messageCount);
    }
  }

  /**
   * Responsibility: trigger conversation list/detail refresh in UI.
   * Input: none.
   * Output: listener callback side-effect.
   */
  private void notifyConversationRefresh() {
    ClientUiListener listener = uiListenerSupplier.get();
    if (listener != null) {
      listener.onConversationDataChanged();
    }
  }

  /**
   * Responsibility: normalize nullable string values.
   * Input: possibly null text.
   * Output: non-null safe string.
   */
  private String safe(String value) {
    return value == null ? "" : value;
  }
}
