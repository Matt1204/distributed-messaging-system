package com.coen6731.chat.client;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.InboundMessage;
import com.coen6731.chat.SendMessageAck;
import com.coen6731.chat.SendStatus;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.ServerEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
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
      Supplier<ClientUiListener> uiListenerSupplier) {
    this.dbManagerSupplier = dbManagerSupplier;
    this.heartbeatManager = heartbeatManager;
    this.reconnectCallback = reconnectCallback;
    this.onConnectionHealthy = onConnectionHealthy;
    this.onAuthSuccess = onAuthSuccess;
    this.onAuthFailed = onAuthFailed;
    this.currentUserIdSupplier = currentUserIdSupplier;
    this.currentEmailSupplier = currentEmailSupplier;
    this.uiListenerSupplier = uiListenerSupplier;
  }

  /**
   * Responsibility: route each server event to the matching domain handler.
   * Input: one ServerEvent from stream.
   * Output: DB/UI/reconnect side-effects.
   */
  @Override
  public void onNext(ServerEvent value) {
    onConnectionHealthy.run();
    switch (value.getPayloadCase()) {
      case INBOUNDMESSAGE:
        handleInboundMessage(value.getInboundMessage());
        break;
      case SENDMESSAGEACK:
        handleSendMessageAck(value.getSendMessageAck());
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
   * Responsibility: persist inbound message and notify UI.
   * Input: canonical inbound message payload.
   * Output: local INBOUND row, conversation refresh, and chat callback.
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
        safe(msg.getFromUserId()),
        senderEmail,
        safe(msg.getToUserId()),
        currentEmail,
        safe(msg.getText()),
        msg.getSentAtMs(),
        SendStatus.PERSISTED_PENDING_DELIVERY.name()); // TODO: SQLite's inbound msg should always be "DELIVERED" or something

    if (!currentUserId.isBlank()) {
      dbManager.updateLastSyncSequenceId(currentUserId, msg.getServerMsgId());
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
   * Output: DB row update/delete and user-visible ack feedback.
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
          effectiveSentAt,
          ack.getStatus().name());
      notifySendAck(ack.getClientMsgId(), true, "", "");
      notifyConversationRefresh();
      return;
    }

    // send failed path, delete the provisional outbound msg row
    dbManager.deleteOutboundByClientMsgId(ack.getClientMsgId());
    notifySendAck(ack.getClientMsgId(), false, ack.getErrorCode(), ack.getErrorReason());
    notifyConversationRefresh();
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
   * Output: non-null trimmed-safe string.
   */
  private String safe(String value) {
    return value == null ? "" : value;
  }
}
