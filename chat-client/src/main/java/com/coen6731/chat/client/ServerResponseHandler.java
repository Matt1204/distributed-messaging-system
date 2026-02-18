package com.coen6731.chat.client;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.ChatMessage;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.ServerEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ServerResponseHandler implements StreamObserver<ServerEvent> {
    private final Supplier<DatabaseManager> dbManagerSupplier;
    private final HeartbeatManager heartbeatManager;
    private final Runnable reconnectCallback;
    private final Runnable onConnectionHealthy;
    private final Consumer<AuthSuccess> onAuthSuccess;
    private final BiConsumerString onAuthFailed;
    private final Supplier<String> currentUserIdSupplier;

    @FunctionalInterface
    public interface BiConsumerString {
        void accept(String code, String reason);
    }

    public ServerResponseHandler(
            Supplier<DatabaseManager> dbManagerSupplier,
            HeartbeatManager heartbeatManager,
            Runnable reconnectCallback,
            Runnable onConnectionHealthy,
            Consumer<AuthSuccess> onAuthSuccess,
            BiConsumerString onAuthFailed,
            Supplier<String> currentUserIdSupplier) {
        this.dbManagerSupplier = dbManagerSupplier;
        this.heartbeatManager = heartbeatManager;
        this.reconnectCallback = reconnectCallback;
        this.onConnectionHealthy = onConnectionHealthy;
        this.onAuthSuccess = onAuthSuccess;
        this.onAuthFailed = onAuthFailed;
        this.currentUserIdSupplier = currentUserIdSupplier;
    }

    @Override
    public void onNext(ServerEvent value) {
        onConnectionHealthy.run();
        switch (value.getPayloadCase()) {
            case CHATMESSAGE:
                handleChatMessage(value.getChatMessage());
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

    @Override
    public void onError(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException sre = (StatusRuntimeException) t;
            Status status = sre.getStatus();
            if (status.getCode() == Status.Code.UNAVAILABLE || status.getCode() == Status.Code.CANCELLED) {
                System.out.println("[client] OnError(), Server code: " + status.getCode());
            } else {
                System.out.println("[client] OnError(), Stream error: " + status);
            }
        } else {
            System.out.println("[client] OnError " + t.getMessage());
        }

        if (heartbeatManager.isThreeStrikes()) {
            System.out.println("[client] OnError(), already 3 strikes, calling reconnect immediately");
            reconnectCallback.run();
        } else {
            System.out.println("[client] OnError(), not 3 strikes, wait next ping...");
        }
    }

    @Override
    public void onCompleted() {
        System.out.println("[client] stream closed by server");
        reconnectCallback.run();
    }

    private void handleAuthSuccess(AuthSuccess authSuccess) {
        onAuthSuccess.accept(authSuccess);
    }

    private void handleChatMessage(ChatMessage msg) {
        DatabaseManager dbManager = dbManagerSupplier.get();
        if (dbManager == null) {
            System.out.println("[client] received message before local user database is ready");
            return;
        }
        dbManager.insertMessage(msg.getServerMsgId(), msg.getFromUserId(), msg.getText(), msg.getTs());
        String currentUserId = currentUserIdSupplier.get();
        if (currentUserId != null && !currentUserId.isBlank()) {
            dbManager.updateLastSyncSequenceId(currentUserId, msg.getServerMsgId());
        }
        System.out.println(
                msg.getFromEmail()
                        + ": "
                        + msg.getText()
                        + " ("
                        + msg.getServerMsgId()
                        + ", "
                        + msg.getTs()
                        + ")");
    }

    private void handleServerError(ServerError err) {
        if (err.getCode().startsWith("AUTH_") || "BAD_REQUEST".equals(err.getCode()) || "INTERNAL".equals(err.getCode())) {
            onAuthFailed.accept(err.getCode(), err.getReason());
        }
        System.out.println("ERROR code=" + err.getCode() + " reason=" + err.getReason());
    }
}
