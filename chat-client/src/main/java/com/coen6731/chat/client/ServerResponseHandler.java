package com.coen6731.chat.client;

import com.coen6731.chat.ChatMessage;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.ServerEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

public class ServerResponseHandler implements StreamObserver<ServerEvent> {
    private final DatabaseManager dbManager;
    private final HeartbeatManager heartbeatManager;
    private final Runnable reconnectCallback;
    private final Runnable onConnectionHealthy;
    private final String currentUserId;

    public ServerResponseHandler(
            DatabaseManager dbManager,
            HeartbeatManager heartbeatManager,
            Runnable reconnectCallback,
            Runnable onConnectionHealthy,
            String currentUserId) {
        this.dbManager = dbManager;
        this.heartbeatManager = heartbeatManager;
        this.reconnectCallback = reconnectCallback;
        this.onConnectionHealthy = onConnectionHealthy;
        this.currentUserId = currentUserId;
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
            System.out.println("[client] OnError(), already 3 strikes, calling Recconect immediately !!!");
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

    private void handleChatMessage(ChatMessage msg) {
        dbManager.insertMessage(msg.getServerMsgId(), msg.getFromUserId(), msg.getText(), msg.getTs());
        if (currentUserId != null) {
            dbManager.updateUserState(currentUserId, currentUserId, msg.getServerMsgId());
        }
        System.out.println(
                msg.getFromUserId()
                        + ": "
                        + msg.getText()
                        + " ("
                        + msg.getServerMsgId()
                        + ", "
                        + msg.getTs()
                        + ")");
    }

    private void handleServerError(ServerError err) {
        System.out.println("ERROR code=" + err.getCode() + " reason=" + err.getReason());
    }
}
