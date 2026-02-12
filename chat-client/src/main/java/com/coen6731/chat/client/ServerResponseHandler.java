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
    private final String currentUserId;

    public ServerResponseHandler(
            DatabaseManager dbManager,
            HeartbeatManager heartbeatManager,
            Runnable reconnectCallback,
            String currentUserId) {
        this.dbManager = dbManager;
        this.heartbeatManager = heartbeatManager;
        this.reconnectCallback = reconnectCallback;
        this.currentUserId = currentUserId;
    }

    @Override
    public void onNext(ServerEvent value) {
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
                System.out.println("[client] Server unavailable/disconnected: " + status.getCode());
            } else {
                System.out.println("[client] Stream error: " + status);
            }
        } else {
            System.out.println("[client] Stream error: " + t.getMessage());
        }
        // Trigger reconnect on error
        reconnectCallback.run();
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
