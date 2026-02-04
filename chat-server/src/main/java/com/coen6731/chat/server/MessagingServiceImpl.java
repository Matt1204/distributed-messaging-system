package com.coen6731.chat.server;

import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.Heartbeat;
import com.coen6731.chat.ChatMessage;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.Register;
import com.coen6731.chat.SendMessage;
import com.coen6731.chat.ServerEvent;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.UUID;

/**
 * Implementation of the MessagingService defined in the proto file.
 * Extends the generated base class.
 */
public class MessagingServiceImpl extends MessagingServiceGrpc.MessagingServiceImplBase {
  private final ConnectionRegistry registry;

  public MessagingServiceImpl(ConnectionRegistry registry) {
    this.registry = registry;
  }

  /**
   * Handles the bidirectional streaming RPC 'Chat'.
   *
   * @param responseObserver The observer to send messages BACK to the client (server -> client).
   * @return A StreamObserver to receive messages FROM the client (client -> server).
   */
  @Override
  public StreamObserver<ClientEvent> chat(StreamObserver<ServerEvent> responseObserver) {
    // We return an anonymous implementation of StreamObserver<ClientEvent> to handle incoming client events.
    return new StreamObserver<ClientEvent>() {
      private String currentUserId;

      // Called when the client sends a message, and server receives it (ClientEvent).
      @Override
      public void onNext(ClientEvent event) {
        switch (event.getPayloadCase()) {
          case REGISTER:
            handleRegister(event.getRegister());
            break;
          case SENDMESSAGE:
            handleSendMessage(event.getSendMessage());
            break;
          case HEARTBEAT:
            handleHeartbeat(event.getHeartbeat());
            break;
          case PAYLOAD_NOT_SET:
          default:
            sendError("BAD_REQUEST", "Empty client event");
            break;
        }
      }

      // Called when the stream encounters an error.
      @Override
      public void onError(Throwable t) {
        System.out.println("[server] onError() - stream error: " + t.getMessage());
        cleanup();
      }

      // Called when the client finishes sending messages (closes the stream).
      @Override
      public void onCompleted() {
        System.out.println("[server] onCompleted() - stream completed");
        cleanup();
        // We must also complete the response observer to signal we are done sending.
        responseObserver.onCompleted();
      }

      private void handleRegister(Register register) {
        String userId = register.getUserId();
        if (userId == null || userId.isBlank()) {
          sendError("BAD_REQUEST", "userId is required");
          return;
        }
        currentUserId = userId;

        registry.register(userId, responseObserver); // Register user. Appending UserId -> stream to to mapping.
        System.out.println("[server] handleRegister() - registered userId=" + userId);
      }

      private void handleSendMessage(SendMessage sendMessage) {
        // Enforce that a user must be registered before sending messages.
        if (currentUserId == null) {
          System.out.println("[server] handleSendMessage() - currentUserId is null");
          sendError("UNREGISTERED", "Please register before sending messages");
          return;
        }
        String toUserId = sendMessage.getToUserId();
        if (toUserId == null || toUserId.isBlank()) {
          System.out.println("[server] handleSendMessage() - toUserId is null or blank");
          sendError("BAD_REQUEST", "toUserId is required");
          return;
        }
        // check is user is registered. "is user connected to the server?"
        UserSession targetSession = registry.getSession(toUserId);
        if (targetSession == null) {
          System.out.println("[server] handleSendMessage() - target user " + toUserId + " is offline");
          sendError("USER_OFFLINE", "User " + toUserId + " is offline");
          return;
        }
        sendChatMessageToTarget(targetSession, sendMessage);
      }

      private void handleHeartbeat(Heartbeat heartbeat) {
        // Update last seen timestamp.
        if (currentUserId != null) {
          System.out.println("[server] handleHeartbeat() - received heartbeat from " + currentUserId);
          registry.updateHeartbeat(currentUserId);
        }
      }

      // Helper to send an error message to the client.
      private void sendError(String code, String reason) {
        ServerError error = ServerError.newBuilder().setCode(code).setReason(reason).build();
        responseObserver.onNext(ServerEvent.newBuilder().setServerError(error).build());
      }

      // Helper to send a chat message to the target user.
      private void sendChatMessageToTarget(
          UserSession targetSession, SendMessage sendMessage) {
        // Construct the payload for "ServerEvent" -> "ChatMessage"
        ChatMessage message =
            ChatMessage.newBuilder()
                .setFromUserId(currentUserId)
                .setText(sendMessage.getText())
                .setServerMsgId(UUID.randomUUID().toString())
                .setTs(Instant.now().toEpochMilli())
                .build();
        // Send the message to the target user.
        targetSession.send(ServerEvent.newBuilder().setChatMessage(message).build());
      }

      // Cleanup logic when connection ends.
      private void cleanup() {
        if (currentUserId != null) {
          registry.unregister(currentUserId, responseObserver);
          System.out.println("[server] unregistered userId=" + currentUserId);
        }
      }
    };
  }
}
