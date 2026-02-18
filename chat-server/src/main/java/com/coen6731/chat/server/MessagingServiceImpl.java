package com.coen6731.chat.server;

import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.RegisterUser;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.HeartbeatPing;
import com.coen6731.chat.HeartbeatPong;
import com.coen6731.chat.ChatMessage;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.SendMessage;
import com.coen6731.chat.ServerEvent;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import io.grpc.Status;

/**
 * Implementation of the MessagingService defined in the proto file.
 */
@Component
public class MessagingServiceImpl extends MessagingServiceGrpc.MessagingServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(MessagingServiceImpl.class);
  private static final String ERROR_NOT_REGISTERED = "NOT_REGISTERED";
  private static final String ERROR_BAD_REQUEST = "BAD_REQUEST";
  private static final String ERROR_USER_OFFLINE = "USER_OFFLINE";

  private final ConnectionRegistry connectionRegistry;
  private final CosmosDBHandler cosmosDBHandler;

  @org.springframework.beans.factory.annotation.Value("${container.app.replica.name}")
  private String serverReplicaId;

  public MessagingServiceImpl(ConnectionRegistry registry, CosmosDBHandler cosmosDBHandler) {
    this.connectionRegistry = registry;
    this.cosmosDBHandler = cosmosDBHandler;
  }

  @Override
  public StreamObserver<ClientEvent> chat(StreamObserver<ServerEvent> responseObserver) {
    final String userId = UserIdInterceptor.USER_ID_CTX_KEY.get();
    boolean userInDB = cosmosDBHandler.userExistsInDB(userId);
    logConnectionState(userId, userInDB);

    UserSession initialSession =
        userInDB
            ? connectionRegistry.handleUserOnline(userId, responseObserver)
            : new UserSession(responseObserver);

    return new StreamObserver<ClientEvent>() {
      private boolean isRegistered = userInDB;
      private UserSession session = initialSession;

      @Override
      public void onNext(ClientEvent event) {
        if (isHeartbeat(event)) {
          handleHeartbeatPing(event.getHeartbeatPing());
          return;
        }

        if (!canProcess(event)) {
          return;
        }

        switch (event.getPayloadCase()) {
          case REGISTERUSER:
            handleRegisterUser(event.getRegisterUser());
            break;
          case SENDMESSAGE:
            handleSendMessage(event.getSendMessage());
            break;
          case PAYLOAD_NOT_SET:
          default:
            sendError(ERROR_BAD_REQUEST, "Empty client event");
            break;
        }
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        if (status.getCode() == Status.Code.CANCELLED) {
          logger.info("[{}] client disconnected (cancelled stream)", serverReplicaId);
        } else {
          logger.warn("[{}] onError() - stream error: {}", serverReplicaId, status);
        }
        cleanup();
      }

      @Override
      public void onCompleted() {
        logger.info("[{}] onCompleted() - stream completed", serverReplicaId);
        cleanup();
        session.close();
      }

      private boolean isHeartbeat(ClientEvent event) {
        return event.getPayloadCase() == ClientEvent.PayloadCase.HEARTBEATPING;
      }

      private boolean canProcess(ClientEvent event) {
        if (isRegistered || event.getPayloadCase() == ClientEvent.PayloadCase.REGISTERUSER) {
          return true;
        }

        logger.warn("[{}] onNext() - user {} is not registered.", serverReplicaId, userId);
        sendError(
            ERROR_NOT_REGISTERED,
            "User " + userId + " is not registered. Please register first.");
        return false;
      }

      /**
      handle the "registerUser" event.
      1. add user record to cosmos DB.
      2. make user "online" in connection registry.
      */
      private void handleRegisterUser(RegisterUser registerUser) {
        String userName = registerUser.getUserName();
        cosmosDBHandler.registerUser(userId, userName);

        // when user first registered, move user to "online" in connection registry.
        if (!isRegistered) {
          isRegistered = true;
          // Transition from temporary stream session to tracked registry session.
          session = connectionRegistry.handleUserOnline(userId, responseObserver);
          logger.info("[{}] registered and activated session for userName={}", serverReplicaId, userName);
        }
      }

      /**
       * Handler for SendMessage event.
       * 
       * Handle the event when cllient wants to send a message to another user.
       * 1. parse message
       * 2. check local connection registry to find the target user session. if found, send message.
       * 3. check redis global connection registry, if found use redis stream to relay message to target instance.
       * 4. if not found, user is offline
       * @param sendMessage
       * 
       */
      private void handleSendMessage(SendMessage sendMessage) {
        String toUserId = sendMessage.getToUserId();
        if (toUserId == null || toUserId.isBlank()) {
          logger.warn("[{}] handleSendMessage() - toUserId is null or blank", serverReplicaId);
          sendError(ERROR_BAD_REQUEST, "toUserId is required");
          return;
        }

        ChatMessage message =
            ChatMessage.newBuilder()
                .setFromUserId(userId)
                .setText(sendMessage.getText())
                .setServerMsgId(UUID.randomUUID().toString())
                .setTs(Instant.now().toEpochMilli())
                .build();
        
        // TODO: Add cosmos DB write logic here. always write to cosmos first.

        // 1. Try local delivery
        UserSession localUserSession = connectionRegistry.getSession(toUserId);
        if (localUserSession != null) {
          logger.info("[{}] handleSendMessage() - HIT local user: userName={}", serverReplicaId, cosmosDBHandler.getUserName(toUserId));
          localUserSession.send(ServerEvent.newBuilder().setChatMessage(message).build());
          return;
        }

        // 2. Try routing via Redis
        String routingInfo = connectionRegistry.getRoutingInfo(toUserId);
        if (routingInfo != null) {
          String[] parts = routingInfo.split(":", 2);
          if (parts.length == 2) {
            String targetInstanceId = parts[0];
            String targetSessionId = parts[1];
            logger.info("[{}] handleSendMessage() - RELAY message to user {} in target instance (targetInstanceId={}, targetSessionId={})", serverReplicaId, cosmosDBHandler.getUserName(toUserId), targetInstanceId, targetSessionId);
            connectionRegistry.ReplayMessageToNode(targetInstanceId, toUserId, targetSessionId, message);
            return;
          }
        }

        // TODO: should be silent, it's not error.
        logger.info("[{}] handleSendMessage() - target user userName={} is offline", serverReplicaId, cosmosDBHandler.getUserName(toUserId));
        // sendError(ERROR_USER_OFFLINE, "User " + toUserId + " is offline");
      }

      private void handleHeartbeatPing(HeartbeatPing heartbeat) {
        logger.debug("[{}] handleHeartbeatPing() - received heartbeat Ping from session: {}", serverReplicaId, session.getSessionId());
        connectionRegistry.updateHeartbeat(userId);

        HeartbeatPong pong = HeartbeatPong.newBuilder().setTs(Instant.now().toEpochMilli()).build();
        session.send(ServerEvent.newBuilder().setHeartbeatPong(pong).build());
      }

      private void sendError(String code, String reason) {
        ServerError error = ServerError.newBuilder().setCode(code).setReason(reason).build();
        session.send(ServerEvent.newBuilder().setServerError(error).build());
      }

      private void cleanup() {
        if (isRegistered) {
          connectionRegistry.handleUserOffline(userId, responseObserver);
          logger.info("[{}] unregistered userId={}", serverReplicaId, userId);
        }
      }
    };
  }

  private void logConnectionState(String userId, boolean userExists) {
    if (userExists) {
      logger.info("[{}] connected user: {}, userId={}", serverReplicaId, cosmosDBHandler.getUserName(userId), userId);
    } else {
      logger.info("[{}] new connection from un-registered userId={}", serverReplicaId, userId);
    }
  }
}
