package com.coen6731.chat.server;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.ChatMessage;
import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.HeartbeatPing;
import com.coen6731.chat.HeartbeatPong;
import com.coen6731.chat.LoginUser;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.RegisterUser;
import com.coen6731.chat.SendMessage;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.ServerEvent;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * Implementation of the MessagingService defined in the proto file.
 */
@Component
public class MessagingServiceImpl extends MessagingServiceGrpc.MessagingServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(MessagingServiceImpl.class);
  private static final String ERROR_BAD_REQUEST = "BAD_REQUEST";
  private static final String ERROR_AUTH_NOT_AUTHENTICATED = "AUTH_NOT_AUTHENTICATED";
  private static final String ERROR_AUTH_INVALID_CREDENTIALS = "AUTH_INVALID_CREDENTIALS";
  private static final String ERROR_AUTH_EMAIL_ALREADY_EXISTS = "AUTH_EMAIL_ALREADY_EXISTS";
  private static final String ERROR_INTERNAL = "INTERNAL";

  private final ConnectionRegistry connectionRegistry;
  private final CosmosDBHandler cosmosDBHandler;
  private final PasswordEncoder passwordEncoder;

  @org.springframework.beans.factory.annotation.Value("${container.app.replica.name}")
  private String serverReplicaId;

  public MessagingServiceImpl(ConnectionRegistry registry, CosmosDBHandler cosmosDBHandler) {
    this.connectionRegistry = registry;
    this.cosmosDBHandler = cosmosDBHandler;
    this.passwordEncoder = new BCryptPasswordEncoder();
  }

  @Override
  public StreamObserver<ClientEvent> chat(StreamObserver<ServerEvent> responseObserver) {
    final String headerUserId = UserIdInterceptor.USER_ID_CTX_KEY.get();
    final Optional<CosmosDBHandler.UserRecord> UserRecordFromHeader =
        (headerUserId == null || headerUserId.isBlank())
            ? Optional.empty()
            : cosmosDBHandler.findUserByUserId(headerUserId);

    final UserSession initialSession;
    if (UserRecordFromHeader.isPresent()) {
      initialSession = connectionRegistry.handleUserOnline(UserRecordFromHeader.get().userId(), responseObserver);
      logger.info(
          "[{}] stream started as authenticated userId={}, email={}",
          serverReplicaId,
          UserRecordFromHeader.get().userId(),
          UserRecordFromHeader.get().email());
    } else {
      initialSession = new UserSession(responseObserver);
      logger.info("[{}] stream started in UNAUTHENTICATED state", serverReplicaId);
    }

    return new StreamObserver<ClientEvent>() {
      private boolean isAuthenticated = UserRecordFromHeader.isPresent();
      private String effectiveUserId = UserRecordFromHeader.map(CosmosDBHandler.UserRecord::userId).orElse(null);
      private String effectiveEmail = UserRecordFromHeader.map(CosmosDBHandler.UserRecord::email).orElse(null);
      private UserSession session = initialSession;

      @Override
      public void onNext(ClientEvent event) {
        if (isHeartbeat(event)) {
          handleHeartbeatPing(event.getHeartbeatPing());
          return;
        }

        switch (event.getPayloadCase()) {
          case LOGINUSER:
            if (isAuthenticated) {
              sendError(ERROR_BAD_REQUEST, "Already authenticated");
              return;
            }
            handleLoginUser(event.getLoginUser());
            break;
          case REGISTERUSER:
            if (isAuthenticated) {
              sendError(ERROR_BAD_REQUEST, "Already authenticated");
              return;
            }
            handleRegisterUser(event.getRegisterUser());
            break;
          case SENDMESSAGE:
            if (!isAuthenticated) {
              sendError(ERROR_AUTH_NOT_AUTHENTICATED, "Authenticate with login/register first");
              return;
            }
            handleSendMessage(event.getSendMessage());
            break;
          case PAYLOAD_NOT_SET:
          default:
            sendError(ERROR_BAD_REQUEST, "Empty or unsupported client event");
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

      private void handleLoginUser(LoginUser loginUser) {
        String email = normalizeEmail(loginUser.getEmail());
        String password = loginUser.getPassword();
        if (email.isBlank() || password == null || password.isBlank()) {
          sendError(ERROR_BAD_REQUEST, "email and password are required");
          return;
        }

        Optional<CosmosDBHandler.UserRecord> found = cosmosDBHandler.findUserByEmail(email);
        if (found.isEmpty() || found.get().passwordHash() == null) {
          sendError(ERROR_AUTH_INVALID_CREDENTIALS, "Invalid email or password");
          return;
        }

        if (!passwordEncoder.matches(password, found.get().passwordHash())) {
          sendError(ERROR_AUTH_INVALID_CREDENTIALS, "Invalid email or password");
          return;
        }

        activateAuthenticatedSession(found.get().userId(), found.get().email());
      }

      private void handleRegisterUser(RegisterUser registerUser) {
        String email = normalizeEmail(registerUser.getEmail());
        String password = registerUser.getPassword();
        if (email.isBlank() || password == null || password.isBlank()) {
          sendError(ERROR_BAD_REQUEST, "email and password are required");
          return;
        }

        if (cosmosDBHandler.findUserByEmail(email).isPresent()) {
          sendError(ERROR_AUTH_EMAIL_ALREADY_EXISTS, "Email is already registered");
          return;
        }

        String passwordHash = passwordEncoder.encode(password);
        Optional<CosmosDBHandler.UserRecord> created = cosmosDBHandler.createUser(email, passwordHash);
        if (created.isEmpty()) {
          sendError(ERROR_INTERNAL, "Failed to create user");
          return;
        }

        activateAuthenticatedSession(created.get().userId(), created.get().email());
      }

      private void activateAuthenticatedSession(String userId, String email) {
        isAuthenticated = true;
        effectiveUserId = userId;
        effectiveEmail = email;
        session = connectionRegistry.handleUserOnline(userId, responseObserver);

        AuthSuccess authSuccess =
            AuthSuccess.newBuilder().setUserId(userId).setEmail(email == null ? "" : email).build();
        session.send(ServerEvent.newBuilder().setAuthSuccess(authSuccess).build());
        logger.info("[{}] authenticated userId={}, email={}", serverReplicaId, userId, email);
      }

      /**
       * Handler for SendMessage event.
       *
       * Handle the event when client wants to send a message to another user.
       * 1. parse message
       * 2. check local connection registry to find the target user session. if found, send message.
       * 3. check redis global connection registry, if found use redis stream to relay message to target instance.
       * 4. if not found, user is offline
       */
      private void handleSendMessage(SendMessage sendMessage) {
        String toEmail = sendMessage.getToEmail();
        if (toEmail == null || toEmail.isBlank()) {
          logger.warn("[{}] handleSendMessage() - toEmail is null or blank", serverReplicaId);
          sendError(ERROR_BAD_REQUEST, "toEmail is required");
          return;
        }

        Optional<CosmosDBHandler.UserRecord> toUserRecord = cosmosDBHandler.findUserByEmail(toEmail);
        if (toUserRecord.isEmpty()) {
          logger.warn("[{}] handleSendMessage() - toEmail={} not found in cosmos DB", serverReplicaId, toEmail);
          sendError(ERROR_BAD_REQUEST, "toEmail=" + toEmail + " not found");
          return;
        }
        String toUserId = toUserRecord.get().userId();

        ChatMessage message =
            ChatMessage.newBuilder()
                .setFromUserId(effectiveUserId)
                .setText(sendMessage.getText())
                .setFromEmail(effectiveEmail)
                .setServerMsgId(UUID.randomUUID().toString())
                .setTs(Instant.now().toEpochMilli())
                .build();

        // TODO: Add cosmos DB write logic here. always write to cosmos first.

        // 1. Try local delivery
        UserSession localUserSession = connectionRegistry.getSession(toUserId);
        if (localUserSession != null) {
          logger.info(
              "[{}] handleSendMessage() - HIT local user: email={}",
              serverReplicaId,
              toEmail);
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
            logger.info(
                "[{}] handleSendMessage() - RELAY message to user {} in target instance (targetInstanceId={}, targetSessionId={})",
                serverReplicaId,
                toEmail,
                targetInstanceId,
                targetSessionId);
            connectionRegistry.ReplayMessageToNode(targetInstanceId, toUserId, targetSessionId, message);
            return;
          }
        }

        logger.info(
            "[{}] handleSendMessage() - target user userName={} is offline",
            serverReplicaId,
            cosmosDBHandler.getUserName(toUserId));
      }

      private void handleHeartbeatPing(HeartbeatPing heartbeat) {
        logger.debug(
            "[{}] handleHeartbeatPing() - received heartbeat Ping from session: {}",
            serverReplicaId,
            session.getSessionId());

        if (isAuthenticated && effectiveUserId != null && !effectiveUserId.isBlank()) {
          connectionRegistry.updateHeartbeat(effectiveUserId);
        }

        HeartbeatPong pong = HeartbeatPong.newBuilder().setTs(Instant.now().toEpochMilli()).build();
        session.send(ServerEvent.newBuilder().setHeartbeatPong(pong).build());
      }

      private void sendError(String code, String reason) {
        ServerError error = ServerError.newBuilder().setCode(code).setReason(reason).build();
        session.send(ServerEvent.newBuilder().setServerError(error).build());
      }

      private void cleanup() {
        if (isAuthenticated && effectiveUserId != null) {
          connectionRegistry.handleUserOffline(effectiveUserId, responseObserver);
          logger.info("[{}] user offline userId={}, email={}", serverReplicaId, effectiveUserId, effectiveEmail);
        }
      }

      private String normalizeEmail(String email) {
        return email == null ? "" : email.trim().toLowerCase();
      }
    };
  }
}
