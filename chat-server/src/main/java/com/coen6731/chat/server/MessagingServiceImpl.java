package com.coen6731.chat.server;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.HeartbeatPing;
import com.coen6731.chat.HeartbeatPong;
import com.coen6731.chat.InboundMessage;
import com.coen6731.chat.LoginUser;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.OutboundMessage;
import com.coen6731.chat.RegisterUser;
import com.coen6731.chat.SendMessageAck;
import com.coen6731.chat.SendStatus;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.ServerEvent;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * Responsibility: gRPC stream handler for auth, heartbeat, and chat send
 * workflows.
 * Input: streaming ClientEvent messages.
 * Output: streaming ServerEvent messages.
 */
@Component
public class MessagingServiceImpl extends MessagingServiceGrpc.MessagingServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(MessagingServiceImpl.class);
  private static final String ERROR_BAD_REQUEST = "BAD_REQUEST";
  private static final String ERROR_AUTH_NOT_AUTHENTICATED = "AUTH_NOT_AUTHENTICATED";
  private static final String ERROR_AUTH_INVALID_CREDENTIALS = "AUTH_INVALID_CREDENTIALS";
  private static final String ERROR_AUTH_EMAIL_ALREADY_EXISTS = "AUTH_EMAIL_ALREADY_EXISTS";
  private static final String ERROR_INTERNAL = "INTERNAL";
  private static final String ERROR_RECIPIENT_NOT_FOUND = "RECIPIENT_NOT_FOUND";
  private static final String ERROR_CONVERSATION_INVALID = "CONVERSATION_INVALID";
  private static final String ERROR_PERSISTENCE_FAILED = "PERSISTENCE_FAILED";
  private static final int MAX_TEXT_LENGTH = 4096;

  private final ConnectionRegistry connectionRegistry;
  private final CosmosDBHandler cosmosDBHandler;
  private final PasswordEncoder passwordEncoder;

  @org.springframework.beans.factory.annotation.Value("${container.app.replica.name}")
  private String serverReplicaId;

  /**
   * Responsibility: create service dependencies.
   * Input: connection registry and cosmos repository.
   * Output: initialized service instance.
   */
  public MessagingServiceImpl(ConnectionRegistry registry, CosmosDBHandler cosmosDBHandler) {
    this.connectionRegistry = registry;
    this.cosmosDBHandler = cosmosDBHandler;
    this.passwordEncoder = new BCryptPasswordEncoder();
  }

  /**
   * Responsibility: open one bidirectional stream for one client connection.
   * Input: stream observer bound to the caller connection.
   * Output: client event observer for receiving incoming events.
   */
  @Override
  public StreamObserver<ClientEvent> chat(StreamObserver<ServerEvent> responseObserver) {
    final String headerUserId = UserIdInterceptor.USER_ID_CTX_KEY.get();
    final Optional<CosmosDBHandler.UserRecord> userRecordFromHeader = (headerUserId == null || headerUserId.isBlank())
        ? Optional.empty()
        : cosmosDBHandler.findUserByUserId(headerUserId);

    final UserSession initialSession;
    if (userRecordFromHeader.isPresent()) {
      initialSession = connectionRegistry.handleUserOnline(userRecordFromHeader.get().userId(), responseObserver);
      logger.info(
          "[{}] stream started as authenticated userId={}, email={}",
          serverReplicaId,
          userRecordFromHeader.get().userId(),
          userRecordFromHeader.get().email());
    } else {
      initialSession = new UserSession(responseObserver);
      logger.info("[{}] stream started in UNAUTHENTICATED state", serverReplicaId);
    }

    return new StreamObserver<>() {
      private boolean isAuthenticated = userRecordFromHeader.isPresent();
      private String effectiveUserId = userRecordFromHeader.map(CosmosDBHandler.UserRecord::userId).orElse(null);
      private String effectiveEmail = userRecordFromHeader.map(CosmosDBHandler.UserRecord::email).orElse(null);
      private UserSession session = initialSession;

      /**
       * Responsibility: dispatch one incoming client event to its handler.
       * Input: parsed ClientEvent payload.
       * Output: corresponding ServerEvent side-effects.
       */
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
          case OUTBOUNDMESSAGE:
            // client -> server: send message
            handleSendMessage(event.getOutboundMessage());
            break;
          case PAYLOAD_NOT_SET:
          default:
            sendError(ERROR_BAD_REQUEST, "Empty or unsupported client event");
            break;
        }
      }

      /**
       * Responsibility: react to stream-level failure from grpc runtime/client
       * disconnect.
       * Input: throwable from grpc stream.
       * Output: cleanup of connection state.
       */
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

      /**
       * Responsibility: finalize stream shutdown initiated by client completion.
       * Input: completion callback without payload.
       * Output: state cleanup and stream close.
       */
      @Override
      public void onCompleted() {
        logger.info("[{}] onCompleted() - stream completed", serverReplicaId);
        cleanup();
        session.close();
      }

      /**
       * Responsibility: check whether an event is heartbeat ping.
       * Input: incoming event.
       * Output: true for heartbeat payload, false otherwise.
       */
      private boolean isHeartbeat(ClientEvent event) {
        return event.getPayloadCase() == ClientEvent.PayloadCase.HEARTBEATPING;
      }

      /**
       * Responsibility: authenticate existing user credentials.
       * Input: login email/password.
       * Output: auth success event or auth error.
       */
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

      /**
       * Responsibility: create and authenticate a new user account.
       * Input: registration email/password.
       * Output: auth success event or creation failure.
       */
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

      /**
       * Responsibility: switch stream state to authenticated and attach the user
       * session.
       * Input: authenticated user id/email.
       * Output: auth success event and active session registration.
       */
      private void activateAuthenticatedSession(String userId, String email) {
        isAuthenticated = true;
        effectiveUserId = userId;
        effectiveEmail = email;
        session = connectionRegistry.handleUserOnline(userId, responseObserver);

        AuthSuccess authSuccess = AuthSuccess.newBuilder().setUserId(userId).setEmail(email == null ? "" : email)
            .build();
        session.send(ServerEvent.newBuilder().setAuthSuccess(authSuccess).build());
        logger.info("[{}] authenticated userId={}, email={}", serverReplicaId, userId, email);
      }

      /**
       * Responsibility: handle outbound message with persistence-first and idempotent
       * semantics.
       * Input: outbound message from sender stream.
       * Output: send ack to sender and optional live inbound delivery to recipient.
       */
      private void handleSendMessage(OutboundMessage outboundMessage) {
        String clientMsgId = safeTrim(outboundMessage.getClientMsgId());
        if (!validateSender(clientMsgId)) {
          return;
        }

        String toEmail = normalizeEmail(outboundMessage.getToEmail());
        String text = outboundMessage.getText() == null ? "" : outboundMessage.getText().trim();
        if (!validateMessageContent(clientMsgId, toEmail, text)) {
          return;
        }

        Optional<CosmosDBHandler.UserRecord> recipientUserRec = cosmosDBHandler.findUserByEmail(toEmail);
        if (recipientUserRec.isEmpty()) {
          sendMessageAck(
              buildFailedAck(clientMsgId, "", "", ERROR_RECIPIENT_NOT_FOUND, "recipient email not found"));
          return;
        }
        String recipientUserId = recipientUserRec.get().userId();

        Optional<CosmosDBHandler.ConversationRecord> conversationOpt = resolveConversationId(outboundMessage.getConversationId());

        if (conversationOpt.isEmpty()) {
          sendMessageAck(
              buildFailedAck(
                  clientMsgId,
                  "",
                  safeTrim(outboundMessage.getConversationId()),
                  ERROR_CONVERSATION_INVALID,
                  "failed to resolve conversation"));
          return;
        }
        CosmosDBHandler.ConversationRecord conversation = conversationOpt.get();

        // generate new serverMsgId for this outbound message.
        String serverMsgId = deriveServerMsgId(effectiveUserId, clientMsgId);
        CosmosDBHandler.MessageRecord persistedMessage = persistMessage(clientMsgId, serverMsgId,
            conversation.conversationId(), recipientUserId, text);

        if (persistedMessage == null) {
          return;
        }

        // cosmosDBHandler.touchConversation(conversation.conversationId(),
        // persistedMessage.sentAtMs());

        sendMessageAck(
            SendMessageAck.newBuilder()
                .setClientMsgId(clientMsgId)
                .setServerMsgId(persistedMessage.serverMsgId())
                .setConversationId(persistedMessage.conversationId())
                .setStatus(SendStatus.PERSISTED_PENDING_DELIVERY)
                .setAckTs(persistedMessage.sentAtMs())
                .build());

        InboundMessage inboundMessage = InboundMessage.newBuilder()
            .setServerMsgId(persistedMessage.serverMsgId())
            .setClientMsgId(persistedMessage.clientMsgId())
            .setConversationId(persistedMessage.conversationId())
            .setFromUserId(persistedMessage.senderUserId())
            .setFromEmail(effectiveEmail == null ? "" : effectiveEmail)
            .setToUserId(persistedMessage.recipientUserId())
            .setText(persistedMessage.text())
            .setSentAtMs(persistedMessage.sentAtMs())
            .build();

        deliverLiveMessage(recipientUserId, toEmail, inboundMessage);
      }

      private boolean validateSender(String clientMsgId) {
        if (!isAuthenticated || effectiveUserId == null || effectiveUserId.isBlank()) {
          sendMessageAck(buildFailedAck(clientMsgId, "", "", ERROR_AUTH_NOT_AUTHENTICATED, "Authenticate first"));
          return false;
        }
        return true;
      }

      private boolean validateMessageContent(String clientMsgId, String toEmail, String text) {
        if (toEmail.isBlank()) {
          sendMessageAck(buildFailedAck(clientMsgId, "", "", ERROR_BAD_REQUEST, "toEmail is required"));
          return false;
        }
        if (clientMsgId.isBlank()) {
          sendMessageAck(buildFailedAck("", "", "", ERROR_BAD_REQUEST, "clientMsgId is required"));
          return false;
        }
        if (text.isBlank()) {
          sendMessageAck(buildFailedAck(clientMsgId, "", "", ERROR_BAD_REQUEST, "text is required"));
          return false;
        }
        if (text.length() > MAX_TEXT_LENGTH) {
          sendMessageAck(
              buildFailedAck(
                  clientMsgId,
                  "",
                  "",
                  ERROR_BAD_REQUEST,
                  "text exceeds max length " + MAX_TEXT_LENGTH));
          return false;
        }
        return true;
      }

      private CosmosDBHandler.MessageRecord persistMessage(String clientMsgId, String serverMsgId,
          String conversationId, String recipientUserId, String text) {
        long nowMs = Instant.now().toEpochMilli();

        CosmosDBHandler.MessageRecord NewMsgCandidate = new CosmosDBHandler.MessageRecord(
            serverMsgId,
            clientMsgId,
            conversationId,
            effectiveUserId,
            recipientUserId,
            text,
            nowMs,
            SendStatus.PERSISTED_PENDING_DELIVERY.name(),
            nowMs,
            nowMs);

        CosmosDBHandler.PersistResult persistResult = cosmosDBHandler.createMessageIfAbsent(NewMsgCandidate);
        if (persistResult.status() == CosmosDBHandler.PersistStatus.CREATED) {
          return persistResult.messageRecord();
        } else if (persistResult.status() == CosmosDBHandler.PersistStatus.ALREADY_EXISTS) {
          Optional<CosmosDBHandler.MessageRecord> existing = cosmosDBHandler.findMessageByServerMsgId(serverMsgId);
          if (existing.isEmpty()) {
            sendMessageAck(
                buildFailedAck(
                    clientMsgId,
                    serverMsgId,
                    conversationId,
                    ERROR_PERSISTENCE_FAILED,
                    "message conflict without existing record"));
            return null;
          }
          return existing.get();
        } else {
          sendMessageAck(
              buildFailedAck(
                  clientMsgId,
                  serverMsgId,
                  conversationId,
                  ERROR_PERSISTENCE_FAILED,
                  Optional.ofNullable(persistResult.errorReason()).orElse("persist failed")));
          return null;
        }
      }

      /**
       * Responsibility: resolve conversation by explicit id or create a new one.
       * Input: optional conversationId and sender/recipient user ids.
       * Output: existing or newly created conversation record.
       */
      private Optional<CosmosDBHandler.ConversationRecord> resolveConversationId(String requestedConversationId) {
        String normalizedReqConversationId = safeTrim(requestedConversationId);
        return cosmosDBHandler.createConversationIfAbsent(
            normalizedReqConversationId, Instant.now().toEpochMilli());
      }

      /**
       * Responsibility: derive deterministic canonical server id for idempotent retry
       * handling.
       * Input: sender user id and client message id.
       * Output: stable UUID string for this logical message.
       */
      private String deriveServerMsgId(String senderUserId, String clientMsgId) {
        String dedupeKey = senderUserId + "::" + clientMsgId;
        return UUID.nameUUIDFromBytes(dedupeKey.getBytes(StandardCharsets.UTF_8)).toString();
      }

      /**
       * Responsibility: route live message to local or remote recipient session when
       * online.
       * Input: recipient id/email and canonical inbound payload.
       * Output: best-effort delivery side-effect (no sender ack downgrade on
       * failure).
       */
      private void deliverLiveMessage(String toUserId, String toEmail, InboundMessage message) {
        UserSession localUserSession = connectionRegistry.getSession(toUserId);
        if (localUserSession != null) {
          localUserSession.send(ServerEvent.newBuilder().setInboundMessage(message).build());
          logger.info(
              "[{}] live-delivered local sender={} recipient={} serverMsgId={} conversationId={}",
              serverReplicaId,
              message.getFromUserId(),
              toUserId,
              message.getServerMsgId(),
              message.getConversationId());
          return;
        }

        String routingInfo = connectionRegistry.getRoutingInfo(toUserId);
        if (routingInfo != null) {
          String[] parts = routingInfo.split(":", 2);
          if (parts.length == 2) {
            String targetInstanceId = parts[0];
            String targetSessionId = parts[1];
            connectionRegistry.ReplayMessageToNode(targetInstanceId, toUserId, targetSessionId, message);
            logger.info(
                "[{}] live-relayed sender={} recipientEmail={} targetInstance={} serverMsgId={} conversationId={}",
                serverReplicaId,
                message.getFromUserId(),
                toEmail,
                targetInstanceId,
                message.getServerMsgId(),
                message.getConversationId());
            return;
          }
        }

        logger.info(
            "[{}] recipient offline sender={} recipient={} serverMsgId={} conversationId={}",
            serverReplicaId,
            message.getFromUserId(),
            toUserId,
            message.getServerMsgId(),
            message.getConversationId());
      }

      /**
       * Responsibility: handle heartbeat ping and keep session/redis ttl alive.
       * Input: heartbeat ping event.
       * Output: heartbeat pong response.
       */
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

      /**
       * Responsibility: send legacy error event for non-send workflows.
       * Input: machine-readable code and reason.
       * Output: serverError event on stream.
       */
      private void sendError(String code, String reason) {
        ServerError error = ServerError.newBuilder().setCode(code).setReason(reason).build();
        session.send(ServerEvent.newBuilder().setServerError(error).build());
      }

      /**
       * Responsibility: push send ack event to sender stream.
       * Input: constructed send ack payload.
       * Output: one server event write.
       */
      private void sendMessageAck(SendMessageAck ack) {
        session.send(ServerEvent.newBuilder().setSendMessageAck(ack).build());
      }

      /**
       * Responsibility: build failure ack payload for validation/persistence errors.
       * Input: ids and error metadata.
       * Output: send ack with FAILED status.
       */
      private SendMessageAck buildFailedAck(
          String clientMsgId,
          String serverMsgId,
          String conversationId,
          String errorCode,
          String errorReason) {
        return SendMessageAck.newBuilder()
            .setClientMsgId(clientMsgId == null ? "" : clientMsgId)
            .setServerMsgId(serverMsgId == null ? "" : serverMsgId)
            .setConversationId(conversationId == null ? "" : conversationId)
            .setStatus(SendStatus.FAILED)
            .setErrorCode(errorCode == null ? ERROR_INTERNAL : errorCode)
            .setErrorReason(errorReason == null ? "unknown send error" : errorReason)
            .setAckTs(Instant.now().toEpochMilli())
            .build();
      }

      /**
       * Responsibility: remove stream state from registry when connection
       * closes/errors.
       * Input: none.
       * Output: user offline registration cleanup.
       */
      private void cleanup() {
        if (isAuthenticated && effectiveUserId != null) {
          connectionRegistry.handleUserOffline(effectiveUserId, responseObserver);
          logger.info("[{}] user offline userId={}, email={}", serverReplicaId, effectiveUserId, effectiveEmail);
        }
      }

      /**
       * Responsibility: normalize email value for case-insensitive comparisons.
       * Input: raw email string.
       * Output: trimmed lowercase email or empty string.
       */
      private String normalizeEmail(String email) {
        return email == null ? "" : email.trim().toLowerCase();
      }

      /**
       * Responsibility: trim nullable strings safely.
       * Input: nullable source string.
       * Output: trimmed string or empty string.
       */
      private String safeTrim(String value) {
        return value == null ? "" : value.trim();
      }
    };
  }
}
