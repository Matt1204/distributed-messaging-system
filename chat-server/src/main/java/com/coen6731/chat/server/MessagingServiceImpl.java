package com.coen6731.chat.server;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.CanonicalMessage;
import com.coen6731.chat.CatchupConversationResult;
import com.coen6731.chat.CatchupRequest;
import com.coen6731.chat.CatchupResult;
import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.ConversationCursor;
import com.coen6731.chat.GetMsgHistoryRequest;
import com.coen6731.chat.HeartbeatPing;
import com.coen6731.chat.HeartbeatPong;
import com.coen6731.chat.InboundMessage;
import com.coen6731.chat.LoginUser;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.MsgHistoryResult;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * Responsibility: gRPC stream handler for auth, heartbeat, send, catchup, and
 * history workflows.
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
  private static final String ERROR_OVERLOADED = "OVERLOADED";
  private static final int MAX_TEXT_LENGTH = 4096;
  private static final int FALLBACK_CATCHUP_LIMIT = 50;
  private static final int MAX_PAGE_LIMIT = 200;

  private final ConnectionRegistry connectionRegistry;
  private final CosmosDBHandler cosmosDBHandler;
  private final RedisHandler redisHandler;
  private final SendAsyncExecutor sendAsyncExecutor;
  private final PasswordEncoder passwordEncoder;

  @org.springframework.beans.factory.annotation.Value("${container.app.replica.name}")
  private String serverReplicaId;

  /**
   * Responsibility: create service dependencies.
   * Input: connection registry, cosmos repository, and redis handler.
   * Output: initialized service instance.
   */
  public MessagingServiceImpl(
      ConnectionRegistry registry,
      CosmosDBHandler cosmosDBHandler,
      RedisHandler redisHandler,
      SendAsyncExecutor sendAsyncExecutor) {
    this.connectionRegistry = registry;
    this.cosmosDBHandler = cosmosDBHandler;
    this.redisHandler = redisHandler;
    this.sendAsyncExecutor = sendAsyncExecutor;
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
          case CATCHUPREQUEST:
            handleCatchupRequest(event.getCatchupRequest());
            break;
          case GETMSGHISTORYREQUEST:
            handleGetMsgHistoryRequest(event.getGetMsgHistoryRequest());
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
        String senderUserIdSnapshot = effectiveUserId;
        String senderEmailSnapshot = effectiveEmail == null ? "" : effectiveEmail;
        String requestedConversationId = safeTrim(outboundMessage.getConversationId());
        UserSession senderSessionSnapshot = session;
        long acceptedAtMs = System.currentTimeMillis();

        boolean accepted = sendAsyncExecutor.submit(
            senderUserIdSnapshot,
            clientMsgId,
            acceptedAtMs,
            () -> processSendMessageAsync(
                senderSessionSnapshot,
                senderUserIdSnapshot,
                senderEmailSnapshot,
                requestedConversationId,
                clientMsgId,
                toEmail,
                text,
                acceptedAtMs));
        if (!accepted) {
          sendMessageAckToSender(
              senderUserIdSnapshot,
              senderSessionSnapshot,
              buildFailedAck(clientMsgId, "", requestedConversationId, 0L, ERROR_OVERLOADED, "send queue is full"));
        }
      }

      /**
       * Responsibility: message processing workflow to be executed asynchronously.
       * Input: sender session, sender user id, sender email, requested conversation id, client message id, to email, text, accepted at milliseconds.
       * Output: none.
       */
      private void processSendMessageAsync(
          UserSession senderSession,
          String senderUserId,
          String senderEmail,
          String requestedConversationId,
          String clientMsgId,
          String toEmail,
          String text,
          long acceptedAtMs) {
        long workerStartedAtMs = System.currentTimeMillis();
        String outcome = "FAILED";
        String resolvedConversationId = safeTrim(requestedConversationId);
        String resolvedRecipientUserId = "";
        String resolvedServerMsgId = "";
        long resolvedSequenceId = 0L;
        try {
          Optional<CosmosDBHandler.UserRecord> recipientUserRec = cosmosDBHandler.findUserByEmail(toEmail);
          if (recipientUserRec.isEmpty()) {
            sendMessageAckToSender(
                senderUserId,
                senderSession,
                buildFailedAck(clientMsgId, "", "", 0L, ERROR_RECIPIENT_NOT_FOUND, "recipient email not found"));
            return;
          }
          String recipientUserId = recipientUserRec.get().userId();
          resolvedRecipientUserId = recipientUserId;

          Optional<CosmosDBHandler.ConversationRecord> conversationOpt = resolveConversationId(
              requestedConversationId,
              senderUserId,
              recipientUserId);
          if (conversationOpt.isEmpty()) {
            sendMessageAckToSender(
                senderUserId,
                senderSession,
                buildFailedAck(
                    clientMsgId,
                    "",
                    requestedConversationId,
                    0L,
                    ERROR_CONVERSATION_INVALID,
                    "failed to resolve conversation"));
            return;
          }
          CosmosDBHandler.ConversationRecord conversation = conversationOpt.get();
          resolvedConversationId = conversation.conversationId();

          long sequenceId = allocateNextSequenceId(conversation.conversationId());
          resolvedSequenceId = sequenceId;
          if (sequenceId <= 0) {
            sendMessageAckToSender(
                senderUserId,
                senderSession,
                buildFailedAck(
                    clientMsgId,
                    "",
                    conversation.conversationId(),
                    0L,
                    ERROR_INTERNAL,
                    "failed to allocate sequence id"));
            return;
          }

          String serverMsgId = deriveServerMsgId(senderUserId, clientMsgId);
          resolvedServerMsgId = serverMsgId;
          CosmosDBHandler.MessageRecord persistedMessage = persistMessage(
              senderSession,
              senderUserId,
              clientMsgId,
              serverMsgId,
              conversation.conversationId(),
              sequenceId,
              recipientUserId,
              text);
          if (persistedMessage == null) {
            return;
          }

          safeTouchConversation(conversation.conversationId(), persistedMessage.sentAtMs());

          sendMessageAckToSender(
              senderUserId,
              senderSession,
              SendMessageAck.newBuilder()
                  .setClientMsgId(clientMsgId)
                  .setServerMsgId(persistedMessage.serverMsgId())
                  .setConversationId(persistedMessage.conversationId())
                  .setStatus(SendStatus.PERSISTED_PENDING_DELIVERY)
                  .setAckTs(persistedMessage.sentAtMs())
                  .setSequenceId(persistedMessage.sequenceId())
                  .build());

          InboundMessage inboundMessage = InboundMessage.newBuilder()
              .setServerMsgId(persistedMessage.serverMsgId())
              .setClientMsgId(persistedMessage.clientMsgId())
              .setConversationId(persistedMessage.conversationId())
              .setFromUserId(persistedMessage.senderUserId())
              .setFromEmail(senderEmail)
              .setToUserId(persistedMessage.recipientUserId())
              .setText(persistedMessage.text())
              .setSentAtMs(persistedMessage.sentAtMs())
              .setSequenceId(persistedMessage.sequenceId())
              .build();

          deliverLiveMessage(recipientUserId, inboundMessage);
          outcome = "PERSISTED";
        } catch (Exception e) {
          logger.warn("[{}] send pipeline exception sender={} clientMsgId={}", serverReplicaId, senderUserId, clientMsgId, e);
          sendMessageAckToSender(
              senderUserId,
              senderSession,
              buildFailedAck(clientMsgId, "", requestedConversationId, 0L, ERROR_INTERNAL, "send pipeline error"));
        } finally {
          long finishedAtMs = System.currentTimeMillis();
          long queueWaitMs = Math.max(0L, workerStartedAtMs - acceptedAtMs);
          long workerExecMs = Math.max(0L, finishedAtMs - workerStartedAtMs);
          long totalMs = Math.max(0L, finishedAtMs - acceptedAtMs);
          SendAsyncExecutor.ExecutorSnapshot executorSnapshot = sendAsyncExecutor.snapshot();
          String workerThread = Thread.currentThread().getName();
          logger.info(
              "[{}] send_latency outcome={} clientMsgId={} "
                  + "sequenceId={} queueWaitMs={} workerExecMs={} totalMs={} "
                  + "workerThread={} workerThreads={} activeWorkers={} queueDepth={} submitted={} completed={} rejected={}",
              serverReplicaId,
              outcome,
              clientMsgId,
              resolvedSequenceId,
              queueWaitMs,
              workerExecMs,
              totalMs,
              workerThread,
              executorSnapshot.workerThreads(),
              executorSnapshot.activeWorkers(),
              executorSnapshot.queueDepth(),
              executorSnapshot.submitted(),
              executorSnapshot.completed(),
              executorSnapshot.rejected());
        }
      }

      /**
       * Responsibility: reconcile missed conversation messages for authenticated
       * user.
       * Input: catchup cursor hints and per-conversation page limit.
       * Output: one catchup result event with per-conversation message batches.
       */
      private void handleCatchupRequest(CatchupRequest request) {
        if (!validateAuthenticatedForGeneralRequest()) {
          return;
        }

        int limit = normalizeLimit(request.getPerConversationLimit(), FALLBACK_CATCHUP_LIMIT);
        // Client-provided catchup hints: {conversationId -> clientLastReceivedSequenceId}.
        Map<String, Long> clientConversationSeqMap = new HashMap<>();
        for (ConversationCursor cursor : request.getCursorHintsList()) {
          String conversationId = safeTrim(cursor.getConversationId());
          if (!conversationId.isBlank()) {
            clientConversationSeqMap.put(conversationId, Math.max(0L, cursor.getClientLastReceivedSequenceId()));
          }
        }

        Map<String, String> emailCache = new HashMap<>();
        CatchupResult.Builder resultBuilder = CatchupResult.newBuilder();

        // Catch up all conversations the user can access.
        List<CosmosDBHandler.ConversationRecord> authorizedConversations = cosmosDBHandler
            .findConversationsByMember(effectiveUserId);
        authorizedConversations.sort(Comparator.comparingLong(CosmosDBHandler.ConversationRecord::lastMessageAtMs)
            .reversed());

        for (CosmosDBHandler.ConversationRecord conversation : authorizedConversations) {
          String conversationId = conversation.conversationId();
          // Client state for this conversation.
          long clientConversationSeq = Math.max(0L, clientConversationSeqMap.getOrDefault(conversationId, 0L));
          // Durable latest message sequence id on the server (must map to a real stored message).
          long serverConversationSeq = cosmosDBHandler.findMaxSequenceId(conversationId);

          CatchupConversationResult.Builder convResult = CatchupConversationResult.newBuilder()
              .setConversationId(conversationId)
              .setConversationLatestSequenceId(serverConversationSeq);

          // Newest-first catchup: only return this conversation's latest missing window.
          if (clientConversationSeq < serverConversationSeq && limit > 0) {
            List<CosmosDBHandler.MessageRecord> newestMissing = cosmosDBHandler.listNewestMessagesAfterSequence(
                conversationId,
                clientConversationSeq,
                limit);
            for (CosmosDBHandler.MessageRecord record : newestMissing) {
              convResult.addMessages(toCanonicalMessage(record, emailCache));
            }
          }

          resultBuilder.addConversationResults(convResult.build());
        }

        resultBuilder.setGeneratedAtMs(Instant.now().toEpochMilli());
        session.send(ServerEvent.newBuilder().setCatchupResult(resultBuilder.build()).build());
      }

      /**
       * Responsibility: return older conversation history page for one authorized
       * conversation.
       * Input: conversation id, before-sequence cursor, and requested page size.
       * Output: one history result event with canonical messages.
       */
      private void handleGetMsgHistoryRequest(GetMsgHistoryRequest request) {
        if (!validateAuthenticatedForGeneralRequest()) {
          return;
        }

        String conversationId = safeTrim(request.getConversationId());
        if (conversationId.isBlank()) {
          sendError(ERROR_BAD_REQUEST, "conversationId is required");
          return;
        }

        Optional<CosmosDBHandler.ConversationRecord> conversationOpt = cosmosDBHandler
            .findConversationById(conversationId);
        if (conversationOpt.isEmpty() || !conversationOpt.get().memberUserIds().contains(effectiveUserId)) {
          sendError(ERROR_CONVERSATION_INVALID, "conversation is not accessible");
          return;
        }

        long beforeSequenceId = request.getBeforeSequenceId();
        if (beforeSequenceId <= 0L) {
          sendError(ERROR_BAD_REQUEST, "beforeSequenceId must be > 0");
          return;
        }

        int retriveMsgQuantity = request.getRetriveMsgQuantity();
        if (retriveMsgQuantity <= 0) {
          sendError(ERROR_BAD_REQUEST, "retriveMsgQuantity is required and must be > 0");
          return;
        }
        int boundedQuantity = Math.min(retriveMsgQuantity, MAX_PAGE_LIMIT);
        List<CosmosDBHandler.MessageRecord> history = cosmosDBHandler.listMessagesFromSequenceDescending(
            conversationId,
            beforeSequenceId,
            boundedQuantity);

        Map<String, String> emailCache = new HashMap<>();
        MsgHistoryResult.Builder result = MsgHistoryResult.newBuilder().setConversationId(conversationId);
        for (CosmosDBHandler.MessageRecord record : history) {
          result.addMessages(toCanonicalMessage(record, emailCache));
        }

        session.send(ServerEvent.newBuilder().setMsgHistoryResult(result.build()).build());
      }

      /**
       * Responsibility: validate sender identity exists for send workflow.
       * Input: outbound client message id.
       * Output: true when authenticated and allowed to send.
       */
      private boolean validateSender(String clientMsgId) {
        if (!isAuthenticated || effectiveUserId == null || effectiveUserId.isBlank()) {
          sendMessageAck(buildFailedAck(clientMsgId, "", "", 0L, ERROR_AUTH_NOT_AUTHENTICATED, "Authenticate first"));
          return false;
        }
        return true;
      }

      /**
       * Responsibility: validate stream authentication for non-send APIs.
       * Input: none.
       * Output: true when authenticated, false after error event.
       */
      private boolean validateAuthenticatedForGeneralRequest() {
        if (!isAuthenticated || effectiveUserId == null || effectiveUserId.isBlank()) {
          sendError(ERROR_AUTH_NOT_AUTHENTICATED, "Authenticate first");
          return false;
        }
        return true;
      }

      /**
       * Responsibility: validate recipient/text/client ids before persistence.
       * Input: outbound fields from sender request.
       * Output: true when input is valid.
       */
      private boolean validateMessageContent(String clientMsgId, String toEmail, String text) {
        if (toEmail.isBlank()) {
          sendMessageAck(buildFailedAck(clientMsgId, "", "", 0L, ERROR_BAD_REQUEST, "toEmail is required"));
          return false;
        }
        if (clientMsgId.isBlank()) {
          sendMessageAck(buildFailedAck("", "", "", 0L, ERROR_BAD_REQUEST, "clientMsgId is required"));
          return false;
        }
        if (text.isBlank()) {
          sendMessageAck(buildFailedAck(clientMsgId, "", "", 0L, ERROR_BAD_REQUEST, "text is required"));
          return false;
        }
        if (text.length() > MAX_TEXT_LENGTH) {
          sendMessageAck(
              buildFailedAck(
                  clientMsgId,
                  "",
                  "",
                  0L,
                  ERROR_BAD_REQUEST,
                  "text exceeds max length " + MAX_TEXT_LENGTH));
          return false;
        }
        return true;
      }

      /**
       * Responsibility: persist canonical message and reconcile idempotent duplicate
       * writes.
       * Input: canonical ids, routing ids, sequence id, and content.
       * Output: persisted message record, or null after failed ack.
       */
      private CosmosDBHandler.MessageRecord persistMessage(
          UserSession senderSession,
          String senderUserId,
          String clientMsgId,
          String serverMsgId,
          String conversationId,
          long sequenceId,
          String recipientUserId,
          String text) {
        long nowMs = Instant.now().toEpochMilli();

        CosmosDBHandler.MessageRecord newMsgCandidate = new CosmosDBHandler.MessageRecord(
            serverMsgId,
            clientMsgId,
            conversationId,
            sequenceId,
            senderUserId,
            recipientUserId,
            text,
            nowMs,
            SendStatus.PERSISTED_PENDING_DELIVERY.name(),
            nowMs,
            nowMs);

        CosmosDBHandler.PersistResult persistResult = cosmosDBHandler.createMessageIfAbsent(newMsgCandidate);
        if (persistResult.status() == CosmosDBHandler.PersistStatus.CREATED) {
          return persistResult.messageRecord();
        } else if (persistResult.status() == CosmosDBHandler.PersistStatus.ALREADY_EXISTS) {
          Optional<CosmosDBHandler.MessageRecord> existing = cosmosDBHandler.findMessageByServerMsgId(serverMsgId);
          if (existing.isEmpty()) {
            sendMessageAckToSender(
                senderUserId,
                senderSession,
                buildFailedAck(
                    clientMsgId,
                    serverMsgId,
                    conversationId,
                    sequenceId,
                    ERROR_PERSISTENCE_FAILED,
                    "message conflict without existing record"));
            return null;
          }
          return existing.get();
        } else {
          sendMessageAckToSender(
              senderUserId,
              senderSession,
              buildFailedAck(
                  clientMsgId,
                  serverMsgId,
                  conversationId,
                  sequenceId,
                  ERROR_PERSISTENCE_FAILED,
                  Optional.ofNullable(persistResult.errorReason()).orElse("persist failed")));
          return null;
        }
      }

      /**
       * Responsibility: resolve conversation by explicit id or create a member-scoped
       * one.
       * Input: optional conversation id and recipient user id.
       * Output: existing or newly created conversation record.
       */
      private Optional<CosmosDBHandler.ConversationRecord> resolveConversationId(
          String requestedConversationId,
          String senderUserId,
          String recipientUserId) {
        String normalizedReqConversationId = safeTrim(requestedConversationId);
        return cosmosDBHandler.createConversationIfAbsent(
            normalizedReqConversationId,
            senderUserId,
            recipientUserId,
            Instant.now().toEpochMilli());
      }

      /**
       * Responsibility: allocate next monotonic sequence id for one conversation.
       * Input: conversation id.
       * Output: newly allocated sequence id.
       */
      private long allocateNextSequenceId(String conversationId) {
        Long redisCursor = redisHandler.getConversationLatestSequenceId(conversationId);
        // If redis 
        if (redisCursor == null) {
          long durableMax = cosmosDBHandler.findMaxSequenceId(conversationId);
          redisHandler.initializeConversationSequenceIfAbsent(conversationId, durableMax);
        }
        return redisHandler.incrementConversationSequence(conversationId);
      }

      /**
       * Responsibility: map durable message record to shared canonical payload.
       * Input: message record plus sender-email cache map.
       * Output: canonical message sent by catchup/history APIs.
       */
      private CanonicalMessage toCanonicalMessage(
          CosmosDBHandler.MessageRecord record,
          Map<String, String> emailCache) {
        String fromEmail = emailCache.computeIfAbsent(
            record.senderUserId(),
            userId -> {
              Optional<CosmosDBHandler.UserRecord> user = cosmosDBHandler.findUserByUserId(userId);
              return user.map(CosmosDBHandler.UserRecord::email).orElse("");
            });

        return CanonicalMessage.newBuilder()
            .setServerMsgId(safeTrim(record.serverMsgId()))
            .setClientMsgId(safeTrim(record.clientMsgId()))
            .setConversationId(safeTrim(record.conversationId()))
            .setSequenceId(record.sequenceId())
            .setFromUserId(safeTrim(record.senderUserId()))
            .setFromEmail(fromEmail)
            .setToUserId(safeTrim(record.recipientUserId()))
            .setText(safeTrim(record.text()))
            .setSentAtMs(record.sentAtMs())
            .build();
      }

      /**
       * Responsibility: normalize requested page limits to configured bounds.
       * Input: requested limit and default fallback.
       * Output: bounded positive page size.
       */
      private int normalizeLimit(int requestedLimit, int defaultValue) {
        if (requestedLimit <= 0) {
          return defaultValue;
        }
        return Math.min(requestedLimit, MAX_PAGE_LIMIT);
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

      private void safeTouchConversation(String conversationId, long lastMessageAtMs) {
        CosmosDBHandler.TouchResult touchResult = cosmosDBHandler.touchConversationFast(conversationId, lastMessageAtMs);
        if (touchResult.success()) {
          return;
        }
        if (touchResult.statusCode() == 404 && cosmosDBHandler.touchConversation(conversationId, lastMessageAtMs)) {
          return;
        }
        logger.debug(
            "[{}] touch conversation skipped conversationId={} statusCode={}",
            serverReplicaId,
            conversationId,
            touchResult.statusCode());
      }

      private void deliverLiveMessage(String toUserId, InboundMessage message) {
        try {
          UserSession localUserSession = connectionRegistry.getSession(toUserId);
          if (localUserSession != null) {
            localUserSession.send(ServerEvent.newBuilder().setInboundMessage(message).build());
            return;
          }

          String routingInfo = connectionRegistry.getRoutingInfo(toUserId);
          if (routingInfo != null) {
            String[] parts = routingInfo.split(":", 2);
            if (parts.length == 2) {
              String targetInstanceId = parts[0];
              String targetSessionId = parts[1];
              connectionRegistry.ReplayMessageToNode(targetInstanceId, toUserId, targetSessionId, message);
            }
          }
        } catch (Exception e) {
          logger.debug(
              "[{}] live delivery skipped recipient={} serverMsgId={}",
              serverReplicaId,
              toUserId,
              message.getServerMsgId(),
              e);
        }
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

      private void sendMessageAckToSender(String senderUserId, UserSession fallbackSession, SendMessageAck ack) {
        UserSession activeSession = senderUserId == null ? null : connectionRegistry.getSession(senderUserId);
        UserSession resolvedSession = activeSession != null ? activeSession : (fallbackSession == null ? session : fallbackSession);
        resolvedSession.send(ServerEvent.newBuilder().setSendMessageAck(ack).build());
      }

      /**
       * Responsibility: build failure ack payload for validation/persistence errors.
       * Input: ids, optional sequence id, and error metadata.
       * Output: send ack with FAILED status.
       */
      private SendMessageAck buildFailedAck(
          String clientMsgId,
          String serverMsgId,
          String conversationId,
          long sequenceId,
          String errorCode,
          String errorReason) {
        return SendMessageAck.newBuilder()
            .setClientMsgId(clientMsgId == null ? "" : clientMsgId)
            .setServerMsgId(serverMsgId == null ? "" : serverMsgId)
            .setConversationId(conversationId == null ? "" : conversationId)
            .setSequenceId(Math.max(0L, sequenceId))
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
