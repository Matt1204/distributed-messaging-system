package com.coen6731.chat.client;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.CatchupRequest;
import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.ConversationCursor;
import com.coen6731.chat.GetMsgHistoryRequest;
import com.coen6731.chat.LoginUser;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.OutboundMessage;
import com.coen6731.chat.RegisterUser;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Responsibility: manage client grpc stream lifecycle, auth flow, and outbound sends.
 * Input: UI auth/send actions and server stream callbacks.
 * Output: grpc requests, DB updates, and UI listener notifications.
 */
public class ChatClientSession {
  private final String target;
  private final boolean isProd;
  private volatile DatabaseManager dbManager;
  private volatile String currentUserId;
  private volatile String currentEmail;

  private ManagedChannel channel;
  private StreamObserver<ClientEvent> requestObserver;

  private final ScheduledExecutorService scheduler;
  private final HeartbeatManager heartbeatManager;

  private final AtomicBoolean isConnected = new AtomicBoolean(false);
  private final AtomicBoolean isReconnecting = new AtomicBoolean(false);
  private final AtomicBoolean isClosing = new AtomicBoolean(false);
  private final AtomicBoolean isAuthenticated = new AtomicBoolean(false);
  private final AtomicBoolean catchupPendingAfterReconnect = new AtomicBoolean(false);

  private final AtomicReference<CountDownLatch> authLatchRef = new AtomicReference<>();
  private volatile boolean lastAuthAttemptSuccess = false;
  private volatile String lastAuthError = null;
  private volatile ClientUiListener uiListener;

  private volatile int reconnectDelayMs = 1000;
  private static final int MAX_RECONNECT_DELAY_MS = 5000;
  private final int catchupPerConversationLimit;

  /**
   * Responsibility: create session object and start initial connect attempt.
   * Input: grpc target endpoint.
   * Output: live connection startup.
   */
  public ChatClientSession(String target, int catchupPerConversationLimit, boolean isProd) {
    this.target = target;
    this.isProd = isProd;
    this.catchupPerConversationLimit = normalizePositiveLimit(catchupPerConversationLimit, 50, 200);
    this.scheduler = Executors.newScheduledThreadPool(2);

    this.heartbeatManager = new HeartbeatManager(scheduler, this::triggerReconnect);
    this.heartbeatManager.setRequestObserverSupplier(() -> this.requestObserver);

    this.currentUserId = null;
    this.currentEmail = null;

    connect();
  }

  /**
   * Responsibility: open grpc stream and attach stream handlers.
   * Input: current auth state for optional header attachment.
   * Output: active request observer and heartbeat scheduler.
   */
  private synchronized void connect() {
    if (isClosing.get() || isConnected.get()) {
      return;
    }

    try {
      notifyInfo("[client] connect(): Connecting to " + target + "...");

      ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
      if (target.endsWith(":443")) {
        builder.useTransportSecurity();
      } else {
        builder.usePlaintext();
      }
      this.channel = builder.build();

      MessagingServiceGrpc.MessagingServiceStub stub = MessagingServiceGrpc.newStub(channel);
      if (isAuthenticated.get() && currentUserId != null && !currentUserId.isBlank()) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), currentUserId);
        stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
      }

      ServerResponseHandler responseHandler =
          new ServerResponseHandler(
              () -> this.dbManager,
              heartbeatManager,
              this::triggerReconnect,
              this::onConnectionHealthy,
              this::onAuthSuccess,
              this::onAuthFailed,
              () -> this.currentUserId,
              () -> this.currentEmail,
              () -> this.uiListener,
              this::handleCatchupRequestAfterConnect);

      this.requestObserver = stub.chat(responseHandler);
      if (isAuthenticated.get()) {
        catchupPendingAfterReconnect.set(true);
      }
      heartbeatManager.start();
      notifyInfo("[client] Connection initiated... waiting for server response.");
    } catch (Exception e) {
      notifyInfo("[client] Connection setup failed: " + e.getMessage());
      triggerReconnect();
    }
  }

  /**
   * Responsibility: mark link healthy and reset reconnect backoff.
   * Input: any successful inbound server event.
   * Output: connection-state callback and heartbeat reset.
   */
  private void onConnectionHealthy() {
    if (!isConnected.get()) {
      isConnected.set(true);
      notifyConnectionState(true);
    }
    heartbeatManager.resetMissedPongs();
    reconnectDelayMs = 1000;
    handleCatchupRequestAfterConnect();
  }

  /**
   * Responsibility: process auth success and initialize per-user local DB.
   * Input: auth success payload.
   * Output: authenticated state and local persistence ready.
   */
  private void onAuthSuccess(AuthSuccess authSuccess) {
    this.currentUserId = authSuccess.getUserId();
    this.currentEmail = authSuccess.getEmail();
    this.isAuthenticated.set(true);
    this.lastAuthAttemptSuccess = true;
    this.lastAuthError = null;

    initializeUserDatabase(currentUserId, currentEmail);

    CountDownLatch latch = authLatchRef.getAndSet(null);
    if (latch != null) {
      latch.countDown();
    }

    notifyInfo(
        "[client] Authentication successful. Logged in as "
            + currentEmail
            + " ("
            + currentUserId
            + ")");
    notifyAuthState(true, currentEmail, null);
    sendCatchupRequest();
    notifyConversationDataChanged();
  }

  /**
   * Responsibility: request catchup once after a header-authenticated reconnect becomes healthy.
   * Input: connection-health callback state.
   * Output: one catchup request per reconnect cycle.
   */
  private void handleCatchupRequestAfterConnect() {
    if (!isAuthenticated.get()) {
      return;
    }
    if (catchupPendingAfterReconnect.compareAndSet(true, false)) {
      sendCatchupRequest();
    }
  }

  /**
   * Responsibility: send one catchup request using local per-conversation cursor hints.
   * Input: current authenticated user id and local cursor snapshot.
   * Output: catchup request event on active grpc stream.
   */
  public void sendCatchupRequest() {
    if (!isAuthenticated.get() || requestObserver == null || currentUserId == null || currentUserId.isBlank()) {
      return;
    }

    CatchupRequest.Builder request =
        CatchupRequest.newBuilder().setPerConversationLimit(catchupPerConversationLimit);
    DatabaseManager localDb = dbManager;
    if (localDb != null) {
      Map<String, Long> cursorMap = localDb.listConversationCursors(currentUserId);
      for (Map.Entry<String, Long> entry : cursorMap.entrySet()) {
        request.addCursorHints(
            ConversationCursor.newBuilder()
                .setConversationId(entry.getKey())
                .setClientLastReceivedSequenceId(Math.max(0L, entry.getValue()))
                .build());
      }
    }

    try {
      requestObserver.onNext(ClientEvent.newBuilder().setCatchupRequest(request.build()).build());
      notifyInfo("[client] Catchup requested.");
    } catch (Exception e) {
      notifyInfo("[client] Failed to request catchup: " + e.getMessage());
    }
  }

  /**
   * Responsibility: request older messages for one conversation before a sequence cursor.
   * Input: conversation id, before-sequence cursor, and page size.
   * Output: history request event on stream.
   */
  public void requestMessageHistory(String conversationId, long beforeSequenceId, int limit) {
    if (!isAuthenticated.get() || requestObserver == null || conversationId == null || conversationId.isBlank()) {
      return;
    }
    int boundedLimit = normalizePositiveLimit(limit, 50, 200);
    long boundedBeforeSequenceId = Math.max(1L, beforeSequenceId);
    GetMsgHistoryRequest request =
        GetMsgHistoryRequest.newBuilder()
            .setConversationId(conversationId)
            .setBeforeSequenceId(boundedBeforeSequenceId)
            .setRetriveMsgQuantity(boundedLimit)
            .build();
    try {
      requestObserver.onNext(ClientEvent.newBuilder().setGetMsgHistoryRequest(request).build());
      notifyInfo(
          "[client] history requested conv="
              + conversationId
              + " beforeSequenceId="
              + boundedBeforeSequenceId
              + " quantity="
              + boundedLimit);
    } catch (Exception e) {
      notifyInfo("[client] Failed to request history: " + e.getMessage());
    }
  }

  /**
   * Responsibility: open/create sqlite db bound to authenticated user email.
   * Input: user id and email from auth success.
   * Output: ready DatabaseManager and updated user_state row.
   */
  private synchronized void initializeUserDatabase(String userId, String email) {
    if (email == null || email.isBlank()) {
      throw new IllegalStateException("Cannot initialize user database: email is empty.");
    }

    String perUserDbPath = resolveUserDbPath(email);
    boolean dbExists = DatabaseManager.databaseExists(perUserDbPath);

    this.dbManager = new DatabaseManager(perUserDbPath);

    dbManager.updateUserState(userId, email);
    if (!dbExists) {
      notifyInfo("[client] created user database: " + perUserDbPath);
    } else {
      notifyInfo("[client] connected to user database: " + perUserDbPath);
    }
  }

  /**
   * Responsibility: generate safe db path for per-user local storage.
   * Input: user email.
   * Output: sqlite file path.
   */
  private String resolveUserDbPath(String email) {
    String normalizedEmail = email.trim().toLowerCase();
    String safeName = normalizedEmail.replaceAll("[^a-z0-9@._-]", "_");
    String prefix = isProd ? "prod_" : "dev_";
    return Path.of("chat-client", "db", prefix + safeName + ".db").toString();
  }

  /**
   * Responsibility: capture auth failure and release waiting caller.
   * Input: error code and reason.
   * Output: auth state notification and latch countdown.
   */
  private void onAuthFailed(String code, String reason) {
    if (code != null && (code.startsWith("AUTH_") || code.equals("BAD_REQUEST") || code.equals("INTERNAL"))) {
      this.lastAuthAttemptSuccess = false;
      this.lastAuthError = "code=" + code + " reason=" + reason;
      notifyAuthState(false, currentEmail, lastAuthError);
      CountDownLatch latch = authLatchRef.getAndSet(null);
      if (latch != null) {
        latch.countDown();
      }
    }
  }

  /**
   * Responsibility: teardown stream and schedule reconnect with backoff+jitter.
   * Input: reconnect trigger event.
   * Output: delayed connect retry.
   */
  private synchronized void triggerReconnect() {
    if (isClosing.get()) {
      return;
    }
    if (isReconnecting.getAndSet(true)) {
      return;
    }

    teardown();

    int jitter = (int) (Math.random() * 500);
    int delay = reconnectDelayMs + jitter;

    notifyInfo("[client] Reconnecting in " + delay + "ms...");
    notifyConnectionState(false);

    try {
      scheduler.schedule(
          () -> {
            isReconnecting.set(false);
            if (!isClosing.get()) {
              connect();
            }
          },
          delay,
          TimeUnit.MILLISECONDS);

      reconnectDelayMs = Math.min(reconnectDelayMs * 2, MAX_RECONNECT_DELAY_MS);
    } catch (RejectedExecutionException e) {
      isReconnecting.set(false);
      notifyInfo("[client] Reconnect scheduling rejected: " + e.getMessage());
    }
  }

  /**
   * Responsibility: close current grpc resources and reset runtime flags.
   * Input: none.
   * Output: closed stream/channel and pending auth latch release.
   */
  private void teardown() {
    heartbeatManager.stop();

    isConnected.set(false);
    if (isClosing.get()) {
      isAuthenticated.set(false);
      currentUserId = null;
      currentEmail = null;
    }

    CountDownLatch latch = authLatchRef.getAndSet(null);
    if (latch != null) {
      lastAuthAttemptSuccess = false;
      lastAuthError = "Connection closed during authentication";
      latch.countDown();
    }

    if (requestObserver != null) {
      try {
      requestObserver.onCompleted();
      } catch (Exception ignored) {
      }
      requestObserver = null;
    }
    if (channel != null) {
      channel.shutdownNow();
      channel = null;
    }
  }

  /**
   * Responsibility: send one outbound message and insert provisional local row before send.
   * Input: recipient email, text, and optional existing conversation/peer user id.
   * Output: outbound grpc event plus provisional SQLite row.
   */
  public void sendMessage(String toEmail, String text, String conversationId, String peerUserId) {
    if (!isAuthenticated.get()) {
      notifyInfo("[client] Not authenticated. Please login/register first.");
      return;
    }
    if (requestObserver == null) {
      notifyInfo("[client] Not connected. Queuing not implemented.");
      return;
    }

    String normalizedToEmail = toEmail == null ? "" : toEmail.trim().toLowerCase();
    String trimmedText = text == null ? "" : text.trim();
    String normalizedConversationId = conversationId == null ? "" : conversationId.trim();
    String normalizedPeerUserId = peerUserId == null ? "" : peerUserId.trim();
    String clientMsgId = UUID.randomUUID().toString();

    if (normalizedToEmail.isBlank() || trimmedText.isBlank()) {
      notifyInfo("[client] sendMessage requires toEmail and non-empty text.");
      return;
    }

    DatabaseManager localDb = this.dbManager;
    if (localDb != null) {
      // Core logic: keep provisional row so UI can render outbound intent before ack arrives.
      localDb.upsertOutboundProvisional(
          clientMsgId,
          normalizedConversationId,
          safe(currentUserId),
          safe(currentEmail),
          normalizedPeerUserId,
          normalizedToEmail,
          trimmedText,
          Instant.now().toEpochMilli());
      notifyConversationDataChanged();
    }

    OutboundMessage outboundMessage =
        OutboundMessage.newBuilder()
            .setToEmail(normalizedToEmail)
            .setText(trimmedText)
            .setClientMsgId(clientMsgId)
            .setConversationId(normalizedConversationId)
            .build();

    try {
      requestObserver.onNext(ClientEvent.newBuilder().setOutboundMessage(outboundMessage).build());
    } catch (Exception e) {
      notifyInfo("[client] Failed to send message: " + e.getMessage());
      if (localDb != null) {
        localDb.deleteOutboundByClientMsgId(clientMsgId);
        notifyConversationDataChanged();
      }
      triggerReconnect();
    }
  }

  /**
   * Responsibility: send login request and wait for auth result.
   * Input: email and password.
   * Output: true when login succeeds.
   */
  public boolean login(String email, String password) {
    LoginUser loginUser = LoginUser.newBuilder().setEmail(email).setPassword(password).build();
    return sendAuthRequest(ClientEvent.newBuilder().setLoginUser(loginUser).build());
  }

  /**
   * Responsibility: send register request and wait for auth result.
   * Input: email and password.
   * Output: true when registration succeeds.
   */
  public boolean register(String email, String password) {
    RegisterUser registerUser = RegisterUser.newBuilder().setEmail(email).setPassword(password).build();
    return sendAuthRequest(ClientEvent.newBuilder().setRegisterUser(registerUser).build());
  }

  /**
   * Responsibility: send auth event and block caller until success/failure/timeout.
   * Input: login/register client event.
   * Output: auth outcome boolean.
   */
  private boolean sendAuthRequest(ClientEvent event) {
    if (requestObserver == null) {
      notifyInfo("[client] Not connected. Cannot authenticate now.");
      return false;
    }
    if (isAuthenticated.get()) {
      notifyInfo("[client] Already authenticated as " + currentEmail);
      return true;
    }

    CountDownLatch latch = new CountDownLatch(1);
    authLatchRef.set(latch);
    lastAuthAttemptSuccess = false;
    lastAuthError = null;

    try {
      requestObserver.onNext(event);
    } catch (Exception e) {
      authLatchRef.set(null);
      lastAuthError = "Failed to send auth request: " + e.getMessage();
      notifyAuthState(false, currentEmail, lastAuthError);
      triggerReconnect();
      return false;
    }

    try {
      boolean completed = latch.await(15, TimeUnit.SECONDS);
      if (!completed) {
        authLatchRef.compareAndSet(latch, null);
        lastAuthError = "Authentication timed out";
        notifyAuthState(false, currentEmail, lastAuthError);
        return false;
      }
      return lastAuthAttemptSuccess;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      authLatchRef.compareAndSet(latch, null);
      lastAuthError = "Authentication interrupted";
      notifyAuthState(false, currentEmail, lastAuthError);
      return false;
    }
  }

  /**
   * Responsibility: expose latest local conversation list for UI.
   * Input: none.
   * Output: sorted conversation summaries.
   */
  public List<DatabaseManager.ConversationSummary> listConversations() {
    DatabaseManager localDb = dbManager;
    if (localDb == null) {
      return Collections.emptyList();
    }
    return localDb.listConversations();
  }

  /**
   * Responsibility: expose latest local messages for selected conversation.
   * Input: conversation id and max items.
   * Output: chronological conversation messages.
   */
  public List<DatabaseManager.MessageRow> listLatestMessages(String conversationId, int limit) {
    DatabaseManager localDb = dbManager;
    if (localDb == null || conversationId == null || conversationId.isBlank()) {
      return Collections.emptyList();
    }
    return localDb.listLatestMessages(conversationId, limit);
  }

  /**
   * Responsibility: expose older local messages before one sequence cursor.
   * Input: conversation id, before-sequence cursor, and max rows.
   * Output: chronological older messages from local SQLite.
   */
  public List<DatabaseManager.MessageRow> listMessagesBeforeSequence(
      String conversationId, long beforeSequenceId, int limit) {
    DatabaseManager localDb = dbManager;
    if (localDb == null || conversationId == null || conversationId.isBlank()) {
      return Collections.emptyList();
    }
    return localDb.listMessagesBeforeSequence(conversationId, beforeSequenceId, limit);
  }

  /**
   * Responsibility: expose local sequence-id existence set for one inclusive range.
   * Input: conversation id and sequence range.
   * Output: set of sequence ids currently present in sqlite.
   */
  public Set<Long> listExistingSequenceIdsInRange(
      String conversationId, long startSequenceId, long endSequenceId) {
    DatabaseManager localDb = dbManager;
    if (localDb == null || conversationId == null || conversationId.isBlank()) {
      return Set.of();
    }
    return localDb.listExistingSequenceIdsInRange(conversationId, startSequenceId, endSequenceId);
  }

  /**
   * Responsibility: expose local messages in one inclusive sequence range.
   * Input: conversation id and sequence range.
   * Output: messages ordered by sequence asc.
   */
  public List<DatabaseManager.MessageRow> listMessagesBySequenceRange(
      String conversationId, long startSequenceId, long endSequenceId) {
    DatabaseManager localDb = dbManager;
    if (localDb == null || conversationId == null || conversationId.isBlank()) {
      return Collections.emptyList();
    }
    return localDb.listMessagesBySequenceRange(conversationId, startSequenceId, endSequenceId);
  }

  /**
   * Responsibility: expose current per-conversation latest sequence id for debug UI.
   * Input: conversation id.
   * Output: local cursor value or 0 when unavailable.
   */
  public long getLatestMessageSequenceId(String conversationId) {
    DatabaseManager localDb = dbManager;
    if (localDb == null
        || currentUserId == null
        || currentUserId.isBlank()
        || conversationId == null
        || conversationId.isBlank()) {
      return 0L;
    }
    return localDb.getConversationCursor(currentUserId, conversationId);
  }

  /**
   * Responsibility: expose max stored sequence id in local messages table for one conversation.
   * Input: conversation id.
   * Output: max locally persisted sequence id or 0.
   */
  public long getMaxStoredSequenceId(String conversationId) {
    DatabaseManager localDb = dbManager;
    if (localDb == null || conversationId == null || conversationId.isBlank()) {
      return 0L;
    }
    return localDb.getMaxStoredSequenceId(conversationId);
  }

  /**
   * Responsibility: return current auth state.
   * Input: none.
   * Output: true when authenticated.
   */
  public boolean isAuthenticated() {
    return isAuthenticated.get();
  }

  /**
   * Responsibility: return last auth failure message for UI.
   * Input: none.
   * Output: error text or null.
   */
  public String getLastAuthError() {
    return lastAuthError;
  }

  /**
   * Responsibility: register UI listener for callbacks.
   * Input: listener implementation.
   * Output: stored callback reference.
   */
  public void setUiListener(ClientUiListener listener) {
    this.uiListener = listener;
  }

  /**
   * Responsibility: close session resources and stop schedulers.
   * Input: none.
   * Output: closed client session.
   */
  public void close() {
    notifyInfo("[client] Closing session...");
    isClosing.set(true);
    teardown();
    scheduler.shutdownNow();
  }

  /**
   * Responsibility: forward informational text to UI listener.
   * Input: info text message.
   * Output: listener callback side-effect.
   */
  private void notifyInfo(String text) {
    ClientUiListener listener = uiListener;
    if (listener != null) {
      listener.onInfo(text);
    }
  }

  /**
   * Responsibility: notify UI about connection state transitions.
   * Input: connected flag.
   * Output: listener callback side-effect.
   */
  private void notifyConnectionState(boolean connected) {
    ClientUiListener listener = uiListener;
    if (listener != null) {
      listener.onConnectionState(connected);
    }
  }

  /**
   * Responsibility: notify UI about auth state changes.
   * Input: auth flag, email, and optional error.
   * Output: listener callback side-effect.
   */
  private void notifyAuthState(boolean authenticated, String email, String error) {
    ClientUiListener listener = uiListener;
    if (listener != null) {
      listener.onAuthState(authenticated, email, error);
    }
  }

  /**
   * Responsibility: notify UI that conversation/message data changed in local DB.
   * Input: none.
   * Output: listener callback side-effect.
   */
  private void notifyConversationDataChanged() {
    ClientUiListener listener = uiListener;
    if (listener != null) {
      listener.onConversationDataChanged();
    }
  }

  /**
   * Responsibility: normalize nullable strings before DB/proto writes.
   * Input: nullable value.
   * Output: non-null string.
   */
  private String safe(String value) {
    return value == null ? "" : value;
  }

  /**
   * Responsibility: normalize configurable page size into positive bounded value.
   * Input: candidate limit, default value, and max bound.
   * Output: sanitized limit safe for server requests.
   */
  private int normalizePositiveLimit(int candidate, int defaultValue, int maxValue) {
    if (candidate <= 0) {
      return defaultValue;
    }
    return Math.min(candidate, maxValue);
  }
}
