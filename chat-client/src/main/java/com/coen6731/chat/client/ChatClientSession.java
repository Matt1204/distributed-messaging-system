package com.coen6731.chat.client;

import com.coen6731.chat.AuthSuccess;
import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.LoginUser;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.RegisterUser;
import com.coen6731.chat.SendMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ChatClientSession {
  private final String target;
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

  private final AtomicReference<CountDownLatch> authLatchRef = new AtomicReference<>();
  private volatile boolean lastAuthAttemptSuccess = false;
  private volatile String lastAuthError = null;

  private volatile int reconnectDelayMs = 1000;
  private static final int MAX_RECONNECT_DELAY_MS = 5000;

  public ChatClientSession(String target) {
    this.target = target;
    this.scheduler = Executors.newScheduledThreadPool(2);

    this.heartbeatManager = new HeartbeatManager(scheduler, this::triggerReconnect);
    this.heartbeatManager.setRequestObserverSupplier(() -> this.requestObserver);

    this.currentUserId = null;
    this.currentEmail = null;

    connect();
  }

  private synchronized void connect() {
    if (isClosing.get()) return;
    if (isConnected.get()) return;

    try {
      System.out.println("[client] connect(): Connecting to " + target + "...");

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

      ServerResponseHandler responseHandler = new ServerResponseHandler(
          () -> this.dbManager,
          heartbeatManager,
          this::triggerReconnect,
          this::onConnectionHealthy,
          this::onAuthSuccess,
          this::onAuthFailed,
          () -> this.currentUserId
      );

      this.requestObserver = stub.chat(responseHandler);

      heartbeatManager.start();
      System.out.println("[client] Connection initiated... waiting for server response.");
    } catch (Exception e) {
      System.out.println("[client] Connection setup failed: " + e.getMessage());
      triggerReconnect();
    }
  }

  private void onConnectionHealthy() {
    if (!isConnected.get()) {
      isConnected.set(true);
    }
    heartbeatManager.resetMissedPongs();
    reconnectDelayMs = 1000;
  }

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

    System.out.println("[client] Authentication successful. Logged in as " + currentEmail + " (" + currentUserId + ")");
  }

  private synchronized void initializeUserDatabase(String userId, String email) {
    if (email == null || email.isBlank()) {
      throw new IllegalStateException("Cannot initialize user database: email is empty.");
    }

    String perUserDbPath = resolveUserDbPath(email);
    boolean dbExists = DatabaseManager.databaseExists(perUserDbPath);

    this.dbManager = new DatabaseManager(perUserDbPath);

    if (!dbExists) {
      dbManager.updateUserState(userId, email, null);
      System.out.println("[client] created user database: " + perUserDbPath);
    } else {
      String currentSync = dbManager.getLastSyncSequenceId(userId);
      dbManager.updateUserState(userId, email, currentSync);
      System.out.println("[client] connected to user database: " + perUserDbPath);
    }
  }

  private String resolveUserDbPath(String email) {
    String normalizedEmail = email.trim().toLowerCase();
    String safeName = normalizedEmail.replaceAll("[^a-z0-9@._-]", "_");
    return Path.of("chat-client", "db", safeName + ".db").toString();
  }

  private void onAuthFailed(String code, String reason) {
    if (code != null && (code.startsWith("AUTH_") || code.equals("BAD_REQUEST") || code.equals("INTERNAL"))) {
      this.lastAuthAttemptSuccess = false;
      this.lastAuthError = "code=" + code + " reason=" + reason;
      CountDownLatch latch = authLatchRef.getAndSet(null);
      if (latch != null) {
        latch.countDown();
      }
    }
  }

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

    System.out.println("[client] Reconnecting in " + delay + "ms...");

    try {
      scheduler.schedule(() -> {
        isReconnecting.set(false);
        if (!isClosing.get()) {
          connect();
        }
      }, delay, TimeUnit.MILLISECONDS);

      reconnectDelayMs = Math.min(reconnectDelayMs * 2, MAX_RECONNECT_DELAY_MS);
    } catch (RejectedExecutionException e) {
      isReconnecting.set(false);
      System.out.println("[client] Reconnect scheduling rejected: " + e.getMessage());
    }
  }

  private void teardown() {
    heartbeatManager.stop();

    isConnected.set(false);
    isAuthenticated.set(false);

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

  public void sendMessage(String toEmail, String text) {
    if (!isAuthenticated.get()) {
      System.out.println("[client] Not authenticated. Please login/register first.");
      return;
    }
    if (requestObserver == null) {
      System.out.println("[client] Not connected. Queuing not implemented.");
      return;
    }
    SendMessage sendMessage = SendMessage.newBuilder()
        .setToEmail(toEmail)
        .setText(text)
        .setClientMsgId(UUID.randomUUID().toString())
        .build();
    try {
      requestObserver.onNext(ClientEvent.newBuilder().setSendMessage(sendMessage).build());
    } catch (Exception e) {
      System.out.println("[client] Failed to send message: " + e.getMessage());
      triggerReconnect();
    }
  }

  public boolean login(String email, String password) {
    LoginUser loginUser = LoginUser.newBuilder().setEmail(email).setPassword(password).build();
    return sendAuthRequest(ClientEvent.newBuilder().setLoginUser(loginUser).build());
  }

  public boolean register(String email, String password) {
    RegisterUser registerUser = RegisterUser.newBuilder().setEmail(email).setPassword(password).build();
    return sendAuthRequest(ClientEvent.newBuilder().setRegisterUser(registerUser).build());
  }

  private boolean sendAuthRequest(ClientEvent event) {
    if (requestObserver == null) {
      System.out.println("[client] Not connected. Cannot authenticate now.");
      return false;
    }
    if (isAuthenticated.get()) {
      System.out.println("[client] Already authenticated as " + currentEmail);
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
      triggerReconnect();
      return false;
    }

    try {
      boolean completed = latch.await(15, TimeUnit.SECONDS);
      if (!completed) {
        authLatchRef.compareAndSet(latch, null);
        lastAuthError = "Authentication timed out";
        return false;
      }
      return lastAuthAttemptSuccess;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      authLatchRef.compareAndSet(latch, null);
      lastAuthError = "Authentication interrupted";
      return false;
    }
  }

  public boolean isAuthenticated() {
    return isAuthenticated.get();
  }

  public String getLastAuthError() {
    return lastAuthError;
  }

  public void close() {
    System.out.println("[client] Closing session...");
    isClosing.set(true);
    teardown();
    scheduler.shutdownNow();
  }
}
