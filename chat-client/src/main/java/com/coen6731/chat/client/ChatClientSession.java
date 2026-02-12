package com.coen6731.chat.client;

import com.coen6731.chat.Catchup;
import com.coen6731.chat.RegisterUser;
import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.SendMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChatClientSession {
  private final String target;
  private final DatabaseManager dbManager;
  private String currentUserId;

  private ManagedChannel channel;
  private StreamObserver<ClientEvent> requestObserver;

  private final ScheduledExecutorService scheduler;
  private final HeartbeatManager heartbeatManager;

  private final AtomicBoolean isConnected = new AtomicBoolean(false);
  private final AtomicBoolean isReconnecting = new AtomicBoolean(false);

  private int reconnectDelayMs = 1000;
  private static final int MAX_RECONNECT_DELAY_MS = 5000;

  public ChatClientSession(String target, String dbPath) {
    this.target = target;
    this.dbManager = new DatabaseManager(dbPath);
    // 2 threads: 1 for heartbeat (managed by HeartbeatManager), 1 for lifecycle/reconnect tasks
    this.scheduler = Executors.newScheduledThreadPool(2);

    // Initialize HeartbeatManager
    this.heartbeatManager = new HeartbeatManager(scheduler, this::triggerReconnect);
    // Provide access to the current requestObserver
    this.heartbeatManager.setRequestObserverSupplier(() -> this.requestObserver);

    String storedUserId = dbManager.getUserId();
    System.out.println("[client] read db " + dbPath + " logged in as " + storedUserId);
    
    if (storedUserId != null && !storedUserId.isBlank()) {
      this.currentUserId = storedUserId;
      connect();
    } else {
      System.out.println("[client] no user_id in database; use /register <userId> to set one");
      this.requestObserver = null; 
    }
  }

  private synchronized void connect() {
    // If already connected, do nothing
    if (isConnected.get()) return;

    try {
      System.out.println("[client] Connecting to " + target + "...");
      
      ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
      if (target.endsWith(":443")) {
        builder.useTransportSecurity();
      } else {
        builder.usePlaintext();
      }
      this.channel = builder.build();

      MessagingServiceGrpc.MessagingServiceStub stub = MessagingServiceGrpc.newStub(channel);

      Metadata metadata = new Metadata();
      metadata.put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), currentUserId);
      stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

      // Create Response Handler
      ServerResponseHandler responseHandler = new ServerResponseHandler(
          dbManager, 
          heartbeatManager, 
          this::triggerReconnect, 
          currentUserId
      );
      
      this.requestObserver = stub.chat(responseHandler);
      
      isConnected.set(true);
      reconnectDelayMs = 1000; // Reset backoff on successful connection setup
      
      heartbeatManager.start();
      // sendCatchup();
      
      System.out.println("[client] Connected.");
    } catch (Exception e) {
      System.out.println("[client] Connection setup failed: " + e.getMessage());
      triggerReconnect();
    }
  }

  private synchronized void triggerReconnect() {
    if (isReconnecting.getAndSet(true)) {
        return; // Already reconnecting
    }
    
    teardown();
    
    // Exponential backoff + Jitter
    int jitter = (int)(Math.random() * 500);
    int delay = reconnectDelayMs + jitter;
    
    System.out.println("[client] Reconnecting in " + delay + "ms...");
    
    scheduler.schedule(() -> {
        isReconnecting.set(false);
        connect();
    }, delay, TimeUnit.MILLISECONDS);
    
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, MAX_RECONNECT_DELAY_MS);
  }

  private void teardown() {
    System.out.println("[client] Teardown (cleaning up resources)...");
    heartbeatManager.stop();
    
    isConnected.set(false);
    
    if (requestObserver != null) {
        try { requestObserver.onCompleted(); } catch (Exception e) {}
        requestObserver = null;
    }
    
    if (channel != null) {
        channel.shutdownNow();
        channel = null;
    }
  }

  public void sendMessage(String toUserId, String text) {
    if (requestObserver == null) {
        System.out.println("[client] Not connected. Queuing not implemented.");
        return;
    }
    SendMessage sendMessage = SendMessage.newBuilder()
        .setToUserId(toUserId)
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

  public void close() {
    System.out.println("[client] Closing session...");
    scheduler.shutdownNow();
    teardown();
  }

  public void sendRegisterUser(String userId, String userName) {
    if (requestObserver == null) {
      System.out.println("[client] Not connected. Queuing not implemented.");
      return;
    }
    RegisterUser registerUser = RegisterUser.newBuilder()
      .setUserId(userId)
      .setUserName(userName)
      .build();
    requestObserver.onNext(ClientEvent.newBuilder().setRegisterUser(registerUser).build());
  }

  private void sendCatchup() {
      try {
          if (requestObserver != null) {
              Catchup catchup = Catchup.newBuilder()
                  .setUserId(currentUserId)
                  .setLastSyncSequenceId(0) // TODO: Get from DB
                  .build();
              requestObserver.onNext(ClientEvent.newBuilder().setCatchup(catchup).build());
              System.out.println("[client] Sent Catchup request");
          }
      } catch (Exception e) {
          System.out.println("[client] Failed to send catchup: " + e.getMessage());
      }
  }
}
