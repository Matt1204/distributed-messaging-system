package com.coen6731.chat.client;

import com.coen6731.chat.ChatMessage;
import com.coen6731.chat.ClientEvent;
import com.coen6731.chat.Heartbeat;
import com.coen6731.chat.MessagingServiceGrpc;
import com.coen6731.chat.Register;
import com.coen6731.chat.SendMessage;
import com.coen6731.chat.ServerError;
import com.coen6731.chat.ServerEvent;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a single gRPC chat session.
 * Responsible for channel lifecycle, stream observers, and heartbeat scheduling.
 */
public class ChatClientSession {
  private final ManagedChannel channel;
  private final StreamObserver<ClientEvent> requestObserver;
  private final ScheduledExecutorService heartbeatExecutor;
  private final AtomicBoolean heartbeatStarted = new AtomicBoolean(false);

  public ChatClientSession(String target) {
    // Create a channel to the server. 'usePlaintext()' is used for unencrypted communication (dev only).
    this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

    // Create an async stub. Stubs are used to call service methods.
    MessagingServiceGrpc.MessagingServiceStub stub = MessagingServiceGrpc.newStub(channel);

    // Executor for sending heartbeat messages periodically.
    this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    // Initiate the bidirectional stream.
    // The call returns a requestObserver that we use to send client events.
    this.requestObserver = stub.chat(createResponseObserver());
  }

  public void register(String userId) {
    Register register = Register.newBuilder().setUserId(userId).build();
    requestObserver.onNext(ClientEvent.newBuilder().setRegister(register).build());

    // Start heartbeats after successful registration attempt.
    if (heartbeatStarted.compareAndSet(false, true)) {
      startHeartbeat();
    }
  }

  public void sendMessage(String toUserId, String text) {
    SendMessage sendMessage =
        SendMessage.newBuilder()
            .setToUserId(toUserId)
            .setText(text)
            .setClientMsgId(UUID.randomUUID().toString())
            .build();
    requestObserver.onNext(ClientEvent.newBuilder().setSendMessage(sendMessage).build());
  }

  public void close() {
    requestObserver.onCompleted();
    heartbeatExecutor.shutdownNow();
    channel.shutdownNow();
  }

  private StreamObserver<ServerEvent> createResponseObserver() {
    // Observer to handle incoming messages FROM the server (ServerEvent).
    return new StreamObserver<>() {
      @Override
      public void onNext(ServerEvent value) {
        switch (value.getPayloadCase()) {
          case CHATMESSAGE:
            ChatMessage msg = value.getChatMessage();
            System.out.println(
                msg.getFromUserId()
                    + ": "
                    + msg.getText()
                    + " ("
                    + msg.getServerMsgId()
                    + ", "
                    + msg.getTs()
                    + ")");
            break;
          case SERVERERROR:
            ServerError err = value.getServerError();
            System.out.println("ERROR code=" + err.getCode() + " reason=" + err.getReason());
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
          if (status.getCode() == Status.Code.UNAVAILABLE) {
            System.out.println("[client] onError() - server unavailable, code" + status.getCode());
            return;
          }
          System.out.println("[client] onError() - stream error: " + status);
          return;
        }
        System.out.println("[client] onError() - stream error: " + t.getMessage());
      }

      @Override
      public void onCompleted() {
        System.out.println("[client] stream closed by server");
      }
    };
  }

  // Sends a heartbeat message every 10 seconds to keep the connection alive.
  private void startHeartbeat() {
    heartbeatExecutor.scheduleAtFixedRate(
        () -> {
          Heartbeat heartbeat = Heartbeat.newBuilder().setTs(Instant.now().toEpochMilli()).build();
          requestObserver.onNext(ClientEvent.newBuilder().setHeartbeat(heartbeat).build());
        },
        10,
        10,
        TimeUnit.SECONDS);
  }
}
