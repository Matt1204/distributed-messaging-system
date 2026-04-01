package com.coen6731.chat.server;

import com.coen6731.chat.InboundMessage;
import com.coen6731.chat.ServerEvent;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Responsibility: asynchronously execute non-critical post-persist message work.
 * Input: persisted message metadata + canonical inbound payload.
 * Output: best-effort conversation touch and live delivery side effects.
 */
@Component
public class PostPersistWorker {
  private static final Logger logger = LoggerFactory.getLogger(PostPersistWorker.class);
  private static final int WORKER_THREADS = 6;
  private static final int QUEUE_CAPACITY = 30000;
  private static final int SHUTDOWN_WAIT_SECONDS = 8;
  private static final long TOUCH_RETRY_BACKOFF_MS = 30L;
  private static final long REJECT_LOG_INTERVAL_MS = 10000L;

  private final ConnectionRegistry connectionRegistry;
  private final CosmosDBHandler cosmosDBHandler;
  private final String serverReplicaId;
  private final ThreadPoolExecutor executor;

  private final LongAdder submittedCount = new LongAdder();
  private final LongAdder completedCount = new LongAdder();
  private final LongAdder rejectedCount = new LongAdder();
  private final LongAdder touchFailedCount = new LongAdder();
  private final LongAdder liveDeliveryFailedCount = new LongAdder();
  private final AtomicLong lastRejectLogAtMs = new AtomicLong(0L);

  public PostPersistWorker(
      ConnectionRegistry connectionRegistry,
      CosmosDBHandler cosmosDBHandler,
      @Value("${container.app.replica.name}") String serverReplicaId) {
    this.connectionRegistry = connectionRegistry;
    this.cosmosDBHandler = cosmosDBHandler;
    this.serverReplicaId = serverReplicaId;
    this.executor = new ThreadPoolExecutor(
        WORKER_THREADS,
        WORKER_THREADS,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(QUEUE_CAPACITY),
        createNamedThreadFactory(),
        createDropAndCountPolicy());
  }

  /**
   * Responsibility: enqueue one post-persist task without blocking send ack path.
   * Input: conversation/message metadata and already built inbound payload.
   * Output: queued background task, or dropped task when queue is full/shutdown.
   */
  public void submit(
      String conversationId,
      long lastMessageAtMs,
      String toUserId,
      String toEmail,
      InboundMessage inboundMessage) {
    submittedCount.increment();
    try {
      executor.execute(
          () -> runTask(new PostPersistTask(conversationId, lastMessageAtMs, toUserId, toEmail, inboundMessage)));
    } catch (Exception e) {
      rejectedCount.increment();
      logRejectedRateLimited("post-persist submit failed (executor unavailable)");
      logger.debug("[{}] post-persist submit exception", serverReplicaId, e);
    }
  }

  /**
   * Responsibility: stop worker and best-effort drain queue on bean shutdown.
   * Input: bean lifecycle callback.
   * Output: executor terminated and shutdown summary log emitted.
   */
  @PreDestroy
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
        int droppedOnShutdown = executor.shutdownNow().size();
        logger.warn(
            "[{}] post-persist shutdown timed out; droppedRemainingTasks={}",
            serverReplicaId,
            droppedOnShutdown);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }

    logger.info(
        "[{}] post-persist shutdown summary submitted={} completed={} rejected={} touchFailed={} liveDeliveryFailed={} queueRemaining={}",
        serverReplicaId,
        submittedCount.sum(),
        completedCount.sum(),
        rejectedCount.sum(),
        touchFailedCount.sum(),
        liveDeliveryFailedCount.sum(),
        executor.getQueue().size());
  }

  private java.util.concurrent.ThreadFactory createNamedThreadFactory() {
    AtomicInteger index = new AtomicInteger(1);
    return runnable -> {
      Thread worker = new Thread(runnable, "post-persist-worker-" + index.getAndIncrement());
      worker.setDaemon(true);
      return worker;
    };
  }

  private RejectedExecutionHandler createDropAndCountPolicy() {
    return (runnable, threadPoolExecutor) -> {
      rejectedCount.increment();
      logRejectedRateLimited(
          "post-persist queue full; dropped task (queueCapacity=" + QUEUE_CAPACITY + ")");
    };
  }

  private void runTask(PostPersistTask task) {
    try {
      safeTouchConversation(task.conversationId(), task.lastMessageAtMs());
      safeDeliverLive(task.toUserId(), task.toEmail(), task.inboundMessage());
    } finally {
      completedCount.increment();
    }
  }

  private void safeTouchConversation(String conversationId, long lastMessageAtMs) {
    CosmosDBHandler.TouchResult touchResult = cosmosDBHandler.touchConversationFast(conversationId, lastMessageAtMs);
    if (touchResult.success()) {
      return;
    }

    if (touchResult.statusCode() != 404) {
      sleepQuietly(TOUCH_RETRY_BACKOFF_MS);
      CosmosDBHandler.TouchResult retryResult = cosmosDBHandler.touchConversationFast(conversationId, lastMessageAtMs);
      if (retryResult.success()) {
        return;
      }
      touchResult = retryResult;
    }

    if (touchResult.statusCode() == 404 && cosmosDBHandler.touchConversation(conversationId, lastMessageAtMs)) {
      return;
    }

    touchFailedCount.increment();
    logger.warn(
        "[{}] postprocess touch failed conversationId={} statusCode={} reason={}",
        serverReplicaId,
        conversationId,
        touchResult.statusCode(),
        touchResult.errorReason());
  }

  private void safeDeliverLive(String toUserId, String toEmail, InboundMessage message) {
    try {
      UserSession localUserSession = connectionRegistry.getSession(toUserId);
      if (localUserSession != null) {
        localUserSession.send(ServerEvent.newBuilder().setInboundMessage(message).build());
        logger.info(
            "[{}] live-delivered local sender={} recipient={} serverMsgId={} conversationId={} sequenceId={}",
            serverReplicaId,
            message.getFromUserId(),
            toUserId,
            message.getServerMsgId(),
            message.getConversationId(),
            message.getSequenceId());
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
              "[{}] live-relayed sender={} recipientEmail={} targetInstance={} serverMsgId={} conversationId={} sequenceId={}",
              serverReplicaId,
              message.getFromUserId(),
              toEmail,
              targetInstanceId,
              message.getServerMsgId(),
              message.getConversationId(),
              message.getSequenceId());
          return;
        }
      }

      logger.info(
          "[{}] recipient offline sender={} recipient={} serverMsgId={} conversationId={} sequenceId={}",
          serverReplicaId,
          message.getFromUserId(),
          toUserId,
          message.getServerMsgId(),
          message.getConversationId(),
          message.getSequenceId());
    } catch (Exception e) {
      liveDeliveryFailedCount.increment();
      logger.warn(
          "[{}] postprocess live delivery failed sender={} recipient={} serverMsgId={}",
          serverReplicaId,
          message.getFromUserId(),
          toUserId,
          message.getServerMsgId(),
          e);
    }
  }

  private void logRejectedRateLimited(String message) {
    long nowMs = System.currentTimeMillis();
    long lastMs = lastRejectLogAtMs.get();
    if (nowMs - lastMs >= REJECT_LOG_INTERVAL_MS && lastRejectLogAtMs.compareAndSet(lastMs, nowMs)) {
      logger.warn(
          "[{}] {} rejectedCount={} queueDepth={}",
          serverReplicaId,
          message,
          rejectedCount.sum(),
          executor.getQueue().size());
    }
  }

  private void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private record PostPersistTask(
      String conversationId,
      long lastMessageAtMs,
      String toUserId,
      String toEmail,
      InboundMessage inboundMessage) {
  }
}
