package com.coen6731.chat.server;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
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
 * Responsibility: execute full send pipeline asynchronously with bounded queue.
 * Input: runnable send task and command metadata.
 * Output: best-effort queued execution, or immediate rejection when overloaded.
 */
@Component
public class SendAsyncExecutor {
  private static final Logger logger = LoggerFactory.getLogger(SendAsyncExecutor.class);
  private static final int QUEUE_CAPACITY = 30000;
  private static final int SHUTDOWN_WAIT_SECONDS = 8;
  private static final long REJECT_LOG_INTERVAL_MS = 10000L;

  private final String serverReplicaId;
  private final ThreadPoolExecutor executor;
  private final LongAdder submittedCount = new LongAdder();
  private final LongAdder rejectedCount = new LongAdder();
  private final LongAdder completedCount = new LongAdder();
  private final AtomicLong lastRejectLogAtMs = new AtomicLong(0L);

  public SendAsyncExecutor(
      @Value("${container.app.replica.name}") String serverReplicaId,
      @Value("${chat.send.worker-threads}") int configuredWorkerThreads) {
    this.serverReplicaId = serverReplicaId;
    int workerThreads = Math.max(1, configuredWorkerThreads);
    this.executor = new ThreadPoolExecutor(
        workerThreads,
        workerThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(QUEUE_CAPACITY),
        runnable -> {
          AtomicInteger index = THREAD_COUNTER;
          Thread worker = new Thread(runnable, "send-worker-" + index.getAndIncrement());
          worker.setDaemon(true);
          return worker;
        });
    logger.info(
        "[{}] send executor initialized workerThreads={} queueCapacity={}",
        serverReplicaId,
        workerThreads,
        QUEUE_CAPACITY);
  }

  private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(1);

  /**
   * Responsibility: enqueue one send command task without blocking caller thread.
   * Input: sender and command identity plus runnable task.
   * Output: true when accepted; false when queue/executor rejects.
   */
  public boolean submit(String senderUserId, String clientMsgId, long acceptedAtMs, Runnable runnableTask) {
    submittedCount.increment();
    try {
      executor.execute(
          () -> {
            try {
              runnableTask.run();
            } finally {
              completedCount.increment();
            }
          });
      return true;
    } catch (RejectedExecutionException rejected) {
      rejectedCount.increment();
      logRejectRateLimited(senderUserId, clientMsgId, acceptedAtMs);
      return false;
    } catch (Exception e) {
      rejectedCount.increment();
      logger.warn(
          "[{}] send queue submit failed sender={} clientMsgId={} queueDepth={}",
          serverReplicaId,
          senderUserId,
          clientMsgId,
          executor.getQueue().size(),
          e);
      return false;
    }
  }

  /**
   * Responsibility: provide a point-in-time executor state snapshot for diagnostics.
   * Input: none.
   * Output: immutable metrics snapshot.
   */
  public ExecutorSnapshot snapshot() {
    return new ExecutorSnapshot(
        executor.getCorePoolSize(),
        executor.getActiveCount(),
        executor.getQueue().size(),
        submittedCount.sum(),
        completedCount.sum(),
        rejectedCount.sum());
  }

  @PreDestroy
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
        int dropped = executor.shutdownNow().size();
        logger.warn("[{}] send executor forced shutdown droppedTasks={}", serverReplicaId, dropped);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }

    logger.info(
        "[{}] send executor summary submitted={} completed={} rejected={} queueRemaining={}",
        serverReplicaId,
        submittedCount.sum(),
        completedCount.sum(),
        rejectedCount.sum(),
        executor.getQueue().size());
  }

  private void logRejectRateLimited(String senderUserId, String clientMsgId, long acceptedAtMs) {
    long nowMs = System.currentTimeMillis();
    long lastMs = lastRejectLogAtMs.get();
    if (nowMs - lastMs >= REJECT_LOG_INTERVAL_MS && lastRejectLogAtMs.compareAndSet(lastMs, nowMs)) {
      logger.warn(
          "[{}] send queue overload sender={} clientMsgId={} queueDepth={} taskAgeMs={} rejected={}",
          serverReplicaId,
          senderUserId,
          clientMsgId,
          executor.getQueue().size(),
          Math.max(0L, nowMs - acceptedAtMs),
          rejectedCount.sum());
    }
  }

  public record ExecutorSnapshot(
      int workerThreads,
      int activeWorkers,
      int queueDepth,
      long submitted,
      long completed,
      long rejected) {
  }
}
