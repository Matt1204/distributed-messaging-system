package com.coen6731.chat.client.perf;

import com.coen6731.chat.client.ChatClientSession;
import com.coen6731.chat.client.ClientUiListener;
import com.coen6731.chat.client.perf.CsvReportWriter.SummaryRow;
import com.coen6731.chat.client.perf.MetricsCollector.RawEventRow;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Responsibility: run headless realtime-latency experiments without GUI interaction.
 * Input: CLI config args plus live server endpoint.
 * Output: raw event CSV, summary CSV, and metadata JSON per scenario point.
 */
public final class PerformanceLoadRunner {
  private PerformanceLoadRunner() {}

  public static void main(String[] args) throws Exception {
    PerfConfig config = PerfConfig.fromArgs(args);
    List<PerfConfig.ExperimentPoint> points = config.expandExperimentPoints();

    Path summaryFile =
        config.outputDir.resolve("summary").resolve("summary.csv");

    System.out.println("[perf] runId=" + config.runId + " scenario=" + config.scenario + " points=" + points.size());
    System.out.println("[perf] target=" + config.target + " isProd=" + config.isProd + " outputDir=" + config.outputDir);

    for (int i = 0; i < points.size(); i++) {
      PerfConfig.ExperimentPoint point = points.get(i);
      String pointRunId = config.runId + "_" + point.scenarioId();
      executePoint(config, point, pointRunId, summaryFile);

      if (i < points.size() - 1 && config.cooldownSec > 0) {
        System.out.println("[perf] cooldown " + config.cooldownSec + "s before next point...");
        Thread.sleep(config.cooldownSec * 1000L);
      }
    }

    System.out.println("[perf] completed. summary=" + summaryFile.toAbsolutePath());
  }

  private static void executePoint(
      PerfConfig config,
      PerfConfig.ExperimentPoint point,
      String pointRunId,
      Path summaryFile)
      throws Exception {
    Instant start = Instant.now();
    long pointStartNs = System.nanoTime();

    MetricsCollector collector = new MetricsCollector(pointRunId, point.scenarioId());
    List<PairRuntime> pairRuntimes = new ArrayList<>();

    System.out.println(
        "[perf] start point="
            + point.scenarioId()
            + " pairs="
            + point.pairs()
            + " ratePerSender="
            + point.ratePerSender()
            + " warmupSec="
            + point.warmupSec()
            + " measureSec="
            + point.measureSec());

    try {
      for (int pairId = 1; pairId <= point.pairs(); pairId++) {
        PairRuntime pair = createPairRuntime(config, collector, pairId);
        pairRuntimes.add(pair);
      }

      runPhase(pairRuntimes, point.ratePerSender(), point.warmupSec(), config.payloadBytes, false, collector);
      runPhase(pairRuntimes, point.ratePerSender(), point.measureSec(), config.payloadBytes, true, collector);

      if (config.drainSec > 0) {
        System.out.println("[perf] drain window " + config.drainSec + "s ...");
        Thread.sleep(config.drainSec * 1000L);
      }

      long nowNs = System.nanoTime();
      List<RawEventRow> rows =
          collector.snapshotMeasuredRows(nowNs, config.ackTimeoutMs, config.e2eTimeoutMs);

      Path rawFile = config.outputDir.resolve("raw").resolve(pointRunId + "_events.csv");
      CsvReportWriter.writeRawEvents(rawFile, rows);

      SummaryRow summary =
          CsvReportWriter.summarize(
              pointRunId,
              point.scenarioId(),
              config.namespace,
              config.target,
              config.isProd,
              point.pairs(),
              point.ratePerSender(),
              point.warmupSec(),
              point.measureSec(),
              config.drainSec,
              config.cooldownSec,
              config.payloadBytes,
              config.ackTimeoutMs,
              config.e2eTimeoutMs,
              rows);
      CsvReportWriter.appendSummary(summaryFile, summary);

      Instant end = Instant.now();
      Path metaFile = config.outputDir.resolve("meta").resolve(pointRunId + "_meta.json");
      Map<String, String> metadata =
          CsvReportWriter.metadata(
              pointRunId,
              point.scenarioId(),
              config.target,
              config.isProd,
              point.pairs(),
              point.ratePerSender(),
              point.warmupSec(),
              point.measureSec(),
              config.payloadBytes,
              config.ackTimeoutMs,
              config.e2eTimeoutMs,
              start,
              end,
              resolveGitCommit());
      CsvReportWriter.writeMetadataJson(metaFile, metadata);

      double elapsedSec = (System.nanoTime() - pointStartNs) / 1_000_000_000.0;
      System.out.println(
          "[perf] point completed="
              + point.scenarioId()
              + " attempted="
              + summary.attemptedMessages
              + " acked="
              + summary.ackedMessages
              + " received="
              + summary.receivedMessages
              + " ackP95Ms="
              + format(summary.ackP95Ms)
              + " e2eP95Ms="
              + format(summary.e2eP95Ms)
              + " elapsedSec="
              + String.format("%.1f", elapsedSec)
              + " raw="
              + rawFile.toAbsolutePath());
    } finally {
      for (PairRuntime pair : pairRuntimes) {
        pair.close();
      }
    }
  }

  private static PairRuntime createPairRuntime(
      PerfConfig config, MetricsCollector collector, int pairId) {
    String senderEmail = buildEmail(config.namespace, pairId, "sender");
    String receiverEmail = buildEmail(config.namespace, pairId, "receiver");

    ChatClientSession senderSession =
        new ChatClientSession(config.target, config.catchupLimit, config.isProd);
    ChatClientSession receiverSession =
        new ChatClientSession(config.target, config.catchupLimit, config.isProd);

    PairRuntime pairRuntime =
        new PairRuntime(pairId, senderEmail, receiverEmail, senderSession, receiverSession);

    senderSession.setUiListener(new SenderListener(collector, pairRuntime));
    receiverSession.setUiListener(new ReceiverListener(collector));

    try {
      AccountProvisioner.ensureAuthenticated(
          senderSession,
          senderEmail,
          config.password,
          config.connectTimeoutMs,
          "pair=" + pairId + " sender=" + senderEmail);
      AccountProvisioner.ensureAuthenticated(
          receiverSession,
          receiverEmail,
          config.password,
          config.connectTimeoutMs,
          "pair=" + pairId + " receiver=" + receiverEmail);
    } catch (RuntimeException e) {
      try {
        senderSession.close();
      } catch (Exception ignored) {
      }
      try {
        receiverSession.close();
      } catch (Exception ignored) {
      }
      throw e;
    }

    System.out.println(
        "[perf] ready pair=" + pairId + " sender=" + senderEmail + " receiver=" + receiverEmail);

    return pairRuntime;
  }

  private static void runPhase(
      List<PairRuntime> pairs,
      int ratePerSender,
      int durationSec,
      int payloadBytes,
      boolean measured,
      MetricsCollector collector)
      throws InterruptedException {
    if (durationSec <= 0 || pairs.isEmpty()) {
      return;
    }

    String phaseName = measured ? "MEASURE" : "WARMUP";
    System.out.println(
        "[perf] phase="
            + phaseName
            + " durationSec="
            + durationSec
            + " pairs="
            + pairs.size()
            + " ratePerSender="
            + ratePerSender);

    long durationNs = durationSec * 1_000_000_000L;
    ExecutorService executor = Executors.newFixedThreadPool(pairs.size());
    CountDownLatch done = new CountDownLatch(pairs.size());

    for (PairRuntime pair : pairs) {
      executor.submit(
          () -> {
            try {
              sendLoop(pair, ratePerSender, durationNs, payloadBytes, measured, collector);
            } finally {
              done.countDown();
            }
          });
    }

    done.await();
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  private static void sendLoop(
      PairRuntime pair,
      int ratePerSender,
      long durationNs,
      int payloadBytes,
      boolean measured,
      MetricsCollector collector) {
    long startNs = System.nanoTime();
    long endNs = startNs + durationNs;

    int boundedRate = Math.max(1, ratePerSender);
    long intervalNs = Math.max(1_000_000L, 1_000_000_000L / boundedRate);
    long nextTickNs = startNs;

    while (System.nanoTime() < endNs) {
      long seq = pair.sendCounter.incrementAndGet();
      String payload = buildPayload(pair.pairId, seq, payloadBytes, measured);

      long sendStartNs = System.nanoTime();
      String conversationId = pair.getConversationId();
      String clientMsgId =
          pair.senderSession.sendMessageAndReturnClientMsgId(
              pair.receiverEmail,
              payload,
              conversationId,
              "");
      collector.registerSend(pair.pairId, clientMsgId, sendStartNs, payloadBytes, measured);

      nextTickNs += intervalNs;
      sleepUntil(nextTickNs);
    }
  }

  private static void sleepUntil(long targetNs) {
    while (true) {
      long remain = targetNs - System.nanoTime();
      if (remain <= 0) {
        return;
      }
      LockSupport.parkNanos(Math.min(remain, 2_000_000L));
    }
  }

  private static String buildEmail(String namespace, int pairId, String role) {
    return "perf_" + namespace + "_p" + String.format("%03d", pairId) + "_" + role + "@example.test";
  }

  private static String buildPayload(int pairId, long seq, int payloadBytes, boolean measured) {
    String prefix = (measured ? "M" : "W") + "|p=" + pairId + "|n=" + seq + "|";
    if (prefix.length() >= payloadBytes) {
      return prefix.substring(0, payloadBytes);
    }
    StringBuilder sb = new StringBuilder(prefix);
    while (sb.length() < payloadBytes) {
      sb.append('x');
    }
    return sb.toString();
  }

  private static String resolveGitCommit() {
    try {
      Process process = new ProcessBuilder("git", "rev-parse", "--short", "HEAD").start();
      boolean finished = process.waitFor(2, TimeUnit.SECONDS);
      if (!finished || process.exitValue() != 0) {
        return "unknown";
      }
      String value = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      return value.isBlank() ? "unknown" : value;
    } catch (Exception ignored) {
      return "unknown";
    }
  }

  private static String format(double value) {
    return value < 0 ? "n/a" : String.format("%.3f", value);
  }

  private static final class PairRuntime {
    final int pairId;
    final String senderEmail;
    final String receiverEmail;
    final ChatClientSession senderSession;
    final ChatClientSession receiverSession;
    final AtomicLong sendCounter = new AtomicLong(0);
    volatile String conversationId = "";

    PairRuntime(
        int pairId,
        String senderEmail,
        String receiverEmail,
        ChatClientSession senderSession,
        ChatClientSession receiverSession) {
      this.pairId = pairId;
      this.senderEmail = senderEmail;
      this.receiverEmail = receiverEmail;
      this.senderSession = senderSession;
      this.receiverSession = receiverSession;
    }

    void close() {
      try {
        senderSession.close();
      } catch (Exception ignored) {
      }
      try {
        receiverSession.close();
      } catch (Exception ignored) {
      }
    }

    String getConversationId() {
      return conversationId == null ? "" : conversationId;
    }

    void observeConversationId(String value) {
      if (value == null || value.isBlank()) {
        return;
      }
      if (conversationId == null || conversationId.isBlank()) {
        conversationId = value;
      }
    }
  }

  private abstract static class NoopListener implements ClientUiListener {
    @Override
    public void onInfo(String text) {}

    @Override
    public void onConnectionState(boolean connected) {}

    @Override
    public void onAuthState(boolean authenticated, String email, String error) {}

    @Override
    public void onChatMessage(String conversationId, String fromEmail, String text, String msgId, long sentAtMs) {}

    @Override
    public void onSendAck(String clientMsgId, boolean success, String code, String reason) {}

    @Override
    public void onConversationDataChanged() {}

    @Override
    public void onHistoryResultSummary(String conversationId, long startSequenceId, int messageCount) {}

    @Override
    public void onCatchupResultSummary(String conversationId, long startSequenceId, int messageCount) {}

    @Override
    public void onError(String code, String reason) {}
  }

  private static final class SenderListener extends NoopListener {
    private final MetricsCollector collector;
    private final PairRuntime pairRuntime;

    SenderListener(MetricsCollector collector, PairRuntime pairRuntime) {
      this.collector = collector;
      this.pairRuntime = pairRuntime;
    }

    @Override
    public void onSendAckDetailed(
        String clientMsgId,
        String serverMsgId,
        String conversationId,
        long sequenceId,
        boolean success,
        String code,
        String reason) {
      collector.recordAckDetailed(
          clientMsgId,
          serverMsgId,
          sequenceId,
          success,
          code,
          reason,
          System.nanoTime());
      if (success) {
        pairRuntime.observeConversationId(conversationId);
      }
    }
  }

  private static final class ReceiverListener extends NoopListener {
    private final MetricsCollector collector;

    ReceiverListener(MetricsCollector collector) {
      this.collector = collector;
    }

    @Override
    public void onChatMessage(String conversationId, String fromEmail, String text, String msgId, long sentAtMs) {
      collector.recordInbound(msgId, System.nanoTime());
    }
  }
}
