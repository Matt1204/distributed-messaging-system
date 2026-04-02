package com.coen6731.chat.client.perf;

import com.coen6731.chat.client.perf.MetricsCollector.RawEventRow;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Responsibility: write raw event rows, run summaries, and metadata artifacts.
 * Input: measured event rows and run metadata.
 * Output: CSV/JSON files under results folders.
 */
public final class CsvReportWriter {
  private CsvReportWriter() {}

  public static void writeRawEvents(Path file, List<RawEventRow> rows) throws IOException {
    ensureParentDir(file);
    try (BufferedWriter writer = Files.newBufferedWriter(file)) {
      writer.write(
          "run_id,scenario_id,pair_id,client_msg_id,server_msg_id,send_start_ns,ack_recv_ns,inbound_recv_ns,"
              + "ack_success,ack_error_code,ack_error_reason,payload_bytes,ack_latency_ms,e2e_latency_ms,"
              + "is_ack_timeout,is_e2e_timeout,sequence_id");
      writer.newLine();
      for (RawEventRow row : rows) {
        writer.write(
            csv(
                row.runId(),
                row.scenarioId(),
                String.valueOf(row.pairId()),
                row.clientMsgId(),
                row.serverMsgId(),
                String.valueOf(row.sendStartNs()),
                String.valueOf(row.ackRecvNs()),
                String.valueOf(row.inboundRecvNs()),
                String.valueOf(row.ackSuccess()),
                row.ackErrorCode(),
                row.ackErrorReason(),
                String.valueOf(row.payloadBytes()),
                formatDouble(row.ackLatencyMs()),
                formatDouble(row.e2eLatencyMs()),
                String.valueOf(row.ackTimeout()),
                String.valueOf(row.e2eTimeout()),
                String.valueOf(row.sequenceId())));
        writer.newLine();
      }
    }
  }

  public static void appendSummary(Path file, SummaryRow summary) throws IOException {
    ensureParentDir(file);
    boolean exists = Files.exists(file);
    try (BufferedWriter writer =
        Files.newBufferedWriter(
            file,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND)) {
      if (!exists) {
        writer.write(
            "run_id,scenario_id,namespace,target,is_prod,pairs,arrival_pattern,rate_per_sender,warmup_sec,measure_sec,"
                + "drain_sec,cooldown_sec,payload_bytes,ack_timeout_ms,e2e_timeout_ms,"
                + "attempted_messages,acked_messages,received_messages,"
                + "ack_fail_count,ack_timeout_count,e2e_timeout_count,"
                + "attempted_rate_per_sec,ack_success_rate,receive_rate,ack_timeout_rate,e2e_timeout_rate,"
                + "ack_latency_samples,e2e_latency_samples,"
                + "ack_p50_ms,ack_p75_ms,ack_p85_ms,ack_p95_ms,ack_p99_ms,"
                + "e2e_p50_ms,e2e_p95_ms,e2e_p99_ms");
        writer.newLine();
      }
      writer.write(
          csv(
              summary.runId,
              summary.scenarioId,
              summary.namespace,
              summary.target,
              String.valueOf(summary.isProd),
              String.valueOf(summary.pairs),
              summary.arrivalPattern,
              String.valueOf(summary.ratePerSender),
              String.valueOf(summary.warmupSec),
              String.valueOf(summary.measureSec),
              String.valueOf(summary.drainSec),
              String.valueOf(summary.cooldownSec),
              String.valueOf(summary.payloadBytes),
              String.valueOf(summary.ackTimeoutMs),
              String.valueOf(summary.e2eTimeoutMs),
              String.valueOf(summary.attemptedMessages),
              String.valueOf(summary.ackedMessages),
              String.valueOf(summary.receivedMessages),
              String.valueOf(summary.ackFailCount),
              String.valueOf(summary.ackTimeoutCount),
              String.valueOf(summary.e2eTimeoutCount),
              formatDouble(summary.attemptedRatePerSec),
              formatDouble(summary.ackSuccessRate),
              formatDouble(summary.receiveRate),
              formatDouble(summary.ackTimeoutRate),
              formatDouble(summary.e2eTimeoutRate),
              String.valueOf(summary.ackLatencySamples),
              String.valueOf(summary.e2eLatencySamples),
              formatDouble(summary.ackP50Ms),
              formatDouble(summary.ackP75Ms),
              formatDouble(summary.ackP85Ms),
              formatDouble(summary.ackP95Ms),
              formatDouble(summary.ackP99Ms),
              formatDouble(summary.e2eP50Ms),
              formatDouble(summary.e2eP95Ms),
              formatDouble(summary.e2eP99Ms)));
      writer.newLine();
    }
  }

  public static void writeMetadataJson(Path file, Map<String, String> metadata) throws IOException {
    ensureParentDir(file);
    List<String> keys = new ArrayList<>(metadata.keySet());
    Collections.sort(keys);
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    for (int i = 0; i < keys.size(); i++) {
      String key = keys.get(i);
      String value = metadata.getOrDefault(key, "");
      sb.append("  \"").append(escapeJson(key)).append("\": \"").append(escapeJson(value)).append("\"");
      if (i < keys.size() - 1) {
        sb.append(',');
      }
      sb.append('\n');
    }
    sb.append("}\n");
    Files.writeString(file, sb.toString());
  }

  public static SummaryRow summarize(
      String runId,
      String scenarioId,
      String namespace,
      String target,
      boolean isProd,
      int pairs,
      String arrivalPattern,
      int ratePerSender,
      int warmupSec,
      int measureSec,
      int drainSec,
      int cooldownSec,
      int payloadBytes,
      long ackTimeoutMs,
      long e2eTimeoutMs,
      List<RawEventRow> rows) {
    List<Double> ackLatencies = new ArrayList<>();
    List<Double> e2eLatencies = new ArrayList<>();

    int acked = 0;
    int received = 0;
    int ackFail = 0;
    int ackTimeout = 0;
    int e2eTimeout = 0;

    for (RawEventRow row : rows) {
      if (row.ackRecvNs() > 0) {
        acked++;
        if (row.ackLatencyMs() >= 0) {
          ackLatencies.add(row.ackLatencyMs());
        }
      }
      if (row.inboundRecvNs() > 0) {
        received++;
        if (row.e2eLatencyMs() >= 0) {
          e2eLatencies.add(row.e2eLatencyMs());
        }
      }
      if (!row.ackSuccess()) {
        if (row.ackRecvNs() > 0 || !isBlank(row.ackErrorCode()) || !isBlank(row.ackErrorReason())) {
          ackFail++;
        }
      }
      if (row.ackTimeout()) {
        ackTimeout++;
      }
      if (row.e2eTimeout()) {
        e2eTimeout++;
      }
    }

    int ackLatencySamples = ackLatencies.size();
    int e2eLatencySamples = e2eLatencies.size();
    double attemptedRatePerSec = safeRate(rows.size(), measureSec);
    double ackSuccessRate = safeRate(acked, rows.size());
    double receiveRate = safeRate(received, rows.size());
    double ackTimeoutRate = safeRate(ackTimeout, rows.size());
    double e2eTimeoutRate = safeRate(e2eTimeout, rows.size());

    return new SummaryRow(
        runId,
        scenarioId,
        namespace,
        target,
        isProd,
        pairs,
        arrivalPattern,
        ratePerSender,
        warmupSec,
        measureSec,
        drainSec,
        cooldownSec,
        payloadBytes,
        ackTimeoutMs,
        e2eTimeoutMs,
        rows.size(),
        acked,
        received,
        ackFail,
        ackTimeout,
        e2eTimeout,
        attemptedRatePerSec,
        ackSuccessRate,
        receiveRate,
        ackTimeoutRate,
        e2eTimeoutRate,
        ackLatencySamples,
        e2eLatencySamples,
        percentile(ackLatencies, 50),
        percentile(ackLatencies, 75),
        percentile(ackLatencies, 85),
        percentile(ackLatencies, 95),
        percentile(ackLatencies, 99),
        percentile(e2eLatencies, 50),
        percentile(e2eLatencies, 95),
        percentile(e2eLatencies, 99));
  }

  private static double percentile(List<Double> values, int p) {
    if (values.isEmpty()) {
      return -1.0;
    }
    List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    int idx = (int) Math.ceil((p / 100.0) * sorted.size()) - 1;
    idx = Math.max(0, Math.min(sorted.size() - 1, idx));
    return sorted.get(idx);
  }

  private static void ensureParentDir(Path file) throws IOException {
    Path parent = file.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
  }

  private static String csv(String... values) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      String value = values[i] == null ? "" : values[i];
      sb.append('"').append(value.replace("\"", "\"\"")).append('"');
    }
    return sb.toString();
  }

  private static String formatDouble(double value) {
    if (value < 0) {
      return "";
    }
    return String.format(Locale.ROOT, "%.3f", value);
  }

  private static double safeRate(int numerator, int denominator) {
    if (denominator <= 0) {
      return -1.0;
    }
    return (double) numerator / denominator;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static String escapeJson(String value) {
    String raw = value == null ? "" : value;
    return raw.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
  }

  public static Map<String, String> metadata(
      String runId,
      String scenarioId,
      String target,
      boolean isProd,
      int pairs,
      int ratePerSender,
      int warmupSec,
      int measureSec,
      int payloadBytes,
      long ackTimeoutMs,
      long e2eTimeoutMs,
      String arrivalPattern,
      Long rngSeed,
      int pairSetupParallelism,
      Instant start,
      Instant end,
      String gitCommit) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("run_id", runId);
    metadata.put("scenario_id", scenarioId);
    metadata.put("target", target);
    metadata.put("is_prod", String.valueOf(isProd));
    metadata.put("pairs", String.valueOf(pairs));
    metadata.put("arrival_pattern", arrivalPattern);
    metadata.put("rate_per_sender", String.valueOf(ratePerSender));
    metadata.put("warmup_sec", String.valueOf(warmupSec));
    metadata.put("measure_sec", String.valueOf(measureSec));
    metadata.put("payload_bytes", String.valueOf(payloadBytes));
    metadata.put("ack_timeout_ms", String.valueOf(ackTimeoutMs));
    metadata.put("e2e_timeout_ms", String.valueOf(e2eTimeoutMs));
    metadata.put("pair_setup_parallelism", String.valueOf(pairSetupParallelism));
    metadata.put("rng_seed", rngSeed == null ? "" : String.valueOf(rngSeed));
    metadata.put("start_time_utc", start.toString());
    metadata.put("end_time_utc", end.toString());
    metadata.put("git_commit", gitCommit == null ? "unknown" : gitCommit);
    return metadata;
  }

  public static final class SummaryRow {
    public final String runId;
    public final String scenarioId;
    public final String namespace;
    public final String target;
    public final boolean isProd;
    public final int pairs;
    public final String arrivalPattern;
    public final int ratePerSender;
    public final int warmupSec;
    public final int measureSec;
    public final int drainSec;
    public final int cooldownSec;
    public final int payloadBytes;
    public final long ackTimeoutMs;
    public final long e2eTimeoutMs;
    public final int attemptedMessages;
    public final int ackedMessages;
    public final int receivedMessages;
    public final int ackFailCount;
    public final int ackTimeoutCount;
    public final int e2eTimeoutCount;
    public final double attemptedRatePerSec;
    public final double ackSuccessRate;
    public final double receiveRate;
    public final double ackTimeoutRate;
    public final double e2eTimeoutRate;
    public final int ackLatencySamples;
    public final int e2eLatencySamples;
    public final double ackP50Ms;
    public final double ackP75Ms;
    public final double ackP85Ms;
    public final double ackP95Ms;
    public final double ackP99Ms;
    public final double e2eP50Ms;
    public final double e2eP95Ms;
    public final double e2eP99Ms;

    SummaryRow(
        String runId,
        String scenarioId,
        String namespace,
        String target,
        boolean isProd,
        int pairs,
        String arrivalPattern,
        int ratePerSender,
        int warmupSec,
        int measureSec,
        int drainSec,
        int cooldownSec,
        int payloadBytes,
        long ackTimeoutMs,
        long e2eTimeoutMs,
        int attemptedMessages,
        int ackedMessages,
        int receivedMessages,
        int ackFailCount,
        int ackTimeoutCount,
        int e2eTimeoutCount,
        double attemptedRatePerSec,
        double ackSuccessRate,
        double receiveRate,
        double ackTimeoutRate,
        double e2eTimeoutRate,
        int ackLatencySamples,
        int e2eLatencySamples,
        double ackP50Ms,
        double ackP75Ms,
        double ackP85Ms,
        double ackP95Ms,
        double ackP99Ms,
        double e2eP50Ms,
        double e2eP95Ms,
        double e2eP99Ms) {
      this.runId = runId;
      this.scenarioId = scenarioId;
      this.namespace = namespace;
      this.target = target;
      this.isProd = isProd;
      this.pairs = pairs;
      this.arrivalPattern = arrivalPattern;
      this.ratePerSender = ratePerSender;
      this.warmupSec = warmupSec;
      this.measureSec = measureSec;
      this.drainSec = drainSec;
      this.cooldownSec = cooldownSec;
      this.payloadBytes = payloadBytes;
      this.ackTimeoutMs = ackTimeoutMs;
      this.e2eTimeoutMs = e2eTimeoutMs;
      this.attemptedMessages = attemptedMessages;
      this.ackedMessages = ackedMessages;
      this.receivedMessages = receivedMessages;
      this.ackFailCount = ackFailCount;
      this.ackTimeoutCount = ackTimeoutCount;
      this.e2eTimeoutCount = e2eTimeoutCount;
      this.attemptedRatePerSec = attemptedRatePerSec;
      this.ackSuccessRate = ackSuccessRate;
      this.receiveRate = receiveRate;
      this.ackTimeoutRate = ackTimeoutRate;
      this.e2eTimeoutRate = e2eTimeoutRate;
      this.ackLatencySamples = ackLatencySamples;
      this.e2eLatencySamples = e2eLatencySamples;
      this.ackP50Ms = ackP50Ms;
      this.ackP75Ms = ackP75Ms;
      this.ackP85Ms = ackP85Ms;
      this.ackP95Ms = ackP95Ms;
      this.ackP99Ms = ackP99Ms;
      this.e2eP50Ms = e2eP50Ms;
      this.e2eP95Ms = e2eP95Ms;
      this.e2eP99Ms = e2eP99Ms;
    }
  }
}
