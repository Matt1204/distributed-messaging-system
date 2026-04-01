package com.coen6731.chat.client.perf;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Responsibility: collect sender and receiver callback events and correlate them into one message timeline.
 * Input: send-start, ack-detailed, and inbound events.
 * Output: immutable raw rows ready for CSV/report aggregation.
 */
public final class MetricsCollector {
  private final String runId;
  private final String scenarioId;

  private final ConcurrentHashMap<String, MessageTrace> tracesByClientMsgId = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> clientMsgIdByServerMsgId = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> pendingInboundNsByServerMsgId = new ConcurrentHashMap<>();

  public MetricsCollector(String runId, String scenarioId) {
    this.runId = runId;
    this.scenarioId = scenarioId;
  }

  public void registerSend(
      int pairId, String clientMsgId, long sendStartNs, int payloadBytes, boolean measured) {
    if (isBlank(clientMsgId)) {
      return;
    }
    tracesByClientMsgId.compute(
        clientMsgId,
        (key, existing) -> {
          MessageTrace trace = existing == null ? new MessageTrace(runId, scenarioId, pairId, key) : existing;
          trace.pairId = pairId;
          trace.sendStartNs = sendStartNs;
          trace.payloadBytes = payloadBytes;
          trace.measured = measured;
          return trace;
        });
  }

  public void recordAckDetailed(
      String clientMsgId,
      String serverMsgId,
      long sequenceId,
      boolean success,
      String code,
      String reason,
      long ackRecvNs) {
    if (isBlank(clientMsgId)) {
      return;
    }

    MessageTrace trace =
        tracesByClientMsgId.computeIfAbsent(
            clientMsgId, key -> new MessageTrace(runId, scenarioId, 0, key));
    trace.ackRecvNs = ackRecvNs;
    trace.ackSuccess = success;
    trace.ackErrorCode = safe(code);
    trace.ackErrorReason = safe(reason);
    trace.sequenceId = sequenceId;

    String normalizedServerMsgId = safe(serverMsgId);
    if (!normalizedServerMsgId.isBlank()) {
      trace.serverMsgId = normalizedServerMsgId;
      clientMsgIdByServerMsgId.put(normalizedServerMsgId, clientMsgId);
      Long pendingInboundNs = pendingInboundNsByServerMsgId.remove(normalizedServerMsgId);
      if (pendingInboundNs != null) {
        trace.inboundRecvNs = pendingInboundNs;
      }
    }
  }

  public void recordInbound(String serverMsgId, long inboundRecvNs) {
    if (isBlank(serverMsgId)) {
      return;
    }

    String clientMsgId = clientMsgIdByServerMsgId.get(serverMsgId);
    if (clientMsgId == null) {
      pendingInboundNsByServerMsgId.put(serverMsgId, inboundRecvNs);
      return;
    }

    MessageTrace trace = tracesByClientMsgId.get(clientMsgId);
    if (trace != null) {
      trace.inboundRecvNs = inboundRecvNs;
    }
  }

  public List<RawEventRow> snapshotMeasuredRows(long nowNs, long ackTimeoutMs, long e2eTimeoutMs) {
    long ackTimeoutNs = ackTimeoutMs * 1_000_000L;
    long e2eTimeoutNs = e2eTimeoutMs * 1_000_000L;

    List<RawEventRow> rows = new ArrayList<>();
    for (Map.Entry<String, MessageTrace> entry : tracesByClientMsgId.entrySet()) {
      MessageTrace trace = entry.getValue();
      if (!trace.measured) {
        continue;
      }

      long sendStartNs = trace.sendStartNs;
      long ackRecvNs = trace.ackRecvNs;
      long inboundRecvNs = trace.inboundRecvNs;

      boolean ackTimeout = sendStartNs > 0 && ackRecvNs <= 0 && nowNs - sendStartNs > ackTimeoutNs;
      boolean e2eTimeout = sendStartNs > 0 && inboundRecvNs <= 0 && nowNs - sendStartNs > e2eTimeoutNs;

      double ackLatencyMs = ackRecvNs > 0 && sendStartNs > 0 ? nanosToMillis(ackRecvNs - sendStartNs) : -1.0;
      double e2eLatencyMs =
          inboundRecvNs > 0 && sendStartNs > 0 ? nanosToMillis(inboundRecvNs - sendStartNs) : -1.0;

      rows.add(
          new RawEventRow(
              trace.runId,
              trace.scenarioId,
              trace.pairId,
              trace.clientMsgId,
              safe(trace.serverMsgId),
              sendStartNs,
              ackRecvNs,
              inboundRecvNs,
              trace.ackSuccess,
              safe(trace.ackErrorCode),
              safe(trace.ackErrorReason),
              trace.payloadBytes,
              ackLatencyMs,
              e2eLatencyMs,
              ackTimeout,
              e2eTimeout,
              trace.sequenceId));
    }

    rows.sort(Comparator.comparingLong(RawEventRow::sendStartNs));
    return rows;
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static double nanosToMillis(long nanos) {
    return nanos / 1_000_000.0;
  }

  public static final class MessageTrace {
    final String runId;
    final String scenarioId;
    volatile int pairId;
    final String clientMsgId;

    volatile String serverMsgId;
    volatile long sendStartNs;
    volatile long ackRecvNs;
    volatile long inboundRecvNs;
    volatile boolean ackSuccess;
    volatile String ackErrorCode;
    volatile String ackErrorReason;
    volatile long sequenceId;
    volatile int payloadBytes;
    volatile boolean measured;

    MessageTrace(String runId, String scenarioId, int pairId, String clientMsgId) {
      this.runId = runId;
      this.scenarioId = scenarioId;
      this.pairId = pairId;
      this.clientMsgId = clientMsgId;
      this.serverMsgId = "";
      this.ackErrorCode = "";
      this.ackErrorReason = "";
    }
  }

  public record RawEventRow(
      String runId,
      String scenarioId,
      int pairId,
      String clientMsgId,
      String serverMsgId,
      long sendStartNs,
      long ackRecvNs,
      long inboundRecvNs,
      boolean ackSuccess,
      String ackErrorCode,
      String ackErrorReason,
      int payloadBytes,
      double ackLatencyMs,
      double e2eLatencyMs,
      boolean ackTimeout,
      boolean e2eTimeout,
      long sequenceId) {}
}
