package com.coen6731.chat.client.perf;

import io.github.cdimascio.dotenv.Dotenv;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Responsibility: parse CLI arguments and expose scenario presets for headless perf runs.
 * Input: main args and environment variables.
 * Output: normalized config object plus experiment-point matrix.
 */
public final class PerfConfig {
  private static final DateTimeFormatter RUN_ID_FMT =
      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC);

  public final String target;
  public final boolean isProd;
  public final String scenario;
  public final String namespace;
  public final String password;
  public final Path outputDir;
  public final int payloadBytes;
  public final long ackTimeoutMs;
  public final long e2eTimeoutMs;
  public final int drainSec;
  public final int cooldownSec;
  public final long connectTimeoutMs;
  public final int catchupLimit;
  public final String runId;

  public final Integer overridePairs;
  public final Integer overrideRatePerSender;
  public final Integer overrideWarmupSec;
  public final Integer overrideMeasureSec;

  private PerfConfig(
      String target,
      boolean isProd,
      String scenario,
      String namespace,
      String password,
      Path outputDir,
      int payloadBytes,
      long ackTimeoutMs,
      long e2eTimeoutMs,
      int drainSec,
      int cooldownSec,
      long connectTimeoutMs,
      int catchupLimit,
      String runId,
      Integer overridePairs,
      Integer overrideRatePerSender,
      Integer overrideWarmupSec,
      Integer overrideMeasureSec) {
    this.target = target;
    this.isProd = isProd;
    this.scenario = scenario;
    this.namespace = namespace;
    this.password = password;
    this.outputDir = outputDir;
    this.payloadBytes = payloadBytes;
    this.ackTimeoutMs = ackTimeoutMs;
    this.e2eTimeoutMs = e2eTimeoutMs;
    this.drainSec = drainSec;
    this.cooldownSec = cooldownSec;
    this.connectTimeoutMs = connectTimeoutMs;
    this.catchupLimit = catchupLimit;
    this.runId = runId;
    this.overridePairs = overridePairs;
    this.overrideRatePerSender = overrideRatePerSender;
    this.overrideWarmupSec = overrideWarmupSec;
    this.overrideMeasureSec = overrideMeasureSec;
  }

  public static PerfConfig fromArgs(String[] args) {
    Map<String, String> argMap = parseArgMap(args);
    Dotenv dotenv = Dotenv.configure()
        .directory("chat-client")
        .ignoreIfMissing()
        .load();

    String target =
        firstNonBlank(
            argMap.get("target"),
            dotenv.get("TARGET"),
            System.getenv("TARGET"));
    if (isBlank(target)) {
      throw new IllegalArgumentException("--target is required (or set TARGET in chat-client/.env or env).");
    }

    String scenario = normalizeScenario(firstNonBlank(argMap.get("scenario"), "baseline"));
    String namespace = sanitizeNamespace(firstNonBlank(argMap.get("namespace"), "default"));
    String password = firstNonBlank(argMap.get("password"), "PerfPass#123");
    String isProdRaw =
        firstNonBlank(
            argMap.get("isProd"),
            dotenv.get("IS_PROD"),
            System.getenv("IS_PROD"));
    boolean isProd = isBlank(isProdRaw) ? inferIsProdFromTarget(target) : parseBoolean(isProdRaw, false);

    Path outputDir = Path.of(firstNonBlank(argMap.get("outputDir"), "results"));
    int payloadBytes = clampInt(parseInt(argMap.get("payloadBytes"), 128), 16, 4096);
    long ackTimeoutMs = clampLong(parseLong(argMap.get("ackTimeoutMs"), 5000L), 1000L, 120000L);
    long e2eTimeoutMs = clampLong(parseLong(argMap.get("e2eTimeoutMs"), 10000L), 1000L, 120000L);
    int drainSec = clampInt(parseInt(argMap.get("drainSec"), 10), 0, 300);
    int cooldownSec = clampInt(parseInt(argMap.get("cooldownSec"), 15), 0, 600);
    long connectTimeoutMs = clampLong(parseLong(argMap.get("connectTimeoutMs"), 20000L), 1000L, 300000L);
    int catchupLimit = clampInt(parseInt(argMap.get("catchupLimit"), 50), 1, 200);

    Integer overridePairs = parseNullableInt(argMap.get("pairs"));
    Integer overrideRate = parseNullableInt(argMap.get("ratePerSender"));
    Integer overrideWarmup = parseNullableInt(argMap.get("warmupSec"));
    Integer overrideMeasure = parseNullableInt(argMap.get("measureSec"));

    if (overridePairs != null) {
      overridePairs = clampInt(overridePairs, 1, 500);
    }
    if (overrideRate != null) {
      overrideRate = clampInt(overrideRate, 1, 500);
    }
    if (overrideWarmup != null) {
      overrideWarmup = clampInt(overrideWarmup, 0, 7200);
    }
    if (overrideMeasure != null) {
      overrideMeasure = clampInt(overrideMeasure, 1, 7200);
    }

    String runId = firstNonBlank(argMap.get("runId"), "run_" + RUN_ID_FMT.format(Instant.now()));

    return new PerfConfig(
        target,
        isProd,
        scenario,
        namespace,
        password,
        outputDir,
        payloadBytes,
        ackTimeoutMs,
        e2eTimeoutMs,
        drainSec,
        cooldownSec,
        connectTimeoutMs,
        catchupLimit,
        runId,
        overridePairs,
        overrideRate,
        overrideWarmup,
        overrideMeasure);
  }

  public List<ExperimentPoint> expandExperimentPoints() {
    List<ExperimentPoint> points = new ArrayList<>();
    switch (scenario) {
      case "baseline":
        points.add(buildPoint("baseline", 1, 1, 60, 240));
        break;
      case "rate-ramp":
        for (int rate : Arrays.asList(5, 10, 20, 40)) {
          points.add(buildPoint("rate-ramp-r" + rate, 1, rate, 60, 180));
        }
        break;
      case "concurrency-ramp":
        for (int pairs : Arrays.asList(1, 5, 10, 20)) {
          points.add(buildPoint("concurrency-ramp-p" + pairs, pairs, 5, 60, 300));
        }
        break;
      case "all":
        points.add(buildPoint("baseline", 1, 1, 60, 240));
        for (int rate : Arrays.asList(5, 10, 20, 40)) {
          points.add(buildPoint("rate-ramp-r" + rate, 1, rate, 60, 180));
        }
        for (int pairs : Arrays.asList(1, 5, 10, 20)) {
          points.add(buildPoint("concurrency-ramp-p" + pairs, pairs, 5, 60, 300));
        }
        break;
      default:
        throw new IllegalStateException("Unsupported scenario: " + scenario);
    }
    return points;
  }

  private ExperimentPoint buildPoint(
      String scenarioId, int defaultPairs, int defaultRatePerSender, int defaultWarmupSec, int defaultMeasureSec) {
    int pairs = overridePairs != null ? overridePairs : defaultPairs;
    int rate = overrideRatePerSender != null ? overrideRatePerSender : defaultRatePerSender;
    int warmupSec = overrideWarmupSec != null ? overrideWarmupSec : defaultWarmupSec;
    int measureSec = overrideMeasureSec != null ? overrideMeasureSec : defaultMeasureSec;
    return new ExperimentPoint(scenarioId, pairs, rate, warmupSec, measureSec);
  }

  public record ExperimentPoint(
      String scenarioId,
      int pairs,
      int ratePerSender,
      int warmupSec,
      int measureSec) {}

  private static Map<String, String> parseArgMap(String[] args) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String token = args[i];
      if (!token.startsWith("--")) {
        continue;
      }
      String key = token.substring(2);
      String value = "true";
      if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        value = args[++i];
      }
      map.put(key, value);
    }
    return map;
  }

  private static boolean parseBoolean(String raw, boolean fallback) {
    if (isBlank(raw)) {
      return fallback;
    }
    String normalized = raw.trim().toLowerCase(Locale.ROOT);
    return "1".equals(normalized)
        || "true".equals(normalized)
        || "yes".equals(normalized)
        || "on".equals(normalized);
  }

  private static int parseInt(String raw, int fallback) {
    if (isBlank(raw)) {
      return fallback;
    }
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException ignored) {
      return fallback;
    }
  }

  private static long parseLong(String raw, long fallback) {
    if (isBlank(raw)) {
      return fallback;
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException ignored) {
      return fallback;
    }
  }

  private static Integer parseNullableInt(String raw) {
    if (isBlank(raw)) {
      return null;
    }
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static int clampInt(int value, int min, int max) {
    return Math.max(min, Math.min(max, value));
  }

  private static long clampLong(long value, long min, long max) {
    return Math.max(min, Math.min(max, value));
  }

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (!isBlank(value)) {
        return value;
      }
    }
    return null;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static String normalizeScenario(String scenario) {
    String normalized = scenario == null ? "baseline" : scenario.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case "baseline":
      case "rate-ramp":
      case "concurrency-ramp":
      case "all":
        return normalized;
      default:
        throw new IllegalArgumentException(
            "Unsupported --scenario '" + scenario + "'. Use baseline|rate-ramp|concurrency-ramp|all");
    }
  }

  private static String sanitizeNamespace(String namespace) {
    String value = namespace == null ? "default" : namespace.trim().toLowerCase(Locale.ROOT);
    if (value.isBlank()) {
      return "default";
    }
    return value.replaceAll("[^a-z0-9_-]", "_");
  }

  private static boolean inferIsProdFromTarget(String target) {
    String normalized = target == null ? "" : target.trim().toLowerCase(Locale.ROOT);
    if (normalized.contains("localhost")
        || normalized.contains("127.0.0.1")
        || normalized.contains("0.0.0.0")
        || normalized.contains(".local")
        || normalized.contains("dev")
        || normalized.contains("staging")
        || normalized.contains("test")) {
      return false;
    }
    return true;
  }
}
