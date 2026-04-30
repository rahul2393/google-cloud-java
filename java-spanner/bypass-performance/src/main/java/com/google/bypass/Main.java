package com.google.bypass;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.google.cloud.NoCredentials;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/** Probe application to run spanner client with enabled bypass. */
final class Main {

  private static final int DEFAULT_WARMUP_CYCLES = 1000;

  // Configuration via environment variables with defaults
  private static int qps = envInt("QPS", 4);
  private static boolean enableBypass = envBool("GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API", false);
  private static boolean enableGrpcGcp = envBool("ENABLE_GRPC_GCP", true);
  private static boolean useOtelForSpannerTracing =
      envBool("SPANNER_USE_OPENTELEMETRY_TRACING", true);
  private static boolean enableSpannerApiTracing = envBool("SPANNER_ENABLE_API_TRACING", true);
  private static boolean enableSpannerExtendedTracing =
      envBool("SPANNER_ENABLE_EXTENDED_TRACING", false);
  private static boolean enableSpannerEndToEndTracing =
      envBool("SPANNER_ENABLE_END_TO_END_TRACING", false);
  private static boolean enableDebugLogging = envBool("ENABLE_DEBUG_LOGGING", false);
  private static String workload = envStr("PROBE_TYPE", "strong_read");
  private static String endpoint = envStr("ENDPOINT", "").trim();
  private static String telemetryProjectId =
      envNonBlankStr("OTEL_PROJECT_ID", "span-cloud-testing");
  private static String spannerProjectId =
      envNonBlankStr("SPANNER_PROJECT_ID", "span-cloud-testing");
  private static String spannerInstanceId =
      envNonBlankStr("SPANNER_INSTANCE_ID", "irahul-load-test");
  private static String spannerDatabaseId = envNonBlankStr("SPANNER_DATABASE_ID", "db");
  private static int numRows = envInt("NUM_ROWS", 10000000);
  private static int payloadSize = envInt("PAYLOAD_SIZE", 1000);
  private static long maxStalenessSeconds = envLong("MAX_STALENESS_SECONDS", 60);
  private static String serviceName = envStr("OTEL_SERVICE_NAME", "irahul-jloadtest");
  private static String ycsbTable = envNonBlankStr("YCSB_TABLE", "usertable");
  private static String ycsbUserId = envNonBlankStr("YCSB_USER_ID", "811092608265");
  private static String ycsbKey = envStr("YCSB_KEY", "");
  private static int ycsbZeroPadding = envInt("YCSB_ZERO_PADDING", 20);

  private static final OpenTelemetrySdk openTelemetrySdk = initializeOpenTelemetry();
  private static final Tracer tracer = openTelemetrySdk.getTracer("jloadtest");
  private static final Meter meter = openTelemetrySdk.getMeter("jloadtest");
  private static final Logger logger = Logger.getLogger(Main.class.getName());
  private static final LongCounter requestCounter =
      meter
          .counterBuilder("jop_count")
          .setDescription("Total requests processed by MySampleApp")
          .setUnit("1")
          .build();

  private static final DoubleHistogram latencyHistogram =
      meter
          .histogramBuilder("jlatency")
          .setDescription("Latency of requests processed by MySampleApp")
          .setUnit("ms")
          .setExplicitBucketBoundariesAdvice(
              Arrays.asList(
                  0.0, 0.01, 0.05, 0.1, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.575, 0.6,
                  0.625, 0.65, 0.675, 0.7, 0.725, 0.75, 0.775, 0.8, 0.825, 0.85, 0.875, 0.9, 0.925,
                  0.95, 0.975, 1.0, 1.05, 1.1, 1.15, 1.2, 1.25, 1.3, 1.35, 1.4, 1.45, 1.5, 1.55,
                  1.6, 1.65, 1.7, 1.75, 1.8, 1.85, 1.9, 1.95, 2.0, 2.05, 2.1, 2.15, 2.2, 2.25, 2.3,
                  2.35, 2.4, 2.45, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7,
                  3.8, 3.9, 4.0, 4.2, 4.5, 4.8, 5.0, 5.5, 6.0, 7.0, 8.0, 10.0, 13.0, 16.0, 20.0,
                  25.0, 30.0, 40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0, 250.0, 300.0,
                  400.0, 500.0, 650.0, 800.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0,
                  100000.0))
          .build();

  private static final String HOST = getHostName();

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return "unknown";
    }
  }

  public static void main(String[] args) throws Exception {
    configureLogging();
    if (qps <= 0) {
      throw new IllegalArgumentException("QPS must be > 0. Current value: " + qps);
    }
    System.out.println("Hello Prober!");
    System.out.println("------------------------------------------------------------------------");
    System.out.println("QPS: " + qps);
    System.out.println("Enable bypass: " + enableBypass);
    System.out.println("Enable grpc-gcp: " + enableGrpcGcp);
    System.out.println("Use OTel for Spanner tracing: " + useOtelForSpannerTracing);
    System.out.println("Enable Spanner API tracing: " + enableSpannerApiTracing);
    System.out.println("Enable Spanner extended tracing: " + enableSpannerExtendedTracing);
    System.out.println("Enable Spanner end-to-end tracing: " + enableSpannerEndToEndTracing);
    System.out.println("Enable debug logging: " + enableDebugLogging);
    System.out.println("Workload: " + workload);
    System.out.println("Endpoint: " + (endpoint.isEmpty() ? "<default>" : endpoint));
    System.out.println("OTel project: " + telemetryProjectId);
    System.out.println("Spanner project: " + spannerProjectId);
    System.out.println("Spanner instance: " + spannerInstanceId);
    System.out.println("Spanner database: " + spannerDatabaseId);
    System.out.println("Num rows: " + numRows);
    System.out.println("Max staleness (s): " + maxStalenessSeconds);
    System.out.println("YCSB table: " + ycsbTable);
    System.out.println("YCSB user id: " + ycsbUserId);
    System.out.println("YCSB key override: " + (ycsbKey.trim().isEmpty() ? "<none>" : ycsbKey));
    System.out.println("YCSB zero padding: " + ycsbZeroPadding);
    System.out.println("Service name: " + serviceName);
    System.out.println("------------------------------------------------------------------------");

    String host = endpoint;
    if (!host.isEmpty() && !host.startsWith("http")) {
      host = "http://" + host;
    }
    if (useOtelForSpannerTracing) {
      SpannerOptions.enableOpenTelemetryTraces();
    }

    SpannerOptions.Builder optionsBuilder =
        SpannerOptions.newBuilder()
            .setOpenTelemetry(openTelemetrySdk)
            .setEnableApiTracing(enableSpannerApiTracing)
            .setEnableExtendedTracing(enableSpannerExtendedTracing)
            .setEnableEndToEndTracing(enableSpannerEndToEndTracing);
    if (!enableGrpcGcp) {
      optionsBuilder.disableGrpcGcpExtension();
    }
    if (!host.isEmpty()) {
      optionsBuilder.setExperimentalHost(host);
      optionsBuilder.setCredentials(NoCredentials.getInstance());
      optionsBuilder.setChannelConfigurator(ManagedChannelBuilder::usePlaintext);
    }
    Spanner spanner = optionsBuilder.build().getService();

    DatabaseClient client =
        spanner.getDatabaseClient(
            DatabaseId.of(spannerProjectId, spannerInstanceId, spannerDatabaseId));
    Probe probe = createProbe(client);
    startProbe(probe);
  }

  private static void warmup(Probe probe) {
    for (int i = 0; i < DEFAULT_WARMUP_CYCLES; i++) {
      probe.probe();
    }
  }

  private static void updateProbeStats(long start) {
    float latency = (System.nanoTime() - start) / 1000000f;
    Attributes attributes = Attributes.of(AttributeKey.stringKey("method"), workload);
    requestCounter.add(1, attributes);
    latencyHistogram.record(latency, attributes);
  }

  static Probe createProbe(DatabaseClient client) {
    return switch (workload) {
      case "stale_read" -> new StaleReadProbe(client, numRows, 15);
      case "strong_read" -> new StrongReadProbe(client, numRows);
      case "read_write" -> new ReadWriteProbe(client, numRows, payloadSize);
      case "rr_occ_dml" ->
          new DmlProbe(client, numRows, payloadSize, /* repeatableReadOcc= */ true);
      case "write" -> new WriteProbe(client, numRows, payloadSize, true);
      case "write_no_rp" -> new WriteProbe(client, numRows, payloadSize, false);
      case "dml" -> new DmlProbe(client, numRows, payloadSize, /* repeatableReadOcc= */ false);
      case "blind_dml" -> new BlindDmlProbe(client, numRows, payloadSize);
      case "multi_blind_dml" ->
          new MultiBlindDmlProbe(client, numRows, payloadSize, /* numDmlStatements= */ 5);
      case "strong_query" -> new QueryProbe(client, numRows);
      case "stale_query" -> new QueryProbe(client, numRows, maxStalenessSeconds);
      case "multi_use_ro_query" -> new MultiUseReadOnlyQueryProbe(client, numRows);
      case "ycsb_fixed_read" ->
          new YcsbFixedReadProbe(client, ycsbTable, ycsbKey, ycsbUserId, ycsbZeroPadding);
      default -> {
        throw new IllegalArgumentException("Unsupported workload: " + workload);
      }
    };
  }

  private static void configureLogging() throws IOException {
    if (!enableDebugLogging) {
      return;
    }
    try (InputStream input =
        Main.class.getClassLoader().getResourceAsStream("logging.properties")) {
      if (input == null) {
        throw new IllegalStateException("Missing logging.properties resource on classpath.");
      }
      LogManager.getLogManager().readConfiguration(input);
      logger.info(
          "Loaded verbose JUL logging configuration from classpath resource logging.properties");
    }
  }

  static void startProbe(Probe probe) {
    warmup(probe);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);
    long periodMicros = Math.max(1L, Math.round(1_000_000d / qps));
    Object unused =
        scheduler.scheduleAtFixedRate(
            () -> {
              if (executor.getActiveCount() >= executor.getMaximumPoolSize()) {
                return;
              }
              executor.execute(
                  () -> {
                    Span span =
                        tracer
                            .spanBuilder("probe." + probe.getName())
                            .setAttribute("probe.type", probe.getName())
                            .setAttribute("probe.bypass_enabled", enableBypass)
                            .setAttribute("probe.host", HOST)
                            .startSpan();
                    long start = System.nanoTime();
                    try (Scope unused2 = span.makeCurrent()) {
                      probe.probe();
                    } catch (Throwable t) {
                      span.setStatus(StatusCode.ERROR, t.getMessage());
                      span.recordException(t);
                      t.printStackTrace(System.err);
                    } finally {
                      float latencyMs = (System.nanoTime() - start) / 1000000f;
                      span.setAttribute("probe.latency_ms", latencyMs);
                      span.end();
                      updateProbeStats(start);
                    }
                  });
            },
            0,
            periodMicros,
            MICROSECONDS);
  }

  public static OpenTelemetrySdk initializeOpenTelemetry() {
    Resource resource =
        Resource.getDefault()
            .merge(
                Resource.create(
                    Attributes.of(AttributeKey.stringKey("service.name"), serviceName)));

    MetricConfiguration metricConfig =
        MetricConfiguration.builder()
            .setProjectId(telemetryProjectId)
            .setPrefix("custom.googleapis.com/irahul")
            .build();
    MetricExporter metricExporter = GoogleCloudMetricExporter.createWithConfiguration(metricConfig);

    SdkMeterProvider sdkMeterProvider =
        SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(10))
                    .build())
            .build();

    // Trace exporter for bypass routing spans
    TraceConfiguration traceConfig =
        TraceConfiguration.builder().setProjectId(telemetryProjectId).build();
    SpanExporter traceExporter = TraceExporter.createWithConfiguration(traceConfig);

    SdkTracerProvider sdkTracerProvider =
        SdkTracerProvider.builder()
            .setResource(resource)
            .setSampler(Sampler.alwaysOn())
            .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
            .build();

    OpenTelemetrySdk openTelemetrySdk =
        OpenTelemetrySdk.builder()
            .setMeterProvider(sdkMeterProvider)
            .setTracerProvider(sdkTracerProvider)
            .buildAndRegisterGlobal();

    return openTelemetrySdk;
  }

  // --- Environment variable helpers ---

  private static String envStr(String key, String defaultValue) {
    String val = System.getenv(key);
    return val != null ? val : defaultValue;
  }

  private static String envNonBlankStr(String key, String defaultValue) {
    String val = System.getenv(key);
    if (val == null) {
      return defaultValue;
    }
    String trimmed = val.trim();
    return trimmed.isEmpty() ? defaultValue : trimmed;
  }

  private static int envInt(String key, int defaultValue) {
    String val = System.getenv(key);
    return val != null ? Integer.parseInt(val) : defaultValue;
  }

  private static long envLong(String key, long defaultValue) {
    String val = System.getenv(key);
    return val != null ? Long.parseLong(val) : defaultValue;
  }

  private static boolean envBool(String key, boolean defaultValue) {
    String val = System.getenv(key);
    return val != null ? Boolean.parseBoolean(val) : defaultValue;
  }

  private Main() {}
}
