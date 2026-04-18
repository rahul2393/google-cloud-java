/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.cloudspanner;

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.spanner.SpannerOptions;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import site.ycsb.Status;

final class OpenTelemetrySupport {

  private static final Logger LOGGER = Logger.getLogger(OpenTelemetrySupport.class.getName());
  private static final Scope NOOP_SCOPE = new Scope() {
    @Override
    public void close() {}
  };

  private static final String PROP_PROJECT_ID = "cloudspanner.otel.project";
  private static final String ENV_PROJECT_ID = "OTEL_PROJECT_ID";
  private static final String PROP_SERVICE_NAME = "cloudspanner.otel.service";
  private static final String ENV_SERVICE_NAME = "OTEL_SERVICE_NAME";
  private static final String PROP_METRIC_PREFIX = "cloudspanner.otel.metric.prefix";
  private static final String ENV_METRIC_PREFIX = "OTEL_METRIC_PREFIX";
  private static final String PROP_CLIENT_NAME = "cloudspanner.otel.client.name";
  private static final String ENV_CLIENT_NAME = "YCSB_OTEL_CLIENT_NAME";
  private static final String ENV_HOSTNAME = "HOSTNAME";
  private static final String PROP_SERVICE_INSTANCE_ID = "cloudspanner.otel.service.instance.id";
  private static final String ENV_SERVICE_INSTANCE_ID = "OTEL_SERVICE_INSTANCE_ID";
  private static final String ENV_POD_NAME = "POD_NAME";
  private static final String ENV_POD_NAMESPACE = "POD_NAMESPACE";
  private static final String ENV_NODE_NAME = "NODE_NAME";
  private static final String PROP_METRIC_EXPORT_INTERVAL_SECONDS =
      "cloudspanner.otel.metric.export.interval.seconds";
  private static final String ENV_METRIC_EXPORT_INTERVAL_SECONDS =
      "OTEL_METRIC_EXPORT_INTERVAL_SECONDS";
  private static final String PROP_METRIC_EXPORT_DEBUG = "cloudspanner.otel.metric.export.debug";
  private static final String ENV_METRIC_EXPORT_DEBUG = "YCSB_OTEL_METRIC_EXPORT_DEBUG";
  private static final String PROP_EXPORT_BUILTIN_METRICS =
      "cloudspanner.metrics.export.builtin.to.otel";
  private static final String ENV_EXPORT_BUILTIN_METRICS =
      "YCSB_EXPORT_SPANNER_BUILTIN_METRICS_TO_OTEL";
  private static final String PROP_CLOUD_MONITORING_DEBUG_LOGGING =
      "cloudspanner.otel.cloud.monitoring.debug.logging";
  private static final String ENV_CLOUD_MONITORING_DEBUG_LOGGING =
      "YCSB_OTEL_CLOUD_MONITORING_DEBUG_LOGGING";
  private static final String PROP_TRACING_ENABLED = "cloudspanner.tracing.enabled";
  private static final String ENV_TRACING_ENABLED = "YCSB_ENABLE_OTEL_TRACING";
  private static final String PROP_USE_OTEL_FOR_SPANNER =
      "cloudspanner.tracing.spannerotel.enabled";
  private static final String ENV_USE_OTEL_FOR_SPANNER = "SPANNER_USE_OPENTELEMETRY_TRACING";
  private static final String PROP_ENABLE_API_TRACING = "cloudspanner.tracing.api.enabled";
  private static final String ENV_ENABLE_API_TRACING = "SPANNER_ENABLE_API_TRACING";
  private static final String PROP_ENABLE_EXTENDED_TRACING =
      "cloudspanner.tracing.extended.enabled";
  private static final String ENV_ENABLE_EXTENDED_TRACING = "SPANNER_ENABLE_EXTENDED_TRACING";
  private static final String PROP_ENABLE_END_TO_END_TRACING =
      "cloudspanner.tracing.endtoend.enabled";
  private static final String ENV_ENABLE_END_TO_END_TRACING =
      "SPANNER_ENABLE_END_TO_END_TRACING";

  private static final String DEFAULT_SERVICE_NAME = "ycsb-cloudspanner";
  private static final String DEFAULT_METRIC_PREFIX = "custom.googleapis.com/ycsb/grpc_gcp";
  private static final int DEFAULT_EXPORT_INTERVAL_SECONDS = 10;

  private static volatile OpenTelemetrySdk openTelemetrySdk;
  private static volatile Tracer tracer;
  private static volatile boolean metricsEnabled;
  private static volatile boolean tracingEnabled;
  private static volatile boolean exportBuiltInMetricsEnabled;
  private static volatile String configuredProjectId = "<disabled>";
  private static volatile String configuredServiceName = "<disabled>";
  private static volatile String configuredMetricPrefix = "<disabled>";
  private static volatile String configuredClientName = "<disabled>";

  private OpenTelemetrySupport() {}

  static void configureSpannerOptions(SpannerOptions.Builder optionsBuilder, Properties properties) {
    OpenTelemetrySdk sdk = getOrCreateSdk(properties);
    if (sdk == null) {
      metricsEnabled = false;
      tracingEnabled = false;
      exportBuiltInMetricsEnabled = false;
      return;
    }

    optionsBuilder.setOpenTelemetry(sdk);
    if (exportBuiltInMetricsEnabled) {
      optionsBuilder.setExportBuiltInMetricsToOpenTelemetry(true);
      optionsBuilder.setBuiltInMetricsClientName(configuredClientName);
    }
    if (tracingEnabled) {
      if (getBoolean(
          properties, PROP_USE_OTEL_FOR_SPANNER, ENV_USE_OTEL_FOR_SPANNER, true)) {
        SpannerOptions.enableOpenTelemetryTraces();
      }
      optionsBuilder.setEnableApiTracing(
          getBoolean(properties, PROP_ENABLE_API_TRACING, ENV_ENABLE_API_TRACING, true));
      optionsBuilder.setEnableExtendedTracing(
          getBoolean(
              properties, PROP_ENABLE_EXTENDED_TRACING, ENV_ENABLE_EXTENDED_TRACING, false));
      optionsBuilder.setEnableEndToEndTracing(
          getBoolean(
              properties,
              PROP_ENABLE_END_TO_END_TRACING,
              ENV_ENABLE_END_TO_END_TRACING,
              false));
    }
  }

  static Span startOperationSpan(String operation, String table) {
    if (!tracingEnabled || tracer == null) {
      return null;
    }
    return tracer
        .spanBuilder("ycsb." + operation)
        .setAttribute("db.system", "spanner")
        .setAttribute("db.operation", operation)
        .setAttribute("db.name", table)
        .startSpan();
  }

  static Scope makeCurrent(Span span) {
    return span == null ? NOOP_SCOPE : span.makeCurrent();
  }

  static void finishSpan(Span span, Status status, Throwable error, long startNanos) {
    if (span == null) {
      return;
    }
    span.setAttribute("ycsb.status", status.getName());
    span.setAttribute("ycsb.latency_ms", nanosToMillis(System.nanoTime() - startNanos));
    if (error != null) {
      span.setStatus(StatusCode.ERROR, error.getMessage());
      span.recordException(error);
    } else if (!status.isOk()) {
      span.setStatus(StatusCode.ERROR, status.getName());
    }
    span.end();
  }

  static boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  static boolean isTracingEnabled() {
    return tracingEnabled;
  }

  static boolean isExportBuiltInMetricsEnabled() {
    return exportBuiltInMetricsEnabled;
  }

  static String getConfiguredProjectId() {
    return configuredProjectId;
  }

  static String getConfiguredServiceName() {
    return configuredServiceName;
  }

  static String getConfiguredMetricPrefix() {
    return configuredMetricPrefix;
  }

  static String getConfiguredClientName() {
    return configuredClientName;
  }

  static void shutdown() {
    OpenTelemetrySdk sdk = openTelemetrySdk;
    if (sdk != null) {
      try {
        sdk.close();
      } catch (Exception e) {
        LOGGER.log(Level.FINE, "Error while closing OpenTelemetry SDK", e);
      }
    }
  }

  private static synchronized OpenTelemetrySdk getOrCreateSdk(Properties properties) {
    if (openTelemetrySdk != null) {
      return openTelemetrySdk;
    }

    configuredProjectId = getString(properties, PROP_PROJECT_ID, ENV_PROJECT_ID, "");
    String configuredService = getString(properties, PROP_SERVICE_NAME, ENV_SERVICE_NAME, "");
    if (configuredProjectId.isEmpty() || configuredService.isEmpty()) {
      return null;
    }

    configuredServiceName = configuredService;
    configuredMetricPrefix =
        getString(properties, PROP_METRIC_PREFIX, ENV_METRIC_PREFIX, DEFAULT_METRIC_PREFIX);
    configuredClientName = resolveClientName(properties, configuredServiceName);
    metricsEnabled = true;
    tracingEnabled = getBoolean(properties, PROP_TRACING_ENABLED, ENV_TRACING_ENABLED, true);
    exportBuiltInMetricsEnabled =
        getBoolean(
            properties,
            PROP_EXPORT_BUILTIN_METRICS,
            ENV_EXPORT_BUILTIN_METRICS,
            metricsEnabled);

    Resource resource = createResource(properties, configuredServiceName, configuredClientName);

    OpenTelemetrySdkBuilder sdkBuilder = OpenTelemetrySdk.builder();

    int exportIntervalSeconds =
        getInt(
            properties,
            PROP_METRIC_EXPORT_INTERVAL_SECONDS,
            ENV_METRIC_EXPORT_INTERVAL_SECONDS,
            DEFAULT_EXPORT_INTERVAL_SECONDS);
    maybeEnableCloudMonitoringDebugLogging(properties);
    MetricConfiguration metricConfiguration =
        MetricConfiguration.builder()
            .setProjectId(configuredProjectId)
            .setPrefix(configuredMetricPrefix)
            .build();
    MetricExporter metricExporter =
        new DiagnosticMetricExporter(
            GoogleCloudMetricExporter.createWithConfiguration(metricConfiguration),
            configuredProjectId,
            configuredMetricPrefix,
            getBoolean(properties, PROP_METRIC_EXPORT_DEBUG, ENV_METRIC_EXPORT_DEBUG, true));
    SdkMeterProviderBuilder meterProviderBuilder =
        SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(exportIntervalSeconds))
                    .build());
    if (exportBuiltInMetricsEnabled) {
      SpannerOptions.registerBuiltInMetricViewsForCustomExporter(meterProviderBuilder);
    }
    sdkBuilder.setMeterProvider(meterProviderBuilder.build());

    if (tracingEnabled) {
      TraceConfiguration traceConfiguration =
          TraceConfiguration.builder().setProjectId(configuredProjectId).build();
      SpanExporter traceExporter = TraceExporter.createWithConfiguration(traceConfiguration);
      SdkTracerProvider tracerProvider =
          SdkTracerProvider.builder()
              .setResource(resource)
              .setSampler(Sampler.alwaysOn())
              .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
              .build();
      sdkBuilder.setTracerProvider(tracerProvider);
    }

    openTelemetrySdk = sdkBuilder.buildAndRegisterGlobal();
    tracer = openTelemetrySdk.getTracer("ycsb-cloudspanner");
    LOGGER.log(
        Level.INFO,
        "Configured YCSB OTEL support: project={0}, service={1}, metricPrefix={2}, "
            + "clientName={3}, metricsEnabled={4}, tracingEnabled={5}, exportBuiltInMetrics={6}",
        new Object[] {
            configuredProjectId,
            configuredServiceName,
            configuredMetricPrefix,
            configuredClientName,
            metricsEnabled,
            tracingEnabled,
            exportBuiltInMetricsEnabled
        });
    return openTelemetrySdk;
  }

  private static boolean getBoolean(
      Properties properties, String propertyName, String envName, boolean defaultValue) {
    return Boolean.parseBoolean(
        getString(properties, propertyName, envName, String.valueOf(defaultValue)));
  }

  private static int getInt(
      Properties properties, String propertyName, String envName, int defaultValue) {
    return Integer.parseInt(
        getString(properties, propertyName, envName, String.valueOf(defaultValue)));
  }

  private static String getString(
      Properties properties, String propertyName, String envName, String defaultValue) {
    String value = properties.getProperty(propertyName);
    if (value != null) {
      return value;
    }
    value = System.getenv(envName);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  private static String resolveClientName(Properties properties, String serviceName) {
    String configured = getString(properties, PROP_CLIENT_NAME, ENV_CLIENT_NAME, "");
    if (!configured.isEmpty()) {
      return configured;
    }
    String hostname = System.getenv(ENV_HOSTNAME);
    if (hostname != null && !hostname.isEmpty()) {
      return serviceName + "/" + hostname;
    }
    return serviceName;
  }

  private static Resource createResource(
      Properties properties, String serviceName, String clientName) {
    AttributesBuilder attributes = Attributes.builder();
    attributes.put(AttributeKey.stringKey("service.name"), serviceName);

    String podName = firstNonEmpty(System.getenv(ENV_POD_NAME), System.getenv(ENV_HOSTNAME));
    String podNamespace = System.getenv(ENV_POD_NAMESPACE);
    String nodeName = System.getenv(ENV_NODE_NAME);
    String serviceInstanceId =
        firstNonEmpty(
            getString(properties, PROP_SERVICE_INSTANCE_ID, ENV_SERVICE_INSTANCE_ID, ""),
            podName,
            clientName);

    putIfPresent(attributes, "service.instance.id", serviceInstanceId);
    putIfPresent(attributes, "service.namespace", podNamespace);
    putIfPresent(attributes, "host.name", podName);
    putIfPresent(attributes, "host.id", serviceInstanceId);
    putIfPresent(attributes, "k8s.pod.name", podName);
    putIfPresent(attributes, "k8s.namespace.name", podNamespace);
    putIfPresent(attributes, "k8s.node.name", nodeName);

    return Resource.getDefault().merge(Resource.create(attributes.build()));
  }

  private static void putIfPresent(AttributesBuilder attributes, String key, String value) {
    if (value != null && !value.isEmpty()) {
      attributes.put(AttributeKey.stringKey(key), value);
    }
  }

  private static String firstNonEmpty(String... values) {
    for (String value : values) {
      if (value != null && !value.isEmpty()) {
        return value;
      }
    }
    return "";
  }

  private static double nanosToMillis(long nanos) {
    return nanos / 1_000_000d;
  }

  private static void maybeEnableCloudMonitoringDebugLogging(Properties properties) {
    if (!getBoolean(
        properties,
        PROP_CLOUD_MONITORING_DEBUG_LOGGING,
        ENV_CLOUD_MONITORING_DEBUG_LOGGING,
        false)) {
      return;
    }

    LogManager logManager = LogManager.getLogManager();
    Logger rootLogger = logManager.getLogger("");
    if (rootLogger != null) {
      rootLogger.setLevel(Level.FINEST);
      for (Handler handler : rootLogger.getHandlers()) {
        handler.setLevel(Level.FINEST);
      }
    }

    setLoggerLevel(logManager, OpenTelemetrySupport.class.getName(), Level.FINE);
    setLoggerLevel(logManager, "com.google.cloud.spanner", Level.FINE);
    setLoggerLevel(logManager, "com.google.cloud.spanner.BuiltInMetricsTracer", Level.FINE);
    setLoggerLevel(logManager, "com.google.cloud.opentelemetry.metric", Level.FINEST);
    setLoggerLevel(logManager, "com.google.cloud.monitoring.v3", Level.FINEST);
    setLoggerLevel(logManager, "com.google.api.gax", Level.FINEST);
    setLoggerLevel(logManager, "io.grpc", Level.FINE);

    LOGGER.log(
        Level.INFO,
        "Enabled JUL debug logging for Cloud Monitoring export. SLF4J logs will route through JUL"
            + " because slf4j-jdk14 is on the classpath.");
  }

  private static void setLoggerLevel(LogManager logManager, String loggerName, Level level) {
    Logger logger = logManager.getLogger(loggerName);
    if (logger == null) {
      logger = Logger.getLogger(loggerName);
    }
    logger.setLevel(level);
  }

  private static final class DiagnosticMetricExporter implements MetricExporter {
    private final MetricExporter delegate;
    private final String projectId;
    private final String metricPrefix;
    private final boolean logSuccessfulExports;
    private final AtomicLong exportAttempts = new AtomicLong();

    private DiagnosticMetricExporter(
        MetricExporter delegate,
        String projectId,
        String metricPrefix,
        boolean logSuccessfulExports) {
      this.delegate = delegate;
      this.projectId = projectId;
      this.metricPrefix = metricPrefix;
      this.logSuccessfulExports = logSuccessfulExports;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
      long exportNumber = exportAttempts.incrementAndGet();
      MetricExportSummary summary = MetricExportSummary.create(metrics);

      if (logSuccessfulExports) {
        LOGGER.log(
            Level.INFO,
            "OTEL metric export attempt #{0}: metricCount={1}, pointCount={2}, prefix={3},"
                + " project={4}, metrics={5}",
            new Object[] {
                exportNumber,
                summary.metricCount,
                summary.pointCount,
                metricPrefix,
                projectId,
                summary.metricNames
            });
      }

      CompletableResultCode result = delegate.export(metrics);
      Runnable completionLogger =
          new Runnable() {
            @Override
            public void run() {
              Level level = result.isSuccess() ? Level.INFO : Level.WARNING;
              if (result.isSuccess() && !logSuccessfulExports) {
                return;
              }
              LOGGER.log(
                  level,
                  "OTEL metric export attempt #{0} completed: success={1}, metricCount={2},"
                      + " pointCount={3}, prefix={4}, project={5}",
                  new Object[] {
                      exportNumber,
                      result.isSuccess(),
                      summary.metricCount,
                      summary.pointCount,
                      metricPrefix,
                      projectId
                  });
            }
          };
      result.whenComplete(completionLogger);
      return result;
    }

    @Override
    public CompletableResultCode flush() {
      return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
      return delegate.shutdown();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(
        io.opentelemetry.sdk.metrics.InstrumentType instrumentType) {
      return delegate.getAggregationTemporality(instrumentType);
    }
  }

  private static final class MetricExportSummary {
    private final int metricCount;
    private final long pointCount;
    private final String metricNames;

    private MetricExportSummary(int metricCount, long pointCount, String metricNames) {
      this.metricCount = metricCount;
      this.pointCount = pointCount;
      this.metricNames = metricNames;
    }

    private static MetricExportSummary create(Collection<MetricData> metrics) {
      StringBuilder names = new StringBuilder();
      long points = 0L;
      int metricCount = 0;
      for (MetricData metric : metrics) {
        if (metricCount > 0) {
          names.append(", ");
        }
        long metricPoints = metric.getData().getPoints().size();
        names.append(metric.getName()).append("[").append(metricPoints).append("]");
        points += metricPoints;
        metricCount++;
      }
      return new MetricExportSummary(metricCount, points, names.toString());
    }
  }
}
