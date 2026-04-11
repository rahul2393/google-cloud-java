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
import com.google.cloud.spanner.SpannerOptions;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

final class MetricsSupport {

  private static final String PROP_METRICS_ENABLED = "cloudspanner.grpcgcp.metrics.enabled";
  private static final String ENV_METRICS_ENABLED = "YCSB_ENABLE_GRPC_GCP_OTEL_METRICS";
  private static final String PROP_METRICS_PROJECT_ID = "cloudspanner.grpcgcp.metrics.project";
  private static final String ENV_METRICS_PROJECT_ID = "OTEL_METRIC_PROJECT_ID";
  private static final String PROP_METRICS_PREFIX = "cloudspanner.grpcgcp.metrics.prefix";
  private static final String ENV_METRICS_PREFIX = "OTEL_METRIC_PREFIX";
  private static final String PROP_SERVICE_NAME = "cloudspanner.grpcgcp.metrics.service";
  private static final String ENV_SERVICE_NAME = "OTEL_SERVICE_NAME";
  private static final String PROP_EXPORT_INTERVAL_SECONDS =
      "cloudspanner.grpcgcp.metrics.export.interval.seconds";
  private static final String ENV_EXPORT_INTERVAL_SECONDS =
      "OTEL_METRIC_EXPORT_INTERVAL_SECONDS";

  private static final String DEFAULT_SERVICE_NAME = "irahul-ycsb-grpc-gcp";
  private static final String DEFAULT_METRICS_PREFIX = "custom.googleapis.com/ycsb/grpc_gcp";
  private static final int DEFAULT_EXPORT_INTERVAL_SECONDS = 10;

  private static final Logger LOGGER = Logger.getLogger(MetricsSupport.class.getName());

  private static volatile OpenTelemetrySdk openTelemetrySdk;
  private static volatile boolean enabled;
  private static volatile String configuredProjectId = "<disabled>";
  private static volatile String configuredMetricPrefix = "<disabled>";
  private static volatile String configuredServiceName = "<disabled>";

  private MetricsSupport() {}

  static void configureSpannerOptions(SpannerOptions.Builder optionsBuilder, Properties properties) {
    if (!isEnabled(properties)) {
      enabled = false;
      return;
    }

    OpenTelemetrySdk sdk = getOrCreateSdk(properties);
    if (sdk == null) {
      enabled = false;
      return;
    }

    optionsBuilder.setOpenTelemetry(sdk);
    enabled = true;
  }

  static boolean isEnabled() {
    return enabled;
  }

  static String getConfiguredProjectId() {
    return configuredProjectId;
  }

  static String getConfiguredMetricPrefix() {
    return configuredMetricPrefix;
  }

  static String getConfiguredServiceName() {
    return configuredServiceName;
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

    configuredProjectId = getString(properties, PROP_METRICS_PROJECT_ID, ENV_METRICS_PROJECT_ID, "");
    if (configuredProjectId.isEmpty()) {
      LOGGER.warning(
          "grpc-gcp OTEL metrics requested, but no metrics project ID was configured. "
              + "Set OTEL_METRIC_PROJECT_ID or cloudspanner.grpcgcp.metrics.project.");
      return null;
    }

    configuredMetricPrefix =
        getString(properties, PROP_METRICS_PREFIX, ENV_METRICS_PREFIX, DEFAULT_METRICS_PREFIX);
    configuredServiceName =
        getString(properties, PROP_SERVICE_NAME, ENV_SERVICE_NAME, DEFAULT_SERVICE_NAME);
    int exportIntervalSeconds =
        getInt(
            properties,
            PROP_EXPORT_INTERVAL_SECONDS,
            ENV_EXPORT_INTERVAL_SECONDS,
            DEFAULT_EXPORT_INTERVAL_SECONDS);

    Resource resource =
        Resource.getDefault()
            .merge(
                Resource.create(
                    Attributes.of(
                        AttributeKey.stringKey("service.name"), configuredServiceName)));

    MetricConfiguration metricConfig =
        MetricConfiguration.builder()
            .setProjectId(configuredProjectId)
            .setPrefix(configuredMetricPrefix)
            .build();
    MetricExporter metricExporter = GoogleCloudMetricExporter.createWithConfiguration(metricConfig);

    SdkMeterProvider sdkMeterProvider =
        SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(exportIntervalSeconds))
                    .build())
            .build();

    openTelemetrySdk = OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();
    return openTelemetrySdk;
  }

  private static boolean isEnabled(Properties properties) {
    return Boolean.parseBoolean(
        getString(properties, PROP_METRICS_ENABLED, ENV_METRICS_ENABLED, "false"));
  }

  private static int getInt(
      Properties properties, String propertyName, String envName, int defaultValue) {
    return Integer.parseInt(getString(properties, propertyName, envName, String.valueOf(defaultValue)));
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
}
