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

import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import com.google.cloud.spanner.SpannerOptions;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.CodeSource;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import site.ycsb.Status;

final class TracingSupport {

  private static final String EXCEPTION_ATTRIBUTE_RESOLVER_CLASS =
      "io.opentelemetry.sdk.internal.ExceptionAttributeResolver";

  private static final String PROP_TRACE_ENABLED = "cloudspanner.tracing.enabled";
  private static final String ENV_TRACE_ENABLED = "YCSB_ENABLE_OTEL_TRACING";
  private static final String PROP_TRACE_PROJECT_ID = "cloudspanner.tracing.project";
  private static final String ENV_TRACE_PROJECT_ID = "OTEL_TRACE_PROJECT_ID";
  private static final String PROP_SERVICE_NAME = "cloudspanner.tracing.service";
  private static final String ENV_SERVICE_NAME = "OTEL_SERVICE_NAME";
  private static final String PROP_USE_OTEL_FOR_SPANNER =
      "cloudspanner.tracing.spannerotel.enabled";
  private static final String ENV_USE_OTEL_FOR_SPANNER = "SPANNER_USE_OPENTELEMETRY_TRACING";
  private static final String PROP_ENABLE_API_TRACING = "cloudspanner.tracing.api.enabled";
  private static final String ENV_ENABLE_API_TRACING = "SPANNER_ENABLE_API_TRACING";
  private static final String PROP_ENABLE_EXTENDED_TRACING =
      "cloudspanner.tracing.extended.enabled";
  private static final String ENV_ENABLE_EXTENDED_TRACING =
      "SPANNER_ENABLE_EXTENDED_TRACING";
  private static final String PROP_ENABLE_END_TO_END_TRACING =
      "cloudspanner.tracing.endtoend.enabled";
  private static final String ENV_ENABLE_END_TO_END_TRACING =
      "SPANNER_ENABLE_END_TO_END_TRACING";

  private static final String DEFAULT_TRACE_PROJECT_ID = "outbound-flight";
  private static final String DEFAULT_SERVICE_NAME = "irahul-ycsb-cloudspanner";

  private static final Logger LOGGER = Logger.getLogger(TracingSupport.class.getName());
  private static final Scope NOOP_SCOPE = () -> {};

  private static volatile OpenTelemetrySdk openTelemetrySdk;
  private static volatile Tracer tracer;
  private static volatile boolean enabled;
  private static volatile String configuredProjectId = "<disabled>";
  private static volatile String configuredServiceName = "<disabled>";

  private TracingSupport() {}

  static void configureSpannerOptions(SpannerOptions.Builder optionsBuilder, Properties properties) {
    if (!isEnabled(properties)) {
      enabled = false;
      return;
    }

    OpenTelemetrySdk sdk = getOrCreateSdk(properties);
    optionsBuilder.setOpenTelemetry(sdk);
    if (getBoolean(properties, PROP_USE_OTEL_FOR_SPANNER, ENV_USE_OTEL_FOR_SPANNER, true)) {
      SpannerOptions.enableOpenTelemetryTraces();
    }
    optionsBuilder.setEnableApiTracing(
        getBoolean(properties, PROP_ENABLE_API_TRACING, ENV_ENABLE_API_TRACING, true));
    optionsBuilder.setEnableExtendedTracing(
        getBoolean(
            properties, PROP_ENABLE_EXTENDED_TRACING, ENV_ENABLE_EXTENDED_TRACING, false));
    optionsBuilder.setEnableEndToEndTracing(
        getBoolean(
            properties, PROP_ENABLE_END_TO_END_TRACING, ENV_ENABLE_END_TO_END_TRACING, false));
  }

  static boolean isEnabled() {
    return enabled;
  }

  static String getConfiguredProjectId() {
    return configuredProjectId;
  }

  static String getConfiguredServiceName() {
    return configuredServiceName;
  }

  static Span startOperationSpan(String operation, String table) {
    if (!enabled || tracer == null) {
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
    span.setAttribute("ycsb.latency_ms", (System.nanoTime() - startNanos) / 1_000_000d);
    if (error != null) {
      span.setStatus(StatusCode.ERROR, error.getMessage());
      span.recordException(error);
    } else if (!status.isOk()) {
      span.setStatus(StatusCode.ERROR, status.getName());
    }
    span.end();
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
    enabled = true;
    configuredProjectId =
        getString(
            properties,
            PROP_TRACE_PROJECT_ID,
            ENV_TRACE_PROJECT_ID,
            DEFAULT_TRACE_PROJECT_ID);
    configuredServiceName =
        getString(properties, PROP_SERVICE_NAME, ENV_SERVICE_NAME, DEFAULT_SERVICE_NAME);

    Resource resource =
        Resource.getDefault()
            .merge(
                Resource.create(
                    Attributes.of(
                        AttributeKey.stringKey("service.name"), configuredServiceName)));

    TraceConfiguration traceConfig =
        TraceConfiguration.builder().setProjectId(configuredProjectId).build();
    SpanExporter traceExporter = TraceExporter.createWithConfiguration(traceConfig);

    logOtelDiagnostics("before-sdk-init");

    SdkTracerProvider sdkTracerProvider;
    try {
      sdkTracerProvider =
          SdkTracerProvider.builder()
              .setResource(resource)
              .setSampler(Sampler.alwaysOn())
              .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
              .build();
    } catch (RuntimeException e) {
      logOtelDiagnostics("sdk-init-failed");
      throw e;
    } catch (LinkageError e) {
      logOtelDiagnostics("sdk-init-linkage-failed");
      throw e;
    }

    openTelemetrySdk =
        OpenTelemetrySdk.builder().setTracerProvider(sdkTracerProvider).buildAndRegisterGlobal();
    tracer = openTelemetrySdk.getTracer("ycsb-cloudspanner");
    return openTelemetrySdk;
  }

  private static boolean isEnabled(Properties properties) {
    return getBoolean(properties, PROP_TRACE_ENABLED, ENV_TRACE_ENABLED, false);
  }

  private static boolean getBoolean(
      Properties properties, String propertyName, String envName, boolean defaultValue) {
    return Boolean.parseBoolean(
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

  private static void logOtelDiagnostics(String phase) {
    LOGGER.log(Level.SEVERE, "OTel diagnostics [{0}]: {1}", new Object[] {phase, otelDetails()});
  }

  private static String otelDetails() {
    return describeClass(OpenTelemetrySdk.class)
        + " | "
        + describeClass(SdkTracerProvider.class)
        + " | "
        + describeClass(EXCEPTION_ATTRIBUTE_RESOLVER_CLASS)
        + " | "
        + describeClass(TraceExporter.class);
  }

  private static String describeClass(Class<?> clazz) {
    Package classPackage = clazz.getPackage();
    String packageVersion =
        classPackage == null ? "<none>" : String.valueOf(classPackage.getImplementationVersion());
    URL resource = clazz.getResource(clazz.getSimpleName() + ".class");
    String codeSource = getCodeSource(clazz);
    String methods = "";
    if (EXCEPTION_ATTRIBUTE_RESOLVER_CLASS.equals(clazz.getName())) {
      methods = ", methods=" + Arrays.toString(getMethodNames(clazz));
      methods += ", hasGetDefault=" + hasNoArgMethod(clazz, "getDefault");
    }
    return clazz.getName()
        + "{implVersion="
        + packageVersion
        + ", codeSource="
        + codeSource
        + ", resource="
        + resource
        + methods
        + "}";
  }

  private static String describeClass(String className) {
    try {
      return describeClass(Class.forName(className));
    } catch (ClassNotFoundException e) {
      return className + "{missing}";
    } catch (LinkageError e) {
      return className + "{linkageError=" + e + "}";
    }
  }

  private static String getCodeSource(Class<?> clazz) {
    CodeSource codeSource = clazz.getProtectionDomain().getCodeSource();
    return codeSource == null ? "<none>" : String.valueOf(codeSource.getLocation());
  }

  private static String[] getMethodNames(Class<?> clazz) {
    Method[] methods = clazz.getDeclaredMethods();
    String[] names = new String[methods.length];
    for (int i = 0; i < methods.length; i++) {
      names[i] = methods[i].toString();
    }
    Arrays.sort(names);
    return names;
  }

  private static boolean hasNoArgMethod(Class<?> clazz, String methodName) {
    try {
      clazz.getDeclaredMethod(methodName);
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
