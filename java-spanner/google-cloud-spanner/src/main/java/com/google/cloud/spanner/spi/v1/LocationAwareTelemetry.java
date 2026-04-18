/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.spi.v1;

import com.google.api.gax.core.GaxProperties;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

final class LocationAwareTelemetry {
  private static final String INSTRUMENTATION_SCOPE = "spanner-java-location-aware";
  private static final AttributeKey<String> STATE_KEY = AttributeKey.stringKey("state");
  private static final AttributeKey<String> METHOD_KEY = AttributeKey.stringKey("method");
  private static final AttributeKey<String> TARGET_ENDPOINT_KEY =
      AttributeKey.stringKey("target_endpoint");
  private static final AttributeKey<String> REASON_KEY = AttributeKey.stringKey("reason");
  private static final AttributeKey<String> DECISION_KEY = AttributeKey.stringKey("decision");
  private static final Set<EndpointStateProvider> PROVIDERS = ConcurrentHashMap.newKeySet();

  private static final Meter METER =
      GlobalOpenTelemetry.get()
          .meterBuilder(INSTRUMENTATION_SCOPE)
          .setInstrumentationVersion(GaxProperties.getLibraryVersion(LocationAwareTelemetry.class))
          .build();
  private static final LongCounter CACHE_UPDATE_COUNTER =
      METER
          .counterBuilder("location_aware.cache_update_count")
          .setDescription("Number of location-aware cache updates received from routed requests.")
          .setUnit("1")
          .build();
  private static final DoubleHistogram CACHE_UPDATE_PROCESSING_LATENCY =
      METER
          .histogramBuilder("location_aware.cache_update_processing_latency")
          .setDescription("Time spent processing location-aware cache updates.")
          .setUnit("ms")
          .build();
  private static final DoubleHistogram CACHE_UPDATE_QUEUE_LATENCY =
      METER
          .histogramBuilder("location_aware.cache_update_queue_latency")
          .setDescription("Time a location-aware cache update spent waiting in the async queue.")
          .setUnit("ms")
          .build();
  private static final LongCounter ENDPOINT_EVICTION_COUNTER =
      METER
          .counterBuilder("location_aware.endpoint_eviction_count")
          .setDescription("Number of routed replica endpoints evicted by the lifecycle manager.")
          .setUnit("1")
          .build();
  private static final LongCounter ENDPOINT_SKIP_COUNTER =
      METER
          .counterBuilder("location_aware.endpoint_skip_count")
          .setDescription("Number of times location-aware routing skipped an endpoint.")
          .setUnit("1")
          .build();
  private static final LongCounter ROUTING_DECISION_COUNTER =
      METER
          .counterBuilder("location_aware.routing_decision_count")
          .setDescription(
              "Final location-aware routing decision, including default-host fallback reasons.")
          .setUnit("1")
          .build();

  static {
    METER
        .gaugeBuilder("location_aware.endpoint_state")
        .ofLongs()
        .setDescription("Current location-aware endpoint state by endpoint.")
        .setUnit("1")
        .buildWithCallback(
            measurement ->
                mergeEndpointStates()
                    .forEach(
                        (address, states) -> {
                          if (address == null || address.isEmpty()) {
                            return;
                          }
                          for (String state : states) {
                            measurement.record(
                                1L,
                                Attributes.builder()
                                    .put(TARGET_ENDPOINT_KEY, address)
                                    .put(STATE_KEY, state)
                                    .build());
                          }
                        }));
    METER
        .gaugeBuilder("location_aware.endpoint_state_count")
        .ofLongs()
        .setDescription("Current count of location-aware endpoint states by endpoint.")
        .setUnit("1")
        .buildWithCallback(
            measurement -> {
              mergeEndpointStates()
                  .forEach(
                      (address, states) -> {
                        if (address == null || address.isEmpty()) {
                          return;
                        }
                        for (String state : states) {
                          measurement.record(
                              1L,
                              Attributes.builder()
                                  .put(TARGET_ENDPOINT_KEY, address)
                                  .put(STATE_KEY, state)
                                  .build());
                        }
                      });
            });
  }

  private LocationAwareTelemetry() {}

  interface EndpointStateProvider {
    Map<String, Set<String>> snapshotEndpointStates();
  }

  static void registerEndpointStateProvider(EndpointStateProvider provider) {
    if (provider != null) {
      PROVIDERS.add(provider);
    }
  }

  static void unregisterEndpointStateProvider(EndpointStateProvider provider) {
    if (provider != null) {
      PROVIDERS.remove(provider);
    }
  }

  static void recordCacheUpdateReceived(@Nullable String targetEndpoint, @Nullable String method) {
    CACHE_UPDATE_COUNTER.add(1L, requestAttributes(targetEndpoint, method));
  }

  static void recordCacheUpdateProcessed(
      @Nullable String targetEndpoint,
      @Nullable String method,
      long queuedAtNanos,
      long processingStartedAtNanos,
      long processingCompletedAtNanos,
      @Nullable Span span) {
    double queueMs = nanosToMillis(processingStartedAtNanos - queuedAtNanos);
    double processingMs = nanosToMillis(processingCompletedAtNanos - processingStartedAtNanos);
    Attributes attributes = requestAttributes(targetEndpoint, method);
    CACHE_UPDATE_QUEUE_LATENCY.record(queueMs, attributes);
    CACHE_UPDATE_PROCESSING_LATENCY.record(processingMs, attributes);
    if (span != null && span.getSpanContext().isValid()) {
      span.addEvent(
          "spanner.cache_update.processed",
          Attributes.builder()
              .put("spanner.cache_update.queue_ms", queueMs)
              .put("spanner.cache_update.processing_ms", processingMs)
              .put("spanner.route.method", method == null ? "unknown" : method)
              .put("spanner.target", targetEndpoint == null ? "unknown" : targetEndpoint)
              .build());
    }
  }

  static void recordEndpointEviction(String reason, @Nullable String address) {
    io.opentelemetry.api.common.AttributesBuilder attributesBuilder = Attributes.builder();
    attributesBuilder.put(REASON_KEY, reason == null ? "unknown" : reason);
    if (address != null && !address.isEmpty()) {
      attributesBuilder.put(TARGET_ENDPOINT_KEY, address);
    }
    ENDPOINT_EVICTION_COUNTER.add(1L, attributesBuilder.build());
  }

  static void recordEndpointSkipped(
      @Nullable String targetEndpoint, @Nullable String method, String reason) {
    io.opentelemetry.api.common.AttributesBuilder attributesBuilder = Attributes.builder();
    attributesBuilder.put(REASON_KEY, reason == null ? "unknown" : reason);
    if (method != null && !method.isEmpty()) {
      attributesBuilder.put(METHOD_KEY, method);
    }
    if (targetEndpoint != null && !targetEndpoint.isEmpty()) {
      attributesBuilder.put(TARGET_ENDPOINT_KEY, targetEndpoint);
    }
    ENDPOINT_SKIP_COUNTER.add(1L, attributesBuilder.build());
  }

  static void recordRoutingDecision(
      String decision, String reason, @Nullable String targetEndpoint, @Nullable String method) {
    io.opentelemetry.api.common.AttributesBuilder attributesBuilder = Attributes.builder();
    attributesBuilder.put(DECISION_KEY, decision == null ? "unknown" : decision);
    attributesBuilder.put(REASON_KEY, reason == null ? "unknown" : reason);
    if (method != null && !method.isEmpty()) {
      attributesBuilder.put(METHOD_KEY, method);
    }
    if (targetEndpoint != null && !targetEndpoint.isEmpty()) {
      attributesBuilder.put(TARGET_ENDPOINT_KEY, targetEndpoint);
    }
    ROUTING_DECISION_COUNTER.add(1L, attributesBuilder.build());
  }

  private static Attributes requestAttributes(
      @Nullable String targetEndpoint, @Nullable String method) {
    io.opentelemetry.api.common.AttributesBuilder attributesBuilder = Attributes.builder();
    if (method != null && !method.isEmpty()) {
      attributesBuilder.put(METHOD_KEY, method);
    }
    if (targetEndpoint != null && !targetEndpoint.isEmpty()) {
      attributesBuilder.put(TARGET_ENDPOINT_KEY, targetEndpoint);
    }
    return attributesBuilder.build();
  }

  private static double nanosToMillis(long nanos) {
    return nanos / 1_000_000d;
  }

  private static Map<String, Set<String>> mergeEndpointStates() {
    Map<String, Set<String>> mergedStates = new HashMap<>();
    for (EndpointStateProvider provider : PROVIDERS) {
      provider
          .snapshotEndpointStates()
          .forEach(
              (address, states) -> {
                if (address == null || address.isEmpty() || states == null || states.isEmpty()) {
                  return;
                }
                mergedStates.computeIfAbsent(address, ignored -> new HashSet<>()).addAll(states);
              });
    }
    return mergedStates;
  }
}
