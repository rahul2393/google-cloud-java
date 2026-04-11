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

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/** Periodically logs coarse routing outcome counters for bypass debugging. */
@InternalApi
final class RouteDecisionSummaryLogger {
  private static final Logger logger = Logger.getLogger(KeyAwareChannel.class.getName());
  private static final long DEFAULT_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(5);
  private static final int ADDRESS_SUMMARY_LIMIT = 5;

  private final long logIntervalNanos;
  private final LongSupplier nanoClock;
  private final AtomicLong nextLogNanos;
  private final AtomicLong totalRequests = new AtomicLong();
  private final AtomicLong routedRequests = new AtomicLong();
  private final AtomicLong defaultEndpointRequests = new AtomicLong();
  private final AtomicLong firstReplicaSelections = new AtomicLong();
  private final AtomicLong leaderSelections = new AtomicLong();
  private final AtomicLong leaderSkippedTransientFailure = new AtomicLong();
  private final AtomicLong leaderSkippedEndpointNotReady = new AtomicLong();
  private final AtomicLong defaultDueToNoHealthyEndpoint = new AtomicLong();
  private final AtomicLong defaultDueToCacheMiss = new AtomicLong();
  private final AtomicLong alternateReplicaAfterResourceExhausted = new AtomicLong();
  private final ConcurrentHashMap<String, AtomicLong> selectedEndpointCounts = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> resourceExhaustedExcludedEndpointCounts =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> transientFailureSkippedEndpointCounts =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> endpointNotReadySkippedEndpointCounts =
      new ConcurrentHashMap<>();

  RouteDecisionSummaryLogger() {
    this(DEFAULT_LOG_INTERVAL_NANOS, System::nanoTime);
  }

  @VisibleForTesting
  RouteDecisionSummaryLogger(long logIntervalNanos, LongSupplier nanoClock) {
    this.logIntervalNanos = logIntervalNanos;
    this.nanoClock = nanoClock;
    this.nextLogNanos = new AtomicLong(nanoClock.getAsLong() + logIntervalNanos);
  }

  void recordAttempt(
      boolean usedDefaultEndpoint,
      boolean hadResourceExhaustedExclusion,
      @Nullable String selectedEndpointAddress,
      @Nullable RouteSelectionDebugInfo debugInfo) {
    totalRequests.incrementAndGet();
    if (usedDefaultEndpoint) {
      defaultEndpointRequests.incrementAndGet();
    } else {
      routedRequests.incrementAndGet();
      if (selectedEndpointAddress != null && !selectedEndpointAddress.isEmpty()) {
        increment(selectedEndpointCounts, selectedEndpointAddress);
      }
    }

    if (debugInfo != null) {
      if (Boolean.TRUE.equals(debugInfo.getSelectedFirstReplica()) && !usedDefaultEndpoint) {
        firstReplicaSelections.incrementAndGet();
      }
      if (Boolean.TRUE.equals(debugInfo.getSelectedLeader()) && !usedDefaultEndpoint) {
        leaderSelections.incrementAndGet();
      }
      if (Boolean.TRUE.equals(debugInfo.getLeaderSkippedTransientFailure())) {
        leaderSkippedTransientFailure.incrementAndGet();
      }
      if (Boolean.TRUE.equals(debugInfo.getLeaderSkippedEndpointNotReady())) {
        leaderSkippedEndpointNotReady.incrementAndGet();
      }
      if (usedDefaultEndpoint) {
        if ("no_healthy_tablet_for_group".equals(debugInfo.getDefaultReasonCode())) {
          defaultDueToNoHealthyEndpoint.incrementAndGet();
        } else if ("range_cache_miss".equals(debugInfo.getDefaultReasonCode())) {
          defaultDueToCacheMiss.incrementAndGet();
        }
      }
    }

    if (hadResourceExhaustedExclusion && !usedDefaultEndpoint) {
      alternateReplicaAfterResourceExhausted.incrementAndGet();
    }
    maybeLog();
  }

  void recordResourceExhaustedExclusion(String address) {
    if (address == null || address.isEmpty()) {
      return;
    }
    increment(resourceExhaustedExcludedEndpointCounts, address);
  }

  void recordTransientFailureSkip(String address) {
    if (address == null || address.isEmpty()) {
      return;
    }
    increment(transientFailureSkippedEndpointCounts, address);
  }

  void recordEndpointNotReadySkip(String address) {
    if (address == null || address.isEmpty()) {
      return;
    }
    increment(endpointNotReadySkippedEndpointCounts, address);
  }

  private void maybeLog() {
    if (!logger.isLoggable(Level.FINE)) {
      return;
    }
    long now = nanoClock.getAsLong();
    long scheduled = nextLogNanos.get();
    if (now < scheduled) {
      return;
    }
    if (!nextLogNanos.compareAndSet(scheduled, now + logIntervalNanos)) {
      return;
    }
    SummarySnapshot snapshot = snapshotAndReset();
    if (snapshot.totalRequests == 0) {
      return;
    }
    logger.log(
        Level.FINE,
        "Bypass routing summary over last {0} ms: total={1}, routed={2}, default={3},"
            + " first_replica_selected={4}, leader_selected={5},"
            + " leader_skipped_transient_failure={6}, leader_skipped_not_ready={7},"
            + " default_due_to_no_healthy_endpoint={8}, default_due_to_cache_miss={9},"
            + " alternate_replica_after_resource_exhausted={10},"
            + " top_selected_endpoints={11}, top_resource_exhausted_exclusions={12},"
            + " top_transient_failure_skips={13}, top_not_ready_skips={14}",
        new Object[] {
          TimeUnit.NANOSECONDS.toMillis(logIntervalNanos),
          snapshot.totalRequests,
          snapshot.routedRequests,
          snapshot.defaultEndpointRequests,
          snapshot.firstReplicaSelections,
          snapshot.leaderSelections,
          snapshot.leaderSkippedTransientFailure,
          snapshot.leaderSkippedEndpointNotReady,
          snapshot.defaultDueToNoHealthyEndpoint,
          snapshot.defaultDueToCacheMiss,
          snapshot.alternateReplicaAfterResourceExhausted,
          snapshot.topSelectedEndpoints,
          snapshot.topResourceExhaustedExcludedEndpoints,
          snapshot.topTransientFailureSkippedEndpoints,
          snapshot.topEndpointNotReadySkippedEndpoints
        });
  }

  @VisibleForTesting
  SummarySnapshot snapshotAndReset() {
    return new SummarySnapshot(
        totalRequests.getAndSet(0),
        routedRequests.getAndSet(0),
        defaultEndpointRequests.getAndSet(0),
        firstReplicaSelections.getAndSet(0),
        leaderSelections.getAndSet(0),
        leaderSkippedTransientFailure.getAndSet(0),
        leaderSkippedEndpointNotReady.getAndSet(0),
        defaultDueToNoHealthyEndpoint.getAndSet(0),
        defaultDueToCacheMiss.getAndSet(0),
        alternateReplicaAfterResourceExhausted.getAndSet(0),
        drainTopCounts(selectedEndpointCounts),
        drainTopCounts(resourceExhaustedExcludedEndpointCounts),
        drainTopCounts(transientFailureSkippedEndpointCounts),
        drainTopCounts(endpointNotReadySkippedEndpointCounts));
  }

  private static void increment(ConcurrentHashMap<String, AtomicLong> counts, String address) {
    counts.computeIfAbsent(address, ignored -> new AtomicLong()).incrementAndGet();
  }

  private static String drainTopCounts(ConcurrentHashMap<String, AtomicLong> counts) {
    List<Map.Entry<String, Long>> entries = new ArrayList<>();
    for (Map.Entry<String, AtomicLong> entry : counts.entrySet()) {
      long value = entry.getValue().getAndSet(0);
      if (value > 0) {
        entries.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), value));
      }
    }
    if (entries.isEmpty()) {
      return "[]";
    }
    entries.sort(Comparator.<Map.Entry<String, Long>>comparingLong(Map.Entry::getValue).reversed());
    StringBuilder builder = new StringBuilder("[");
    for (int i = 0; i < Math.min(entries.size(), ADDRESS_SUMMARY_LIMIT); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      Map.Entry<String, Long> entry = entries.get(i);
      builder.append(entry.getKey()).append('=').append(entry.getValue());
    }
    builder.append(']');
    return builder.toString();
  }

  @VisibleForTesting
  static final class SummarySnapshot {
    final long totalRequests;
    final long routedRequests;
    final long defaultEndpointRequests;
    final long firstReplicaSelections;
    final long leaderSelections;
    final long leaderSkippedTransientFailure;
    final long leaderSkippedEndpointNotReady;
    final long defaultDueToNoHealthyEndpoint;
    final long defaultDueToCacheMiss;
    final long alternateReplicaAfterResourceExhausted;
    final String topSelectedEndpoints;
    final String topResourceExhaustedExcludedEndpoints;
    final String topTransientFailureSkippedEndpoints;
    final String topEndpointNotReadySkippedEndpoints;

    private SummarySnapshot(
        long totalRequests,
        long routedRequests,
        long defaultEndpointRequests,
        long firstReplicaSelections,
        long leaderSelections,
        long leaderSkippedTransientFailure,
        long leaderSkippedEndpointNotReady,
        long defaultDueToNoHealthyEndpoint,
        long defaultDueToCacheMiss,
        long alternateReplicaAfterResourceExhausted,
        String topSelectedEndpoints,
        String topResourceExhaustedExcludedEndpoints,
        String topTransientFailureSkippedEndpoints,
        String topEndpointNotReadySkippedEndpoints) {
      this.totalRequests = totalRequests;
      this.routedRequests = routedRequests;
      this.defaultEndpointRequests = defaultEndpointRequests;
      this.firstReplicaSelections = firstReplicaSelections;
      this.leaderSelections = leaderSelections;
      this.leaderSkippedTransientFailure = leaderSkippedTransientFailure;
      this.leaderSkippedEndpointNotReady = leaderSkippedEndpointNotReady;
      this.defaultDueToNoHealthyEndpoint = defaultDueToNoHealthyEndpoint;
      this.defaultDueToCacheMiss = defaultDueToCacheMiss;
      this.alternateReplicaAfterResourceExhausted = alternateReplicaAfterResourceExhausted;
      this.topSelectedEndpoints = topSelectedEndpoints;
      this.topResourceExhaustedExcludedEndpoints = topResourceExhaustedExcludedEndpoints;
      this.topTransientFailureSkippedEndpoints = topTransientFailureSkippedEndpoints;
      this.topEndpointNotReadySkippedEndpoints = topEndpointNotReadySkippedEndpoints;
    }
  }
}
