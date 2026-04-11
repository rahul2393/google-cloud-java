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
  private static final long DEFAULT_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
  private static final int ADDRESS_SUMMARY_LIMIT = 5;

  private final long logIntervalNanos;
  private final LongSupplier nanoClock;
  private final AtomicLong nextLogNanos;
  private final AtomicLong totalRequests = new AtomicLong();
  private final AtomicLong routedRequests = new AtomicLong();
  private final AtomicLong defaultEndpointRequests = new AtomicLong();
  private final AtomicLong firstReplicaSelections = new AtomicLong();
  private final AtomicLong leaderSelections = new AtomicLong();
  private final AtomicLong leaderFirstSelections = new AtomicLong();
  private final AtomicLong leaderNonFirstSelections = new AtomicLong();
  private final AtomicLong nonLeaderFirstSelections = new AtomicLong();
  private final AtomicLong nonLeaderNonFirstSelections = new AtomicLong();
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
  private final ConcurrentHashMap<String, MethodSummaryCounters> methodSummaries =
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
      @Nullable String routeMethod,
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
      if (!usedDefaultEndpoint) {
        boolean selectedFirstReplica = Boolean.TRUE.equals(debugInfo.getSelectedFirstReplica());
        boolean selectedLeader = Boolean.TRUE.equals(debugInfo.getSelectedLeader());
        if (selectedFirstReplica && selectedLeader) {
          leaderFirstSelections.incrementAndGet();
        } else if (!selectedFirstReplica && selectedLeader) {
          leaderNonFirstSelections.incrementAndGet();
        } else if (selectedFirstReplica) {
          nonLeaderFirstSelections.incrementAndGet();
        } else {
          nonLeaderNonFirstSelections.incrementAndGet();
        }
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
    if (routeMethod != null && !routeMethod.isEmpty()) {
      methodSummaries
          .computeIfAbsent(routeMethod, ignored -> new MethodSummaryCounters())
          .recordAttempt(
              usedDefaultEndpoint,
              hadResourceExhaustedExclusion,
              selectedEndpointAddress,
              debugInfo);
    }
    maybeLog();
  }

  void recordResourceExhaustedExclusion(@Nullable String routeMethod, String address) {
    if (address == null || address.isEmpty()) {
      return;
    }
    increment(resourceExhaustedExcludedEndpointCounts, address);
    if (routeMethod != null && !routeMethod.isEmpty()) {
      methodSummaries
          .computeIfAbsent(routeMethod, ignored -> new MethodSummaryCounters())
          .recordResourceExhaustedExclusion(address);
    }
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
            + " leader_first_selected={6}, leader_non_first_selected={7},"
            + " non_leader_first_selected={8}, non_leader_non_first_selected={9},"
            + " leader_skipped_transient_failure={10}, leader_skipped_not_ready={11},"
            + " default_due_to_no_healthy_endpoint={12}, default_due_to_cache_miss={13},"
            + " alternate_replica_after_resource_exhausted={14},"
            + " top_selected_endpoints={15}, top_resource_exhausted_exclusions={16},"
            + " top_transient_failure_skips={17}, top_not_ready_skips={18}",
        new Object[] {
          TimeUnit.NANOSECONDS.toMillis(logIntervalNanos),
          snapshot.totalRequests,
          snapshot.routedRequests,
          snapshot.defaultEndpointRequests,
          snapshot.firstReplicaSelections,
          snapshot.leaderSelections,
          snapshot.leaderFirstSelections,
          snapshot.leaderNonFirstSelections,
          snapshot.nonLeaderFirstSelections,
          snapshot.nonLeaderNonFirstSelections,
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
    for (MethodSummarySnapshot methodSnapshot : drainMethodSnapshots()) {
      if (methodSnapshot.totalRequests == 0) {
        continue;
      }
      logger.log(
          Level.FINE,
          "Bypass routing summary for method={0} over last {1} ms: total={2}, routed={3},"
              + " default={4}, first_replica_selected={5}, leader_selected={6},"
              + " leader_first_selected={7}, leader_non_first_selected={8},"
              + " non_leader_first_selected={9}, non_leader_non_first_selected={10},"
              + " default_due_to_no_healthy_endpoint={11}, default_due_to_cache_miss={12},"
              + " alternate_replica_after_resource_exhausted={13},"
              + " top_selected_endpoints={14}, top_resource_exhausted_exclusions={15}",
          new Object[] {
            methodSnapshot.routeMethod,
            TimeUnit.NANOSECONDS.toMillis(logIntervalNanos),
            methodSnapshot.totalRequests,
            methodSnapshot.routedRequests,
            methodSnapshot.defaultEndpointRequests,
            methodSnapshot.firstReplicaSelections,
            methodSnapshot.leaderSelections,
            methodSnapshot.leaderFirstSelections,
            methodSnapshot.leaderNonFirstSelections,
            methodSnapshot.nonLeaderFirstSelections,
            methodSnapshot.nonLeaderNonFirstSelections,
            methodSnapshot.defaultDueToNoHealthyEndpoint,
            methodSnapshot.defaultDueToCacheMiss,
            methodSnapshot.alternateReplicaAfterResourceExhausted,
            methodSnapshot.topSelectedEndpoints,
            methodSnapshot.topResourceExhaustedExcludedEndpoints
          });
    }
  }

  @VisibleForTesting
  SummarySnapshot snapshotAndReset() {
    return new SummarySnapshot(
        totalRequests.getAndSet(0),
        routedRequests.getAndSet(0),
        defaultEndpointRequests.getAndSet(0),
        firstReplicaSelections.getAndSet(0),
        leaderSelections.getAndSet(0),
        leaderFirstSelections.getAndSet(0),
        leaderNonFirstSelections.getAndSet(0),
        nonLeaderFirstSelections.getAndSet(0),
        nonLeaderNonFirstSelections.getAndSet(0),
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

  private List<MethodSummarySnapshot> drainMethodSnapshots() {
    List<MethodSummarySnapshot> snapshots = new ArrayList<>();
    for (Map.Entry<String, MethodSummaryCounters> entry : methodSummaries.entrySet()) {
      MethodSummarySnapshot snapshot = entry.getValue().snapshotAndReset(entry.getKey());
      if (snapshot.totalRequests == 0
          && "[]".equals(snapshot.topSelectedEndpoints)
          && "[]".equals(snapshot.topResourceExhaustedExcludedEndpoints)) {
        continue;
      }
      snapshots.add(snapshot);
    }
    snapshots.sort(Comparator.comparing(snapshot -> snapshot.routeMethod));
    return snapshots;
  }

  @VisibleForTesting
  static final class SummarySnapshot {
    final long totalRequests;
    final long routedRequests;
    final long defaultEndpointRequests;
    final long firstReplicaSelections;
    final long leaderSelections;
    final long leaderFirstSelections;
    final long leaderNonFirstSelections;
    final long nonLeaderFirstSelections;
    final long nonLeaderNonFirstSelections;
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
        long leaderFirstSelections,
        long leaderNonFirstSelections,
        long nonLeaderFirstSelections,
        long nonLeaderNonFirstSelections,
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
      this.leaderFirstSelections = leaderFirstSelections;
      this.leaderNonFirstSelections = leaderNonFirstSelections;
      this.nonLeaderFirstSelections = nonLeaderFirstSelections;
      this.nonLeaderNonFirstSelections = nonLeaderNonFirstSelections;
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

  private static final class MethodSummaryCounters {
    private final AtomicLong totalRequests = new AtomicLong();
    private final AtomicLong routedRequests = new AtomicLong();
    private final AtomicLong defaultEndpointRequests = new AtomicLong();
    private final AtomicLong firstReplicaSelections = new AtomicLong();
    private final AtomicLong leaderSelections = new AtomicLong();
    private final AtomicLong leaderFirstSelections = new AtomicLong();
    private final AtomicLong leaderNonFirstSelections = new AtomicLong();
    private final AtomicLong nonLeaderFirstSelections = new AtomicLong();
    private final AtomicLong nonLeaderNonFirstSelections = new AtomicLong();
    private final AtomicLong defaultDueToNoHealthyEndpoint = new AtomicLong();
    private final AtomicLong defaultDueToCacheMiss = new AtomicLong();
    private final AtomicLong alternateReplicaAfterResourceExhausted = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> selectedEndpointCounts =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> resourceExhaustedExcludedEndpointCounts =
        new ConcurrentHashMap<>();

    private void recordAttempt(
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
        boolean selectedFirstReplica = Boolean.TRUE.equals(debugInfo.getSelectedFirstReplica());
        boolean selectedLeader = Boolean.TRUE.equals(debugInfo.getSelectedLeader());
        if (selectedFirstReplica && !usedDefaultEndpoint) {
          firstReplicaSelections.incrementAndGet();
        }
        if (selectedLeader && !usedDefaultEndpoint) {
          leaderSelections.incrementAndGet();
        }
        if (!usedDefaultEndpoint) {
          if (selectedFirstReplica && selectedLeader) {
            leaderFirstSelections.incrementAndGet();
          } else if (selectedLeader) {
            leaderNonFirstSelections.incrementAndGet();
          } else if (selectedFirstReplica) {
            nonLeaderFirstSelections.incrementAndGet();
          } else {
            nonLeaderNonFirstSelections.incrementAndGet();
          }
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
    }

    private void recordResourceExhaustedExclusion(String address) {
      increment(resourceExhaustedExcludedEndpointCounts, address);
    }

    private MethodSummarySnapshot snapshotAndReset(String routeMethod) {
      return new MethodSummarySnapshot(
          routeMethod,
          totalRequests.getAndSet(0),
          routedRequests.getAndSet(0),
          defaultEndpointRequests.getAndSet(0),
          firstReplicaSelections.getAndSet(0),
          leaderSelections.getAndSet(0),
          leaderFirstSelections.getAndSet(0),
          leaderNonFirstSelections.getAndSet(0),
          nonLeaderFirstSelections.getAndSet(0),
          nonLeaderNonFirstSelections.getAndSet(0),
          defaultDueToNoHealthyEndpoint.getAndSet(0),
          defaultDueToCacheMiss.getAndSet(0),
          alternateReplicaAfterResourceExhausted.getAndSet(0),
          drainTopCounts(selectedEndpointCounts),
          drainTopCounts(resourceExhaustedExcludedEndpointCounts));
    }
  }

  private static final class MethodSummarySnapshot {
    private final String routeMethod;
    private final long totalRequests;
    private final long routedRequests;
    private final long defaultEndpointRequests;
    private final long firstReplicaSelections;
    private final long leaderSelections;
    private final long leaderFirstSelections;
    private final long leaderNonFirstSelections;
    private final long nonLeaderFirstSelections;
    private final long nonLeaderNonFirstSelections;
    private final long defaultDueToNoHealthyEndpoint;
    private final long defaultDueToCacheMiss;
    private final long alternateReplicaAfterResourceExhausted;
    private final String topSelectedEndpoints;
    private final String topResourceExhaustedExcludedEndpoints;

    private MethodSummarySnapshot(
        String routeMethod,
        long totalRequests,
        long routedRequests,
        long defaultEndpointRequests,
        long firstReplicaSelections,
        long leaderSelections,
        long leaderFirstSelections,
        long leaderNonFirstSelections,
        long nonLeaderFirstSelections,
        long nonLeaderNonFirstSelections,
        long defaultDueToNoHealthyEndpoint,
        long defaultDueToCacheMiss,
        long alternateReplicaAfterResourceExhausted,
        String topSelectedEndpoints,
        String topResourceExhaustedExcludedEndpoints) {
      this.routeMethod = routeMethod;
      this.totalRequests = totalRequests;
      this.routedRequests = routedRequests;
      this.defaultEndpointRequests = defaultEndpointRequests;
      this.firstReplicaSelections = firstReplicaSelections;
      this.leaderSelections = leaderSelections;
      this.leaderFirstSelections = leaderFirstSelections;
      this.leaderNonFirstSelections = leaderNonFirstSelections;
      this.nonLeaderFirstSelections = nonLeaderFirstSelections;
      this.nonLeaderNonFirstSelections = nonLeaderNonFirstSelections;
      this.defaultDueToNoHealthyEndpoint = defaultDueToNoHealthyEndpoint;
      this.defaultDueToCacheMiss = defaultDueToCacheMiss;
      this.alternateReplicaAfterResourceExhausted = alternateReplicaAfterResourceExhausted;
      this.topSelectedEndpoints = topSelectedEndpoints;
      this.topResourceExhaustedExcludedEndpoints = topResourceExhaustedExcludedEndpoints;
    }
  }
}
