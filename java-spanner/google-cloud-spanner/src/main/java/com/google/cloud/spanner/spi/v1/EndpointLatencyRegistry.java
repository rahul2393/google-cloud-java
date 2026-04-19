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

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Shared process-local latency scores for routed Spanner endpoints. */
final class EndpointLatencyRegistry {

  static final Duration DEFAULT_ERROR_PENALTY = Duration.ofSeconds(10);

  private static final String LEADER_SUFFIX = "-LEADER";
  private static final ConcurrentHashMap<TrackerKey, LatencyTracker> TRACKERS =
      new ConcurrentHashMap<>();

  private EndpointLatencyRegistry() {}

  static boolean hasScore(long operationUid, String endpointLabelOrAddress) {
    TrackerKey trackerKey = trackerKey(operationUid, endpointLabelOrAddress);
    return trackerKey != null && TRACKERS.containsKey(trackerKey);
  }

  static double getScore(long operationUid, String endpointLabelOrAddress) {
    TrackerKey trackerKey = trackerKey(operationUid, endpointLabelOrAddress);
    if (trackerKey == null) {
      return Double.MAX_VALUE;
    }
    LatencyTracker tracker = TRACKERS.get(trackerKey);
    return tracker == null ? Double.MAX_VALUE : tracker.getScore();
  }

  static void recordLatency(long operationUid, String endpointLabelOrAddress, Duration latency) {
    TrackerKey trackerKey = trackerKey(operationUid, endpointLabelOrAddress);
    if (trackerKey == null || latency == null) {
      return;
    }
    TRACKERS.computeIfAbsent(trackerKey, ignored -> new EwmaLatencyTracker()).update(latency);
  }

  static void recordError(long operationUid, String endpointLabelOrAddress) {
    recordError(operationUid, endpointLabelOrAddress, DEFAULT_ERROR_PENALTY);
  }

  static void recordError(long operationUid, String endpointLabelOrAddress, Duration penalty) {
    TrackerKey trackerKey = trackerKey(operationUid, endpointLabelOrAddress);
    if (trackerKey == null || penalty == null) {
      return;
    }
    TRACKERS.computeIfAbsent(trackerKey, ignored -> new EwmaLatencyTracker()).recordError(penalty);
  }

  @VisibleForTesting
  static void clear() {
    TRACKERS.clear();
  }

  @VisibleForTesting
  static String normalizeAddress(String endpointLabelOrAddress) {
    if (endpointLabelOrAddress == null || endpointLabelOrAddress.isEmpty()) {
      return null;
    }
    if (endpointLabelOrAddress.endsWith(LEADER_SUFFIX)) {
      return endpointLabelOrAddress.substring(
          0, endpointLabelOrAddress.length() - LEADER_SUFFIX.length());
    }
    return endpointLabelOrAddress;
  }

  @VisibleForTesting
  static String formatScoreKey(long operationUid, String endpointLabelOrAddress) {
    TrackerKey trackerKey = trackerKey(operationUid, endpointLabelOrAddress);
    return trackerKey == null ? null : trackerKey.toString();
  }

  @VisibleForTesting
  static TrackerKey trackerKey(long operationUid, String endpointLabelOrAddress) {
    String address = normalizeAddress(endpointLabelOrAddress);
    if (operationUid <= 0 || address == null) {
      return null;
    }
    return new TrackerKey(operationUid, address);
  }

  @VisibleForTesting
  static final class TrackerKey {
    private final long operationUid;
    private final String address;

    private TrackerKey(long operationUid, String address) {
      this.operationUid = operationUid;
      this.address = address;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof TrackerKey)) {
        return false;
      }
      TrackerKey that = (TrackerKey) other;
      return operationUid == that.operationUid && Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operationUid, address);
    }

    @Override
    public String toString() {
      return operationUid + "@" + address;
    }
  }
}
