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

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.spanner.v1.RoutingHint;
import javax.annotation.Nullable;

/** Carries debug context for a single route selection attempt. */
final class RouteSelectionDebugInfo {
  private static final int MAX_KEY_HEX_CHARS = 96;

  @Nullable private String routeSource;
  @Nullable private String routeMethod;
  @Nullable private String defaultReasonCode;
  @Nullable private String defaultReasonDetail;
  @Nullable private String requestKeyHex;
  @Nullable private String requestLimitKeyHex;
  @Nullable private String databaseId;
  @Nullable private Long rangeLookupDurationMs;
  @Nullable private Long tabletSelectionDurationMs;
  @Nullable private Integer matchedTabletCount;
  @Nullable private Integer replicaFilteredCount;
  @Nullable private Integer serverSkipCount;
  @Nullable private Integer emptyAddressCount;
  @Nullable private Integer excludedEndpointCount;
  @Nullable private Integer transientFailureCount;
  @Nullable private Integer endpointNotReadyCount;
  @Nullable private Boolean tabletSelected;
  @Nullable private Boolean selectedFirstReplica;
  @Nullable private Boolean selectedLeader;
  @Nullable private Boolean leaderSkippedTransientFailure;
  @Nullable private Boolean leaderSkippedEndpointNotReady;

  void setRouteSource(String routeSource) {
    this.routeSource = routeSource;
  }

  @Nullable
  String getRouteSource() {
    return routeSource;
  }

  void setRouteMethod(String routeMethod) {
    this.routeMethod = routeMethod;
  }

  @Nullable
  String getRouteMethod() {
    return routeMethod;
  }

  void setDefaultReason(String code, @Nullable String detail) {
    this.defaultReasonCode = code;
    this.defaultReasonDetail = detail;
  }

  @Nullable
  String getDefaultReasonCode() {
    return defaultReasonCode;
  }

  @Nullable
  String getDefaultReasonDetail() {
    return defaultReasonDetail;
  }

  void setDatabaseId(@Nullable String databaseId) {
    this.databaseId = databaseId;
  }

  @Nullable
  String getDatabaseId() {
    return databaseId;
  }

  void setRangeLookupDurationMs(long rangeLookupDurationMs) {
    this.rangeLookupDurationMs = rangeLookupDurationMs;
  }

  @Nullable
  Long getRangeLookupDurationMs() {
    return rangeLookupDurationMs;
  }

  void setTabletSelectionDurationMs(long tabletSelectionDurationMs) {
    this.tabletSelectionDurationMs = tabletSelectionDurationMs;
  }

  @Nullable
  Long getTabletSelectionDurationMs() {
    return tabletSelectionDurationMs;
  }

  void recordTabletSelectionStats(
      int matchedTabletCount,
      int replicaFilteredCount,
      int serverSkipCount,
      int emptyAddressCount,
      int excludedEndpointCount,
      int transientFailureCount,
      int endpointNotReadyCount,
      boolean tabletSelected,
      boolean selectedFirstReplica,
      boolean selectedLeader,
      boolean leaderSkippedTransientFailure,
      boolean leaderSkippedEndpointNotReady) {
    this.matchedTabletCount = matchedTabletCount;
    this.replicaFilteredCount = replicaFilteredCount;
    this.serverSkipCount = serverSkipCount;
    this.emptyAddressCount = emptyAddressCount;
    this.excludedEndpointCount = excludedEndpointCount;
    this.transientFailureCount = transientFailureCount;
    this.endpointNotReadyCount = endpointNotReadyCount;
    this.tabletSelected = tabletSelected;
    this.selectedFirstReplica = selectedFirstReplica;
    this.selectedLeader = selectedLeader;
    this.leaderSkippedTransientFailure = leaderSkippedTransientFailure;
    this.leaderSkippedEndpointNotReady = leaderSkippedEndpointNotReady;
  }

  @Nullable
  Integer getMatchedTabletCount() {
    return matchedTabletCount;
  }

  @Nullable
  Integer getReplicaFilteredCount() {
    return replicaFilteredCount;
  }

  @Nullable
  Integer getServerSkipCount() {
    return serverSkipCount;
  }

  @Nullable
  Integer getEmptyAddressCount() {
    return emptyAddressCount;
  }

  @Nullable
  Integer getExcludedEndpointCount() {
    return excludedEndpointCount;
  }

  @Nullable
  Integer getTransientFailureCount() {
    return transientFailureCount;
  }

  @Nullable
  Integer getEndpointNotReadyCount() {
    return endpointNotReadyCount;
  }

  @Nullable
  Boolean getTabletSelected() {
    return tabletSelected;
  }

  @Nullable
  Boolean getSelectedFirstReplica() {
    return selectedFirstReplica;
  }

  @Nullable
  Boolean getSelectedLeader() {
    return selectedLeader;
  }

  @Nullable
  Boolean getLeaderSkippedTransientFailure() {
    return leaderSkippedTransientFailure;
  }

  @Nullable
  Boolean getLeaderSkippedEndpointNotReady() {
    return leaderSkippedEndpointNotReady;
  }

  void captureRoutingHint(RoutingHint.Builder hintBuilder) {
    requestKeyHex = encodeForLog(hintBuilder.getKey());
    requestLimitKeyHex = encodeForLog(hintBuilder.getLimitKey());
  }

  @Nullable
  String getRequestKeyHex() {
    return requestKeyHex;
  }

  @Nullable
  String getRequestLimitKeyHex() {
    return requestLimitKeyHex;
  }

  @Nullable
  private static String encodeForLog(ByteString value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    String hex = BaseEncoding.base16().lowerCase().encode(value.toByteArray());
    if (hex.length() <= MAX_KEY_HEX_CHARS) {
      return hex;
    }
    return hex.substring(0, MAX_KEY_HEX_CHARS) + "...";
  }
}
