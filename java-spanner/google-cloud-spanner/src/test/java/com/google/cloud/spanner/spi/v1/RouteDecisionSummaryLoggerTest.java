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

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RouteDecisionSummaryLoggerTest {

  @Test
  public void snapshotAndResetCapturesRequestedRoutingOutcomes() {
    RouteDecisionSummaryLogger logger = new RouteDecisionSummaryLogger();

    logger.recordAttempt(
        /* usedDefaultEndpoint= */ false,
        /* hadResourceExhaustedExclusion= */ false,
        "server-1:1234",
        routedSelection(/* selectedFirstReplica= */ true, /* selectedLeader= */ true));
    logger.recordAttempt(
        /* usedDefaultEndpoint= */ true,
        /* hadResourceExhaustedExclusion= */ false,
        null,
        defaultSelection(
            "no_healthy_tablet_for_group",
            /* leaderSkippedTransientFailure= */ false,
            /* leaderSkippedEndpointNotReady= */ true));
    logger.recordAttempt(
        /* usedDefaultEndpoint= */ true,
        /* hadResourceExhaustedExclusion= */ false,
        null,
        defaultSelection(
            "range_cache_miss",
            /* leaderSkippedTransientFailure= */ false,
            /* leaderSkippedEndpointNotReady= */ false));
    logger.recordAttempt(
        /* usedDefaultEndpoint= */ false,
        /* hadResourceExhaustedExclusion= */ true,
        "server-2:1234",
        routedSelection(/* selectedFirstReplica= */ false, /* selectedLeader= */ false));
    logger.recordAttempt(
        /* usedDefaultEndpoint= */ true,
        /* hadResourceExhaustedExclusion= */ false,
        null,
        defaultSelection(
            "no_healthy_tablet_for_group",
            /* leaderSkippedTransientFailure= */ true,
            /* leaderSkippedEndpointNotReady= */ false));
    logger.recordResourceExhaustedExclusion("server-1:1234");
    logger.recordResourceExhaustedExclusion("server-1:1234");
    logger.recordResourceExhaustedExclusion("server-2:1234");
    logger.recordTransientFailureSkip("server-3:1234");
    logger.recordTransientFailureSkip("server-3:1234");
    logger.recordEndpointNotReadySkip("server-4:1234");

    RouteDecisionSummaryLogger.SummarySnapshot snapshot = logger.snapshotAndReset();

    assertThat(snapshot.totalRequests).isEqualTo(5);
    assertThat(snapshot.routedRequests).isEqualTo(2);
    assertThat(snapshot.defaultEndpointRequests).isEqualTo(3);
    assertThat(snapshot.firstReplicaSelections).isEqualTo(1);
    assertThat(snapshot.leaderSelections).isEqualTo(1);
    assertThat(snapshot.leaderSkippedTransientFailure).isEqualTo(1);
    assertThat(snapshot.leaderSkippedEndpointNotReady).isEqualTo(1);
    assertThat(snapshot.defaultDueToNoHealthyEndpoint).isEqualTo(2);
    assertThat(snapshot.defaultDueToCacheMiss).isEqualTo(1);
    assertThat(snapshot.alternateReplicaAfterResourceExhausted).isEqualTo(1);
    assertThat(snapshot.topSelectedEndpoints).contains("server-1:1234=1");
    assertThat(snapshot.topSelectedEndpoints).contains("server-2:1234=1");
    assertThat(snapshot.topResourceExhaustedExcludedEndpoints).contains("server-1:1234=2");
    assertThat(snapshot.topTransientFailureSkippedEndpoints).contains("server-3:1234=2");
    assertThat(snapshot.topEndpointNotReadySkippedEndpoints).contains("server-4:1234=1");

    RouteDecisionSummaryLogger.SummarySnapshot cleared = logger.snapshotAndReset();
    assertThat(cleared.totalRequests).isEqualTo(0);
    assertThat(cleared.routedRequests).isEqualTo(0);
    assertThat(cleared.defaultEndpointRequests).isEqualTo(0);
  }

  private static RouteSelectionDebugInfo routedSelection(
      boolean selectedFirstReplica, boolean selectedLeader) {
    RouteSelectionDebugInfo debugInfo = new RouteSelectionDebugInfo();
    debugInfo.recordTabletSelectionStats(
        3,
        0,
        0,
        0,
        0,
        0,
        0,
        /* tabletSelected= */ true,
        selectedFirstReplica,
        selectedLeader,
        /* leaderSkippedTransientFailure= */ false,
        /* leaderSkippedEndpointNotReady= */ false);
    return debugInfo;
  }

  private static RouteSelectionDebugInfo defaultSelection(
      String defaultReasonCode,
      boolean leaderSkippedTransientFailure,
      boolean leaderSkippedEndpointNotReady) {
    RouteSelectionDebugInfo debugInfo = new RouteSelectionDebugInfo();
    debugInfo.setDefaultReason(defaultReasonCode, "detail");
    debugInfo.recordTabletSelectionStats(
        3,
        0,
        0,
        0,
        0,
        leaderSkippedTransientFailure ? 1 : 0,
        leaderSkippedEndpointNotReady ? 1 : 0,
        /* tabletSelected= */ false,
        /* selectedFirstReplica= */ false,
        /* selectedLeader= */ false,
        leaderSkippedTransientFailure,
        leaderSkippedEndpointNotReady);
    return debugInfo;
  }
}
