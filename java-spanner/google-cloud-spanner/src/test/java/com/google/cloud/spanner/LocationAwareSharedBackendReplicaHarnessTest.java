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

package com.google.cloud.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.MockSpannerServiceImpl.SimulatedExecutionTime;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.spi.v1.KeyRecipeCache;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.TextFormat;
import com.google.protobuf.Value;
import com.google.rpc.RetryInfo;
import com.google.spanner.v1.CacheUpdate;
import com.google.spanner.v1.DirectedReadOptions;
import com.google.spanner.v1.DirectedReadOptions.IncludeReplicas;
import com.google.spanner.v1.DirectedReadOptions.ReplicaSelection;
import com.google.spanner.v1.Group;
import com.google.spanner.v1.Range;
import com.google.spanner.v1.ReadRequest;
import com.google.spanner.v1.RecipeList;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.RoutingHint;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Tablet;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LocationAwareSharedBackendReplicaHarnessTest {

  private static final String PROJECT = "fake-project";
  private static final String INSTANCE = "fake-instance";
  private static final String DATABASE = "fake-database";
  private static final String TABLE = "T";
  private static final String REPLICA_LOCATION = "us-east1";
  private static final String STREAMING_READ_METHOD = "google.spanner.v1.Spanner/StreamingRead";
  private static final Statement SEED_QUERY = Statement.of("SELECT 1");
  private static final ByteString RESUME_TOKEN_AFTER_FIRST_ROW =
      ByteString.copyFromUtf8("000000001");
  private static final DirectedReadOptions DIRECTED_READ_OPTIONS =
      DirectedReadOptions.newBuilder()
          .setIncludeReplicas(
              IncludeReplicas.newBuilder()
                  .addReplicaSelections(
                      ReplicaSelection.newBuilder()
                          .setLocation(REPLICA_LOCATION)
                          .setType(ReplicaSelection.Type.READ_ONLY)
                          .build())
                  .build())
          .build();

  @BeforeClass
  public static void enableLocationAwareRouting() {
    SpannerOptions.useEnvironment(
        new SpannerOptions.SpannerEnvironment() {
          @Override
          public boolean isEnableLocationApi() {
            return true;
          }
        });
  }

  @AfterClass
  public static void restoreEnvironment() {
    SpannerOptions.useDefaultEnvironment();
  }

  @After
  public void resetGlobalTelemetry() {
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  public void singleUseReadReroutesOnResourceExhaustedForBypassTraffic() throws Exception {
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpanner(harness)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      harness
          .replicas
          .get(0)
          .putMethodErrors(
              SharedBackendReplicaHarness.METHOD_STREAMING_READ,
              resourceExhausted("busy-routed-replica"));

      try (ResultSet resultSet =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(resultSet.next());
      }

      assertEquals(
          1,
          harness
              .replicas
              .get(0)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          1,
          harness
              .replicas
              .get(1)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          0,
          harness
              .defaultReplica
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      ReadRequest replicaARequest =
          (ReadRequest)
              harness
                  .replicas
                  .get(0)
                  .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
                  .get(0);
      assertTrue(replicaARequest.getResumeToken().isEmpty());
      assertRetriedOnSameLogicalRequest(
          harness
              .replicas
              .get(0)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0),
          harness
              .replicas
              .get(1)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0));
    }
  }

  @Test
  public void builtInMetricsCustomExporterIncludesTargetEndpointForBypassTraffic()
      throws Exception {
    InMemoryMetricReader metricReader = InMemoryMetricReader.create();
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpannerWithCustomExporter(harness, metricReader)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);

      try (ResultSet resultSet =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(resultSet.next());
      }

      MetricData operationCountMetric =
          getMetricData(
              metricReader,
              BuiltInMetricsConstant.CUSTOM_EXPORT_METER_NAME
                  + "/"
                  + BuiltInMetricsConstant.OPERATION_COUNT_NAME);
      assertNotNull(operationCountMetric);
      assertThat(operationCountMetric.getLongSumData().getPoints()).isNotEmpty();
      boolean foundTargetEndpoint =
          operationCountMetric.getLongSumData().getPoints().stream()
              .anyMatch(
                  point ->
                      point.getValue() > 0
                          && harness
                              .replicaAddresses
                              .get(0)
                              .concat("-LEADER")
                              .equals(
                                  point
                                      .getAttributes()
                                      .get(BuiltInMetricsConstant.TARGET_ENDPOINT_KEY))
                          && "ycsb/test-pod-1"
                              .equals(
                                  point.getAttributes().get(BuiltInMetricsConstant.CLIENT_NAME_KEY))
                          && DATABASE.equals(
                              point.getAttributes().get(BuiltInMetricsConstant.DATABASE_KEY)));
      assertTrue(
          "Expected target_endpoint in built-in metrics points: "
              + operationCountMetric.getLongSumData().getPoints(),
          foundTargetEndpoint);
    }
  }

  @Test
  public void routingDecisionMetricsAreExportedToCustomerExporter() throws Exception {
    InMemoryMetricReader metricReader = InMemoryMetricReader.create();
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpannerWithCustomExporter(harness, metricReader)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      MetricData routingDecisionMetric =
          getMetricData(metricReader, "location_aware.routing_decision_count");
      assertNull(routingDecisionMetric);

      try (ResultSet firstRead =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(firstRead.next());
      }

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);

      try (ResultSet routedRead =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(routedRead.next());
      }

      routingDecisionMetric = getMetricData(metricReader, "location_aware.routing_decision_count");
      assertNotNull(routingDecisionMetric);
      assertThat(routingDecisionMetric.getLongSumData().getPoints()).isNotEmpty();

      AttributeKey<String> decisionKey = AttributeKey.stringKey("decision");
      AttributeKey<String> reasonKey = AttributeKey.stringKey("reason");
      AttributeKey<String> methodKey = AttributeKey.stringKey("method");
      AttributeKey<String> endpointKey = AttributeKey.stringKey("target_endpoint");

      boolean foundCacheMissDefaultHostDecision =
          routingDecisionMetric.getLongSumData().getPoints().stream()
              .anyMatch(
                  point ->
                      point.getValue() > 0
                          && "default_host".equals(point.getAttributes().get(decisionKey))
                          && "cache_miss".equals(point.getAttributes().get(reasonKey))
                          && STREAMING_READ_METHOD.equals(point.getAttributes().get(methodKey))
                          && harness.defaultAddress.equals(point.getAttributes().get(endpointKey)));
      assertTrue(
          "Expected default_host/cache_miss routing metric point: "
              + routingDecisionMetric.getLongSumData().getPoints(),
          foundCacheMissDefaultHostDecision);

      boolean foundRoutedReplicaDecision =
          routingDecisionMetric.getLongSumData().getPoints().stream()
              .anyMatch(
                  point ->
                      point.getValue() > 0
                          && "routed_replica".equals(point.getAttributes().get(decisionKey))
                          && "selected".equals(point.getAttributes().get(reasonKey))
                          && STREAMING_READ_METHOD.equals(point.getAttributes().get(methodKey))
                          && harness
                              .replicaAddresses
                              .get(0)
                              .concat("-LEADER")
                              .equals(point.getAttributes().get(endpointKey)));
      assertTrue(
          "Expected routed_replica/selected routing metric point: "
              + routingDecisionMetric.getLongSumData().getPoints(),
          foundRoutedReplicaDecision);
    }
  }

  @Test
  public void routingSkippedTabletMetricsAreExportedToCustomerExporter() throws Exception {
    InMemoryMetricReader metricReader = InMemoryMetricReader.create();
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpannerWithCustomExporter(harness, metricReader)) {
      configureBackend(
          harness, singleRowReadResultSet("b"), cacheUpdateWithSkippedLeaderReplica(harness));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 1);
      harness.clearRequests();

      try (ResultSet routedRead =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(routedRead.next());
      }

      ReadRequest routedReplicaRequest =
          (ReadRequest)
              harness
                  .replicas
                  .get(1)
                  .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
                  .get(0);
      assertEquals(1, routedReplicaRequest.getRoutingHint().getSkippedTabletUidCount());
      assertEquals(11L, routedReplicaRequest.getRoutingHint().getSkippedTabletUid(0).getTabletUid());

      MetricData skippedTabletMetric =
          getMetricData(metricReader, "location_aware.routing_skipped_tablet_count");
      assertNotNull(skippedTabletMetric);
      assertThat(skippedTabletMetric.getLongSumData().getPoints()).isNotEmpty();

      AttributeKey<String> decisionKey = AttributeKey.stringKey("decision");
      AttributeKey<String> reasonKey = AttributeKey.stringKey("reason");
      AttributeKey<String> methodKey = AttributeKey.stringKey("method");
      AttributeKey<String> endpointKey = AttributeKey.stringKey("target_endpoint");
      AttributeKey<String> skippedEndpointKey = AttributeKey.stringKey("skipped_target_endpoint");

      boolean foundSkippedTabletMetric =
          skippedTabletMetric.getLongSumData().getPoints().stream()
              .anyMatch(
                  point ->
                      point.getValue() > 0
                          && "routed_replica".equals(point.getAttributes().get(decisionKey))
                          && "tablet_marked_skip".equals(point.getAttributes().get(reasonKey))
                          && STREAMING_READ_METHOD.equals(point.getAttributes().get(methodKey))
                          && harness.replicaAddresses.get(1).equals(point.getAttributes().get(endpointKey))
                          && harness
                              .replicaAddresses
                              .get(0)
                              .concat("-LEADER")
                              .equals(point.getAttributes().get(skippedEndpointKey)));
      assertTrue(
          "Expected routed skipped-tablet metric point: "
              + skippedTabletMetric.getLongSumData().getPoints(),
          foundSkippedTabletMetric);
    }
  }

  @Test
  public void singleUseReadCooldownSkipsReplicaOnNextRequestForBypassTraffic() throws Exception {
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpanner(harness)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      harness
          .replicas
          .get(0)
          .putMethodErrors(
              SharedBackendReplicaHarness.METHOD_STREAMING_READ,
              resourceExhaustedWithRetryInfo("busy-routed-replica"));

      try (ResultSet firstRead =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(firstRead.next());
      }

      try (ResultSet secondRead =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(secondRead.next());
      }

      assertEquals(
          1,
          harness
              .replicas
              .get(0)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          2,
          harness
              .replicas
              .get(1)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          0,
          harness
              .defaultReplica
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      List<AbstractMessage> replicaBRequests =
          harness.replicas.get(1).getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ);
      for (AbstractMessage request : replicaBRequests) {
        assertTrue(((ReadRequest) request).getResumeToken().isEmpty());
      }
      List<String> replicaBRequestIds =
          harness.replicas.get(1).getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ);
      assertRetriedOnSameLogicalRequest(
          harness
              .replicas
              .get(0)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0),
          replicaBRequestIds.get(0));
      assertNotEquals(
          XGoogSpannerRequestId.of(replicaBRequestIds.get(0)).getLogicalRequestKey(),
          XGoogSpannerRequestId.of(replicaBRequestIds.get(1)).getLogicalRequestKey());
    }
  }

  @Test
  public void singleUseReadRetriesRoutedReplicaWhenAllReplicasAreExcludedOrCoolingDown()
      throws Exception {
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpanner(harness)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      harness
          .replicas
          .get(0)
          .putMethodErrors(
              SharedBackendReplicaHarness.METHOD_STREAMING_READ,
              resourceExhaustedWithRetryInfo("leader-overloaded"));
      harness
          .replicas
          .get(1)
          .putMethodErrors(
              SharedBackendReplicaHarness.METHOD_STREAMING_READ,
              resourceExhaustedWithRetryInfo("replica-overloaded"));

      try (ResultSet resultSet =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(resultSet.next());
      }

      int replica0Requests =
          harness
              .replicas
              .get(0)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size();
      int replica1Requests =
          harness
              .replicas
              .get(1)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size();
      assertEquals(3, replica0Requests + replica1Requests);
      assertTrue(replica0Requests >= 1);
      assertTrue(replica1Requests >= 1);
      assertEquals(
          0,
          harness
              .defaultReplica
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());

      List<String> allRequestIds = new ArrayList<>();
      allRequestIds.addAll(
          harness
              .replicas
              .get(0)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ));
      allRequestIds.addAll(
          harness
              .replicas
              .get(1)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ));
      assertEquals(3, allRequestIds.size());

      String logicalRequestKey = XGoogSpannerRequestId.of(allRequestIds.get(0)).getLogicalRequestKey();
      Set<Long> attempts = new HashSet<>();
      for (String requestId : allRequestIds) {
        XGoogSpannerRequestId parsed = XGoogSpannerRequestId.of(requestId);
        assertEquals(logicalRequestKey, parsed.getLogicalRequestKey());
        attempts.add(parsed.getAttempt());
      }
      assertThat(attempts).containsExactly(1L, 2L, 3L);
    }
  }

  @Test
  public void singleUseReadReroutesOnUnavailableForBypassTraffic() throws Exception {
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpanner(harness)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      harness
          .replicas
          .get(0)
          .putMethodErrors(
              SharedBackendReplicaHarness.METHOD_STREAMING_READ, unavailable("isolated-replica"));

      try (ResultSet resultSet =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(resultSet.next());
      }

      assertEquals(
          1,
          harness
              .replicas
              .get(0)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          1,
          harness
              .replicas
              .get(1)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          0,
          harness
              .defaultReplica
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      ReadRequest replicaARequest =
          (ReadRequest)
              harness
                  .replicas
                  .get(0)
                  .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
                  .get(0);
      assertTrue(replicaARequest.getResumeToken().isEmpty());
      assertRetriedOnSameLogicalRequest(
          harness
              .replicas
              .get(0)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0),
          harness
              .replicas
              .get(1)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0));
    }
  }

  @Test
  public void reroutedStreamingReadExportsUnavailableAttemptStatusForFailedReplica()
      throws Exception {
    InMemoryMetricReader metricReader = InMemoryMetricReader.create();
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpannerWithCustomExporter(harness, metricReader)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      harness
          .replicas
          .get(0)
          .putMethodErrors(
              SharedBackendReplicaHarness.METHOD_STREAMING_READ, unavailable("isolated-replica"));

      try (ResultSet resultSet =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(resultSet.next());
      }

      MetricData attemptCountMetric =
          getMetricData(
              metricReader,
              BuiltInMetricsConstant.CUSTOM_EXPORT_METER_NAME
                  + "/"
                  + BuiltInMetricsConstant.ATTEMPT_COUNT_NAME);
      assertNotNull(attemptCountMetric);
      assertThat(attemptCountMetric.getLongSumData().getPoints()).isNotEmpty();

      boolean foundUnavailableAttempt =
          attemptCountMetric.getLongSumData().getPoints().stream()
              .anyMatch(
                  point ->
                      point.getValue() > 0
                          && "Spanner.StreamingRead"
                              .equals(point.getAttributes().get(BuiltInMetricsConstant.METHOD_KEY))
                          && "UNAVAILABLE"
                              .equals(point.getAttributes().get(BuiltInMetricsConstant.STATUS_KEY)));
      assertTrue(
          "Expected UNAVAILABLE attempt status for rerouted streaming read: "
              + attemptCountMetric.getLongSumData().getPoints(),
          foundUnavailableAttempt);

      boolean foundOkAttempt =
          attemptCountMetric.getLongSumData().getPoints().stream()
              .anyMatch(
                  point ->
                      point.getValue() > 0
                          && "Spanner.StreamingRead"
                              .equals(point.getAttributes().get(BuiltInMetricsConstant.METHOD_KEY))
                          && "OK"
                              .equals(point.getAttributes().get(BuiltInMetricsConstant.STATUS_KEY)));
      assertTrue(
          "Expected OK attempt status for successful rerouted streaming read: "
              + attemptCountMetric.getLongSumData().getPoints(),
          foundOkAttempt);
    }
  }

  @Test
  public void singleUseReadCooldownSkipsUnavailableReplicaOnNextRequestForBypassTraffic()
      throws Exception {
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpanner(harness)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      harness
          .replicas
          .get(0)
          .putMethodErrors(
              SharedBackendReplicaHarness.METHOD_STREAMING_READ, unavailable("isolated-replica"));

      try (ResultSet firstRead =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(firstRead.next());
      }

      try (ResultSet secondRead =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        assertTrue(secondRead.next());
      }

      assertEquals(
          1,
          harness
              .replicas
              .get(0)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          2,
          harness
              .replicas
              .get(1)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          0,
          harness
              .defaultReplica
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      List<AbstractMessage> replicaBRequests =
          harness.replicas.get(1).getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ);
      for (AbstractMessage request : replicaBRequests) {
        assertTrue(((ReadRequest) request).getResumeToken().isEmpty());
      }
      List<String> replicaBRequestIds =
          harness.replicas.get(1).getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ);
      assertRetriedOnSameLogicalRequest(
          harness
              .replicas
              .get(0)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0),
          replicaBRequestIds.get(0));
      assertNotEquals(
          XGoogSpannerRequestId.of(replicaBRequestIds.get(0)).getLogicalRequestKey(),
          XGoogSpannerRequestId.of(replicaBRequestIds.get(1)).getLogicalRequestKey());
    }
  }

  @Test
  public void routedReplicaUsesSingleConnectionWhenDefaultPoolHasMultipleChannels()
      throws Exception {
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpanner(harness, 4)) {
      configureBackend(harness, singleRowReadResultSet("b"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      ExecutorService executor = Executors.newFixedThreadPool(8);
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
          futures.add(
              executor.submit(
                  () -> {
                    try (ResultSet resultSet =
                        client
                            .singleUse()
                            .read(
                                TABLE,
                                KeySet.singleKey(Key.of("b")),
                                Arrays.asList("k"),
                                Options.directedRead(DIRECTED_READ_OPTIONS))) {
                      assertTrue(resultSet.next());
                    }
                  }));
        }
        for (Future<?> future : futures) {
          future.get(10L, TimeUnit.SECONDS);
        }
      } finally {
        executor.shutdownNow();
        executor.awaitTermination(10L, TimeUnit.SECONDS);
      }

      Set<String> replicaPeers =
          new HashSet<>(
              harness
                  .replicas
                  .get(0)
                  .getPeerAddresses(SharedBackendReplicaHarness.METHOD_STREAMING_READ));

      assertEquals(
          "location-aware replica channel should use one underlying direct endpoint connection",
          1,
          replicaPeers.size());
    }
  }

  @Test
  public void singleUseReadMidStreamRecvFailureWithoutRetryInfoRetriesForBypassTraffic()
      throws Exception {
    try (SharedBackendReplicaHarness harness = SharedBackendReplicaHarness.create(2);
        Spanner spanner = createSpanner(harness)) {
      configureBackend(harness, multiRowReadResultSet("b", "c", "d"));
      DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(PROJECT, INSTANCE, DATABASE));

      seedLocationMetadata(client);
      waitForReplicaRoutedRead(client, harness, 0);
      harness.clearRequests();

      harness.backend.setStreamingReadExecutionTime(
          SimulatedExecutionTime.ofStreamException(resourceExhausted("busy-routed-replica"), 1L));

      List<String> rows = new ArrayList<>();
      try (ResultSet resultSet =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        while (resultSet.next()) {
          rows.add(resultSet.getString(0));
        }
      }

      assertEquals(Arrays.asList("b", "c", "d"), rows);
      assertEquals(
          1,
          harness
              .replicas
              .get(0)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          1,
          harness
              .replicas
              .get(1)
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());
      assertEquals(
          0,
          harness
              .defaultReplica
              .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .size());

      ReadRequest replicaARequest =
          (ReadRequest)
              harness
                  .replicas
                  .get(0)
                  .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
                  .get(0);
      ReadRequest replicaBRequest =
          (ReadRequest)
              harness
                  .replicas
                  .get(1)
                  .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
                  .get(0);
      assertTrue(replicaARequest.getResumeToken().isEmpty());
      assertEquals(RESUME_TOKEN_AFTER_FIRST_ROW, replicaBRequest.getResumeToken());
      assertRetriedOnSameLogicalRequest(
          harness
              .replicas
              .get(0)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0),
          harness
              .replicas
              .get(1)
              .getRequestIds(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
              .get(0));
    }
  }

  private static Spanner createSpanner(SharedBackendReplicaHarness harness) {
    return createSpanner(harness, null);
  }

  private static Spanner createSpanner(SharedBackendReplicaHarness harness, Integer numChannels) {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .usePlainText()
            .setExperimentalHost(harness.defaultAddress)
            .setProjectId(PROJECT)
            .setCredentials(NoCredentials.getInstance())
            .setChannelEndpointCacheFactory(null);
    if (numChannels != null) {
      builder.setNumChannels(numChannels);
    }
    return builder.build().getService();
  }

  private static Spanner createSpannerWithCustomExporter(
      SharedBackendReplicaHarness harness, InMemoryMetricReader metricReader) {
    GlobalOpenTelemetry.resetForTest();
    SdkMeterProviderBuilder meterProviderBuilder =
        SdkMeterProvider.builder().registerMetricReader(metricReader);
    SpannerOptions.registerBuiltInMetricViewsForCustomExporter(meterProviderBuilder);
    OpenTelemetry openTelemetry =
        OpenTelemetrySdk.builder()
            .setMeterProvider(meterProviderBuilder.build())
            .buildAndRegisterGlobal();

    return SpannerOptions.newBuilder()
        .usePlainText()
        .setExperimentalHost(harness.defaultAddress)
        .setProjectId(PROJECT)
        .setCredentials(NoCredentials.getInstance())
        .setChannelEndpointCacheFactory(null)
        .setBuiltInMetricsEnabled(false)
        .setOpenTelemetry(openTelemetry)
        .setExportBuiltInMetricsToOpenTelemetry(true)
        .setBuiltInMetricsClientName("ycsb/test-pod-1")
        .build()
        .getService();
  }

  private static void configureBackend(
      SharedBackendReplicaHarness harness, com.google.spanner.v1.ResultSet readResultSet)
      throws TextFormat.ParseException {
    configureBackend(harness, readResultSet, cacheUpdate(harness));
  }

  private static void configureBackend(
      SharedBackendReplicaHarness harness,
      com.google.spanner.v1.ResultSet readResultSet,
      CacheUpdate cacheUpdate)
      throws TextFormat.ParseException {
    Statement readStatement =
        StatementResult.createReadStatement(
            TABLE, KeySet.singleKey(Key.of("b")), Arrays.asList("k"));
    harness.backend.putStatementResult(StatementResult.query(readStatement, readResultSet));
    harness.backend.putStatementResult(
        StatementResult.query(
            SEED_QUERY,
            singleRowReadResultSet("seed").toBuilder()
                .setCacheUpdate(cacheUpdate)
                .build()));
  }

  private static void seedLocationMetadata(DatabaseClient client) {
    try (com.google.cloud.spanner.ResultSet resultSet =
        client.singleUse().executeQuery(SEED_QUERY)) {
      while (resultSet.next()) {
        // Consume the cache update on the first query result.
      }
    }
  }

  private static void waitForReplicaRoutedRead(
      DatabaseClient client, SharedBackendReplicaHarness harness, int replicaIndex)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadlineNanos) {
      try (ResultSet resultSet =
          client
              .singleUse()
              .read(
                  TABLE,
                  KeySet.singleKey(Key.of("b")),
                  Arrays.asList("k"),
                  Options.directedRead(DIRECTED_READ_OPTIONS))) {
        if (resultSet.next()
            && !harness
                .replicas
                .get(replicaIndex)
                .getRequests(SharedBackendReplicaHarness.METHOD_STREAMING_READ)
                .isEmpty()) {
          return;
        }
      }
      Thread.sleep(50L);
    }
    throw new AssertionError("Timed out waiting for location-aware read to route to replica");
  }

  private static CacheUpdate cacheUpdate(SharedBackendReplicaHarness harness)
      throws TextFormat.ParseException {
    RecipeList recipes = readRecipeList();
    RoutingHint routingHint = exactReadRoutingHint(recipes);
    ByteString limitKey = routingHint.getLimitKey();
    if (limitKey.isEmpty()) {
      limitKey = routingHint.getKey().concat(ByteString.copyFrom(new byte[] {0}));
    }

    return CacheUpdate.newBuilder()
        .setDatabaseId(12345L)
        .setKeyRecipes(recipes)
        .addRange(
            Range.newBuilder()
                .setStartKey(routingHint.getKey())
                .setLimitKey(limitKey)
                .setGroupUid(1L)
                .setSplitId(1L)
                .setGeneration(com.google.protobuf.ByteString.copyFromUtf8("gen1")))
        .addGroup(
            Group.newBuilder()
                .setGroupUid(1L)
                .setGeneration(com.google.protobuf.ByteString.copyFromUtf8("gen1"))
                .setLeaderIndex(0)
                .addTablets(
                    Tablet.newBuilder()
                        .setTabletUid(11L)
                        .setServerAddress(harness.replicaAddresses.get(0))
                        .setLocation(REPLICA_LOCATION)
                        .setRole(Tablet.Role.READ_ONLY)
                        .setDistance(0))
                .addTablets(
                    Tablet.newBuilder()
                        .setTabletUid(12L)
                        .setServerAddress(harness.replicaAddresses.get(1))
                        .setLocation(REPLICA_LOCATION)
                        .setRole(Tablet.Role.READ_ONLY)
                        .setDistance(0)))
        .build();
  }

  private static CacheUpdate cacheUpdateWithSkippedLeaderReplica(SharedBackendReplicaHarness harness)
      throws TextFormat.ParseException {
    RecipeList recipes = readRecipeList();
    RoutingHint routingHint = exactReadRoutingHint(recipes);
    ByteString limitKey = routingHint.getLimitKey();
    if (limitKey.isEmpty()) {
      limitKey = routingHint.getKey().concat(ByteString.copyFrom(new byte[] {0}));
    }

    return CacheUpdate.newBuilder()
        .setDatabaseId(12345L)
        .setKeyRecipes(recipes)
        .addRange(
            Range.newBuilder()
                .setStartKey(routingHint.getKey())
                .setLimitKey(limitKey)
                .setGroupUid(1L)
                .setSplitId(1L)
                .setGeneration(com.google.protobuf.ByteString.copyFromUtf8("gen-skip")))
        .addGroup(
            Group.newBuilder()
                .setGroupUid(1L)
                .setGeneration(com.google.protobuf.ByteString.copyFromUtf8("gen-skip"))
                .setLeaderIndex(0)
                .addTablets(
                    Tablet.newBuilder()
                        .setTabletUid(11L)
                        .setServerAddress(harness.replicaAddresses.get(0))
                        .setLocation(REPLICA_LOCATION)
                        .setRole(Tablet.Role.READ_ONLY)
                        .setDistance(0)
                        .setSkip(true))
                .addTablets(
                    Tablet.newBuilder()
                        .setTabletUid(12L)
                        .setServerAddress(harness.replicaAddresses.get(1))
                        .setLocation(REPLICA_LOCATION)
                        .setRole(Tablet.Role.READ_ONLY)
                        .setDistance(0)))
        .build();
  }

  private static RecipeList readRecipeList() throws TextFormat.ParseException {
    RecipeList.Builder recipes = RecipeList.newBuilder();
    TextFormat.merge(
        "schema_generation: \"1\"\n"
            + "recipe {\n"
            + "  table_name: \""
            + TABLE
            + "\"\n"
            + "  part { tag: 1 }\n"
            + "  part {\n"
            + "    order: ASCENDING\n"
            + "    null_order: NULLS_FIRST\n"
            + "    type { code: STRING }\n"
            + "    identifier: \"k\"\n"
            + "  }\n"
            + "}\n",
        recipes);
    return recipes.build();
  }

  private static RoutingHint exactReadRoutingHint(RecipeList recipes) {
    KeyRecipeCache recipeCache = new KeyRecipeCache();
    recipeCache.addRecipes(recipes);
    ReadRequest.Builder request =
        ReadRequest.newBuilder()
            .setSession(
                String.format(
                    "projects/%s/instances/%s/databases/%s/sessions/test-session",
                    PROJECT, INSTANCE, DATABASE))
            .setTable(TABLE)
            .addAllColumns(Arrays.asList("k"))
            .setDirectedReadOptions(DIRECTED_READ_OPTIONS);
    KeySet.singleKey(Key.of("b")).appendToProto(request.getKeySetBuilder());
    recipeCache.computeKeys(request);
    return request.getRoutingHint();
  }

  private static io.grpc.StatusRuntimeException resourceExhaustedWithRetryInfo(String description) {
    Metadata trailers = new Metadata();
    trailers.put(
        ProtoUtils.keyForProto(RetryInfo.getDefaultInstance()),
        RetryInfo.newBuilder()
            .setRetryDelay(
                com.google.protobuf.Duration.newBuilder()
                    .setNanos((int) TimeUnit.MILLISECONDS.toNanos(1L))
                    .build())
            .build());
    return Status.RESOURCE_EXHAUSTED.withDescription(description).asRuntimeException(trailers);
  }

  private static StatusRuntimeException resourceExhausted(String description) {
    return Status.RESOURCE_EXHAUSTED.withDescription(description).asRuntimeException();
  }

  private static StatusRuntimeException unavailable(String description) {
    return Status.UNAVAILABLE.withDescription(description).asRuntimeException();
  }

  private static void assertRetriedOnSameLogicalRequest(
      String firstRequestId, String secondRequestId) {
    XGoogSpannerRequestId first = XGoogSpannerRequestId.of(firstRequestId);
    XGoogSpannerRequestId second = XGoogSpannerRequestId.of(secondRequestId);
    assertEquals(first.getLogicalRequestKey(), second.getLogicalRequestKey());
    assertEquals(first.getAttempt() + 1, second.getAttempt());
  }

  private static com.google.spanner.v1.ResultSet singleRowReadResultSet(String value) {
    return readResultSet(Arrays.asList(value));
  }

  private static com.google.spanner.v1.ResultSet multiRowReadResultSet(String... values) {
    return readResultSet(Arrays.asList(values));
  }

  private static MetricData getMetricData(InMemoryMetricReader reader, String metricName) {
    Collection<MetricData> metrics = reader.collectAllMetrics();
    return metrics.stream()
        .filter(metric -> metric.getName().equals(metricName))
        .findFirst()
        .orElse(null);
  }

  private static com.google.spanner.v1.ResultSet readResultSet(List<String> values) {
    com.google.spanner.v1.ResultSet.Builder builder =
        com.google.spanner.v1.ResultSet.newBuilder()
            .setMetadata(
                ResultSetMetadata.newBuilder()
                    .setRowType(
                        StructType.newBuilder()
                            .addFields(
                                StructType.Field.newBuilder()
                                    .setName("k")
                                    .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                    .build())
                            .build()));
    for (String value : values) {
      builder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(value).build())
              .build());
    }
    return builder.build();
  }
}
