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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.protobuf.ListValue;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.TypeCode;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.Date;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuiltInMetricsToCustomOpenTelemetryTest extends AbstractNettyMockServerTest {
  private static final Statement SELECT1 = Statement.of("SELECT 1 AS COL1");
  private static final OAuth2Credentials TEST_CREDENTIALS =
      OAuth2Credentials.create(
          new AccessToken(
              "TEST_TOKEN",
              new Date(System.currentTimeMillis() + java.util.concurrent.TimeUnit.DAYS.toMillis(1))));
  private static final ResultSetMetadata SELECT1_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      StructType.Field.newBuilder()
                          .setName("COL1")
                          .setType(
                              com.google.spanner.v1.Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  private static final com.google.spanner.v1.ResultSet SELECT1_RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(com.google.protobuf.Value.newBuilder().setStringValue("1").build())
                  .build())
          .setMetadata(SELECT1_METADATA)
          .build();

  private InMemoryMetricReader globalMetricReader;
  private InMemoryMetricReader injectedMetricReader;
  private DatabaseClient client;

  @BeforeClass
  public static void setupResults() {
    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.query(SELECT1, SELECT1_RESULTSET));
  }

  @Override
  public void createSpannerInstance() {
    globalMetricReader = InMemoryMetricReader.create();
    GlobalOpenTelemetry.resetForTest();
    OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(globalMetricReader).build())
        .buildAndRegisterGlobal();

    injectedMetricReader = InMemoryMetricReader.create();
    SdkMeterProviderBuilder meterProviderBuilder =
        SdkMeterProvider.builder().registerMetricReader(injectedMetricReader);
    SpannerOptions.registerBuiltInMetricViewsForCustomExporter(meterProviderBuilder);
    OpenTelemetry openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProviderBuilder.build()).build();

    String endpoint = address.getHostString() + ":" + server.getPort();
    spanner =
        SpannerOptions.newBuilder()
            .setProjectId("test-project")
            .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
            .setHost("http://" + endpoint)
            .setCredentials(TEST_CREDENTIALS)
            .setSessionPoolOption(SessionPoolOptions.newBuilder().setFailOnSessionLeak().build())
            .setBuiltInMetricsEnabled(false)
            .setOpenTelemetry(openTelemetry)
            .setExportBuiltInMetricsToOpenTelemetry(true)
            .setBuiltInMetricsClientName("ycsb/test-pod-1")
            .build()
            .getService();
    client = spanner.getDatabaseClient(DatabaseId.of("test-project", "i", "d"));
  }

  @After
  public void resetGlobalTelemetry() {
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  public void builtInMetricsAreExportedToInjectedOpenTelemetryOnly() {
    try (ResultSet resultSet = client.singleUse().executeQuery(SELECT1)) {
      assertTrue(resultSet.next());
      assertThat(resultSet.getLong(0)).isEqualTo(1L);
    }

    MetricData injectedOperationCount =
        getMetricData(
            injectedMetricReader,
            BuiltInMetricsConstant.CUSTOM_EXPORT_METER_NAME
                + "/"
                + BuiltInMetricsConstant.OPERATION_COUNT_NAME);
    assertNotNull(injectedOperationCount);
    assertThat(injectedOperationCount.getLongSumData().getPoints()).isNotEmpty();
    assertThat(
            injectedOperationCount.getLongSumData().getPoints().stream()
                .anyMatch(
                    point ->
                        point.getValue() > 0
                            && "Spanner.ExecuteStreamingSql"
                                .equals(point.getAttributes().get(BuiltInMetricsConstant.METHOD_KEY))
                            && "OK"
                                .equals(point.getAttributes().get(BuiltInMetricsConstant.STATUS_KEY))
                            && "ycsb/test-pod-1"
                                .equals(
                                    point
                                        .getAttributes()
                                        .get(BuiltInMetricsConstant.CLIENT_NAME_KEY))
                            && "d"
                                .equals(point.getAttributes().get(BuiltInMetricsConstant.DATABASE_KEY))))
        .isTrue();

    MetricData globalOperationCount =
        getMetricData(
            globalMetricReader,
            BuiltInMetricsConstant.CUSTOM_EXPORT_METER_NAME
                + "/"
                + BuiltInMetricsConstant.OPERATION_COUNT_NAME);
    assertNull(globalOperationCount);

    MetricData reservedNameMetric =
        getMetricData(
            injectedMetricReader,
            BuiltInMetricsConstant.METER_NAME + "/" + BuiltInMetricsConstant.OPERATION_COUNT_NAME);
    assertNull(reservedNameMetric);
  }

  private MetricData getMetricData(InMemoryMetricReader reader, String metricName) {
    Collection<MetricData> metrics = reader.collectAllMetrics();
    return metrics.stream().filter(metric -> metric.getName().equals(metricName)).findFirst().orElse(null);
  }
}
