/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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

import com.google.common.base.Joiner;
import com.google.cloud.NoCredentials;
import com.google.cloud.grpc.GcpManagedChannelOptions.GcpChannelPoolOptions;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.TimestampBound;
import io.grpc.ManagedChannelBuilder;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

/**
 * YCSB Client for Google's Cloud Spanner.
 */
public class CloudSpannerClient extends DB {

  /**
   * The names of properties which can be specified in the config files and flags.
   */
  public static final class CloudSpannerProperties {
    private CloudSpannerProperties() {}

    /**
     * The Cloud Spanner database name to use when running the YCSB benchmark, e.g. 'ycsb-database'.
     */
    static final String DATABASE = "cloudspanner.database";
    /**
     * The Cloud Spanner instance ID to use when running the YCSB benchmark, e.g. 'ycsb-instance'.
     */
    static final String INSTANCE = "cloudspanner.instance";
    /**
     * Choose between 'read' and 'query'. Affects both read() and scan() operations.
     */
    static final String READ_MODE = "cloudspanner.readmode";
    /**
     * The number of inserts to batch during the bulk loading phase. The default value is 1, which means no batching
     * is done. Recommended value during data load is 1000.
     */
    static final String BATCH_INSERTS = "cloudspanner.batchinserts";
    /**
     * Number of seconds we allow reads to be stale for. Set to 0 for strong reads (default).
     * For performance gains, this should be set to 10 seconds.
     */
    static final String BOUNDED_STALENESS = "cloudspanner.boundedstaleness";

    // The properties below usually do not need to be set explicitly.

    /**
     * The Cloud Spanner project ID to use when running the YCSB benchmark, e.g. 'myproject'. This is not strictly
     * necessary and can often be inferred from the environment.
     */
    static final String PROJECT = "cloudspanner.project";
    /**
     * The Cloud Spanner host name to use in the YCSB run.
     */
    static final String HOST = "cloudspanner.host";
    /**
     * Number of Cloud Spanner client channels to use. It's recommended to leave this to be the default value.
     */
    static final String NUM_CHANNELS = "cloudspanner.channels";
    /**
     * Enables dynamic channel pooling (grpc-gcp). When enabled, cloudspanner.channels is ignored.
     */
    static final String DYNAMIC_CHANNEL_POOL_ENABLED =
        "cloudspanner.dynamicchannelpool.enabled";
    /**
     * Initial number of channels in the dynamic channel pool.
     */
    static final String DYNAMIC_CHANNEL_POOL_INIT_SIZE =
        "cloudspanner.dynamicchannelpool.initsize";
    /**
     * Minimum number of channels in the dynamic channel pool.
     */
    static final String DYNAMIC_CHANNEL_POOL_MIN_SIZE =
        "cloudspanner.dynamicchannelpool.minsize";
    /**
     * Maximum number of channels in the dynamic channel pool.
     */
    static final String DYNAMIC_CHANNEL_POOL_MAX_SIZE =
        "cloudspanner.dynamicchannelpool.maxsize";
    /**
     * Minimum desired average concurrent RPCs per channel for grpc-gcp dynamic scaling.
     */
    static final String DYNAMIC_CHANNEL_POOL_MIN_RPC_PER_CHANNEL =
        "cloudspanner.dynamicchannelpool.minrpcperchannel";
    /**
     * Maximum desired average concurrent RPCs per channel for grpc-gcp dynamic scaling.
     */
    static final String DYNAMIC_CHANNEL_POOL_MAX_RPC_PER_CHANNEL =
        "cloudspanner.dynamicchannelpool.maxrpcperchannel";
    /**
     * Minimum streams per channel before grpc-gcp adds another channel in non-dynamic mode.
     */
    static final String DYNAMIC_CHANNEL_POOL_CONCURRENT_STREAMS_LOW_WATERMARK =
            "cloudspanner.dynamicchannelpool.concurrentstreamslowwatermark";
    /**
     * Use plain text for communication.
     */
    static final String USE_PLAINTEXT = "cloudspanner.useplaintext";
    
    /**
     * Connect to Experimental host instead of cloud spanner API.
     */
    static final String EXPERIMENTAL_HOST="cloudspanner.experimentalhost";    
  }

  private static int fieldCount;

  private static boolean queriesForReads;

  private static int batchInserts;

  private static TimestampBound timestampBound;

  private static String standardQuery;

  private static String standardScan;

  private static final ArrayList<String> STANDARD_FIELDS = new ArrayList<>();

  private static final String PRIMARY_KEY_COLUMN = "id";

  private static final Logger LOGGER = Logger.getLogger(CloudSpannerClient.class.getName());

  // Static lock for the class.
  private static final Object CLASS_LOCK = new Object();

  // Single Spanner client per process.
  private static Spanner spanner = null;

  // Single database client per process.
  private static DatabaseClient dbClient = null;

  // Buffered mutations on a per object/thread basis for batch inserts.
  // Note that we have a separate CloudSpannerClient object per thread.
  private final ArrayList<Mutation> bufferedMutations = new ArrayList<>();

  private static void constructStandardQueriesAndFields(Properties properties) {
    String table = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    final String fieldprefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
                                                      CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
    standardQuery = new StringBuilder()
        .append("SELECT * FROM ").append(table).append(" WHERE id=@key").toString();
    standardScan = new StringBuilder()
        .append("SELECT * FROM ").append(table).append(" WHERE id>=@startKey LIMIT @count").toString();
    for (int i = 0; i < fieldCount; i++) {
      STANDARD_FIELDS.add(fieldprefix + i);
    }
  }

  private static Spanner getSpanner(Properties properties, String host, String project) {
    if (spanner != null) {
      return spanner;
    }
    String numChannels = properties.getProperty(CloudSpannerProperties.NUM_CHANNELS);
    boolean dynamicChannelPoolEnabled =
        Boolean.parseBoolean(
            properties.getProperty(
                CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_ENABLED, String.valueOf(false)));
    int numThreads = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder()
        .setSessionPoolOption(SessionPoolOptions.newBuilder()
            .setMinSessions(numThreads)
            // Since we have no read-write transactions, we can set the write session fraction to 0.
            .setWriteSessionsFraction(0)
            .build());
    if (host != null) {
      optionsBuilder.setHost(host);
    }
    if (project != null) {
      optionsBuilder.setProjectId(project);
    }
    if (dynamicChannelPoolEnabled) {
      GcpChannelPoolOptions defaultChannelPoolOptions =
          SpannerOptions.createDefaultDynamicChannelPoolOptions();
      int initSize =
          Integer.parseInt(
              properties.getProperty(CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_INIT_SIZE, "1"));
      int minSize =
          Integer.parseInt(
              properties.getProperty(CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MIN_SIZE, "1"));
      int maxSize =
          Integer.parseInt(
              properties.getProperty(CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MAX_SIZE, "256"));
      int concurrentStreamsLowWatermark =
              Integer.parseInt(
                      properties.getProperty(
                              CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_CONCURRENT_STREAMS_LOW_WATERMARK,
                              "100"));
      if (minSize > initSize || initSize > maxSize) {
        throw new IllegalArgumentException(
            "Invalid dynamic channel pool sizes: expected min <= init <= max, got min="
                + minSize
                + ", init="
                + initSize
                + ", max="
                + maxSize);
      }
      GcpChannelPoolOptions.Builder channelPoolOptionsBuilder =
          GcpChannelPoolOptions.newBuilder(defaultChannelPoolOptions)
              .setMinSize(minSize)
              .setInitSize(initSize)
              .setMaxSize(maxSize);
      if (properties.containsKey(CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MIN_RPC_PER_CHANNEL)
          || properties.containsKey(
              CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MAX_RPC_PER_CHANNEL)) {
        int minRpcPerChannel =
            Integer.parseInt(
                properties.getProperty(
                    CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MIN_RPC_PER_CHANNEL,
                    String.valueOf(defaultChannelPoolOptions.getMinRpcPerChannel())));
        int maxRpcPerChannel =
            Integer.parseInt(
                properties.getProperty(
                    CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MAX_RPC_PER_CHANNEL,
                    String.valueOf(defaultChannelPoolOptions.getMaxRpcPerChannel())));
        channelPoolOptionsBuilder.setDynamicScaling(
            minRpcPerChannel, maxRpcPerChannel, defaultChannelPoolOptions.getScaleDownInterval());
      }
      if (properties.containsKey(
          CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_CONCURRENT_STREAMS_LOW_WATERMARK)) {
        channelPoolOptionsBuilder.setConcurrentStreamsLowWatermark(
            concurrentStreamsLowWatermark);
      }
      optionsBuilder.enableDynamicChannelPool();
      optionsBuilder.setGcpChannelPoolOptions(channelPoolOptionsBuilder.build());
      if (numChannels != null) {
        LOGGER.log(
            Level.INFO,
            "Ignoring {0}={1} because dynamic channel pool is enabled.",
            new Object[] {CloudSpannerProperties.NUM_CHANNELS, numChannels});
      }
    } else if (numChannels != null) {
      optionsBuilder.setNumChannels(Integer.parseInt(numChannels));
    }
    boolean usePlaintext = Boolean.parseBoolean(properties.getProperty(CloudSpannerProperties.USE_PLAINTEXT));
    if (usePlaintext) {
      optionsBuilder.setChannelConfigurator(ManagedChannelBuilder::usePlaintext);
      //Disable credentials and built in metrics when plaintext is used.
      optionsBuilder.setCredentials(NoCredentials.getInstance());
      optionsBuilder.setBuiltInMetricsEnabled(false);
    }
    String experimentalHost = properties.getProperty(CloudSpannerProperties.EXPERIMENTAL_HOST);
    if (experimentalHost != null) {
      optionsBuilder.setExperimentalHost(experimentalHost);
    }

    spanner = optionsBuilder.build().getService();
    Runtime.getRuntime().addShutdownHook(new Thread("spannerShutdown") {
        @Override
        public void run() {
          spanner.close();
        }
      });
    return spanner;
  }

  @Override
  public void init() throws DBException {
    synchronized (CLASS_LOCK) {
      if (dbClient != null) {
        return;
      }
      Properties properties = getProperties();
      String host = properties.getProperty(CloudSpannerProperties.HOST);
      String project = properties.getProperty(CloudSpannerProperties.PROJECT);
      String instance = properties.getProperty(CloudSpannerProperties.INSTANCE, "ycsb-instance");
      String database = properties.getProperty(CloudSpannerProperties.DATABASE, "ycsb-database");

      fieldCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      queriesForReads = properties.getProperty(CloudSpannerProperties.READ_MODE, "query").equals("query");
      batchInserts = Integer.parseInt(properties.getProperty(CloudSpannerProperties.BATCH_INSERTS, "1"));
      constructStandardQueriesAndFields(properties);

      int boundedStalenessSeconds = Integer.parseInt(properties.getProperty(
          CloudSpannerProperties.BOUNDED_STALENESS, "0"));
      timestampBound = (boundedStalenessSeconds <= 0) ?
          TimestampBound.strong() : TimestampBound.ofMaxStaleness(boundedStalenessSeconds, TimeUnit.SECONDS);

      try {
        spanner = getSpanner(properties, host, project);
        if (project == null) {
          project = spanner.getOptions().getProjectId();
        }
        dbClient = spanner.getDatabaseClient(DatabaseId.of(project, instance, database));
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "init()", e);
        throw new DBException(e);
      }

      LOGGER.log(Level.INFO, new StringBuilder()
          .append("\nHost: ").append(spanner.getOptions().getHost())
          .append("\nProject: ").append(project)
          .append("\nInstance: ").append(instance)
          .append("\nDatabase: ").append(database)
          .append("\nUsing queries for reads: ").append(queriesForReads)
          .append("\nBatching inserts: ").append(batchInserts)
          .append("\nBounded staleness seconds: ").append(boundedStalenessSeconds)
          .append("\nConfigured num channels: ")
          .append(properties.getProperty(CloudSpannerProperties.NUM_CHANNELS, "<default>"))
          .append("\nDynamic channel pool enabled: ")
          .append(spanner.getOptions().isDynamicChannelPoolEnabled())
          .append("\nConfigured dynamic pool init/min/max: ")
          .append(
              properties.getProperty(CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_INIT_SIZE, "<default>"))
          .append("/")
          .append(
              properties.getProperty(CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MIN_SIZE, "<default>"))
          .append("/")
          .append(
              properties.getProperty(CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MAX_SIZE, "<default>"))
          .append("\nConfigured dynamic pool min/max RPC per channel: ")
          .append(
              properties.getProperty(
                  CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MIN_RPC_PER_CHANNEL, "<default>"))
          .append("/")
          .append(
              properties.getProperty(
                  CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_MAX_RPC_PER_CHANNEL, "<default>"))
          .append("\nConfigured dynamic pool stream low watermark: ")
          .append(
              properties.getProperty(
                      CloudSpannerProperties.DYNAMIC_CHANNEL_POOL_CONCURRENT_STREAMS_LOW_WATERMARK,
                      "<default>"))
          .toString());
    }
  }

  private Status readUsingQuery(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Statement query;
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    if (fields == null || fields.size() == fieldCount) {
      query = Statement.newBuilder(standardQuery).bind("key").to(key).build();
    } else {
      Joiner joiner = Joiner.on(',');
      query = Statement.newBuilder("SELECT ")
          .append(joiner.join(fields))
          .append(" FROM ")
          .append(table)
          .append(" WHERE id=@key")
          .bind("key").to(key)
          .build();
    }
    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      resultSet.next();
      decodeStruct(columns, resultSet, result);
      if (resultSet.next()) {
        throw new Exception("Expected exactly one row for each read.");
      }

      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "readUsingQuery()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (queriesForReads) {
      return readUsingQuery(table, key, fields, result);
    }
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    try {
      Struct row = dbClient.singleUse(timestampBound).readRow(table, Key.of(key), columns);
      decodeStruct(columns, row, result);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "read()", e);
      return Status.ERROR;
    }
  }

  private Status scanUsingQuery(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    Statement query;
    if (fields == null || fields.size() == fieldCount) {
      query = Statement.newBuilder(standardScan).bind("startKey").to(startKey).bind("count").to(recordCount).build();
    } else {
      Joiner joiner = Joiner.on(',');
      query = Statement.newBuilder("SELECT ")
          .append(joiner.join(fields))
          .append(" FROM ")
          .append(table)
          .append(" WHERE id>=@startKey LIMIT @count")
          .bind("startKey").to(startKey)
          .bind("count").to(recordCount)
          .build();
    }
    try (ResultSet resultSet = dbClient.singleUse(timestampBound).executeQuery(query)) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scanUsingQuery()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    if (queriesForReads) {
      return scanUsingQuery(table, startKey, recordCount, fields, result);
    }
    Iterable<String> columns = fields == null ? STANDARD_FIELDS : fields;
    KeySet keySet =
        KeySet.newBuilder().addRange(KeyRange.closedClosed(Key.of(startKey), Key.of())).build();
    try (ResultSet resultSet = dbClient.singleUse(timestampBound)
                                       .read(table, keySet, columns, Options.limit(recordCount))) {
      while (resultSet.next()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        decodeStruct(columns, resultSet, row);
        result.add(row);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scan()", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
    m.set(PRIMARY_KEY_COLUMN).to(key);
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      m.set(e.getKey()).to(e.getValue().toString());
    }
    try {
      dbClient.writeAtLeastOnce(Arrays.asList(m.build()));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "update()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (bufferedMutations.size() < batchInserts) {
      Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(table);
      m.set(PRIMARY_KEY_COLUMN).to(key);
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
        m.set(e.getKey()).to(e.getValue().toString());
      }
      bufferedMutations.add(m.build());
    } else {
      LOGGER.log(Level.INFO, "Limit of cached mutations reached. The given mutation with key " + key +
          " is ignored. Is this a retry?");
    }
    if (bufferedMutations.size() < batchInserts) {
      return Status.BATCHED_OK;
    }
    try {
      dbClient.writeAtLeastOnce(bufferedMutations);
      bufferedMutations.clear();
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "insert()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public void cleanup() {
    try {
      if (bufferedMutations.size() > 0) {
        dbClient.writeAtLeastOnce(bufferedMutations);
        bufferedMutations.clear();
      }
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "cleanup()", e);
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      dbClient.writeAtLeastOnce(Arrays.asList(Mutation.delete(table, Key.of(key))));
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "delete()", e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  private static void decodeStruct(
      Iterable<String> columns, StructReader structReader, Map<String, ByteIterator> result) {
    for (String col : columns) {
      result.put(col, new StringByteIterator(structReader.getString(col)));
    }
  }
}
