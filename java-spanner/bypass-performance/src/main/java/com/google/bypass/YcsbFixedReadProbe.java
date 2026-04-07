package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import java.util.logging.Logger;

/** Probe that mirrors the YCSB Cloud Spanner strong-read path for a fixed key. */
public final class YcsbFixedReadProbe implements Probe {
  private static final Logger logger = Logger.getLogger(YcsbFixedReadProbe.class.getName());
  private static final String DEFAULT_KEY_PREFIX = "user";

  private final DatabaseClient client;
  private final String table;
  private final String key;
  private final ImmutableList<String> columns;

  public YcsbFixedReadProbe(
      DatabaseClient client, String table, String explicitKey, String userId, int zeroPadding) {
    this.client = client;
    this.table = table;
    this.key = buildKey(explicitKey, userId, zeroPadding);
    this.columns = buildColumns();
  }

  @Override
  public String getName() {
    return "ycsb_fixed_read";
  }

  @Override
  public void probe() {
    logger.fine(
        String.format("Issuing YCSB fixed read. table=%s key=%s columns=%s", table, key, columns));
    try (ResultSet rs =
        client
            .singleUse()
            .read(
                table,
                KeySet.singleKey(Key.of(key)),
                columns,
                Options.tag("probe_type=" + getName()))) {
      if (!rs.next()) {
        throw new IllegalStateException(
            String.format("No row returned for YCSB fixed read. table=%s key=%s", table, key));
      }
      Struct row = rs.getCurrentRowAsStruct();
      if (rs.next()) {
        throw new IllegalStateException(
            String.format(
                "Expected exactly one row for YCSB fixed read. table=%s key=%s", table, key));
      }
      logger.fine(
          String.format(
              "Completed YCSB fixed read. table=%s key=%s columns_read=%d struct_fields=%d",
              table, key, columns.size(), row.getType().getStructFields().size()));
    }
  }

  static String buildKey(String explicitKey, String userId, int zeroPadding) {
    if (explicitKey != null && !explicitKey.trim().isEmpty()) {
      return explicitKey.trim();
    }
    String trimmedUserId = userId.trim();
    int zeroCount = Math.max(0, zeroPadding - trimmedUserId.length());
    StringBuilder builder =
        new StringBuilder(DEFAULT_KEY_PREFIX.length() + zeroCount + trimmedUserId.length());
    builder.append(DEFAULT_KEY_PREFIX);
    for (int i = 0; i < zeroCount; i++) {
      builder.append('0');
    }
    builder.append(trimmedUserId);
    return builder.toString();
  }

  private static ImmutableList<String> buildColumns() {
    ImmutableList.Builder<String> columns = ImmutableList.builder();
    columns.add("field0");
    columns.add("field1");
    columns.add("field2");
    columns.add("field3");
    columns.add("field4");
    columns.add("field5");
    columns.add("field6");
    columns.add("field7");
    columns.add("field8");
    columns.add("field9");
    return columns.build();
  }
}
