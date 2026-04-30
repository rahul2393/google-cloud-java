package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

/** Probe for multi-use read-only transaction queries. */
public class MultiUseReadOnlyQueryProbe implements Probe {
  private final DatabaseClient client;
  private final int numRows;

  public MultiUseReadOnlyQueryProbe(DatabaseClient client, int numRows) {
    this.client = client;
    this.numRows = numRows;
  }

  @Override
  public String getName() {
    return "multi_use_ro_query";
  }

  @Override
  public void probe() {
    int firstKey = (int) (Math.random() * numRows);
    int secondKey = (int) (Math.random() * numRows);
    try (ReadOnlyTransaction txn = client.readOnlyTransaction()) {
      runSingleRead(txn, firstKey);
      runSingleQuery(txn, secondKey);
    }
  }

  private void runSingleRead(ReadOnlyTransaction txn, int key) {
    try (ResultSet rs =
        txn.read(
            TABLE,
            KeySet.singleKey(Key.of(key)),
            COLUMNS,
            Options.tag("probe_type=" + getName()))) {
      if (rs.next()) {
        Probe.validateRows(rs);
      }
    }
  }

  private void runSingleQuery(ReadOnlyTransaction txn, int key) {
    Statement statement =
        Statement.newBuilder(
                String.format(
                    "select %s, %s from %s where %s = @Id",
                    COLUMNS.get(0), COLUMNS.get(1), TABLE, COLUMNS.get(0)))
            .bind("Id")
            .to(key)
            .build();
    try (ResultSet rs = txn.executeQuery(statement, Options.tag("probe_type=" + getName()))) {
      if (rs.next()) {
        Probe.validateRows(rs);
      }
    }
  }
}
