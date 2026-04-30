package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.TransactionRunner;

/** Read/Write prober. */
public class ReadWriteProbe implements Probe {
  private final DatabaseClient client;
  private final int numRows;
  private final int payloadSize;

  public ReadWriteProbe(DatabaseClient client, int numRows, int payloadSize) {
    this.client = client;
    this.numRows = numRows;
    this.payloadSize = payloadSize;
  }

  @Override
  public String getName() {
    return "read_write";
  }

  private Mutation createMutation(int key) {
    return Mutation.newInsertOrUpdateBuilder(TABLE)
        .set(COLUMNS.get(0))
        .to(key)
        .set(COLUMNS.get(1))
        .to(Probe.generateRandomString(payloadSize))
        .build();
  }

  @Override
  public void probe() {
    int key = (int) (Math.random() * numRows);
    TransactionRunner runner = client.readWriteTransaction(Options.tag("probe_type=" + getName()));
    runner.run(
        txn -> {
          Probe.validateRows(txn.readRow(TABLE, Key.of(key), COLUMNS));
          txn.buffer(createMutation(key));
          return null;
        });
  }
}
