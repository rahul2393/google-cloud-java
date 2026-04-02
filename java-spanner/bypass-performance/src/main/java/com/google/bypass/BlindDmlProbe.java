package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Statement;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;

/** DML prober. */
public class BlindDmlProbe implements Probe {

  @VisibleForTesting
  static final String INSERT_OR_UPDATE_DML =
      String.format(
          "insert or update %s (%s, %s) VALUES(@Id, @payload)",
          TABLE, COLUMNS.get(0), COLUMNS.get(1));

  private final DatabaseClient client;
  private final int numRows;
  private final int payloadSize;

  public BlindDmlProbe(DatabaseClient client, int numRows, int payloadSize) {
    this.client = client;
    this.numRows = numRows;
    this.payloadSize = payloadSize;
  }

  @Override
  public String getName() {
    return "blind_dml";
  }

  @Override
  public void probe() {
    int key = (int) (Math.random() * numRows);

    client
        .readWriteTransaction(
            Options.tag("probe_type=" + getName()), Options.maxCommitDelay(Duration.ZERO))
        .run(
            txn -> {
              txn.executeUpdate(
                  Statement.newBuilder(INSERT_OR_UPDATE_DML)
                      .bind("Id")
                      .to(key)
                      .bind("payload")
                      .to(Probe.generateRandomString(payloadSize))
                      .build(),
                  Options.lastStatement());
              return null;
            });
  }
}
