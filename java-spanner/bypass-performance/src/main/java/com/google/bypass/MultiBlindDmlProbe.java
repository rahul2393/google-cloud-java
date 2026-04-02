package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Statement;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;

/** Multi statement blind DML prober. */
public class MultiBlindDmlProbe implements Probe {

  @VisibleForTesting
  static final String INSERT_OR_UPDATE_DML =
      String.format(
          "insert or update %s (%s, %s) VALUES(@Id, @payload)",
          TABLE, COLUMNS.get(0), COLUMNS.get(1));

  private final DatabaseClient client;
  private final int numRows;
  private final int payloadSize;
  private final int numDmlStatements;

  public MultiBlindDmlProbe(
      DatabaseClient client, int numRows, int payloadSize, int numDmlStatements) {
    this.client = client;
    this.numRows = numRows;
    this.payloadSize = payloadSize;
    this.numDmlStatements = numDmlStatements;
  }

  @Override
  public String getName() {
    return "multi_blind_dml";
  }

  @Override
  public void probe() {
    // Generate numDmlStatements distinct random rows.
    int[] keys = new int[numDmlStatements];
    // If number of rows is small (in unit tests), then generate rows sequentially.
    if (numRows <= numDmlStatements) {
      for (int i = 0; i < numDmlStatements; i++) {
        keys[i] = i;
      }
    } else {
      // Generate the first index randomly.
      keys[0] = (int) (Math.random() * numRows);
      // Generate the rest of the indexes sequentially.
      for (int i = 1; i < numDmlStatements; i++) {
        keys[i] = keys[i - 1] + 1;
      }
    }

    String payload = Probe.generateRandomString(payloadSize);
    client
        .readWriteTransaction(
            Options.tag("probe_type=" + getName()), Options.maxCommitDelay(Duration.ZERO))
        .run(
            txn -> {
              for (int j = 0; j < numDmlStatements; j++) {
                if (j == numDmlStatements - 1) {
                  // Specify laste_statement optimization on the last statement in this transaction.
                  txn.executeUpdate(
                      Statement.newBuilder(INSERT_OR_UPDATE_DML)
                          .bind("Id")
                          .to(keys[j])
                          .bind("payload")
                          .to(payload)
                          .build(),
                      Options.lastStatement());
                } else {
                  txn.executeUpdate(
                      Statement.newBuilder(INSERT_OR_UPDATE_DML)
                          .bind("Id")
                          .to(keys[j])
                          .bind("payload")
                          .to(payload)
                          .build());
                }
              }
              return null;
            });
  }
}
