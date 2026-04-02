package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.common.annotations.VisibleForTesting;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;

/** DML prober. */
public class DmlProbe implements Probe {

  @VisibleForTesting
  static final String INSERT_DML =
      String.format(
          "insert %s (%s, %s) VALUES(@Id, @payload)", TABLE, COLUMNS.get(0), COLUMNS.get(1));

  private static final String UPDATE_DML =
      String.format(
          "update %s set %s = @payload where %s = @Id", TABLE, COLUMNS.get(1), COLUMNS.get(0));
  private final DatabaseClient client;
  private final int numRows;
  private final int payloadSize;
  private final boolean repeatableReadOcc;

  public DmlProbe(DatabaseClient client, int numRows, int payloadSize, boolean repeatableReadOcc) {
    this.client = client;
    this.numRows = numRows;
    this.payloadSize = payloadSize;
    this.repeatableReadOcc = repeatableReadOcc;
  }

  @Override
  public String getName() {
    return repeatableReadOcc ? "rr_occ_dml" : "dml";
  }

  @Override
  public void probe() {
    int key = (int) (Math.random() * numRows);
    String forUpdateClause = repeatableReadOcc ? " FOR UPDATE" : "";
    Statement s =
        // Sets a FOR UPDATE clause if it's a repeatable read transaction as we're updating the data
        // that was read, which is necessary for correctness in this isolation level.
        Statement.newBuilder(
                String.format(
                    "select %s, %s from %s where %s =" + " @Id" + forUpdateClause,
                    COLUMNS.get(0),
                    COLUMNS.get(1),
                    TABLE,
                    COLUMNS.get(0)))
            .bind("Id")
            .to(key)
            .build();

    IsolationLevel isolationLevel =
        repeatableReadOcc ? IsolationLevel.REPEATABLE_READ : IsolationLevel.SERIALIZABLE;
    client
        .readWriteTransaction(
            Options.isolationLevel(isolationLevel), Options.tag("probe_type=" + getName()))
        .run(
            txn -> {
              boolean update = false;
              try (ResultSet rs = txn.executeQuery(s)) {
                if (rs.next()) {
                  update = true;
                  Probe.validateRows(rs);
                }
              }
              txn.executeUpdate(
                  Statement.newBuilder(update ? UPDATE_DML : INSERT_DML)
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
