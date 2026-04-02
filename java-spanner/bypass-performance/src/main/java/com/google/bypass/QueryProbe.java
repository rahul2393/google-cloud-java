package com.google.bypass;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;

/** Query prober. */
public class QueryProbe implements Probe {
  private final DatabaseClient client;
  private final int numRows;
  private final String name;
  private final TimestampBound timestampBound;

  public QueryProbe(DatabaseClient client, int numRows) {
    this(client, numRows, 0);
  }

  public QueryProbe(DatabaseClient client, int numRows, long maxStaleness) {
    this.client = client;
    this.numRows = numRows;
    this.name = maxStaleness == 0 ? "strong_query" : "stale_query";
    this.timestampBound =
        maxStaleness == 0
            ? TimestampBound.strong()
            : TimestampBound.ofMaxStaleness(maxStaleness, SECONDS);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void probe() {
    int key = (int) (Math.random() * numRows);
    Statement s =
        Statement.newBuilder(
                String.format(
                    "select %s, %s from %s where %s =" + " @Id",
                    COLUMNS.get(0), COLUMNS.get(1), TABLE, COLUMNS.get(0)))
            .bind("Id")
            .to(key)
            .build();
    try (ResultSet rs =
        client.singleUse(timestampBound).executeQuery(s, Options.tag("probe_type=" + getName()))) {
      if (rs.next()) {
        Probe.validateRows(rs);
      }
    }
  }
}
