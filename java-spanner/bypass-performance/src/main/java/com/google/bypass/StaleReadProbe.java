package com.google.bypass;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TimestampBound;

/** Stale read prober. */
public class StaleReadProbe implements Probe {
  private final DatabaseClient client;
  private final int numRows;
  private final long maxStaleness;

  public StaleReadProbe(DatabaseClient client, int numRows, long maxStaleness) {
    this.client = client;
    this.numRows = numRows;
    this.maxStaleness = maxStaleness;
  }

  @Override
  public String getName() {
    return "stale_read";
  }

  @Override
  public void probe() {
    int k = (int) (Math.random() * numRows);

    try (ResultSet rs =
        client
            .singleUse(TimestampBound.ofMaxStaleness(maxStaleness, SECONDS))
            .read(
                TABLE,
                KeySet.singleKey(Key.of(k)),
                COLUMNS,
                Options.tag("probe_type=" + getName()))) {
      if (!rs.next()) {
        throw new IllegalStateException("No rows returned for key: " + k);
      }
    }
  }
}
