package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;

/** Strong read prober. */
public class StrongReadProbe implements Probe {
  private final DatabaseClient client;
  private final int numRows;

  public StrongReadProbe(DatabaseClient client, int numRows) {
    this.client = client;
    this.numRows = numRows;
  }

  @Override
  public String getName() {
    return "strong_read";
  }

  @Override
  public void probe() {
    int k = (int) (Math.random() * numRows);
    try (ResultSet rs =
        client
            .singleUse()
            .read(
                TABLE,
                KeySet.singleKey(Key.of(k)),
                COLUMNS,
                Options.tag("probe_type=" + getName()))) {
      if (rs.next()) {
        Probe.validateRows(rs);
      }
    }
  }
}
