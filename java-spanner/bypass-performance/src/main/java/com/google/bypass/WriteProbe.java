package com.google.bypass;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import java.time.Duration;
import java.util.Collections;

/** Write prober performs blind write to Spanner. */
public class WriteProbe implements Probe {
  private final DatabaseClient client;
  private final int numRows;
  private final int payloadSize;
  private final boolean replayProtection;

  public WriteProbe(DatabaseClient client, int numRows, int payloadSize, boolean replayProtection) {
    this.client = client;
    this.numRows = numRows;
    this.payloadSize = payloadSize;
    this.replayProtection = replayProtection;
  }

  @Override
  public String getName() {
    return replayProtection ? "write" : "write_no_rp";
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
    if (replayProtection) {
      var unused =
          client.writeWithOptions(
              Collections.singletonList(createMutation(key)),
              Options.tag("probe_type=" + getName()),
              Options.maxCommitDelay(Duration.ZERO));
    } else {
      var unused =
          client.writeAtLeastOnceWithOptions(
              Collections.singletonList(createMutation(key)),
              Options.tag("probe_type=" + getName()),
              Options.maxCommitDelay(Duration.ZERO));
    }
  }
}
