package site.ycsb;

import static org.testng.Assert.assertEquals;

import java.util.Properties;
import org.testng.annotations.Test;

public class TestClient {

  @Test
  public void operationCountUsesLongValuesForTransactions() {
    Properties props = new Properties();
    props.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000000000000");

    assertEquals(Client.operationCount(props, true), 1000000000000L);
  }

  @Test
  public void operationCountUsesLongValuesForLoads() {
    Properties props = new Properties();
    props.setProperty(Client.RECORD_COUNT_PROPERTY, "1000000000000");

    assertEquals(Client.operationCount(props, false), 1000000000000L);
  }

  @Test
  public void operationCountForThreadDistributesRemainderToEarlierThreads() {
    assertEquals(Client.operationCountForThread(10L, 3, 0), 4L);
    assertEquals(Client.operationCountForThread(10L, 3, 1), 3L);
    assertEquals(Client.operationCountForThread(10L, 3, 2), 3L);
  }
}
