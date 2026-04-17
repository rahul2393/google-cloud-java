/**                                                                                                                                                                                
 * Copyright (c) 2016 YCSB contributors. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */
package site.ycsb.workloads;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Properties;

import org.testng.annotations.Test;

import site.ycsb.Client;
import site.ycsb.WorkloadException;
import site.ycsb.generator.AcknowledgedCounterGenerator;
import site.ycsb.generator.DiscreteGenerator;

public class TestCoreWorkload {

  @Test
  public void createOperationChooser() {
    final Properties p = new Properties();
    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0.20");
    p.setProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "0.20");
    final DiscreteGenerator generator = CoreWorkload.createOperationGenerator(p);
    final int[] counts = new int[5];
    
    for (int i = 0; i < 100; ++i) {
      switch (generator.nextString()) {
      case "READ":
        ++counts[0];
        break;
      case "UPDATE":
        ++counts[1];
        break;
      case "INSERT": 
        ++counts[2];
        break;
      case "SCAN":
        ++counts[3];
        break;
      default:
        ++counts[4];
      } 
    }
    
    for (int i : counts) {
      // Doesn't do a wonderful job of equal distribution, but in a hundred, if we 
      // don't see at least one of each operation then the generator is really broke.
      assertTrue(i > 1);
    }
  }
  
  @Test (expectedExceptions = IllegalArgumentException.class)
  public void createOperationChooserNullProperties() {
    CoreWorkload.createOperationGenerator(null);
  }

  @Test
  public void requestPartitionRangeWithFixedSizeShards() throws WorkloadException {
    CoreWorkload.RequestPartitionRange range =
        CoreWorkload.requestPartitionRange(0, 999, 4, 2, 100);

    assertEquals(600L, range.lowerBound());
    assertEquals(699L, range.upperBound());
    assertEquals("fixed-size shard 2/4 (shard=6 size=100)", range.description());
  }

  @Test
  public void requestPartitionRangeWithContiguousPartitions() throws WorkloadException {
    CoreWorkload.RequestPartitionRange range =
        CoreWorkload.requestPartitionRange(100, 199, 4, 1, 0);

    assertEquals(125L, range.lowerBound());
    assertEquals(149L, range.upperBound());
    assertEquals("contiguous partition 1/4", range.description());
  }

  @Test(expectedExceptions = WorkloadException.class)
  public void zipfianPartitionsRejectInsertProportion() throws WorkloadException {
    Properties p = new Properties();
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000");
    p.setProperty(CoreWorkload.REQUEST_PARTITION_COUNT_PROPERTY, "2");
    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0.1");

    CoreWorkload.createRequestDistributionState(
        p, "zipfian", 0, 1000, 1000, new AcknowledgedCounterGenerator(1000));
  }

  @Test
  public void zipfianFixedSizeLastPartitionDoesNotCollapseToSingleKey() throws WorkloadException {
    Properties p = new Properties();
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000000000000");
    p.setProperty(CoreWorkload.REQUEST_PARTITION_COUNT_PROPERTY, "20");
    p.setProperty(CoreWorkload.REQUEST_PARTITION_INDEX_PROPERTY, "19");
    p.setProperty(CoreWorkload.REQUEST_PARTITION_SIZE_PROPERTY, "100000000");

    CoreWorkload.RequestDistributionState state =
        CoreWorkload.createRequestDistributionState(
            p,
            "zipfian",
            0,
            1000000000000L,
            1000000000000L,
            new AcknowledgedCounterGenerator(1000000000000L));

    assertEquals(999900000000L, state.lowerBound());
    assertEquals(999999999999L, state.upperBound());
    assertEquals("fixed-size shard 19/20 (shard=9999 size=100000000)", state.partitionDescription());
  }
}
