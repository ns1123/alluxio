/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link SimpleWriteDefinition}.
 */
public class SimpleWriteDefinitionTest {
  private static final SimpleWriteDefinition DEFINITION = new SimpleWriteDefinition();

  /**
   * Tests that average throughput and duration are properly calculated for three threads writing
   * 1GB of data, taking 1 second on average.
   */
  @Test
  public void processTest() {
    String fileSize = "1GB";
    int threadNum = 3;
    SimpleWriteConfig config = new SimpleWriteConfig("64MB", "4MB", fileSize, "ALLUXIO", 1,
        threadNum, "THROUGH", "/simple-read-write/", false, false, FreeAfterType.NONE.toString());
    List<List<Long>> timesNs = Lists.newArrayList();
    // Average time is 1 second, so average throughput is 1GB/s, or 1024MB/s.
    timesNs.add(Lists.newArrayList((long) 1e9, (long) 1.5e9, (long) 0.5e9));

    IOThroughputResult result = DEFINITION.process(config, timesNs);
    Assert.assertEquals(1024.0, result.getThroughput(), 0.00001);
    // 1e9 nanoseconds is 1000 milliseconds
    Assert.assertEquals(1000.0, result.getDuration(), 0.00001);
  }
}
