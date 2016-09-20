/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.Constants;
import alluxio.job.benchmark.IOThroughputResult;
import alluxio.job.benchmark.SimpleReadDefinition;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link SimpleReadDefinition}.
 */
public class SimpleReadDefinitionTest {
  /**
   * Tests that average throughput and duration are properly calculated for three threads reading
   * 1GB of data, taking 1 second on average.
   */
  @Test
  public void processTest() {
    SimpleReadDefinition definition = new SimpleReadDefinition();
    // Tell the read definition that it has read 1GB three times.
    ConcurrentLinkedQueue<Long> readBytesQueue = new ConcurrentLinkedQueue<Long>();
    readBytesQueue
        .addAll(Lists.newArrayList((long) Constants.GB, (long) Constants.GB, (long) Constants.GB));
    Whitebox.setInternalState(definition, "mReadBytesQueue", readBytesQueue);

    int threadNum = 3;
    SimpleReadConfig config =
        new SimpleReadConfig("64MB", "ALLUXIO", "NO_CACHE", threadNum, "/simple-read-write/",
            false, false);
    List<List<Long>> timesNs = Lists.newArrayList();
    // Average time is 1 second, so average throughput is 1GB/s, or 1024MB/s.
    timesNs.add(Lists.newArrayList((long) 1e9, (long) 1.5e9, (long) 0.5e9));

    IOThroughputResult result = definition.process(config, timesNs);
    Assert.assertEquals(1024.0, result.getThroughput(), 0.00001);
    // 1e9 nanoseconds is 1000 milliseconds
    Assert.assertEquals(1000.0, result.getDuration(), 0.00001);
  }
}
