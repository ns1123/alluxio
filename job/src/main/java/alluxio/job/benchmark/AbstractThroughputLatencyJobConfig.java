/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * The base configuration for the throughput latency related benchmarks.
 */
public abstract class AbstractThroughputLatencyJobConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = -4312014263857861337L;

  private final int mLoad;
  private final double mExpectedThroughput;
  private final long mStartTimeNano;

  /**
   * Creates an instance of AbstractThroughputAndLatencyJobConfig.
   *
   * @param threadNum the number of client threads
   * @param cleanUp whether to clean up after the test
   * @param load the load to put on the server
   * @param expectedThroughput the expected throughput used to stress the server
   */
  public AbstractThroughputLatencyJobConfig(int load, double expectedThroughput, int threadNum,
      boolean cleanUp) {
    super(threadNum, 1, cleanUp);
    mLoad = load;
    mExpectedThroughput = expectedThroughput;
    mStartTimeNano = System.nanoTime();
  }

  /**
   * @return the load
   */
  public int getLoad() {
    return mLoad;
  }

  /**
   * @return the expected throughput
   */
  public double getExpectedThroughput() {
    return mExpectedThroughput;
  }

  /**
   * @return the start time in nano second
   */
  public long getStartTimeNano() {
    return mStartTimeNano;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("startTime", mStartTimeNano)
        .add("load", mLoad).add("expectedThroughput", mExpectedThroughput).toString();
  }
}