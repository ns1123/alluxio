/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

/**
 * The base configuration for the throughput latency related benchmarks.
 */
public class ThroughputLatencyJobConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = -4312014263857861337L;

  private final int mLoad;
  private final double mExpectedThroughput;
  private final String mName;

  private final long mStartTimeNano;
  /**
   * Creates an instance of AbstractThroughputAndLatencyJobConfig.
   * @param threadNum the number of threads
   *
   */
  public ThroughputLatencyJobConfig(String name, int threadNum, boolean cleanUp, int load,
      double expectedThroughput) {
    super(threadNum, 1, cleanUp);
    mName = name;
    mLoad = load;
    mExpectedThroughput = expectedThroughput;
    mStartTimeNano = System.nanoTime();
  }

  @Override
  public String getName() {
    return mName;
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
}