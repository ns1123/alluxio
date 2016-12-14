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
 * Records the IO throughput of the benchmark operation.
 */
public class IOThroughputResult implements BenchmarkTaskResult {
  private static final long serialVersionUID = 1L;

  private double mThroughput;
  private double mDuration;

  /**
   * Creates a new instance of {@link IOThroughputResult}.
   *
   * @param throughput the throughput
   * @param duration the duration of one batch of the test in milliseconds
   */
  public IOThroughputResult(double throughput, double duration) {
    mThroughput = throughput;
    mDuration = duration;
  }

  /**
   * @return the throughput of the operation
   */
  public double getThroughput() {
    return mThroughput;
  }

  /**
   * @return the duration of the operation
   */
  public double getDuration() {
    return mDuration;
  }
}
