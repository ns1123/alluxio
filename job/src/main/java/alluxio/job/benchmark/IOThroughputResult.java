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

  /**
   * Creates a new instance of {@link IOThroughputResult}.
   *
   * @param throughput the throughput
   */
  public IOThroughputResult(double throughput) {
    mThroughput = throughput;
  }

  /**
   * @return the throughput of the operation
   */
  public double getThroughput() {
    return mThroughput;
  }
}
