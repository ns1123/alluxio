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
 * Records the IO throughput of the async write benchmark operation.
 */
public class AsyncIOThroughputResult extends IOThroughputResult {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private double mAsyncWriteThroughput;

  /**
   * Creates a new instance of {@link AsyncIOThroughputResult}.
   *
   * @param throughput the throughput of writing into Alluxio
   * @param asyncThroughput the throughput of writing into under storage
   * @param duration the duration of one batch of the test in milliseconds
   */
  public AsyncIOThroughputResult(double throughput, double asyncThroughput, double duration) {
    super(throughput, duration);
    mAsyncWriteThroughput = asyncThroughput;
  }

  /**
   * @return the throughput from writing to Alluxio to asynchronously writing to under storage
   */
  public double getAsyncThroughput() {
    return mAsyncWriteThroughput;
  }
}
