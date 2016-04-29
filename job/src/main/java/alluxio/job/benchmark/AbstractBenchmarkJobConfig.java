/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.job.JobConfig;

/**
 * The abstract configuration for all the benchmark jobs.
 * By default, the same task runs in some threads in parallel (called one batch). Then repeat
 * this several times.
 */
public abstract class AbstractBenchmarkJobConfig implements JobConfig {
  private static final long serialVersionUID = -1332267808035280727L;

  /** The number of threads to run the benchmark with. */
  private int mThreadNum;

  /** The number of batches to run sequentially. */
  private int mBatchNum;

  /**
   * Creates a new instance of {@link AbstractBenchmarkJobConfig}.
   *
   * @param threadNum the number of threads
   */
  public AbstractBenchmarkJobConfig(int threadNum, int batchNum) {
    mThreadNum = threadNum;
    mBatchNum = batchNum;
  }

  /**
   * @return the number of threads to run the benchmark with
   */
  public int getThreadNum() {
    return mThreadNum;
  }

  /**
   * @return the number of batches to run in sequential
   */
  public int getBatchNum() {
    return mBatchNum;
  }
}
