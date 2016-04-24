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
 */
public abstract class AbstractBenchmarkJobConfig implements JobConfig {
  private static final long serialVersionUID = -1332267808035280727L;

  /** The number of threads to run the benchmark with. */
  private int mThreadNum;

  /**
   * Creates a new instance of {@link AbstractBenchmarkJobConfig}.
   *
   * @param threadNum the number of threads
   */
  public AbstractBenchmarkJobConfig(int threadNum) {
    mThreadNum = threadNum;
  }

  /**
   * @return the number of threads to run the benchmark with
   */
  public int getThreadNum() {
    return mThreadNum;
  }
}
