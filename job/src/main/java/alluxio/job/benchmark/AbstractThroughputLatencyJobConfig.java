/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.client.WriteType;

import com.google.common.base.Objects;

/**
 * The base configuration for the throughput latency related benchmarks.
 */
public abstract class AbstractThroughputLatencyJobConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = -4312014263857861337L;

  private WriteType mWriteType;
  private final int mLoad;
  private final double mExpectedThroughput;
  private final String mWorkDir;

  /**
   * Creates an instance of AbstractThroughputAndLatencyJobConfig.
   *
   * @param load the load (the total number of operations) to put on the server
   * @param expectedThroughput the expected throughput
   * @param workDir the working directory
   * @param threadNum the number of client threads
   * @param fileSystemType the type of file system to use
   * @param verbose whether to print verbose result
   * @param cleanUp whether to clean up after the test
   */
  public AbstractThroughputLatencyJobConfig(String writeType, int load, double expectedThroughput,
      String workDir, int threadNum, String fileSystemType, boolean verbose, boolean cleanUp) {
    super(threadNum, 1, fileSystemType, verbose, cleanUp);
    mLoad = load;
    mExpectedThroughput = expectedThroughput;
    mWorkDir = workDir;
    mWriteType = WriteType.valueOf(writeType);
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
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return the work directory
   */
  public String getWorkDir() {
    return mWorkDir;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("load", mLoad)
        .add("expectedThroughput", mExpectedThroughput)
        .add("workDir", mWorkDir)
        .add("threadNum", getThreadNum())
        .add("cleanUp", isCleanUp()).toString();
  }
}
