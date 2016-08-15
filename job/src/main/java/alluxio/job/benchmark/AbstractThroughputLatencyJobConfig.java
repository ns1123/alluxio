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
  private boolean mLocal;
  private boolean mShuffleLoad;

  /**
   * Creates an instance of AbstractThroughputAndLatencyJobConfig.
   *
   * @param writeType the write type
   * @param load the load (the total number of operations) to put on the server
   * @param concurrency the concurrency. The same task (e.g. list status on a file) will be executed
   *                    this many times. Note that it is not guaranteed that these duplicate tasks
   *                    will run concurrently.
   * @param expectedThroughput the expected throughput
   * @param workDir the working directory
   * @param local whether run the tasks with locality
   * @param threadNum the number of client threads
   * @param fileSystemType the type of file system to use
   * @param shuffleLoad whether to shuffle the load
   * @param verbose whether to print verbose result
   * @param cleanUp whether to clean up after the test
   */
  public AbstractThroughputLatencyJobConfig(String writeType, int load, int concurrency, double expectedThroughput,
      String workDir, boolean local, int threadNum, String fileSystemType, boolean shuffleLoad, boolean verbose,
      boolean cleanUp) {
    super(threadNum, 1, fileSystemType, verbose, cleanUp);
    mLoad = load * concurrency;
    mExpectedThroughput = expectedThroughput;
    mWorkDir = workDir;
    mLocal = local;
    mWriteType = WriteType.valueOf(writeType);
    mShuffleLoad = shuffleLoad;
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

  /**
   * @return whether to shuffle the load
   */
  public boolean isShuffleLoad() {
    return mShuffleLoad;
  }

  /**
   * @return whether to run tasks with locality
   */
  public boolean isLocal() {
    return mLocal;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("writeType", mWriteType)
        .add("load", mLoad)
        .add("expectedThroughput", mExpectedThroughput)
        .add("workDir", mWorkDir)
        .add("local", mLocal)
        .add("shuffleLoad", mShuffleLoad)
        .add("threadNum", getThreadNum())
        .add("cleanUp", isCleanUp()).toString();
  }
}
