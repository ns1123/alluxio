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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The abstract configuration for all the benchmark jobs. By default, the same task runs in some
 * threads in parallel (called one batch). Then repeat this several times.
 */
public abstract class AbstractBenchmarkJobConfig implements JobConfig {
  private static final long serialVersionUID = -1332267808035280727L;

  /** The number of threads to run the benchmark with. */
  private int mThreadNum;

  /** The number of batches to run sequentially. */
  private int mBatchNum;

  /** File system type, which can be "Alluxio" or "HDFS". */
  private FileSystemType mFileSystem;

  /**
   * whether to shows the verbose result. Usually verbose result includes the the perf number per
   * worker.
   */
  private boolean mVerbose;

  /** Whether to clean up after test. */
  private boolean mCleanUp;

  /** A unique ID to identify a test. It is currently approximated as nanoTime. */
  private long mUniqueTestId;

  /**
   * Creates a new instance of {@link AbstractBenchmarkJobConfig}.
   *
   * @param threadNum the number of threads
   * @param batchNum the number of batches
   * @param fileSystemType the file system type
   * @param verbose the verbose result
   * @param cleanUp run clean up after test if set to true, deprecated
   */
  public AbstractBenchmarkJobConfig(int threadNum, int batchNum, String fileSystemType,
      boolean verbose, boolean cleanUp) {
    Preconditions.checkNotNull(fileSystemType, "the file system type cannot be null");
    Preconditions.checkArgument(threadNum > 0, "the thread num should at least be 1");
    mThreadNum = threadNum;
    mBatchNum = batchNum;
    mFileSystem = FileSystemType.valueOf(fileSystemType);
    mVerbose = verbose;
    mCleanUp = cleanUp;
    mUniqueTestId = System.nanoTime();
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

  /**
   * @return the file system type
   */
  public FileSystemType getFileSystemType() {
    return mFileSystem;
  }

  /**
   * @return whether to display verbose result
   */
  public boolean isVerbose() {
    return mVerbose;
  }

  /**
   * @return whether to clean up after test
   */
  public boolean isCleanUp() {
    return mCleanUp;
  }

  /**
   * @return the unique test ID
   */
  public long getUniqueTestId() {
    return mUniqueTestId;
  }


  /**
   * Updates the toStringHelper.
   *
   * @param helper handler to toStringHelper
   * @return updated toStringHelper
   */
  protected Objects.ToStringHelper updateToStringHelper(Objects.ToStringHelper helper) {
    return helper
        .add("batchNum", mBatchNum)
        .add("cleanUp", mCleanUp)
        .add("fileSystem", mFileSystem)
        .add("threadNum", mThreadNum)
        .add("uniqueTestId", mUniqueTestId)
        .add("verbose", mVerbose);
  }

  @Override
  public String toString() {
    return updateToStringHelper(Objects.toStringHelper(this)).toString();
  }
}
