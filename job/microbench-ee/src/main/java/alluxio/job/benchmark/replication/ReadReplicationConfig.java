/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.replication;

import alluxio.job.benchmark.AbstractBenchmarkJobConfig;
import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for the ReadReplication benchmark job.
 */
@JsonTypeName(ReadReplicationConfig.NAME)
public class ReadReplicationConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 482302550207093462L;
  public static final String NAME = "ReadReplication";
  private final long mBlockSize;
  private final long mBufferSize;
  private final long mFileSize;
  private final int mReplication;

  /**
   * Creates a new instance of {@link ReadReplicationConfig}.
   *
   * @param blockSize the block size in bytes
   * @param bufferSize the buffer size in bytes
   * @param fileSize the file size in bytes
   * @param threadNum the number of threads to write (different) files concurrently
   * @param replication the min replication number after setReplication
   */
  public ReadReplicationConfig(
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("replication") int replication) {
    super(threadNum, 1, "ALLUXIO", true, true);
    Preconditions.checkArgument(replication >= 0);
    mBlockSize = FormatUtils.parseSpaceSize(blockSize);
    mBufferSize = FormatUtils.parseSpaceSize(bufferSize);
    mFileSize = FormatUtils.parseSpaceSize(fileSize);
    mReplication = replication;
  }

  /**
   * @return the buffer size in bytes
   */
  public long getBufferSize() {
    return mBufferSize;
  }

  /**
   * @return the block size in bytes
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the file size in bytes
   */
  public long getFileSize() {
    return mFileSize;
  }

  /**
   * @return the min replication after set replication
   */
  public int getReplication() {
    return mReplication;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("batchNum", getBatchNum())
        .add("blockSize", mBlockSize)
        .add("bufferSize", mBufferSize)
        .add("cleanUp", isCleanUp())
        .add("fileSize", mFileSize)
        .add("fileSystemType", getFileSystemType().toString())
        .add("replication", mReplication)
        .add("threadNum", getThreadNum())
        .add("verbose", isVerbose())
        .toString();
  }
}
