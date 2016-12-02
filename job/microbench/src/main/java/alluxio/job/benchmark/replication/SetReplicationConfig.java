/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.replication;

import alluxio.Constants;
import alluxio.job.benchmark.AbstractBenchmarkJobConfig;
import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for the SetReplication benchmark job.
 */
@JsonTypeName(SetReplicationConfig.NAME)
public class SetReplicationConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 6957862937943548524L;
  public static final String NAME = "SetReplication";

  private final long mBlockSize;
  private final long mBufferSize;
  private final long mFileSize;
  private final int mReplicationMaxBefore;
  private final int mReplicationMinBefore;
  private final int mReplicationMaxAfter;
  private final int mReplicationMinAfter;
  private final int mReplicationTimeout;

  /**
   * Creates a new instance of {@link SetReplicationConfig}.
   *
   * @param blockSize the block size in bytes
   * @param bufferSize the buffer size in bytes
   * @param fileSize the file size in bytes
   * @param threadNum the number of threads to write (different) files concurrently
   * @param replicationMaxBefore the max replication number before setReplication
   * @param replicationMinBefore the min replication number before setReplication
   * @param replicationMaxAfter the max replication number after setReplication
   * @param replicationMinAfter the min replication number after setReplication
   * @param replicationTimeout the time out on waiting for replication in seconds
   */
  public SetReplicationConfig(
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("replicationMaxBefore") int replicationMaxBefore,
      @JsonProperty("replicationMinBefore") int replicationMinBefore,
      @JsonProperty("replicationMaxAfter") int replicationMaxAfter,
      @JsonProperty("replicationMinAfter") int replicationMinAfter,
      @JsonProperty("replicationTimeout") int replicationTimeout) {
    super(threadNum, 1, "ALLUXIO", true, true);
    Preconditions.checkArgument(replicationMinBefore >= 0);
    Preconditions.checkArgument(replicationMinAfter >= 0);
    Preconditions.checkArgument(replicationMaxBefore >= 0
        || replicationMaxBefore == Constants.REPLICATION_MAX_INFINITY);
    Preconditions.checkArgument(replicationMaxAfter >= 0
        || replicationMaxAfter == Constants.REPLICATION_MAX_INFINITY);
    mBlockSize = FormatUtils.parseSpaceSize(blockSize);
    mBufferSize = FormatUtils.parseSpaceSize(bufferSize);
    mFileSize = FormatUtils.parseSpaceSize(fileSize);
    mReplicationMaxBefore = replicationMaxBefore;
    mReplicationMinBefore = replicationMinBefore;
    mReplicationMaxAfter = replicationMaxAfter;
    mReplicationMinAfter = replicationMinAfter;
    mReplicationTimeout = replicationTimeout;
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
   * @return the max replication before set replication
   */
  public int getReplicationMaxBefore() {
    return mReplicationMaxBefore;
  }

  /**
   * @return the min replication before set replication
   */
  public int getReplicationMinBefore() {
    return mReplicationMinBefore;
  }

  /**
   * @return the max replication after set replication
   */
  public int getReplicationMaxAfter() {
    return mReplicationMaxAfter;
  }

  /**
   * @return the min replication after set replication
   */
  public int getReplicationMinAfter() {
    return mReplicationMinAfter;
  }

  /**
   * @return the time out on waiting for replication job to complete
   */
  public int getReplicationTimeout() {
    return mReplicationTimeout;
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
        .add("replicationMaxAfter", mReplicationMaxAfter)
        .add("replicationMaxBefore", mReplicationMaxBefore)
        .add("replicationMinAfter", mReplicationMinAfter)
        .add("replicationMinBefore", mReplicationMinBefore)
        .add("replicationTimeout", mReplicationTimeout)
        .add("threadNum", getThreadNum())
        .add("verbose", isVerbose())
        .toString();
  }
}
