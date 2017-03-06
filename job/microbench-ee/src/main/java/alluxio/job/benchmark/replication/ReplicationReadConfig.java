/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.replication;

import alluxio.job.benchmark.AbstractSimpleReadConfig;
import alluxio.job.benchmark.FileSystemType;
import alluxio.job.benchmark.FreeAfterType;
import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for the ReadFile benchmark job.
 */
@JsonTypeName(ReplicationReadConfig.NAME)
public final class ReplicationReadConfig extends AbstractSimpleReadConfig {
  private static final long serialVersionUID = 482302550207093462L;
  public static final String NAME = "ReplicationReadConfig";

  private final long mBlockSize;
  private final long mFileSize;
  private final int mReplication;

  /**
   * Creates a new instance of {@link ReplicationReadConfig}.
   *
   * @param blockSize the block size in bytes
   * @param bufferSize the buffer size in bytes
   * @param cleanUp whether to clean up Alluxio files created by SimpleWrite
   * @param fileSize the file size in bytes
   * @param fileToRead the path of the file for each thread to read
   * @param readType the read type
   * @param replication the min replication number after setReplication
   * @param threadNum the number of threads to write (different) files concurrently
   * @param verbose whether the report is verbose
   */
  public ReplicationReadConfig(
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("cleanUp") boolean cleanUp,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("fileToRead") String fileToRead,
      @JsonProperty("readType") String readType,
      @JsonProperty("replication") int replication,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("verbose") boolean verbose) {
    super(null /* baseDir, not used */, bufferSize, cleanUp,
        FileSystemType.ALLUXIO.toString() /* fs type, fixed to Alluxio*/,
        fileToRead, FreeAfterType.NONE.toString(), readType, threadNum,
        verbose);
    Preconditions.checkArgument(replication >= 0);
    mBlockSize = FormatUtils.parseSpaceSize(blockSize);
    mFileSize = FormatUtils.parseSpaceSize(fileSize);
    mReplication = replication;
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
   * @return the min replication of the file to read
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
    return Objects.toStringHelper(super.getClass())
        .add("blockSize", mBlockSize)
        .add("fileSize", mFileSize)
        .add("replication", mReplication)
        .toString();
  }
}
