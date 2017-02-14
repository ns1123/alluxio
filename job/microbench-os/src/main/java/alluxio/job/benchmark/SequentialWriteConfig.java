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
import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for SequentialWrite benchmark job.
 */
@JsonTypeName(SequentialWriteConfig.NAME)
public class SequentialWriteConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 5017449643156275656L;
  public static final String NAME = "SequentialWrite";

  private final long mFileSize;
  private final long mBlockSize;
  private final WriteType mWriteType;
  // The number of writes in each batch.
  private final int mBatchSize;

  /**
   * Creates a new instance of {@link SequentialWriteConfig}.
   *
   * @param batchSize the batch size
   * @param batchNum the number of batches
   * @param blockSize the block size
   * @param bufferSize the buffer size
   * @param fileSize the file size
   * @param fileSystemType the file system type
   * @param writeType the write type
   */
  public SequentialWriteConfig(
      @JsonProperty("batchSize") int batchSize,
      @JsonProperty("batchNum") int batchNum,
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("writeType") String writeType) {
    // Sequential writes should only use 1 thread.
    super(1, batchNum, fileSystemType, true, true);
    Preconditions.checkNotNull(batchSize, "batch size cannot be null");
    Preconditions.checkNotNull(bufferSize, "buffer size cannot be null");
    Preconditions.checkNotNull(writeType, "write type cannot be null");
    Preconditions.checkNotNull(fileSize, "file size cannot be null");
    mFileSize = FormatUtils.parseSpaceSize(fileSize);
    mBlockSize = FormatUtils.parseSpaceSize(blockSize);
    mWriteType = WriteType.valueOf(writeType);
    mBatchSize = batchSize;
  }

  /**
   * @return the file size
   */
  public long getFileSize() {
    return mFileSize;
  }

  /**
   * @return the block size
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return the batch size
   */
  public int getBatchSize() {
    return mBatchSize;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("batchNum", getBatchNum())
        .add("batchSize", mBatchSize)
        .add("blockSize", mBlockSize)
        .add("fileSize", mFileSize)
        .add("fileSystemType", getFileSystemType().toString())
        .add("threadNum", getThreadNum())
        .add("verbose", isVerbose())
        .add("writeType", mWriteType)
        .add("cleanUp", isCleanUp())
        .toString();
  }
}
