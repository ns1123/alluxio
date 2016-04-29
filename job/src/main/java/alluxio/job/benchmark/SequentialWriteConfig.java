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

/**
 * The configuration for SequentialWrite benchmark job.
 */
public class SequentialWriteConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 5017449643156275656L;
  public static final String NAME = "SequentialWrite";

  private final long mFileSize;
  private final long mBlockSize;
  private final WriteType mWriteType;
  // The number of writes in each batch.
  private final int mBatchSize;

  public SequentialWriteConfig(@JsonProperty("batchNum") int batchNum,
      @JsonProperty("fileSize") String fileSize, @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("blockSize") String blockSize, @JsonProperty("writeType") String writeType,
      @JsonProperty("batchSize") int batchSize) {
    // Sequential writes should only use 1 thread.
    super(1, batchNum);
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
}