/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for the {@link AsyncWriteDefinition} benchmark job.
 */
public class AsyncWriteConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 8696209904079086810L;
  public static final String NAME = "AsyncWrite";

  private String mBlockSize;
  private String mFileSize;
  private String mBufferSize;

  /**
   * Creates a new instance of {@link AsyncWriteConfig}.
   *
   * @param blockSize the block size
   * @param bufferSize the buffer size
   * @param fileSize the file size
   * @param threadNum the thread number
   * @param verbose whether the report is verbose
   */
  public AsyncWriteConfig(
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("verbose") boolean verbose) {
    super(threadNum, 1, "ALLUXIO", verbose, true);
    Preconditions.checkNotNull(blockSize, "block size cannot be null");
    Preconditions.checkNotNull(bufferSize, "buffer size cannot be null");
    Preconditions.checkNotNull(fileSize, "file size cannot be null");
    // validate the input to fail fast
    FormatUtils.parseSpaceSize(fileSize);
    mFileSize = fileSize;
    FormatUtils.parseSpaceSize(bufferSize);
    mBufferSize = bufferSize;
    FormatUtils.parseSpaceSize(blockSize);
    mBlockSize = blockSize;
  }

  /**
   * @return the block size
   */
  public String getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the buffer size
   */
  public String getBufferSize() {
    return mBufferSize;
  }

  /**
   * @return the file size
   */
  public String getFileSize() {
    return mFileSize;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("batchSize", getBatchNum())
        .add("blockSize", mBlockSize)
        .add("bufferSize", mBufferSize)
        .add("cleanUp", isCleanUp())
        .add("fileSize", mFileSize)
        .add("threadNum", getThreadNum())
        .add("verbose", isVerbose())
        .add("cleanUp", isCleanUp())
        .toString();
  }
}
