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
import com.google.common.base.Objects;

/**
 * The configuration for the SimpleWrite benchmark job.
 */
public class SimpleWriteConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 8696209904079086810L;
  public static final String NAME = "SimpleWrite";

  private String mBlockSize;
  private String mFileSize;
  private String mBufferSize;
  private WriteType mWriteType;
  private boolean mCleanUp;

  /**
   * Creates a new instance of {@link SimpleWriteConfig}.
   *
   * @param blockSize the block size
   * @param fileSize the file size
   * @param bufferSize the buffer size
   * @param writeType the write type
   * @param cleanUp whether to cleanup the state after the test
   * @param threadNum the thread number
   * @param verbose whether the report is verbose
   */
  public SimpleWriteConfig(@JsonProperty("blockSize") String blockSize,
      @JsonProperty("fileSize") String fileSize, @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("writeType") String writeType, @JsonProperty("cleanUp") boolean cleanUp,
      @JsonProperty("threadNum") int threadNum, @JsonProperty("verbose") boolean verbose) {
    super(threadNum, 1, verbose);
    // validate the input to fail fast
    FormatUtils.parseSpaceSize(fileSize);
    mFileSize = fileSize;
    FormatUtils.parseSpaceSize(bufferSize);
    mBufferSize = bufferSize;
    FormatUtils.parseSpaceSize(blockSize);
    mBlockSize = blockSize;
    mWriteType = WriteType.valueOf(writeType);
    mCleanUp = cleanUp;
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

  /**
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return true if it needs to clean up after test
   */
  boolean getCleanUp() {
    return mCleanUp;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("blockSize", mBlockSize).add("bufferSize", mBufferSize)
        .add("fileSize", mFileSize).add("writeType", mWriteType).toString();
  }
}
