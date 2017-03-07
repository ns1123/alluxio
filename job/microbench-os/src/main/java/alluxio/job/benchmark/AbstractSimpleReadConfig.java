/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.client.ReadType;
import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for the {@link AbstractSimpleReadConfig} benchmark job.
 */
public abstract class AbstractSimpleReadConfig extends AbstractIOBenchmarkConfig {
  private static final long serialVersionUID = -4588479606679358186L;
  private long mBufferSize;
  private ReadType mReadType;
  private String mFileToRead;

  /**
   * Creates a new instance of {@link AbstractSimpleReadConfig}.
   *
   * @param baseDir the base directory for the test files
   * @param bufferSize the buffer size
   * @param cleanUp whether to clean up Alluxio files created by SimpleWrite
   * @param fileSystemType the file system type
   * @param fileToRead the path of the file for each thread to read
   * @param freeAfterType the type of freeing files in file system after test
   * @param readType the read type
   * @param threadNum the thread number
   * @param verbose whether the report is verbose
   */
  public AbstractSimpleReadConfig(
      String baseDir, String bufferSize, boolean cleanUp, String fileSystemType, String fileToRead,
      String freeAfterType, String readType, int threadNum, boolean verbose) {
    super(threadNum, 1, fileSystemType, verbose, cleanUp, baseDir, freeAfterType);
    Preconditions.checkNotNull(readType, "read type cannot be null");
    Preconditions.checkNotNull(bufferSize, "buffer size cannot be null");
    // validate the input to fail fast
    mBufferSize = FormatUtils.parseSpaceSize(bufferSize);
    mFileToRead = fileToRead;
    mReadType = ReadType.valueOf(readType);
  }

  /**
   * @return the buffer size
   */
  public long getBufferSize() {
    return mBufferSize;
  }

  /**
   * @return the read path
   */
  public String getFileToRead() {
    return mFileToRead;
  }

  /**
   * @return the read type
   */
  public ReadType getReadType() {
    return mReadType;
  }

  @Override
  protected Objects.ToStringHelper updateToStringHelper(Objects.ToStringHelper helper) {
    return super.updateToStringHelper(helper)
        .add("bufferSize", mBufferSize)
        .add("fileToRead", mFileToRead)
        .add("readType", mReadType);
  }
}
