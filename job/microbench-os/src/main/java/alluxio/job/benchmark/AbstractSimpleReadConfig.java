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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for the {@link AbstractSimpleReadConfig} benchmark job.
 */
public abstract class AbstractSimpleReadConfig extends AbstractIOBenchmarkConfig {
  private static final long serialVersionUID = -4588479606679358186L;

  private String mBufferSize;
  private ReadType mReadType;

  /**
   * Creates a new instance of {@link AbstractSimpleReadConfig}.
   *
   * @param bufferSize the buffer size
   * @param fileSystemType the file system type
   * @param readType the read type
   * @param threadNum the thread number
   * @param baseDir the base directory for the test files
   * @param verbose whether the report is verbose
   * @param cleanUp whether to clean up Alluxio files created by SimpleWrite
   * @param freeAfterType the type of freeing files in file system after test
   */
  public AbstractSimpleReadConfig(
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("readType") String readType,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("verbose") boolean verbose,
      @JsonProperty("cleanUp") boolean cleanUp,
      @JsonProperty("freeAfterType") String freeAfterType) {
    super(threadNum, 1, fileSystemType, verbose, cleanUp, baseDir, freeAfterType);
    Preconditions.checkNotNull(readType, "read type cannot be null");
    Preconditions.checkNotNull(bufferSize, "buffer size cannot be null");
    // validate the input to fail fast
    FormatUtils.parseSpaceSize(bufferSize);
    mBufferSize = bufferSize;
    mReadType = ReadType.valueOf(readType);
  }

  /**
   * @return the buffer size
   */
  public String getBufferSize() {
    return mBufferSize;
  }

  /**
   * @return the read type
   */
  public ReadType getReadType() {
    return mReadType;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(super.getClass())
        .add("bufferSize", mBufferSize)
        .add("readType", mReadType)
        .toString();
  }
}
