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

/**
 * The configuration for the SimpleRead benchmark job.
 */
public class SimpleReadConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = -4588479606679358186L;
  public static final String NAME = "SimpleRead";

  private String mBufferSize;
  private ReadType mReadType;

  /**
   * Creates a new instance of {@link SimpleReadConfig}.
   *
   * @param bufferSize the buffer size
   * @param readType the read type
   * @param threadNum the thread number
   * @param verbose whether the report is verbose
   */
  public SimpleReadConfig(@JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("readType") String readType, @JsonProperty("threadNum") int threadNum,
      @JsonProperty("verbose") boolean verbose) {
    super(threadNum, 1, verbose);

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
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("threadNum", getThreadNum())
        .add("batchNum", getBatchNum())
        .add("verbose", isVerbose())
        .add("bufferSize", mBufferSize)
        .add("readType", mReadType)
        .toString();
  }
}
