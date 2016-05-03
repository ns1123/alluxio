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
  private boolean mRunCleanUp;

  /**
   * Creates a new instance of {@link SimpleReadConfig}.
   *
   * @param bufferSize the buffer size
   * @param readType the read type
   * @param runCleanUp whether to cleanup the state after the test
   * @param threadNum the thread number
   */
  public SimpleReadConfig(@JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("readType") String readType,
      @JsonProperty("runCleanUp") boolean runCleanUp, @JsonProperty("threadNum")int threadNum) {
    super(threadNum, 1);

    // validate the input to fail fast
    FormatUtils.parseSpaceSize(bufferSize);
    mBufferSize = bufferSize;
    mReadType = ReadType.valueOf(readType);
    mRunCleanUp = runCleanUp;
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

  /**
   * @return true if it needs to clean up after test
   */
  boolean getRunCleanUp() {
    return mRunCleanUp;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("bufferSize", mBufferSize).add("readType", mReadType)
        .toString();
  }
}
