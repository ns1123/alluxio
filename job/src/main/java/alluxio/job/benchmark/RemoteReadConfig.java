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
 * The configuration for the RemoteRead benchmark job.
 */
public final class RemoteReadConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 3677039635371902043L;

  public static final String NAME = "RemoteRead";

  private String mBufferSize;
  private ReadType mReadType;
  private long mReadTargetTaskId;
  private long mReadTargetTaskOffset;

  /**
   * Creates a new instance of {@link RemoteReadConfig}. Same as ${@link SimpleReadConfig} except
   * for two additional fields that can specify which target task to read from.
   *
   * The target task id is determined by the following criteria.
   * 1) if (readTargetTaskId != -1) targetTaskId = readTargetTaskId
   * 2) otherwise targetTaskId = ((workerId + readTargetOffset) % totalNumWorkers
   *
   * In other words, readTargetTaskId has higher priority than readTargetTaskOffset. Only if the
   * readTargetTaskId is -1, readTargetTaskOffset will take effect. Otherwise readTargetTaskId
   * would overwrite the effect of readTargetTaskOffset.
   *
   * @param bufferSize the buffer size
   * @param fileSystemType the file system type
   * @param readType the read type
   * @param readTargetTaskId the read target task ID
   * @param readTargetTaskOffset the read target task offset
   * @param threadNum the thread number
   */
  public RemoteReadConfig(
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("readType") String readType,
      @JsonProperty("readTargetTaskId") long readTargetTaskId,
      @JsonProperty("readTargetTaskOffset") long readTargetTaskOffset,
      @JsonProperty("threadNum") int threadNum) {
    super(threadNum, 1, FileSystemType.valueOf(fileSystemType));

    // validate the input to fail fast
    FormatUtils.parseSpaceSize(bufferSize);
    mBufferSize = bufferSize;
    mReadType = ReadType.valueOf(readType);
    mReadTargetTaskId = readTargetTaskId;
    mReadTargetTaskOffset = readTargetTaskOffset;
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
   * @return the read target task id
   */
  public long getReadTargetTaskId() {
    return mReadTargetTaskId;
  }

  /**
   * @return the read target task offset
   */
  public long getReadTargetTaskOffset() {
    return mReadTargetTaskOffset;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("batchNum", getBatchNum())
        .add("bufferSize", mBufferSize)
        .add("fileSystemType", getFileSystemType().toString())
        .add("readType", mReadType)
        .add("readTargetTaskId", mReadTargetTaskId)
        .add("readTargetTaskOffset", mReadTargetTaskOffset)
        .add("threadNum", getThreadNum())
        .toString();
  }
}
