/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;

/**
 * The configuration for the RemoteRead benchmark job.
 */
@JsonTypeName(RemoteReadConfig.NAME)
public final class RemoteReadConfig extends AbstractSimpleReadConfig {
  private static final long serialVersionUID = 3677039635371902043L;
  public static final String NAME = "RemoteRead";
  private long mReadTargetTaskId;
  private long mReadTargetTaskOffset;

  /**
   * Creates a new instance of {@link RemoteReadConfig}. Same as ${@link AbstractSimpleReadConfig}
   * except for two additional fields that can specify which target task to read from.
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
   * @param baseDir the base directory for the test files
   * @param verbose whether the report is verbose
   * @param cleanUp whether to clean up Alluxio files created by SimpleWrite
   * @param freeAfterType the type of freeing files in file system after test
   */
  public RemoteReadConfig(
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("readType") String readType,
      @JsonProperty("readTargetTaskId") long readTargetTaskId,
      @JsonProperty("readTargetTaskOffset") long readTargetTaskOffset,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("verbose") boolean verbose,
      @JsonProperty("cleanUp") boolean cleanUp,
      @JsonProperty("freeAfterType") String freeAfterType) {
    super(baseDir, bufferSize, cleanUp, fileSystemType, null, freeAfterType, readType, threadNum,
        verbose);
    mReadTargetTaskId = readTargetTaskId;
    mReadTargetTaskOffset = readTargetTaskOffset;
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
    return Objects.toStringHelper(super.getClass())
        .add("readTargetTaskId", mReadTargetTaskId)
        .add("readTargetTaskOffset", mReadTargetTaskOffset)
        .toString();
  }
}
