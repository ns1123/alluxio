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

/**
 * The configuration for the SimpleRead benchmark job.
 */
@JsonTypeName(SimpleReadConfig.NAME)
public final class SimpleReadConfig extends AbstractSimpleReadConfig {
  private static final long serialVersionUID = -4058874106056127064L;
  public static final String NAME = "SimpleRead";

  /**
   * Creates a new instance of {@link SimpleReadConfig}.
   *
   * @param bufferSize the buffer size
   * @param fileSystemType the file system type
   * @param readType the read type
   * @param threadNum the thread number
   * @param baseDir the base directory for the test files
   * @param verbose whether the report is verbose
   * @param cleanUp whether to clean up Alluxio files created by SimpleWrite
   * @param freeAfterType the type of freeing files in file system after test
   * @param fileToRead the path of the file for each thread to read
   */
  public SimpleReadConfig(
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("readType") String readType,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("verbose") boolean verbose,
      @JsonProperty("cleanUp") boolean cleanUp,
      @JsonProperty("freeAfterType") String freeAfterType,
      @JsonProperty("fileToRead") String fileToRead) {
    super(baseDir, bufferSize, cleanUp, fileSystemType, fileToRead, freeAfterType, readType,
        threadNum, verbose);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
