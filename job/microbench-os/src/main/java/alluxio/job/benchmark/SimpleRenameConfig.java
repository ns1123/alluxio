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
 * The configuration for the SimpleRename benchmark job. By default, this class will rename files
 * output by the SimpleWrite benchmark.
 */
@JsonTypeName(SimpleRenameConfig.NAME)
public class SimpleRenameConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = -4588479606679358186L;
  public static final String NAME = "SimpleRename";

  private String mBaseDir;

  /**
   * Creates a new instance of {@link SimpleRenameConfig}.
   *
   * @param fileSystemType the file system type
   * @param threadNum the thread number
   * @param baseDir the base directory for the test files
   * @param verbose whether the report is verbose
   * @param cleanUp whether to clean up the input files
   */
  public SimpleRenameConfig(
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("verbose") boolean verbose,
      @JsonProperty("cleanUp") boolean cleanUp) {
    super(threadNum, 1, fileSystemType, verbose, cleanUp);
    mBaseDir = baseDir != null ? baseDir : SimpleWriteConfig.READ_WRITE_DIR;
  }

  /**
   * @return the base directory for the test files
   */
  public String getBaseDir() {
    return mBaseDir;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("batchNum", getBatchNum())
        .add("fileSystemType", getFileSystemType().toString())
        .add("threadNum", getThreadNum())
        .add("baseDir", getBaseDir())
        .add("verbose", isVerbose())
        .add("cleanUp", isCleanUp())
        .toString();
  }
}
