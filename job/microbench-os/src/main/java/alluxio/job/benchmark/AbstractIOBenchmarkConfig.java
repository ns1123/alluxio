/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

/**
 * The abstract configuration for all the IO benchmark jobs.
 */
public abstract class AbstractIOBenchmarkConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 4856138152745476747L;

  public static final String IO_BENCHMARK_DIR = "/io-benchmark/";

  /** Where to store the benchmark files. */
  private String mBaseDir;
  /** Describing what files should be freed after the test. */
  private FreeAfterType mFreeAfterType;

  /**
   * Constructor for {@link AbstractIOBenchmarkConfig}.
   *
   * @param threadNum the thread number
   * @param batchNum the batch size
   * @param fileSystemType the file system type
   * @param verbose whether the report is verbose
   * @param cleanUp whether to cleanup the state after the test
   * @param baseDir the base directory for the test files
   * @param freeAfterType the type of freeing files in file system after test
   */
  public AbstractIOBenchmarkConfig(int threadNum, int batchNum, String fileSystemType,
      boolean verbose, boolean cleanUp, String baseDir, String freeAfterType) {
    super(threadNum, batchNum, fileSystemType, verbose, cleanUp);
    mBaseDir = baseDir != null ? baseDir : IO_BENCHMARK_DIR;
    mFreeAfterType =
        freeAfterType == null ? FreeAfterType.ALL : FreeAfterType.valueOf(freeAfterType);
  }

  /**
   * @return the base directory for the test files
   */
  public String getBaseDir() {
    return mBaseDir;
  }

  /**
   * @return what files should be freed after the test
   */
  public FreeAfterType getFreeAfterType() {
    return mFreeAfterType;
  }
}
