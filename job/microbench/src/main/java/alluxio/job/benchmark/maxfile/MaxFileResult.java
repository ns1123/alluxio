/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.maxfile;

import alluxio.job.benchmark.BenchmarkTaskResult;

/**
 * Records the number of the files have been written on a task.
 */
public class MaxFileResult implements BenchmarkTaskResult {
  private static final long serialVersionUID = 8723853716791720615L;
  private final Long mNumFiles;

  /**
   * Creates a new instance of {@link MaxFileResult}.
   *
   * @param numFiles the number of files
   */
  public MaxFileResult(Long numFiles) {
    mNumFiles = numFiles;
  }

  /**
   * @return the number of files
   */
  public Long getNumFiles() {
    return mNumFiles;
  }
}
