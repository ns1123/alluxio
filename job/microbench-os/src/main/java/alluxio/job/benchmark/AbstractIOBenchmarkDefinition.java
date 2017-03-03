/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.AlluxioFS;

import java.io.Serializable;

/**
 * The abstract class for all the IO benchmark job implementations. This class carries the
 * information of the path where the IO tests will write to, as well as the cleanup method that
 * cleans up the path afterwards.
 *
 * @param <T> the benchmark job configuration
 * @param <R> the benchmark task arg
 */
public abstract class AbstractIOBenchmarkDefinition<T extends AbstractIOBenchmarkConfig,
    R extends Serializable> extends AbstractBenchmarkJobDefinition<T, R, IOThroughputResult> {

  /**
   * Gets the write tasks working directory prefix.
   *
   * @param baseDir the base directory for the test files
   * @param fs the file system
   * @param ctx the job worker context
   * @return the tasks working directory prefix
   */
  private String getWritePrefix(String baseDir, AbstractFS fs, JobWorkerContext ctx) {
    String path = baseDir + ctx.getTaskId();
    // If the FS is not Alluxio, apply the Alluxio UNDERFS_ADDRESS prefix to the file path.
    // Thereforce, the UFS files are also written to the Alluxio mapped directory.
    if (!(fs instanceof AlluxioFS)) {
      path = Configuration.get(PropertyKey.UNDERFS_ADDRESS) + path;
    }
    return new StringBuilder().append(path).toString();
  }

  @Override
  protected void after(AbstractIOBenchmarkConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {

    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getWritePrefix(config.getBaseDir(), fs, jobWorkerContext);
    switch (config.getFreeAfterType()) {
      case ALLUXIO_ONLY:
        ((AlluxioFS) fs).free(path, true/* recursive */);
        break;
      case ALL:
        fs.delete(path, true /* recursive */);
        break;
      case NONE:
        // do nothing
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized freeAfter type " + config.getFreeAfterType());
    }
  }
}
