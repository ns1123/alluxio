/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.util.SerializableVoid;

/**
 * A simple instantiation of read micro benchmark {@link AbstractSimpleReadDefinition}.
 */
public final class SimpleReadDefinition
    extends AbstractSimpleReadDefinition<SimpleReadConfig, SerializableVoid> {
  /**
   * Constructs a new {@link SimpleReadDefinition}.
   */
  public SimpleReadDefinition() {}

  @Override
  public Class<SimpleReadConfig> getJobConfigClass() {
    return SimpleReadConfig.class;
  }

  /**
   * @param config config
   * @param jobWorkerContext job worker context
   * @param batch batch of this job
   * @param threadIndex thread of this executor
   * @return the path to the file to read
   */
  public String getReadFilePath(SimpleReadConfig config, JobWorkerContext jobWorkerContext,
      int batch, int threadIndex) {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    return getWritePrefix(config.getBaseDir(), fs, jobWorkerContext) + "/" + threadIndex;
  }
}
