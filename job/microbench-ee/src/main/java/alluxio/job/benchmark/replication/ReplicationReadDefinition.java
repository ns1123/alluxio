/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.replication;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.AbstractSimpleReadDefinition;
import alluxio.job.benchmark.BenchmarkUtils;
import alluxio.job.util.SerializableVoid;

import java.io.OutputStream;

/**
 * A simple benchmark that reads a given file with a specific replication factor. If the replication
 * factor is greater than zero, this benchmark tests the performance reading from Alluxio space;
 * otherwise, the result is about concurrently reading from UFS.
 */
public final class ReplicationReadDefinition
    extends AbstractSimpleReadDefinition<ReplicationReadConfig, SerializableVoid> {
  private final FileSystem mFileSystem;

  /**
   * Constructs a new {@link ReplicationReadDefinition}.
   */
  public ReplicationReadDefinition() {
    mFileSystem = FileSystem.Factory.get();
  }

  @Override
  public Class<ReplicationReadConfig> getJobConfigClass() {
    return ReplicationReadConfig.class;
  }

  @Override
  protected void before(ReplicationReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Clean (if target file already exists) and create the file for benchmark
    if (config.getFileToRead() != null && jobWorkerContext.getTaskId() == 0) {
      AlluxioURI uri = new AlluxioURI(config.getFileToRead());
      if (mFileSystem.exists(uri)) {
        mFileSystem.delete(uri);
      }
      long blockSize = config.getBlockSize();
      long fileSize = config.getFileSize();
      int replication = config.getReplication();
      int writeBufferSize = 1 << 23; // use 8MB, not really important for this benchmark
      CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(blockSize);
      if (replication > 0) {
        options.setReplicationMin(replication);
      } else {
        options.setWriteType(WriteType.THROUGH);
      }
      try (OutputStream os = mFileSystem.createFile(uri, options)) {
        BenchmarkUtils.writeInputStream(os, writeBufferSize, fileSize);
      }
    }
  }

  @Override
  protected void after(ReplicationReadConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Delete the file used by this task.
    if (config.getFileToRead() != null && jobWorkerContext.getTaskId() == 0) {
      AlluxioURI uri = new AlluxioURI(config.getFileToRead());
      if (mFileSystem.exists(uri)) {
        mFileSystem.delete(uri);
      }
    }
  }
}
