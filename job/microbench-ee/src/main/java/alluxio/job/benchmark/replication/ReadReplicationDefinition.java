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
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.AbstractNoArgBenchmarkJobDefinition;
import alluxio.job.benchmark.BenchmarkUtils;
import alluxio.job.benchmark.DatabaseConstants;
import alluxio.job.benchmark.IOThroughputResult;
import alluxio.job.benchmark.ReportFormatUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * A simple benchmark that reads a given file with a specific replication level.
 */
public final class ReadReplicationDefinition
    extends AbstractNoArgBenchmarkJobDefinition<ReadReplicationConfig, IOThroughputResult> {
  public static final AlluxioURI FILE_PATH = new AlluxioURI("/FileToRead");
  private final FileSystem mFileSystem;

  /**
   * Constructs a new {@link ReadReplicationDefinition}.
   */
  public ReadReplicationDefinition() {
    mFileSystem = FileSystem.Factory.get();
  }

  @Override
  public String join(ReadReplicationConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils
        .createThroughputResultReport(config, taskResults, DatabaseConstants.REPLICATION);
  }

  @Override
  protected void before(ReadReplicationConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Clean (if target file already exists) and create the file for benchmark
    if (jobWorkerContext.getTaskId() == 0) {
      if (mFileSystem.exists(FILE_PATH)) {
        mFileSystem.delete(FILE_PATH);
      }
      long blockSize = config.getBlockSize();
      long fileSize = config.getFileSize();
      int replication = config.getReplication();
      int writeBufferSize = 1 << 23; // use 8MB, not really important for this benchmark
      try (OutputStream os = mFileSystem.createFile(FILE_PATH,
          CreateFileOptions.defaults().setReplicationMin(replication)
              .setBlockSizeBytes(blockSize))) {
        BenchmarkUtils.writeFile(os, writeBufferSize, fileSize);
      }
    }
  }

  @Override
  protected void run(ReadReplicationConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws Exception {
    long fileSize;
    try (InputStream is = mFileSystem
        .openFile(FILE_PATH, OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE))) {
      fileSize = BenchmarkUtils.readFile(is, config.getBufferSize());
    }
    if (fileSize != config.getFileSize()) {
      throw new IOException(String
          .format("Task%d Thread%d reads back %d bytes, expected %d", jobWorkerContext.getTaskId(),
              threadIndex, fileSize, config.getFileSize()));
    }
  }

  @Override
  protected void after(ReadReplicationConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Delete the file used by this task.
    if (jobWorkerContext.getTaskId() == 0) {
      if (mFileSystem.exists(FILE_PATH)) {
        mFileSystem.delete(FILE_PATH);
      }
    }
  }

  @Override
  protected IOThroughputResult process(ReadReplicationConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    long bytes = config.getFileSize();
    double timeSec = 1.0 * benchmarkThreadTimeList.get(0).get(0) / Constants.SECOND_NANO;
    double tputMBps = (1.0 * bytes / Constants.MB) / timeSec;
    double timeMs = timeSec * Constants.SECOND_MS;
    return new IOThroughputResult(tputMBps, timeMs);
  }

  @Override
  public Class<ReadReplicationConfig> getJobConfigClass() {
    return ReadReplicationConfig.class;
  }
}
