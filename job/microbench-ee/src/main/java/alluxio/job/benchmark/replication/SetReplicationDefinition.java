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
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.job.JobWorkerContext;
import alluxio.job.benchmark.AbstractNoArgBenchmarkJobDefinition;
import alluxio.job.benchmark.BenchmarkUtils;
import alluxio.job.benchmark.DatabaseConstants;
import alluxio.job.benchmark.IOThroughputResult;
import alluxio.job.benchmark.ReportFormatUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * A simple benchmark on writing files with a specific replication level and later on changing the
 * replication level of these files. Each thread writes a file to URI
 * "/replication-[task-id]/[thread-id]".
 */
public final class SetReplicationDefinition
    extends AbstractNoArgBenchmarkJobDefinition<SetReplicationConfig, IOThroughputResult> {
  public static final String BASE_DIR_PREFIX = "/replication-";
  private final FileSystem mFileSystem;

  /**
   * Constructs a new {@link SetReplicationDefinition}.
   */
  public SetReplicationDefinition() {
    mFileSystem = FileSystem.Factory.get();
  }

  @Override
  public String join(SetReplicationConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils
        .createThroughputResultReport(config, taskResults, DatabaseConstants.REPLICATION);
  }

  @Override
  protected void before(SetReplicationConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    AlluxioURI dir = getTaskOutputDir(jobWorkerContext);
    if (mFileSystem.exists(dir)) {
      mFileSystem.delete(dir, DeleteOptions.defaults().setRecursive(true));
    }
    mFileSystem.createDirectory(dir, CreateDirectoryOptions.defaults().setRecursive(true));
  }

  @Override
  protected void run(SetReplicationConfig config, SerializableVoid args,
      JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws Exception {
    AlluxioURI uri = getTaskOutputDir(jobWorkerContext).join(String.valueOf(threadIndex));
    long blockSize = config.getBlockSize();
    long bufferSize = config.getBufferSize();
    long fileSize = config.getFileSize();
    int replicationMaxBefore = config.getReplicationMaxBefore();
    int replicationMinBefore = config.getReplicationMinBefore();
    int replicationMaxAfter = config.getReplicationMaxAfter();
    int replicationMinAfter = config.getReplicationMinAfter();

    try (OutputStream os = mFileSystem.createFile(uri,
        CreateFileOptions.defaults().setReplicationMin(replicationMinBefore)
            .setReplicationMax(replicationMaxBefore).setBlockSizeBytes(blockSize))) {
      BenchmarkUtils.writeInputStream(os, bufferSize, fileSize);
    }

    checkReplication(uri, replicationMinBefore, replicationMaxBefore);

    mFileSystem.setAttribute(uri,
        SetAttributeOptions.defaults().setReplicationMax(replicationMaxAfter)
            .setReplicationMin(replicationMinAfter));

    waitForReplication(uri, config);

    checkReplication(uri, replicationMinAfter, replicationMaxAfter);
  }

  @Override
  protected void after(SetReplicationConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // Delete the file used by this task.
    AlluxioURI dir = getTaskOutputDir(jobWorkerContext);
    if (mFileSystem.exists(dir)) {
      mFileSystem.delete(dir, DeleteOptions.defaults().setRecursive(true));
    }
  }

  @Override
  protected IOThroughputResult process(SetReplicationConfig config,
      List<List<Long>> benchmarkThreadTimeList) {
    long bytes = config.getFileSize();
    double timeSec = 1.0 * benchmarkThreadTimeList.get(0).get(0) / Constants.SECOND_NANO;
    double throughputMbps = (1.0 * bytes / Constants.MB) / timeSec;
    double timeMs = timeSec * Constants.SECOND_MS;
    return new IOThroughputResult(throughputMbps, timeMs);
  }

  @Override
  public Class<SetReplicationConfig> getJobConfigClass() {
    return SetReplicationConfig.class;
  }

  /**
   * Gets the output directory for this task in the format of "/replication-[task-id]".
   *
   * @param jobWorkerContext context of this job worker
   * @return the output path
   */
  private AlluxioURI getTaskOutputDir(JobWorkerContext jobWorkerContext) {
    return new AlluxioURI(BASE_DIR_PREFIX + jobWorkerContext.getTaskId());
  }

  /**
   * Checks if all blocks of a given uri have the target number of replication.
   *
   * @param uri the URI of the file to check
   * @param min target min replication (inclusive)
   * @param max target max replication (inclusive)
   * @throws Exception if check fails
   */
  private void checkReplication(AlluxioURI uri, int min, int max) throws Exception {
    URIStatus status = mFileSystem.getStatus(uri);
    for (FileBlockInfo info : status.getFileBlockInfos()) {
      int replication = info.getBlockInfo().getLocations().size();
      long id = info.getBlockInfo().getBlockId();
      if (replication < min || max != Constants.REPLICATION_MAX_INFINITY && max < replication) {
        throw new IOException(String
            .format("BlockId %d: %d replicas found, expected range [%d, %d].", id, replication, min,
                max));
      }
    }
  }

  /**
   * Waits until the target file gets replicated for required number of copies or time out.
   *
   * @param uri Uri of the targe file
   * @param config Configuration of this test
   */
  private void waitForReplication(final AlluxioURI uri, final SetReplicationConfig config) {
    BenchmarkUtils.waitFor(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
          URIStatus status = mFileSystem.getStatus(uri);
          for (FileBlockInfo info : status.getFileBlockInfos()) {
            int replication = info.getBlockInfo().getLocations().size();
            if (replication < config.getReplicationMinAfter()) {
              return false;
            }
          }
          return true;
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, (long) config.getReplicationTimeout() * Constants.SECOND_MS);
  }
}
