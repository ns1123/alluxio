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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.JobUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Skeleton implementation of a simple read micro benchmark that reads a file in a thread.
 * This benchmark task will read the
 * file written by the {@link SimpleWriteDefinition}, so each thread will read the file
 * simple-read-write/[task-id]/[thread-id].
 *
 * @param <T> the benchmark job configuration
 * @param <R> the benchmark task arg
 */
public abstract class AbstractSimpleReadDefinition<T extends AbstractSimpleReadConfig, R extends
    Serializable>
    extends AbstractIOBenchmarkDefinition<T, R> {
  /** A queue tracks the total read byte per thread. */
  private ConcurrentLinkedQueue<Long> mReadBytesQueue = null;

  @Override
  public Map<WorkerInfo, R> selectExecutors(T config, List<WorkerInfo> workerInfoList,
      JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, R> result = new TreeMap<>(JobUtils.createWorkerInfoComparator());
    for (WorkerInfo workerInfo : workerInfoList) {
      result.put(workerInfo, null);
    }
    return result;
  }

  @Override
  public String join(T config, Map<WorkerInfo, IOThroughputResult> taskResults) throws Exception {
    return ReportFormatUtils.createThroughputResultReport(config, taskResults,
        DatabaseConstants.MICROBENCH_DURATION_THROUGHPUT);
  }

  @Override
  protected synchronized void before(AbstractSimpleReadConfig config,
      JobWorkerContext jobWorkerContext) throws Exception {
    Configuration.set(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, config.getReadType());
    // instantiates the queue
    if (mReadBytesQueue == null) {
      mReadBytesQueue = new ConcurrentLinkedQueue<>();
    }
  }

  @Override
  protected void run(T config, R args, JobWorkerContext jobWorkerContext, int batch,
      int threadIndex) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String path = getReadFilePath(config, jobWorkerContext, batch, threadIndex);

    long bufferSize = FormatUtils.parseSpaceSize(config.getBufferSize());
    ReadType readType = config.getReadType();
    long readBytes;
    try (InputStream is = fs.open(path, readType)) {
      readBytes = BenchmarkUtils.readInputStream(is, (int) bufferSize);
    }
    mReadBytesQueue.add(readBytes);
  }

  @Override
  public IOThroughputResult process(T config, List<List<Long>> benchmarkThreadTimeList) {
    Preconditions
        .checkArgument(benchmarkThreadTimeList.size() == 1, "SimpleRead only does one batch");
    // calc the average time
    long totalTimeNs = 0;
    for (long time : benchmarkThreadTimeList.get(0)) {
      totalTimeNs += time;
    }
    long totalBytes = 0;
    for (long bytes : mReadBytesQueue) {
      totalBytes += bytes;
    }
    // release the queue
    mReadBytesQueue = null;

    double averageThroughputBpns = totalBytes / (double) totalTimeNs;
    double averageThroughputMbps = (averageThroughputBpns / Constants.MB) * Constants.SECOND_NANO;
    double averageTimeNs = totalTimeNs / (double) config.getThreadNum();
    double averageTimeMs = (averageTimeNs / Constants.SECOND_NANO) * Constants.SECOND_MS;

    return new IOThroughputResult(averageThroughputMbps, averageTimeMs);
  }

  /**
   * @param config config
   * @param jobWorkerContext job worker context
   * @param batch batch of this job
   * @param threadIndex thread of this executor
   * @return the path to the file to read
   */
  public String getReadFilePath(T config, JobWorkerContext jobWorkerContext, int batch,
      int threadIndex) {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    if (config.getFileToRead() != null) {
      return config.getFileToRead();
    } else {
      return getWritePrefix(config.getBaseDir(), fs, jobWorkerContext) + "/" + threadIndex;
    }
  }
}
