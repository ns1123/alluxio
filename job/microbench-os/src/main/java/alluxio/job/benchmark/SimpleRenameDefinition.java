/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.Constants;
import alluxio.collections.ConcurrentHashSet;
import alluxio.job.JobMasterContext;
import alluxio.job.JobWorkerContext;
import alluxio.job.fs.AbstractFS;
import alluxio.job.fs.JobUtils;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A simple rename micro benchmark that renames a file in a thread. By default, this benchmark task
 * will rename files written by the {@link SimpleWriteDefinition}, so each thread will rename the
 * file simple-read-write/[task-id]/[thread-id] to simple-read-write/[task-id]-[thread-id].
 */
public final class SimpleRenameDefinition
<<<<<<< HEAD:job/microbench-os/src/main/java/alluxio/job/benchmark/SimpleRenameDefinition.java
    extends AbstractIOBenchmarkDefinition<SimpleRenameConfig, SerializableVoid> {

||||||| merged common ancestors
    extends AbstractNoArgBenchmarkJobDefinition<SimpleRenameConfig, IOThroughputResult> {
  private static final Logger LOG = LoggerFactory.getLogger(alluxio.Constants.LOGGER_TYPE);
=======
    extends AbstractNoArgBenchmarkJobDefinition<SimpleRenameConfig, IOThroughputResult> {

>>>>>>> enterprise-1.4:job/microbench/src/main/java/alluxio/job/benchmark/SimpleRenameDefinition.java
  /** A queue tracks the total renamed byte per thread. */
  private ConcurrentLinkedQueue<Long> mRenamedBytesQueue = null;
  private ConcurrentHashSet<String> mPathsToDelete = new ConcurrentHashSet<>();

  /**
   * Constructs a new {@link SimpleRenameDefinition}.
   */
  public SimpleRenameDefinition() {}

  @Override
  public Map<WorkerInfo, SerializableVoid> selectExecutors(SimpleRenameConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) throws Exception {
    Map<WorkerInfo, SerializableVoid> result = new TreeMap<>(JobUtils.createWorkerInfoComparator());
    for (WorkerInfo workerInfo : workerInfoList) {
      result.put(workerInfo, (SerializableVoid) null);
    }
    return result;
  }

  @Override
  public String join(SimpleRenameConfig config, Map<WorkerInfo, IOThroughputResult> taskResults)
      throws Exception {
    return ReportFormatUtils.createThroughputResultReport(config, taskResults,
        DatabaseConstants.MICROBENCH_DURATION_THROUGHPUT);
  }

  @Override
  protected synchronized void before(SimpleRenameConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    // instantiates the queue
    if (mRenamedBytesQueue == null) {
      mRenamedBytesQueue = new ConcurrentLinkedQueue<>();
    }
  }

  @Override
  protected void run(SimpleRenameConfig config, SerializableVoid args,
                     JobWorkerContext jobWorkerContext, int batch, int threadIndex) throws Exception {
    AbstractFS fs = config.getFileSystemType().getFileSystem();
    String src = SimpleWriteDefinition.getWritePrefix(config.getBaseDir(), fs, jobWorkerContext)
        + "/" + threadIndex;
    String dst = SimpleWriteDefinition.getWritePrefix(config.getBaseDir(), fs, jobWorkerContext)
        + "-" + threadIndex;

    long renamedBytes = renameFile(fs, src, dst);
    mPathsToDelete.add(dst);
    mRenamedBytesQueue.add(renamedBytes);
  }

  private long renameFile(AbstractFS fs, String src, String dst) throws Exception {
    fs.rename(src, dst);
    return fs.getLength(dst);
  }

  @Override
  protected IOThroughputResult process(SimpleRenameConfig config,
                                       List<List<Long>> benchmarkThreadTimeList) {
    Preconditions.checkArgument(benchmarkThreadTimeList.size() == 1,
        "SimpleWrite only does one batch");
    // calc the average time
    long totalTimeNs = 0;
    for (long time : benchmarkThreadTimeList.get(0)) {
      totalTimeNs += time;
    }
    long totalBytes = 0;
    for (long bytes : mRenamedBytesQueue) {
      totalBytes += bytes;
    }
    // release the queue
    mRenamedBytesQueue = null;

    double averageThroughputBpns = totalBytes / (double) totalTimeNs;
    double averageThroughputMbps = (averageThroughputBpns / Constants.MB) * Constants.SECOND_NANO;
    double averageTimeNs = totalTimeNs / (double) config.getThreadNum();
    double averageTimeMs = (averageTimeNs / Constants.SECOND_NANO) * Constants.SECOND_MS;

    return new IOThroughputResult(averageThroughputMbps, averageTimeMs);
  }

  @Override
  public Class<SimpleRenameConfig> getJobConfigClass() {
    return SimpleRenameConfig.class;
  }
}
